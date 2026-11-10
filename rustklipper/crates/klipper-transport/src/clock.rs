//! MCU clock regression and host/MCU time conversion.
//! Ported from Klipper klippy/clocksync.py for USB serial transports.
use std::time::Duration;
const DECAY: f64 = 1.0 / 30.0;
const RTT_AGE: f64 = 0.000010 / (60.0 * 60.0);
const TRANSMIT_EXTRA: f64 = 0.001;
const OUTLIER_SIGMA: f64 = 5.0;
const OUTLIER_FLOOR_SECONDS: f64 = 0.000500;

/// Keep UART reservation deadlines finite when adding a duration near the f64 limit.
fn saturating_host_time_add(time: f64, duration: f64) -> f64 {
    let result = time + duration;
    if result.is_finite() { result } else { f64::MAX }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockEstimate {
    pub sample_time: f64,
    pub clock: f64,
    pub frequency: f64,
}
impl ClockEstimate {
    pub fn clock_at(self, host_time: f64) -> u64 {
        (self.clock + (host_time - self.sample_time) * self.frequency).max(0.0) as u64
    }
    pub fn host_time_at(self, clock: u64) -> f64 {
        self.sample_time + (clock as f64 - self.clock) / self.frequency
    }
    pub fn is_valid(self) -> bool {
        self.sample_time.is_finite() && self.clock.is_finite()
            && self.frequency.is_finite() && self.frequency > 0.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockUpdate {
    pub queue_frequency: f64,
    pub queue_conv_time: f64,
    pub queue_conv_clock: u64,
    pub estimate: ClockEstimate,
    pub rejected_outlier: bool,
}

impl ClockUpdate {
    /// Convert a host monotonic timestamp to the conservative MCU clock used
    /// by the serial command queue. This is intentionally distinct from the
    /// best-RTT estimate used for timestamp conversion and multi-MCU sync.
    pub fn queue_clock_at(self, host_time: f64) -> Result<u64, ClockSyncError> {
        if !host_time.is_finite()
            || !self.queue_conv_time.is_finite()
            || !self.queue_frequency.is_finite()
            || self.queue_frequency <= 0.0
        {
            return Err(ClockSyncError::InvalidTimestamp);
        }
        let clock = self.queue_conv_clock as f64
            + (host_time - self.queue_conv_time) * self.queue_frequency;
        if !clock.is_finite() {
            return Err(ClockSyncError::InvalidTimestamp);
        }
        Ok(clock.max(0.0) as u64)
    }
}

#[derive(Debug, Clone)]
pub struct ClockSync {
    mcu_frequency: f64,
    last_clock: u64,
    time_avg: f64,
    time_variance: f64,
    clock_avg: f64,
    clock_covariance: f64,
    prediction_variance: f64,
    last_prediction_time: f64,
    min_half_rtt: f64,
    min_rtt_time: f64,
    estimate: ClockEstimate,
}

impl ClockSync {
    /// Initialize clock tracking from Klipper's `uptime high=%u clock=%u`
    /// response. `sent_time` is the host timestamp recorded when the
    /// `get_uptime` request was transmitted; `high` and `clock` form the
    /// upper and lower 32-bit halves of the MCU's 64-bit clock.
    pub fn from_uptime(
        mcu_frequency: f64,
        sent_time: f64,
        high: u32,
        clock: u32,
    ) -> Result<Self, ClockSyncError> {
        let initial_clock = (u64::from(high) << 32) | u64::from(clock);
        Self::new(mcu_frequency, sent_time, initial_clock)
    }

    pub fn new(mcu_frequency: f64, initial_host_time: f64, initial_clock: u64)
        -> Result<Self, ClockSyncError>
    {
        if !mcu_frequency.is_finite() || mcu_frequency <= 0.0 {
            return Err(ClockSyncError::InvalidFrequency);
        }
        if !initial_host_time.is_finite() || initial_host_time < 0.0 {
            return Err(ClockSyncError::InvalidTimestamp);
        }
        let prediction_variance = (0.001 * mcu_frequency).powi(2);
        if !prediction_variance.is_finite() {
            return Err(ClockSyncError::InvalidFrequency);
        }
        let estimate = ClockEstimate {
            sample_time: initial_host_time,
            clock: initial_clock as f64,
            frequency: mcu_frequency,
        };
        Ok(Self {
            mcu_frequency, last_clock: initial_clock,
            time_avg: initial_host_time, time_variance: 0.0,
            clock_avg: initial_clock as f64, clock_covariance: 0.0,
            prediction_variance,
            last_prediction_time: 0.0,
            min_half_rtt: f64::MAX, min_rtt_time: 0.0, estimate,
        })
    }

    pub fn estimate(&self) -> ClockEstimate { self.estimate }
    pub fn last_clock(&self) -> u64 { self.last_clock }
    pub fn mcu_frequency(&self) -> f64 { self.mcu_frequency }

    /// Extend an arbitrary 32-bit MCU timestamp to the closest signed delta.
    ///
    /// This is used for asynchronous status/event clocks, which may legitimately
    /// refer to a point slightly before the most recently observed clock sample.
    pub fn extend_clock32(&self, clock32: u32) -> u64 {
        let delta = clock32.wrapping_sub(self.last_clock as u32) as i32 as i64;
        if delta < 0 { self.last_clock.saturating_sub((-delta) as u64) }
        else { self.last_clock.saturating_add(delta as u64) }
    }

    /// Extend an ordered get_clock sample monotonically forward, matching
    /// Klipper ClockSync._handle_clock(). A lower low-word means wrap here.
    fn extend_clock_sample32(&self, clock32: u32) -> u64 {
        self.last_clock
            .saturating_add(clock32.wrapping_sub(self.last_clock as u32) as u64)
    }

    /// Process one of Klipper's initial clock-calibration samples.
    ///
    /// During `ClockSync.connect()`, Klipper resets `last_prediction_time`
    /// before each of its initial get_clock queries so a large forward sample
    /// is not rejected by the steady-state outlier guard. Call this for each
    /// startup calibration response, then use `update()` for periodic samples.
    pub fn update_initial_sample(
        &mut self,
        clock32: u32,
        sent_time: f64,
        receive_time: f64,
    ) -> Result<Option<ClockUpdate>, ClockSyncError> {
        let mut candidate = self.clone();
        candidate.last_prediction_time = -9999.0;
        let result = candidate.update_inner(clock32, sent_time, receive_time)?;
        *self = candidate;
        Ok(result)
    }

    /// Process a get_clock response with monotonic send and receive timestamps.
    pub fn update(&mut self, clock32: u32, sent_time: f64, receive_time: f64)
        -> Result<Option<ClockUpdate>, ClockSyncError>
    {
        let mut candidate = self.clone();
        let result = candidate.update_inner(clock32, sent_time, receive_time)?;
        *self = candidate;
        Ok(result)
    }

    fn update_inner(&mut self, clock32: u32, sent_time: f64, receive_time: f64)
        -> Result<Option<ClockUpdate>, ClockSyncError>
    {
        if !sent_time.is_finite()
            || !receive_time.is_finite()
            || sent_time < 0.0
            || receive_time < sent_time
        {
            return Err(ClockSyncError::InvalidTimestamp);
        }
        let clock = self.extend_clock_sample32(clock32);
        self.last_clock = clock;
        if sent_time == 0.0 { return Ok(None); }

        let expected = (sent_time - self.time_avg) * self.estimate.frequency + self.clock_avg;
        let diff2 = (clock as f64 - expected).powi(2);
        let outlier = diff2 > OUTLIER_SIGMA.powi(2) * self.prediction_variance
            && diff2 > (OUTLIER_FLOOR_SECONDS * self.mcu_frequency).powi(2);
        if outlier {
            if clock as f64 > expected && sent_time < self.last_prediction_time + 10.0 {
                return Ok(Some(self.current_update(true)));
            }
            self.prediction_variance = (0.001 * self.mcu_frequency).powi(2);
        } else {
            self.last_prediction_time = sent_time;
            self.prediction_variance = (1.0 - DECAY)
                * (self.prediction_variance + diff2 * DECAY);
        }

        let dt = sent_time - self.time_avg;
        self.time_avg += DECAY * dt;
        self.time_variance = (1.0 - DECAY) * (self.time_variance + dt * dt * DECAY);
        let dc = clock as f64 - self.clock_avg;
        self.clock_avg += DECAY * dc;
        self.clock_covariance = (1.0 - DECAY)
            * (self.clock_covariance + dt * dc * DECAY);

        let half_rtt = (receive_time - sent_time) * 0.5;
        let aged_rtt = (sent_time - self.min_rtt_time) * RTT_AGE;
        if half_rtt < self.min_half_rtt + aged_rtt {
            self.min_half_rtt = half_rtt;
            self.min_rtt_time = sent_time;
        }
        let frequency = if self.time_variance > f64::EPSILON {
            self.clock_covariance / self.time_variance
        } else { self.mcu_frequency };
        if !frequency.is_finite() || frequency <= 0.0 {
            return Err(ClockSyncError::InvalidRegression);
        }
        self.estimate = ClockEstimate {
            sample_time: self.time_avg + self.min_half_rtt,
            clock: self.clock_avg,
            frequency,
        };
        Ok(Some(self.current_update(false)))
    }

    fn current_update(&self, rejected_outlier: bool) -> ClockUpdate {
        let stddev = self.prediction_variance.sqrt();
        let conv_clock = (self.clock_avg - 3.0 * stddev).max(0.0) as u64;
        let frequency = if self.time_variance > f64::EPSILON {
            self.clock_covariance / self.time_variance
        } else { self.mcu_frequency };
        ClockUpdate {
            queue_frequency: frequency,
            queue_conv_time: self.time_avg + TRANSMIT_EXTRA,
            queue_conv_clock: conv_clock,
            estimate: self.estimate,
            rejected_outlier,
        }
    }

    pub fn clock32_to_clock64(&self, clock32: u32) -> u64 {
        self.extend_clock32(clock32)
    }
    pub fn print_time_to_clock(&self, print_time: f64) -> u64 {
        (print_time * self.mcu_frequency).max(0.0) as u64
    }
    pub fn clock_to_print_time(&self, clock: u64) -> f64 {
        clock as f64 / self.mcu_frequency
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClockSyncError {
    #[error("invalid MCU clock frequency")]
    InvalidFrequency,
    #[error("invalid host timestamp for MCU clock conversion")]
    InvalidTimestamp,
    #[error("MCU clock regression produced an invalid frequency")]
    InvalidRegression,
}

/// UART 8N1 wire-time model (one start, eight data, one stop bit).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UartTiming {
    baud_rate: u32,
    byte_time: f64,
    idle_until: f64,
}
impl UartTiming {
    pub fn new(baud_rate: u32) -> Result<Self, ClockSyncError> {
        if baud_rate == 0 { return Err(ClockSyncError::InvalidFrequency); }
        Ok(Self { baud_rate, byte_time: 10.0 / baud_rate as f64, idle_until: 0.0 })
    }
    pub fn baud_rate(self) -> u32 { self.baud_rate }
    pub fn byte_time(self) -> f64 { self.byte_time }
    pub fn duration_for_bytes(self, bytes: usize) -> Duration {
        let seconds = self.seconds_for_bytes(bytes);
        if !seconds.is_finite() || seconds >= u64::MAX as f64 {
            Duration::MAX
        } else {
            Duration::from_secs_f64(seconds)
        }
    }
    pub fn seconds_for_bytes(self, bytes: usize) -> f64 {
        self.byte_time * bytes as f64
    }
    pub fn idle_until(self) -> f64 { self.idle_until }

    /// Reset the estimated output deadline after the transport discards its
    /// pending driver bytes (for example before Klipper's sync-prefixed retry).
    pub fn reset_idle(&mut self, now: f64) {
        if now.is_finite() {
            self.idle_until = now;
        }
    }

    /// Extend the estimated idle deadline when the OS reports queued bytes.
    /// This takes the maximum with the existing reservation so bytes already
    /// represented by our own frame reservations are not counted twice.
    /// Invalid host timestamps are ignored rather than poisoning future timing.
    pub fn observe_pending(&mut self, now: f64, pending_bytes: usize) {
        if !now.is_finite() {
            return;
        }
        self.idle_until = self.idle_until.max(
            saturating_host_time_add(now, self.seconds_for_bytes(pending_bytes)),
        );
    }

    /// Reserve UART time including bytes already pending in the host driver.
    /// An invalid timestamp leaves the existing reservation unchanged.
    pub fn reserve_transmission(&mut self, now: f64, pending_bytes: usize, frame_bytes: usize) -> f64 {
        if !now.is_finite() {
            return self.idle_until;
        }
        let pending_until = saturating_host_time_add(now, self.seconds_for_bytes(pending_bytes));
        let start = now.max(self.idle_until).max(pending_until);
        self.idle_until = saturating_host_time_add(start, self.seconds_for_bytes(frame_bytes));
        self.idle_until
    }
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn clock_sync_rejects_negative_monotonic_timestamps() {
        assert_eq!(
            ClockSync::new(1_000_000.0, -0.001, 0),
            Err(ClockSyncError::InvalidTimestamp),
        );

        let mut sync = ClockSync::new(1_000_000.0, 1.0, 100_000).unwrap();
        let before = sync.clone();
        assert_eq!(
            sync.update(110_000, -0.001, 0.0),
            Err(ClockSyncError::InvalidTimestamp),
        );
        assert_eq!(sync.last_clock(), before.last_clock());
        assert_eq!(sync.estimate(), before.estimate());
    }

    #[test]
    fn rejected_clock_regression_does_not_mutate_sync_state() {
        let mut sync = ClockSync::new(1_000_000.0, 1.0, 100_000).unwrap();
        sync.update(110_000, 1.1, 1.102).unwrap();
        let before = sync.clone();

        let result = sync.update(120_000, 1.2, 1.1);
        assert_eq!(result, Err(ClockSyncError::InvalidTimestamp));
        assert_eq!(sync.last_clock(), before.last_clock());
        assert_eq!(sync.estimate(), before.estimate());
    }

    #[test]
    fn uart_duration_clamps_instead_of_panicking_for_unrepresentable_lengths() {
        let uart = UartTiming::new(1).unwrap();
        assert_eq!(uart.duration_for_bytes(usize::MAX), Duration::MAX);
    }

    #[test]
    fn uart_reservations_saturate_at_finite_timestamp_limit() {
        let mut uart = UartTiming::new(1).unwrap();
        let near_limit = f64::MAX - 1.0;

        uart.observe_pending(near_limit, usize::MAX);
        assert_eq!(uart.idle_until(), f64::MAX);

        uart.reset_idle(near_limit);
        let deadline = uart.reserve_transmission(near_limit, usize::MAX, usize::MAX);
        assert_eq!(deadline, f64::MAX);
        assert!(uart.idle_until().is_finite());
    }

    #[test]
    fn invalid_uart_timestamps_do_not_corrupt_reservations() {
        let mut uart = UartTiming::new(115_200).unwrap();
        uart.reserve_transmission(2.0, 0, 100);
        let reserved = uart.idle_until();

        uart.reset_idle(f64::NAN);
        assert_eq!(uart.idle_until(), reserved);
        uart.reset_idle(f64::INFINITY);
        assert_eq!(uart.idle_until(), reserved);

        uart.observe_pending(f64::NAN, 1000);
        uart.observe_pending(f64::NEG_INFINITY, 1000);
        assert_eq!(uart.idle_until(), reserved);

        assert_eq!(uart.reserve_transmission(f64::NAN, 1000, 1000), reserved);
        assert_eq!(uart.reserve_transmission(f64::INFINITY, 1000, 1000), reserved);
        assert_eq!(uart.idle_until(), reserved);
    }

    #[test]
    fn uptime_initialization_combines_high_and_low_clock_words() {
        let sync = ClockSync::from_uptime(16_000_000.0, 12.5, 3, 0x89ab_cdef).unwrap();
        assert_eq!(sync.last_clock(), 0x0000_0003_89ab_cdef);
        assert_eq!(sync.estimate().sample_time, 12.5);
        assert_eq!(sync.estimate().clock, 0x0000_0003_89ab_cdefu64 as f64);
    }

    #[test]
    fn initial_clock_sample_bypasses_steady_state_forward_outlier_guard() {
        let mut sync = ClockSync::from_uptime(1_000_000.0, 1.0, 0, 1_000_000).unwrap();
        let update = sync.update_initial_sample(1_150_000, 1.05, 1.052).unwrap().unwrap();
        assert!(!update.rejected_outlier);
        assert_eq!(sync.last_clock(), 1_150_000);
    }

    #[test]
    fn steady_state_clock_sample_still_rejects_early_forward_outlier() {
        let mut sync = ClockSync::from_uptime(1_000_000.0, 1.0, 0, 1_000_000).unwrap();
        let update = sync.update(1_150_000, 1.05, 1.052).unwrap().unwrap();
        assert!(update.rejected_outlier);
        assert_eq!(sync.last_clock(), 1_150_000);
    }

    #[test]
    fn clock32_wrap_extends_forward() {
        let sync = ClockSync::new(1_000_000.0, 10.0, 0xffff_fff0).unwrap();
        assert_eq!(sync.clock32_to_clock64(0x10), 0x1_0000_0010);
    }
    #[test]
    fn clock32_extends_small_backward_sample() {
        let sync = ClockSync::new(1_000_000.0, 10.0, 0x1_0000_0010).unwrap();
        assert_eq!(sync.clock32_to_clock64(0xffff_fff0), 0xffff_fff0);
    }
    #[test]
    fn clock_sample_extension_always_moves_forward_across_wrap() {
        let mut sync = ClockSync::new(1_000_000.0, 10.0, 0x1_0000_0010).unwrap();

        // get_clock samples are ordered observations. A lower low-word is
        // interpreted as the next 32-bit epoch, not a stale event timestamp.
        assert_eq!(sync.update(0xffff_fff0, 0.0, 0.0).unwrap(), None);
        assert_eq!(sync.last_clock(), 0x1_ffff_fff0);
    }

    #[test]
    fn arbitrary_event_clock_can_still_extend_slightly_backward() {
        let sync = ClockSync::new(1_000_000.0, 10.0, 0x1_0000_0010).unwrap();
        assert_eq!(sync.clock32_to_clock64(0xffff_fff0), 0xffff_fff0);
    }

    #[test]
    fn queue_clock_conversion_uses_queue_conservative_baseline() {
        let update = ClockUpdate {
            queue_frequency: 1_000.0,
            queue_conv_time: 10.0,
            queue_conv_clock: 5_000,
            estimate: ClockEstimate {
                sample_time: 10.1,
                clock: 5_100.0,
                frequency: 1_000.0,
            },
            rejected_outlier: false,
        };
        assert_eq!(update.queue_clock_at(11.0).unwrap(), 6_000);
        assert_eq!(update.queue_clock_at(9.0).unwrap(), 4_000);
    }

    #[test]
    fn queue_clock_conversion_rejects_invalid_host_time() {
        let update = ClockUpdate {
            queue_frequency: 1_000.0,
            queue_conv_time: 10.0,
            queue_conv_clock: 5_000,
            estimate: ClockEstimate {
                sample_time: 10.0,
                clock: 5_000.0,
                frequency: 1_000.0,
            },
            rejected_outlier: false,
        };
        assert_eq!(update.queue_clock_at(f64::NAN), Err(ClockSyncError::InvalidTimestamp));
    }

    #[test]
    fn queue_clock_conversion_rejects_arithmetic_overflow() {
        let update = ClockUpdate {
            queue_frequency: 2.0,
            queue_conv_time: -f64::MAX,
            queue_conv_clock: 0,
            estimate: ClockEstimate {
                sample_time: 0.0,
                clock: 0.0,
                frequency: 2.0,
            },
            rejected_outlier: false,
        };
        assert_eq!(
            update.queue_clock_at(f64::MAX),
            Err(ClockSyncError::InvalidTimestamp),
        );
    }

    #[test]
    fn clock_sync_errors_implement_standard_error_traits() {
        let error = ClockSyncError::InvalidFrequency;
        assert_eq!(error.to_string(), "invalid MCU clock frequency");
        let _: &dyn std::error::Error = &error;
    }

    #[test]
    fn clock_conversion_uses_estimated_frequency() {
        let estimate = ClockEstimate { sample_time: 5.0, clock: 1_000.0, frequency: 2_000.0 };
        assert_eq!(estimate.clock_at(6.0), 3_000);
        assert_eq!(estimate.host_time_at(3_000), 6.0);
    }
    #[test]
    fn uart_8n1_uses_ten_bits_per_byte() {
        let uart = UartTiming::new(250_000).unwrap();
        assert!((uart.seconds_for_bytes(100) - 0.004).abs() < 1e-12);
    }
    #[test]
    fn uart_reservation_accounts_for_pending_bytes() {
        let mut uart = UartTiming::new(115_200).unwrap();
        let first = uart.reserve_transmission(1.0, 0, 10);
        let second = uart.reserve_transmission(1.001, 5, 10);
        assert!(second > first && second > 1.0);
    }
    #[test]
    fn uart_reservation_does_not_double_count_observed_pending_bytes() {
        let mut uart = UartTiming::new(100_000).unwrap();
        let first = uart.reserve_transmission(1.0, 0, 100);
        let second = uart.reserve_transmission(1.001, 100, 10);
        assert!((first - 1.01).abs() < 1e-12);
        assert!((second - 1.012).abs() < 1e-12);
    }

    #[test]
    fn uart_reservations_serialize_frames_without_recounting_driver_bytes() {
        let mut uart = UartTiming::new(115_200).unwrap();
        let first = uart.reserve_transmission(1.0, 0, 10);
        let second = uart.reserve_transmission(1.0005, 0, 10);
        let frame_time = uart.seconds_for_bytes(10);
        assert!((first - (1.0 + frame_time)).abs() < 1e-12);
        assert!((second - (first + frame_time)).abs() < 1e-12);
    }

    #[test]
    fn observed_driver_backlog_extends_but_never_double_counts_uart_time() {
        let mut uart = UartTiming::new(100_000).unwrap();
        let reserved = uart.reserve_transmission(1.0, 0, 100);
        assert!((reserved - 1.01).abs() < 1e-12);

        uart.observe_pending(1.005, 100);
        assert!((uart.idle_until() - 1.015).abs() < 1e-12);
        uart.observe_pending(1.006, 10);
        assert!((uart.idle_until() - 1.015).abs() < 1e-12);
    }

    #[test]
    fn uart_idle_deadline_can_be_reset_after_output_queue_flush() {
        let mut uart = UartTiming::new(100_000).unwrap();
        let first = uart.reserve_transmission(1.0, 0, 100);
        assert!(first > 1.0);
        uart.reset_idle(1.001);
        let retry = uart.reserve_transmission(1.001, 0, 10);
        assert!((retry - 1.002).abs() < 1e-12);
    }

    #[test]
    fn uart_reservation_starts_at_now_after_previous_idle_deadline() {
        let mut uart = UartTiming::new(100_000).unwrap();
        let first = uart.reserve_transmission(1.0, 0, 10);
        let second = uart.reserve_transmission(2.0, 0, 10);
        assert!((first - 1.001).abs() < 1e-12);
        assert!((second - 2.001).abs() < 1e-12);
    }

    #[test]
    fn clock_sync_rejects_frequency_that_overflows_prediction_variance() {
        assert!(matches!(
            ClockSync::new(f64::MAX, 1.0, 0),
            Err(ClockSyncError::InvalidFrequency),
        ));
    }

    #[test]
    fn clock_sync_converges_on_constant_frequency() {
        let mut sync = ClockSync::new(1_000_000.0, 1.0, 1_000_000).unwrap();
        for i in 1..=12 {
            let t = 1.0 + i as f64 * 0.1;
            let clock = 1_000_000 + i * 100_000;
            sync.update(clock as u32, t, t + 0.002).unwrap();
        }
        assert!((sync.estimate().frequency - 1_000_000.0).abs() < 5_000.0);
    }
}
