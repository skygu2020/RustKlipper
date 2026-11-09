//! MCU clock regression and host/MCU time conversion.
//! Ported from Klipper klippy/clocksync.py for USB serial transports.
use std::time::Duration;
const DECAY: f64 = 1.0 / 30.0;
const RTT_AGE: f64 = 0.000010 / (60.0 * 60.0);
const TRANSMIT_EXTRA: f64 = 0.001;
const OUTLIER_SIGMA: f64 = 5.0;
const OUTLIER_FLOOR_SECONDS: f64 = 0.000500;

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
    pub fn new(mcu_frequency: f64, initial_host_time: f64, initial_clock: u64)
        -> Result<Self, ClockSyncError>
    {
        if !mcu_frequency.is_finite() || mcu_frequency <= 0.0 {
            return Err(ClockSyncError::InvalidFrequency);
        }
        if !initial_host_time.is_finite() {
            return Err(ClockSyncError::InvalidTimestamp);
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
            prediction_variance: (0.001 * mcu_frequency).powi(2),
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

    /// Process a get_clock response with monotonic send and receive timestamps.
    pub fn update(&mut self, clock32: u32, sent_time: f64, receive_time: f64)
        -> Result<Option<ClockUpdate>, ClockSyncError>
    {
        if !sent_time.is_finite() || !receive_time.is_finite() || receive_time < sent_time {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClockSyncError { InvalidFrequency, InvalidTimestamp, InvalidRegression }

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
        Duration::from_secs_f64(self.byte_time * bytes as f64)
    }
    pub fn seconds_for_bytes(self, bytes: usize) -> f64 {
        self.byte_time * bytes as f64
    }
    pub fn idle_until(self) -> f64 { self.idle_until }

    /// Reserve UART time including bytes already pending in the host driver.
    pub fn reserve_transmission(&mut self, now: f64, pending_bytes: usize, frame_bytes: usize) -> f64 {
        let start = now.max(self.idle_until);
        self.idle_until = start + self.seconds_for_bytes(pending_bytes + frame_bytes);
        self.idle_until
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(sync.last_clock(), 0x2_0000_0000);
    }

    #[test]
    fn arbitrary_event_clock_can_still_extend_slightly_backward() {
        let sync = ClockSync::new(1_000_000.0, 10.0, 0x1_0000_0010).unwrap();
        assert_eq!(sync.clock32_to_clock64(0xffff_fff0), 0xffff_fff0);
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
