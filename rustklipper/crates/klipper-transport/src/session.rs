//! Reliable USB serial session: frame I/O, scheduling, ACK/NAK and retries.
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

use serde_json::Value;

/// Klipper ClockSync periodic query cadence.
const PERIODIC_CLOCK_INTERVAL_SECONDS: f64 = 0.9839;
use klipper_protocol::{encode_identify_request, DictionaryError, Frame, IdentifyAssembler, IdentifyChunk, IdentifyDictionary, IDENTIFY_CHUNK_SIZE, MESSAGE_MAX, MESSAGE_MIN};

use crate::{
    AckEvent, AckRetryDecision, AckTracker, ClockUpdate, CommandScheduler, ConfigAction, ConfigError, ConfigResponse, McuConfigHandshake, Response, ResponseRouter, ResponseWaiter, RetryTransaction, ScheduleError, ScheduledCommand, TransportError, UsbSerialTransport,
};

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Schedule(#[from] ScheduleError),
    #[error("serial session is no longer usable after an I/O failure")]
    Disconnected,
    #[error("notification ID is already registered or could not be bound to a sent frame: {0}")]
    Notification(u64),
    #[error("response-tracked command already has a notification ID")]
    CommandAlreadyNotified,
    #[error("register a response waiter before enqueueing {name} with oid {oid:?}")]
    MissingResponseWaiter { name: String, oid: Option<u32> },
    #[error("identify requests must use the same scheduler queue")]
    IdentifyQueueMismatch,
    #[error("identify chunk size must be between 1 and 255 bytes")]
    InvalidIdentifyChunkSize,
    #[error(transparent)]
    Dictionary(#[from] DictionaryError),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Clock(#[from] crate::ClockSyncError),
    #[error("expected an unscoped config response, received {name} with oid {oid:?}")]
    UnexpectedConfigResponse { name: String, oid: Option<u32> },
    #[error("invalid MCU startup order: expected {expected}, current stage is {actual:?}")]
    StartupOrder { expected: &'static str, actual: McuStartupStage },
}

#[derive(Debug, Clone, PartialEq)]
pub enum SessionEvent {
    Received {
        frame: Frame,
        result: crate::ReceivedFrameResult,
    },
    AckedNotify { id: u64, sent_time: f64, receive_time: f64 },
    ResponseRouted { name: String, oid: Option<u32>, matched: bool },
    RetryExhausted { name: String, oid: Option<u32> },
    Output { message: String, receive_time: f64 },
    Sent {
        sequence: u64,
        bytes: usize,
    },
    Retransmitted {
        sequence: u64,
        bytes: usize,
    },
    IdentifyChunk { chunk: IdentifyChunk, receive_time: f64 },
    IdentifyProgress { progress: IdentifyProgress },
    DecodedMessage { name: String, params: BTreeMap<String, Value>, receive_time: f64 },
    DecodeError { error: String, receive_time: f64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentifyProgress {
    More { next_offset: u32 },
    Complete { version: String },
}

/// Observable high-level state of the MCU startup sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McuStartupStage {
    AwaitingIdentify,
    Identified,
    AwaitingUptime,
    CalibratingClock { remaining_samples: u8 },
    ClockReady,
    Configuring,
    Ready,
}

/// Owns one USB serial link and its reliable MCU command scheduler.
///
/// A transport or protocol error is terminal for this session. Re-open the
/// serial device and perform MCU identify/configuration again before reuse.
#[derive(Debug, Clone)]
struct PendingResponseSend { transaction_id: u64, name: String, oid: Option<u32> }

#[derive(Debug, Clone)]
struct PendingRetryRequest {
    queue_id: u64,
    command: ScheduledCommand,
    name: String,
    oid: Option<u32>,
    waiter_id: u64,
    transaction: RetryTransaction,
}

fn advance_ack_clock(last_ack_clock: u64, observed_ack_clock: u64) -> u64 {
    last_ack_clock.max(observed_ack_clock)
}

/// Compute the next reactor-style periodic clock deadline.
///
/// The cadence is anchored to the prior scheduled deadline. If polling was
/// delayed across one or more intervals, advance to the first future tick
/// instead of emitting a burst of catch-up requests.
fn next_periodic_clock_deadline(due_at: f64, now: f64) -> f64 {
    let mut next_due = due_at + PERIODIC_CLOCK_INTERVAL_SECONDS;
    while next_due <= now {
        next_due += PERIODIC_CLOCK_INTERVAL_SECONDS;
    }
    next_due
}

fn allocate_retry_transaction_id(
    next_id: &mut u64,
    in_flight: &HashMap<u64, PendingRetryRequest>,
) -> u64 {
    loop {
        let candidate = (*next_id).max(1);
        *next_id = candidate.wrapping_add(1).max(1);
        if !in_flight.contains_key(&candidate) {
            return candidate;
        }
    }
}

/// Finalize ACK-exhausted requests only after the current frame's payload has
/// had a chance to complete them. Returns transaction IDs whose attempt state
/// still needs to be retired by the owning session.
fn finalize_exhausted_transactions(
    exhausted: Vec<(u64, String, Option<u32>, u64)>,
    retry_transactions: &mut HashMap<u64, PendingRetryRequest>,
    sent_response_requests: &mut VecDeque<PendingResponseSend>,
    response_router: &ResponseRouter,
    events: &mut Vec<SessionEvent>,
) -> Vec<u64> {
    let mut retired = Vec::new();
    for (transaction_id, name, oid, waiter_id) in exhausted {
        if retry_transactions.remove(&transaction_id).is_some() {
            sent_response_requests.retain(|entry| entry.transaction_id != transaction_id);
            response_router.cancel_waiter(&name, oid, waiter_id);
            events.push(SessionEvent::RetryExhausted { name, oid });
            retired.push(transaction_id);
        }
    }
    retired
}

pub struct ReliableSerialSession {
    transport: UsbSerialTransport,
    scheduler: CommandScheduler,
    ack_tracker: AckTracker,
    response_router: ResponseRouter,
    response_requests: HashMap<u64, u64>,
    retry_attempts: HashMap<u64, u64>,
    retry_transactions: HashMap<u64, PendingRetryRequest>,
    next_retry_transaction_id: u64,
    response_ack_receivers: HashMap<u64, std::sync::mpsc::Receiver<AckEvent>>,
    sent_response_requests: VecDeque<PendingResponseSend>,
    next_response_notify_id: u64,
    dictionary: Option<IdentifyDictionary>,
    clock_sync: Option<crate::ClockSync>,
    clock_calibration_remaining: u8,
    periodic_clock_in_flight: bool,
    periodic_clock_waiter: Option<ResponseWaiter>,
    periodic_clock_queue_id: Option<u64>,
    next_periodic_clock_at: Option<f64>,
    startup_stage: McuStartupStage,
    identify_assembler: IdentifyAssembler,
    identify_queue_id: Option<u64>,
    identify_chunk_count: Option<u8>,
    started: Instant,
    wire_time_per_byte: f64,
    last_ack_clock: u64,
    disconnected: bool,
}

impl ReliableSerialSession {
    pub fn open(settings: &crate::SerialSettings) -> Result<Self, SessionError> {
        if settings.baud_rate == 0 {
            return Err(SessionError::Schedule(ScheduleError::Retransmit(
                crate::RetransmitError::InvalidTime,
            )));
        }
        let mut session = Self::new(UsbSerialTransport::open(settings)?);
        session.wire_time_per_byte = 10.0 / settings.baud_rate as f64;
        session.scheduler.set_wire_time_per_byte(session.wire_time_per_byte)?;
        Ok(session)
    }

    pub fn new(transport: UsbSerialTransport) -> Self {
        Self {
            transport,
            scheduler: CommandScheduler::new(),
            ack_tracker: AckTracker::new(),
            response_router: ResponseRouter::new(),
            response_requests: HashMap::new(),
            retry_attempts: HashMap::new(),
            retry_transactions: HashMap::new(),
            next_retry_transaction_id: 1,
            response_ack_receivers: HashMap::new(),
            sent_response_requests: VecDeque::new(),
            next_response_notify_id: u64::MAX,
            dictionary: None,
            clock_sync: None,
            clock_calibration_remaining: 0,
            periodic_clock_in_flight: false,
            periodic_clock_waiter: None,
            periodic_clock_queue_id: None,
            next_periodic_clock_at: None,
            startup_stage: McuStartupStage::AwaitingIdentify,
            identify_assembler: IdentifyAssembler::new(),
            identify_queue_id: None,
            identify_chunk_count: None,
            started: Instant::now(),
            wire_time_per_byte: 0.0,
            last_ack_clock: 0,
            disconnected: false,
        }
    }

    /// Return this session's monotonic host timestamp in seconds.
    ///
    /// Use this clock for every timestamp passed to `ClockSync::new`,
    /// `ClockSync::update`, and `ReliableSerialSession::apply_clock_update`.
    /// Its origin is the construction of this session (not the system boot
    /// time), matching the timestamps emitted in `SessionEvent`.
    pub fn monotonic_time(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }

    fn now(&self) -> f64 {
        self.monotonic_time()
    }

    fn ensure_connected(&self) -> Result<(), SessionError> {
        if self.disconnected { Err(SessionError::Disconnected) } else { Ok(()) }
    }

    /// Install the MCU-provided dictionary after identify has completed.
    pub fn set_dictionary(&mut self, dictionary: IdentifyDictionary) {
        self.dictionary = Some(dictionary);
        self.startup_stage = McuStartupStage::Identified;
    }

    pub fn has_dictionary(&self) -> bool { self.dictionary.is_some() }

    /// Current high-level MCU startup phase. This is observational; the
    /// lower-level request APIs remain available for advanced/manual flows.
    pub fn startup_stage(&self) -> McuStartupStage {
        self.startup_stage
    }

    /// Consume one bootstrap identify_response chunk. Returns the next byte
    /// offset while more data is needed; installs the dictionary on completion.
    pub fn accept_identify_chunk(&mut self, chunk: IdentifyChunk) -> Result<IdentifyProgress, SessionError> {
        self.ensure_connected()?;
        if self.dictionary.is_some() {
            return Err(SessionError::Dictionary(DictionaryError::InvalidField {
                field: "identify_response".into(),
                reason: "dictionary is already installed".into(),
            }));
        }
        let complete = self.identify_assembler.push(chunk)?;
        if complete {
            let assembler = std::mem::take(&mut self.identify_assembler);
            let dictionary = assembler.into_dictionary()?;
            let version = dictionary.version().to_owned();
            self.set_dictionary(dictionary);
            Ok(IdentifyProgress::Complete { version })
        } else {
            let next_offset = self.identify_assembler.next_offset()?;
            // Continue the bootstrap exchange automatically on the queue that
            // carried the initial identify request.
            if let Some(queue_id) = self.identify_queue_id {
                let count = self.identify_chunk_count.unwrap_or(IDENTIFY_CHUNK_SIZE);
                let bytes = encode_identify_request(next_offset, count);
                self.scheduler.enqueue(queue_id, ScheduledCommand::new(
                    bytes, 0, crate::BACKGROUND_PRIORITY_CLOCK,
                ))?;
                self.scheduler.promote_ready(self.last_ack_clock);
            }
            Ok(IdentifyProgress::More { next_offset })
        }
    }

    /// Register a notification before enqueueing a command carrying this ID.
    pub fn register_notify(&mut self, id: u64) -> Result<std::sync::mpsc::Receiver<crate::AckEvent>, SessionError> {
        self.ensure_connected()?;
        self.ack_tracker.register_notify(id).ok_or(SessionError::Notification(id))
    }

    /// Register a response waiter before enqueueing the command that requests it.
    pub fn register_response(&self, name: impl Into<String>, oid: Option<u32>) -> ResponseWaiter {
        self.response_router.register(name, oid)
    }

    pub fn response_router(&self) -> ResponseRouter { self.response_router.clone() }

    /// Queue a dictionary-encoded get_config query and return its response waiter.
    /// The caller must continue polling the session while waiting for the response.
    pub fn request_get_config(&mut self, queue_id: u64) -> Result<ResponseWaiter, SessionError> {
        self.ensure_connected()?;
        let dictionary = self.dictionary.as_ref().ok_or_else(|| {
            SessionError::Dictionary(DictionaryError::UnknownMessage("get_config".into()))
        })?;
        let command = dictionary.encode_command("get_config", &BTreeMap::new())?;
        let waiter = self.response_router.register("config", None);
        self.enqueue_request(
            queue_id,
            ScheduledCommand::new(command, 0, crate::BACKGROUND_PRIORITY_CLOCK),
            "config",
            None,
        )?;
        self.scheduler.promote_ready(self.last_ack_clock);
        Ok(waiter)
    }

    /// Start the clock bootstrap sequence by requesting MCU uptime.
    /// After receiving this waiter's response, pass it to
    /// begin_clock_calibration() to initialize ClockSync and queue sample one.
    pub fn start_clock_calibration(
        &mut self,
        queue_id: u64,
    ) -> Result<ResponseWaiter, SessionError> {
        if self.startup_stage != McuStartupStage::Identified {
            return Err(SessionError::StartupOrder {
                expected: "Identified",
                actual: self.startup_stage,
            });
        }
        if !self.has_dictionary() {
            return Err(SessionError::Dictionary(DictionaryError::UnknownMessage("get_uptime".into())));
        }
        let waiter = self.request_get_uptime(queue_id)?;
        self.startup_stage = McuStartupStage::AwaitingUptime;
        Ok(waiter)
    }
    /// Queue Klipper's initial uptime query after identify has installed the dictionary.
    /// The returned response carries the actual UART-write timestamp used to
    /// establish the initial host/MCU clock correspondence.
    pub fn request_get_uptime(&mut self, queue_id: u64) -> Result<ResponseWaiter, SessionError> {
        self.ensure_connected()?;
        let dictionary = self.dictionary.as_ref().ok_or_else(|| {
            SessionError::Dictionary(DictionaryError::UnknownMessage("get_uptime".into()))
        })?;
        let command = dictionary.encode_command("get_uptime", &BTreeMap::new())?;
        let waiter = self.response_router.register("uptime", None);
        self.enqueue_request(
            queue_id,
            ScheduledCommand::new(command, 0, crate::BACKGROUND_PRIORITY_CLOCK),
            "uptime",
            None,
        )?;
        self.scheduler.promote_ready(self.last_ack_clock);
        Ok(waiter)
    }

    /// Queue one get_clock calibration/synchronization query.
    /// Registering the response before enqueueing preserves the request/write
    /// timestamp association maintained by the session.
    pub fn request_get_clock(&mut self, queue_id: u64) -> Result<ResponseWaiter, SessionError> {
        self.ensure_connected()?;
        let dictionary = self.dictionary.as_ref().ok_or_else(|| {
            SessionError::Dictionary(DictionaryError::UnknownMessage("get_clock".into()))
        })?;
        let command = dictionary.encode_command("get_clock", &BTreeMap::new())?;
        let waiter = self.response_router.register("clock", None);
        self.enqueue_request(
            queue_id,
            ScheduledCommand::new(command, 0, crate::BACKGROUND_PRIORITY_CLOCK),
            "clock",
            None,
        )?;
        self.scheduler.promote_ready(self.last_ack_clock);
        Ok(waiter)
    }

    /// Feed a get_clock response into ClockSync and advance scheduled MCU work.
    ///
    /// Set initial_sample for each of Klipper's eight startup calibration
    /// queries; use false for subsequent periodic clock samples. Both response
    /// timestamps are generated by this session's monotonic clock.
    pub fn apply_clock_response(
        &mut self,
        clock_sync: &mut crate::ClockSync,
        response: Response,
        initial_sample: bool,
    ) -> Result<Option<u64>, SessionError> {
        self.ensure_connected()?;
        if response.name != "clock" || response.oid.is_some() {
            return Err(SessionError::Dictionary(DictionaryError::InvalidField {
                field: "clock".into(),
                reason: format!("expected unscoped clock response, received {} with oid {:?}", response.name, response.oid),
            }));
        }
        let clock = response.params.get("clock").and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| DictionaryError::InvalidField {
                field: "clock.clock".into(),
                reason: "missing or out-of-range u32 value".into(),
            })?;
        let update = if initial_sample {
            clock_sync.update_initial_sample(clock, response.sent_time, response.receive_time)?
        } else {
            clock_sync.update(clock, response.sent_time, response.receive_time)?
        };
        match update {
            Some(update) => self.apply_clock_update(update, response.receive_time),
            None => Ok(None),
        }
    }
    /// Build ClockSync from the response to request_get_uptime().
    ///
    /// This deliberately uses Response.sent_time (the timestamp of the
    /// successful request write), not receive_time or the time the caller
    /// happens to consume the waiter. CLOCK_FREQ comes from the identified MCU.
    pub fn clock_sync_from_uptime_response(
        &self,
        response: Response,
    ) -> Result<crate::ClockSync, SessionError> {
        self.ensure_connected()?;
        if response.name != "uptime" || response.oid.is_some() {
            return Err(SessionError::Dictionary(DictionaryError::InvalidField {
                field: "uptime".into(),
                reason: format!("expected unscoped uptime response, received {} with oid {:?}", response.name, response.oid),
            }));
        }
        let dictionary = self.dictionary.as_ref().ok_or_else(|| {
            SessionError::Dictionary(DictionaryError::UnknownMessage("uptime".into()))
        })?;
        let frequency = dictionary.constant_f64("CLOCK_FREQ").ok_or_else(|| {
            DictionaryError::InvalidField {
                field: "CLOCK_FREQ".into(),
                reason: "missing or invalid MCU clock frequency".into(),
            }
        })?;
        let high = response.params.get("high").and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| DictionaryError::InvalidField {
                field: "uptime.high".into(),
                reason: "missing or out-of-range u32 value".into(),
            })?;
        let clock = response.params.get("clock").and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| DictionaryError::InvalidField {
                field: "uptime.clock".into(),
                reason: "missing or out-of-range u32 value".into(),
            })?;
        Ok(crate::ClockSync::from_uptime(frequency, response.sent_time, high, clock)?)
    }

    /// Begin Klipper-compatible startup clock calibration from the uptime response.
    /// This queues the first of eight get_clock requests; each subsequent request
    /// is queued only after the previous response has been processed.
    pub fn begin_clock_calibration(
        &mut self,
        queue_id: u64,
        uptime_response: Response,
    ) -> Result<ResponseWaiter, SessionError> {
        if self.startup_stage != McuStartupStage::AwaitingUptime {
            return Err(SessionError::StartupOrder {
                expected: "AwaitingUptime",
                actual: self.startup_stage,
            });
        }
        let clock_sync = self.clock_sync_from_uptime_response(uptime_response)?;
        self.clock_sync = Some(clock_sync);
        self.clock_calibration_remaining = 8;
        self.startup_stage = McuStartupStage::CalibratingClock { remaining_samples: 8 };
        match self.request_get_clock(queue_id) {
            Ok(waiter) => Ok(waiter),
            Err(error) => {
                self.clock_calibration_remaining = 0;
                self.clock_sync = None;
                self.startup_stage = McuStartupStage::AwaitingUptime;
                Err(error)
            }
        }
    }

    /// Process one of the eight sequential startup get_clock responses.
    /// Returns the next response waiter, or None once all eight samples are done.
    pub fn handle_clock_calibration_response(
        &mut self,
        queue_id: u64,
        response: Response,
    ) -> Result<Option<ResponseWaiter>, SessionError> {
        self.ensure_connected()?;
        if !matches!(self.startup_stage, McuStartupStage::CalibratingClock { .. }) {
            return Err(SessionError::StartupOrder {
                expected: "CalibratingClock",
                actual: self.startup_stage,
            });
        }
        if self.clock_calibration_remaining == 0 {
            return Err(SessionError::Dictionary(DictionaryError::InvalidField {
                field: "clock_calibration".into(),
                reason: "no startup clock sample is currently expected".into(),
            }));
        }
        let update = self.update_owned_clock_sync(&response, true)?;
        if let Some(update) = update {
            self.apply_clock_update(update, response.receive_time)?;
        }
        self.clock_calibration_remaining -= 1;
        if self.clock_calibration_remaining == 0 {
            self.startup_stage = McuStartupStage::ClockReady;
            self.periodic_clock_queue_id = Some(queue_id);
            self.next_periodic_clock_at = Some(response.receive_time);
            return Ok(None);
        }
        self.startup_stage = McuStartupStage::CalibratingClock {
            remaining_samples: self.clock_calibration_remaining,
        };
        self.request_get_clock(queue_id).map(Some)
    }

    /// Current MCU clock synchronizer, available after uptime initialization.
    pub fn clock_sync(&self) -> Option<&crate::ClockSync> {
        self.clock_sync.as_ref()
    }

    /// Number of initial get_clock samples still expected (out of eight).
    pub fn clock_calibration_remaining(&self) -> u8 {
        self.clock_calibration_remaining
    }

    /// Whether uptime initialization and all eight startup clock samples completed.
    pub fn is_clock_calibrated(&self) -> bool {
        self.clock_sync.is_some() && self.clock_calibration_remaining == 0
    }

    /// Process a periodic get_clock response after startup calibration.
    pub fn handle_periodic_clock_response(
        &mut self,
        response: Response,
    ) -> Result<Option<u64>, SessionError> {
        self.ensure_connected()?;
        if self.clock_calibration_remaining != 0 {
            return Err(SessionError::Dictionary(DictionaryError::InvalidField {
                field: "clock_calibration".into(),
                reason: "periodic clock update received before startup calibration completed".into(),
            }));
        }
        if !self.periodic_clock_in_flight {
            return Err(SessionError::Dictionary(DictionaryError::InvalidField {
                field: "clock_sync".into(),
                reason: "periodic clock response received without an outstanding periodic query".into(),
            }));
        }
        // The response has been consumed, so this request is no longer
        // in flight even if its payload or clock estimate proves invalid.
        // Leaving the flag set on an update error would permanently suppress
        // every future periodic query (the waiter itself has already gone).
        self.periodic_clock_in_flight = false;
        let update = self.update_owned_clock_sync(&response, false)?;
        match update {
            Some(update) => self.apply_clock_update(update, response.receive_time),
            None => Ok(None),
        }
    }

    /// Queue one periodic clock query. The caller should invoke this again only
    /// after handling the response; this guard prevents poll loops or duplicate
    /// timers from accumulating multiple outstanding get_clock requests.
    pub fn request_periodic_clock(&mut self, queue_id: u64) -> Result<ResponseWaiter, SessionError> {
        self.ensure_connected()?;
        if !self.is_clock_calibrated() {
            return Err(SessionError::Dictionary(DictionaryError::InvalidField {
                field: "clock_sync".into(),
                reason: "periodic clock query requested before startup calibration completed".into(),
            }));
        }
        if self.periodic_clock_in_flight {
            return Err(SessionError::Dictionary(DictionaryError::InvalidField {
                field: "clock_sync".into(),
                reason: "a periodic clock query is already outstanding".into(),
            }));
        }
        let waiter = self.request_get_clock(queue_id)?;
        self.periodic_clock_in_flight = true;
        Ok(waiter)
    }

    /// Queue a periodic query when Klipper's 0.9839-second cadence expires.
    ///
    /// The normal startup flow configures this timer automatically and
    /// poll() owns and consumes its response waiter. This method remains
    /// public for advanced/manual integrations; callers using it directly
    /// must retain and consume the returned waiter themselves. It returns
    /// None until the deadline and never overlaps an outstanding query.
    pub fn poll_periodic_clock(
        &mut self,
        queue_id: u64,
    ) -> Result<Option<ResponseWaiter>, SessionError> {
        self.ensure_connected()?;
        if !self.is_clock_calibrated() {
            return Ok(None);
        }
        if self.periodic_clock_in_flight {
            return Ok(None);
        }
        let due_at = match self.next_periodic_clock_at {
            Some(due_at) => due_at,
            None => return Ok(None),
        };
        let now = self.now();
        if now < due_at {
            return Ok(None);
        }
        // Keep the reactor-style cadence anchored to the previous scheduled
        // event, not to response latency. Skip missed ticks rather than
        // issuing a burst of catch-up queries after a slow response.
        let next_due = next_periodic_clock_deadline(due_at, now);
        let waiter = self.request_periodic_clock(queue_id)?;
        self.next_periodic_clock_at = Some(next_due);
        Ok(Some(waiter))
    }

    /// Next periodic query deadline in this session's monotonic time base.
    pub fn next_periodic_clock_at(&self) -> Option<f64> {
        self.next_periodic_clock_at
    }

    /// Whether a periodic get_clock request is awaiting its response.
    pub fn periodic_clock_in_flight(&self) -> bool {
        self.periodic_clock_in_flight
    }

    /// Update the ClockSync instance owned by this session. Keeping the mutable
    /// synchronizer private prevents callers from accidentally maintaining a
    /// second, divergent clock model for the same serial link.
    fn update_owned_clock_sync(
        &mut self,
        response: &Response,
        initial_sample: bool,
    ) -> Result<Option<ClockUpdate>, SessionError> {
        if response.name != "clock" || response.oid.is_some() {
            return Err(SessionError::Dictionary(DictionaryError::InvalidField {
                field: "clock".into(),
                reason: format!("expected unscoped clock response, received {} with oid {:?}", response.name, response.oid),
            }));
        }
        let clock = response.params.get("clock").and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or_else(|| DictionaryError::InvalidField {
                field: "clock.clock".into(),
                reason: "missing or out-of-range u32 value".into(),
            })?;
        let clock_sync = self.clock_sync.as_mut().ok_or_else(|| {
            SessionError::Dictionary(DictionaryError::InvalidField {
                field: "clock_sync".into(),
                reason: "clock sync has not been initialized from uptime".into(),
            })
        })?;
        if initial_sample {
            Ok(clock_sync.update_initial_sample(clock, response.sent_time, response.receive_time)?)
        } else {
            Ok(clock_sync.update(clock, response.sent_time, response.receive_time)?)
        }
    }

    /// Start the first half of the two-query MCU configuration handshake.
    pub fn begin_config_handshake(
        &mut self,
        queue_id: u64,
    ) -> Result<ResponseWaiter, SessionError> {
        if self.startup_stage != McuStartupStage::ClockReady {
            return Err(SessionError::StartupOrder {
                expected: "ClockReady",
                actual: self.startup_stage,
            });
        }
        let waiter = self.request_get_config(queue_id)?;
        self.startup_stage = McuStartupStage::Configuring;
        Ok(waiter)
    }

    /// Consume the initial response, queue config/restart/init commands, then
    /// queue the final get_config query on the same FIFO scheduler queue.
    pub fn advance_config_handshake(
        &mut self,
        queue_id: u64,
        handshake: &mut McuConfigHandshake,
        initial_response: Response,
        firmware_restart: bool,
    ) -> Result<ResponseWaiter, SessionError> {
        self.ensure_connected()?;
        if self.startup_stage != McuStartupStage::Configuring {
            return Err(SessionError::StartupOrder {
                expected: "Configuring",
                actual: self.startup_stage,
            });
        }
        let config = Self::parse_config_response(initial_response)?;
        let action = handshake.handle_initial_response(config, firmware_restart)?;
        self.enqueue_config_action(queue_id, &action)?;
        self.request_get_config(queue_id)
    }

    /// Validate the final response and return the usable move-slot count.
    pub fn complete_config_handshake(
        &mut self,
        handshake: &mut McuConfigHandshake,
        final_response: Response,
    ) -> Result<u16, SessionError> {
        self.ensure_connected()?;
        if self.startup_stage != McuStartupStage::Configuring {
            return Err(SessionError::StartupOrder {
                expected: "Configuring",
                actual: self.startup_stage,
            });
        }
        let config = Self::parse_config_response(final_response)?;
        let usable = handshake.handle_final_response(config)?;
        self.startup_stage = McuStartupStage::Ready;
        Ok(usable)
    }

    fn parse_config_response(response: Response) -> Result<ConfigResponse, SessionError> {
        if response.name != "config" || response.oid.is_some() {
            return Err(SessionError::UnexpectedConfigResponse {
                name: response.name,
                oid: response.oid,
            });
        }
        Ok(ConfigResponse::from_params(&response.params)?)
    }

    /// Queue the command sequence selected by McuConfigPlan. Commands are
    /// validated before any are added, preserving queue atomicity for malformed
    /// or oversized config messages.
    pub fn enqueue_config_action(
        &mut self,
        queue_id: u64,
        action: &ConfigAction,
    ) -> Result<(), SessionError> {
        self.ensure_connected()?;
        if self.startup_stage != McuStartupStage::Configuring {
            return Err(SessionError::StartupOrder {
                expected: "Configuring",
                actual: self.startup_stage,
            });
        }
        let commands = match action {
            ConfigAction::SendFullConfig { commands, .. }
            | ConfigAction::SendRestartAndInit { commands, .. } => commands,
        };
        let max_payload = MESSAGE_MAX - 5;
        for command in commands {
            if command.is_empty() {
                return Err(SessionError::Schedule(ScheduleError::EmptyCommand));
            }
            if command.len() > max_payload {
                return Err(SessionError::Schedule(ScheduleError::CommandTooLarge(command.len())));
            }
        }
        for command in commands {
            self.scheduler.enqueue(
                queue_id,
                ScheduledCommand::new(
                    command.clone(), 0, crate::BACKGROUND_PRIORITY_CLOCK,
                ),
            )?;
        }
        self.scheduler.promote_ready(self.last_ack_clock);
        Ok(())
    }

    /// Queue a bootstrap identify request. The protocol's built-in message ID
    /// is available before the dynamic dictionary has been downloaded.
    pub fn request_identify_chunk(
        &mut self, queue_id: u64, offset: u32, count: u8,
    ) -> Result<(), SessionError> {
        self.ensure_connected()?;
        if self.dictionary.is_some() {
            return Err(SessionError::Dictionary(DictionaryError::InvalidField {
                field: "identify".into(),
                reason: "dictionary is already installed".into(),
            }));
        }
        if count == 0 { return Err(SessionError::InvalidIdentifyChunkSize); }
        if self.identify_queue_id.is_some_and(|existing| existing != queue_id) {
            return Err(SessionError::IdentifyQueueMismatch);
        }
        if self.identify_chunk_count.is_some_and(|existing| existing != count) {
            return Err(SessionError::InvalidIdentifyChunkSize);
        }
        self.identify_queue_id = Some(queue_id);
        self.identify_chunk_count = Some(count);
        let bytes = encode_identify_request(offset, count);
        self.scheduler.enqueue(queue_id, ScheduledCommand::new(
            bytes, 0, crate::BACKGROUND_PRIORITY_CLOCK,
        ))?;
        self.scheduler.promote_ready(self.last_ack_clock);
        Ok(())
    }

    /// Enqueue a command and associate its eventual response with the command's
    /// actual successful UART-write timestamp. The command must not already carry
    /// a caller-managed notify ID.
    pub fn enqueue_request(
        &mut self,
        queue_id: u64,
        command: ScheduledCommand,
        response_name: impl Into<String>,
        oid: Option<u32>,
    ) -> Result<(), SessionError> {
        self.enqueue_request_with_retry(queue_id, command, response_name, oid, true)
    }

    /// Queue a response-tracked request with Klipper's optional application retry.
    /// When retry is false, the initial ACK is still awaited but no resend occurs.
    pub fn enqueue_request_with_retry(
        &mut self,
        queue_id: u64,
        mut command: ScheduledCommand,
        response_name: impl Into<String>,
        oid: Option<u32>,
        retry: bool,
    ) -> Result<(), SessionError> {
        self.ensure_connected()?;
        if command.notify_id.is_some() {
            return Err(SessionError::CommandAlreadyNotified);
        }
        let transaction_id = allocate_retry_transaction_id(
            &mut self.next_retry_transaction_id,
            &self.retry_transactions,
        );
        let name = response_name.into();
        let waiter_id = self.response_router.claim_oldest(&name, oid)
            .ok_or_else(|| SessionError::MissingResponseWaiter {
                name: name.clone(),
                oid,
            })?;
        let template = command.clone();
        command.notify_id = None;
        self.retry_transactions.insert(transaction_id, PendingRetryRequest {
            queue_id,
            command: template,
            name,
            oid,
            waiter_id,
            transaction: RetryTransaction::new(
                if retry { 5 } else { 0 },
                std::time::Duration::from_millis(10),
            ),
        });
        if let Err(error) = self.enqueue_retry_attempt(transaction_id, command) {
            // The response waiter was claimed before the scheduler accepted the
            // initial attempt. Roll that reservation back together with the
            // transaction so a later request cannot inherit a stale FIFO waiter.
            if let Some(pending) = self.retry_transactions.remove(&transaction_id) {
                self.response_router.cancel_waiter(&pending.name, pending.oid, pending.waiter_id);
            }
            return Err(error);
        }
        // Promote the initial attempt just like a retry. Otherwise a command
        // with min_clock == 0 stays upcoming and poll() cannot build its frame.
        self.scheduler.promote_ready(self.last_ack_clock);
        Ok(())
    }

    /// Enqueue one attempt for a logical response transaction. Each attempt
    /// receives a new ACK notify ID; command bytes and clock constraints persist.
    fn enqueue_retry_attempt(
        &mut self,
        transaction_id: u64,
        mut command: ScheduledCommand,
    ) -> Result<(), SessionError> {
        let mut candidate = self.next_response_notify_id;
        loop {
            if !self.ack_tracker.contains_notify(candidate)
                && !self.response_requests.contains_key(&candidate)
            {
                break;
            }
            candidate = candidate.wrapping_sub(1);
        }
        self.next_response_notify_id = candidate.wrapping_sub(1);
        let ack_receiver = self.ack_tracker.register_notify(candidate)
            .ok_or(SessionError::Notification(candidate))?;
        self.response_ack_receivers.insert(candidate, ack_receiver);
        command.notify_id = Some(candidate);
        let queue_id = self.retry_transactions.get(&transaction_id)
            .ok_or(SessionError::Notification(candidate))?.queue_id;
        if let Err(error) = self.scheduler.enqueue(queue_id, command) {
            self.ack_tracker.cancel_notify(candidate);
            self.response_ack_receivers.remove(&candidate);
            return Err(SessionError::Schedule(error));
        }
        self.response_requests.insert(candidate, transaction_id);
        self.retry_attempts.insert(candidate, transaction_id);
        Ok(())
    }

    fn route_decoded_message(
        &mut self,
        name: String,
        params: BTreeMap<String, Value>,
        sent_time: f64,
        receive_time: f64,
        events: &mut Vec<SessionEvent>,
    ) {
        if name == "#output" {
            let message = params.get("#msg").and_then(Value::as_str).unwrap_or("").to_owned();
            events.push(SessionEvent::Output { message, receive_time });
            return;
        }
        let oid = params.get("oid").and_then(|value| {
            value.as_u64().and_then(|v| u32::try_from(v).ok())
                .or_else(|| value.as_i64().and_then(|v| u32::try_from(v).ok()))
        });
        let pending_index = self.sent_response_requests.iter()
            .position(|pending| pending.name == name && pending.oid == oid);
        let matched = if let Some(index) = pending_index {
            let pending = self.sent_response_requests.remove(index)
                .expect("located response request must exist");
            let response = Response {
                name: name.clone(), oid, params: params.clone(),
                sent_time, receive_time,
            };
            let waiter_id = self.retry_transactions.get(&pending.transaction_id)
                .map(|transaction| transaction.waiter_id);
            let delivered = waiter_id.is_some_and(|waiter_id| {
                self.response_router.dispatch_to_waiter(response, waiter_id)
            });
            if delivered {
                if let Some(mut transaction) = self.retry_transactions.remove(&pending.transaction_id) {
                    transaction.transaction.response_received();
                    self.cancel_retry_attempts(pending.transaction_id);
                }
            }
            delivered
        } else { false };
        events.push(SessionEvent::DecodedMessage { name: name.clone(), params, receive_time });
        events.push(SessionEvent::ResponseRouted { name, oid, matched });
    }

    /// Retire all per-attempt scheduler and ACK state after a logical
    /// response transaction completes or its waiter is abandoned. Commands
    /// already written cannot be recalled, but queued attempts must not leak
    /// into a later poll and stale ACKs must not schedule further retries.
    fn cancel_retry_attempts(&mut self, transaction_id: u64) {
        let notify_ids: Vec<u64> = self.response_requests.iter()
            .filter_map(|(notify_id, id)| (*id == transaction_id).then_some(*notify_id))
            .chain(self.retry_attempts.iter()
                .filter_map(|(notify_id, id)| (*id == transaction_id).then_some(*notify_id)))
            .collect();

        for notify_id in notify_ids {
            self.scheduler.cancel_notify(notify_id);
            self.ack_tracker.cancel_notify(notify_id);
            self.response_ack_receivers.remove(&notify_id);
            self.response_requests.remove(&notify_id);
            self.retry_attempts.remove(&notify_id);
        }
    }

    fn prune_abandoned_response_requests(&mut self) {
        // Keep response_requests until the command is actually written. A
        // waiter may be dropped while its command is still queued; deleting
        // this entry early would orphan its reserved ACK notification and
        // prevent the response request from being associated with its eventual
        // response. Once sent, the entry moves to
        // sent_response_requests and can be pruned normally.
        let router = self.response_router.clone();
        let abandoned: Vec<u64> = self.retry_transactions.iter()
            .filter_map(|(id, pending)| {
                let waiter_is_live = router.contains_waiter(
                    &pending.name, pending.oid, pending.waiter_id,
                );
                (!waiter_is_live).then_some(*id)
            }).collect();
        for id in abandoned {
            self.retry_transactions.remove(&id);
            self.sent_response_requests.retain(|pending| pending.transaction_id != id);

            // Cancel queued attempts and retire their ACK bookkeeping.
            self.cancel_retry_attempts(id);
        }
        self.sent_response_requests.retain(|pending| {
            self.retry_transactions.contains_key(&pending.transaction_id)
        });
    }

    pub fn scheduler(&self) -> &CommandScheduler { &self.scheduler }
    pub fn scheduler_mut(&mut self) -> &mut CommandScheduler { &mut self.scheduler }

    pub fn enqueue(
        &mut self, queue_id: u64, command: ScheduledCommand,
    ) -> Result<(), SessionError> {
        self.ensure_connected()?;
        if let Some(id) = command.notify_id {
            if !self.ack_tracker.contains_notify(id) {
                return Err(SessionError::Notification(id));
            }
        }
        self.scheduler.enqueue(queue_id, command)?;
        Ok(())
    }

    /// Release commands whose minimum MCU clock is now eligible.
    ///
    /// `ack_clock` must be an extended MCU clock (not the four-bit wire value).
    /// The session retains the greatest observed value so commands queued later,
    /// including application-level retries, are promoted against the same clock
    /// high-water mark without requiring a duplicate clock update.
    pub fn promote_ready(&mut self, ack_clock: u64) -> Result<Option<u64>, SessionError> {
        self.ensure_connected()?;
        self.last_ack_clock = advance_ack_clock(self.last_ack_clock, ack_clock);
        Ok(self.scheduler.promote_ready(self.last_ack_clock))
    }

    /// Apply a ClockSync update to the scheduler using the conservative queue
    /// clock model. host_time must use the same monotonic time base supplied to
    /// ClockSync::update(). Rejected outliers are intentionally ignored, matching
    /// Klipper's ClockSync._handle_clock() behavior.
    pub fn apply_clock_update(
        &mut self,
        update: ClockUpdate,
        host_time: f64,
    ) -> Result<Option<u64>, SessionError> {
        self.ensure_connected()?;
        if update.rejected_outlier {
            return Ok(None);
        }
        let queue_clock = update.queue_clock_at(host_time)?;
        self.promote_ready(queue_clock)
    }

    /// Apply a clock update at the session's current monotonic time.
    ///
    /// Prefer this over `apply_clock_update` when the update was produced from
    /// this session's timestamps. It prevents accidentally mixing the session
    /// clock with process-wide or system-wide monotonic timestamps.
    pub fn apply_clock_update_now(
        &mut self,
        update: ClockUpdate,
    ) -> Result<Option<u64>, SessionError> {
        self.apply_clock_update(update, self.now())
    }

    fn write_retry_batch(
        &mut self,
        retries: Vec<crate::Retransmission>,
        events: &mut Vec<SessionEvent>,
    ) -> Result<(), SessionError> {
        if retries.is_empty() {
            return Ok(());
        }
        if let Err(error) = self.transport.write_retransmission_batch(&retries) {
            self.disconnected = true;
            return Err(SessionError::Transport(error));
        }
        let sent_at = self.now();
        for retry in retries {
            if let Err(error) = self.scheduler.retransmit_state_mut()
                .mark_retransmission_sent(retry.sequence, sent_at)
            {
                self.disconnected = true;
                return Err(SessionError::Schedule(ScheduleError::Retransmit(error)));
            }
            events.push(SessionEvent::Retransmitted {
                sequence: retry.sequence,
                bytes: retry.bytes.len(),
            });
        }
        Ok(())
    }

    /// Perform one session iteration, bounded by the configured serial read timeout.
    ///
    /// Reads all available frames, processes ACK/NAK, writes required retries,
    /// then writes newly eligible scheduled frames. Serial read timeout is idle.
    pub fn poll(&mut self) -> Result<Vec<SessionEvent>, SessionError> {
        self.ensure_connected()?;
        // Drop response metadata when its waiter timed out or was cancelled.
        self.prune_abandoned_response_requests();
        let incoming = match self.transport.read_frames() {
            Ok(frames) => frames,
            Err(error) => {
                self.disconnected = true;
                return Err(SessionError::Transport(error));
            }
        };
        let mut events = Vec::new();

        for frame in incoming {
            // C serialqueue passes the input-event timestamp to ACK notifications,
            // but timestamps data messages after read() with the frame's wire time removed.
            let eventtime = self.now();
            let result = match self.scheduler.handle_received_frame(&frame, eventtime) {
                Ok(result) => result,
                Err(error) => {
                    self.disconnected = true;
                    return Err(SessionError::Schedule(error));
                }
            };
            let receive_time = adjusted_receive_time(
                self.now(), self.wire_time_per_byte, frame.payload.len() + MESSAGE_MIN,
            );
            // Defer retry exhaustion until this frame's payload has been
            // routed. An MCU may acknowledge the final attempt and return its
            // response in the same frame; the response must win that race.
            let mut exhausted_transactions = Vec::new();
            for (id, ack) in self.ack_tracker.acknowledge_events(
                result.receive_sequence, result.has_payload, result.last_receive_sent_time, eventtime,
            ) {
                if let Some(receiver) = self.response_ack_receivers.remove(&id) {
                    let _ = receiver.try_recv();
                }
                if let Some(transaction_id) = self.retry_attempts.remove(&id) {
                    let exhausted = self.retry_transactions.get_mut(&transaction_id)
                        .and_then(|pending| match pending.transaction.attempt_acked(eventtime) {
                            AckRetryDecision::Exhausted => Some((pending.name.clone(), pending.oid, pending.waiter_id)),
                            AckRetryDecision::RetryAfter(_) | AckRetryDecision::Complete => None,
                        });
                    if let Some((name, oid, waiter_id)) = exhausted {
                        exhausted_transactions.push((transaction_id, name, oid, waiter_id));
                    }
                }
                events.push(SessionEvent::AckedNotify {
                    id, sent_time: ack.sent_time, receive_time: ack.receive_time,
                });
            }

            self.write_retry_batch(result.retransmissions.clone(), &mut events)?;

            if !frame.payload.is_empty() {
                if let Some(dictionary) = self.dictionary.as_ref() {
                    match dictionary.decode_messages(&frame.payload) {
                        Ok(messages) => {
                            for (name, params) in messages {
                                self.route_decoded_message(name, params, result.response_sent_time, receive_time, &mut events);
                            }
                        }
                        Err(error) => events.push(SessionEvent::DecodeError {
                            error: error.to_string(), receive_time,
                        }),
                    }
                } else {
                    match klipper_protocol::decode_identify_response(&frame.payload) {
                        Ok(chunk) => {
                            events.push(SessionEvent::IdentifyChunk {
                                chunk: chunk.clone(), receive_time,
                            });
                            match self.accept_identify_chunk(chunk) {
                                Ok(progress) => events.push(SessionEvent::IdentifyProgress { progress }),
                                Err(error) => events.push(SessionEvent::DecodeError {
                                    error: error.to_string(), receive_time,
                                }),
                            }
                        },
                        Err(error) => events.push(SessionEvent::DecodeError {
                            error: error.to_string(), receive_time,
                        }),
                    }
                }
            }

            // Only report exhaustion if routing this frame did not complete
            // the transaction. In particular, a response carried alongside
            // the final ACK removes the transaction in route_decoded_message().
            let retired = finalize_exhausted_transactions(
                exhausted_transactions,
                &mut self.retry_transactions,
                &mut self.sent_response_requests,
                &self.response_router,
                &mut events,
            );
            for transaction_id in retired {
                self.cancel_retry_attempts(transaction_id);
            }
            events.push(SessionEvent::Received { frame, result });
        }

        // Consume the internally-owned periodic clock waiter after routing all
        // responses from this read batch. Keeping the waiter in the session is
        // essential: dropping it would unregister the response and cancel retry
        // bookkeeping before the MCU can answer.
        if let Some(waiter) = self.periodic_clock_waiter.take() {
            match waiter.try_recv() {
                Ok(Some(response)) => {
                    self.handle_periodic_clock_response(response)?;
                }
                Ok(None) => {
                    self.periodic_clock_waiter = Some(waiter);
                }
                Err(_) => {
                    // A disconnected waiter normally means the response
                    // transaction exhausted its retries and was retired.
                    // poll() already emitted RetryExhausted for that request;
                    // allow the next scheduled clock tick to recover instead
                    // of turning a missed sample into a terminal session error.
                    self.periodic_clock_in_flight = false;
                }
            }
        }

        let now = self.now();
        // Application-level retries are distinct from frame retransmissions:
        // after an ACK, wait 10/20/40/80/160ms, then enqueue a fresh frame.
        let due: Vec<u64> = self.retry_transactions.iter_mut()
            .filter_map(|(id, pending)| pending.transaction.take_due_retry(now).then_some(*id))
            .collect();
        for transaction_id in due {
            let command = match self.retry_transactions.get(&transaction_id) {
                Some(pending) => pending.command.clone(),
                None => continue,
            };
            // Preserve the original min_clock and req_clock on every retry,
            // matching SerialRetryCommand's repeated raw_send arguments.
            self.enqueue_retry_attempt(transaction_id, command)?;
            self.scheduler.promote_ready(self.last_ack_clock);
        }

        let timeout_retries = match self.scheduler.poll_retransmissions(now) {
            Ok(retries) => retries,
            Err(error) => {
                self.disconnected = true;
                return Err(SessionError::Schedule(error));
            }
        };
        self.write_retry_batch(timeout_retries, &mut events)?;

        // Once startup calibration has completed, drive Klipper's periodic
        // get_clock timer from the session poll loop. The waiter remains owned
        // here until route_decoded_message delivers its response.
        if self.periodic_clock_waiter.is_none() {
            if let Some(queue_id) = self.periodic_clock_queue_id {
                if let Some(waiter) = self.poll_periodic_clock(queue_id)? {
                    self.periodic_clock_waiter = Some(waiter);
                }
            }
        }

        loop {
            let frame = match self.scheduler.build_frame() {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(error) => {
                    self.disconnected = true;
                    return Err(SessionError::Schedule(error));
                }
            };
            if let Err(error) = self.transport.write_scheduled_frame(&frame) {
                self.disconnected = true;
                return Err(SessionError::Transport(error));
            }
            let sent_at = self.now();
            if let Err(error) = self.scheduler.mark_frame_sent(&frame, sent_at) {
                self.disconnected = true;
                return Err(SessionError::Schedule(error));
            }
            for id in &frame.notify_ids {
                if !self.ack_tracker.mark_sent(*id, frame.send_sequence, sent_at) {
                    self.disconnected = true;
                    return Err(SessionError::Notification(*id));
                }
                if let Some(transaction_id) = self.response_requests.remove(id) {
                    if let Some(pending) = self.retry_transactions.get(&transaction_id) {
                        if !self.sent_response_requests.iter()
                            .any(|entry| entry.transaction_id == transaction_id)
                        {
                            self.sent_response_requests.push_back(PendingResponseSend {
                                transaction_id,
                                name: pending.name.clone(),
                                oid: pending.oid,
                            });
                        }
                    }
                }
            }
            events.push(SessionEvent::Sent {
                sequence: frame.send_sequence,
                bytes: frame.bytes.len(),
            });
        }

        Ok(events)
    }

    pub fn is_disconnected(&self) -> bool { self.disconnected }
}

/// Match serialqueue.c's data-message receive timestamp: post-read monotonic
/// time minus the UART time occupied by the complete encoded frame.
fn adjusted_receive_time(now: f64, wire_time_per_byte: f64, frame_len: usize) -> f64 {
    (now - wire_time_per_byte * frame_len as f64).max(0.0)
}

#[cfg(test)]
mod exhausted_transaction_tests {
    use super::*;

    fn pending(waiter_id: u64) -> PendingRetryRequest {
        PendingRetryRequest {
            queue_id: 7,
            command: ScheduledCommand::new(vec![0x01], 0, 0),
            name: "status".into(),
            oid: Some(3),
            waiter_id,
            transaction: RetryTransaction::default(),
        }
    }

    #[test]
    fn same_frame_response_completion_suppresses_retry_exhausted_event() {
        let router = ResponseRouter::new();
        let waiter = router.register("status", Some(3));
        let waiter_id = router.claim_oldest("status", Some(3)).unwrap();
        let mut transactions = HashMap::new();
        transactions.insert(9, pending(waiter_id));
        let mut sent = VecDeque::from([PendingResponseSend {
            transaction_id: 9, name: "status".into(), oid: Some(3),
        }]);
        let mut events = Vec::new();

        // Simulate successful response routing: the exact waiter and sent
        // response marker are consumed before deferred exhaustion is finalized.
        assert!(router.dispatch_to_waiter(Response {
            name: "status".into(), oid: Some(3), params: BTreeMap::new(),
            sent_time: 1.0, receive_time: 2.0,
        }, waiter_id));
        transactions.remove(&9);
        sent.retain(|entry| entry.transaction_id != 9);
        let retired = finalize_exhausted_transactions(
            vec![(9, "status".into(), Some(3), waiter_id)],
            &mut transactions, &mut sent, &router, &mut events,
        );

        assert!(retired.is_empty());
        assert!(events.is_empty());
        assert!(sent.is_empty());
        assert_eq!(router.pending_count("status", Some(3)), 0);
        drop(waiter);
    }

    #[test]
    fn genuinely_exhausted_request_is_removed_and_reported_once() {
        let router = ResponseRouter::new();
        let waiter = router.register("status", Some(3));
        let waiter_id = router.claim_oldest("status", Some(3)).unwrap();
        let mut transactions = HashMap::from([(9, pending(waiter_id))]);
        let mut sent = VecDeque::from([PendingResponseSend {
            transaction_id: 9, name: "status".into(), oid: Some(3),
        }]);
        let mut events = Vec::new();

        let retired = finalize_exhausted_transactions(
            vec![(9, "status".into(), Some(3), waiter_id)],
            &mut transactions, &mut sent, &router, &mut events,
        );

        assert_eq!(retired, vec![9]);
        assert!(!transactions.contains_key(&9));
        assert!(sent.is_empty());
        assert_eq!(router.pending_count("status", Some(3)), 0);
        assert!(matches!(events.as_slice(), [SessionEvent::RetryExhausted { name, oid: Some(3) }] if name == "status"));
        drop(waiter);
    }
}

#[cfg(test)]
mod retry_transaction_id_tests {
    use super::*;

    fn pending() -> PendingRetryRequest {
        PendingRetryRequest {
            queue_id: 7,
            command: ScheduledCommand::new(vec![0x01], 0, 0),
            name: "status".into(),
            oid: Some(3),
            waiter_id: 11,
            transaction: RetryTransaction::default(),
        }
    }

    #[test]
    fn allocator_skips_an_in_flight_id_at_wraparound() {
        let mut in_flight = HashMap::new();
        in_flight.insert(1, pending());
        let mut next_id = u64::MAX;

        assert_eq!(allocate_retry_transaction_id(&mut next_id, &in_flight), u64::MAX);
        assert_eq!(allocate_retry_transaction_id(&mut next_id, &in_flight), 2);
        assert!(in_flight.contains_key(&1));
    }

    #[test]
    fn allocator_skips_zero_and_currently_occupied_candidate() {
        let mut in_flight = HashMap::new();
        in_flight.insert(1, pending());
        let mut next_id = 1;

        assert_eq!(allocate_retry_transaction_id(&mut next_id, &in_flight), 2);
    }
}

#[cfg(test)]
mod receive_timestamp_tests {
    use super::adjusted_receive_time;

    #[test]
    fn receive_timestamp_subtracts_complete_frame_wire_time() {
        assert!((adjusted_receive_time(2.0, 0.001, 10) - 1.99).abs() < 1e-9);
    }

    #[test]
    fn receive_timestamp_never_precedes_session_start() {
        assert_eq!(adjusted_receive_time(0.001, 0.001, 10), 0.0);
    }
}

#[cfg(test)]
mod config_handshake_session_tests {
    use super::*;

    fn response(name: &str, oid: Option<u32>) -> Response {
        Response {
            name: name.to_owned(),
            oid,
            params: BTreeMap::from([
                ("is_config".into(), Value::from(1)),
                ("crc".into(), Value::from(0x1234_u64)),
                ("is_shutdown".into(), Value::from(0)),
                ("move_count".into(), Value::from(64)),
            ]),
            sent_time: 1.0,
            receive_time: 2.0,
        }
    }

    #[test]
    fn parses_unscoped_config_response_for_handshake() {
        let parsed = ReliableSerialSession::parse_config_response(response("config", None)).unwrap();
        assert_eq!(parsed, ConfigResponse {
            is_config: true,
            crc: 0x1234,
            is_shutdown: false,
            move_count: 64,
        });
    }

    #[test]
    fn rejects_wrong_response_name_or_oid_for_config_handshake() {
        assert!(matches!(
            ReliableSerialSession::parse_config_response(response("identify_response", None)),
            Err(SessionError::UnexpectedConfigResponse { .. })
        ));
        assert!(matches!(
            ReliableSerialSession::parse_config_response(response("config", Some(3))),
            Err(SessionError::UnexpectedConfigResponse { .. })
        ));
    }
}


#[cfg(test)]
mod periodic_clock_deadline_tests {
    use super::{next_periodic_clock_deadline, PERIODIC_CLOCK_INTERVAL_SECONDS};

    #[test]
    fn next_tick_is_anchored_to_previous_deadline() {
        let due_at = 10.0;
        let next = next_periodic_clock_deadline(due_at, due_at);
        assert!((next - (due_at + PERIODIC_CLOCK_INTERVAL_SECONDS)).abs() < 1e-9);
    }

    #[test]
    fn delayed_poll_skips_missed_ticks_without_catch_up_burst() {
        let due_at = 10.0;
        let now = due_at + PERIODIC_CLOCK_INTERVAL_SECONDS * 3.4;
        let next = next_periodic_clock_deadline(due_at, now);
        assert!(next > now);
        assert!((next - (due_at + PERIODIC_CLOCK_INTERVAL_SECONDS * 4.0)).abs() < 1e-9);
    }

    #[test]
    fn exact_tick_boundary_advances_to_a_future_deadline() {
        let due_at = 10.0;
        let now = due_at + PERIODIC_CLOCK_INTERVAL_SECONDS;
        let next = next_periodic_clock_deadline(due_at, now);
        assert!(next > now);
        assert!((next - (due_at + PERIODIC_CLOCK_INTERVAL_SECONDS * 2.0)).abs() < 1e-9);
    }
}

#[cfg(test)]
mod ack_clock_promotion_tests {
    use super::advance_ack_clock;

    #[test]
    fn ack_clock_advances_monotonically() {
        assert_eq!(advance_ack_clock(0, 120), 120);
        assert_eq!(advance_ack_clock(120, 150), 150);
        // A stale caller update must not move the clock behind a retry's
        // already-satisfied min_clock.
        assert_eq!(advance_ack_clock(150, 90), 150);
    }
}
