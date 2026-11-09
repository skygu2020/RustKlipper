//! Reliable USB serial session: frame I/O, scheduling, ACK/NAK and retries.
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;

use serde_json::Value;
use klipper_protocol::{Frame, IdentifyChunk, IdentifyDictionary};

use crate::{
    AckEvent, AckTracker, CommandScheduler, Response, ResponseRouter, ResponseWaiter, ScheduleError, ScheduledCommand, TransportError, UsbSerialTransport,
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
}

#[derive(Debug, Clone, PartialEq)]
pub enum SessionEvent {
    Received {
        frame: Frame,
        result: crate::ReceivedFrameResult,
    },
    AckedNotify { id: u64, sent_time: f64, receive_time: f64 },
    ResponseRouted { name: String, oid: Option<u32>, matched: bool },
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
    DecodedMessage { name: String, params: BTreeMap<String, Value>, receive_time: f64 },
    DecodeError { error: String, receive_time: f64 },
}

/// Owns one USB serial link and its reliable MCU command scheduler.
///
/// A transport or protocol error is terminal for this session. Re-open the
/// serial device and perform MCU identify/configuration again before reuse.
#[derive(Debug, Clone)]
struct PendingResponseSend { name: String, oid: Option<u32>, sent_time: f64 }

pub struct ReliableSerialSession {
    transport: UsbSerialTransport,
    scheduler: CommandScheduler,
    ack_tracker: AckTracker,
    response_router: ResponseRouter,
    response_requests: HashMap<u64, (String, Option<u32>)>,
    response_ack_receivers: HashMap<u64, std::sync::mpsc::Receiver<AckEvent>>,
    sent_response_requests: VecDeque<PendingResponseSend>,
    next_response_notify_id: u64,
    dictionary: Option<IdentifyDictionary>,
    started: Instant,
    disconnected: bool,
}

impl ReliableSerialSession {
    pub fn open(settings: &crate::SerialSettings) -> Result<Self, SessionError> {
        Ok(Self::new(UsbSerialTransport::open(settings)?))
    }

    pub fn new(transport: UsbSerialTransport) -> Self {
        Self {
            transport,
            scheduler: CommandScheduler::new(),
            ack_tracker: AckTracker::new(),
            response_router: ResponseRouter::new(),
            response_requests: HashMap::new(),
            response_ack_receivers: HashMap::new(),
            sent_response_requests: VecDeque::new(),
            next_response_notify_id: u64::MAX,
            dictionary: None,
            started: Instant::now(),
            disconnected: false,
        }
    }

    fn now(&self) -> f64 {
        self.started.elapsed().as_secs_f64()
    }

    fn ensure_connected(&self) -> Result<(), SessionError> {
        if self.disconnected { Err(SessionError::Disconnected) } else { Ok(()) }
    }

    /// Install the MCU-provided dictionary after identify has completed.
    pub fn set_dictionary(&mut self, dictionary: IdentifyDictionary) {
        self.dictionary = Some(dictionary);
    }

    pub fn has_dictionary(&self) -> bool { self.dictionary.is_some() }

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

    /// Enqueue a command and associate its eventual response with the command's
    /// actual successful UART-write timestamp. The command must not already carry
    /// a caller-managed notify ID.
    pub fn enqueue_request(
        &mut self,
        queue_id: u64,
        mut command: ScheduledCommand,
        response_name: impl Into<String>,
        oid: Option<u32>,
    ) -> Result<(), SessionError> {
        self.ensure_connected()?;
        if command.notify_id.is_some() {
            return Err(SessionError::CommandAlreadyNotified);
        }
        let name = response_name.into();
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
        // Reserve the ID in AckTracker too, preventing collisions with public
        // notifications and ensuring it is bound only after a successful write.
        let ack_receiver = self.ack_tracker.register_notify(candidate)
            .ok_or(SessionError::Notification(candidate))?;
        self.response_ack_receivers.insert(candidate, ack_receiver);
        command.notify_id = Some(candidate);
        if let Err(error) = self.scheduler.enqueue(queue_id, command) {
            self.ack_tracker.cancel_notify(candidate);
            self.response_ack_receivers.remove(&candidate);
            return Err(SessionError::Schedule(error));
        }
        self.response_requests.insert(candidate, (name, oid));
        Ok(())
    }

    fn route_decoded_message(
        &mut self,
        name: String,
        params: BTreeMap<String, Value>,
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
            self.response_router.dispatch(Response {
                name: name.clone(), oid, params: params.clone(),
                sent_time: pending.sent_time, receive_time,
            })
        } else { false };
        events.push(SessionEvent::DecodedMessage { name: name.clone(), params, receive_time });
        events.push(SessionEvent::ResponseRouted { name, oid, matched });
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
    pub fn promote_ready(&mut self, ack_clock: u64) -> Result<Option<u64>, SessionError> {
        self.ensure_connected()?;
        Ok(self.scheduler.promote_ready(ack_clock))
    }

    fn write_retry_batch(
        &mut self,
        retries: Vec<crate::Retransmission>,
        events: &mut Vec<SessionEvent>,
    ) -> Result<(), SessionError> {
        for retry in retries {
            if let Err(error) = self.transport.write_retransmission(&retry) {
                self.disconnected = true;
                return Err(SessionError::Transport(error));
            }
            let sent_at = self.now();
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
        let incoming = match self.transport.read_frames() {
            Ok(frames) => frames,
            Err(error) => {
                self.disconnected = true;
                return Err(SessionError::Transport(error));
            }
        };
        let mut events = Vec::new();

        for frame in incoming {
            let now = self.now();
            let result = match self.scheduler.handle_received_frame(&frame, now) {
                Ok(result) => result,
                Err(error) => {
                    self.disconnected = true;
                    return Err(SessionError::Schedule(error));
                }
            };
            self.write_retry_batch(result.retransmissions.clone(), &mut events)?;

            let receive_time = self.now();
            for (id, ack) in self.ack_tracker.acknowledge_events(
                result.receive_sequence, result.has_payload, 0.0, receive_time,
            ) {
                if let Some(receiver) = self.response_ack_receivers.remove(&id) {
                    // Consume the same event through the waiter channel as well;
                    // retaining the receiver prevents a detached internal notify.
                    let _ = receiver.try_recv();
                }
                events.push(SessionEvent::AckedNotify {
                    id, sent_time: ack.sent_time, receive_time: ack.receive_time,
                });
            }

            if !frame.payload.is_empty() {
                let receive_time = self.now();
                if let Some(dictionary) = self.dictionary.as_ref() {
                    match dictionary.decode_messages(&frame.payload) {
                        Ok(messages) => {
                            for (name, params) in messages {
                                self.route_decoded_message(name, params, receive_time, &mut events);
                            }
                        }
                        Err(error) => events.push(SessionEvent::DecodeError {
                            error: error.to_string(), receive_time,
                        }),
                    }
                } else {
                    match klipper_protocol::decode_identify_response(&frame.payload) {
                        Ok(chunk) => events.push(SessionEvent::IdentifyChunk {
                            chunk, receive_time,
                        }),
                        Err(error) => events.push(SessionEvent::DecodeError {
                            error: error.to_string(), receive_time,
                        }),
                    }
                }
            }
            events.push(SessionEvent::Received { frame, result });
        }

        let now = self.now();
        let timeout_retries = match self.scheduler.poll_retransmissions(now) {
            Ok(retries) => retries,
            Err(error) => {
                self.disconnected = true;
                return Err(SessionError::Schedule(error));
            }
        };
        self.write_retry_batch(timeout_retries, &mut events)?;

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
                if let Some((name, oid)) = self.response_requests.remove(id) {
                    self.sent_response_requests.push_back(PendingResponseSend {
                        name, oid, sent_time: sent_at,
                    });
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
