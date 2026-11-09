//! Reliable USB serial session: frame I/O, scheduling, ACK/NAK and retries.
use std::time::Instant;

use klipper_protocol::Frame;

use crate::{
    CommandScheduler, ScheduleError, ScheduledCommand, TransportError, UsbSerialTransport,
};

#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error(transparent)]
    Transport(#[from] TransportError),
    #[error(transparent)]
    Schedule(#[from] ScheduleError),
    #[error("serial session is no longer usable after an I/O failure")]
    Disconnected,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SessionEvent {
    Received {
        frame: Frame,
        result: crate::ReceivedFrameResult,
    },
    Sent {
        sequence: u64,
        bytes: usize,
    },
    Retransmitted {
        sequence: u64,
        bytes: usize,
    },
}

/// Owns one USB serial link and its reliable MCU command scheduler.
///
/// A transport or protocol error is terminal for this session. Re-open the
/// serial device and perform MCU identify/configuration again before reuse.
pub struct ReliableSerialSession {
    transport: UsbSerialTransport,
    scheduler: CommandScheduler,
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

    pub fn scheduler(&self) -> &CommandScheduler { &self.scheduler }
    pub fn scheduler_mut(&mut self) -> &mut CommandScheduler { &mut self.scheduler }

    pub fn enqueue(
        &mut self, queue_id: u64, command: ScheduledCommand,
    ) -> Result<(), SessionError> {
        self.ensure_connected()?;
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
            events.push(SessionEvent::Sent {
                sequence: frame.send_sequence,
                bytes: frame.bytes.len(),
            });
        }

        Ok(events)
    }

    pub fn is_disconnected(&self) -> bool { self.disconnected }
}
