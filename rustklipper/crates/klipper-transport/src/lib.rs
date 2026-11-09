//! Cross-platform USB serial transport and MCU transaction primitives.
pub mod ack;
pub mod response;
pub mod retry;
pub mod retransmit;
pub mod scheduler;
pub mod clock;
pub mod config;
pub mod pin;
pub mod session;
pub use session::{IdentifyProgress, McuStartupStage, ReliableSerialSession, SessionError, SessionEvent};

pub use ack::{AckEvent, AckTracker};
pub use response::{Response, ResponseRouter, ResponseWaiter, WaitError};
pub use retry::{AckRetryDecision, RetryPolicy, RetryTransaction};
pub use retransmit::{AckResult, Retransmission, RetransmitError, RetransmitState, TrackedFrame, DEFAULT_RTO_SECONDS, DUP_ACK_THRESHOLD, MAX_RTO_SECONDS, MIN_RTO_SECONDS};
pub use scheduler::{CommandScheduler, ReceivedFrameResult, ScheduleError, ScheduledCommand, ScheduledFrame, BACKGROUND_PRIORITY_CLOCK};
pub use clock::{ClockEstimate, ClockSync, ClockSyncError, ClockUpdate, UartTiming};
pub use config::{ConfigAction, ConfigError, ConfigHandshakeStage, ConfigResponse, McuConfigHandshake, McuConfigPlan, OidAllocator};
pub use pin::{PinResolver, PinResolverError};


use std::io::{Read, Write};
use std::time::Duration;

use klipper_protocol::{encode_frame, Frame, FrameDecoder, ProtocolError, MESSAGE_SYNC};
use serialport::{ClearBuffer, SerialPort};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("serial port error: {0}")]
    Serial(#[from] serialport::Error),
    #[error("serial I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error("serial read returned no data before timeout")]
    Timeout,
}

#[derive(Debug, Clone)]
pub struct SerialSettings {
    pub path: String,
    pub baud_rate: u32,
    pub timeout: Duration,
}

impl SerialSettings {
    pub fn new(path: impl Into<String>, baud_rate: u32) -> Self {
        Self {
            path: path.into(),
            baud_rate,
            timeout: Duration::from_millis(20),
        }
    }
}

/// A single USB serial connection to one MCU.
#[cfg(test)]
mod retransmission_batch_tests {
    use super::*;

    #[test]
    fn retransmission_batch_has_one_leading_sync_and_preserves_frames() {
        let retries = [
            Retransmission { sequence: 3, bytes: vec![0x08, 0x13, 0x7e], transmissions: 2, timeout_seconds: 0.5 },
            Retransmission { sequence: 4, bytes: vec![0x09, 0x14, 0x7e], transmissions: 2, timeout_seconds: 0.5 },
        ];
        assert_eq!(
            encode_retransmission_batch(&retries),
            vec![MESSAGE_SYNC, 0x08, 0x13, 0x7e, 0x09, 0x14, 0x7e]
        );
        assert!(encode_retransmission_batch(&[]).is_empty());
    }
}

fn encode_retransmission_batch(retries: &[Retransmission]) -> Vec<u8> {
    if retries.is_empty() {
        return Vec::new();
    }
    let capacity = 1 + retries.iter().map(|retry| retry.bytes.len()).sum::<usize>();
    let mut batch = Vec::with_capacity(capacity);
    batch.push(MESSAGE_SYNC);
    for retry in retries {
        batch.extend_from_slice(&retry.bytes);
    }
    batch
}

pub struct UsbSerialTransport {
    port: Box<dyn SerialPort>,
    decoder: FrameDecoder,
    read_buffer: [u8; 256],
}

impl UsbSerialTransport {
    pub fn open(settings: &SerialSettings) -> Result<Self, TransportError> {
        let port = serialport::new(&settings.path, settings.baud_rate)
            .timeout(settings.timeout)
            .open()?;
        Ok(Self {
            port,
            decoder: FrameDecoder::new(),
            read_buffer: [0; 256],
        })
    }

    /// Read from the byte stream and return all complete valid frames.
    ///
    /// A timeout is not a disconnect: it simply returns an empty frame list.
    pub fn read_frames(&mut self) -> Result<Vec<Frame>, TransportError> {
        match self.port.read(&mut self.read_buffer) {
            Ok(0) => Ok(Vec::new()),
            Ok(count) => Ok(self.decoder.push(&self.read_buffer[..count])),
            Err(error) if error.kind() == std::io::ErrorKind::TimedOut => Ok(Vec::new()),
            Err(error) => Err(TransportError::Io(error)),
        }
    }

    /// Write an already encoded MCU frame without changing its sequence or checksum.
    /// This is the required path for retransmission: callers pass the exact cached
    /// bytes returned by ScheduledFrame or Retransmission.
    pub fn write_encoded_frame(&mut self, encoded: &[u8]) -> Result<(), TransportError> {
        // Do not call Write::flush here: serialport's POSIX implementation
        // maps it to tcdrain(), which blocks until the UART has transmitted
        // every byte. Klipper's serialqueue writes into the OS queue and
        // tracks the remaining wire time asynchronously.
        self.port.write_all(encoded)?;
        Ok(())
    }

    /// Send the exact encoded frame produced by CommandScheduler.
    pub fn write_scheduled_frame(
        &mut self,
        frame: &scheduler::ScheduledFrame,
    ) -> Result<(), TransportError> {
        self.write_encoded_frame(&frame.bytes)
    }

    /// Retransmit a complete outstanding window. Klipper prefixes the
    /// concatenated cached frames with one sync byte so the MCU can recover
    /// framing after a partial/corrupt prior transmission.
    pub fn write_retransmission_batch(
        &mut self,
        retries: &[Retransmission],
    ) -> Result<(), TransportError> {
        let batch = encode_retransmission_batch(retries);
        if batch.is_empty() {
            return Ok(());
        }
        // Match serialqueue.c's TCOFLUSH before replaying the outstanding
        // window; otherwise bytes from a stalled/partial UART write may be
        // transmitted immediately before the sync-prefixed retry batch.
        self.clear_output()?;
        self.write_encoded_frame(&batch)
    }

    /// Retransmit one cached frame with the same leading sync used by Klipper.
    pub fn write_retransmission(
        &mut self,
        retry: &Retransmission,
    ) -> Result<(), TransportError> {
        self.write_retransmission_batch(std::slice::from_ref(retry))
    }

    /// Encode and write one MCU frame. Scheduling and ACK tracking are higher layers.
    pub fn write_frame(&mut self, sequence: u8, payload: &[u8]) -> Result<(), TransportError> {
        let frame = encode_frame(sequence, payload)?;
        self.write_encoded_frame(&frame)
    }

    pub fn clear_input(&mut self) -> Result<(), TransportError> {
        self.port.clear(ClearBuffer::Input)?;
        self.decoder.clear();
        Ok(())
    }

    pub fn clear_output(&mut self) -> Result<(), TransportError> {
        self.port.clear(ClearBuffer::Output)?;
        Ok(())
    }

    pub fn set_timeout(&mut self, timeout: Duration) -> Result<(), TransportError> {
        self.port.set_timeout(timeout)?;
        Ok(())
    }

    pub fn bytes_to_read(&self) -> Result<u32, TransportError> {
        Ok(self.port.bytes_to_read()?)
    }

    pub fn bytes_to_write(&self) -> Result<u32, TransportError> {
        Ok(self.port.bytes_to_write()?)
    }

    pub fn close(self) {
        drop(self);
    }
}
