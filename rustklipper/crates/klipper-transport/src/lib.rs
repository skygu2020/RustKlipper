//! Cross-platform USB serial transport primitives.
//!
//! This first transport layer deliberately provides synchronous, timeout-bounded
//! I/O and incremental MCU frame extraction. Reactor integration, command
//! scheduling, ACK notifications, retries, and reconnect policy are separate
//! layers and are not implied by this crate yet.

use std::io::{Read, Write};
use std::time::Duration;

use klipper_protocol::{encode_frame, Frame, FrameDecoder, ProtocolError};
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

    /// Encode and write one MCU frame. Scheduling and ACK tracking are higher layers.
    pub fn write_frame(&mut self, sequence: u8, payload: &[u8]) -> Result<(), TransportError> {
        let frame = encode_frame(sequence, payload)?;
        self.port.write_all(&frame)?;
        self.port.flush()?;
        Ok(())
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
