//! Klipper MCU wire-protocol primitives.
//!
//! This crate contains the framing and integer encoding layer, plus the
//! firmware-provided identify dictionary codec.

pub mod dictionary;
pub use dictionary::{decode_identify_response, encode_identify_request, DictionaryError, IdentifyChunk, IdentifyDictionary, MessageKind};

use thiserror::Error;

pub const MESSAGE_MIN: usize = 5;
pub const MESSAGE_MAX: usize = 64;
pub const MESSAGE_HEADER_SIZE: usize = 2;
pub const MESSAGE_TRAILER_SIZE: usize = 3;
pub const MESSAGE_SEQ_MASK: u8 = 0x0f;
pub const MESSAGE_DEST: u8 = 0x10;
pub const MESSAGE_SYNC: u8 = 0x7e;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("truncated variable-length integer")]
    TruncatedInteger,
    #[error("variable-length integer exceeds 5 bytes")]
    IntegerTooLong,
    #[error("packet length {0} is outside the valid range")]
    InvalidLength(usize),
    #[error("invalid destination/sequence byte 0x{0:02x}")]
    InvalidSequence(u8),
    #[error("packet sync byte is missing")]
    MissingSync,
    #[error("CRC mismatch: expected {expected:02x?}, got {actual:02x?}")]
    CrcMismatch { expected: [u8; 2], actual: [u8; 2] },
    #[error("incomplete packet")]
    Incomplete,
}

pub fn crc16_ccitt(data: &[u8]) -> [u8; 2] {
    let mut crc: u16 = 0xffff;
    for &byte in data {
        let mut value = byte ^ (crc as u8);
        value ^= value << 4;
        crc = ((value as u16) << 8) | (crc >> 8);
        crc ^= (value as u16) >> 4;
        crc ^= (value as u16) << 3;
    }
    [(crc >> 8) as u8, crc as u8]
}

/// Encode Klipper's signed base-128 integer representation.
pub fn encode_i32(value: i32, out: &mut Vec<u8>) {
    let v = value as i64;
    if v >= 0x0c00_0000 || v < -0x0400_0000 {
        out.push(((v >> 28) as u8 & 0x7f) | 0x80);
    }
    if v >= 0x0018_0000 || v < -0x0008_0000 {
        out.push(((v >> 21) as u8 & 0x7f) | 0x80);
    }
    if v >= 0x0000_3000 || v < -0x0000_1000 {
        out.push(((v >> 14) as u8 & 0x7f) | 0x80);
    }
    if v >= 0x0000_0060 || v < -0x0000_0020 {
        out.push(((v >> 7) as u8 & 0x7f) | 0x80);
    }
    out.push(v as u8 & 0x7f);
}

/// Encode an unsigned 32-bit value using the same wire representation.
pub fn encode_u32(value: u32, out: &mut Vec<u8>) {
    if value >= 0x0c00_0000 {
        out.push(((value >> 28) as u8 & 0x7f) | 0x80);
    }
    if value >= 0x0018_0000 {
        out.push(((value >> 21) as u8 & 0x7f) | 0x80);
    }
    if value >= 0x0000_3000 {
        out.push(((value >> 14) as u8 & 0x7f) | 0x80);
    }
    if value >= 0x0000_0060 {
        out.push(((value >> 7) as u8 & 0x7f) | 0x80);
    }
    out.push((value & 0x7f) as u8);
}

/// Decode a signed Klipper base-128 integer, returning (value, bytes_used).
pub fn decode_i32(input: &[u8]) -> Result<(i32, usize), ProtocolError> {
    let first = *input.first().ok_or(ProtocolError::TruncatedInteger)?;
    let mut value = (first & 0x7f) as i32;
    if first & 0x60 == 0x60 {
        value |= !0x1f;
    }
    let mut byte = first;
    let mut pos = 1;
    while byte & 0x80 != 0 {
        if pos >= input.len() {
            return Err(ProtocolError::TruncatedInteger);
        }
        if pos >= 5 {
            return Err(ProtocolError::IntegerTooLong);
        }
        byte = input[pos];
        value = (value << 7) | (byte & 0x7f) as i32;
        pos += 1;
    }
    Ok((value, pos))
}

pub fn decode_u32(input: &[u8]) -> Result<(u32, usize), ProtocolError> {
    let (value, used) = decode_i32(input)?;
    Ok((value as u32, used))
}

/// Build one MCU protocol frame from a message payload (message-id + params).
pub fn encode_frame(sequence: u8, payload: &[u8]) -> Result<Vec<u8>, ProtocolError> {
    let len = MESSAGE_MIN + payload.len();
    if !(MESSAGE_MIN..=MESSAGE_MAX).contains(&len) {
        return Err(ProtocolError::InvalidLength(len));
    }
    let mut frame = Vec::with_capacity(len);
    frame.push(len as u8);
    frame.push((sequence & MESSAGE_SEQ_MASK) | MESSAGE_DEST);
    frame.extend_from_slice(payload);
    let crc = crc16_ccitt(&frame);
    frame.extend_from_slice(&crc);
    frame.push(MESSAGE_SYNC);
    Ok(frame)
}

/// One validated MCU frame, excluding the two-byte header and three-byte trailer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub sequence: u8,
    pub payload: Vec<u8>,
}

/// Incremental decoder for an arbitrary serial byte stream.
///
/// It tolerates split frames and attempts to recover synchronization after
/// malformed bytes. Each call returns every complete valid frame currently
/// buffered; invalid candidate frames are discarded one byte at a time.
#[derive(Debug, Default)]
pub struct FrameDecoder {
    buffer: Vec<u8>,
    discarded_bytes: u64,
    rejected_frames: u64,
}

impl FrameDecoder {
    pub fn new() -> Self { Self::default() }

    pub fn discarded_bytes(&self) -> u64 { self.discarded_bytes }
    pub fn rejected_frames(&self) -> u64 { self.rejected_frames }
    pub fn buffered_len(&self) -> usize { self.buffer.len() }

    pub fn push(&mut self, bytes: &[u8]) -> Vec<Frame> {
        self.buffer.extend_from_slice(bytes);
        let mut frames = Vec::new();
        loop {
            if self.buffer.len() < MESSAGE_HEADER_SIZE { break; }
            let len = self.buffer[0] as usize;
            if !(MESSAGE_MIN..=MESSAGE_MAX).contains(&len) {
                self.buffer.remove(0);
                self.discarded_bytes += 1;
                self.rejected_frames += 1;
                continue;
            }
            if self.buffer.len() < len { break; }
            match check_frame(&self.buffer[..len]) {
                Ok(_) => {
                    let sequence = self.buffer[1] & MESSAGE_SEQ_MASK;
                    let payload = self.buffer[MESSAGE_HEADER_SIZE..len - MESSAGE_TRAILER_SIZE].to_vec();
                    self.buffer.drain(..len);
                    frames.push(Frame { sequence, payload });
                }
                Err(_) => {
                    self.buffer.remove(0);
                    self.discarded_bytes += 1;
                    self.rejected_frames += 1;
                }
            }
        }
        frames
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

/// Validate a complete frame. Returns its declared length.
pub fn check_frame(frame: &[u8]) -> Result<usize, ProtocolError> {
    if frame.len() < MESSAGE_MIN {
        return Err(ProtocolError::Incomplete);
    }
    let len = frame[0] as usize;
    if !(MESSAGE_MIN..=MESSAGE_MAX).contains(&len) {
        return Err(ProtocolError::InvalidLength(len));
    }
    if frame.len() < len {
        return Err(ProtocolError::Incomplete);
    }
    let sequence = frame[1];
    if sequence & !MESSAGE_SEQ_MASK != MESSAGE_DEST {
        return Err(ProtocolError::InvalidSequence(sequence));
    }
    if frame[len - 1] != MESSAGE_SYNC {
        return Err(ProtocolError::MissingSync);
    }
    let expected = crc16_ccitt(&frame[..len - MESSAGE_TRAILER_SIZE]);
    let actual = [frame[len - 3], frame[len - 2]];
    if expected != actual {
        return Err(ProtocolError::CrcMismatch { expected, actual });
    }
    Ok(len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_integer_round_trip_boundaries() {
        let values = [
            i32::MIN, -0x0400_0001, -0x0400_0000, -0x80001, -0x80000,
            -4097, -4096, -33, -32, -1, 0, 1, 31, 32, 95, 96,
            4095, 4096, 0x17ffff, 0x180000, 0x0bffffff, 0x0c000000,
            i32::MAX,
        ];
        for value in values {
            let mut encoded = Vec::new();
            encode_i32(value, &mut encoded);
            let (decoded, used) = decode_i32(&encoded).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(used, encoded.len());
        }
    }

    #[test]
    fn unsigned_integer_round_trip() {
        for value in [0, 1, 127, 128, 0x7fff_ffff, 0x8000_0000, u32::MAX] {
            let mut encoded = Vec::new();
            encode_u32(value, &mut encoded);
            let (decoded, used) = decode_u32(&encoded).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(used, encoded.len());
        }
    }

    #[test]
    fn frame_round_trip_and_crc_rejection() {
        let frame = encode_frame(7, &[0x23, 0x01, 0x02, 0x03]).unwrap();
        assert_eq!(check_frame(&frame), Ok(frame.len()));
        let mut damaged = frame;
        damaged[2] ^= 1;
        assert!(matches!(check_frame(&damaged), Err(ProtocolError::CrcMismatch { .. })));
    }

    #[test]
    fn stream_decoder_handles_fragmented_and_concatenated_frames() {
        let a = encode_frame(2, &[1, 2, 3]).unwrap();
        let b = encode_frame(3, &[4, 5]).unwrap();
        let mut decoder = FrameDecoder::new();
        assert!(decoder.push(&a[..2]).is_empty());
        let mut rest = a[2..].to_vec();
        rest.extend_from_slice(&b);
        let frames = decoder.push(&rest);
        assert_eq!(frames, vec![
            Frame { sequence: 2, payload: vec![1, 2, 3] },
            Frame { sequence: 3, payload: vec![4, 5] },
        ]);
        assert_eq!(decoder.buffered_len(), 0);
    }

    #[test]
    fn stream_decoder_resynchronizes_after_garbage_and_bad_crc() {
        let good = encode_frame(4, &[9, 8]).unwrap();
        let mut bad = encode_frame(1, &[7]).unwrap();
        bad[2] ^= 0x40;
        let mut decoder = FrameDecoder::new();
        let mut input = vec![0xff, 0x00, 0x01];
        input.extend_from_slice(&bad);
        input.extend_from_slice(&good);
        let frames = decoder.push(&input);
        assert_eq!(frames, vec![Frame { sequence: 4, payload: vec![9, 8] }]);
        assert!(decoder.discarded_bytes() >= 3);
    }

    #[test]
    fn rejects_invalid_frame_size() {
        assert!(matches!(encode_frame(0, &[0; 60]), Err(ProtocolError::InvalidLength(_))));
    }
}
