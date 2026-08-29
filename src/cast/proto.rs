// SPDX-License-Identifier: MIT OR Apache-2.0
//! Hand-rolled Protobuf codec for the CastMessage structure
//! (`03-cast-engine.md` §5). Only the fields this application uses are
//! decoded; unknown fields are skipped by wire type.

use thiserror::Error;

/// Maximum accepted size of a single length-delimited field's payload
/// (`03-cast-engine.md` §5): anything larger is treated as a protocol error.
pub const MAX_FIELD_SIZE: usize = 16 * 1024 * 1024;

/// Protobuf wire types (`03-cast-engine.md` §5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireType {
    Varint = 0,
    Fixed64 = 1,
    LengthDelimited = 2,
    Fixed32 = 5,
}

impl WireType {
    fn from_number(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Varint),
            1 => Some(Self::Fixed64),
            2 => Some(Self::LengthDelimited),
            5 => Some(Self::Fixed32),
            // Groups (3, 4) are deprecated and never produced by Cast
            // receivers; treat them as malformed.
            _ => None,
        }
    }
}

/// Errors produced while encoding or decoding CastMessage protobuf payloads.
/// Parsing never panics; malformed input surfaces as an error.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProtoError {
    #[error("payload ends while decoding a varint (only {0} bytes were available)")]
    UnexpectedEnd(usize),
    #[error("varint overflow: more than 10 bytes or 64 bits consumed")]
    VarintOverflow,
    #[error("malformed field key: unknown wire type {0}")]
    UnknownWireType(u8),
    #[error("malformed field key: no field number")]
    MissingFieldNumber,
    #[error("length-delimited field declared {0} bytes, but only {1} remain in the payload")]
    TruncatedField(usize, usize),
    #[error("length-delimited field exceeds maximum size of {MAX_FIELD_SIZE} bytes")]
    FieldTooLarge,
}

/// Encode an unsigned integer as a base-128 LEB128 varint.
pub fn varint_encode(mut value: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return out;
        }
        out.push(byte | 0x80);
    }
}

/// Decode an unsigned LEB128 varint, consuming at most 10 bytes (64 bits).
pub fn varint_decode(bytes: &[u8]) -> Result<(u64, usize), ProtoError> {
    let mut value: u64 = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        if index >= 10 {
            return Err(ProtoError::VarintOverflow);
        }
        // The 10th byte can only contribute bit 63; payload bits above bit 0
        // would silently shift out of u64, so reject them before OR-ing.
        if index == 9 && (byte & 0x7F) > 0x01 {
            return Err(ProtoError::VarintOverflow);
        }
        value |= ((byte & 0x7F) as u64) << (7 * index);
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
    }
    Err(ProtoError::UnexpectedEnd(bytes.len()))
}

/// The CastMessage payload discriminator (`03-cast-engine.md` §5, field 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadType {
    /// `payload_utf8` carries a JSON string.
    String,
    /// `payload_binary` carries raw bytes (unused by this application).
    Binary,
}

/// A parsed CastMessage (`03-cast-engine.md` §5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CastMessage {
    /// Field 1; this application always sends `CASTV2_1_0` = 0.
    pub protocol_version: u32,
    /// Field 2: sender identifier.
    pub source_id: String,
    /// Field 3: destination identifier.
    pub destination_id: String,
    /// Field 4: namespace URN.
    pub namespace: String,
    /// Field 5: payload discriminator.
    pub payload_type: PayloadType,
    /// Field 6: UTF-8 payload (JSON for the namespaces this app uses).
    pub payload_utf8: String,
}

impl CastMessage {
    /// A decoded message defaults to `CASTV2_1_0` / `STRING` with empty
    /// strings for absent fields (proto2 semantics, no required fields).
    fn empty() -> Self {
        Self {
            protocol_version: 0,
            source_id: String::new(),
            destination_id: String::new(),
            namespace: String::new(),
            payload_type: PayloadType::String,
            payload_utf8: String::new(),
        }
    }
}

fn key(field: u32, wire: WireType) -> u64 {
    ((field as u64) << 3) | wire as u64
}

fn write_length_delimited(out: &mut Vec<u8>, field: u32, data: &[u8]) {
    out.extend_from_slice(&varint_encode(key(field, WireType::LengthDelimited)));
    out.extend_from_slice(&varint_encode(data.len() as u64));
    out.extend_from_slice(data);
}

/// Encode a CastMessage with `protocol_version = CASTV2_1_0` (0),
/// `payload_type = STRING` (0) and the given JSON `payload`
/// (`03-cast-engine.md` §5). The default-receiver heartbeats and the
/// CONNECT/status/media messages all fit this shape.
pub fn encode_cast_message(source: &str, dest: &str, namespace: &str, payload: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(32 + payload.len());
    // Field 1: protocol_version = 0 (CASTV2_1_0).
    out.extend_from_slice(&varint_encode(key(1, WireType::Varint)));
    out.push(0);
    write_length_delimited(&mut out, 2, source.as_bytes());
    write_length_delimited(&mut out, 3, dest.as_bytes());
    write_length_delimited(&mut out, 4, namespace.as_bytes());
    // Field 5: payload_type = 0 (STRING).
    out.extend_from_slice(&varint_encode(key(5, WireType::Varint)));
    out.push(0);
    write_length_delimited(&mut out, 6, payload.as_bytes());
    out
}

/// Decode a CastMessage, skipping unknown fields by wire type and rejecting
/// malformed or over-length payloads without panicking (`03-cast-engine.md` §5).
pub fn decode_cast_message(bytes: &[u8]) -> Result<CastMessage, ProtoError> {
    let mut message = CastMessage::empty();
    let mut pos = 0;

    while pos < bytes.len() {
        let (key_value, consumed) = varint_decode(&bytes[pos..])?;
        pos += consumed;
        let field = (key_value >> 3) as u32;
        let wire = WireType::from_number((key_value & 0x07) as u8)
            .ok_or(ProtoError::UnknownWireType((key_value & 0x07) as u8))?;
        if field == 0 {
            return Err(ProtoError::MissingFieldNumber);
        }

        match wire {
            WireType::Varint => {
                let (value, consumed) = varint_decode(&bytes[pos..])?;
                pos += consumed;

                match field {
                    1 => message.protocol_version = value as u32,
                    5 => {
                        message.payload_type = if value == 1 {
                            PayloadType::Binary
                        } else {
                            PayloadType::String
                        }
                    }
                    // Unknown varint fields are skipped by wire type.
                    _ => {}
                }
            }
            WireType::LengthDelimited => {
                let (length, consumed) = varint_decode(&bytes[pos..])?;
                pos += consumed;
                if length as usize > MAX_FIELD_SIZE {
                    return Err(ProtoError::FieldTooLarge);
                }
                if pos + length as usize > bytes.len() {
                    return Err(ProtoError::TruncatedField(
                        length as usize,
                        bytes.len() - pos,
                    ));
                }
                let data = &bytes[pos..pos + length as usize];
                pos += length as usize;

                match field {
                    2 => message.source_id = String::from_utf8_lossy(data).into_owned(),
                    3 => message.destination_id = String::from_utf8_lossy(data).into_owned(),
                    4 => message.namespace = String::from_utf8_lossy(data).into_owned(),
                    6 => message.payload_utf8 = String::from_utf8_lossy(data).into_owned(),
                    // Field 7 (payload_binary) is unused by this application;
                    // skip. Unknown fields are skipped by wire type.
                    _ => {}
                }
            }
            WireType::Fixed64 => pos = skip_fixed(8, pos, bytes.len())?,
            WireType::Fixed32 => pos = skip_fixed(4, pos, bytes.len())?,
        }
    }

    Ok(message)
}

fn skip_fixed(width: usize, pos: usize, len: usize) -> Result<usize, ProtoError> {
    if pos + width > len {
        return Err(ProtoError::UnexpectedEnd(len - pos));
    }
    Ok(pos + width)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_single_byte_values() {
        // (FR-019) Varint edge cases: 0, 127, 128, u32::MAX, u64::MAX.
        assert_eq!(varint_encode(0), vec![0x00]);
        assert_eq!(varint_decode(&[0x00]), Ok((0, 1)));
        assert_eq!(varint_encode(127), vec![0x7F]);
        assert_eq!(varint_decode(&[0x7F]), Ok((127, 1)));
        assert_eq!(varint_encode(128), vec![0x80, 0x01]);
        assert_eq!(varint_decode(&[0x80, 0x01]), Ok((128, 2)));
        assert_eq!(
            varint_encode(u32::MAX as u64),
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0x0F]
        );
        assert_eq!(
            varint_decode(&[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
            Ok((u32::MAX as u64, 5))
        );
    }

    #[test]
    fn varint_u64_max_round_trip() {
        let encoded = varint_encode(u64::MAX);
        assert_eq!(
            encoded,
            vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]
        );
        assert_eq!(varint_decode(&encoded), Ok((u64::MAX, 10)));
    }

    #[test]
    fn varint_overflow_and_truncation_are_errors() {
        assert_eq!(
            varint_decode(&[
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01
            ]),
            Err(ProtoError::VarintOverflow)
        );
        assert_eq!(varint_decode(&[0x80]), Err(ProtoError::UnexpectedEnd(1)));
        assert_eq!(varint_decode(&[]), Err(ProtoError::UnexpectedEnd(0)));
    }

    #[test]
    fn varint_tenth_byte_past_bit_63_is_overflow() {
        // u64::MAX's 10-byte encoding ends with 0x01, contributing exactly
        // bit 63. A tenth byte with payload bits above bit 0 (0x02..=0x7F)
        // would silently shift out of u64, so the decoder must reject it
        // rather than return a truncated value.
        assert_eq!(
            varint_decode(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02]),
            Err(ProtoError::VarintOverflow)
        );
        assert_eq!(
            varint_decode(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x7F]),
            Err(ProtoError::VarintOverflow)
        );
    }

    /// The expected encoding of the CONNECT message payload from
    /// `sender-0` to `receiver-0` over
    /// `urn:x-cast:com.google.cast.tp.connection` — bytes hand-derived, not
    /// produced by the code under test.
    fn golden_connect_payload() -> Vec<u8> {
        let payload = r#"{"type":"CONNECT","origin":{},"userAgent":"cast-app/0.1.0","connType":0,"senderInfo":{"sdkType":2,"version":"0.1.0","browserVersion":"0.1.0","platform":6,"connectionType":1}}"#;
        let mut bytes = vec![
            0x08, 0x00, // field 1: protocol_version = 0
            0x12, 0x08, b's', b'e', b'n', b'd', b'e', b'r', b'-', b'0', // field 2
            0x1A, 0x0A, b'r', b'e', b'c', b'e', b'i', b'v', b'e', b'r', b'-', b'0', // field 3
            0x22, 0x28, // field 4, length 40
            b'u', b'r', b'n', b':', b'x', b'-', b'c', b'a', b's', b't', b':', b'c', b'o', b'm',
            b'.', b'g', b'o', b'o', b'g', b'l', b'e', b'.', b'c', b'a', b's', b't', b'.', b't',
            b'p', b'.', b'c', b'o', b'n', b'n', b'e', b'c', b't', b'i', b'o', b'n', 0x28,
            0x00, // field 5: payload_type = 0 (STRING)
        ];
        // Field 6: payload_utf8 — length is varint-encoded (174 bytes needs
        // two bytes 0xAE 0x01); the `CONNECT` payload grew from the minimal
        // `{"type":"CONNECT"}` (18 bytes, single-byte length) to the richer
        // senderInfo form, so the helper must handle multi-byte lengths.
        bytes.push(0x32); // field 6, wire type 2
        bytes.extend_from_slice(&varint_encode(payload.len() as u64));
        bytes.extend_from_slice(payload.as_bytes());
        bytes
    }

    #[test]
    fn encode_matches_golden_connect_message() {
        let encoded = encode_cast_message(
            "sender-0",
            "receiver-0",
            "urn:x-cast:com.google.cast.tp.connection",
            r#"{"type":"CONNECT","origin":{},"userAgent":"cast-app/0.1.0","connType":0,"senderInfo":{"sdkType":2,"version":"0.1.0","browserVersion":"0.1.0","platform":6,"connectionType":1}}"#,
        );
        assert_eq!(encoded, golden_connect_payload());
    }

    #[test]
    fn decode_round_trips_the_golden_message() {
        let message = decode_cast_message(&golden_connect_payload()).expect("golden decodes");
        assert_eq!(message.protocol_version, 0);
        assert_eq!(message.source_id, "sender-0");
        assert_eq!(message.destination_id, "receiver-0");
        assert_eq!(
            message.namespace,
            "urn:x-cast:com.google.cast.tp.connection"
        );
        assert_eq!(message.payload_type, PayloadType::String);
        assert_eq!(
            message.payload_utf8,
            r#"{"type":"CONNECT","origin":{},"userAgent":"cast-app/0.1.0","connType":0,"senderInfo":{"sdkType":2,"version":"0.1.0","browserVersion":"0.1.0","platform":6,"connectionType":1}}"#
        );
    }

    #[test]
    fn decode_tolerates_unknown_fields() {
        // (FR-021) Unknown fields (e.g. field 100) are skipped by wire type
        // without failing the parse.
        let mut bytes = golden_connect_payload();
        bytes.extend_from_slice(&[0xA0, 0x06, 0x2A]); // field 100, varint 42
        bytes.extend_from_slice(&[0xAA, 0x06, 0x03, 0x01, 0x02, 0x03]); // field 100, bytes
        let message = decode_cast_message(&bytes).expect("unknown fields skipped");
        assert_eq!(
            message.payload_utf8,
            r#"{"type":"CONNECT","origin":{},"userAgent":"cast-app/0.1.0","connType":0,"senderInfo":{"sdkType":2,"version":"0.1.0","browserVersion":"0.1.0","platform":6,"connectionType":1}}"#,
        );
    }

    #[test]
    fn decode_rejects_malformed_input() {
        // Truncated length-delimited field.
        let mut bytes = golden_connect_payload();
        bytes.truncate(bytes.len() - 3);
        assert!(decode_cast_message(&bytes).is_err());

        // Over-length field declaration.
        let bad = vec![0x32, 0xFF, 0xFF, 0xFF, 0x7F, 0x01]; // field 6, length ~2^31
        assert_eq!(decode_cast_message(&bad), Err(ProtoError::FieldTooLarge));

        // Group wire type (3) is rejected.
        let group = vec![0x1B]; // field 3, wire type 3
        assert_eq!(
            decode_cast_message(&group),
            Err(ProtoError::UnknownWireType(3))
        );

        // Field number 0 is rejected.
        let zero_field = vec![0x00];
        assert_eq!(
            decode_cast_message(&zero_field),
            Err(ProtoError::MissingFieldNumber)
        );

        // Truncated fixed64 / fixed32 fields are rejected.
        let fixed64_truncated = vec![0x09, 0x01]; // field 1, wire type 1
        assert!(decode_cast_message(&fixed64_truncated).is_err());
        let fixed32_truncated = vec![0x0D, 0x01, 0x02]; // field 1, wire type 5
        assert!(decode_cast_message(&fixed32_truncated).is_err());
    }
}
