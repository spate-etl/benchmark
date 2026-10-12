//! ClickHouse's compressed-frame encoding, for an ingest ceiling taken with a
//! compressed body.
//!
//! An HTTP insert sent with `decompress=1` carries its body as a sequence of
//! frames, each one `[16-byte checksum][method byte][compressed size: u32 LE]
//! [uncompressed size: u32 LE][payload]`. The compressed size counts the 9-byte
//! header as well as the payload. The checksum is CityHash128 v1.0.2 over the
//! header and payload, stored as a little-endian `u128` with its halves
//! swapped. The server decompresses the frames into one continuous byte stream
//! before parsing it, so a frame boundary need not fall on a row or a block.
//!
//! Written here rather than through a ClickHouse client crate's insert API, so
//! the ceiling rig does not frame its bodies with the code an arm inserts
//! through. It does compile the same codec crates the arm's client links.

/// Target uncompressed bytes per frame.
///
/// 64 KiB, the chunk size the Spate arm's sink seals a Native block at, one
/// block per frame. Frame size moves both the ratio and the per-frame cost, so
/// a ceiling taken with much larger frames would describe a cheaper insert than
/// the arm sends.
pub const FRAME_BYTES: usize = 64 * 1024;

const METHOD_LZ4: u8 = 0x82;
const METHOD_ZSTD: u8 = 0x90;
const HEADER_BYTES: usize = 9;

/// A frame codec the ceiling rig can send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    /// No compression: the body is sent as it is, without frames.
    None,
    /// LZ4 block compression.
    Lz4,
    /// Zstandard at the given level.
    Zstd(i32),
}

impl Codec {
    /// Parses `none`, `lz4`, `zstd` (level 3) or `zstd:<1-22>`.
    ///
    /// # Errors
    ///
    /// Any other spelling, or a zstd level outside 1..=22.
    pub fn parse(spec: &str) -> Result<Self, String> {
        match spec {
            "none" => Ok(Self::None),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd(3)),
            other => {
                let level = other
                    .strip_prefix("zstd:")
                    .ok_or_else(|| {
                        format!("unknown compression {other:?}: none, lz4, zstd or zstd:<1-22>")
                    })?
                    .parse::<i32>()
                    .map_err(|e| format!("compression {other:?}: {e}"))?;
                if (1..=22).contains(&level) {
                    Ok(Self::Zstd(level))
                } else {
                    Err(format!(
                        "compression {other:?}: zstd level must be in 1..=22"
                    ))
                }
            }
        }
    }

    /// The spelling [`Codec::parse`] accepts, for logs.
    #[must_use]
    pub fn name(self) -> String {
        match self {
            Self::None => "none".to_owned(),
            Self::Lz4 => "lz4".to_owned(),
            Self::Zstd(level) => format!("zstd:{level}"),
        }
    }

    fn method(self) -> u8 {
        match self {
            Self::None => unreachable!("an uncompressed body has no frames"),
            Self::Lz4 => METHOD_LZ4,
            Self::Zstd(_) => METHOD_ZSTD,
        }
    }

    fn compress(self, input: &[u8]) -> Result<Vec<u8>, String> {
        match self {
            Self::None => Ok(input.to_vec()),
            Self::Lz4 => Ok(lz4_flex::block::compress(input)),
            Self::Zstd(level) => {
                zstd::bulk::compress(input, level).map_err(|e| format!("zstd compress: {e}"))
            }
        }
    }
}

/// Encodes `body` as consecutive frames of at most `frame_bytes` uncompressed
/// bytes each, or returns it unchanged for [`Codec::None`].
///
/// # Errors
///
/// The codec's own failure, or a frame too large for the header's `u32` sizes.
pub fn encode(body: &[u8], codec: Codec, frame_bytes: usize) -> Result<Vec<u8>, String> {
    if codec == Codec::None {
        return Ok(body.to_vec());
    }
    let mut out = Vec::with_capacity(body.len());
    for chunk in body.chunks(frame_bytes.max(1)) {
        let payload = codec.compress(chunk)?;
        let compressed = u32::try_from(HEADER_BYTES + payload.len())
            .map_err(|_| "a compressed frame exceeds u32".to_owned())?;
        let uncompressed =
            u32::try_from(chunk.len()).map_err(|_| "a frame exceeds u32".to_owned())?;

        let mut framed = Vec::with_capacity(HEADER_BYTES + payload.len());
        framed.push(codec.method());
        framed.extend_from_slice(&compressed.to_le_bytes());
        framed.extend_from_slice(&uncompressed.to_le_bytes());
        framed.extend_from_slice(&payload);

        let checksum = cityhash_rs::cityhash_102_128(&framed).rotate_right(64);
        out.extend_from_slice(&checksum.to_le_bytes());
        out.extend_from_slice(&framed);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHECKSUM_BYTES: usize = 16;

    /// Splits an encoded stream back into its frames, checking each header and
    /// checksum, and returns the decompressed bytes.
    fn decode(stream: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut rest = stream;
        while !rest.is_empty() {
            let (checksum, framed) = rest.split_at(CHECKSUM_BYTES);
            let compressed = u32::from_le_bytes(framed[1..5].try_into().unwrap()) as usize;
            let uncompressed = u32::from_le_bytes(framed[5..9].try_into().unwrap()) as usize;
            let (frame, tail) = framed.split_at(compressed);
            assert_eq!(
                u128::from_le_bytes(checksum.try_into().unwrap()),
                cityhash_rs::cityhash_102_128(frame).rotate_right(64),
            );
            let payload = &frame[HEADER_BYTES..];
            let plain = match frame[0] {
                METHOD_LZ4 => lz4_flex::block::decompress(payload, uncompressed).unwrap(),
                METHOD_ZSTD => zstd::bulk::decompress(payload, uncompressed).unwrap(),
                other => panic!("unknown method byte {other:#x}"),
            };
            assert_eq!(plain.len(), uncompressed);
            out.extend_from_slice(&plain);
            rest = tail;
        }
        out
    }

    /// Every codec round-trips across frame boundaries, including a short last
    /// frame, with each frame's header and checksum intact.
    #[test]
    fn frames_round_trip() {
        let body: Vec<u8> = (0..200_000u32)
            .flat_map(|i| (i % 251).to_le_bytes())
            .collect();
        for codec in [Codec::Lz4, Codec::Zstd(1), Codec::Zstd(3)] {
            let stream = encode(&body, codec, FRAME_BYTES).unwrap();
            assert!(stream.len() < body.len(), "{}", codec.name());
            assert_eq!(decode(&stream), body, "{}", codec.name());
        }
        assert_eq!(encode(&body, Codec::None, FRAME_BYTES).unwrap(), body);
    }

    #[test]
    fn parses_the_sink_spellings() {
        assert_eq!(Codec::parse("none"), Ok(Codec::None));
        assert_eq!(Codec::parse("lz4"), Ok(Codec::Lz4));
        assert_eq!(Codec::parse("zstd"), Ok(Codec::Zstd(3)));
        assert_eq!(Codec::parse("zstd:1"), Ok(Codec::Zstd(1)));
        assert!(Codec::parse("zstd:0").is_err());
        assert!(Codec::parse("zstd:23").is_err());
        assert!(Codec::parse("gzip").is_err());
        assert_eq!(Codec::Zstd(3).name(), "zstd:3");
    }
}
