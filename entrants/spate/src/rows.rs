//! The ClickHouse row type for this workload, and the flatten that produces
//! it.
//!
//! These live in the Spate arm rather than in the harness deliberately, and the
//! reason is structural rather than tidiness. The harness computes expectations
//! and encodes Avro; it never *serialises* a row — only an arm does that, using
//! its own framework's encoder. Keeping [`Row`] here is what leaves the
//! harness with zero dependency on any system under test, which is the property
//! that lets a competitor audit how they were measured without trusting our
//! crates. CI asserts it.
//!
//! It also makes this arm symmetric with the Flink arm, which owns its own
//! `SensorRow.java` and `Rows.java` for exactly the same reason.
//!
//! The flatten itself is *pipeline logic*, which methodology/ rule 1 assigns
//! to the arm: every system writes this, and writing it well is expected.
//!
//! # Why nothing here is imported from the oracle
//!
//! Nothing in this module comes from `spate_benchmark_harness::corpus`, and the
//! absence is the point. `corpus` is the ORACLE — it computes the closed-form
//! expectations the correctness gate holds every arm to. An arm importing
//! `ascii_upper` and `value_scaled_of` from the oracle cannot disagree with the
//! marking scheme by construction, while the Flink arm, which reimplements both
//! in Java, can. Changing the oracle's uppercase to something Unicode-aware
//! would then move this arm silently while failing a gate Flink should have
//! passed — a guarantee one-sided in the author's favour.
//!
//! What every arm *does* share is the wire contract alone: the registry-served
//! `sensor_batch.avsc`, which the Flink arm parses from the same committed
//! file. A schema is not the transform. [`SensorBatch`]/[`Event`] below are
//! this arm's *reading* of that wire contract (serde matches fields by name
//! against the writer schema), not oracle logic.
//!
//! METHODOLOGY settles whose job the transform is: pipeline logic — the
//! flatten, the filter, the derived columns — "is user code in every system,
//! and every arm writes them". This arm writes them, below, and
//! `harness/tests/each_arm_restates_the_transform.rs` pins both arms'
//! restatements against the workload definition so a change to it cannot
//! re-specify one arm and not the other.

use serde::Serialize;
use spate_clickhouse::{DateTime64Micros, DateTime64Millis};
use spate_core::deser::RecFamily;
use spate_core::ops::Emitter;

/// The `unit` value the workload drops, restated for this arm.
///
/// Specified by `workload/workload.toml`'s `drop_unit`, which is hashed into
/// `dataset_version` — so the *value* is specification, and restating it is what
/// makes a change to the specification fail loudly in every arm at once instead
/// of flowing silently into one. The Flink arm restates the same constant in
/// `Rows.java`; a test holds both to the workload.
const DROP_UNIT: &str = "drop";

/// The workload's quality floor, restated for this arm. See [`DROP_UNIT`].
const QUALITY_FLOOR: f64 = 0.2;

/// ASCII-only uppercase, this arm's own.
///
/// The contract specifies ASCII-only rather than "uppercase" because Java's
/// `String.toUpperCase()` is locale-dependent and even `toUpperCase(Locale.ROOT)`
/// is Unicode-aware — it maps `ß` to `SS` and `ı` to `I` — so "uppercase" alone
/// would not be the same operation in every language. `str::to_ascii_uppercase`
/// folds exactly `a-z`, which is the specified operation.
fn ascii_upper(s: &str) -> String {
    s.to_ascii_uppercase()
}

/// `value_scaled = value * 1000 / (event_seq + 1)`, truncating toward zero.
///
/// `i64` throughout, deliberately: `value` runs to `2^31 - 1`, so `value * 1000`
/// reaches ~2.1e12 and overflows 32 bits. Rust's `/` on integers truncates toward
/// zero and `value` is non-negative by construction, so the truncation matches
/// the contract without a rounding mode to argue about.
fn value_scaled(value: i64, seq: u32) -> i64 {
    value * 1000 / i64::from(seq + 1)
}

// ---------------------------------------------------------------------------
// Decode target — this arm's reading of the wire contract
// ---------------------------------------------------------------------------

/// One decoded message, borrowed: every string field points into the payload
/// buffer the source handed the chain. Field names match the registry-served
/// `sensor_batch.avsc`; the single-pass deserializer decodes the datum
/// directly into this shape with no intermediate value tree.
#[derive(Debug, serde::Deserialize)]
pub struct SensorBatch<'a> {
    /// Batch identifier.
    pub batch_id: i64,
    /// Sensor identifier.
    #[serde(borrow)]
    pub sensor: &'a str,
    /// Nullable region — the union the decode path must handle.
    #[serde(borrow)]
    pub region: Option<&'a str>,
    /// Event timestamp, epoch milliseconds.
    pub batch_ts_ms: i64,
    /// Producer's intended send time, epoch microseconds.
    pub send_ts_us: i64,
    /// The events to fan out.
    pub events: Vec<Event<'a>>,
}

/// One event inside a [`SensorBatch`].
#[derive(Debug, serde::Deserialize)]
pub struct Event<'a> {
    /// Position within the batch; half of the row identity.
    pub seq: i32,
    /// Metric name.
    #[serde(borrow)]
    pub name: &'a str,
    /// Metric unit; `"drop"` is the filter sentinel.
    #[serde(borrow)]
    pub unit: &'a str,
    /// Metric value.
    pub value: i64,
    /// Nullable quality — the second union.
    pub quality: Option<f64>,
    /// Inner array-of-string.
    #[serde(borrow)]
    pub tags: Vec<&'a str>,
}

/// The borrowed [`SensorBatch`] record family the chain decodes into.
#[derive(Debug)]
pub struct BatchFam;
impl RecFamily for BatchFam {
    type Rec<'buf> = SensorBatch<'buf>;
}

// ---------------------------------------------------------------------------
// Output row. Field order IS the wire contract for RowBinary and Native, and
// must match `workload/clickhouse/ddl.sql` exactly.
// ---------------------------------------------------------------------------

/// The output row, matching `sensor_events`.
///
/// Borrowed where the value passes through from the message unchanged
/// (`sensor`, `region`, `unit`, the `tags` contents): the encoder writes the
/// same bytes from a `&str` as from a `String`, so borrowing changes the
/// allocation profile, not the output. `name_upper` is *derived* — a fresh
/// ASCII-uppercased string — so it is the one owned field, and the single
/// per-row allocation.
#[derive(Debug, Serialize)]
pub struct Row<'a> {
    /// Batch identifier.
    pub batch_id: u64,
    /// Event position within the batch.
    pub event_seq: u16,
    /// Sensor identifier.
    pub sensor: &'a str,
    /// Region, with the Avro null coalesced to the empty string.
    pub region: &'a str,
    /// ASCII-uppercased metric name.
    pub name_upper: String,
    /// Metric unit.
    pub unit: &'a str,
    /// Metric value.
    pub value: i64,
    /// Derived scaled value.
    pub value_scaled: i64,
    /// Metric quality.
    pub quality: Option<f64>,
    /// Tags. The `Vec` is the one the decoder built for this event — moved,
    /// not re-collected.
    pub tags: Vec<&'a str>,
    /// Event timestamp.
    pub batch_ts: DateTime64Millis,
    /// Producer's intended send time. The Native leaf writer does no
    /// `DateTime64` rescaling, so this wrapper is what keeps the value out of
    /// 1970.
    pub send_ts: DateTime64Micros,
}

/// The borrowed [`Row`] record family the flatten emits.
#[derive(Debug)]
pub struct RowFam;
impl RecFamily for RowFam {
    type Rec<'buf> = Row<'buf>;
}

// ---------------------------------------------------------------------------
// Flatten — the pipeline logic
// ---------------------------------------------------------------------------
//
// This lives in the library rather than in the SUT binary for a concrete
// reason: the rigs are declared `test = false`, so a `#[cfg(test)]` module
// inside a bin is never compiled or run. Tests placed there would be silently
// dead, and this is logic whose correctness the published numbers depend on.

/// Flatten a decoded batch into output rows, applying the specified filters
/// and derivations. Consumes the batch so each surviving event's `tags` vector
/// moves into its row instead of being copied.
pub fn flatten<'buf, F: FnMut(Row<'buf>)>(batch: SensorBatch<'buf>, mut emit: F) {
    let batch_id = u64::try_from(batch.batch_id).expect("batch_id non-negative");
    let region = batch.region.unwrap_or("");
    for e in batch.events {
        if e.unit == DROP_UNIT {
            continue;
        }
        if matches!(e.quality, Some(q) if q < QUALITY_FLOOR) {
            continue;
        }
        let seq = u32::try_from(e.seq).expect("seq non-negative");
        emit(Row {
            batch_id,
            event_seq: u16::try_from(e.seq).expect("seq fits u16"),
            sensor: batch.sensor,
            region,
            name_upper: ascii_upper(e.name),
            unit: e.unit,
            value: e.value,
            value_scaled: value_scaled(e.value, seq),
            quality: e.quality,
            tags: e.tags,
            batch_ts: DateTime64Millis(batch.batch_ts_ms),
            send_ts: DateTime64Micros(batch.send_ts_us),
        });
    }
}

/// The chain's `flat_map` stage: a `fn` item, as borrowing record families
/// require. `Emitter::emit`'s `Flow` return is deliberately discarded — the
/// emitter latches backpressure internally and the chain reads it after the
/// fan-out returns; breaking out mid-batch would drop the rest of this
/// message's events (the resume cursor is per input record).
pub fn explode<'buf>(batch: SensorBatch<'buf>, out: &mut Emitter<'_, RowFam>) {
    flatten(batch, |row| {
        out.emit(row);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_avro::{AvroDeserializerBuilder, AvroMode, AvroSettings, SchemaSource};
    use spate_benchmark_harness::corpus::{encode_batch, expected, region_of};
    use spate_core::checkpoint::AckRef;
    use spate_core::deser::{Deserializer, EmitRecord};
    use spate_core::record::{Flow, PartitionId, RawPayload, Record};

    /// The committed schema file — the same wire contract the registry serves
    /// at run time; `raw` mode here because tests decode bare datums.
    fn builder() -> AvroDeserializerBuilder {
        let schema_path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../workload/schema/sensor_batch.avsc"
        );
        let settings = AvroSettings {
            mode: AvroMode::Raw,
            schema: Some(SchemaSource::path(schema_path)),
            ..AvroSettings::default()
        };
        let rt = Box::leak(Box::new(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime"),
        ));
        AvroDeserializerBuilder::from_settings(&settings, rt.handle()).expect("builder")
    }

    /// Decode one corpus batch through the arm's actual decode path (the
    /// single-pass borrowed deserializer) and flatten it.
    fn flatten_batch<G: FnMut(Row<'_>)>(batch_id: u64, send_ts_us: i64, mut per_row: G) {
        struct Sink<'g, G>(&'g mut G);
        impl<'buf, G: FnMut(Row<'_>)> EmitRecord<'buf, SensorBatch<'buf>> for Sink<'_, G> {
            fn emit(&mut self, rec: Record<SensorBatch<'buf>>) -> Flow {
                flatten(rec.payload, |row| (self.0)(row));
                Flow::Continue
            }
        }
        let datum = encode_batch(batch_id, send_ts_us);
        let raw = RawPayload {
            bytes: &datum,
            key: None,
            partition: PartitionId(0),
            offset: 0,
            timestamp_ms: 0,
        };
        let (ack, _rx) = AckRef::test_pair();
        builder()
            .build_datum::<BatchFam>()
            .expect("datum deserializer")
            .deserialize(&raw, &ack, &mut Sink(&mut per_row))
            .expect("decode");
    }

    /// The flatten must agree with `expected()`, which is what the correctness
    /// gate compares ClickHouse against — decoded through the same single-pass
    /// path the pipeline uses. If they disagreed, a correct arm would fail the
    /// gate or a broken one would pass it.
    #[test]
    fn flatten_agrees_with_the_gate_expectations() {
        let batches = 300u64;
        let mut rows = 0u64;
        let (mut sum, mut scaled) = (0i128, 0i128);
        for batch_id in 0..batches {
            flatten_batch(batch_id, 0, |r| {
                rows += 1;
                sum += i128::from(r.value);
                scaled += i128::from(r.value_scaled);
            });
        }
        let exp = expected(batches);
        assert_eq!((exp.rows, exp.value_sum), (rows, sum), "rows and value sum");
        assert_eq!(exp.value_scaled_sum, scaled, "scaled sum");
    }

    /// A null region must be coalesced to the empty string, because the target
    /// column is `LowCardinality(String)` and not nullable.
    #[test]
    fn a_null_region_becomes_the_empty_string() {
        assert!(region_of(10).is_none());
        let mut seen = Vec::new();
        flatten_batch(10, 0, |r| seen.push(r.region.to_owned()));
        assert!(!seen.is_empty(), "batch 10 must yield surviving rows");
        assert!(seen.iter().all(String::is_empty), "null region coalesces");
    }

    /// The borrowed fields must actually borrow: pointer provenance against
    /// the datum buffer, so a regression to copying shows up as a test
    /// failure rather than a silent allocation.
    #[test]
    fn surviving_rows_borrow_from_the_payload() {
        let datum = encode_batch(1, 0);
        let range = datum.as_ptr() as usize..datum.as_ptr() as usize + datum.len();
        let raw = RawPayload {
            bytes: &datum,
            key: None,
            partition: PartitionId(0),
            offset: 0,
            timestamp_ms: 0,
        };
        let (ack, _rx) = AckRef::test_pair();
        struct Probe<'a>(&'a std::ops::Range<usize>, u64);
        impl<'buf> EmitRecord<'buf, SensorBatch<'buf>> for Probe<'_> {
            fn emit(&mut self, rec: Record<SensorBatch<'buf>>) -> Flow {
                let range = self.0;
                let mut rows = 0;
                flatten(rec.payload, |row| {
                    assert!(range.contains(&(row.sensor.as_ptr() as usize)), "sensor");
                    assert!(range.contains(&(row.unit.as_ptr() as usize)), "unit");
                    for t in &row.tags {
                        assert!(range.contains(&(t.as_ptr() as usize)), "tag");
                    }
                    rows += 1;
                });
                self.1 += rows;
                Flow::Continue
            }
        }
        let mut probe = Probe(&range, 0);
        builder()
            .build_datum::<BatchFam>()
            .expect("datum deserializer")
            .deserialize(&raw, &ack, &mut probe)
            .expect("decode");
        assert!(probe.1 > 0, "batch 1 must yield surviving rows");
    }
}
