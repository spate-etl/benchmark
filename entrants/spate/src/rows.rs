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
//! file. A schema is not the transform.
//!
//! METHODOLOGY settles whose job the transform is: pipeline logic — the
//! flatten, the filter, the derived columns — "is user code in every system,
//! and every arm writes them". This arm writes them, below, and
//! `harness/tests/each_arm_restates_the_transform.rs` pins both arms'
//! restatements against the workload definition so a change to it cannot
//! re-specify one arm and not the other.

use apache_avro::types::Value as AvroValue;
use serde::Serialize;
use spate_clickhouse::{DateTime64Micros, DateTime64Millis};

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
// Output row. Field order IS the wire contract for RowBinary and Native, and
// must match `workload/clickhouse/ddl.sql` exactly.
// ---------------------------------------------------------------------------

/// The output row, matching `sensor_events`.
#[derive(Clone, Debug, Serialize)]
pub struct Row {
    /// Batch identifier.
    pub batch_id: u64,
    /// Event position within the batch.
    pub event_seq: u16,
    /// Sensor identifier.
    pub sensor: String,
    /// Region, with the Avro null coalesced to the empty string.
    pub region: String,
    /// ASCII-uppercased metric name.
    pub name_upper: String,
    /// Metric unit.
    pub unit: String,
    /// Metric value.
    pub value: i64,
    /// Derived scaled value.
    pub value_scaled: i64,
    /// Metric quality.
    pub quality: Option<f64>,
    /// Tags.
    pub tags: Vec<String>,
    /// Event timestamp.
    pub batch_ts: DateTime64Millis,
    /// Producer's intended send time. The Native leaf writer does no
    /// `DateTime64` rescaling, so this wrapper is what keeps the value out of
    /// 1970.
    pub send_ts: DateTime64Micros,
}

// ---------------------------------------------------------------------------
// Flatten — the pipeline logic
// ---------------------------------------------------------------------------
//
// This lives in the library rather than in the SUT binary for a concrete
// reason: the rigs are declared `test = false`, so a `#[cfg(test)]` module
// inside a bin is never compiled or run. Tests placed there would be silently
// dead, and this is logic whose correctness the published numbers depend on.

/// `SensorBatch` field order as declared in the committed schema. Test-only:
/// this is the expectation that
/// `avro_field_order_matches_the_positional_constants` checks, which is what
/// licenses the positional indexing in [`flatten_value`].
#[cfg(test)]
const BATCH_FIELDS: [&str; 6] = [
    "batch_id",
    "sensor",
    "region",
    "batch_ts_ms",
    "send_ts_us",
    "events",
];

/// `Event` field order as declared in the committed schema. Test-only; see
/// [`BATCH_FIELDS`].
#[cfg(test)]
const EVENT_FIELDS: [&str; 6] = ["seq", "name", "unit", "value", "quality", "tags"];

fn as_record(v: &AvroValue) -> &[(String, AvroValue)] {
    match v {
        AvroValue::Record(fields) => fields,
        other => panic!("expected an Avro record, got {other:?}"),
    }
}

fn as_long(v: &AvroValue) -> i64 {
    match v {
        AvroValue::Long(n) => *n,
        AvroValue::Int(n) => i64::from(*n),
        other => panic!("expected an Avro long, got {other:?}"),
    }
}

fn as_str(v: &AvroValue) -> &str {
    match v {
        AvroValue::String(s) => s,
        other => panic!("expected an Avro string, got {other:?}"),
    }
}

/// Unwrap a `["null", T]` union to its present branch, or `None`.
fn as_union(v: &AvroValue) -> Option<&AvroValue> {
    match v {
        AvroValue::Union(_, inner) => match inner.as_ref() {
            AvroValue::Null => None,
            present => Some(present),
        },
        AvroValue::Null => None,
        other => Some(other),
    }
}

fn as_tags(v: &AvroValue) -> Vec<String> {
    match v {
        AvroValue::Array(items) => items.iter().map(|t| as_str(t).to_owned()).collect(),
        other => panic!("expected an Avro array, got {other:?}"),
    }
}

/// Flatten a decoded batch into output rows, applying the specified filters and
/// derivations.
///
/// Fields are addressed **positionally**, which is safe because the schema is
/// committed and `avro_field_order_matches_the_positional_constants` proves the
/// decoder yields that order. A name lookup per event would cost six string
/// comparisons on every event for no correctness gain.
pub fn flatten_value<F: FnMut(Row)>(v: &AvroValue, mut emit: F) {
    let rec = as_record(v);
    let batch_id = u64::try_from(as_long(&rec[0].1)).expect("batch_id non-negative");
    let sensor = as_str(&rec[1].1);
    let region = as_union(&rec[2].1).map_or_else(String::new, |r| as_str(r).to_owned());
    let batch_ts_ms = as_long(&rec[3].1);
    let send_ts_us = as_long(&rec[4].1);
    let AvroValue::Array(events) = &rec[5].1 else {
        panic!("events is not an array")
    };
    for ev in events {
        let e = as_record(ev);
        let unit = as_str(&e[2].1);
        if unit == DROP_UNIT {
            continue;
        }
        let quality = as_union(&e[4].1).map(|q| match q {
            AvroValue::Double(d) => *d,
            other => panic!("expected an Avro double, got {other:?}"),
        });
        if matches!(quality, Some(q) if q < QUALITY_FLOOR) {
            continue;
        }
        let seq_raw = as_long(&e[0].1);
        let seq = u32::try_from(seq_raw).expect("seq non-negative");
        let value = as_long(&e[3].1);
        emit(Row {
            batch_id,
            event_seq: u16::try_from(seq_raw).expect("seq fits u16"),
            sensor: sensor.to_owned(),
            region: region.clone(),
            name_upper: ascii_upper(as_str(&e[1].1)),
            unit: unit.to_owned(),
            value,
            value_scaled: value_scaled(value, seq),
            quality,
            tags: as_tags(&e[5].1),
            batch_ts: DateTime64Millis(batch_ts_ms),
            send_ts: DateTime64Micros(send_ts_us),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spate_benchmark_harness::corpus::{encode_batch, expected, region_of, schema};

    fn decode(batch_id: u64, send_ts_us: i64) -> AvroValue {
        let datum = encode_batch(batch_id, send_ts_us);
        #[expect(deprecated, reason = "a test fixture decodes with the free function")]
        let value =
            apache_avro::from_avro_datum(schema(), &mut datum.as_slice(), None).expect("decode");
        value
    }

    /// The positional indexing in `flatten_value` is only safe if the decoder
    /// yields fields in the schema's declared order. Prove it rather than trust
    /// it — a silent reordering would mis-assign every column.
    #[test]
    fn avro_field_order_matches_the_positional_constants() {
        let v = decode(7, 1);
        let rec = as_record(&v);
        let names: Vec<&str> = rec.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, BATCH_FIELDS, "SensorBatch field order");

        let AvroValue::Array(events) = &rec[5].1 else {
            panic!("events is not an array")
        };
        let ev_names: Vec<&str> = as_record(&events[0])
            .iter()
            .map(|(n, _)| n.as_str())
            .collect();
        assert_eq!(ev_names, EVENT_FIELDS, "Event field order");
    }

    /// The flatten must agree with `expected()`, which is what the correctness
    /// gate compares ClickHouse against. If they disagreed, a correct arm would
    /// fail the gate or a broken one would pass it.
    #[test]
    fn flatten_agrees_with_the_gate_expectations() {
        let batches = 300u64;
        let mut rows = 0u64;
        let (mut sum, mut scaled) = (0i128, 0i128);
        for batch_id in 0..batches {
            let v = decode(batch_id, 0);
            flatten_value(&v, |r| {
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
        let v = decode(10, 0);
        let mut seen = Vec::new();
        flatten_value(&v, |r| seen.push(r.region.clone()));
        assert!(!seen.is_empty(), "batch 10 must yield surviving rows");
        assert!(seen.iter().all(String::is_empty), "null region coalesces");
    }
}
