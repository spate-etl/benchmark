//! The deterministic corpus for the cross-framework comparison.
//!
//! Every framework in the comparison receives byte-identical input, and the
//! correctness gates have to be able to say more than "the same number of rows
//! arrived". Both properties come from the same place: the corpus is a **pure
//! function of `batch_id`**.
//!
//! That buys three things a random or timestamp-seeded generator could not:
//!
//! * The expected row count, the expected checksum, and the expected
//!   filtered counts are all computable without reading what any framework
//!   produced — so a framework that silently transforms wrongly fails the gate
//!   just as loudly as one that drops rows.
//! * `(batch_id, seq)` is a true row identity, which makes
//!   `uniqExact((batch_id, event_seq))` an exact loss count and
//!   `count() - uniqExact(...)` an exact duplicate count.
//! * A prefilled topic can be regenerated identically months later, on a
//!   different machine, to re-run a published arm.
//!
//! The field derivations here are the normative ones from
//! `benchmarks/comparisons/README.md`. If the two ever disagree, this file is
//! wrong — the README is what the competitor implementations were written
//! against.
//!
//! One deliberate property worth stating: `value` is always non-negative
//! (a `%` of a positive modulus over unsigned arithmetic). That removes a real
//! cross-language hazard from the transform — integer division truncates toward
//! zero in Rust, Java and ClickHouse alike for non-negative operands, so
//! `value * 1000 / (event_seq + 1)` cannot disagree between implementations the
//! way it could if the sign varied.
//!
//! # The corpus-version markers
//!
//! Two marker comments below delimit the part of this file that determines a
//! byte of the corpus: the field derivations, the transform definitions,
//! the Avro encoding, the Confluent framing and the prefill timestamp.
//! `harness/build.rs` hashes exactly that region — comments stripped, whitespace
//! collapsed — into `DATASET_VERSION`.
//!
//! It is hashed at all because the arithmetic used not to be. The version was
//! derived from `workload.toml`, the `.avsc` and the DDL only, so changing
//! `1_000_003` to `1_000_033` in this file changed every `value` in the corpus
//! while `DATASET_VERSION` stood still — and post-change records would have been
//! medianed together with pre-change ones under one comparability group.
//!
//! It is a marked region rather than the whole file because the rest of this
//! module is the Kafka producer, the prefill loop and the correctness gates, none
//! of which change what the data *is*. Hashing those would re-version a
//! byte-identical corpus every time `linger.ms` was retuned, which is the
//! opposite failure and just as expensive: it splits published records from the
//! tree for a change that moved no byte.

#![expect(
    deprecated,
    reason = "apache-avro 0.22 deprecates the datum free functions; the corpus \
              generator and its tests call them directly"
)]

use apache_avro::types::Value as AvroValue;
use apache_avro::{Schema, to_avro_datum};
use serde::Deserialize;
use std::sync::OnceLock;

/// The one Avro schema, read from the file the competitor implementations also
/// read. Embedded so a rig cannot drift from the registered subject.
pub const SCHEMA_JSON: &str = include_str!("../../workload/schema/sensor_batch.avsc");

// The generator's tunables are NOT written here. They live in
// `workload/workload.toml` and are emitted as constants by `harness/build.rs`,
// which also hashes that file into `DATASET_VERSION`.
//
// The indirection buys one specific guarantee: a change to what the data *is*
// cannot be made without the corpus version moving, so two result sets produced
// from different corpora can never be silently placed on the same axis. Writing
// the constants in both places would let them drift, and a drifted corpus
// constant is invisible until two published numbers disagree for no stated
// reason.
//
// The reasoning behind each value lives beside it in workload.toml.
include!(concat!(env!("OUT_DIR"), "/workload_consts.rs"));

/// The parsed schema, compiled once per process.
///
/// # Panics
/// If the committed `.avsc` does not parse — which would mean the file every
/// framework reads is invalid, and no arm could be trusted.
pub fn schema() -> &'static Schema {
    static SCHEMA: OnceLock<Schema> = OnceLock::new();
    SCHEMA
        .get_or_init(|| Schema::parse_str(SCHEMA_JSON).expect("committed sensor_batch.avsc parses"))
}

// dataset-version:begin
// ---------------------------------------------------------------------------
// Field derivations — the single source of truth for producer and gates alike.
// ---------------------------------------------------------------------------

/// `sensor` for a batch.
#[must_use]
pub fn sensor_of(batch_id: u64) -> String {
    format!("sensor-{}", batch_id % SENSORS)
}

/// `region` for a batch: null one batch in ten, which is what forces every
/// implementation through a real union-decode path.
#[must_use]
pub fn region_of(batch_id: u64) -> Option<String> {
    if batch_id.is_multiple_of(10) {
        None
    } else {
        Some(format!("region-{}", batch_id % 7))
    }
}

/// Event timestamp for a batch, epoch milliseconds.
#[must_use]
pub fn batch_ts_ms_of(batch_id: u64) -> i64 {
    BASE_TS_MS + i64::try_from(batch_id).expect("batch_id fits i64")
}

/// `name` for an event.
#[must_use]
pub fn name_of(batch_id: u64, seq: u32) -> String {
    format!("metric_{}", (batch_id * 31 + u64::from(seq)) % NAMES)
}

/// `unit` for an event.
#[must_use]
pub fn unit_of(batch_id: u64, seq: u32) -> &'static str {
    UNITS[usize::try_from((batch_id * 7 + u64::from(seq)) % 8).expect("index fits usize")]
}

/// `value` for an event. Always non-negative — see the module docs.
#[must_use]
pub fn value_of(batch_id: u64, seq: u32) -> i64 {
    let v = (batch_id.wrapping_mul(1_000_003) + u64::from(seq) * 97) % 2_147_483_647;
    i64::try_from(v).expect("value below 2^31")
}

/// `quality` for an event: null one event in five.
#[must_use]
pub fn quality_of(batch_id: u64, seq: u32) -> Option<f64> {
    let s = u64::from(seq);
    if (batch_id + s).is_multiple_of(5) {
        None
    } else {
        #[expect(
            clippy::cast_precision_loss,
            reason = "the numerator is a residue mod 100, exactly representable"
        )]
        Some(((batch_id * 13 + s * 7) % 100) as f64 / 100.0)
    }
}

/// `tags` for an event: 0..=3 elements, the second nesting level.
#[must_use]
pub fn tags_of(batch_id: u64, seq: u32) -> Vec<String> {
    let s = u64::from(seq);
    (0..((batch_id + s) % 4))
        .map(|j| format!("tag-{}", (batch_id + s + j) % TAGS))
        .collect()
}

/// ASCII-only uppercase, the `name_upper` derivation.
///
/// ASCII-only is specified rather than incidental: Java's
/// `String.toUpperCase()` is locale-dependent, so an unqualified "uppercase"
/// would not be the same operation in every implementation.
#[must_use]
pub fn ascii_upper(s: &str) -> String {
    s.to_ascii_uppercase()
}

/// The `value_scaled` derivation.
#[must_use]
pub fn value_scaled_of(value: i64, seq: u32) -> i64 {
    value * 1000 / i64::from(seq + 1)
}

/// Whether the workload keeps this event: the unit sentinel and the quality
/// floor are the two filter predicates every arm must apply.
#[must_use]
pub fn keeps(batch_id: u64, seq: u32) -> bool {
    if unit_of(batch_id, seq) == DROP_UNIT {
        return false;
    }
    !matches!(quality_of(batch_id, seq), Some(q) if q < QUALITY_FLOOR)
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

/// Encode one batch as a bare Avro datum.
///
/// `send_ts_us` is supplied by the caller because it is the producer's
/// **intended** schedule time, not a property of `batch_id` — it is the one
/// field that legitimately varies between a prefill and a live run.
///
/// # Panics
/// If the datum cannot be encoded against the committed schema.
#[must_use]
pub fn encode_batch(batch_id: u64, send_ts_us: i64) -> Vec<u8> {
    let sensor = sensor_of(batch_id);
    let region = match region_of(batch_id) {
        // Branch indices follow the schema's declared union order,
        // `["null","string"]`.
        None => AvroValue::Union(0, Box::new(AvroValue::Null)),
        Some(r) => AvroValue::Union(1, Box::new(AvroValue::String(r))),
    };
    let events = (0..EVENTS_PER_BATCH)
        .map(|seq| {
            let quality = match quality_of(batch_id, seq) {
                None => AvroValue::Union(0, Box::new(AvroValue::Null)),
                Some(q) => AvroValue::Union(1, Box::new(AvroValue::Double(q))),
            };
            let tags = AvroValue::Array(
                tags_of(batch_id, seq)
                    .into_iter()
                    .map(AvroValue::String)
                    .collect(),
            );
            AvroValue::Record(vec![
                (
                    "seq".to_owned(),
                    AvroValue::Int(i32::try_from(seq).expect("seq fits i32")),
                ),
                ("name".to_owned(), AvroValue::String(name_of(batch_id, seq))),
                (
                    "unit".to_owned(),
                    AvroValue::String(unit_of(batch_id, seq).to_owned()),
                ),
                ("value".to_owned(), AvroValue::Long(value_of(batch_id, seq))),
                ("quality".to_owned(), quality),
                ("tags".to_owned(), tags),
            ])
        })
        .collect();

    let record = AvroValue::Record(vec![
        (
            "batch_id".to_owned(),
            AvroValue::Long(i64::try_from(batch_id).expect("batch_id fits i64")),
        ),
        ("sensor".to_owned(), AvroValue::String(sensor)),
        ("region".to_owned(), region),
        (
            "batch_ts_ms".to_owned(),
            AvroValue::Long(batch_ts_ms_of(batch_id)),
        ),
        ("send_ts_us".to_owned(), AvroValue::Long(send_ts_us)),
        ("events".to_owned(), AvroValue::Array(events)),
    ]);

    to_avro_datum(schema(), record).expect("encode sensor batch datum")
}

/// Wrap a datum in Confluent wire format: `0x00`, big-endian u32 schema id,
/// then the datum.
///
/// Confluent framing is used for every arm because three of the five
/// competitors effectively require a registry, and the lookup is cached so it
/// costs nothing at steady state.
#[must_use]
pub fn frame_confluent(schema_id: u32, datum: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(5 + datum.len());
    framed.push(0);
    framed.extend_from_slice(&schema_id.to_be_bytes());
    framed.extend_from_slice(datum);
    framed
}

/// `send_ts_us` for a **prefilled** batch: derived from `batch_id`, not the
/// clock.
///
/// This is deliberate. A prefilled corpus is replayed by every arm from offset
/// 0, so deriving the timestamp keeps the corpus byte-identical across
/// re-prefills and makes a published arm reproducible months later. The cost is
/// that drain-mode latency is meaningless by construction — the difference
/// between `ingest_ts` and this value is backlog age, not pipeline latency —
/// which is why drain mode reports throughput only. Sustained mode uses real
/// intended-schedule timestamps instead.
///
/// It sits inside the corpus-version region rather than beside the prefill loop
/// because it is one of the numbers written into the bytes on the topic: moving
/// it produces a different corpus under an unchanged `DATASET_VERSION` unless the
/// version covers it.
#[must_use]
pub fn send_ts_us_prefill(batch_id: u64) -> i64 {
    BASE_TS_MS * 1000 + i64::try_from(batch_id).expect("batch_id fits i64")
}
// dataset-version:end

// ---------------------------------------------------------------------------
// The sustained schedule
//
// Deliberately OUTSIDE the corpus-version region above. Everything inside that
// region is a function of `batch_id` and is therefore a byte of a reproducible
// corpus. These two are readings of a wall clock: they have no closed form, they
// are the one field `expected_range` declines to check, and moving them inside
// would re-version the dataset for a value the dataset does not contain.
// ---------------------------------------------------------------------------

/// Microseconds after a sustained producer's origin at which the message with
/// global index `n` is **due**.
///
/// A schedule fixed in advance from the target rate, and never `sleep(1/rate)`
/// in a loop. The two are not the same function. A per-iteration sleep adds its
/// own overhead to every gap, so the offered rate sags below the requested one
/// by an amount nobody measured — and the arm is then credited with keeping up
/// with a load that was never offered, which is the flattering direction.
///
/// Integer division truncates, so the schedule runs at most one microsecond per
/// message ahead of the exact one and never behind it. The multiplication
/// overflows `u64` at roughly 1.8e13 messages, which at any rate this harness
/// can offer is hundreds of thousands of years of producing.
#[must_use]
pub fn sustained_due_us(n: u64, rate: u64) -> u64 {
    (n * 1_000_000) / rate
}

/// `send_ts_us` for a **sustained** message: the time it was due, not the time
/// it went.
///
/// # Do not "fix" this into `now()`
///
/// This is the coordinated-omission correction, and it is the line in this
/// harness most likely to be tidied into incorrectness by someone who reads it
/// as a stale timestamp being carried around for no reason.
///
/// `ingest_ts - send_ts` is the published latency. With the **scheduled** time
/// here, a message the producer could not send on time — because the broker
/// pushed back, or because the host was busy — is charged for the whole of its
/// wait, so the pipeline is billed for time a row spent queued behind a producer
/// that had fallen behind. With `now()` taken at the moment of the send, that
/// wait disappears: the clock restarts exactly when the system is failing, and
/// the reported distribution *improves* as the pipeline gets worse. A benchmark
/// that stamps at actual send time publishes a flattering tail precisely under
/// the load a reader cares about, which is the most common way a latency number
/// lies.
///
/// The signature is what keeps that true. It takes the due **offset** rather
/// than the index and the rate, so the number that paced the send is literally
/// the number that is stamped: [`sustained_due_us`] is called once per message
/// and its result is used twice. Passing `(n, rate)` here instead would put a
/// second call in the file for a later edit to drift, and two clocks that are
/// meant to agree are the same defect wearing better clothes.
#[must_use]
pub fn send_ts_us_sustained(origin_epoch_us: i64, due_us: u64) -> i64 {
    origin_epoch_us + i64::try_from(due_us).expect("schedule offset fits i64")
}

// ---------------------------------------------------------------------------
// Decode targets
// ---------------------------------------------------------------------------

/// A decoded message. Field names match the Avro schema.
#[derive(Clone, Debug, Deserialize)]
pub struct SensorBatch {
    /// Dense, monotonic batch identifier; half of the row identity.
    pub batch_id: i64,
    /// Sensor identifier.
    pub sensor: String,
    /// Nullable region — the union the decode path must handle.
    pub region: Option<String>,
    /// Event timestamp, epoch milliseconds.
    pub batch_ts_ms: i64,
    /// Producer's intended send time, epoch microseconds.
    pub send_ts_us: i64,
    /// The events to fan out.
    pub events: Vec<Event>,
}

/// One event inside a [`SensorBatch`].
#[derive(Clone, Debug, Deserialize)]
pub struct Event {
    /// Position within the batch; the other half of the row identity.
    pub seq: i32,
    /// Metric name.
    pub name: String,
    /// Metric unit; `"drop"` is the filter sentinel.
    pub unit: String,
    /// Metric value.
    pub value: i64,
    /// Nullable quality — the second union.
    pub quality: Option<f64>,
    /// Inner array-of-string.
    pub tags: Vec<String>,
}

/// The target table every arm writes.
pub const TABLE: &str = "sensor_events";

/// The target columns, positional — used to build the Native schema and the
/// sink `columns` list from one definition. The order is the RowBinary and
/// Native wire contract.
pub const COLUMNS: &[(&str, &str)] = &[
    ("batch_id", "UInt64"),
    ("event_seq", "UInt16"),
    ("sensor", "LowCardinality(String)"),
    ("region", "LowCardinality(String)"),
    ("name_upper", "LowCardinality(String)"),
    ("unit", "LowCardinality(String)"),
    ("value", "Int64"),
    ("value_scaled", "Int64"),
    ("quality", "Nullable(Float64)"),
    ("tags", "Array(LowCardinality(String))"),
    ("batch_ts", "DateTime64(3)"),
    ("send_ts", "DateTime64(6)"),
];

// ---------------------------------------------------------------------------
// Expectations for the correctness gates
// ---------------------------------------------------------------------------

/// A checksum of one short ASCII string, reproducible byte-for-byte in Rust and
/// in ClickHouse.
///
/// The gate has to compare a string column against a closed form, and it has to
/// do it with `sum()`. An exact-distinct over string *values* is the shape of
/// query that asked ClickHouse for 10.45 GiB against a 10.8 GiB limit and was
/// killed, so every string column becomes an integer before it is aggregated.
///
/// The integer has to be one both sides compute identically, which rules out the
/// obvious choice. ClickHouse ships `CRC32`, `CRC32IEEE`, `CRC64` and
/// `cityHash64`, all stronger than this — and every one of them would have to be
/// reimplemented here byte-exactly, against a server whose variant is not
/// pinned by anything in this repository. A gate that fails an honest arm
/// because our CRC polynomial disagrees with the server's is worse than a weaker
/// gate that never does. `reinterpretAsUInt64` has one documented behaviour:
/// first eight bytes, little-endian, zero-padded when short and truncated when
/// long.
///
/// Head **and** reversed head, added, because the head alone stops at eight
/// bytes and `sensor-1023` is eleven. The two together read the first and last
/// eight bytes of the string, which is every byte of anything up to sixteen —
/// and the longest string here, a three-element tag concatenation, is eighteen.
/// So the fingerprint is *not* injective in general. It is injective over the
/// 1116 strings this corpus can emit at the committed generator constants, and
/// `the_string_fingerprint_separates_every_string_the_corpus_can_produce`
/// asserts that rather than assuming it — which is what would fail if a constant
/// grew the alphabet past what sixteen bytes can separate.
///
/// Byte reversal, not character reversal — ClickHouse's `reverse` is byte-wise.
/// The two coincide only because every string the generator emits is ASCII,
/// which is a property of the generator and is likewise tested.
#[must_use]
pub fn str_fingerprint(s: &str) -> i128 {
    let raw = s.as_bytes();

    let mut head = [0u8; 8];
    let n = raw.len().min(8);
    head[..n].copy_from_slice(&raw[..n]);

    let mut tail = [0u8; 8];
    for (slot, byte) in tail.iter_mut().zip(raw.iter().rev()) {
        *slot = *byte;
    }

    i128::from(u64::from_le_bytes(head)) + i128::from(u64::from_le_bytes(tail))
}

/// What a correct arm must have produced.
///
/// Every field is both a closed form over `batch_id` and a single ClickHouse
/// aggregate, and that pairing is the design constraint rather than a
/// coincidence: the gate must be one query over one bounded pass, because the
/// unbounded version of this check is what exhausted ClickHouse's memory and
/// took a completed, valid measurement down with it.
///
/// The three original fields — rows, `value` and `value_scaled` — proved that
/// two arms moved the same rows and did the same *integer* arithmetic, and
/// nothing else. `name_upper`, `tags`, `region`, `sensor`, `unit`, `quality` and
/// the timestamps were all unchecked, and the gap was cheaply exploitable: an
/// arm emitting `tags = []` skips the `Array(LowCardinality(String))` encode on
/// every one of 150,000,000 rows — a large, real speed-up — and passed every
/// gate. So did an arm that dropped the null-`region` coalesce, and so did the
/// exact regression `ddl.sql` warns about, losing the `DateTime64` scaling so
/// that "every value silently lands in 1970".
///
/// The gap was also asymmetric, which is why it mattered more than it looks. The
/// Spate arm imports `ascii_upper` and `value_scaled_of` from this module, so
/// the vendor's arm and the oracle move together by construction, while Flink
/// reimplements them independently. An unchecked column is therefore a free pass
/// aimed squarely at the arm we have the most reason not to flatter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Expected {
    /// Distinct `(batch_id, event_seq)` rows. Anything less is data loss.
    pub rows: u64,
    /// Sum of `value` over distinct rows. Computed as `i128` because a large
    /// corpus would overflow the `Int64` that ClickHouse's `sum` would
    /// otherwise return — the gate query casts to `Int128` to match.
    pub value_sum: i128,
    /// Sum of `value_scaled` over distinct rows.
    pub value_scaled_sum: i128,
    /// Sum of [`str_fingerprint`] over the `sensor` column.
    pub sensor_sum: i128,
    /// Sum of [`str_fingerprint`] over the `region` column, with a null region
    /// fingerprinted as the empty string — which is what the specified coalesce
    /// must have put in the non-nullable target column.
    pub region_sum: i128,
    /// Sum of [`str_fingerprint`] over the `name_upper` column.
    pub name_sum: i128,
    /// Sum of [`str_fingerprint`] over the `unit` column.
    pub unit_sum: i128,
    /// Sum of `length(tags)`. Separate from [`Expected::tag_sum`] so that an arm
    /// which drops the array entirely and one which fills it with the wrong
    /// strings produce different failure messages.
    pub tag_count_sum: i128,
    /// Sum of [`str_fingerprint`] over the tag elements concatenated with no
    /// separator. Zero for an arm that emits `tags = []` on every row, because
    /// the fingerprint of the empty string is zero.
    pub tag_sum: i128,
    /// Sum of `batch_ts` in epoch milliseconds. This is the `DateTime64`
    /// scaling check: an arm that writes seconds, or writes milliseconds into a
    /// micros column, lands near 1970 and misses by orders of magnitude.
    pub batch_ts_sum: i128,
    /// Rows whose `quality` is null. The second Avro union, and the only part of
    /// `quality` a closed form can pin — see [`expected_range`].
    pub null_quality_rows: u64,
}

/// Compute what `batches` messages must yield.
///
/// Deliberately a loop over the generator rather than a closed form: the point
/// of the gate is to catch a transform that disagrees with the specification,
/// and a closed form derived from the same misreading of the spec would agree
/// with the bug.
#[must_use]
pub fn expected(batches: u64) -> Expected {
    expected_range(0, batches)
}

/// How many rows `batches` messages must yield, and nothing else.
///
/// [`expected`] is the gate's oracle and formats a string per string column per
/// row to get there. That is the right price for a gate — under a second over
/// the bounded window, paid once per arm after the measurement has finished —
/// and the wrong price for the callers that want a row count and ask for it over
/// the **whole** corpus. At 1,500,000 batches each of those would spend roughly
/// fifteen seconds computing ten fields in order to read one.
///
/// The duplicated loop is what that costs, and
/// `the_cheap_row_count_agrees_with_the_full_expectation` is what stops it
/// drifting into a second, disagreeing definition of which rows the workload
/// keeps.
#[must_use]
pub fn expected_rows(batches: u64) -> u64 {
    let mut rows = 0;
    for batch_id in 0..batches {
        for seq in 0..EVENTS_PER_BATCH {
            if !keeps(batch_id, seq) {
                continue;
            }
            rows += 1;
        }
    }
    rows
}

/// Compute what batches `lo..hi` must yield.
///
/// Sustained mode needs this rather than [`expected`]: the producer runs
/// continuously and the consumer is stopped mid-stream, so the range that
/// actually landed is some `[min(batch_id), max(batch_id)]` window rather than a
/// prefix starting at zero. Gating against a prefix would either fail a correct
/// arm or, worse, pass a broken one whose totals happened to coincide.
///
/// # What this deliberately does not cover
///
/// * **`quality`'s values.** They are `f64`, and a sum of floats depends on the
///   order the server happened to add them in, so an exact comparison would fail
///   correct arms at random. Only the null pattern is pinned, which is the part
///   that proves the second union was decoded rather than flattened away.
/// * **`send_ts`'s value.** In sustained mode it is the producer's intended
///   schedule time, a property of the clock and not of `batch_id`, so no closed
///   form exists for it in the mode that matters. [`run_gates`] checks instead
///   that it is not *before* `BASE_TS_MS`, which is true in both modes and which
///   the `DateTime64` regression violates.
///
/// # Cost
///
/// Covering the string columns means the loop now formats `name` and `tags` per
/// event rather than doing pure integer work, so it allocates where it used not
/// to. Measured on the reference host, the ten-million-row gate window costs
/// under a second in release. It is paid once per arm, after the measurement
/// window has closed, and it buys the only check that notices an arm skipping the
/// tags encode.
///
/// The obvious optimisation is to memoise each derivation on its own residue —
/// there are only 32 distinct names and 16 distinct tag concatenations. It is
/// deliberately not taken: the memo key would restate `(batch_id * 31 + seq) %
/// NAMES` outside `name_of`, which is the duplicated arithmetic this whole
/// approach exists to avoid, and a memo keyed on a misread modulus would agree
/// with a generator that misread it the same way.
#[must_use]
pub fn expected_range(lo: u64, hi: u64) -> Expected {
    let mut out = Expected {
        rows: 0,
        value_sum: 0,
        value_scaled_sum: 0,
        sensor_sum: 0,
        region_sum: 0,
        name_sum: 0,
        unit_sum: 0,
        tag_count_sum: 0,
        tag_sum: 0,
        batch_ts_sum: 0,
        null_quality_rows: 0,
    };
    for batch_id in lo..hi {
        // Hoisted out of the event loop because `sensor`, `region` and
        // `batch_ts` are properties of the *batch*. Recomputing them per event
        // would multiply their string formatting by EVENTS_PER_BATCH for no
        // extra coverage, and this loop already runs ten million times per gate.
        let sensor_fp = str_fingerprint(&sensor_of(batch_id));
        let region_fp = str_fingerprint(region_of(batch_id).as_deref().unwrap_or(""));
        let batch_ts = i128::from(batch_ts_ms_of(batch_id));

        for seq in 0..EVENTS_PER_BATCH {
            if !keeps(batch_id, seq) {
                continue;
            }
            let value = value_of(batch_id, seq);
            // The target column is `name_upper`. Derived through `ascii_upper`
            // rather than by upper-casing here, so the oracle and the
            // specification cannot drift apart.
            let name = ascii_upper(&name_of(batch_id, seq));
            let tags = tags_of(batch_id, seq);

            out.rows += 1;
            out.value_sum += i128::from(value);
            out.sensor_sum += sensor_fp;
            out.region_sum += region_fp;
            out.name_sum += str_fingerprint(&name);
            out.unit_sum += str_fingerprint(unit_of(batch_id, seq));
            out.tag_count_sum += i128::try_from(tags.len()).expect("tag count fits i128");
            out.tag_sum += str_fingerprint(&tags.concat());
            out.batch_ts_sum += batch_ts;
            if quality_of(batch_id, seq).is_none() {
                out.null_quality_rows += 1;
            }
            out.value_scaled_sum += i128::from(value_scaled_of(value, seq));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Target schema
// ---------------------------------------------------------------------------

/// The committed target DDL, applied verbatim so the driver and the competitor
/// implementations cannot disagree about the target tables.
pub const DDL: &str = include_str!("../../workload/clickhouse/ddl.sql");

/// Split [`DDL`] into executable statements.
///
/// Line comments are stripped **before** splitting on `;`, which is load-bearing
/// rather than tidy: the file documents the correctness-gate queries in trailing
/// `--` comments and those contain semicolons. Splitting first would try to
/// execute fragments of prose.
///
/// This lives in the library rather than in the driver binary because the rigs
/// are declared `test = false` — a `#[cfg(test)]` module inside a bin is never
/// compiled or run, so tests placed there would be silently dead.
#[must_use]
pub fn ddl_statements() -> Vec<String> {
    split_sql(DDL)
}

/// Split any committed SQL file into executable statements, the way
/// [`ddl_statements`] splits the workload DDL.
///
/// Public because an entrant's own `arm_sql`/`arm_teardown_sql` (the
/// `[clickhouse]` descriptor hook) is applied by the driver with exactly these
/// rules — comments stripped before the `;` split, so a documented gate query in
/// a trailing comment is prose rather than a statement fragment. Two splitters
/// would eventually disagree about precisely that.
///
/// The split is a character state machine rather than a line-wise
/// strip-then-split, and the quote-awareness is load-bearing for the
/// entrant-authored half of the contract. The workload DDL is ours and can be
/// written around a naive splitter; an arm's SQL is somebody else's, and
/// ClickHouse SQL routinely puts both split tokens inside string literals —
/// `splitByString('--', x)`, `WHERE unit != ';'`. A splitter that read those as
/// a comment opener and a statement boundary would execute mangled fragments of
/// a statement the entrant wrote correctly, and the failure would surface as a
/// server exception naming SQL that appears in no committed file.
///
/// Inside a single-quoted string literal, `--` and `;` are content. Both of
/// ClickHouse's escape forms are honoured — the doubled quote (`''`) and the
/// backslash (`\'`) — because either one, misread as a closing quote, silently
/// re-opens code where the entrant wrote data. Backtick- and double-quoted
/// identifiers are treated as quoted regions under the same rules, since an
/// identifier can legally contain either token too. Outside quotes, `--` starts
/// a comment that runs to end of line and `;` splits. Comment-only lines
/// disappear, statements are trimmed, and empty fragments are dropped — exactly
/// the behaviour the committed workload DDL has always relied on.
#[must_use]
pub fn split_sql(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    // The quote character currently open, if any. One slot is enough: quoted
    // regions in SQL cannot nest, they only close on their own delimiter.
    let mut quote: Option<char> = None;
    let mut chars = sql.chars().peekable();

    let mut flush = |buf: &mut String| {
        let stmt = buf.trim();
        if !stmt.is_empty() {
            out.push(stmt.to_owned());
        }
        buf.clear();
    };

    while let Some(c) = chars.next() {
        if let Some(q) = quote {
            buf.push(c);
            if c == '\\' {
                // Backslash escape: whatever follows is content, including a
                // quote character that would otherwise close the region.
                if let Some(&next) = chars.peek() {
                    buf.push(next);
                    chars.next();
                }
            } else if c == q {
                if chars.peek() == Some(&q) {
                    // The doubled-delimiter escape: two delimiters are one
                    // literal delimiter, and the region stays open.
                    buf.push(q);
                    chars.next();
                } else {
                    quote = None;
                }
            }
        } else {
            match c {
                '\'' | '"' | '`' => {
                    quote = Some(c);
                    buf.push(c);
                }
                '-' if chars.peek() == Some(&'-') => {
                    // A comment runs to end of line. The newline itself is
                    // kept, so a statement continued on the next line keeps
                    // the whitespace the comment replaced.
                    for rest in chars.by_ref() {
                        if rest == '\n' {
                            buf.push('\n');
                            break;
                        }
                    }
                }
                ';' => flush(&mut buf),
                _ => buf.push(c),
            }
        }
    }
    // An unterminated quote reaches here still open; the text is passed
    // through as-is and the server, not this splitter, reports the syntax
    // error — inventing a closing quote would execute SQL nobody wrote.
    flush(&mut buf);
    out
}

// ---------------------------------------------------------------------------
// Registry and prefill
// ---------------------------------------------------------------------------

/// The topic the corpus is produced to and every arm consumes from.
///
/// [`SUBJECT`] is derived from it, so the registry name and the topic cannot
/// drift apart.
pub const TOPIC: &str = "comparison-sensor-batches";

/// The registry subject: `<topic>-value`, Confluent's topic-name strategy.
///
/// Only Kafka Connect can be broken by this name, and it breaks by decoding
/// nothing at all rather than by decoding badly: its `AvroConverter` resolves
/// the schema *version*, which is a subject-scoped lookup, while Flink, spate
/// and ClickHouse's `AvroConfluent` resolve by the id in the Confluent frame
/// and Vector never contacts the registry. `subject_follows_the_topic_name_strategy`
/// holds the two in step, since no arm that resolves by id can catch a drift.
///
/// Editing it re-versions nothing: the subject determines no byte of the corpus
/// — the Confluent frame carries the schema *id* — so it sits outside the
/// `dataset-version` region.
pub const SUBJECT: &str = "comparison-sensor-batches-value";

/// Register the committed schema under [`SUBJECT`] and return its id.
///
/// Idempotent: re-registering identical schema text returns the existing id.
///
/// # Panics
/// If the registry rejects the schema or returns no id — every arm decodes
/// through this id, so a failure here invalidates the whole run rather than one
/// arm.
#[must_use]
pub fn register_schema(host: &str, port: u16) -> u32 {
    let body = serde_json::json!({ "schema": SCHEMA_JSON, "schemaType": "AVRO" }).to_string();
    let path = format!("/subjects/{SUBJECT}/versions");
    let resp = crate::http::post_typed(
        host,
        port,
        &path,
        Some("application/vnd.schemaregistry.v1+json"),
        &body,
    )
    .expect("schema registry POST");
    let parsed: serde_json::Value = serde_json::from_str(&resp)
        .unwrap_or_else(|e| panic!("schema registry returned non-JSON {resp:?}: {e}"));
    let id = parsed
        .get("id")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| panic!("schema registry response carries no id: {resp}"));
    u32::try_from(id).expect("schema id fits u32")
}

/// A live producer running open-loop at a fixed offered rate.
///
/// This is the sustained-mode load source, and it is the whole
/// coordinated-omission defence. Two properties do that work:
///
/// * **Sends are scheduled against a fixed origin**, `origin + n / rate`, never
///   `sleep(1/rate)` in a loop. A per-iteration sleep accumulates its own
///   overhead as drift, so the offered rate would silently sag below the
///   requested one and the arm would look better than it is.
/// * **`send_ts_us` carries the *intended* time, not the actual send time.** If
///   the producer falls behind — because the broker pushed back, or the host was
///   busy — that delay lands in the measured latency instead of vanishing from
///   it. Stamping `now()` at send is precisely the mistake that makes a
///   saturated system report excellent percentiles.
///
/// The caller is expected to reject the arm when [`LoadReport::achieved_share`]
/// falls materially below 1.0: at that point the producer, not the framework,
/// was the constraint, and the measurement is of the harness.
/// The generator is **multi-threaded**, and that is a requirement rather than an
/// optimisation. Measured on this host, one producer thread tops out near 73k
/// messages/s (~1.47M rows/s), at which point the framework under test was still
/// using only 1.05 of its 4 cores. A single-threaded generator would therefore
/// make every arm producer-bound, and the comparison would be between frameworks
/// that are all idling — the most expensive way to measure nothing.
///
/// Threads interleave by stride: thread `i` of `n` sends global indices
/// `i, i+n, i+2n, ...`. The global schedule is preserved exactly, because each
/// message's due time is derived from its **global** index, not from a per-thread
/// counter. `batch_id` therefore remains dense across the whole generator, which
/// the correctness gate's contiguity test depends on.
#[derive(Debug)]
pub struct SustainedLoad {
    handles: Vec<std::thread::JoinHandle<LoadReport>>,
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

/// Delivery-counting context for the sustained producer.
///
/// Without this the producer counts messages it *enqueued*, not messages the
/// broker *acknowledged* — and `BaseProducer` discards a failed delivery
/// silently. That is not a cosmetic difference: a handful of dropped sends puts
/// gaps in the middle of the `batch_id` sequence, and the correctness gate then
/// reports them as the framework losing rows. This was found exactly that way.
struct DeliveryCounter {
    delivered: std::sync::Arc<std::sync::atomic::AtomicU64>,
    failed: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl rdkafka::ClientContext for DeliveryCounter {}

impl rdkafka::producer::ProducerContext for DeliveryCounter {
    type DeliveryOpaque = ();
    fn delivery(&self, result: &rdkafka::producer::DeliveryResult<'_>, _: ()) {
        use std::sync::atomic::Ordering;
        match result {
            Ok(_) => {
                self.delivered.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// What a sustained producer actually managed to offer.
#[derive(Clone, Copy, Debug)]
pub struct LoadReport {
    /// Messages enqueued.
    pub sent: u64,
    /// Messages the broker acknowledged. Anything less than `sent` means the
    /// **harness** lost messages, not the framework.
    pub delivered: u64,
    /// Messages whose delivery failed.
    pub failed: u64,
    /// Wall seconds the producer ran.
    pub elapsed_s: f64,
    /// Requested offered rate, messages/s.
    pub target_rate: u64,
    /// Achieved rate as a fraction of the target. Below ~0.99 means the load
    /// generator was the bottleneck and the arm is not measuring the framework.
    pub achieved_share: f64,
    /// Largest amount by which any send ran behind its intended schedule.
    /// A large value with `achieved_share` near 1.0 means the producer caught
    /// up in bursts rather than tracking the schedule smoothly.
    pub max_schedule_lag_ms: f64,
}

impl SustainedLoad {
    /// Start producing `rate` messages/s with a **strictly monotonic**
    /// `batch_id` starting at `first_batch_id`.
    ///
    /// Monotonic, never cycling, and that is load-bearing for the correctness
    /// gates rather than incidental. `(batch_id, event_seq)` is the row identity:
    /// if the producer wrapped around a fixed corpus, repeated identities would
    /// be *expected*, and the gates could no longer distinguish a legitimate
    /// replay from a framework emitting duplicates — which is one of the few
    /// ways an arm can look fast for a dishonest reason.
    ///
    /// `first_batch_id` lets a later run continue past an earlier one on the same
    /// topic without colliding with rows already in the target table.
    ///
    /// # Panics
    /// If the producer cannot be created.
    #[must_use]
    pub fn start(
        bootstrap: &str,
        topic: &str,
        partitions: i32,
        schema_id: u32,
        rate: u64,
        first_batch_id: u64,
        threads: u64,
    ) -> Self {
        use rdkafka::config::ClientConfig;
        use rdkafka::producer::{BaseProducer, BaseRecord};
        use std::sync::atomic::AtomicBool;
        use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

        assert!(rate > 0, "sustained load needs a positive rate");
        assert!(threads > 0, "sustained load needs at least one thread");

        let stop = std::sync::Arc::new(AtomicBool::new(false));

        // ONE origin for every thread, read before any of them is spawned.
        //
        // Defect this closes: each thread took its own `Instant::now()` and its
        // own epoch reading *inside* the closure, so thread 7's timeline began
        // however long `thread::spawn` had taken to reach it. A message's due
        // time is derived from its GLOBAL index, and that only reconstructs one
        // aggregate schedule if every thread measures that index against the
        // same zero. It did not: adjacent `batch_id`s produced by different
        // threads carried `send_ts_us` values offset from each other by the
        // spawn skew, and because `send_ts` is one end of the published latency,
        // the skew landed in the measurement as noise nobody had put there.
        //
        // Sharing the origin also makes the lateness of a late-starting thread
        // *honest* rather than invisible: its first sends really were due before
        // it existed, that lateness is charged to latency like any other, and
        // `max_schedule_lag_ms` reports it instead of hiding it behind a private
        // clock.
        let origin = Instant::now();
        let origin_epoch_us = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_micros(),
        )
        .expect("epoch micros fit i64");

        let handles = (0..threads)
            .map(|slot| {
                let stop_thread = std::sync::Arc::clone(&stop);
                let (bootstrap, topic) = (bootstrap.to_owned(), topic.to_owned());

                std::thread::spawn(move || {
                    use std::sync::atomic::{AtomicU64, Ordering};
                    let delivered = std::sync::Arc::new(AtomicU64::new(0));
                    let failed = std::sync::Arc::new(AtomicU64::new(0));
                    let producer: BaseProducer<DeliveryCounter> = ClientConfig::new()
                        .set("bootstrap.servers", &bootstrap)
                        .set("linger.ms", "5")
                        .set("batch.size", "1048576")
                        // Retry a transient send rather than dropping it: a dropped
                        // message becomes a hole in the `batch_id` sequence that the
                        // correctness gate cannot distinguish from the framework losing
                        // a row. Idempotence keeps the retries from duplicating.
                        .set("enable.idempotence", "true")
                        .set("message.send.max.retries", "10")
                        .create_with_context(DeliveryCounter {
                            delivered: std::sync::Arc::clone(&delivered),
                            failed: std::sync::Arc::clone(&failed),
                        })
                        .expect("sustained producer");

                    let mut sent = 0u64;
                    let mut max_lag_us = 0i64;
                    while !stop_thread.load(Ordering::Relaxed) {
                        // This thread's `sent`-th message is global index
                        // `slot + sent * threads`. Deriving the due time from the GLOBAL
                        // index is what keeps the aggregate schedule exact: a per-thread
                        // counter would give each thread its own timeline and the offered
                        // rate would drift.
                        let global = slot + sent * threads;
                        let due_us = sustained_due_us(global, rate);
                        let elapsed_us =
                            u64::try_from(origin.elapsed().as_micros()).unwrap_or(u64::MAX);
                        if elapsed_us < due_us {
                            // Ahead of schedule: serve the client and wait, but never
                            // longer than the remaining gap.
                            let wait = Duration::from_micros((due_us - elapsed_us).min(2_000));
                            producer.poll(wait);
                            continue;
                        }
                        max_lag_us =
                            max_lag_us.max(i64::try_from(elapsed_us - due_us).unwrap_or(i64::MAX));

                        let batch_id = first_batch_id + global;
                        // The INTENDED schedule time, never `now()`, and `due_us`
                        // is the same value that decided this send was allowed to
                        // happen a few lines above. Control reaches here only when
                        // the producer is at or *behind* schedule, so the gap
                        // between `due_us` and the wall clock right now is real
                        // lateness — and stamping the schedule is what charges the
                        // published latency for it instead of restarting the clock
                        // at the moment the producer finally caught up.
                        //
                        // `send_ts_us_sustained` carries the argument in full. Read
                        // it before changing this line: a `now()` here is a one-word
                        // edit that makes every saturated arm report an excellent
                        // tail, and nothing else in the harness would notice.
                        let send_ts_us = send_ts_us_sustained(origin_epoch_us, due_us);
                        let payload =
                            frame_confluent(schema_id, &encode_batch(batch_id, send_ts_us));
                        let key = sensor_of(batch_id);
                        let partition = i32::try_from(
                            global % u64::try_from(partitions).expect("partitions > 0"),
                        )
                        .expect("partition fits i32");
                        match producer.send(
                            BaseRecord::to(&topic)
                                .partition(partition)
                                .key(&key)
                                .payload(&payload),
                        ) {
                            Ok(()) => sent += 1,
                            Err((e, _))
                                if e.rdkafka_error_code()
                                    == Some(rdkafka::types::RDKafkaErrorCode::QueueFull) =>
                            {
                                producer.poll(Duration::from_millis(1));
                            }
                            Err((e, _)) => panic!("sustained produce: {e}"),
                        }
                        if sent.is_multiple_of(4096) {
                            producer.poll(Duration::ZERO);
                        }
                    }
                    let elapsed_s = origin.elapsed().as_secs_f64();
                    use rdkafka::producer::Producer;
                    producer
                        .flush(Duration::from_secs(60))
                        .expect("flush sustained producer");
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "message counts stay far below f64's exact integer range"
                    )]
                    // This thread's share of the global target.
                    let achieved = sent as f64 / ((rate as f64 / threads as f64) * elapsed_s);
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "microsecond lag stays far below f64's exact integer range"
                    )]
                    let max_schedule_lag_ms = max_lag_us as f64 / 1000.0;
                    LoadReport {
                        sent,
                        delivered: delivered.load(Ordering::Relaxed),
                        failed: failed.load(Ordering::Relaxed),
                        elapsed_s,
                        target_rate: rate,
                        achieved_share: achieved,
                        max_schedule_lag_ms,
                    }
                })
            })
            .collect();

        Self { handles, stop }
    }

    /// Stop producing and collect what was actually offered, summed across
    /// threads.
    ///
    /// `achieved_share` is recomputed from the totals rather than averaged: one
    /// thread keeping up cannot compensate for another falling behind, and the
    /// question the gate asks is whether the *aggregate* offered rate hit its
    /// target. `max_schedule_lag_ms` is the worst lag any thread saw.
    ///
    /// # Panics
    /// If a producer thread panicked, or if there were no threads.
    #[must_use]
    pub fn stop(mut self) -> LoadReport {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let parts: Vec<LoadReport> = std::mem::take(&mut self.handles)
            .into_iter()
            .map(|h| h.join().expect("sustained producer thread"))
            .collect();
        assert!(!parts.is_empty(), "no producer threads");

        let sent: u64 = parts.iter().map(|p| p.sent).sum();
        let delivered: u64 = parts.iter().map(|p| p.delivered).sum();
        let failed: u64 = parts.iter().map(|p| p.failed).sum();
        let elapsed_s = parts.iter().map(|p| p.elapsed_s).fold(0.0_f64, f64::max);
        let target_rate = parts[0].target_rate;
        #[expect(
            clippy::cast_precision_loss,
            reason = "message counts stay far below f64's exact integer range"
        )]
        let achieved_share = sent as f64 / (target_rate as f64 * elapsed_s);
        LoadReport {
            sent,
            delivered,
            failed,
            elapsed_s,
            target_rate,
            achieved_share,
            max_schedule_lag_ms: parts
                .iter()
                .map(|p| p.max_schedule_lag_ms)
                .fold(0.0_f64, f64::max),
        }
    }
}

impl Drop for SustainedLoad {
    /// Defect this closes: there was no `Drop`, so every path that abandoned a
    /// sustained run — an arm whose container exited during warm-up, a row-count
    /// probe that failed five times running, a panic out of a ClickHouse
    /// assertion — dropped this value, detached its threads, and left a
    /// multi-threaded producer offering hundreds of thousands of messages a
    /// second at the *next* arm on a host `methodology/` documents as
    /// oversubscribed. Nothing about that is visible in the result: the next arm
    /// simply produces a slower number, and a slower number is what a benchmark
    /// is for. It is the same shape as the orphaned-sampler defect in
    /// `sampler::Sampler`, and it fails in the same direction.
    ///
    /// Idempotent with [`SustainedLoad::stop`], which takes the handles: a value
    /// that has already been stopped drops with nothing left to join.
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        for h in std::mem::take(&mut self.handles) {
            // The join result is discarded rather than unwrapped: this runs on
            // the unwind path of a panic that has already been diagnosed, and a
            // second panic from `Drop` during unwinding aborts the process,
            // taking the refusal message with it.
            let _ = h.join();
        }
    }
}

/// What one prefill produced.
#[derive(Clone, Copy, Debug)]
pub struct PrefillReport {
    /// Batches on the topic after this call.
    pub batches: u64,
    /// Total framed payload bytes produced (0 when the corpus was reused).
    pub bytes: u64,
    /// Wall seconds spent producing (0 when reused).
    pub elapsed_s: f64,
    /// Whether an existing corpus was reused rather than reproduced.
    pub reused: bool,
}

/// Count messages currently on `topic`, for callers outside this module.
///
/// Drain mode needs this to know how much work exists: a window that outlasts the
/// corpus measures an idle pipeline, not its throughput.
///
/// # Panics
/// If a consumer cannot be created.
#[must_use]
pub fn topic_message_count(bootstrap: &str, topic: &str, partitions: i32) -> u64 {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{Consumer, base_consumer::BaseConsumer};
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "comparison-depth-probe")
        .create()
        .expect("depth probe consumer");
    (0..partitions)
        .map(|p| {
            consumer
                .fetch_watermarks(topic, p, std::time::Duration::from_secs(10))
                .map_or(0, |(low, high)| u64::try_from(high - low).unwrap_or(0))
        })
        .sum()
}

/// Count messages currently on `topic` by summing per-partition watermarks.
fn topic_depth(producer: &rdkafka::producer::BaseProducer, topic: &str, partitions: i32) -> u64 {
    use rdkafka::producer::Producer;
    (0..partitions)
        .map(|p| {
            producer
                .client()
                .fetch_watermarks(topic, p, std::time::Duration::from_secs(10))
                .map_or(0, |(low, high)| u64::try_from(high - low).unwrap_or(0))
        })
        .sum()
}

/// Fill `topic` with `batches` Confluent-framed messages, or reuse what is
/// already there.
///
/// Reuse is keyed on the topic already holding exactly `batches` messages.
/// Because the corpus is a pure function of `batch_id` and `send_ts_us` is
/// derived, a topic of the right depth necessarily holds the right bytes — so
/// reuse is safe, and it saves re-producing gigabytes on every one of the ~50
/// arms in a sweep.
///
/// Partitions are assigned round-robin explicitly rather than by key hash: an
/// uneven partition distribution would penalise whichever arm is
/// partition-parallelism-bound, for reasons that have nothing to do with the
/// framework.
///
/// # Panics
/// If the producer cannot be created, a send fails for any reason other than a
/// full queue, or the final flush times out.
#[must_use]
pub fn prefill(
    bootstrap: &str,
    topic: &str,
    partitions: i32,
    batches: u64,
    schema_id: u32,
) -> PrefillReport {
    use rdkafka::config::ClientConfig;
    use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
    use std::time::{Duration, Instant};

    crate::kafka::ensure_topic(bootstrap, topic, partitions);
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("linger.ms", "20")
        .set("batch.size", "1048576")
        .set("compression.type", "none")
        .create()
        .expect("prefill producer");

    let depth = topic_depth(&producer, topic, partitions);
    if depth == batches {
        eprintln!("prefill: reusing {batches} existing messages on {topic}");
        return PrefillReport {
            batches,
            bytes: 0,
            elapsed_s: 0.0,
            reused: true,
        };
    }
    assert_eq!(
        depth, 0,
        "topic {topic} holds {depth} messages but the run wants {batches}. \
         A partially-filled corpus would make every arm replay different bytes; \
         delete the topic and re-prefill."
    );

    let start = Instant::now();
    let mut bytes = 0u64;
    for batch_id in 0..batches {
        let datum = encode_batch(batch_id, send_ts_us_prefill(batch_id));
        let payload = frame_confluent(schema_id, &datum);
        bytes += payload.len() as u64;
        let key = sensor_of(batch_id);
        let partition =
            i32::try_from(batch_id % u64::try_from(partitions).expect("partitions > 0"))
                .expect("partition fits i32");
        loop {
            match producer.send(
                BaseRecord::to(topic)
                    .partition(partition)
                    .key(&key)
                    .payload(&payload),
            ) {
                Ok(()) => break,
                Err((e, _))
                    if e.rdkafka_error_code()
                        == Some(rdkafka::types::RDKafkaErrorCode::QueueFull) =>
                {
                    producer.poll(Duration::from_millis(5));
                }
                Err((e, _)) => panic!("prefill produce: {e}"),
            }
        }
        if batch_id.is_multiple_of(4096) {
            producer.poll(Duration::ZERO);
        }
    }
    producer.flush(Duration::from_secs(300)).expect("flush");

    let landed = topic_depth(&producer, topic, partitions);
    assert_eq!(
        landed, batches,
        "prefill produced {batches} messages but the topic holds {landed}; \
         every arm would replay a different corpus"
    );
    PrefillReport {
        batches,
        bytes,
        elapsed_s: start.elapsed().as_secs_f64(),
        reused: false,
    }
}

// ---------------------------------------------------------------------------
// Correctness gates
// ---------------------------------------------------------------------------

/// Outcome of the correctness gates for one arm.
///
/// Every field is reported rather than collapsed into a boolean, because the
/// *reason* an arm failed is what tells us whether the framework dropped rows,
/// duplicated them, or transformed them wrongly.
#[derive(Clone, Copy, Debug)]
pub struct Gates {
    /// Lowest `batch_id` present in the table.
    pub min_batch: u64,
    /// Highest `batch_id` present in the table.
    pub max_batch: u64,
    /// Rows in the interior range.
    pub rows: u64,
    /// Distinct `(batch_id, event_seq)` pairs in the interior range.
    pub distinct_ids: u64,
    /// Distinct `batch_id`s in the interior range.
    pub distinct_batches: u64,
    /// `rows - distinct_ids`. Reported, never suppressed: these are all
    /// at-least-once systems and some duplication is legitimate.
    pub duplicates: u64,
    /// Whether the interior `batch_id`s form an unbroken run — the loss test.
    pub contiguous: bool,
    /// Whether the row count matches the generator's expectation.
    pub rows_match: bool,
    /// Whether `sum(value)` matches the generator's expectation.
    pub value_sum_match: bool,
    /// Whether `sum(value_scaled)` matches the generator's expectation.
    pub value_scaled_match: bool,
    /// Whether the `sensor` column's fingerprint sum matches.
    pub sensor_match: bool,
    /// Whether the `region` column's fingerprint sum matches — the null
    /// coalesce.
    pub region_match: bool,
    /// Whether the `name_upper` column's fingerprint sum matches — the
    /// ASCII uppercase.
    pub name_match: bool,
    /// Whether the `unit` column's fingerprint sum matches.
    pub unit_match: bool,
    /// Whether `sum(length(tags))` matches — the array's cardinality.
    pub tag_count_match: bool,
    /// Whether the tag elements' fingerprint sum matches — the array's content.
    pub tag_match: bool,
    /// Whether `sum(batch_ts)` in milliseconds matches — the `DateTime64`
    /// scaling.
    pub batch_ts_match: bool,
    /// Whether the count of null `quality` values matches — the second union.
    pub null_quality_match: bool,
    /// Whether the earliest `send_ts` is at or after `BASE_TS_MS`. A bound
    /// rather than a checksum, because sustained mode's `send_ts` is a clock
    /// reading and has no closed form; see [`expected_range`].
    pub send_ts_after_base: bool,
}

impl Gates {
    /// Whether this arm may be published.
    ///
    /// Duplicates deliberately do **not** fail the gate: at-least-once permits
    /// them, and they are published as a metric so a reader can judge. Loss and
    /// wrong arithmetic do fail it.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.contiguous
            && self.rows_match
            && self.value_sum_match
            && self.value_scaled_match
            && self.sensor_match
            && self.region_match
            && self.name_match
            && self.unit_match
            && self.tag_count_match
            && self.tag_match
            && self.batch_ts_match
            && self.null_quality_match
            && self.send_ts_after_base
    }

    /// Human-readable reason for a failure, for the driver's refusal message.
    ///
    /// Every check reports separately, and each message names the *regression*
    /// rather than the column. A gate that says only "correctness failed" sends
    /// whoever reads it back into the arm's source with nothing to look for,
    /// which on a sweep that has already spent an hour is the expensive kind of
    /// unhelpful.
    #[must_use]
    pub fn failure(&self) -> Option<String> {
        if self.passed() {
            return None;
        }
        let mut why = Vec::new();
        if !self.contiguous {
            why.push(format!(
                "batch_ids are not contiguous ({} distinct across [{}, {}]) — rows were LOST",
                self.distinct_batches, self.min_batch, self.max_batch
            ));
        }
        if !self.rows_match {
            why.push(format!(
                "row count {} disagrees with the generator",
                self.rows
            ));
        }
        if !self.value_sum_match {
            why.push("sum(value) disagrees with the generator — the arm did different work".into());
        }
        if !self.value_scaled_match {
            why.push("sum(value_scaled) disagrees — the value_scaled derivation is wrong".into());
        }
        if !self.sensor_match {
            why.push("the sensor column disagrees — the key was not carried through".into());
        }
        if !self.region_match {
            why.push(
                "the region column disagrees — the null-region coalesce is missing or wrong".into(),
            );
        }
        if !self.name_match {
            why.push(
                "the name_upper column disagrees — the ASCII uppercase was skipped or is \
                 locale-dependent"
                    .into(),
            );
        }
        if !self.unit_match {
            why.push("the unit column disagrees — the filter sentinel was not carried".into());
        }
        if !self.tag_count_match {
            why.push(
                "sum(length(tags)) disagrees — the arm did not emit the array it was given, and \
                 skipping the Array(LowCardinality(String)) encode is a large speed-up"
                    .into(),
            );
        }
        if !self.tag_match {
            why.push(
                "the tag elements disagree — the array is present but holds wrong values".into(),
            );
        }
        if !self.batch_ts_match {
            why.push(
                "sum(batch_ts) disagrees — the DateTime64 scaling is wrong, which is how every \
                 value silently lands in 1970"
                    .into(),
            );
        }
        if !self.null_quality_match {
            why.push(
                "the null-quality count disagrees — the nullable union was not decoded".into(),
            );
        }
        if !self.send_ts_after_base {
            why.push(
                "the earliest send_ts predates the corpus base timestamp — the DateTime64(6) \
                 column was written at the wrong scale"
                    .into(),
            );
        }
        Some(why.join("; "))
    }
}

/// The ClickHouse expression matching [`str_fingerprint`] for one string column.
///
/// The `CAST ... AS String` is not decoration. Every string column in the target
/// is `LowCardinality(String)` and `reinterpretAsUInt64` takes a `String`, so the
/// cast is what keeps the gate from dying on a type error at the single moment
/// it is most expensive to discover — after an arm has finished running.
fn fingerprint_sql(expr: &str) -> String {
    format!(
        "toInt128(reinterpretAsUInt64(CAST({expr} AS String))) + \
         toInt128(reinterpretAsUInt64(reverse(CAST({expr} AS String))))"
    )
}

/// Run the correctness gates against the target table.
///
/// **The first and last `batch_id` are excluded.** A sealed sink chunk can split
/// one message's rows across two batches, so at the instant the driver snapshots
/// the table, the boundary batches may be only partially landed. Gating over the
/// interior range removes that fence-post without weakening the test: any loss or
/// mis-transformation strictly inside the range still fails.
///
/// **The exact tests cover at most `max_batches` of the most recent range.**
/// `uniqExact` builds a hash set proportional to cardinality, and over a
/// saturated run's 229M rows that exhausted ClickHouse's memory limit outright —
/// so the exact gate is bounded, and the slice is taken from the top of the range
/// because that is the part produced during and after the measurement window.
/// The slice is still tens of millions of rows; a framework that drops,
/// duplicates or mis-transforms does so systematically, not once.
///
/// **Two queries, one scan each, and no unbounded hash set beyond the
/// `uniqExact` the bound already covers.** Everything the gate learns about the
/// string and array columns is learned by reducing each to a fixed-width scalar
/// before aggregating, never by collecting distinct values — see [`Expected`]
/// for what that buys and [`str_fingerprint`] for why the reduction is the one
/// it is.
///
/// # Errors
/// If the table cannot be queried, or ClickHouse returns unparseable output — a
/// gate that silently degrades to "passed" would be worse than no gate.
///
/// Returns rather than panicking because a sweep runs two dozen arms: a gate
/// that cannot execute should refuse *that arm* and let the queue continue, not
/// abort hours of work. This was a panic, and an out-of-memory gate query took
/// the whole sweep down with it.
pub fn run_gates(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    max_batches: u64,
) -> Result<Gates, String> {
    let sql = |q: &str| -> Result<Vec<String>, String> {
        let body = crate::docker::clickhouse_sql(host, port, user, password, q)
            .map_err(|e| format!("gate query failed ({q}): {e}"))?;
        Ok(body.trim().split(['\t', '\n']).map(str::to_owned).collect())
    };
    let num = |s: &str| -> Result<i128, String> {
        s.parse::<i128>()
            .map_err(|e| format!("gate expected a number, got {s:?}: {e}"))
    };

    let table = TABLE;
    let bounds = sql(&format!("SELECT min(batch_id), max(batch_id) FROM {table}"))?;
    if bounds.len() < 2 {
        return Err(format!("gate bounds query returned {bounds:?} for {table}"));
    }
    // An empty table yields 0/0 from min/max; treat it as a hard failure rather
    // than an empty-but-passing range.
    let (min_batch, max_batch) = (num(&bounds[0])? as u64, num(&bounds[1])? as u64);
    if max_batch <= min_batch + 1 {
        return Err(format!(
            "{table} holds too narrow a batch_id range ([{min_batch}, {max_batch}]) to \
             gate; the arm produced almost nothing"
        ));
    }
    // Bounded slice from the top of the range; see the doc comment.
    let hi = max_batch;
    let lo = (min_batch + 1).max(hi.saturating_sub(max_batches));

    // `min(send_ts)` rides along on the counting scan rather than the sum query
    // below, and deliberately so: it is idempotent under duplication, so it needs
    // no deduplication, and keeping it out of the DISTINCT projection keeps eight
    // bytes per row out of that query's hash set.
    let counts = sql(&format!(
        "SELECT count(), uniqExact((batch_id, event_seq)), uniqExact(batch_id), \
                min(toUnixTimestamp64Micro(send_ts)) FROM {table} \
         WHERE batch_id >= {lo} AND batch_id < {hi}"
    ))?;
    if counts.len() < 4 {
        return Err(format!("gate count query returned {counts:?}"));
    }
    let rows = num(&counts[0])? as u64;
    let distinct_ids = num(&counts[1])? as u64;
    let distinct_batches = num(&counts[2])? as u64;
    let min_send_ts_us = num(&counts[3])?;

    // The sums are taken over DEDUPLICATED rows, which is not a detail: these are
    // at-least-once systems, so a legitimate duplicate would otherwise inflate
    // `sum(value)` and fail a *correct* arm. Deduplicating on the full row is
    // sound because a replayed record re-encodes identically.
    //
    // The projection is entirely fixed-width scalars, and that is the load-bearing
    // property. Every string column is reduced to a fingerprint and `tags` to a
    // count plus a fingerprint *inside* the subquery, so the DISTINCT hash set
    // holds ~120 bytes per row — about 1.2 GiB over the ten-million-row window,
    // against the 10.8 GiB limit. Putting `tags` itself in the DISTINCT key would
    // have put an array of strings there and taken the query back to where the
    // unbounded gate died.
    //
    // `toInt128` because ClickHouse's `sum` over `Int64` returns `Int64`, which a
    // large corpus would overflow silently.
    let sensor_fp = fingerprint_sql("sensor");
    let region_fp = fingerprint_sql("region");
    let name_fp = fingerprint_sql("name_upper");
    let unit_fp = fingerprint_sql("unit");
    let tag_fp = fingerprint_sql("arrayStringConcat(CAST(tags AS Array(String)), '')");

    let sums = sql(&format!(
        "SELECT sum(toInt128(value)), sum(toInt128(value_scaled)), sum(sensor_fp), \
                sum(region_fp), sum(name_fp), sum(unit_fp), sum(toInt128(tag_count)), \
                sum(tag_fp), sum(toInt128(batch_ts_ms)), sum(toInt128(quality_null)) \
         FROM (SELECT DISTINCT batch_id, event_seq, value, value_scaled, \
                      {sensor_fp} AS sensor_fp, \
                      {region_fp} AS region_fp, \
                      {name_fp} AS name_fp, \
                      {unit_fp} AS unit_fp, \
                      toUInt8(length(tags)) AS tag_count, \
                      {tag_fp} AS tag_fp, \
                      toUnixTimestamp64Milli(batch_ts) AS batch_ts_ms, \
                      isNull(quality) AS quality_null \
               FROM {table} WHERE batch_id >= {lo} AND batch_id < {hi})"
    ))?;
    if sums.len() < 10 {
        return Err(format!("gate sum query returned {sums:?}"));
    }
    let value_sum = num(&sums[0])?;
    let value_scaled_sum = num(&sums[1])?;
    let sensor_sum = num(&sums[2])?;
    let region_sum = num(&sums[3])?;
    let name_sum = num(&sums[4])?;
    let unit_sum = num(&sums[5])?;
    let tag_count_sum = num(&sums[6])?;
    let tag_sum = num(&sums[7])?;
    let batch_ts_sum = num(&sums[8])?;
    let null_quality_rows = num(&sums[9])?;

    let exp = expected_range(lo, hi);
    Ok(Gates {
        min_batch,
        max_batch,
        rows,
        distinct_ids,
        distinct_batches,
        duplicates: rows.saturating_sub(distinct_ids),
        contiguous: distinct_batches == hi - lo,
        // Compared against distinct ids, so a duplicate cannot mask a loss.
        rows_match: distinct_ids == exp.rows,
        value_sum_match: value_sum == exp.value_sum,
        value_scaled_match: value_scaled_sum == exp.value_scaled_sum,
        sensor_match: sensor_sum == exp.sensor_sum,
        region_match: region_sum == exp.region_sum,
        name_match: name_sum == exp.name_sum,
        unit_match: unit_sum == exp.unit_sum,
        tag_count_match: tag_count_sum == exp.tag_count_sum,
        tag_match: tag_sum == exp.tag_sum,
        batch_ts_match: batch_ts_sum == exp.batch_ts_sum,
        null_quality_match: null_quality_rows == i128::from(exp.null_quality_rows),
        send_ts_after_base: min_send_ts_us >= i128::from(BASE_TS_MS) * 1000,
    })
}

/// Consume the first `sample` messages of `topic` and prove that what is
/// actually on the wire matches the contract.
///
/// This exists because the round-trip unit test only proves `encode_batch` and
/// the typed decoder agree with each other. It does not prove that the *framed
/// bytes sitting in Kafka* are what a registry-based consumer expects — and
/// every competitor arm reads those bytes, not our encoder. So this checks the
/// Confluent header byte-for-byte, checks the embedded schema id, decodes the
/// datum, and re-derives every field from `batch_id` to confirm it.
///
/// Returns the number of messages verified.
///
/// # Panics
/// On any framing, schema-id, decode or field mismatch. A corpus that does not
/// match the contract invalidates the entire run, not one arm.
pub fn verify_corpus(bootstrap: &str, topic: &str, schema_id: u32, sample: u64) -> u64 {
    use rdkafka::consumer::{Consumer, base_consumer::BaseConsumer};
    use rdkafka::message::Message;
    use rdkafka::{Offset, TopicPartitionList, config::ClientConfig};
    use std::time::{Duration, Instant};

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "comparison-corpus-verify")
        .set("enable.auto.commit", "false")
        .create()
        .expect("verify consumer");
    let mut tpl = TopicPartitionList::new();
    // Partition 0 only: prefill assigns round-robin, so partition 0 holds every
    // `partitions`-th batch_id — enough to exercise both union branches and a
    // spread of tag lengths without consuming the whole corpus.
    tpl.add_partition_offset(topic, 0, Offset::Beginning)
        .expect("assign offset");
    consumer.assign(&tpl).expect("assign");

    let mut seen = 0u64;
    let deadline = Instant::now() + Duration::from_secs(60);
    while seen < sample {
        assert!(
            Instant::now() < deadline,
            "only verified {seen} of {sample} messages before the deadline"
        );
        let Some(result) = consumer.poll(Duration::from_millis(500)) else {
            continue;
        };
        let msg = result.expect("consume");
        let payload = msg.payload().expect("message has a payload");

        assert!(
            payload.len() > 5,
            "payload is {} bytes, too short to be Confluent-framed",
            payload.len()
        );
        assert_eq!(payload[0], 0x00, "Confluent magic byte");
        let embedded = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
        assert_eq!(embedded, schema_id, "embedded schema id");

        let value = apache_avro::from_avro_datum(schema(), &mut &payload[5..], None)
            .expect("decode datum from the wire");
        let batch: SensorBatch = apache_avro::from_value(&value).expect("into SensorBatch");
        let batch_id = u64::try_from(batch.batch_id).expect("batch_id non-negative");

        assert_eq!(batch.sensor, sensor_of(batch_id), "sensor for {batch_id}");
        assert_eq!(batch.region, region_of(batch_id), "region for {batch_id}");
        assert_eq!(
            batch.batch_ts_ms,
            batch_ts_ms_of(batch_id),
            "batch_ts_ms for {batch_id}"
        );
        assert_eq!(
            batch.send_ts_us,
            send_ts_us_prefill(batch_id),
            "send_ts_us for {batch_id}"
        );
        assert_eq!(
            u32::try_from(batch.events.len()).expect("event count fits u32"),
            EVENTS_PER_BATCH,
            "event count for {batch_id}"
        );
        for ev in &batch.events {
            let seq = u32::try_from(ev.seq).expect("seq non-negative");
            assert_eq!(ev.name, name_of(batch_id, seq), "name {batch_id}/{seq}");
            assert_eq!(ev.unit, unit_of(batch_id, seq), "unit {batch_id}/{seq}");
            assert_eq!(ev.value, value_of(batch_id, seq), "value {batch_id}/{seq}");
            assert_eq!(
                ev.quality,
                quality_of(batch_id, seq),
                "quality {batch_id}/{seq}"
            );
            assert_eq!(ev.tags, tags_of(batch_id, seq), "tags {batch_id}/{seq}");
        }
        seen += 1;
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The subject a Confluent client derives from the topic must be the subject
    /// the prefill registers under. An assertion rather than a comment, because
    /// every other arm resolves the schema by wire id and would stay green while
    /// the two disagreed.
    #[test]
    fn subject_follows_the_topic_name_strategy() {
        assert_eq!(
            SUBJECT,
            format!("{TOPIC}-value"),
            "the registry subject must be the topic-name-strategy name for {TOPIC}; \
             Kafka Connect's AvroConverter resolves the schema version by subject and \
             decodes nothing at all when it does not exist"
        );
    }

    #[test]
    fn the_committed_schema_parses() {
        let s = schema();
        assert!(
            matches!(s, Schema::Record(r) if r.name.name() == "SensorBatch"),
            "expected a SensorBatch record schema"
        );
    }

    /// The load-bearing test: an encoded datum must decode back into the typed
    /// struct with every field intact, including both unions and the inner
    /// array. If this passes, the explicit `AvroValue` construction agrees with
    /// the committed schema.
    #[test]
    fn a_batch_round_trips_through_the_typed_decoder() {
        for batch_id in [0u64, 1, 9, 10, 37, 1023, 1024] {
            let datum = encode_batch(batch_id, 42);
            let value = apache_avro::from_avro_datum(schema(), &mut datum.as_slice(), None)
                .expect("decode");
            let decoded: SensorBatch = apache_avro::from_value(&value).expect("into struct");

            assert_eq!(decoded.batch_id, i64::try_from(batch_id).unwrap());
            assert_eq!(decoded.sensor, sensor_of(batch_id));
            assert_eq!(decoded.region, region_of(batch_id));
            assert_eq!(decoded.batch_ts_ms, batch_ts_ms_of(batch_id));
            assert_eq!(decoded.send_ts_us, 42);
            assert_eq!(decoded.events.len() as u32, EVENTS_PER_BATCH);

            for (seq, ev) in (0u32..).zip(&decoded.events) {
                assert_eq!(ev.seq, i32::try_from(seq).unwrap());
                assert_eq!(ev.name, name_of(batch_id, seq));
                assert_eq!(ev.unit, unit_of(batch_id, seq));
                assert_eq!(ev.value, value_of(batch_id, seq));
                assert_eq!(ev.quality, quality_of(batch_id, seq));
                assert_eq!(ev.tags, tags_of(batch_id, seq));
            }
        }
    }

    /// Both nullable branches must actually occur in a small corpus, or the
    /// round-trip test above would be passing without ever exercising a union.
    #[test]
    fn both_union_branches_occur() {
        assert!(region_of(0).is_none(), "batch 0 has a null region");
        assert!(region_of(1).is_some(), "batch 1 has a present region");
        let qualities: Vec<_> = (0..EVENTS_PER_BATCH).map(|s| quality_of(3, s)).collect();
        assert!(
            qualities.iter().any(Option::is_none),
            "some quality is null"
        );
        assert!(qualities.iter().any(Option::is_some), "some quality is set");
    }

    #[test]
    fn the_generator_is_deterministic() {
        assert_eq!(encode_batch(12_345, 7), encode_batch(12_345, 7));
        assert_ne!(encode_batch(12_345, 7), encode_batch(12_346, 7));
    }

    #[test]
    fn confluent_framing_is_five_bytes_of_header() {
        let framed = frame_confluent(0x0102_0304, &[0xAA, 0xBB]);
        assert_eq!(framed, vec![0x00, 0x01, 0x02, 0x03, 0x04, 0xAA, 0xBB]);
    }

    #[test]
    fn value_is_never_negative_so_truncation_cannot_differ_across_languages() {
        for batch_id in [0u64, 1, 999, 1_000_000, 5_000_000] {
            for seq in 0..EVENTS_PER_BATCH {
                assert!(value_of(batch_id, seq) >= 0);
            }
        }
    }

    /// The committed DDL must yield exactly one `CREATE TABLE` statement and
    /// nothing else — in particular the trailing `--` comments documenting the
    /// gate queries must not be mistaken for executable SQL.
    #[test]
    fn ddl_splits_into_the_single_create_only() {
        let stmts = ddl_statements();
        assert_eq!(stmts.len(), 1, "expected 1 statement, got {stmts:#?}");
        assert!(stmts[0].contains("CREATE TABLE IF NOT EXISTS sensor_events"));
        for s in &stmts {
            assert!(
                !s.contains("uniqExact"),
                "a documented gate query leaked into executable DDL: {s}"
            );
        }
    }

    #[test]
    fn comment_stripping_does_not_eat_code_on_the_same_line() {
        let stmts = split_sql("CREATE TABLE t (a UInt64) -- note; with a semicolon\n;");
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0], "CREATE TABLE t (a UInt64)");
    }

    /// A `;` inside a string literal is content, not a statement boundary.
    /// Splitting on it would hand the server two fragments of one statement the
    /// entrant wrote correctly.
    #[test]
    fn a_semicolon_inside_a_string_literal_does_not_split() {
        let stmts = split_sql("SELECT 1 WHERE unit != ';';\nSELECT 2;");
        assert_eq!(stmts.len(), 2, "{stmts:#?}");
        assert_eq!(stmts[0], "SELECT 1 WHERE unit != ';'");
        assert_eq!(stmts[1], "SELECT 2");
    }

    /// A `--` inside a string literal is content, not a comment opener. The
    /// old line-wise strip read everything after it as prose and truncated the
    /// statement mid-literal.
    #[test]
    fn a_comment_marker_inside_a_string_literal_is_content() {
        let stmts = split_sql("SELECT splitByString('--', x) FROM t;");
        assert_eq!(stmts.len(), 1, "{stmts:#?}");
        assert_eq!(stmts[0], "SELECT splitByString('--', x) FROM t");
    }

    /// Both of ClickHouse's escape forms keep the literal open. Either one,
    /// misread as a closing quote, re-opens code where the entrant wrote data —
    /// and the very next `;` in the literal would then split the statement.
    #[test]
    fn escaped_quotes_keep_the_literal_open() {
        let doubled = split_sql("SELECT 'it''s; not a boundary' AS a;SELECT 2;");
        assert_eq!(doubled.len(), 2, "{doubled:#?}");
        assert_eq!(doubled[0], "SELECT 'it''s; not a boundary' AS a");

        let backslashed = split_sql("SELECT 'it\\'s; not a boundary' AS a;SELECT 2;");
        assert_eq!(backslashed.len(), 2, "{backslashed:#?}");
        assert_eq!(backslashed[0], "SELECT 'it\\'s; not a boundary' AS a");
    }

    /// Quoted identifiers get the same protection as string literals: a
    /// backtick- or double-quoted name can legally contain both split tokens.
    #[test]
    fn quoted_identifiers_are_quoted_regions() {
        let stmts = split_sql("SELECT `odd;--name`, \"other;--name\" FROM t;");
        assert_eq!(stmts.len(), 1, "{stmts:#?}");
        assert_eq!(stmts[0], "SELECT `odd;--name`, \"other;--name\" FROM t");
    }

    /// After a literal closes, `--` is a comment again. Quote-awareness must
    /// not overshoot into treating the whole line as protected.
    #[test]
    fn a_comment_after_code_on_a_line_with_a_literal_is_still_stripped() {
        let stmts = split_sql("SELECT 'a' -- trailing; note\nFROM t;");
        assert_eq!(stmts.len(), 1, "{stmts:#?}");
        assert_eq!(stmts[0], "SELECT 'a' \nFROM t");
    }

    /// The committed workload DDL must split exactly as it always has: one
    /// non-empty statement. The state machine replaced a line-wise splitter,
    /// and `dataset_version` hashes the DDL text — so a behavioural drift here
    /// would change what the driver executes without changing the hash that
    /// claims nothing changed.
    #[test]
    fn the_state_machine_splits_the_committed_ddl_unchanged() {
        let stmts = ddl_statements();
        assert!(!stmts.is_empty());
        assert_eq!(stmts.len(), 1, "expected 1 statement, got {stmts:#?}");
    }

    /// Every column the DDL declares must appear in the positional column list,
    /// in the same order. The column order *is* the RowBinary and Native wire
    /// contract, so a silent divergence here would corrupt every row rather
    /// than fail loudly.
    #[test]
    fn declared_columns_match_the_ddl_in_order() {
        let stmts = ddl_statements();
        let stmt = &stmts[0];
        let mut cursor = 0usize;
        for (name, ty) in COLUMNS {
            let needle = format!("\n    {name} ");
            let at = stmt[cursor..]
                .find(&needle)
                .map(|i| i + cursor)
                .unwrap_or_else(|| {
                    panic!("column {name} missing from {TABLE} DDL or out of order")
                });
            // The declared type must match too, or Native would encode a
            // leaf the server reads as a different type.
            let line_end = stmt[at + 1..].find('\n').map_or(stmt.len(), |i| at + 1 + i);
            let line = &stmt[at..line_end];
            assert!(
                line.contains(ty),
                "column {name} in {TABLE} is declared {line:?}, expected type {ty}"
            );
            cursor = at + 1;
        }
    }

    /// A range's expectation must be the difference of two prefixes, or the
    /// sustained gates and the drain gates would disagree about the same rows.
    ///
    /// Every field, not a sample of them: an accumulator that is not additive
    /// over `batch_id` would pass in drain mode and fail every sustained arm,
    /// and it would do so with a message blaming the framework.
    #[test]
    fn a_range_expectation_is_the_difference_of_two_prefixes() {
        let prefix_700 = expected(700);
        let prefix_200 = expected(200);
        let range = expected_range(200, 700);
        assert_eq!(range.rows, prefix_700.rows - prefix_200.rows, "rows");
        assert_eq!(
            range.null_quality_rows,
            prefix_700.null_quality_rows - prefix_200.null_quality_rows,
            "null_quality_rows"
        );
        for (label, got, whole, part) in [
            (
                "value_sum",
                range.value_sum,
                prefix_700.value_sum,
                prefix_200.value_sum,
            ),
            (
                "value_scaled_sum",
                range.value_scaled_sum,
                prefix_700.value_scaled_sum,
                prefix_200.value_scaled_sum,
            ),
            (
                "sensor_sum",
                range.sensor_sum,
                prefix_700.sensor_sum,
                prefix_200.sensor_sum,
            ),
            (
                "region_sum",
                range.region_sum,
                prefix_700.region_sum,
                prefix_200.region_sum,
            ),
            (
                "name_sum",
                range.name_sum,
                prefix_700.name_sum,
                prefix_200.name_sum,
            ),
            (
                "unit_sum",
                range.unit_sum,
                prefix_700.unit_sum,
                prefix_200.unit_sum,
            ),
            (
                "tag_count_sum",
                range.tag_count_sum,
                prefix_700.tag_count_sum,
                prefix_200.tag_count_sum,
            ),
            (
                "tag_sum",
                range.tag_sum,
                prefix_700.tag_sum,
                prefix_200.tag_sum,
            ),
            (
                "batch_ts_sum",
                range.batch_ts_sum,
                prefix_700.batch_ts_sum,
                prefix_200.batch_ts_sum,
            ),
        ] {
            assert_eq!(got, whole - part, "{label}");
        }
    }

    /// The cheap row count exists only because the full expectation got
    /// expensive. Two loops that disagree about which rows the workload keeps
    /// would make the driver announce one figure and the gate demand another.
    #[test]
    fn the_cheap_row_count_agrees_with_the_full_expectation() {
        for batches in [1u64, 2, 7, 10, 137, 1000] {
            assert_eq!(
                expected_rows(batches),
                expected(batches).rows,
                "over {batches} batches"
            );
        }
    }

    /// The fingerprint has to separate any two strings the corpus can put in a
    /// column, or two different columns could sum to the same total and the gate
    /// would report agreement it did not verify.
    ///
    /// Head-plus-reversed-head is not injective over arbitrary strings — it reads
    /// sixteen bytes and the longest tag concatenation is eighteen. So the
    /// property is asserted over the actual alphabet rather than argued for in
    /// general, and this test is what would fail if a generator constant grew the
    /// alphabet past what the fingerprint can separate.
    #[test]
    fn the_string_fingerprint_separates_every_string_the_corpus_can_produce() {
        let mut strings = std::collections::BTreeSet::new();
        // Everything a batch-scoped column can hold, including the coalesced
        // null region.
        strings.insert(String::new());
        for batch_id in 0..SENSORS.max(64) {
            strings.insert(sensor_of(batch_id));
            if let Some(r) = region_of(batch_id) {
                strings.insert(r);
            }
        }
        // Everything an event-scoped column can hold. Two full periods of every
        // modulus in the derivations is far more than enough to enumerate them.
        // The lowercase name is included alongside `name_upper`'s content
        // because it is what an arm that skipped the uppercase would write, and
        // that regression test relies on the two summing differently.
        for batch_id in 0..256u64 {
            for seq in 0..EVENTS_PER_BATCH {
                let name = name_of(batch_id, seq);
                strings.insert(ascii_upper(&name));
                strings.insert(name);
                strings.insert(unit_of(batch_id, seq).to_owned());
                strings.insert(tags_of(batch_id, seq).concat());
            }
        }

        let mut seen = std::collections::BTreeMap::new();
        for s in &strings {
            assert!(
                s.is_ascii(),
                "the generator emitted {s:?}, which is not ASCII — ClickHouse's byte-wise \
                 `reverse` and a character-wise reverse would then disagree"
            );
            if let Some(other) = seen.insert(str_fingerprint(s), s.clone()) {
                panic!("{s:?} and {other:?} share a fingerprint");
            }
        }
    }

    /// An arm emitting `tags = []` skips the `Array(LowCardinality(String))`
    /// encode on every row, which is a large and real speed-up. Before the tag
    /// expectations existed it passed every gate.
    #[test]
    fn an_arm_that_emits_empty_tags_now_fails_a_closed_form_expectation() {
        let exp = expected(500);
        assert_eq!(
            str_fingerprint(""),
            0,
            "an empty array concatenates to the empty string, whose fingerprint is what an \
             arm emitting no tags would sum to"
        );
        assert_ne!(exp.tag_count_sum, 0, "tag_count_sum");
        assert_ne!(exp.tag_sum, 0, "tag_sum");
    }

    /// The target column is `name_upper`. An arm that writes `name` through
    /// unchanged skips a per-row transform on 150,000,000 rows.
    #[test]
    fn an_arm_that_skips_the_ascii_uppercase_now_fails_a_closed_form_expectation() {
        let batches = 500;
        let exp = expected(batches);
        let mut lower = 0i128;
        for batch_id in 0..batches {
            for seq in 0..EVENTS_PER_BATCH {
                if !keeps(batch_id, seq) {
                    continue;
                }
                lower += str_fingerprint(&name_of(batch_id, seq));
            }
        }
        assert_ne!(exp.name_sum, lower);
    }

    /// The regression `ddl.sql` warns about: lose the `DateTime64` scaling and
    /// "every value silently lands in 1970". The expectation is far enough from
    /// zero that any such arm misses it by orders of magnitude.
    #[test]
    fn an_arm_that_loses_the_datetime64_scaling_now_fails_a_closed_form_expectation() {
        let exp = expected(500);
        let floor = i128::from(BASE_TS_MS) * i128::from(exp.rows);
        assert!(
            exp.batch_ts_sum >= floor,
            "batch_ts_sum {} is below {floor}, so an arm landing in 1970 could coincide with it",
            exp.batch_ts_sum
        );
    }

    /// Every arm must coalesce a null `region` to `''` — the target column is
    /// `LowCardinality(String)`, not `LowCardinality(Nullable(String))`. An arm
    /// that writes the region of every batch, null ones included, disagrees.
    #[test]
    fn an_arm_that_drops_the_null_region_coalesce_now_fails_a_closed_form_expectation() {
        let batches = 500;
        let exp = expected(batches);
        let mut uncoalesced = 0i128;
        for batch_id in 0..batches {
            // What an arm that forgot the null branch would most plausibly write:
            // the region string the batch would have had. Summed over the rows
            // the workload keeps, so the two sums differ only in the coalesce.
            let fp = str_fingerprint(&format!("region-{}", batch_id % 7));
            for seq in 0..EVENTS_PER_BATCH {
                if !keeps(batch_id, seq) {
                    continue;
                }
                uncoalesced += fp;
            }
        }
        assert_ne!(exp.region_sum, uncoalesced);
    }

    #[test]
    fn ascii_upper_is_ascii_only() {
        assert_eq!(ascii_upper("metric_7"), "METRIC_7");
        // Left alone, which is the property that makes the operation identical
        // in Rust, Java and ClickHouse regardless of locale.
        assert_eq!(ascii_upper("straße"), "STRAßE");
    }

    /// The filter must drop strictly more than nothing and strictly less than
    /// everything, or it is not being exercised.
    #[test]
    fn the_filter_drops_a_meaningful_fraction() {
        let batches = 500;
        let total = batches * u64::from(EVENTS_PER_BATCH);
        let exp = expected(batches);
        assert!(
            exp.rows > 0 && exp.rows < total,
            "the filter dropped {} of {total}",
            total - exp.rows,
        );
        // The unit sentinel alone removes one row in eight; the quality floor
        // removes more. Anything outside this band means a derivation drifted.
        let dropped = (total - exp.rows) as f64 / total as f64;
        assert!(
            (0.12..0.45).contains(&dropped),
            "the filter dropped {dropped:.3} of rows, outside the expected band"
        );
        assert!(exp.value_scaled_sum > 0);
    }

    /// `expected` must agree with an independent flatten of the same corpus,
    /// so a mistake in the accumulator cannot pass as an expectation.
    ///
    /// The flatten reads the columns back out of *decoded Avro*, not out of the
    /// derivation functions, which is what makes it independent: an expectation
    /// computed from the same misreading of the specification as the encoder
    /// would agree with the encoder and prove nothing.
    #[test]
    fn expectations_agree_with_an_independent_flatten() {
        let batches = 200u64;
        let mut rows = 0u64;
        let mut sum = 0i128;
        let mut sensor_sum = 0i128;
        let mut region_sum = 0i128;
        let mut name_sum = 0i128;
        let mut unit_sum = 0i128;
        let mut tag_count_sum = 0i128;
        let mut tag_sum = 0i128;
        let mut batch_ts_sum = 0i128;
        let mut null_quality_rows = 0u64;
        for batch_id in 0..batches {
            let datum = encode_batch(batch_id, 0);
            let v = apache_avro::from_avro_datum(schema(), &mut datum.as_slice(), None)
                .expect("decode");
            let b: SensorBatch = apache_avro::from_value(&v).expect("into struct");
            for ev in &b.events {
                let seq = u32::try_from(ev.seq).unwrap();
                if !keeps(batch_id, seq) {
                    continue;
                }
                rows += 1;
                sum += i128::from(ev.value);
                sensor_sum += str_fingerprint(&b.sensor);
                region_sum += str_fingerprint(b.region.as_deref().unwrap_or(""));
                name_sum += str_fingerprint(&ascii_upper(&ev.name));
                unit_sum += str_fingerprint(&ev.unit);
                tag_count_sum += i128::try_from(ev.tags.len()).unwrap();
                tag_sum += str_fingerprint(&ev.tags.concat());
                batch_ts_sum += i128::from(b.batch_ts_ms);
                if ev.quality.is_none() {
                    null_quality_rows += 1;
                }
            }
        }
        let exp = expected(batches);
        assert_eq!(exp.rows, rows);
        assert_eq!(exp.value_sum, sum);
        assert_eq!(exp.sensor_sum, sensor_sum);
        assert_eq!(exp.region_sum, region_sum);
        assert_eq!(exp.name_sum, name_sum);
        assert_eq!(exp.unit_sum, unit_sum);
        assert_eq!(exp.tag_count_sum, tag_count_sum);
        assert_eq!(exp.tag_sum, tag_sum);
        assert_eq!(exp.batch_ts_sum, batch_ts_sum);
        assert_eq!(exp.null_quality_rows, null_quality_rows);
    }

    /// The coordinated-omission property, stated as an assertion rather than as
    /// a comment.
    ///
    /// A message due at `t` carries `t` whether it went at `t`, a millisecond
    /// late, or five seconds late — so the whole of the wait it spent behind a
    /// producer that had fallen behind lands in `ingest_ts - send_ts`. The
    /// alternative implementation, `now()` at the moment of the send, is
    /// modelled here as the thing that must NOT be equal to what is stamped:
    /// under lateness it reports a shorter wait, and it reports the shortest
    /// wait exactly when the pipeline is worst.
    #[test]
    fn a_sustained_send_is_stamped_with_its_scheduled_time_however_late_it_actually_went() {
        let origin = 1_772_000_000_000_000i64;
        let rate = 50_000u64;

        for n in [0u64, 1, 49_999, 50_000, 1_000_000] {
            let due = sustained_due_us(n, rate);
            let stamped = send_ts_us_sustained(origin, due);
            // The stamp is the schedule, and the schedule does not know when the
            // send happened. Three different actual send times, one stamp.
            for late_us in [0i64, 1_000, 5_000_000] {
                let actually_sent_at = origin + i64::try_from(due).unwrap() + late_us;
                assert_eq!(
                    stamped,
                    send_ts_us_sustained(origin, due),
                    "the stamp moved with the send time"
                );
                assert!(
                    actually_sent_at >= stamped,
                    "a send can be late but never early: control only reaches the stamp \
                     once the schedule is due"
                );
                // What `now()` at send would have reported, and why it is wrong:
                // it charges the pipeline for zero of the producer's lateness.
                let omitted = actually_sent_at - stamped;
                assert_eq!(omitted, late_us);
            }
        }
    }

    /// The schedule is a pure function of the index and the target rate, so the
    /// offered rate is what was asked for rather than whatever a loop of sleeps
    /// happened to achieve.
    #[test]
    fn the_sustained_schedule_is_a_fixed_function_of_the_index_and_the_rate() {
        let rate = 40_000u64;
        // Exactly `rate` messages fall due in each whole second.
        assert_eq!(sustained_due_us(0, rate), 0);
        assert_eq!(sustained_due_us(rate, rate), 1_000_000);
        assert_eq!(sustained_due_us(rate * 60, rate), 60_000_000);

        // Monotonic and non-accumulating: the gap between the first and last
        // message of a long run is the run's length, to within the one
        // microsecond integer division can lose per message.
        let n = 10_000_000u64;
        let span_us = sustained_due_us(n, rate) - sustained_due_us(0, rate);
        let exact_us = n * 1_000_000 / rate;
        assert!(exact_us.abs_diff(span_us) <= 1, "{span_us} vs {exact_us}");
    }

    /// The stride interleave has to cover every global index exactly once, and
    /// every index has to keep the due time it would have had with one thread —
    /// or the aggregate offered rate is not the rate that was requested, and
    /// `batch_id` stops being dense enough for the gate's contiguity test.
    #[test]
    fn every_message_belongs_to_exactly_one_producer_thread_and_keeps_its_global_schedule() {
        let rate = 96_000u64;
        for threads in [1u64, 2, 3, 8] {
            let per_thread = 1_000u64;
            let mut seen = std::collections::BTreeSet::new();
            for slot in 0..threads {
                for k in 0..per_thread {
                    let global = slot + k * threads;
                    assert!(seen.insert(global), "index {global} sent twice");
                    // Derived from the GLOBAL index, so a thread's own counter
                    // never becomes a private timeline.
                    assert_eq!(sustained_due_us(global, rate), (global * 1_000_000) / rate);
                }
            }
            assert_eq!(
                u64::try_from(seen.len()).expect("count fits u64"),
                threads * per_thread
            );
            assert_eq!(*seen.first().expect("non-empty"), 0);
            assert_eq!(
                *seen.last().expect("non-empty"),
                threads * per_thread - 1,
                "the covered range must be contiguous, or batch_id is not dense"
            );
        }
    }
}
