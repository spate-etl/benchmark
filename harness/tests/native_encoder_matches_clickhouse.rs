//! The Native encoder, proven against a live ClickHouse rather than against
//! itself.
//!
//! `harness/src/ceiling.rs` writes ClickHouse's Native block format so that the
//! ingest ceiling can cover the arms that write it — which is every headline arm
//! of this benchmark's own vendor. That encoder cannot be trusted because it was
//! written carefully. A Native block that is subtly wrong does not fail loudly:
//! a `LowCardinality` index width one step too narrow truncates the indexes past
//! the boundary and lands the *wrong dictionary entry* in those rows, and the
//! server accepts the block. A ceiling measured against that is worse than no
//! ceiling, because the refusal it replaced was at least honest.
//!
//! # The oracle is the corpus, not a round trip
//!
//! The unit tests in `ceiling.rs` decode a block with a reference decoder
//! written beside the encoder, which proves the bytes are internally consistent
//! and proves nothing about whether ClickHouse reads them the same way. So this
//! file does not check bytes at all. It POSTs blocks at a real server, under the
//! **committed** `workload/clickhouse/ddl.sql`, and then runs
//! [`corpus::run_gates`] — the same closed-form gate every published arm is held
//! to. A block whose landed rows satisfy row identity, both value sums, the
//! `sensor`/`region`/name/`unit`/`tags` fingerprints, the `DateTime64` scaling
//! and the null-`quality` count has been checked against the same oracle, by the
//! same query, as the arms the ceiling exists to gate.
//!
//! That is a far stronger statement than "our decoder agrees with our encoder",
//! and it is stronger in the specific direction that matters: the fingerprint
//! sums are exactly what notices a `LowCardinality` column that landed real
//! strings in the wrong rows.
//!
//! RowBinary runs through the identical path as a **control**. It is already
//! trusted and already measured, so if it fails here the fault is in this file's
//! rig — the container, the DDL, the gate window — rather than in the encoder
//! under test. Without it, a rig bug would read as a Native defect.
//!
//! JSONEachRow and ArrowStream run through the same path for the same reason
//! Native does: each carries encoding decisions a unit test cannot prove the
//! server shares — that a numeric-epoch `DateTime64` is read digit-by-digit and
//! timezone-free, that a full-width `UInt64` survives the JSON number path,
//! that `Utf8` casts onto `LowCardinality` and an Arrow `Timestamp` with
//! `"UTC"` metadata lands tick-for-tick. Their ceilings are not committed until
//! this file has passed for them; the encoders in `ceiling.rs` say so.
//!
//! # Running it
//!
//! These tests need a Docker daemon and pull `clickhouse/clickhouse-server:26.8`,
//! so they are `#[ignore]`d and `cargo test` does not run them. To run them:
//!
//! ```text
//! cargo test --package spate-benchmark-harness \
//!     --test native_encoder_matches_clickhouse -- --ignored
//! ```
//!
//! Each test brings up its **own** container on its own port and removes it on
//! the way out, including on a panic, so they are safe to run in parallel and
//! leave nothing behind. That costs a few container starts and buys the property
//! that a failure names one format rather than one shared fixture.
//!
//! This is the repository's first Docker-requiring test. If you add another,
//! copy the shape: a guard value constructed before anything is started, a
//! `Drop` that cleans up, a distinct container name and port, and `#[ignore]`
//! with the command to run it in the doc comment.

use std::time::{Duration, Instant};

use spate_benchmark_harness::ceiling::{Format, encode_insert_block, insert_encoded_block};
use spate_benchmark_harness::corpus;
use spate_benchmark_harness::{docker, http};

/// The server these blocks are proven against.
///
/// Pinned to a version rather than `latest`, for the reason the whole harness
/// pins images: a claim about what a server accepted is worth nothing if nobody
/// can say which server.
const IMAGE: &str = "clickhouse/clickhouse-server:26.8";

/// The password the container is started with, matching `infra::start_clickhouse`
/// so the two rigs do not differ in a way somebody has to remember.
const PASSWORD: &str = "bench";

/// Seconds to wait for a fresh container to answer `/ping`. Generous because the
/// first run of these tests pulls the image.
const READY_TIMEOUT_S: u64 = 300;

/// Batches encoded into each block under test.
///
/// Chosen to cross the `LowCardinality` index-width boundary rather than for
/// speed. `sensor` is `batch_id % 1024`, so 400 batches give 400 distinct
/// sensors plus the reserved default — past 256, which is where the indexes step
/// from one byte to two. A block that stayed under the boundary would exercise
/// only the narrow branch and would pass with the dangerous one broken.
///
/// It also has to leave the gate something to check: `corpus::run_gates`
/// excludes the lowest and highest `batch_id`, so this is 398 gated batches and
/// roughly 29,000 rows after the workload's filters.
const BATCHES: u64 = 400;

// ---------------------------------------------------------------------------
// The container
// ---------------------------------------------------------------------------

/// A ClickHouse container that removes itself when it goes out of scope.
///
/// The `Drop` is the point. A test that panics mid-way — which is what a failing
/// assertion does — must not leave a container holding a port, or the next run
/// fails for a reason that has nothing to do with the encoder.
struct Server {
    name: &'static str,
    port: u16,
}

impl Server {
    /// Starts a fresh container and waits for it to answer.
    ///
    /// The guard is constructed **before** `docker run`, so every failure path
    /// after this point — a refused `run`, a server that never answers, a failed
    /// DDL statement — unwinds through `Drop` and takes the container with it.
    fn start(name: &'static str, port: u16) -> Self {
        let server = Self { name, port };
        // A container left behind by a hard kill (which skips `Drop`) would make
        // `docker run --name` fail with a name conflict, so the previous one is
        // removed rather than reused: these tests want a cold, empty server.
        let _ = docker::docker_try(&["rm", "-f", name]);

        let publish = format!("{port}:8123");
        docker::docker(&[
            "run",
            "-d",
            "--name",
            name,
            "-p",
            &publish,
            "-e",
            &format!("CLICKHOUSE_PASSWORD={PASSWORD}"),
            "--ulimit",
            "nofile=262144:262144",
            IMAGE,
        ]);

        let deadline = Instant::now() + Duration::from_secs(READY_TIMEOUT_S);
        while !http::get("localhost", port, "/ping").is_ok_and(|b| b.contains("Ok")) {
            assert!(
                Instant::now() < deadline,
                "{name} did not answer /ping within {READY_TIMEOUT_S}s"
            );
            std::thread::sleep(Duration::from_millis(500));
        }

        // The committed DDL, verbatim. A hand-written simplification of the
        // table would prove the encoder against a target no arm writes to —
        // and `LowCardinality`, `Nullable` and `Array(LowCardinality(String))`
        // are exactly the columns a simplification would drop.
        for stmt in corpus::ddl_statements() {
            docker::clickhouse_sql("localhost", port, "default", PASSWORD, &stmt)
                .unwrap_or_else(|e| panic!("{name}: DDL failed: {e}"));
        }
        server
    }

    fn sql(&self, sql: &str) -> String {
        docker::clickhouse_sql("localhost", self.port, "default", PASSWORD, sql)
            .unwrap_or_else(|e| panic!("{}: {sql} failed: {e}", self.name))
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = docker::docker_try(&["rm", "-f", self.name]);
    }
}

// ---------------------------------------------------------------------------
// The check
// ---------------------------------------------------------------------------

/// Encodes one block, POSTs it, and holds what landed to the corpus's own
/// closed-form expectations.
///
/// Every assertion here is the gate's, not this file's. There is deliberately no
/// hand-written expected row count or expected sum: `corpus::run_gates` derives
/// them from the generator, so a test that agreed with a mistaken expectation
/// would be agreeing with the same mistake the gate would.
fn block_satisfies_every_closed_form_expectation(server: &Server, format: Format) {
    server.sql(&format!("TRUNCATE TABLE {}", corpus::TABLE));

    let block = encode_insert_block(format, 0, BATCHES);
    assert_eq!(
        block.rows,
        corpus::expected_rows(BATCHES),
        "{format:?} block carries the wrong number of rows before it is even sent",
    );
    insert_encoded_block(
        "localhost",
        server.port,
        "default",
        PASSWORD,
        format,
        &block,
    )
    .unwrap_or_else(|e| panic!("{format:?}: the server refused the block: {e}"));

    // A refused insert answers with an exception and is caught above; a block
    // the server accepts and reads as fewer rows than it holds is the quieter
    // failure, so the landed count is checked before the gate runs.
    let landed: u64 = server
        .sql(&format!("SELECT count() FROM {}", corpus::TABLE))
        .trim()
        .parse()
        .expect("a row count");
    assert_eq!(
        landed, block.rows,
        "{format:?}: the server accepted the block and landed {landed} of {} rows",
        block.rows
    );

    let gates = corpus::run_gates("localhost", server.port, "default", PASSWORD, BATCHES)
        .unwrap_or_else(|e| panic!("{format:?}: the gate failed: {e}"));

    assert_eq!(
        gates.failure(),
        None,
        "{format:?}: the landed rows disagree with the generator",
    );
    assert!(gates.passed());
    assert_eq!(
        gates.duplicates, 0,
        "{format:?}: one block cannot legitimately duplicate a row",
    );
}

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

/// The encoder's hard columns, live: four `LowCardinality(String)`
/// columns, a `Nullable(Float64)` and an `Array(LowCardinality(String))`, at a
/// block wide enough to force two-byte dictionary indexes on `sensor` — plus
/// the filtered row set, the uppercased name column and `value_scaled`.
#[test]
#[ignore = "needs a Docker daemon; run with `cargo test --test native_encoder_matches_clickhouse -- --ignored`"]
fn a_native_block_satisfies_every_closed_form_expectation() {
    let server = Server::start("spate-bench-native-encoder", 18131);
    block_satisfies_every_closed_form_expectation(&server, Format::Native);
}

/// The control. RowBinary is already trusted and already measured, so it must
/// pass this rig; if it does not, the container, the DDL or the
/// gate window is wrong rather than the Native encoder. Without this, every rig
/// bug would present as a Native defect and cost an afternoon.
#[test]
#[ignore = "needs a Docker daemon; run with `cargo test --test native_encoder_matches_clickhouse -- --ignored`"]
fn the_rowbinary_control_satisfies_the_same_expectations_through_the_same_rig() {
    let server = Server::start("spate-bench-native-encoder-control", 18133);
    block_satisfies_every_closed_form_expectation(&server, Format::RowBinary);
}

/// The JSONEachRow encoder's decisions, live: numeric-epoch `DateTime64` values
/// that must land tick-for-tick regardless of the server's timezone (the
/// `batch_ts` sum is the check), full-width integers through the JSON number
/// path, `null` into `Nullable(Float64)`, and JSON arrays into
/// `Array(LowCardinality(String))`. A ceiling measured through this encoder is
/// not committed until this has passed — see the format-addition rule on
/// `ceiling::Format`.
#[test]
#[ignore = "needs a Docker daemon; run with `cargo test --test native_encoder_matches_clickhouse -- --ignored`"]
fn a_json_each_row_block_satisfies_every_closed_form_expectation() {
    let server = Server::start("spate-bench-json-each-row-encoder", 18135);
    block_satisfies_every_closed_form_expectation(&server, Format::JsonEachRow);
}

/// The ArrowStream encoder's schema mapping, live: `Utf8` cast onto four
/// `LowCardinality(String)` columns, `List<Utf8>` onto the array of
/// dictionaries, Arrow validity onto `Nullable(Float64)`, and both
/// `Timestamp` units — with their explicit `"UTC"` — onto the `DateTime64`
/// scales, at the same index-width-crossing block size as Native. Same
/// commitment rule as JSONEachRow above.
#[test]
#[ignore = "needs a Docker daemon; run with `cargo test --test native_encoder_matches_clickhouse -- --ignored`"]
fn an_arrow_stream_block_satisfies_every_closed_form_expectation() {
    let server = Server::start("spate-bench-arrow-stream-encoder", 18137);
    block_satisfies_every_closed_form_expectation(&server, Format::ArrowStream);
}
