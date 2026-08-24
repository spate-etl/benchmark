//! What the shared infrastructure can absorb, measured — and refused when it
//! does not describe this corpus.
//!
//! `methodology/` says that before any arm is published, a ceiling pass
//! measures what ClickHouse and the broker can actually absorb at their declared
//! caps, and that **an arm exceeding 70% of either ceiling is infra-bound and
//! cannot be published as a system comparison**. That rule is the entire defence
//! of the claim "we are not measuring ClickHouse". This module is where it
//! becomes true rather than asserted.
//!
//! # The defect this module closes
//!
//! The committed ceiling recorded `consume_msgs_per_s: 305554` alongside
//! `consume_mb_per_s: 256.7`, and those two numbers agree only at the message
//! size the rig was pointed at: 840 bytes. The corpus's `events_per_batch` was
//! later raised from 20 to 100, taking the message from ~842 bytes to ~4056, and
//! the messages-per-second figure was kept as the denominator. Gating a
//! 4056-byte corpus against an 840-byte message rate asserts that the consume
//! path serves 1.24 GB/s — **4.8x the byte rate the same rig actually
//! sustained.** Measured against the byte rate it did sustain, the headline arm
//! sits at or over the 70% limit rather than comfortably below it.
//!
//! Nothing caught that, because nothing recorded what the number had been
//! measured against. So the fix is not the re-measurement; the fix is that a
//! ceiling now carries the message size, the insert format, the corpus version
//! and the infrastructure envelope it was taken at, and the harness **refuses to
//! gate against one taken at a materially different message size rather than
//! extrapolating from it**. See [`Ceilings::gate`]. The re-measurement is merely
//! the first use of that refusal.
//!
//! # The second defect: an ingest ceiling every arm cleared
//!
//! The first measured pass produced a ClickHouse ceiling that **every published
//! arm exceeded**, by between 100% and 162%. A ceiling an arm exceeds is not a
//! ceiling; it is a measurement of the rig, and it is the most expensive kind of
//! wrong number this file can hold, because a too-low ingest ceiling marks honest
//! arms `infra_bound` and refuses to publish them.
//!
//! The cause was that one `--threads` drove both passes. The consume pass is
//! naturally bounded by partition count and correctly refuses more threads than
//! the topic has partitions — a thread with no partitions consumes nothing and
//! drags the aggregate down. The ingest pass has no such bound and was starved by
//! the same number anyway: it POSTed at the consume pass's four, and could not
//! have been raised past the topic's eight without the consume pass refusing the
//! flag. Meanwhile the arm that figure was the denominator for ran four shards
//! times four in-flight requests. Measured afterwards, Native gave
//! 3,416,495 rows/s at four inserters and 4,427,028 at eight — 29.6% more, and
//! still climbing.
//!
//! So the two knobs are separate: `--threads` is the consume pass's and keeps its
//! partition refusal, and the ingest pass owns its own concurrency. It does not
//! take that concurrency from an operator either, because a single sample at
//! whatever number was typed is a floor rather than a ceiling. It **sweeps**, and
//! [`Sweep`] documents the ladder, the stopping rule and the bound. The rung that
//! won is recorded on the ceiling as [`IngestCeiling::threads`], and the ladder
//! that chose it — every rung and what it absorbed — as [`IngestCeiling::sweep`],
//! for the same reason the message size is recorded on the consume ceiling: a
//! figure whose shape is not on the record is a figure nobody can tell apart
//! from a starved one.
//!
//! And when the sweep runs out of ladder while still improving, the pass
//! **refuses** rather than publishing its last sample. Throughput that was still
//! climbing when the rig stopped asking is not a ceiling; it is the largest
//! number this rig happened to reach.
//!
//! # The third defect: the rig inserted from outside the VM
//!
//! The swept figures were 36% to 125% higher than the starved ones and every
//! combination plateaued cleanly at sixteen inserters — and they were still not
//! the target's, because the rig POSTed from the harness process on macOS
//! through Docker Desktop's published port while **every arm inserts
//! container-to-container and never crosses that boundary**.
//!
//! Four readings said so, and none of them is an inference from this rig's own
//! clock. Every format plateaued at 246–264 MB/s on the wire regardless
//! of row width, which is a byte wall rather than a row wall. ClickHouse's own
//! `system.query_log` put 84.9–85.5% of total insert
//! duration in `NetworkReceiveElapsed` — the server blocked reading the request
//! body — against 5.7ms of user and 17.3ms of system CPU per 697ms insert. The
//! container sat at 270–315% of its 500% cap, so the target was not saturated.
//! And a direct probe of the same statement at the same concurrency, 800 MB into
//! `ENGINE = Null`, gave 274–279 MB/s from the host and 2,598–3,126 MB/s from
//! inside the network: roughly ten times.
//!
//! The inserter therefore moved to the side of the boundary the arms are on. See
//! [`crate::inserter`] for the container, the protocol and the reason a stock
//! Python image rather than a purpose-built one.
//!
//! **Every ingest ceiling now says which side it was taken from**, in its rig
//! string, in a provenance note and — see the fifth defect below — in a field the
//! gate reads, because a figure taken from outside and one taken from inside
//! differ by an order of magnitude and a reader cannot be left to guess which
//! they hold.
//!
//! # The fourth defect: the target deduplicated the rig's own blocks
//!
//! A rung POSTing 38,000,000 rows left exactly 800,000 in the table.
//! The committed DDL sets `non_replicated_deduplication_window = 1000`
//! and the rig cycles a pool of pre-encoded blocks, so after the first turn
//! through the pool ClickHouse parsed, sorted, compressed and part-wrote every
//! repeat and then dropped it at commit as a duplicate.
//!
//! What that omits is the background merging of rows that **stay**, and an arm's
//! rows stay. So the figure was pointed high and the gate left lenient, which is
//! the direction this module refuses to err in.
//!
//! The pass therefore measures against a **dedicated ceiling table** — see
//! [`ceiling_table_ddl`] — derived from the committed DDL by changing the name
//! and the deduplication window and nothing else, so the column types, the
//! engine, the sort key and the materialised `ingest_ts` are the arms' own. The
//! rows land, the merges happen, and the pass reports the server's own account
//! of what it did with them: rows landed against rows POSTed, merge activity
//! from `system.part_log`, and the target's cgroup CPU against its cap.
//!
//! # What the two together changed, measured
//!
//! With both closed, the target is the constraint and says so in its own
//! accounting. At the winning rung of every format it runs at 100–102%
//! of its five-core cap and is CFS-throttled; 1–2% of insert duration is
//! `NetworkReceiveElapsed`, down from 85%; every row POSTed is still in the
//! table when the rung ends; and `system.part_log` shows 48–113% of the rows
//! written merged again inside the same sweep. Native rose from a
//! 246–264 MB/s wall to 855–859 MB/s while RowBinary rose only
//! from ~250 to ~337 MB/s, which is the shape the diagnosis predicted: lifting a
//! byte wall does nothing for a format that was already CPU-bound server-side.
//!
//! # The fifth defect: the consume pass was still outside, and it was the
//! # nearest ceiling
//!
//! Fixing the ingest pass moved the binding constraint. With ClickHouse
//! measured honestly the **broker** became the nearest ceiling to the fastest
//! arms — and the broker's figure was still being read by `rdkafka` in the
//! harness process on macOS, through Docker's published port 9092, while every
//! arm consumes container-to-container. The identical confounder, in the other
//! domain, now sitting under the numbers that decide whether the headline arms
//! are publishable at all.
//!
//! It was worth what the ingest one was worth. The same corpus, the same
//! broker, the same partition count and the same window served **72,349
//! messages a second through the published port and 1,719,373 from a container
//! on the bench network** — twenty-four times, so every consume share ever
//! computed here was twenty-four times too large. See [`crate::fetcher`] for the
//! client, why it is a stdlib Python program speaking Kafka's fetch protocol
//! rather than a library, and the measurements that rejected each alternative.
//!
//! # And the refusal that makes both of them stick
//!
//! The pass that moved the inserter inside recorded the fact in prose and said,
//! in this very module, that the right eventual shape was a *field* plus a
//! refusal in [`Ceilings::gate`] — the same shape as
//! [`ConsumeCeiling::message_bytes`], for the same reason: prose is something a
//! reader has to read, and a field is something the harness can drop a ceiling
//! over. It was left undone because it would have touched the driver's test
//! fixtures.
//!
//! It is done. Both ceilings now carry [`Location`], both passes record where
//! their client ran, and a ceiling that says `"outside"` — or that cannot say at
//! all, which is what every figure committed before the field deserialises to —
//! is **dropped with its reason** rather than gated against. The refusal is
//! derived from recorded provenance rather than from a flag somebody has to
//! remember to set, which is this repository's whole argument for why its
//! refusals work.
//!
//! # The consume pass's new bound: the corpus, not the broker
//!
//! Moving the consumer inside exposed something the slow path had hidden. At
//! 1.72M messages a second the entire 1,500,000-message corpus is **0.87
//! seconds** of backlog, so `--seconds 8` is not a window this corpus can offer;
//! seven of those eight seconds would be spent measuring an idle broker, which
//! the ported `DRAINED` refusal correctly declines to report. The pass therefore
//! calibrates, sizes its window against the shallowest consumer's backlog, and
//! records both the window and the share of the corpus it burned. A backlog too
//! shallow for a window worth measuring is a refusal about the *corpus*, whose
//! remedy is a deeper prefill.
//!
//! # The sixth defect: the ceiling was honest and the envelope behind it was not
//!
//! With the passes measured from the right side of the network, the numbers
//! finally described the infrastructure — and what they described was an
//! infrastructure allocated the wrong way round. Against the first honest
//! ceilings, the vendor's RowBinary arms sat at **72.5%** and **67.9%** of the
//! ClickHouse ingest ceiling, both of which become 80.4%
//! and 75.3% once the retracted drain-window defect is corrected for. Two arms
//! over or at the limit, and both of them the vendor's own.
//!
//! The obvious remedy is the one `methodology/` names: shrink the arm envelope
//! until the arms are engine-bound. The measurements said something else. At the
//! consume ceiling the broker occupied **3.87 of its eight cores and was
//! throttled in zero CFS periods**, while serving a client reading forty times
//! faster than the fastest arm; at every RowBinary winning rung ClickHouse
//! occupied **100–102% of its five cores and was CFS-throttled**. Those two
//! readings are the same finding twice: the shared infrastructure had its cores
//! on the wrong side.
//!
//! So the envelope was searched rather than shrunk, and the outcome moved four
//! cores from the broker to the target. The allocation in force is declared in
//! the environment profile, which is what this module gates against.
//!
//! Two things in this module exist because of it. [`select_combinations`] lets a
//! pass measure the format that actually binds instead of all of them, because a
//! search is a dozen passes and a fifteen-minute pass makes it a day; and
//! [`Ceilings::measured_under_other_envelopes`] refuses to commit the half-file
//! that a narrowed pass under new caps would otherwise leave behind. Neither
//! changes how any ceiling is measured.
//!
//! One thing the search exposed is **not** fixed here and is recorded so that it
//! is not rediscovered. At nine cores the target absorbs Native rows faster than
//! this rig can offer them: it was at 100% of its cap at 256 inserters, still
//! improving, and the rig could not open 512 connections at all. The sweep
//! refused, correctly, and the pass was re-run against a ladder it could drive.
//! A rig that has to be told where to stop is one allocation away from a rig that
//! cannot measure the target at all, and the remedy is a bigger block per request
//! rather than more sockets.
//!
//! # Two ceilings, because the methodology names two
//!
//! * The **consume ceiling** is a property of the broker's fetch path. It is
//!   stored in messages per second *and* bytes per second, because the review
//!   above showed that a message rate alone does not transfer across message
//!   sizes — that is the whole defect — and a byte rate alone cannot be compared
//!   against an arm whose work is per-message. Both are recorded, both are
//!   gated, and the stricter of the two binds.
//! * The **ClickHouse ingest ceiling** is a property of the target, and it is
//!   stored **per insert format**. Rule 5 of the methodology says
//!   Native, RowBinary and JSONEachRow are not the same amount of server-side
//!   work; a single ClickHouse number would therefore gate every arm against
//!   work it does not do, and it would err leniently for exactly the arms whose
//!   format is cheapest server-side.
//!
//! # What the ingest ceiling can honestly be measured for
//!
//! The rig emits the formats it can emit **correctly**, from this crate, with no
//! dependency on any system under test — today that is `Native`, `RowBinary`,
//! `RowBinaryWithNamesAndTypes`, `JSONEachRow` and `ArrowStream` (see
//! [`Format`]). The last two are the newest: their encoders exist so that the
//! next ceiling pass can measure the formats two new arms will report, and the
//! live-server proof that gated Native's arrival —
//! `harness/tests/native_encoder_matches_clickhouse.rs`, POSTing this crate's
//! bytes at a real server and holding what lands to the corpus's closed-form
//! oracle — covers them too and **must pass before a ceiling measured through
//! either is committed**. It does not substitute one format's figure for
//! another's.
//!
//! The substitution is tempting and is refused on purpose. RowBinary is
//! row-oriented and has to be transposed server-side, so its ceiling is very
//! probably a *lower* bound on Native's, which would make the gate too strict
//! rather than too lenient — the safe direction. But "very probably" is an
//! assertion about ClickHouse's internals that this repository has not measured,
//! and the last time this suite carried an unmeasured conversion factor between
//! two rates it published numbers gated against 4.8x the rate the rig sustained.
//! An arm whose format has no measured ceiling is therefore **not gated against
//! ClickHouse at all**, and says so, which is a visible gap rather than a silent
//! one.
//!
//! # Why `Native` is now emitted rather than declined
//!
//! `Native` was the live instance of that gap, and an expensive one: every
//! headline arm of this benchmark's own vendor declares
//! `wire_format = "native"`, so the arms with the most reason to be held to a
//! ceiling were the only ones not held to any. Refusing was honest and it was
//! not free — a permanent gap aimed squarely at ourselves is not neutrality, it
//! is an exemption nobody voted for.
//!
//! So the rig writes a real Native block: varuint column count, varuint row
//! count, then per column a name, a type and that column's values contiguously,
//! with the `LowCardinality` dictionaries, the `Nullable` null maps and the
//! `Array` offsets that entails. See [`Column::write_native`] for the byte
//! layout of each and for what each constant means.
//!
//! The encoder is **not** trusted because it was written carefully. A Native
//! block that is subtly wrong is worse than no Native ceiling, because the
//! refusal it replaces is at least honest: a mis-serialised `LowCardinality`
//! index width does not fail loudly, it lands the wrong dictionary entry in
//! every row and reports a ceiling for an insert nobody performs. So the
//! proof is mechanical, and it runs twice over. Every measurement re-proves
//! its encoder inside the pass itself: [`prove_format_lands_correctly`] POSTs
//! a proof block at the live target before a single rung is timed and checks
//! what landed against [`corpus::run_gates`] — the same closed-form oracle
//! every published arm is held to, covering row identity, both value sums, the
//! `sensor`/`region`/name/`unit`/`tags` fingerprints, the `DateTime64` scaling
//! and the null pattern — and a measurement whose rows the oracle rejects is
//! refused rather than recorded. Before that,
//! `harness/tests/native_encoder_matches_clickhouse.rs` runs the same chain
//! against a fresh `clickhouse/clickhouse-server:26.3` under the committed
//! DDL, with RowBinary as a control so a failure there indicts the test rig
//! rather than the encoder. That test needs a Docker daemon and is
//! `#[ignore]`d; it is the pre-merge proof that a format may be claimed at
//! all, not the only time the claim is checked.
//!
//! # Why the measured figure is safe to gate against even if our inserter is slow
//!
//! The ingest pass generates rows from the corpus, encodes them **before the
//! clock starts**, and then POSTs pre-encoded blocks from several threads. If
//! the harness's own inserter is the constraint rather than ClickHouse, the
//! measured ceiling comes out too *low*, the gate comes out too *strict*, and
//! the cost is a result we decline to publish. The opposite error — a ceiling
//! that is too high — publishes an infra-bound arm as a system comparison, which
//! is the one outcome the limit exists to prevent. The error is deliberately
//! pointed in the direction that only ever costs us results.
//!
//! # This is a maintainer command
//!
//! A pass brings up real infrastructure, reads gigabytes off the broker and
//! writes tens of millions of rows into ClickHouse. It takes minutes, it holds
//! the arm lock, and it truncates the target tables. It is not a CI check and
//! `bench validate` does not run it.

use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::corpus;
use crate::docker;
use crate::infra::Endpoints;

/// The fraction of a measured ceiling above which an arm is infra-bound.
///
/// Above this we are measuring the shared infrastructure rather than the system,
/// and the run is recorded with `status: infra_bound` rather than published.
///
/// It lives here rather than beside the environment profile because it is a
/// property of the ceilings, not of the hardware; `crate::environment`
/// re-exports it so the name every existing call site uses still resolves.
pub const HEADROOM_LIMIT: f64 = 0.70;

/// How far a ceiling's message size may differ from the corpus's before the
/// harness refuses to gate against it.
///
/// Five percent, and the number is chosen against the noise rather than against
/// intuition. The reference environment's own profile records run-to-run spread
/// reaching 14.5% on throughput, so a message-size difference inside this band
/// cannot move a headroom share by more than the spread every chart already
/// draws — while anything outside it is being extrapolated rather than measured.
/// It is deliberately tighter than that noise floor: a rule whose tolerance is
/// as wide as the noise is a rule that never fires.
///
/// The band is not a licence to interpolate. Inside it, [`Ceiling::headroom`]
/// still compares the arm's **byte** rate against the measured byte rate as well
/// as its message rate against the measured message rate, and takes whichever
/// share is larger — so a ceiling measured 4% away is applied at its stricter
/// reading rather than at its more flattering one.
pub const MESSAGE_BYTES_TOLERANCE: f64 = 0.05;

/// Batches sampled to establish the corpus's mean framed message size.
///
/// Equal to the batch count `harness/tests/golden_corpus.rs` fingerprints, so
/// that the size this module derives and the size that test pins move together:
/// that test asserts 1000 batches encode to 4,051,124 datum bytes, and five
/// bytes of Confluent framing per message make the mean 4056.
const MESSAGE_SIZE_SAMPLE: u64 = 1000;

// ---------------------------------------------------------------------------
// The committed file
// ---------------------------------------------------------------------------

/// The ceilings measured for one environment, exactly as committed.
///
/// Parsed with `deny_unknown_fields`, which is load-bearing rather than tidy. The
/// previous shape of this file carried a top-level `consume_msgs_per_s`, and a
/// hand-edited key that the harness silently ignored is precisely the failure
/// this module exists to make impossible: a number in a file that looks
/// authoritative and reaches nothing.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Ceilings {
    /// What the broker's fetch path sustained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consume: Option<ConsumeCeiling>,
    /// What ClickHouse absorbed, one entry per insert format.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub clickhouse: Vec<IngestCeiling>,
}

/// What a ceiling was measured against.
///
/// Every field here exists because its absence caused, or would have caused, a
/// wrong number to be gated against. The envelope digest and the corpus version
/// are recorded rather than assumed because a ceiling taken under different
/// broker caps, or against a different corpus, is not this environment's
/// ceiling — and neither fact is visible in a rate.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance {
    /// `YYYY-MM-DD` the pass was taken.
    pub date: String,
    /// `DATASET_VERSION` in force when it was taken. Empty means the
    /// measurement predates this field and cannot state it.
    #[serde(default)]
    pub dataset_version: String,
    /// `Environment::infra_digest` in force when it was taken. Empty means the
    /// envelope was never recorded, which is itself a refusal to gate: there is
    /// nothing to compare this environment against.
    #[serde(default)]
    pub infra_digest: String,
    /// Host the pass ran on, for a reader rather than for the gate.
    #[serde(default)]
    pub host: String,
    /// The command that produced it, so the pass can be reproduced by hand.
    #[serde(default)]
    pub rig: String,
}

/// Which side of the bench network's boundary a ceiling's client ran on.
///
/// The fifth defect, closed. Both passes were once taken from the harness
/// process on macOS through Docker Desktop's published ports, while every arm
/// speaks container-to-container over [`crate::docker::NETWORK`] and crosses no
/// boundary at all. Measured on this host that boundary is worth an order of
/// magnitude in both directions — 246–279 MB/s against 2.6–3.1 GB/s for an
/// insert, 72,349 against 1,719,373 messages a second for a fetch — so a figure
/// taken on the wrong side is not a slightly different ceiling, it is a
/// different measurement.
///
/// The previous pass recorded this in a prose note and said, in the module docs
/// above, that the right eventual shape was a field and a refusal derived from
/// it. This is that shape. Prose is something a reader has to read; a field is
/// something [`Ceilings::gate`] can drop a ceiling over, which is the same
/// reasoning that made [`ConsumeCeiling::message_bytes`] a field rather than a
/// caveat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Location {
    /// A container on the bench network, as every arm is.
    Inside,
    /// The harness process on the host, through a published port.
    Outside,
}

impl Location {
    /// This location's serialisation, which is what a ceiling records.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Inside => "inside",
            Self::Outside => "outside",
        }
    }
}

/// The location a `"inside"`/`"outside"` string names.
///
/// A `String` on the ceiling rather than an enum, and resolved here, so that
/// a value nobody recognises is a
/// *refusal with a reason* rather than a parse failure that takes the whole
/// committed file down. `""` — which is what every ceiling measured before this
/// field existed deserialises to — is one of those values, and it is exactly the
/// case the refusal is for: a figure that cannot say where it was taken is not
/// gateable.
fn location_named(name: &str) -> Option<Location> {
    match name {
        "inside" => Some(Location::Inside),
        "outside" => Some(Location::Outside),
        _ => None,
    }
}

/// Why a ceiling whose client did not run inside the bench network is dropped.
///
/// One spelling for both passes, because they were the same defect and a reader
/// who has understood one refusal has understood the other.
fn wrong_side_refusal(what: &str, recorded: &str) -> String {
    let stated = match location_named(recorded) {
        Some(Location::Outside) => "was measured from OUTSIDE the bench network, on the host \
                                    through Docker's published port"
            .to_owned(),
        Some(Location::Inside) => unreachable!("an inside ceiling is not refused"),
        None if recorded.is_empty() => {
            "does not record which side of the bench network its client ran on, because it \
             predates the field"
                .to_owned()
        }
        None => format!(
            "records its client as {recorded:?}, which names neither side of the bench network"
        ),
    };
    format!(
        "{what} {stated}. Every arm speaks container-to-container over {}, and on this host \
         that boundary is worth an order of magnitude in both directions: 246-279 MB/s against \
         2.6-3.1 GB/s for an insert, and 72,349 against 1,719,373 messages a second for a \
         fetch. A ceiling taken on the far side of it is a floor on the infrastructure and a \
         ceiling on the rig, so it is dropped rather than applied. Re-measure with \
         `bench ceiling --measure`.",
        crate::docker::NETWORK
    )
}

/// What the broker's fetch path sustained, at a stated message size.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumeCeiling {
    /// Messages per second.
    ///
    /// Messages, not rows, deliberately: the ceiling is a property of the
    /// consume path, which moves messages, and rows per second depends on the
    /// fan-out factor. Storing rows would mean that changing `events_per_batch`
    /// silently invalidated this file.
    ///
    /// That reasoning was right and was not sufficient. It kept the file honest
    /// about fan-out and left it silent about **size**, so raising
    /// `events_per_batch` invalidated the figure anyway — by making each message
    /// 4.8x larger while the message rate stood still. Hence
    /// [`ConsumeCeiling::message_bytes`], and hence the refusal.
    pub msgs_per_s: u64,
    /// Megabytes per second, where a megabyte is 10^6 bytes.
    ///
    /// Recorded as measured rather than derived from `msgs_per_s` and
    /// `message_bytes`, because the point of storing it is to be a second,
    /// independent reading of the same pass — one that survives a change of
    /// message size when the message rate does not.
    pub mb_per_s: f64,
    /// Mean framed message size, in bytes, the figures above were taken at.
    ///
    /// The gate compares this against what the corpus currently produces and
    /// refuses when they differ materially. It is the single field whose absence
    /// caused the defect this module closes.
    pub message_bytes: u64,
    /// Topic partition count during the pass. With few partitions a single
    /// consumer has few fetch streams, so this figure is substantially partition
    /// parallelism and not a broker limit.
    pub partitions: i32,
    /// Broker image and caps, for a reader.
    #[serde(default)]
    pub broker: String,
    /// Consumer threads the pass ran with.
    #[serde(default)]
    pub threads: u64,
    /// Which side of the bench network the consumer ran on: [`Location::name`].
    ///
    /// Empty means the pass predates this field, which is a refusal rather than
    /// a default — see [`Location`]. Every figure this repository committed
    /// before it was read through Docker's published port and is therefore a
    /// floor on the broker, at roughly a twenty-fourth of what the same corpus
    /// and the same broker serve to a container on the bench network.
    #[serde(default)]
    pub client: String,
    /// How the measured window was sized, and against how much backlog.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window: Option<ConsumeWindow>,
    /// What the broker itself spent over that window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_cgroup: Option<Cgroup>,
    /// What it was measured against.
    pub provenance: Provenance,
}

/// What ClickHouse absorbed for one insert format.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IngestCeiling {
    /// The descriptor's `reports.wire_format` value this ceiling gates —
    /// `rowbinary`, `rowbinary_nt`. An arm declaring anything else is not gated
    /// against ClickHouse, because it does not do this work.
    pub format: String,
    /// Rows per second the target absorbed.
    pub rows_per_s: u64,
    /// Megabytes per second on the wire, where a megabyte is 10^6 bytes.
    pub mb_per_s: f64,
    /// Mean encoded size of one row in this format, in bytes. The counterpart of
    /// [`ConsumeCeiling::message_bytes`]: a rows-per-second figure taken against
    /// a materially different row is the same class of mistake.
    pub row_bytes: u64,
    /// Concurrent inserters the pass ran with. A ceiling taken at a
    /// concurrency the target could have exceeded is a floor; recording it is
    /// what lets a later reader tell the two apart.
    ///
    /// It used to hold whatever `--threads` said, which the consume pass bounds
    /// by the topic's partition count — so the field faithfully recorded the
    /// starvation and nobody read it as such. It now holds the **winning rung of
    /// the concurrency sweep**: the concurrency at which this target absorbed
    /// the most, out of a ladder that carried on past it and failed to beat it.
    /// The ladder itself is [`IngestCeiling::sweep`], so a reader can check that
    /// the rungs above this one failed to beat it rather than taking it on
    /// trust.
    #[serde(default)]
    pub threads: u64,
    /// ClickHouse image and caps, for a reader.
    #[serde(default)]
    pub clickhouse: String,
    /// Which side of the bench network the inserter ran on: [`Location::name`].
    ///
    /// Empty means the pass predates this field — see [`Location`]. The pass
    /// that moved the inserter inside recorded the fact in a rig string and a
    /// provenance note and left the field for later, on the grounds that adding
    /// one would touch the driver's test fixtures. That objection has been paid;
    /// the note remains, and this is what the gate reads.
    #[serde(default)]
    pub client: String,
    /// Every rung of the concurrency ladder. The winning figure is a ceiling
    /// only because the rungs above it failed to beat it, so the curve is what
    /// makes it one rather than a floor.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sweep: Vec<SweepPoint>,
    /// What the target itself spent over the winning rung.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_cgroup: Option<Cgroup>,
    /// What the winning rung POSTed against what stayed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub landed: Option<Landed>,
    /// What the server wrote and merged over the sweep.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parts: Option<PartLog>,
    /// How much of the server's insert time was spent reading this rig's bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkWait>,
    /// The longest a rung waited for the target to go quiet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settle: Option<Settle>,
    /// The rung that could not be driven, where one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped_at: Option<Refusal>,
    /// What it was measured against.
    pub provenance: Provenance,
}

/// What the target absorbed at one rung of the ingest concurrency sweep.
///
/// Kept as a value rather than folded straight into a summary string because the
/// shape of the curve is what decides whether the winning figure is a ceiling at
/// all: two rungs above it that failed to beat it is a plateau, and one rung
/// above it that beat it is a rig that stopped asking too early. That decision
/// is [`Sweep::still_climbing`], and it needs the numbers rather than prose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SweepPoint {
    /// Concurrent inserters this rung ran with.
    pub concurrency: u64,
    /// Rows per second the target absorbed at it.
    pub rows_per_s: u64,
}

/// A cgroup v2 reading over a measured window.
///
/// The reading that decides whose limit a figure is. A client cannot tell "the
/// server is saturated" from "my bytes are arriving slowly" by timing itself;
/// the server's own cgroup can, and a mean core count against a declared cap
/// needs no interpretation. At the cap the figure is the server's ceiling; well
/// under it, the figure is a floor on the server and a ceiling on this rig,
/// which is the strict direction.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Cgroup {
    /// Mean cores occupied over the window.
    pub cores: f64,
    /// The container's declared cap.
    pub cap_cores: f64,
    /// Share of the CPU it spent in user mode, where 1.0 is all of it.
    pub user_share: f64,
    /// CFS periods in which the cap actually bit.
    pub nr_throttled: u64,
    /// Microseconds spent throttled by it.
    pub throttled_us: u64,
}

/// How the consume window was sized, and against how much backlog.
///
/// From inside the bench network the whole corpus is under a second of reading,
/// so the window is sized against the backlog rather than taken from `--seconds`
/// — which makes the figure partly a statement about the corpus's depth. A
/// figure whose window is not on the record cannot be told apart from one whose
/// window was long enough.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumeWindow {
    /// Seconds `--seconds` asked for.
    pub requested_s: f64,
    /// Seconds the measured window actually ran.
    pub actual_s: f64,
    /// Messages the measured window read.
    pub messages_read: u64,
    /// Messages on the topic when it ran.
    pub topic_depth: u64,
    /// What a calibration window read, in messages per second.
    pub calibrated_msgs_per_s: u64,
}

/// What the winning rung POSTed against what the table still held.
///
/// Rows that land are only worth landing, for a ceiling's purposes, if the
/// target does the merging they cause. A count below `posted` means the rig's
/// repeated blocks were deduplicated at commit, and the pass refuses rather than
/// publishing a figure that skipped that work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Landed {
    /// Rows the winning rung POSTed.
    pub posted: u64,
    /// Rows still in the ceiling table when it ended. `None` when the count
    /// could not be read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counted: Option<u64>,
}

/// What the server wrote and merged over the sweep, from its own `part_log`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PartLog {
    /// Parts written.
    pub parts_written: u64,
    /// Rows in them.
    pub rows_written: u64,
    /// Merges performed.
    pub merges: u64,
    /// Rows merged again inside the same interval.
    pub rows_merged: u64,
}

/// How much of the server's insert time was spent waiting for this rig's bytes.
///
/// The question every ingest ceiling has to answer and none of them could: is
/// this figure a property of ClickHouse, or of the client measuring it? Asked of
/// the server rather than inferred from this rig's clock, because the server is
/// the only party that can distinguish the two.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkWait {
    /// Inserts the server counted.
    pub inserts: u64,
    /// Their total duration, in milliseconds.
    pub duration_ms: f64,
    /// How much of that was spent blocked reading the request body.
    pub waiting_ms: f64,
}

/// The longest a rung waited for the target to go quiet before it started.
///
/// Not a measure of the merge work that fell outside a timed window: `TRUNCATE`
/// cancels outstanding merges along with the parts they were merging, so this is
/// the cost of the removal rather than of the work removed. What the rungs did
/// pay is in [`PartLog`].
#[derive(Debug, Clone, Copy, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Settle {
    /// The longest wait, in seconds.
    pub max_wait_s: f64,
    /// The rung it preceded.
    pub at_concurrency: u64,
    /// Additional quiet imposed on top, in milliseconds.
    pub quiet_ms: u64,
}

/// Who stopped the ladder, and with what error.
///
/// The two are opposite findings. `Target` is ClickHouse declining work it
/// cannot merge fast enough, which is the ceiling doing its job; `Rig` is a
/// socket this rig could not open, which says nothing about the target and means
/// the figure is the best of what could be asked for rather than the most that
/// could be given.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusedBy {
    /// The target's own back-pressure.
    Target,
    /// This rig's transport.
    Rig,
}

/// The rung that could not be driven, and which side refused it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Refusal {
    /// The concurrency that failed.
    pub concurrency: u64,
    /// Which side said no.
    pub refused_by: RefusedBy,
    /// The error, verbatim.
    pub error: String,
}

impl Ceilings {
    /// Reads a ceilings file.
    ///
    /// # Errors
    ///
    /// If the file is missing or does not parse.
    pub fn load(path: &Path) -> Result<Self, String> {
        let src =
            std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        serde_json::from_str(&src).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Writes a ceilings file, pretty-printed and newline-terminated.
    ///
    /// # Errors
    ///
    /// If the file cannot be serialised or written.
    pub fn save(&self, path: &Path) -> Result<(), String> {
        let mut json = serde_json::to_string_pretty(self)
            .map_err(|e| format!("serialise {}: {e}", path.display()))?;
        json.push('\n');
        std::fs::write(path, json).map_err(|e| format!("write {}: {e}", path.display()))
    }

    /// Folds a freshly measured pass into these ceilings.
    ///
    /// Merges by key rather than replacing the file, and that is deliberate: a
    /// pass that measured RowBinary must not delete a JSONEachRow ceiling
    /// somebody else measured. A pass that re-measures an existing key replaces
    /// it, because a ceiling has one current value.
    pub fn merge(&mut self, pass: Pass) {
        self.consume = Some(pass.consume);
        for measured in pass.ingest {
            let at = self
                .clickhouse
                .iter()
                .position(|c| c.format == measured.format);
            match at {
                Some(i) => self.clickhouse[i] = measured,
                None => self.clickhouse.push(measured),
            }
        }
        self.clickhouse.sort_by(|a, b| a.format.cmp(&b.format));
    }

    /// Which ceilings in this file were measured under some **other**
    /// infrastructure envelope than `infra_digest`, named for a refusal.
    ///
    /// A ceilings file is provenance for every published record, and one whose
    /// entries were taken under two different envelopes cannot describe the
    /// environment it names. That state became reachable when `--only` did: a
    /// restricted pass under new infrastructure caps re-measures the combination
    /// it was asked for and leaves the rest of the file describing the caps that
    /// were in force yesterday.
    ///
    /// It is not a *silent* state — [`Ceilings::gate`] drops every foreign-envelope
    /// entry with its reason, and the arms it would have covered carry
    /// `Flag::HeadroomUnproven`. But the operator learns that at the end of a
    /// sweep rather than at the moment of writing, and at the moment of writing
    /// the remedy is one flag away: drop `--only` and measure the file.
    ///
    /// Empty when every entry, including the consume ceiling, was measured under
    /// this envelope — which is what a full pass always leaves behind.
    #[must_use]
    pub fn measured_under_other_envelopes(&self, infra_digest: &str) -> Vec<String> {
        let mut stale = Vec::new();
        if let Some(c) = &self.consume
            && c.provenance.infra_digest != infra_digest
        {
            stale.push(format!(
                "the broker consume ceiling ({})",
                described_envelope(&c.provenance.infra_digest)
            ));
        }
        for c in &self.clickhouse {
            if c.provenance.infra_digest != infra_digest {
                stale.push(format!(
                    "the ClickHouse ingest ceiling for {} ({})",
                    c.format,
                    described_envelope(&c.provenance.infra_digest)
                ));
            }
        }
        stale
    }

    /// Resolves what an arm on this corpus may actually be gated against.
    ///
    /// This is the refusal, and it is the point of the module. A ceiling that
    /// does not describe the bytes the arms read, or the envelope they run
    /// under, is not converted, scaled or interpolated — it is dropped, with the
    /// reason carried through to the record so that "no ceiling applied" is
    /// never mistaken for "the ceiling was satisfied".
    ///
    /// `corpus_message_bytes` is what the generator currently emits, from
    /// [`corpus_message_bytes`]; `infra_digest` is the envelope-defining digest
    /// of the environment profile.
    #[must_use]
    pub fn gate(&self, corpus_message_bytes: u64, infra_digest: &str) -> Ceiling {
        let mut refusals = Vec::new();
        let mut consume_msgs_per_s = 0;
        let mut consume_bytes_per_s = 0;

        match &self.consume {
            None => refusals.push(
                "no consume ceiling has been measured for this environment, so no arm can \
                 be shown to be engine-bound rather than broker-bound. Measure one with \
                 `bench ceiling --measure`."
                    .to_owned(),
            ),
            Some(c) => {
                let mut usable = true;
                if size_differs_materially(c.message_bytes, corpus_message_bytes) {
                    usable = false;
                    refusals.push(format!(
                        "the consume ceiling was measured at {}-byte messages but this corpus \
                         produces {corpus_message_bytes}-byte ones ({:.1}x). A messages-per-second \
                         figure does not transfer across message sizes: the same pass recorded \
                         {:.1} MB/s, and gating {corpus_message_bytes}-byte messages against an \
                         {}-byte message rate asserts a byte rate the broker was never shown to \
                         serve. Re-measure with `bench ceiling --measure`.",
                        c.message_bytes,
                        size_ratio(corpus_message_bytes, c.message_bytes),
                        c.mb_per_s,
                        c.message_bytes,
                    ));
                }
                if location_named(&c.client) != Some(Location::Inside) {
                    usable = false;
                    refusals.push(wrong_side_refusal("the consume ceiling", &c.client));
                }
                if c.provenance.infra_digest.is_empty() {
                    usable = false;
                    refusals.push(format!(
                        "the consume ceiling records no infrastructure envelope, so there is \
                         nothing to check against this environment's ({infra_digest}). A ceiling \
                         taken under different broker caps or a different partition count is not \
                         this environment's ceiling. Re-measure with `bench ceiling --measure`."
                    ));
                } else if c.provenance.infra_digest != infra_digest {
                    usable = false;
                    refusals.push(format!(
                        "the consume ceiling was measured under infrastructure envelope {} but \
                         this environment is {infra_digest}. Re-measure with \
                         `bench ceiling --measure`.",
                        c.provenance.infra_digest
                    ));
                }
                if usable {
                    consume_msgs_per_s = c.msgs_per_s;
                    consume_bytes_per_s = mb_to_bytes(c.mb_per_s);
                }
            }
        }

        let mut ingest = Vec::new();
        for c in &self.clickhouse {
            if c.provenance.infra_digest.is_empty() || c.provenance.infra_digest != infra_digest {
                refusals.push(format!(
                    "the ClickHouse ingest ceiling for {} was measured under \
                     infrastructure envelope {:?} but this environment is {infra_digest}. \
                     Re-measure with `bench ceiling --measure`.",
                    c.format, c.provenance.infra_digest
                ));
                continue;
            }
            if location_named(&c.client) != Some(Location::Inside) {
                refusals.push(wrong_side_refusal(
                    &format!("the ClickHouse ingest ceiling for {}", c.format),
                    &c.client,
                ));
                continue;
            }
            ingest.push(GateableIngest {
                format: c.format.clone(),
                rows_per_s: c.rows_per_s,
            });
        }

        Ceiling {
            consume_msgs_per_s,
            consume_bytes_per_s,
            corpus_message_bytes,
            ingest,
            refusals,
        }
    }
}

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

/// One ClickHouse ceiling that survived [`Ceilings::gate`].
#[derive(Debug, Clone)]
struct GateableIngest {
    format: String,
    rows_per_s: u64,
}

/// The ceilings an arm on the current corpus may actually be gated against.
///
/// Note what this type does **not** offer: a way to ask for a ceiling that was
/// refused. A refused ceiling is not present, and the reason is, so the only
/// thing a caller can do with a stale measurement is report why it was not used.
#[derive(Debug, Clone)]
pub struct Ceiling {
    /// Messages per second the consume path sustained, at a message size that
    /// matches this corpus. `0` when there is no ceiling that may be gated
    /// against, which is what raises `Flag::HeadroomUnproven`.
    pub consume_msgs_per_s: u64,
    /// Bytes per second it sustained. `0` alongside a zero message rate.
    pub consume_bytes_per_s: u64,
    /// The corpus's mean framed message size, so an arm's message rate can be
    /// turned into a byte rate without the caller knowing the corpus.
    pub corpus_message_bytes: u64,
    ingest: Vec<GateableIngest>,
    refusals: Vec<String>,
}

/// What an arm achieved, in the units the ceilings are stated in.
///
/// One argument struct rather than a widening parameter list, so that adding a
/// third ceiling changes this type and the body of [`Ceiling::headroom`] and
/// leaves the driver's call site alone. The previous shape — a bare
/// `ceiling_msgs_per_s: u64` threaded through the run record — is why adding the
/// ClickHouse ceiling had nowhere to go.
#[derive(Debug, Clone, Copy)]
pub struct Achieved<'a> {
    /// Messages per second the arm consumed. The driver has rows per second and
    /// the workload's row yield per message; the conversion belongs at the call
    /// site because only the driver knows the yield it actually measured.
    pub msgs_per_s: f64,
    /// Rows per second the arm inserted.
    pub rows_per_s: f64,
    /// The arm's declared `reports.wire_format`.
    pub wire_format: &'a str,
    /// Whether the arm installs its own ClickHouse objects (a `[clickhouse]`
    /// arm DDL hook) between its inserts and the target — a landing table, a
    /// materialized view. The ingest ceilings were measured as direct inserts
    /// into the bare target with no arm objects installed, so an insert that
    /// also pays a view's flatten, filters and derived columns is doing
    /// server-side work no measured ceiling describes.
    pub server_side_transform: bool,
}

/// Which ceiling a [`Share`] was taken against.
///
/// Machine-readable, because a consumer that has to identify a ceiling by
/// matching on `Share::against` is coupled to prose — and that coupling goes
/// stale silently, in the direction of reporting a figure the arm was never
/// held to. `against` is for a reader; this is for the record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Against {
    /// The broker's consume path, at this corpus's message size.
    BrokerConsume,
    /// ClickHouse's ingest path, for this arm's insert format.
    ClickHouseIngest,
}

/// One ceiling an arm was checked against.
#[derive(Debug, Clone)]
pub struct Share {
    /// Which ceiling this is, for a consumer.
    pub kind: Against,
    /// What the arm was checked against, for a human.
    pub against: String,
    /// The arm's share of it, where 1.0 is the ceiling itself.
    pub share: f64,
    /// The ceiling itself, in the unit its own path is measured in — messages
    /// per second for the broker, rows per second for ClickHouse.
    ///
    /// Carried so a record can say what it was actually held to without going
    /// back to the committed file and re-deriving which entry the gate chose.
    /// Two derivations of one decision is how they come to disagree.
    pub ceiling: u64,
}

/// Every ceiling an arm was checked against, and every one it could not be.
#[derive(Debug, Clone)]
pub struct Headroom {
    shares: Vec<Share>,
    unproven: Vec<String>,
}

impl Ceiling {
    /// Checks an arm against every ceiling that applies to it.
    ///
    /// The consume share is the **larger** of the arm's share of the measured
    /// message rate and its share of the measured byte rate. They coincide when
    /// the corpus is exactly the size the ceiling was taken at and diverge
    /// within the tolerance band, and taking the larger is what stops a ceiling
    /// measured at a slightly smaller message being applied at its more
    /// flattering reading.
    #[must_use]
    pub fn headroom(&self, arm: Achieved<'_>) -> Headroom {
        let mut shares = Vec::new();
        let mut unproven = self.refusals.clone();

        if self.consume_msgs_per_s > 0 && self.consume_bytes_per_s > 0 {
            let by_msgs = arm.msgs_per_s / self.consume_msgs_per_s as f64;
            let by_bytes = (arm.msgs_per_s * self.corpus_message_bytes as f64)
                / self.consume_bytes_per_s as f64;
            shares.push(Share {
                kind: Against::BrokerConsume,
                against: "broker consume".to_owned(),
                share: by_msgs.max(by_bytes),
                ceiling: self.consume_msgs_per_s,
            });
        }

        if arm.server_side_transform {
            unproven.push(format!(
                "this arm installs its own ClickHouse objects (arm DDL), so every insert \
                 also pays server-side work — a materialized view's flatten, filters and \
                 derived columns — that the {:?} ingest ceiling, measured as direct \
                 inserts into the bare target, does not describe. It is deliberately NOT \
                 gated against that ceiling: same format over a different shape is the \
                 same unmeasured substitution this gate already refuses across formats.",
                arm.wire_format
            ));
        } else {
            match self.ingest.iter().find(|c| c.format == arm.wire_format) {
                Some(c) if c.rows_per_s > 0 => shares.push(Share {
                    kind: Against::ClickHouseIngest,
                    against: format!("clickhouse ingest ({})", c.format),
                    share: arm.rows_per_s / c.rows_per_s as f64,
                    ceiling: c.rows_per_s,
                }),
                _ => unproven.push(format!(
                    "no ClickHouse ingest ceiling has been measured for wire format {:?}, so this \
                 arm is not gated against the target. It is deliberately NOT \
                 gated against another format's figure: the insert format materially changes \
                 server-side work, and substituting one for another is the same unmeasured \
                 conversion that produced the message-size defect. Measure it with \
                 `bench ceiling --measure`.",
                    arm.wire_format
                )),
            }
        }

        Headroom { shares, unproven }
    }

    /// Why a ceiling present in the environment's file is not being gated
    /// against. Empty when everything the file holds is usable.
    #[must_use]
    pub fn refusals(&self) -> &[String] {
        &self.refusals
    }
}

impl Headroom {
    /// Every ceiling this arm was checked against.
    #[must_use]
    pub fn shares(&self) -> &[Share] {
        &self.shares
    }

    /// The ClickHouse ingest ceiling this arm was held to, if it was held to
    /// one at all.
    ///
    /// `None` means no ceiling covers this arm's insert format, which is a
    /// different statement from "it cleared the gate" and is why the record
    /// also carries `Flag::HeadroomUnproven`.
    #[must_use]
    pub fn applied_ingest_rows_per_s(&self) -> Option<u64> {
        self.shares
            .iter()
            .find(|s| s.kind == Against::ClickHouseIngest)
            .map(|s| s.ceiling)
    }

    /// The ceiling the arm is closest to, which is the one that decides.
    #[must_use]
    pub fn binding(&self) -> Option<&Share> {
        self.shares
            .iter()
            .max_by(|a, b| a.share.total_cmp(&b.share))
    }

    /// Whether the arm exceeded the headroom limit against any ceiling.
    ///
    /// "Any", not "the consume ceiling", because the methodology says *either*.
    /// Reading only one of the two is how an arm bound by ClickHouse got
    /// published as a system comparison.
    #[must_use]
    pub fn infra_bound(&self) -> bool {
        self.binding().is_some_and(|s| s.share > HEADROOM_LIMIT)
    }

    /// Whether every ceiling the methodology names was actually checked.
    ///
    /// `false` means the arm's headroom is unknown rather than satisfied, and
    /// the record must carry `Flag::HeadroomUnproven`. An arm that passes the
    /// checks it happens to have is not the same as an arm that passes the
    /// checks the methodology promises, and the difference has to reach the
    /// record or a reader cannot tell them apart.
    #[must_use]
    pub fn is_proven(&self) -> bool {
        self.unproven.is_empty()
    }

    /// The ceilings that could not be checked, and why.
    #[must_use]
    pub fn unproven(&self) -> &[String] {
        &self.unproven
    }

    /// One line naming every share, and flagging anything unchecked.
    ///
    /// Written for a record's `note` as much as for the terminal, so a reader of
    /// the archive can see which ceilings an arm was held to without holding the
    /// environment file open beside it.
    #[must_use]
    pub fn summary(&self) -> String {
        let mut out = if self.shares.is_empty() {
            "no ceiling applied".to_owned()
        } else {
            self.shares
                .iter()
                .map(|s| format!("{} {:.0}%", s.against, s.share * 100.0))
                .collect::<Vec<_>>()
                .join(", ")
        };
        if !self.unproven.is_empty() {
            out.push_str(&format!("; UNPROVEN: {}", self.unproven.join("; ")));
        }
        out
    }
}

// ---------------------------------------------------------------------------
// The corpus's message size
// ---------------------------------------------------------------------------

/// The mean framed size, in bytes, of the messages this corpus produces.
///
/// Derived from the generator rather than declared, so it moves when the corpus
/// moves and cannot be left behind the way `payload_bytes: 840` was. Sampled
/// over [`MESSAGE_SIZE_SAMPLE`] batches — the message is not a fixed size, since
/// `tags` carries zero to three elements and `region` is null one batch in ten —
/// and computed through [`corpus::frame_confluent`] rather than by adding five,
/// so the framing's width is read from the framing.
///
/// Computed once per process: it costs a thousand Avro encodes, and the gate
/// asks for it on every arm.
#[must_use]
pub fn corpus_message_bytes() -> u64 {
    static BYTES: OnceLock<u64> = OnceLock::new();
    *BYTES.get_or_init(|| {
        let total: u64 = (0..MESSAGE_SIZE_SAMPLE)
            .map(|batch_id| {
                let datum = corpus::encode_batch(batch_id, corpus::send_ts_us_prefill(batch_id));
                corpus::frame_confluent(0, &datum).len() as u64
            })
            .sum();
        total / MESSAGE_SIZE_SAMPLE
    })
}

/// Whether `measured` and `current` are far enough apart that a rate taken at one
/// cannot be gated against the other.
fn size_differs_materially(measured: u64, current: u64) -> bool {
    if measured == 0 || current == 0 {
        return true;
    }
    let ratio = measured as f64 / current as f64;
    !(1.0 - MESSAGE_BYTES_TOLERANCE..=1.0 + MESSAGE_BYTES_TOLERANCE).contains(&ratio)
}

/// How many times bigger the larger of two sizes is, for a refusal message.
fn size_ratio(a: u64, b: u64) -> f64 {
    let (a, b) = (a as f64, b as f64);
    if a <= 0.0 || b <= 0.0 {
        return 0.0;
    }
    if a > b { a / b } else { b / a }
}

/// A megabyte-per-second figure as bytes per second, where a megabyte is 10^6
/// bytes. Stated rather than assumed because the two conventions differ by 5%,
/// which is the whole tolerance band.
fn mb_to_bytes(mb_per_s: f64) -> u64 {
    (mb_per_s * 1e6).round() as u64
}

// ---------------------------------------------------------------------------
// Insert formats
// ---------------------------------------------------------------------------

/// A ClickHouse insert format this rig can emit, and therefore one an ingest
/// ceiling can exist for.
///
/// The set is small on purpose. Every format here is encoded by this crate, from
/// the committed column list, with no dependency on any system under test — the
/// same rule that makes the harness write its own Avro rather than borrow the
/// framework's. And an encoder is not trusted because it was written carefully;
/// it is trusted because the chain that checks it is mechanical, twice. Every
/// ceiling measurement re-proves its encoder against the live server **in the
/// pass itself**: [`prove_format_lands_correctly`] POSTs a proof block and
/// holds what lands to [`corpus::run_gates`] before any rung is timed, so a
/// format whose bytes the oracle rejects cannot produce a committable figure —
/// not by discipline, but because [`measure_ingest`] refuses before it
/// measures. The Docker-gated
/// `harness/tests/native_encoder_matches_clickhouse.rs` runs the same chain
/// against a fresh pinned server and is the **pre-merge** proof — the reason a
/// new `Format` variant may land in this enum at all — not the only time the
/// proof runs. The encoders exist first so the next ceiling pass can measure
/// the formats the two new benchmark arms will report; until a format's
/// ceiling is committed, an arm declaring it is not gated against ClickHouse
/// at all — the honest consequence stated in the module docs and enforced in
/// [`Ceiling::headroom`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// ClickHouse's own columnar block format. What the headline
    /// `spate:native` arm writes, and the format whose absence left that arm
    /// ungated against the target.
    Native,
    /// Rows only. What `spate:rowbinary` writes.
    RowBinary,
    /// Rows behind a name-and-type header, so the server validates the column
    /// contract rather than trusting position. What the Flink arm writes.
    RowBinaryWithNamesAndTypes,
    /// One JSON object per row, newline-separated — the format everything that
    /// cannot speak a binary protocol falls back to, and the one whose
    /// server-side parse cost is the reason rule 5 refuses to share ceilings
    /// across formats. See [`encode_json_each_row_block`] for the encoding
    /// decisions, each of which exists to keep the text round-trip exact.
    JsonEachRow,
    /// Arrow's IPC streaming format: a schema message, then columnar record
    /// batches. Columnar like Native but in a vocabulary ClickHouse has to
    /// convert on arrival — `Utf8` into `LowCardinality`, `Timestamp` into
    /// `DateTime64` — so its ceiling prices that conversion. See
    /// [`encode_arrow_stream_block`] for the schema mapping and why every
    /// timestamp carries an explicit `"UTC"`.
    ArrowStream,
}

/// Every format the rig measures, in the order it measures them.
pub const FORMATS: [Format; 5] = [
    Format::Native,
    Format::RowBinary,
    Format::RowBinaryWithNamesAndTypes,
    Format::JsonEachRow,
    Format::ArrowStream,
];

impl Format {
    /// The descriptor value this format gates — the `reports.wire_format` an
    /// entrant declares.
    #[must_use]
    pub fn wire_format(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::RowBinary => "rowbinary",
            Self::RowBinaryWithNamesAndTypes => "rowbinary_nt",
            Self::JsonEachRow => "json_each_row",
            Self::ArrowStream => "arrow_stream",
        }
    }

    /// The name ClickHouse knows it by, for the `FORMAT` clause.
    #[must_use]
    pub fn clickhouse_name(self) -> &'static str {
        match self {
            Self::Native => "Native",
            Self::RowBinary => "RowBinary",
            Self::RowBinaryWithNamesAndTypes => "RowBinaryWithNamesAndTypes",
            Self::JsonEachRow => "JSONEachRow",
            Self::ArrowStream => "ArrowStream",
        }
    }

    /// The format an entrant's declared `wire_format` names, if this rig can
    /// measure it.
    ///
    /// `None` is not a parse failure; it is the answer for a format that exists
    /// and is not measurable here — `protobuf`, say, or any other format an arm
    /// could declare and this crate has no encoder for — and the caller's job is
    /// to decline to gate rather than to substitute. It is also the answer for a
    /// near-miss spelling of a format this rig **can** measure: `JSONEachRow`'s
    /// descriptor is `json_each_row`, and `jsoneachrow` deliberately does not
    /// resolve to it, because a matcher that forgave one spelling would be a
    /// second place the descriptor grammar is defined.
    #[must_use]
    pub fn parse(wire_format: &str) -> Option<Self> {
        FORMATS.into_iter().find(|f| f.wire_format() == wire_format)
    }
}

/// Every insert format the ingest pass can measure, in the order it measures
/// them.
///
/// One list rather than a loop written out at the call site, because
/// [`select_combinations`] has to be able to filter it and a test has to be able
/// to assert what a restriction selected without standing up a server.
#[must_use]
pub fn all_combinations() -> Vec<Format> {
    FORMATS.to_vec()
}

/// The formats a `--only` argument names, or all of them when none is given.
///
/// # Why a restriction exists at all
///
/// A full pass measures five formats and takes minutes apiece, which
/// is the right cost for the pass that produces a committed ceiling and the wrong
/// cost for the pass that is one point of a **search**. A search over
/// infrastructure allocations needs the ClickHouse ingest ceiling at each of
/// them, and only the format that actually binds decides it; measuring the
/// others at every point would buy nothing and spend the difference.
///
/// # Why it does not weaken anything
///
/// A restriction changes **which** ceilings a pass measures and never **how**.
/// Every format it does measure goes through the same sweep, the same
/// dedicated ceiling table, the same landed-row check and the same refusals; a
/// format it skips is simply not re-measured, and [`Ceilings::merge`] has
/// always merged by key for exactly that reason. What the restriction must not be
/// allowed to do is leave a *committed* file half-describing one envelope and
/// half another, and that is refused separately — see
/// [`Ceilings::measured_under_other_envelopes`].
///
/// # The grammar
///
/// `--only <format>` selects one format. Repeatable, and duplicates collapse,
/// so `--only rowbinary --only rowbinary` is `--only rowbinary`.
///
/// # Errors
///
/// If a spec names a format this rig cannot emit.
/// A refusal with the available names rather than a silent empty selection: a
/// typo that measured nothing would look exactly like a pass that had nothing to
/// measure.
pub fn select_combinations(specs: &[String]) -> Result<Vec<Format>, String> {
    if specs.is_empty() {
        return Ok(all_combinations());
    }
    let mut chosen: Vec<Format> = Vec::new();
    for spec in specs {
        let format = Format::parse(spec).ok_or_else(|| {
            format!(
                "--only {spec:?} names insert format {spec:?}, which this rig cannot \
                 emit. It measures the formats it can encode itself, from the committed \
                 column list, with no dependency on any system under test: {}.",
                FORMATS
                    .iter()
                    .map(|f| f.wire_format())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
        if !chosen.contains(&format) {
            chosen.push(format);
        }
    }
    // Back into the canonical order, so that a pass's terminal output and its
    // rig string do not depend on the order the flags were typed in.
    Ok(all_combinations()
        .into_iter()
        .filter(|c| chosen.contains(c))
        .collect())
}

/// A restriction as it is recorded on a ceiling's `provenance.rig`.
///
/// Empty when the pass measured everything, so the rig string of a full pass is
/// unchanged. A restricted pass says so, because the rig string's job is to let
/// the pass be reproduced by hand and reproducing this one exactly means typing
/// the same restriction.
fn describe_restriction(chosen: &[Format]) -> String {
    if chosen.len() == all_combinations().len() {
        return String::new();
    }
    chosen
        .iter()
        .map(|f| format!(" --only {}", f.wire_format()))
        .collect()
}

// ---------------------------------------------------------------------------
// The pass
// ---------------------------------------------------------------------------

/// What one ceiling pass is asked to do.
#[derive(Debug, Clone)]
pub struct PassOptions {
    /// Topic holding the prefilled corpus. Read, never written and never
    /// committed against: the corpus lives inside the broker and a ceiling pass
    /// that consumed it destructively would cost every subsequent arm its input.
    pub topic: String,
    /// Seconds each sub-pass runs for.
    ///
    /// Short on purpose for the consume pass — the backlog has to outlast it, or
    /// the figure is the rate of a broker that ran out of work to serve. The
    /// original rig documented the same constraint and refused with `DRAINED`;
    /// so does this one.
    pub seconds: u64,
    /// Consumer threads for the consume pass, and nothing else.
    ///
    /// It used to be the ingest pass's concurrency as well, which is the second
    /// defect in the module docs: this number is bounded by the topic's
    /// partition count, the ingest pass is bounded by nothing of the sort, and
    /// sharing one knob silently starved the target.
    pub consume_threads: u64,
    /// The highest concurrency the ingest sweep may climb to.
    ///
    /// A bound rather than a setting: the operator does not choose what the
    /// target is measured at — choosing it is what produced a ceiling every arm
    /// exceeded — but the operator does decide how far the pass is allowed to go
    /// looking, because every rung costs a window. [`Sweep`] states what happens
    /// when the ladder runs out before the throughput does.
    pub ingest_max_concurrency: u64,
    /// The insert formats the ingest pass measures.
    ///
    /// Everything, unless `--only` narrowed it — see [`select_combinations`] for
    /// why a narrowing exists and what it may not do. Held as a resolved list
    /// rather than as the operator's strings so that the parse, and its refusal
    /// of a format this rig cannot emit, happens once and before a container
    /// starts.
    pub ingest: Vec<Format>,
    /// `YYYY-MM-DD` to stamp on the provenance.
    ///
    /// Supplied by the caller rather than read from a clock here, so that the
    /// ceiling pass has no calendar of its own and every date this CLI writes is
    /// formatted by one routine.
    pub date: String,
}

/// One complete pass: the consume ceiling and every ingest ceiling it measured.
#[derive(Debug, Clone)]
pub struct Pass {
    /// What the broker's fetch path sustained.
    pub consume: ConsumeCeiling,
    /// What ClickHouse absorbed, one entry per format.
    pub ingest: Vec<IngestCeiling>,
}

/// Measures both ceilings against live infrastructure.
///
/// Ordered consume-then-ingest so the two never overlap: a consume pass running
/// while ClickHouse is being hammered would measure the two competing for the
/// host rather than either of them, which is the same host-contention effect
/// that forced the drain-versus-sustained split in the methodology.
///
/// # Errors
///
/// If the corpus is absent or too shallow, if the consume pass drains before its
/// window closes, or if ClickHouse refuses an insert. Every one of those is a
/// refusal rather than a degraded number: a ceiling that quietly came out low
/// would tighten the gate against every arm measured after it.
pub fn measure(
    env: &crate::environment::Environment,
    ep: &Endpoints,
    opts: &PassOptions,
) -> Result<Pass, String> {
    let partitions = env.spec.infra.partitions;
    let infra_digest = env.infra_digest();

    // The declared caps, not ones read here. `infra::bring_up` has already read
    // the applied caps out of the running containers' cgroups and refused the run
    // if either disagreed, so these are the same numbers and there is one place
    // that gets to decide them.
    let broker_cap_cores: f64 = env.spec.infra.broker.cpus.parse().unwrap_or(0.0);
    let cap_cores: f64 = env.spec.infra.clickhouse.cpus.parse().unwrap_or(0.0);

    let consume = measure_consume(ep, opts, partitions, broker_cap_cores)?;
    eprintln!(
        "consume ceiling: {} msgs/s, {:.1} MB/s at {} B/message over {} partitions, {} \
         consumers, {:.3}s window",
        consume.msgs_per_s,
        consume.mb_per_s,
        consume.message_bytes,
        partitions,
        consume.threads,
        consume.window_s,
    );
    eprintln!(
        "  window {:.3}s of a {:.0}s request, {} of {} messages on the topic",
        consume.window.actual_s,
        consume.window.requested_s,
        consume.window.messages_read,
        consume.window.topic_depth,
    );
    if let Some(g) = consume.broker_cgroup {
        eprintln!("  broker {}", describe_cgroup(g));
    }

    let mut ingest = Vec::new();
    for format in opts.ingest.iter().copied() {
        let measured = measure_ingest(ep, opts, format, cap_cores)?;
        eprintln!(
            "clickhouse ceiling: {} — {} rows/s, {:.1} MB/s at {} B/row, {} \
             inserters (swept {})",
            format.wire_format(),
            measured.ceiling.rows_per_s,
            measured.ceiling.mb_per_s,
            measured.ceiling.row_bytes,
            measured.ceiling.threads,
            describe_sweep(&measured.observed.sweep),
        );
        // What this pass observed about its own fidelity, at the terminal as
        // well as in the file. An operator deciding whether to `--write` is the
        // reader these are for, and they would otherwise only reach a reader of
        // the file that had already been written.
        let o = &measured.observed;
        if let Some(g) = o.target_cgroup {
            eprintln!("  target {}", describe_cgroup(g));
        }
        eprintln!(
            "  landed {} of {} rows POSTed",
            o.landed
                .counted
                .map_or_else(|| "?".to_owned(), |c| c.to_string()),
            o.landed.posted,
        );
        if let Some(p) = o.parts {
            eprintln!(
                "  parts {} written carrying {} rows, {} merges over {} rows ({:.0}%)",
                p.parts_written,
                p.rows_written,
                p.merges,
                p.rows_merged,
                percent(p.rows_merged, p.rows_written),
            );
        }
        if let Some(n) = o.network {
            eprintln!(
                "  server read the request body for {:.0}% of {} inserts' duration",
                n.waiting_ms / n.duration_ms * 100.0,
                n.inserts,
            );
        }
        if let Some(r) = &o.stopped_at {
            eprintln!(
                "  ! ladder stopped at {} inserters, refused by the {}: {}",
                r.concurrency,
                match r.refused_by {
                    RefusedBy::Target => "target",
                    RefusedBy::Rig => "rig",
                },
                r.error,
            );
        }
        ingest.push(measured);
    }

    // Note what is NOT here any more: a truncation of the arms' own tables. The
    // rig used to write its measured rows into the very tables the arms are
    // gated on, so it had to tidy up after itself or the next arm's correctness
    // gate would fail against data the arm never produced. The measurement now
    // goes into a table of its own, whose lifetime is a [`CeilingTable`] value.
    // The one write that still touches the arms' table — the pre-sweep proof
    // block, which lands there because [`corpus::run_gates`] queries that table
    // by construction — carries its own [`ProofRows`] guard for exactly the
    // failure mode this comment used to have to explain.

    let b = &env.spec.infra.broker;
    let c = &env.spec.infra.clickhouse;
    let host = env.spec.host.description.clone();
    let provenance = |rig: String| Provenance {
        date: opts.date.clone(),
        dataset_version: crate::report::DATASET_VERSION.to_owned(),
        infra_digest: infra_digest.clone(),
        host: host.clone(),
        rig,
    };

    let consume_rig = format!(
        "bench ceiling --measure --seconds {} --threads {} (raw ListOffsets and Fetch from a \
         container on {}, one forked consumer per thread over an assigned partition split, no \
         consumer group and no offset ever committed; window sized against the backlog at \
         {:.3}s)",
        opts.seconds,
        opts.consume_threads,
        crate::docker::NETWORK,
        consume.window_s,
    );

    Ok(Pass {
        consume: ConsumeCeiling {
            msgs_per_s: consume.msgs_per_s,
            mb_per_s: consume.mb_per_s,
            message_bytes: consume.message_bytes,
            partitions,
            broker: format!("{}, {} CPU", b.image, b.cpus),
            threads: consume.threads,
            client: Location::Inside.name().to_owned(),
            window: Some(consume.window),
            broker_cgroup: consume.broker_cgroup,
            provenance: provenance(consume_rig),
        },
        ingest: ingest
            .into_iter()
            .map(
                |MeasuredIngest {
                     ceiling: mut m,
                     observed,
                 }| {
                    m.clickhouse = format!("{}, {} CPU, {} memory", c.image, c.cpus, c.memory);
                    m.sweep = observed.sweep;
                    m.target_cgroup = observed.target_cgroup;
                    m.landed = Some(observed.landed);
                    m.parts = observed.parts;
                    m.network = observed.network;
                    m.settle = observed.settle;
                    m.stopped_at = observed.stopped_at;
                    m.provenance = provenance(format!(
                        "bench ceiling --measure --seconds {} --ingest-max {}{} \
                         (pre-encoded blocks of {} batches, POSTed concurrently over \
                         HTTP by a container on {} into {}; concurrency swept, best at \
                         {} inserters)",
                        opts.seconds,
                        opts.ingest_max_concurrency,
                        describe_restriction(&opts.ingest),
                        INSERT_BLOCK_BATCHES,
                        crate::docker::NETWORK,
                        ceiling_table(),
                        m.threads
                    ));
                    m
                },
            )
            .collect(),
    })
}

// ---------------------------------------------------------------------------
// The consume pass
// ---------------------------------------------------------------------------
/// What one consume pass observed.
#[derive(Debug, Clone)]
struct ConsumePass {
    msgs_per_s: u64,
    mb_per_s: f64,
    message_bytes: u64,
    /// Consumers the winning window ran with.
    threads: u64,
    /// Seconds the measured window actually ran for.
    ///
    /// Recorded because it is no longer whatever `--seconds` asked for. At the
    /// rate this client reads, the whole corpus is under a second of backlog, so
    /// the window is sized against the backlog and the number that comes out is a
    /// statement about the corpus's depth. A figure whose window is not on the
    /// record cannot be told apart from one whose window was long enough.
    window_s: f64,
    /// What this particular pass observed and a later reader could not
    /// How this window was sized, and against how much backlog.
    window: ConsumeWindow,
    /// What the broker itself spent while it was being read.
    broker_cgroup: Option<Cgroup>,
}

/// The share of the shallowest consumer's backlog one window may consume.
///
/// Seven tenths, and the three left over are not slack for its own sake: the
/// backlog is measured before the window and the rate is measured during it, so
/// the window's length is a *prediction*, and a prediction that lands slightly
/// long against a partition with nothing left in it is a `DRAINED` refusal
/// rather than a slightly worse number. The cost of the margin is a shorter
/// window; the cost of not having it is a pass that has to be re-run.
///
/// The size of the margin is set by how wrong the prediction can be rather than
/// by taste. Measured here, a calibration window reads 3–8% slower than the
/// window it sizes — it pays the same first fetch over a shorter run — so the
/// share has to leave more than that or the corpus runs out. At 0.75 and a
/// 0.15s calibration this pass consumed 94% of the topic, which is a refusal one
/// unlucky fetch away.
const CONSUME_BACKLOG_SHARE: f64 = 0.70;

/// Seconds of calibration used to size the window.
///
/// It costs nothing in backlog terms — every window starts again from each
/// partition's earliest offset, so calibrating does not consume anything the
/// measured window then goes without — but it does cost accuracy when it is too
/// short. A calibration pays for its consumers' first fetch exactly as the
/// measured window does, and over 0.15s that fixed cost put the estimate 22%
/// low, which ate the whole backlog margin above. Long enough to amortise it,
/// short enough that the pass is not two windows long.
const CONSUME_CALIBRATION_S: f64 = 0.30;

/// The shortest window this pass will report a ceiling from.
///
/// Below this the figure stops being a rate and starts being a sample of the
/// first few fetches: at 1.6M messages a second a quarter of a second is still
/// 400,000 messages, but a tenth would put the broker's first fetch of each
/// partition — the one that pays for the connection and the file handle — inside
/// the measurement rather than beside it.
///
/// Reaching it is a **refusal**, and what it refuses is a corpus too shallow to
/// measure this path: the remedy is a deeper prefill, not a longer window.
const CONSUME_WINDOW_MIN_S: f64 = 0.25;

/// Whether this many consumers may be pointed at this many partitions.
///
/// The consume pass assigns partitions round-robin across its threads, so a
/// thread beyond the partition count is assigned nothing at all: it contributes
/// zero messages to the numerator and its window to the denominator, and the
/// figure becomes a property of this rig's arithmetic rather than of the broker.
///
/// The bound is correct and it is **the consume pass's alone**. It is a function
/// of how many fetch streams a topic offers, and the ingest pass has no fetch
/// streams; sharing one `--threads` between the two made the target's ceiling a
/// function of the topic's partition count, which is the second defect in the
/// module docs. Extracted from the pass body so that fact is testable without a
/// broker — the refusal and its non-application to the ingest sweep are both
/// pinned by
/// `the_consume_pass_refuses_more_threads_than_partitions_and_the_ingest_sweep_is_not_bound_by_them`.
///
/// # Errors
///
/// If there are fewer partitions than threads.
fn consume_threads_fit_partitions(threads: u64, partitions: i32) -> Result<(), String> {
    if u64::try_from(partitions).unwrap_or(0) < threads {
        return Err(format!(
            "REFUSED: the ceiling pass was asked for {threads} consumer threads but the topic \
             has {partitions} partitions. A thread with no partitions consumes nothing and \
             drags the aggregate down, so the figure would be a property of this rig's \
             arithmetic rather than of the broker."
        ));
    }
    Ok(())
}

/// Which partitions each consumer reads.
///
/// Round-robin, so that consecutive partitions land on different consumers and
/// no consumer is handed a contiguous run that one broker shard happens to own.
/// It lives here rather than in the container because it is a decision — the
/// partition bound above is the same decision stated as a refusal — and a
/// decision inside a Python program shipped over stdin is a decision no test can
/// reach.
fn partition_split(threads: u64, partitions: i32) -> Vec<Vec<i32>> {
    let threads = usize::try_from(threads.max(1)).unwrap_or(1);
    let mut split = vec![Vec::new(); threads];
    for p in 0..partitions {
        split[usize::try_from(p).unwrap_or(0) % threads].push(p);
    }
    split
}

/// How long a window the backlog can support at a measured rate.
///
/// The consume pass's counterpart of the ingest sweep's ladder bound, and it
/// exists because the two passes are limited by opposite things. The ingest
/// pass's window is bounded by nothing and it may run for as long as it likes;
/// the consume pass's window is bounded by how much backlog is in front of it,
/// and once the client moved inside the bench network that bound stopped being
/// theoretical. At 1.72M messages a second the whole 1,500,000-message corpus is
/// 0.87 seconds of reading, so `--seconds 8` is not a window this corpus can
/// offer — it is seven seconds of measuring an idle broker, which the `DRAINED`
/// refusal correctly declines to report.
///
/// The window is therefore derived rather than taken: the shallowest consumer's
/// backlog, divided by the rate that consumer is reading at — its share of the
/// fetch streams, not of the backlog — times [`CONSUME_BACKLOG_SHARE`], and
/// never longer than the operator asked for.
///
/// `depths` is per partition, `split` is what [`partition_split`] decided, and
/// `msgs_per_s` is the aggregate rate a calibration window observed.
fn backlog_window(
    depths: &[u64],
    split: &[Vec<i32>],
    msgs_per_s: f64,
    requested: Duration,
) -> Duration {
    let assigned = |consumer: &Vec<i32>| -> u64 {
        consumer
            .iter()
            .filter_map(|p| depths.get(usize::try_from(*p).ok()?))
            .sum()
    };
    let total: u64 = depths.iter().sum();
    if msgs_per_s <= 0.0 || total == 0 {
        return requested;
    }
    // A consumer's share of the aggregate rate is its share of the FETCH
    // STREAMS, not its share of the backlog: a partition is served at whatever
    // rate a partition is served at, and one holding fewer messages simply
    // reaches the end of itself sooner. Dividing by the backlog share instead
    // would flatter exactly the case this exists to catch — the shallow
    // partition would be credited with a proportionally slower reader and would
    // appear to last as long as every other.
    let streams: usize = split.iter().map(Vec::len).sum();
    if streams == 0 {
        return requested;
    }
    let soonest = split
        .iter()
        .filter(|c| !c.is_empty())
        .map(|c| {
            let share = c.len() as f64 / streams as f64;
            assigned(c) as f64 / (msgs_per_s * share)
        })
        .fold(f64::INFINITY, f64::min);
    if !soonest.is_finite() {
        return requested;
    }
    Duration::from_secs_f64((soonest * CONSUME_BACKLOG_SHARE).min(requested.as_secs_f64()))
}

/// Reads the prefilled corpus as fast as the broker will serve it, from inside
/// the bench network.
///
/// Ported from the rig this file's predecessor documented
/// (`MODE=split INSTANCES=1 THREADS=4`), with three deliberate departures.
///
/// **It reads the corpus rather than a synthetic payload.** The rig produced
/// three million 840-byte messages and measured those; keeping that shape is
/// precisely how the recorded figure came to describe a message the arms do not
/// read. The ceiling has to be measured against the bytes the arms actually
/// fetch, so this pass requires a prefilled corpus and refuses without one.
///
/// **Partitions are assigned explicitly rather than subscribed to.** A group
/// subscription spends its first seconds in a rebalance, and a window that
/// contains a rebalance understates the ceiling — which would make the gate
/// stricter, but for a reason that has nothing to do with the broker. Assignment
/// also means no group is joined and no offset is ever committed, so the pass
/// cannot disturb the corpus every arm is about to replay.
///
/// **It runs inside the bench network**, through [`crate::fetcher`], because
/// every arm consumes container-to-container and a figure taken through Docker's
/// published port is a property of that port. That module holds the evidence and
/// the alternatives considered; the short version is that the same corpus, the
/// same broker and the same window read 72,349 messages a second from the host
/// and 1,719,373 from a container on the bench network.
///
/// # Errors
///
/// If the topic is empty, if the topic does not hold this corpus, if fewer
/// partitions exist than threads were asked for, if the backlog cannot support a
/// window worth measuring, or if a partition runs dry before the window closes.
fn measure_consume(
    ep: &Endpoints,
    opts: &PassOptions,
    partitions: i32,
    cap_cores: f64,
) -> Result<ConsumePass, String> {
    let threads = opts.consume_threads.max(1);
    consume_threads_fit_partitions(threads, partitions)?;
    let split = partition_split(threads, partitions);

    // Removed by its own `Drop`, so every way out of this function takes it with
    // it — including the four that are refusals.
    let mut fetcher = crate::fetcher::Fetcher::start(ep, &opts.topic, partitions)?;
    let depths = fetcher.depths().to_vec();
    let depth: u64 = depths.iter().sum();
    if depth == 0 {
        return Err(format!(
            "REFUSED: {} holds no messages. The consume ceiling is measured against the \
             corpus's own bytes, so that it describes exactly what the arms read; run \
             `bench prefill` first.",
            opts.topic
        ));
    }
    eprintln!(
        "consume pass: {depth} messages on {} over {partitions} partitions, {threads} consumers \
         inside {}",
        opts.topic,
        crate::docker::NETWORK,
    );

    // Calibration first, and it is not free evidence — it is the only way to
    // know how long a window the backlog can support. See [`backlog_window`].
    let requested = Duration::from_secs(opts.seconds.max(1));
    let calibration = fetcher.burst(&split, Duration::from_secs_f64(CONSUME_CALIBRATION_S))?;
    let calibrated = calibration.msgs as f64 / calibration.elapsed_s;
    let window = backlog_window(&depths, &split, calibrated, requested);
    eprintln!(
        "  calibration: {:.0} msgs/s over {:.3}s — the backlog supports a {:.3}s window \
         ({:.0}s requested)",
        calibrated,
        calibration.elapsed_s,
        window.as_secs_f64(),
        requested.as_secs_f64(),
    );
    if window.as_secs_f64() < CONSUME_WINDOW_MIN_S {
        return Err(format!(
            "REFUSED: {} holds {depth} messages and this client reads {calibrated:.0} of them a \
             second from inside {}, so the whole corpus is {:.2}s of backlog and the longest \
             window it can support is {:.3}s — under the {CONSUME_WINDOW_MIN_S}s a rate can \
             honestly be taken over. That is a statement about the corpus rather than about the \
             broker, and the remedy is a deeper prefill rather than a shorter window: a window \
             this short would put the broker's first fetch of each partition inside the \
             measurement.",
            opts.topic,
            crate::docker::NETWORK,
            depth as f64 / calibrated,
            window.as_secs_f64(),
        ));
    }

    // Either side of the window and of nothing else, so the CPU it reports is
    // this window's rather than this window's plus the calibration before it.
    let before = crate::infra::cgroup_cpu(&ep.broker_container);
    let at = Instant::now();
    let read = fetcher.burst(&split, window)?;
    let server = server_cost(before, &ep.broker_container, at.elapsed(), cap_cores);
    // Stopped before anything is reported, so nothing is still reading the
    // broker the ingest pass is about to be measured beside.
    drop(fetcher);

    if read.msgs == 0 || read.elapsed_s <= 0.0 {
        return Err("REFUSED: the consume pass read no messages".to_owned());
    }

    // What the topic actually holds, not what the generator says it would hold.
    // A ceiling measured against some other topic's bytes is the defect this
    // module exists to close, so the pass checks rather than assumes.
    let message_bytes = read.bytes / read.msgs;
    let expected = corpus_message_bytes();
    if size_differs_materially(message_bytes, expected) {
        return Err(format!(
            "REFUSED: {} holds {message_bytes}-byte messages but this corpus produces \
             {expected}-byte ones. The ceiling would not describe the bytes the arms read. \
             Delete the topic and re-run `bench prefill`.",
            opts.topic
        ));
    }

    let msgs_per_s = (read.msgs as f64 / read.elapsed_s).round() as u64;

    Ok(ConsumePass {
        msgs_per_s,
        mb_per_s: read.bytes as f64 / read.elapsed_s / 1e6,
        message_bytes,
        threads,
        window_s: read.elapsed_s,
        window: ConsumeWindow {
            requested_s: requested.as_secs_f64(),
            actual_s: read.elapsed_s,
            messages_read: read.msgs,
            topic_depth: depth,
            calibrated_msgs_per_s: calibrated.round() as u64,
        },
        broker_cgroup: server.map(Cgroup::from),
    })
}

impl From<ServerCost> for Cgroup {
    fn from(s: ServerCost) -> Self {
        Self {
            cores: s.cores,
            cap_cores: s.cap_cores,
            user_share: percent(s.user_us, s.user_us + s.system_us) / 100.0,
            nr_throttled: s.nr_throttled,
            throttled_us: s.throttled_us,
        }
    }
}

// ---------------------------------------------------------------------------
// The ClickHouse ingest pass
// ---------------------------------------------------------------------------

/// Batches encoded into one insert block. A thousand batches is 73,500 rows
/// after the workload's filters, which is the order of block a real arm sends.
const INSERT_BLOCK_BATCHES: u64 = 1_000;
/// Distinct blocks pre-encoded before the clock starts.
///
/// Distinct rather than one block re-sent, because a single block would give
/// every insert the same handful of `LowCardinality` dictionary entries and the
/// same array shapes, and the target would absorb it faster than it absorbs the
/// corpus. Eight blocks cover eight thousand batches, which spans every modulus
/// the generator's derivations use.
const INSERT_BLOCK_POOL: u64 = 8;
/// Seconds one insert may take before the pass gives up on it.
const INSERT_TIMEOUT_S: u64 = 120;

/// Batches in the proof block every ingest measurement POSTs, and gates,
/// before its sweep is allowed to start. See [`prove_format_lands_correctly`].
///
/// The same width the Docker-gated live test uses, for the same reason: it is
/// chosen to cross the `LowCardinality` index-width boundary rather than for
/// speed. `sensor` is `batch_id % 1024`, so 400 batches put more than 256
/// distinct entries in the dictionary — where the indexes step from one byte
/// to two — and [`corpus::run_gates`] excludes the boundary batches, leaving
/// 398 gated and roughly 29,000 rows after the workload's filters. A narrower
/// proof would exercise only the narrow branch and pass with the dangerous one
/// broken.
const PROOF_BATCHES: u64 = 400;

/// The concurrency the ingest sweep's first rung runs at.
///
/// Below the shape of any arm this ceiling gates — the Flink arm keeps two
/// requests in flight and the Spate arm four shards times four — so the ladder
/// is guaranteed to contain at least one rung the target is demonstrably not
/// saturated at. That rung buys nothing for the figure itself and everything for
/// the reader: a recorded sweep that starts already flat cannot distinguish "the
/// target saturates here" from "this rig saturates here".
const INGEST_CONCURRENCY_FROM: u64 = 2;

/// What each rung multiplies the last by.
///
/// Doubling, for two reasons. It bounds the pass at
/// `log2(max / from) + 1` rungs — six at today's constants, a window each —
/// where a linear ladder fine enough to be interesting would cost tens. And it
/// keeps each rung's step far larger than this host's noise: the environment
/// profile records run-to-run spread reaching 14.5% on throughput, so a ladder
/// whose rungs were 10% apart would be reading noise as signal in both
/// directions.
///
/// The cost of a coarse ladder is that the winning rung is the best of a handful
/// of powers of two rather than the true optimum, so the figure can be slightly
/// below what a perfectly tuned inserter would extract. That is the strict
/// direction — a slightly low ceiling refuses results, a high one publishes
/// infra-bound arms as system comparisons — and it is the same trade this module
/// makes everywhere else.
const INGEST_CONCURRENCY_STEP: u64 = 2;

/// How far the sweep may climb before the pass gives up looking.
///
/// Something has to bound it or a target that improves by a hair at every rung
/// would sweep until the host ran out of threads. The pass does **not** quietly
/// report its last sample when it gets there: see [`Sweep::still_climbing`].
///
/// It was 64, chosen as "well above every arm's shape" — the Flink arm keeps two
/// requests in flight and the Spate arm four shards times four — and that
/// reasoning was about the arms rather than about the target, which is why it
/// stopped being true the moment the inserter moved inside the bench network. On
/// the first pass from inside, Native was still climbing at 64
/// (5.97M rows/s at 2, then 6.31M, 6.69M, 9.31M, 13.92M, 14.91M) and the pass
/// correctly refused to publish a floor as a ceiling. It plateaus at 64–128 and
/// falls back at 256, so this is two doublings past the highest winner any
/// format has shown — enough ladder for the plateau to be demonstrated
/// rather than assumed, and still far under the server's
/// `max_concurrent_queries`.
///
/// Public because `bench ceiling --ingest-max` defaults to it, and a default
/// spelled twice is a default that comes to differ.
pub const INGEST_CONCURRENCY_MAX: u64 = 256;

/// How much a rung must beat the incumbent best by to count as an improvement.
///
/// Three percent, and it is deliberately far *below* this host's 14.5% noise
/// rather than above it. The two errors are not symmetric. Setting the margin
/// above the noise would stop the sweep at the first genuine-but-modest gain,
/// producing exactly the too-low ceiling this sweep exists to prevent; setting
/// it low means noise occasionally buys one more rung, which costs one window
/// and nothing else. Termination is guaranteed by [`INGEST_CONCURRENCY_MAX`]
/// rather than by this number, which is what lets it be this small.
///
/// The same margin decides the figure: a rung inside it is not an improvement,
/// so it neither continues the sweep nor becomes the ceiling. The figure is
/// therefore understated by at most this much — the strict direction again.
const INGEST_SWEEP_MARGIN: f64 = 0.03;

/// Consecutive rungs that must fail to improve before the sweep calls it a
/// plateau.
///
/// Two, because one is noise. At 14.5% spread a single rung landing below its
/// predecessor says nothing at all, and a "first non-improvement wins" rule
/// would stop one rung after the first unlucky sample — which is how a rig
/// reports a ceiling the arms it gates go on to exceed.
const INGEST_SWEEP_PATIENCE: usize = 2;

/// Milliseconds of quiet between a settled table and the rung timed against it.
///
/// A `TRUNCATE` drops the previous rung's parts asynchronously, and a rung that
/// started into that cleanup would be charged for the rung before it. Since the
/// ladder is climbed in ascending order, that error would fall entirely on the
/// higher rungs and would manufacture a plateau out of the rig's own tidying.
///
/// Seconds [`settle`] will wait for a truncated table to have no parts and no
/// merges before giving up on the wait.
///
/// The concurrency ladder, and the decision about when to stop climbing it.
///
/// Split out from the measurement so the rule is testable without a server. What
/// it decides is the difference between a ceiling and a floor, and a rule that
/// can only be exercised by a ten-minute pass against live infrastructure is a
/// rule that gets exercised once.
///
/// The rule, in full:
///
/// * The ladder starts at [`INGEST_CONCURRENCY_FROM`] and multiplies by
///   [`INGEST_CONCURRENCY_STEP`] until it would pass the bound, which it then
///   tries exactly — so `--ingest-max 24` really does try 24 rather than
///   stopping at 16 and calling the operator's number a suggestion.
/// * A rung becomes the incumbent best only by beating it by more than
///   [`INGEST_SWEEP_MARGIN`]; anything less is a miss.
/// * [`INGEST_SWEEP_PATIENCE`] consecutive misses is a plateau, and the sweep
///   stops.
/// * If the ladder runs out first and the incumbent best is the last rung tried,
///   throughput was still climbing when the rig stopped asking. That is not a
///   ceiling and [`Sweep::still_climbing`] says so; the caller refuses rather
///   than publishing the last sample.
#[derive(Debug)]
struct Sweep {
    step: u64,
    max: u64,
    /// The next rung to run, or `None` once the sweep is over.
    next: Option<u64>,
    points: Vec<SweepPoint>,
    /// Index into `points` of the incumbent best.
    best: usize,
    /// Consecutive rungs since the incumbent was set.
    misses: usize,
}

impl Sweep {
    /// A sweep that will climb from `from` to `max`.
    fn new(from: u64, step: u64, max: u64) -> Self {
        let max = max.max(1);
        Self {
            step: step.max(2),
            max,
            next: Some(from.clamp(1, max)),
            points: Vec::new(),
            best: 0,
            misses: 0,
        }
    }

    /// The next concurrency to measure, or `None` when the sweep is done.
    fn next_rung(&self) -> Option<u64> {
        self.next
    }

    /// Records what the target absorbed at `concurrency` and decides whether to
    /// keep climbing.
    fn observe(&mut self, concurrency: u64, rows_per_s: u64) {
        self.points.push(SweepPoint {
            concurrency,
            rows_per_s,
        });
        let improved = self.points.len() == 1
            || rows_per_s as f64
                > self.points[self.best].rows_per_s as f64 * (1.0 + INGEST_SWEEP_MARGIN);
        if improved {
            self.best = self.points.len() - 1;
            self.misses = 0;
        } else {
            self.misses += 1;
        }
        // Two ways to run out: the plateau the sweep is looking for, and the
        // bound that guarantees it terminates against a target that never
        // plateaus. They are not the same outcome — [`Sweep::still_climbing`] is
        // what tells the caller which one happened — but they end the ladder
        // identically.
        self.next = if self.misses >= INGEST_SWEEP_PATIENCE || concurrency >= self.max {
            None
        } else {
            Some(concurrency.saturating_mul(self.step).min(self.max))
        };
    }

    /// Every rung tried, in the order they were tried.
    fn points(&self) -> &[SweepPoint] {
        &self.points
    }

    /// The rung whose figure this ceiling is, if any rung ran.
    fn best(&self) -> Option<SweepPoint> {
        self.points.get(self.best).copied()
    }

    /// Whether the sweep ended with its best figure at the highest concurrency
    /// it tried.
    ///
    /// True means the ladder ran out before the throughput did. The winning
    /// figure is then a floor — the most this rig was allowed to ask for — and
    /// publishing it as a ceiling would gate arms against a number nothing
    /// showed the target could not beat.
    fn still_climbing(&self) -> bool {
        !self.points.is_empty() && self.best + 1 == self.points.len()
    }
}

/// A sweep's rungs and figures, for a log line, a rig string and a note.
///
/// One spelling, used by all three, because a reader comparing the terminal
/// against the committed file must not have to work out whether two differently
/// worded summaries describe the same ladder.
fn describe_sweep(points: &[SweepPoint]) -> String {
    if points.is_empty() {
        return "no sweep recorded".to_owned();
    }
    points
        .iter()
        .map(|p| format!("{} -> {} rows/s", p.concurrency, p.rows_per_s))
        .collect::<Vec<_>>()
        .join(", ")
}

/// One pre-encoded block, ready to POST.
///
/// Public, with public fields, because the only honest proof that the encoders
/// in this module are correct is a live ClickHouse accepting their bytes and the
/// corpus's own closed-form expectations agreeing with what landed. That proof
/// is `harness/tests/native_encoder_matches_clickhouse.rs`, which is a separate
/// crate and can therefore only reach what this one exports. Exposing the block
/// rather than a bespoke test hook is what keeps the test checking the bytes the
/// measurement pass actually sends.
#[derive(Debug, Clone)]
pub struct Block {
    /// The encoded block, exactly as it goes on the wire.
    pub body: Vec<u8>,
    /// Rows it carries.
    pub rows: u64,
}

/// One measured ingest ceiling and the sweep that produced it.
///
/// The ladder travels beside the ceiling rather than inside it because
/// [`IngestCeiling`] is the committed file's shape and [`measure`] is the only
/// function that may write provenance — it is the only one holding the
/// environment profile, and one writer is what keeps a ceiling's caps and its
/// numbers from disagreeing.
#[derive(Debug, Clone)]
struct MeasuredIngest {
    ceiling: IngestCeiling,
    /// What this particular pass observed and a later reader could not
    /// reconstruct: the shape of the ladder, what the target did with the rows,
    /// and what the server says it was waiting on. Folded into the ceiling by
    /// [`measure`].
    observed: Observed,
}

/// The readings one ingest pass takes about itself.
#[derive(Debug, Clone, Default)]
struct Observed {
    sweep: Vec<SweepPoint>,
    target_cgroup: Option<Cgroup>,
    landed: Landed,
    parts: Option<PartLog>,
    settle: Option<Settle>,
    network: Option<NetworkWait>,
    stopped_at: Option<Refusal>,
}

/// What one rung of the sweep POSTed, how long it took, and what the server did
/// with it.
#[derive(Debug, Clone, Copy)]
struct Burst {
    rows: u64,
    bytes: u64,
    elapsed_s: f64,
    /// Rows the table actually held when the rung ended.
    ///
    /// Read back rather than assumed to equal `rows`. It used to be far smaller
    /// — the rig's repeated blocks were deduplicated at commit — and the whole
    /// point of the dedicated ceiling table is that the two now agree. So it is
    /// checked rather than merely reported: see [`landed_note`]. `None` when the
    /// count could not be read, which is a missing caveat rather than a failed
    /// measurement.
    landed: Option<u64>,
    /// The target's own cgroup CPU over exactly this rung, and its cap.
    ///
    /// The reading that answers "is this the target's ceiling or the rig's?"
    /// without asking the rig. `None` when the counters could not be read.
    server: Option<ServerCost>,
    /// Seconds [`settle`] waited for the table to be empty and quiet before this
    /// rung was timed.
    settle_s: f64,
}

/// What the target spent while one rung ran.
#[derive(Debug, Clone, Copy)]
struct ServerCost {
    /// Mean cores occupied over the rung.
    cores: f64,
    /// The container's declared cap, so a share can be stated rather than left
    /// for a reader to divide.
    cap_cores: f64,
    /// User-mode share of the CPU it spent.
    user_us: u64,
    /// Kernel-mode share.
    system_us: u64,
    /// CFS periods in which the cap actually bit.
    nr_throttled: u64,
    /// Microseconds spent throttled by it.
    throttled_us: u64,
}

impl ServerCost {
    /// Cores as a share of the cap, where 1.0 is the cap itself.
    fn share_of_cap(self) -> f64 {
        if self.cap_cores > 0.0 {
            self.cores / self.cap_cores
        } else {
            0.0
        }
    }
}

impl Burst {
    fn rows_per_s(self) -> u64 {
        (self.rows as f64 / self.elapsed_s).round() as u64
    }

    fn mb_per_s(self) -> f64 {
        self.bytes as f64 / self.elapsed_s / 1e6
    }

    fn row_bytes(self) -> u64 {
        self.bytes / self.rows.max(1)
    }
}

/// Rows the pre-sweep proof leaves in the arms' own table, gone again for as
/// long as this value says so.
///
/// The proof has to land in [`corpus::TABLE`] because that is the table
/// [`corpus::run_gates`] queries — the oracle is shared with every published
/// arm precisely by not being parameterised — and the arms' table is not this
/// pass's to leave rows in. A `Drop` truncation rather than a call at the end
/// of [`prove_format_lands_correctly`], because that function refuses on four
/// paths and a refusal that left 29,000 proof rows behind would fail the next
/// arm's correctness gate against data the arm never produced — the exact
/// failure mode moving the measurement into [`CeilingTable`] closed.
#[derive(Debug)]
struct ProofRows<'a> {
    ep: &'a Endpoints,
}

impl Drop for ProofRows<'_> {
    fn drop(&mut self) {
        // Best-effort by design, like `CeilingTable::remove`: this is
        // housekeeping, and the pass has already succeeded or refused on the
        // merits by the time it runs.
        let _ = docker::try_clickhouse_sql(
            &self.ep.ch_host,
            self.ep.ch_port,
            &self.ep.ch_user,
            &self.ep.ch_password,
            &format!("TRUNCATE TABLE IF EXISTS {}", corpus::TABLE),
        );
    }
}

/// Re-proves one format's encoder against the live target, on every
/// measurement, before a single rung is timed.
///
/// One block of [`PROOF_BATCHES`] batches is POSTed into the arms' own table
/// behind the same statement shape the sweep sends, and what lands is held to
/// [`corpus::run_gates`] — the same closed-form oracle every published arm is
/// gated on, covering row identity, both value sums, the string and tag
/// fingerprints, the `DateTime64` scaling and the null pattern. A measurement
/// whose bytes fail that oracle is refused here rather than recorded: the
/// Docker-gated `harness/tests/native_encoder_matches_clickhouse.rs` proves
/// each encoder before its format is merged, but a pre-merge proof cannot see
/// the server this pass is actually measuring, and a ceiling whose rows the
/// gates would reject is a rate for an insert no correct arm performs.
///
/// The proof goes through [`corpus::TABLE`] rather than the ceiling table
/// because `run_gates` queries the former by construction. That table's
/// deduplication window is live, unlike the ceiling table's, and it does not
/// matter here: the block is POSTed once, so there is no repeat for the window
/// to drop — which the landed-count check below would catch if it ever changed.
///
/// # Errors
///
/// If the truncation or the insert is refused, if fewer rows landed than were
/// POSTed, or if the gates fail or find duplicates. Every one is a refusal of
/// this format's measurement before it costs a window.
fn prove_format_lands_correctly(ep: &Endpoints, format: Format) -> Result<(), String> {
    // Constructed before the table is touched, so every failure path below
    // unwinds through the truncation.
    let _rows = ProofRows { ep };
    let sql = |s: &str| {
        docker::try_clickhouse_sql(&ep.ch_host, ep.ch_port, &ep.ch_user, &ep.ch_password, s)
    };
    let body = sql(&format!("TRUNCATE TABLE IF EXISTS {}", corpus::TABLE))
        .map_err(|e| format!("truncate {} for the encoder proof: {e}", corpus::TABLE))?;
    if body.contains("DB::Exception") {
        return Err(format!(
            "truncate {} for the encoder proof: {body}",
            corpus::TABLE
        ));
    }

    let block = encode_batches(format, 0, PROOF_BATCHES);
    insert(
        &ep.ch_host,
        ep.ch_port,
        &ep.ch_user,
        &ep.ch_password,
        &insert_sql(format),
        &block.body,
    )
    .map_err(|e| {
        format!(
            "REFUSED: the target rejected this rig's {} proof block, so nothing this \
             pass could measure for that format would describe an insert that works: {e}",
            format.wire_format(),
        )
    })?;

    let landed = landed_rows(ep, corpus::TABLE)
        .ok_or_else(|| format!("counting the {} proof rows failed", format.wire_format()))?;
    if landed != block.rows {
        return Err(format!(
            "REFUSED: the target accepted the {} proof block and landed {landed} of its \
             {} rows. A block the server reads as a different number of rows than it \
             holds is mis-encoded in a way the gates cannot even reach.",
            format.wire_format(),
            block.rows,
        ));
    }

    let gates = corpus::run_gates(
        &ep.ch_host,
        ep.ch_port,
        &ep.ch_user,
        &ep.ch_password,
        PROOF_BATCHES,
    )
    .map_err(|e| {
        format!(
            "the {} proof block could not be gated: {e}",
            format.wire_format()
        )
    })?;
    if let Some(why) = gates.failure() {
        return Err(format!(
            "REFUSED: the {} proof block landed rows the corpus gates reject — {why}. A \
             ceiling measured through bytes the oracle refuses would be a rate for an \
             insert no correct arm performs.",
            format.wire_format(),
        ));
    }
    if gates.duplicates != 0 {
        return Err(format!(
            "REFUSED: one {} proof block cannot legitimately duplicate a row, and the \
             gates counted {} duplicates.",
            format.wire_format(),
            gates.duplicates,
        ));
    }
    Ok(())
}

/// Measures how fast ClickHouse absorbs rows for one format, at the
/// concurrency it absorbs them fastest at.
///
/// Uses the committed DDL's table and the committed positional column list, so
/// the ceiling describes the same insert the arms perform rather than a
/// simplified one. The rows are the corpus's own rows, which matters more than
/// it looks: every string column in the target is `LowCardinality`, and
/// synthetic rows with a single repeated value would make the dictionary
/// encoding almost free and overstate the ceiling.
///
/// The concurrency is swept rather than supplied. What it costs is a window per
/// rung; what it buys is the difference between "this is what ClickHouse
/// absorbs" and "this is what ClickHouse absorbed when POSTed at whatever number
/// the consume pass happened to be using" — which was the previous shape, and
/// which produced a ceiling every published arm exceeded. See [`Sweep`].
///
/// The block pool is encoded once and shared by every rung. Encoding is by far
/// the most expensive part of a rung and re-encoding per rung would make a sweep
/// unaffordable, but the deeper reason is fidelity: every rung must POST the
/// same bytes, or the ladder would be comparing concurrencies **and** payloads
/// and could not attribute a difference to either. It is handed to the inserter
/// once, before any rung is timed, for the same reason it was pre-encoded in the
/// first place.
///
/// Two things about *where* this happens are load-bearing, and both are the
/// third and fourth defects in the module docs:
///
/// * The POSTs come from a container on the bench network, via
///   [`crate::inserter`], because every arm inserts container-to-container and a
///   figure taken through Docker's published port is a property of that port.
/// * They go into a table of this pass's own, created by [`ceiling_table_ddl`]
///   from the committed DDL with deduplication turned off, because the arms'
///   table deduplicated the rig's repeated blocks and so charged it for
///   everything except the merging of rows that stay.
///
/// # Errors
///
/// If the pre-sweep proof finds the encoder's landed rows failing the corpus
/// gates, if the ceiling table cannot be created or settled, if ClickHouse
/// refuses an insert at the first rung, if the rows a rung POSTed did not land,
/// or if the sweep reaches its bound while still improving — the last of which
/// is a refusal precisely because the alternative is publishing a floor as a
/// ceiling.
fn measure_ingest(
    ep: &Endpoints,
    opts: &PassOptions,
    format: Format,
    cap_cores: f64,
) -> Result<MeasuredIngest, String> {
    // Before the pool is even encoded: a format whose bytes the gates refuse
    // has no measurement worth the minutes a sweep costs.
    prove_format_lands_correctly(ep, format)?;
    eprintln!(
        "  {}: encoder proven against the live target — one {PROOF_BATCHES}-batch block \
         landed and passed the corpus gates",
        format.wire_format(),
    );

    let pool: Vec<Block> = (0..INSERT_BLOCK_POOL)
        .map(|k| encode_block(format, k * INSERT_BLOCK_BATCHES))
        .collect();
    // Removed by its own `Drop`, so every way out of this function takes it with
    // it — including the four that are refusals.
    let ceiling_table = CeilingTable::create(ep)?;
    let table = ceiling_table.name();
    let sql = insert_sql_into(table, format);
    let window = Duration::from_secs(opts.seconds.max(1));

    // Started once for the whole ladder, so the pool crosses the pipe once and
    // every rung POSTs bytes that are not merely equal but the same.
    let mut inserter = crate::inserter::Inserter::start(ep, &sql, &pool)?;

    let mut sweep = Sweep::new(
        INGEST_CONCURRENCY_FROM,
        INGEST_CONCURRENCY_STEP,
        opts.ingest_max_concurrency,
    );
    let mut bursts: Vec<(u64, Burst)> = Vec::new();
    let mut undrivable: Option<(u64, String)> = None;
    let swept_from = Instant::now();
    while let Some(concurrency) = sweep.next_rung() {
        // Every rung against the same target state: an empty table, no parts and
        // no merge in flight. Truncating only once, at the start, would charge
        // each rung for the parts every rung before it left behind — and since
        // the ladder ascends, that error lands entirely on the high rungs and
        // invents a plateau.
        let settle_s = settle(ep, table)?;

        // Either side of the rung and of nothing else, so the CPU it reports is
        // this rung's rather than this rung's plus the settle it followed.
        let before = crate::infra::cgroup_cpu(&ep.ch_container);
        let at = Instant::now();
        let burst = match inserter.burst(concurrency, window) {
            Ok(b) => {
                let server = server_cost(before, &ep.ch_container, at.elapsed(), cap_cores);
                Burst {
                    rows: b.rows,
                    bytes: b.bytes,
                    elapsed_s: b.elapsed_s,
                    // Counted before the next rung truncates it away.
                    landed: landed_rows(ep, table),
                    server,
                    settle_s,
                }
            }
            // A rung that this rig cannot drive ends the ladder rather than the
            // pass — but only once a lower rung has POSTed the same bytes
            // successfully. That distinction is the whole safety of this arm of
            // the match: every rung sends byte-identical blocks, so a failure
            // after a success cannot be the encoder and can only be about the
            // concurrency. A failure at the FIRST rung is the encoder, or the
            // server, or the DDL, and it is refused exactly as before.
            //
            // It really happens, and it is not a hypothetical: at 64 inserters
            // the host-side rig starved a connection until ClickHouse gave up
            // reading the request body (`SOCKET_TIMEOUT` after 30s). Failing the
            // whole pass there would have thrown away five correctly measured
            // ceilings to report that the rig cannot saturate its own transport.
            Err(e) if !bursts.is_empty() => {
                eprintln!(
                    "  {}: {concurrency} inserters — UNDRIVABLE, ending the sweep: {e}",
                    format.wire_format(),
                );
                undrivable = Some((concurrency, e));
                break;
            }
            Err(e) => return Err(e),
        };
        eprintln!(
            "  {}: {concurrency} inserters — {} rows/s, {:.1} MB/s{}, {} landed, \
             settled in {:.1}s{}",
            format.wire_format(),
            burst.rows_per_s(),
            burst.mb_per_s(),
            describe_server(burst),
            burst
                .landed
                .map_or_else(|| "?".to_owned(), |n| n.to_string()),
            burst.settle_s,
            describe_landed(burst),
        );
        sweep.observe(concurrency, burst.rows_per_s());
        bursts.push((concurrency, burst));
    }
    let swept_s = swept_from.elapsed().as_secs() + 1;

    let best = sweep.best().ok_or_else(|| {
        format!(
            "REFUSED: the {} ingest sweep ran no rungs",
            format.wire_format(),
        )
    })?;
    if sweep.still_climbing() {
        return Err(format!(
            "REFUSED: the {} ingest sweep was still climbing when it ran out of \
             ladder — its best figure is at {}, the highest concurrency it tried ({}){}. A \
             figure the rig was never shown to be unable to beat is a floor, not a ceiling, \
             and gating arms against it would mark honest arms infra_bound against this \
             rig's own limit. Re-run with a higher `--ingest-max`.",
            format.wire_format(),
            best.concurrency,
            describe_sweep(sweep.points()),
            undrivable.map_or_else(String::new, |(c, e)| format!(
                ", and the rung at {c} could not be driven at all ({e})"
            )),
        ));
    }
    let burst = bursts
        .iter()
        .find(|(c, _)| *c == best.concurrency)
        .map(|(_, b)| *b)
        .ok_or_else(|| "the winning rung was not measured".to_owned())?;

    // Stopped before anything is read back, so the counts and the logs below
    // describe a target nobody is still POSTing at.
    drop(inserter);

    let observed = Observed {
        sweep: sweep.points().to_vec(),
        target_cgroup: burst.server.map(Cgroup::from),
        landed: check_landed(burst, table)?,
        parts: read_part_log(ep, table, swept_s),
        settle: settle_reading(&bursts),
        network: read_network_wait(ep, table, swept_s),
        stopped_at: undrivable.map(|(concurrency, e)| Refusal {
            concurrency,
            refused_by: if target_refused(&e) {
                RefusedBy::Target
            } else {
                RefusedBy::Rig
            },
            error: e,
        }),
    };
    // Dropped here rather than at the closing brace, so that it is gone before
    // the next combination starts encoding its pool — and read as a statement
    // that every question above has now been asked of it.
    drop(ceiling_table);

    Ok(MeasuredIngest {
        ceiling: IngestCeiling {
            format: format.wire_format().to_owned(),
            rows_per_s: burst.rows_per_s(),
            mb_per_s: burst.mb_per_s(),
            row_bytes: burst.row_bytes(),
            threads: best.concurrency,
            // Left empty here and filled by [`measure`], which is the only place
            // that has the environment profile. Recording provenance in the
            // function that also knows the caps is what keeps the two from
            // disagreeing — the failure mode this whole file exists to close.
            clickhouse: String::new(),
            // Stated here rather than by [`measure`], because it is this
            // function's own fact: the POSTs came from a container on the bench
            // network, via [`crate::inserter`], and nothing above this line
            // could tell.
            client: Location::Inside.name().to_owned(),
            // Every field below is filled by [`measure`] from `observed`.
            sweep: Vec::new(),
            target_cgroup: None,
            landed: None,
            parts: None,
            network: None,
            settle: None,
            stopped_at: None,
            provenance: Provenance::default(),
        },
        observed,
    })
}

/// Whether a refused rung was the target saying no rather than the rig failing.
///
/// The distinction decides what a reader is told about the top of the ladder,
/// and the two are opposite findings: "too many parts" is ClickHouse declining
/// work it cannot merge fast enough, which is the ceiling doing its job, while a
/// socket the rig could not open is the rig's own limit and says nothing about
/// the target at all.
fn target_refused(error: &str) -> bool {
    ["TOO_MANY_PARTS", "MEMORY_LIMIT_EXCEEDED", "TOO_MANY_"]
        .iter()
        .any(|code| error.contains(code))
}

/// What the winning rung landed against what it POSTed — and a refusal when the
/// two disagree.
///
/// Rows that land are only worth landing, for a ceiling's purposes, if the target
/// does the merging they cause. A count below what was POSTed means the rig's
/// repeated blocks were deduplicated at commit, so the figure would exclude the
/// background merging of rows that stay — which an arm pays in full — and would
/// point the ceiling HIGH and the gate LENIENT. The ceiling table is created with
/// that window at zero for exactly this reason.
fn check_landed(burst: Burst, table: &str) -> Result<Landed, String> {
    if let Some(landed) = burst.landed
        && landed < burst.rows
    {
        return Err(format!(
            "REFUSED: the winning rung POSTed {} rows into {table} and only {landed} were \
             still there when it ended — {:.1}%. That is deduplication: the rig cycles a \
             pool of {} pre-encoded blocks, and a table with a non-zero \
             non_replicated_deduplication_window writes each repeat's part and drops it at \
             commit. The ceiling table is created with that window at zero, so this means \
             the DDL or the transformation that derives it has changed.",
            burst.rows,
            landed as f64 / burst.rows as f64 * 100.0,
            INSERT_BLOCK_POOL,
        ));
    }
    Ok(Landed {
        posted: burst.rows,
        counted: burst.landed,
    })
}

/// How long the target needed to be quiet again between rungs.
///
/// Whether the next rung starts against a clean target is something to ask the
/// server rather than to assume: a rung leaves thousands of parts and a merge
/// queue behind it, and an unpaid debt would land entirely on the top of an
/// ascending ladder and manufacture a plateau.
fn settle_reading(bursts: &[(u64, Burst)]) -> Option<Settle> {
    let (at_concurrency, max_wait_s) = bursts
        .iter()
        .map(|(c, b)| (*c, b.settle_s))
        .max_by(|a, b| a.1.total_cmp(&b.1))?;
    Some(Settle {
        max_wait_s,
        at_concurrency,
        quiet_ms: crate::serverside::SETTLE_QUIET_MS,
    })
}

/// What the server wrote and merged while the sweep ran, from its own
/// `part_log`.
///
/// The other half of the landed-rows check, in the server's own accounting
/// rather than in this rig's inference. Non-fatal by construction, like every
/// diagnostic here.
fn read_part_log(ep: &Endpoints, table: &str, since_s: u64) -> Option<PartLog> {
    let body = query_tsv(
        ep,
        &format!(
            "SELECT countIf(event_type = 'NewPart'), sumIf(rows, event_type = 'NewPart'), \
             countIf(event_type = 'MergeParts'), sumIf(rows, event_type = 'MergeParts') \
             FROM system.part_log WHERE table = '{table}' \
             AND event_time > now() - INTERVAL {since_s} SECOND FORMAT TSV"
        ),
    )?;
    let mut fields = body.trim().split('\t');
    let mut next = || -> Option<u64> { fields.next()?.trim().parse().ok() };
    let reading = PartLog {
        parts_written: next()?,
        rows_written: next()?,
        merges: next()?,
        rows_merged: next()?,
    };
    (reading.parts_written > 0).then_some(reading)
}

/// How much of the server's insert time was spent waiting for this rig's bytes.
///
/// The question every ingest ceiling has to answer and none of them could: is
/// this figure a property of ClickHouse, or of the client measuring it? The
/// server's own `system.query_log` answers it directly, and it is asked of the
/// server rather than inferred from this rig's clock because a client cannot
/// distinguish "the target is saturated" from "my bytes are arriving slowly" by
/// timing itself.
///
/// Non-fatal by construction. A missing or disabled `query_log` costs the
/// reading, not the measurement.
fn read_network_wait(ep: &Endpoints, table: &str, since_s: u64) -> Option<NetworkWait> {
    let body = query_tsv(
        ep,
        &format!(
            "SELECT count(), sum(query_duration_ms), \
             sum(ProfileEvents['NetworkReceiveElapsedMicroseconds']) / 1000 \
             FROM system.query_log WHERE type = 'QueryFinish' AND query_kind = 'Insert' \
             AND event_time > now() - INTERVAL {since_s} SECOND \
             AND query LIKE 'INSERT INTO {table} (%' FORMAT TSV"
        ),
    )?;
    let mut fields = body.trim().split('\t');
    let reading = NetworkWait {
        inserts: fields.next()?.trim().parse().ok()?,
        duration_ms: fields.next()?.trim().parse().ok()?,
        waiting_ms: fields.next()?.trim().parse().ok()?,
    };
    (reading.inserts > 0 && reading.duration_ms > 0.0).then_some(reading)
}

/// One read-only query against the server's own log tables.
///
/// The logs are flushed on a timer, so the rungs just measured are not in them
/// yet; both statements are read-only and neither touches an arm's data.
fn query_tsv(ep: &Endpoints, sql: &str) -> Option<String> {
    let run = |s: &str| {
        docker::try_clickhouse_sql(&ep.ch_host, ep.ch_port, &ep.ch_user, &ep.ch_password, s).ok()
    };
    run("SYSTEM FLUSH LOGS")?;
    run(sql)
}

/// One quantity as a percentage of another, guarding a zero denominator.
fn percent(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        return 0.0;
    }
    part as f64 / whole as f64 * 100.0
}

/// A cgroup reading as one terminal line.
fn describe_cgroup(g: Cgroup) -> String {
    format!(
        "{:.2}/{:.1} cores ({:.0}% of cap, {:.0}% user), throttled {} periods for {:.1}s",
        g.cores,
        g.cap_cores,
        if g.cap_cores > 0.0 {
            g.cores / g.cap_cores * 100.0
        } else {
            0.0
        },
        g.user_share * 100.0,
        g.nr_throttled,
        g.throttled_us as f64 / 1e6,
    )
}

/// The server-CPU clause of a rung's log line.
fn describe_server(burst: Burst) -> String {
    burst.server.map_or_else(String::new, |s| {
        format!(
            ", target {:.2}/{:.1} cores ({:.0}% of cap)",
            s.cores,
            s.cap_cores,
            s.share_of_cap() * 100.0
        )
    })
}

/// The landed-versus-POSTed clause of a rung's log line, when they disagree.
///
/// Silent when they agree, which is now the expected case: the interesting event
/// is a shortfall, and [`landed_note`] refuses the pass over one.
fn describe_landed(burst: Burst) -> String {
    match burst.landed {
        Some(landed) if landed < burst.rows => format!(
            " (ONLY {landed} of {} rows landed — the rest were deduplicated)",
            burst.rows
        ),
        _ => String::new(),
    }
}

/// The rows a table holds right now.
///
/// `None` rather than an error: this is the accounting behind a caveat, and a
/// ceiling pass that failed because a `count()` did not answer would be trading a
/// measurement for a footnote. A count that answers and disagrees with what was
/// POSTed is a different matter entirely — see [`landed_note`].
fn landed_rows(ep: &Endpoints, table: &str) -> Option<u64> {
    let body = docker::try_clickhouse_sql(
        &ep.ch_host,
        ep.ch_port,
        &ep.ch_user,
        &ep.ch_password,
        &format!("SELECT count() FROM {table} FORMAT TSV"),
    )
    .ok()?;
    body.trim().parse().ok()
}

/// The target's cgroup CPU across one rung, given the reading taken before it.
///
/// `None` if either reading failed, because half a delta is not a measurement.
fn server_cost(
    before: Option<crate::infra::CpuStat>,
    container: &str,
    over: Duration,
    cap_cores: f64,
) -> Option<ServerCost> {
    let before = before?;
    let after = crate::infra::cgroup_cpu(container)?;
    let spent = after.since(before);
    Some(ServerCost {
        cores: crate::infra::CpuStat::cores_between(before, after, over.as_secs_f64()),
        cap_cores,
        user_us: spent.user_us,
        system_us: spent.system_us,
        nr_throttled: spent.nr_throttled,
        throttled_us: spent.throttled_us,
    })
}

/// The statement one block is POSTed behind, into a named table.
///
/// One definition, used by the measurement pass and by the encoder's live
/// correctness test alike. Two spellings of the same insert is how a test comes
/// to prove something about a statement the pass does not send: the column list
/// is the positional wire contract for RowBinary, and it is also what tells a
/// reader of the query log which insert a ceiling describes.
///
/// The table is a parameter because the callers legitimately differ. The
/// ceiling sweep inserts into its own table, whose deduplication window is off
/// so that its rows land; the pre-sweep proof and the encoder's live test
/// insert into the arms' table, because what each proves is that the bytes
/// this rig sends satisfy the same closed-form oracle an arm is gated on, and
/// [`corpus::run_gates`] asks that of the real target table.
fn insert_sql_into(table: &str, format: Format) -> String {
    format!(
        "INSERT INTO {table} ({}) FORMAT {}",
        corpus::COLUMNS
            .iter()
            .map(|(n, _)| *n)
            .collect::<Vec<_>>()
            .join(", "),
        format.clickhouse_name()
    )
}

/// The statement the pre-sweep proof and the encoder's live correctness test
/// POST: the arms' own table, which is the one [`corpus::run_gates`] queries.
fn insert_sql(format: Format) -> String {
    insert_sql_into(corpus::TABLE, format)
}

// ---------------------------------------------------------------------------
// The ceiling table
// ---------------------------------------------------------------------------

/// The setting whose value is the fourth defect.
const DEDUPLICATION_WINDOW: &str = "non_replicated_deduplication_window";

/// The table the ingest ceilings are measured against.
///
/// Prefixed rather than suffixed so that no ceiling table can ever be mistaken
/// for the arms' own — a `sensor_events_ceiling` sitting beside `sensor_events`
/// is one careless `SHOW TABLES` away from being read as a second target.
fn ceiling_table() -> &'static str {
    "ceiling_sensor_events"
}

/// The ceiling table's DDL, derived from the committed DDL.
///
/// **Derived, not written.** The methodology's requirement is that the table a
/// ceiling is measured against has the same schema as the one the arms write,
/// because the column types are most of the server-side work — every string
/// column is `LowCardinality`, `quality` is `Nullable`, `tags` is an array of
/// dictionaries and `ingest_ts` is materialised per row. A second hand-written
/// CREATE would be a second thing to keep in step with `workload/clickhouse/ddl.sql`,
/// and the whole history of this file is what happens when two spellings of one
/// fact drift apart. So exactly two things are changed: the table name, and the
/// deduplication window.
///
/// Turning the window off is the point. With it on, the rig's cycled pool of
/// pre-encoded blocks is recognised as duplicate and dropped at commit, so the
/// target never accumulates rows and never merges them; a rung POSTing
/// 38,000,000 rows left 800,000 behind. The alternatives were considered and are
/// worse. A pool large enough that no block repeats inside a window of 1000
/// would need thousands of blocks and gigabytes of pre-encoded memory, and
/// shrinking the blocks to afford them would change the part size, which is
/// itself a large part of the server-side cost. A distinct
/// `insert_deduplication_token` per POST would land the rows, but ClickHouse
/// then skips the block hash entirely — the same work this loses — while adding
/// token bookkeeping the arms do not all do.
///
/// What is lost is exactly that block hash: a server with the window on hashes
/// each inserted block to check it against the window, and one with it off does
/// not. That is work an arm which sends no `insert_deduplication_token` pays and
/// this rig now does not, so it points the figure slightly HIGH — and it is far
/// smaller than the merge work the deduplicating rig omitted, which is the trade
/// this makes. For the arm that does send a token (the Spate arm's writer sets
/// one on every sealed batch, which `ddl.sql` records as a declared deviation)
/// the ceiling table is not an approximation at all: no hash, and rows that
/// stay, is precisely what that arm asks of the server.
///
/// # Errors
///
/// If the committed DDL has no statement for the target table, or no
/// deduplication window to turn off. Both are refusals rather than defaults: a
/// ceiling table silently created *with* a deduplication window would restore
/// the defect this exists to close, and would do it invisibly.
fn ceiling_table_ddl() -> Result<String, String> {
    let head = format!("CREATE TABLE IF NOT EXISTS {}", corpus::TABLE);
    let stmt = corpus::ddl_statements()
        .into_iter()
        .find(|s| {
            s.strip_prefix(&head)
                .is_some_and(|rest| !rest.starts_with(|c: char| c.is_alphanumeric() || c == '_'))
        })
        .ok_or_else(|| {
            format!(
                "REFUSED: the committed DDL has no CREATE for {}, so the ceiling table \
                 cannot be derived from it. A hand-written copy is not an \
                 acceptable substitute: the ceiling has to be measured against the arms' \
                 own column types.",
                corpus::TABLE,
            )
        })?;

    let renamed = stmt.replacen(
        &head,
        &format!("CREATE TABLE IF NOT EXISTS {}", ceiling_table()),
        1,
    );
    without_deduplication(&renamed)
}

/// Rewrites a `non_replicated_deduplication_window = N` to zero.
///
/// Parsed rather than string-replaced against the literal `= 1000`, because a
/// replacement that silently matched nothing when the committed value changed
/// would restore the defect and say nothing — and this is a setting somebody
/// will one day tune.
fn without_deduplication(ddl: &str) -> Result<String, String> {
    let refuse = || {
        format!(
            "REFUSED: the committed DDL for {} does not set {DEDUPLICATION_WINDOW} in a \
             shape this can turn off, so the ceiling table cannot be shown NOT to \
             deduplicate the rig's repeated blocks. That is the difference between a \
             figure that includes the merging of rows that stay and one that does not.",
            corpus::TABLE,
        )
    };
    let at = ddl.find(DEDUPLICATION_WINDOW).ok_or_else(refuse)?;
    let rest = &ddl[at + DEDUPLICATION_WINDOW.len()..];
    let after_eq = rest.trim_start().strip_prefix('=').ok_or_else(refuse)?;
    let value = after_eq.trim_start();
    let digits = value
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(value.len());
    if digits == 0 {
        return Err(refuse());
    }
    Ok(format!(
        "{}{DEDUPLICATION_WINDOW} = 0{}",
        &ddl[..at],
        &value[digits..]
    ))
}

/// A ceiling table that exists for as long as this value does.
///
/// The table holds tens of gigabytes by the top of a ladder, so removing it is
/// not tidiness: it is a disk the target is being measured on. A `Drop` rather
/// than a call at the end of [`measure_ingest`] because that function has five
/// ways out and four of them are refusals — a sweep still climbing at its bound,
/// a rung that landed fewer rows than it POSTed, a first rung the server
/// rejected — and the refusals are exactly the runs somebody re-runs
/// immediately. [`crate::sampler`] records what the same omission cost when a
/// refusal abandoned a sampler container; this is the same shape of bug, spelled
/// out before it could be paid for twice.
#[derive(Debug)]
struct CeilingTable<'a> {
    ep: &'a Endpoints,
}

impl<'a> CeilingTable<'a> {
    /// Creates the ceiling table, replacing any left by an earlier pass.
    ///
    /// Dropped first rather than created `IF NOT EXISTS` alone: a table left
    /// behind by a pass that was killed outright would carry that pass's rows
    /// and, worse, that pass's schema — and a ceiling measured against a stale
    /// schema is the one failure [`ceiling_table_ddl`] exists to prevent.
    ///
    /// # Errors
    ///
    /// If the DDL cannot be derived, or the server refuses it.
    fn create(ep: &'a Endpoints) -> Result<Self, String> {
        let ddl = ceiling_table_ddl()?;
        let table = Self { ep };
        table.remove();
        let body =
            docker::try_clickhouse_sql(&ep.ch_host, ep.ch_port, &ep.ch_user, &ep.ch_password, &ddl)
                .map_err(|e| format!("create {}: {e}", ceiling_table()))?;
        if body.contains("DB::Exception") {
            return Err(format!("create {}: {body}", ceiling_table()));
        }
        Ok(table)
    }

    /// The table's name, for a statement or a log line.
    fn name(&self) -> &'static str {
        ceiling_table()
    }

    /// Removes the table and its data, now rather than in eight minutes.
    ///
    /// `SYNC` because an Atomic database keeps a dropped table's data for
    /// `database_atomic_delay_before_drop_table_sec`, which is long enough for a
    /// six-combination pass to leave every rung it ever ran on the disk.
    ///
    /// Best-effort by design: this is housekeeping, and a pass that refused
    /// because a `DROP` did not answer would trade a measurement for it.
    fn remove(&self) {
        let _ = docker::try_clickhouse_sql(
            &self.ep.ch_host,
            self.ep.ch_port,
            &self.ep.ch_user,
            &self.ep.ch_password,
            &format!("DROP TABLE IF EXISTS {} SYNC", self.name()),
        );
    }
}

impl Drop for CeilingTable<'_> {
    fn drop(&mut self) {
        self.remove();
    }
}

/// Empties the ceiling table and waits until the server has nothing left to do
/// about it, returning the seconds waited.
///
/// The wait is how long the target needed to finish merging what the previous
/// rung left, so it measures work that fell outside the window that was timed.
/// The ladder ascends, so an unpaid debt lands on the high rungs and
/// manufactures a plateau out of the rig's own tidying.
///
/// # Errors
///
/// If the truncation is refused. A wait that runs past
/// [`crate::serverside::SETTLE_MAX_S`] is not an error: the pass proceeds and
/// the duration reaches the record through [`settle_note`].
fn settle(ep: &Endpoints, table: &str) -> Result<f64, String> {
    let body = docker::try_clickhouse_sql(
        &ep.ch_host,
        ep.ch_port,
        &ep.ch_user,
        &ep.ch_password,
        &format!("TRUNCATE TABLE IF EXISTS {table}"),
    )
    .map_err(|e| format!("truncate {table}: {e}"))?;
    if body.contains("DB::Exception") {
        return Err(format!("truncate {table}: {body}"));
    }
    Ok(crate::serverside::wait_until_settled(
        &ep.ch_host,
        ep.ch_port,
        &ep.ch_user,
        &ep.ch_password,
        table,
    ))
}

// ---------------------------------------------------------------------------
// Row encoding
// ---------------------------------------------------------------------------

/// One column's value for one row.
///
/// A typed cell rather than raw bytes, so the encoder can be checked against the
/// committed DDL without a server: [`Cell::declared_type`] names the ClickHouse
/// type each variant claims to write, and a test asserts that a row's cells
/// declare exactly the columns `corpus::COLUMNS` declares, in order. Column order
/// is the wire contract for RowBinary, so a silent divergence here would corrupt
/// every row rather than fail loudly.
#[derive(Debug, Clone)]
enum Cell {
    UInt64(u64),
    UInt16(u16),
    Int64(i64),
    LowCardString(String),
    NullableFloat64(Option<f64>),
    LowCardStringArray(Vec<String>),
    DateTime64 {
        /// What goes on the wire in the binary formats: a `DateTime64` travels
        /// as its underlying `Int64` tick count.
        ticks: i64,
        /// Never written by RowBinary or Native — there the scale is a property
        /// of the column type in the header, not of the wire — and read by two
        /// consumers. The JSONEachRow encoder needs it to know how many
        /// fractional digits the epoch-seconds decimal carries (see
        /// [`push_json_datetime64`]), and
        /// `every_encoded_row_declares_the_columns_the_ddl_declares_in_order`
        /// checks the type each cell claims against the committed DDL — the only
        /// thing standing between a reordered `row_of` and a silently corrupt
        /// block: column order is the wire contract for RowBinary, so a
        /// divergence does not fail loudly, it mis-writes every row.
        scale: u8,
    },
}

impl Cell {
    /// The ClickHouse type this cell claims to write, spelled as the DDL spells
    /// it.
    ///
    /// Test-only on purpose. A block's name-and-type header is built from
    /// `corpus::COLUMNS`, which is derived from the DDL itself, so this must
    /// never become a second runtime spelling of the same thing. It exists so a
    /// test can assert the encoder's cells line up with that header, position
    /// for position.
    #[cfg(test)]
    fn declared_type(&self) -> String {
        match self {
            Self::UInt64(_) => "UInt64".to_owned(),
            Self::UInt16(_) => "UInt16".to_owned(),
            Self::Int64(_) => "Int64".to_owned(),
            Self::LowCardString(_) => "LowCardinality(String)".to_owned(),
            Self::NullableFloat64(_) => "Nullable(Float64)".to_owned(),
            Self::LowCardStringArray(_) => "Array(LowCardinality(String))".to_owned(),
            Self::DateTime64 { scale, .. } => format!("DateTime64({scale})"),
        }
    }
}

impl Cell {
    /// Appends this cell in RowBinary.
    ///
    /// `LowCardinality` is transparent in RowBinary — the wire carries the
    /// underlying `String` and the server builds the dictionary — which is why
    /// the dictionary construction stays inside the measurement rather than
    /// being pre-computed by the client.
    fn write(&self, out: &mut Vec<u8>) {
        match self {
            Self::UInt64(v) => out.extend_from_slice(&v.to_le_bytes()),
            Self::UInt16(v) => out.extend_from_slice(&v.to_le_bytes()),
            Self::Int64(v) => out.extend_from_slice(&v.to_le_bytes()),
            Self::LowCardString(s) => put_string(out, s),
            // The null marker, then the value only when there is one. An arm
            // that wrote eight zero bytes after the marker would be writing a
            // different number of bytes per row and the ceiling would describe
            // a wider row than the arms send.
            Self::NullableFloat64(None) => out.push(1),
            Self::NullableFloat64(Some(v)) => {
                out.push(0);
                out.extend_from_slice(&v.to_le_bytes());
            }
            Self::LowCardStringArray(vs) => {
                put_varint(out, vs.len() as u64);
                for s in vs {
                    put_string(out, s);
                }
            }
            // DateTime64 travels as its underlying Int64 tick count; the scale
            // is a property of the column, not of the wire. Getting this wrong
            // is the regression `ddl.sql` warns about, where every value lands
            // in 1970.
            Self::DateTime64 { ticks, .. } => out.extend_from_slice(&ticks.to_le_bytes()),
        }
    }
}

/// ClickHouse's unsigned LEB128.
fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// A length-prefixed string.
fn put_string(out: &mut Vec<u8>, s: &str) {
    put_varint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

// ---------------------------------------------------------------------------
// Native column encoding
// ---------------------------------------------------------------------------

/// The `LowCardinality` key-serialisation version every block carries.
///
/// One, which ClickHouse calls `SharedDictionariesWithAdditionalKeys`. It is the
/// only version the server's reader accepts and it is written once per
/// `LowCardinality` column, immediately after that column's type name, before
/// anything else the column emits — for `Array(LowCardinality(String))` that
/// means **before the offsets**, because the server's array serialisation
/// delegates its state prefix to the nested type and then writes the offsets.
/// Getting that order wrong misreads the first eight bytes of the offsets as the
/// version and rejects the block.
const LOW_CARDINALITY_KEY_VERSION: u64 = 1;

/// The flag bits set in every `LowCardinality` index-type word, above the index
/// width in the low byte.
///
/// `HasAdditionalKeysBit` (`1 << 9`) says "the keys for this group travel in
/// this group", which is what makes a block self-contained. It is required:
/// without it there are no keys for the indexes to point at.
///
/// `NeedUpdateDictionary` (`1 << 10`) tells the server to drop whatever
/// dictionary it was carrying rather than extend it. It is **not** required —
/// 26.3 accepts a block that omits it, which was checked by sending one — and it
/// is set because `spate-clickhouse`'s Native writer sets it. That agreement is
/// the point rather than a coincidence: this ceiling is the denominator the
/// Spate arm's own Native writer is judged against, so it has to exercise the
/// same server-side path. See the fidelity note on [`Column::write_native`].
///
/// `NeedGlobalDictionaryBit` (`1 << 8`) is deliberately **not** set, and cannot
/// be: it describes a dictionary carried across blocks in one stream, and a
/// ceiling pass POSTs independent blocks over independent HTTP requests. A block
/// that sets it is refused outright with a 400, which was likewise checked
/// rather than assumed.
const LOW_CARDINALITY_INDEX_FLAGS: u64 = (1 << 9) | (1 << 10);

/// The integer width one `LowCardinality` column's indexes are written at.
///
/// Chosen from the dictionary size rather than declared, exactly as ClickHouse's
/// own `ColumnLowCardinality::Index` widens its positions column when a position
/// stops fitting. This is the single most dangerous field in the whole encoder:
/// a width one step too narrow does not fail, it truncates every index above the
/// boundary and lands the wrong dictionary entry in those rows — which the
/// corpus's string fingerprints catch and nothing else would.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexWidth {
    /// Dictionaries up to 256 entries. `region`, `unit`, `name` and `tags` all
    /// land here on this corpus.
    U8,
    /// Up to 65,536. `sensor` lands here on any block spanning 256 batches or
    /// more — 256 sensors beside the reserved default is 257 entries — which is
    /// why the live test spans 400 and the reference-decoder test spans 300.
    U16,
    /// Up to 2^32.
    U32,
    /// Anything larger.
    U64,
}

impl IndexWidth {
    /// The width a dictionary of `entries` values needs.
    ///
    /// Sized against the largest *index*, `entries - 1`, not against the entry
    /// count: a 256-entry dictionary has a maximum index of 255 and fits `U8`,
    /// and rounding that up would write a block ClickHouse reads correctly but
    /// no competent writer would send.
    fn for_dictionary(entries: usize) -> Self {
        match (entries as u64).saturating_sub(1) {
            max if max <= u64::from(u8::MAX) => Self::U8,
            max if max <= u64::from(u16::MAX) => Self::U16,
            max if max <= u64::from(u32::MAX) => Self::U32,
            _ => Self::U64,
        }
    }

    /// The low byte of the index-type word. The server reads the width from
    /// here, so this and [`IndexWidth::put`] must never disagree.
    fn code(self) -> u64 {
        match self {
            Self::U8 => 0,
            Self::U16 => 1,
            Self::U32 => 2,
            Self::U64 => 3,
        }
    }

    /// Appends one index at this width, little-endian.
    fn put(self, out: &mut Vec<u8>, index: u64) {
        match self {
            Self::U8 => out.push(index as u8),
            Self::U16 => out.extend_from_slice(&(index as u16).to_le_bytes()),
            Self::U32 => out.extend_from_slice(&(index as u32).to_le_bytes()),
            Self::U64 => out.extend_from_slice(&index.to_le_bytes()),
        }
    }
}

/// One `LowCardinality(String)` column, accumulated as a dictionary and an index
/// per value.
///
/// The dictionary is built **per block** and holds only the values that block
/// uses, in first-appearance order. That is a fidelity decision rather than a
/// convenience: see the note on [`Column::write_native`] for what it means for
/// the arm this ceiling gates.
#[derive(Debug)]
struct Dictionary {
    entries: Vec<String>,
    /// Value to its position, so a block of 100,000 rows over a 1024-value
    /// alphabet costs one hash lookup per row rather than a linear scan.
    positions: std::collections::HashMap<String, u64>,
    indexes: Vec<u64>,
}

impl Default for Dictionary {
    /// A dictionary whose index 0 is already the inner type's default, the
    /// empty string.
    ///
    /// The reservation is not decoration and it is not ours: ClickHouse's
    /// `ColumnLowCardinality` keeps its default value at index 0, and
    /// `spate-clickhouse` — the writer behind every `spate:*-native` arm — seeds
    /// exactly this slot before interning anything. Omitting it produces a block
    /// the server still reads correctly, and a dictionary one entry shorter than
    /// the arm's, which moves the index-width boundary by one value. Matching it
    /// costs one empty string per column and keeps the ceiling on the same side
    /// of that boundary as the insert it is the denominator for.
    ///
    /// On this corpus the slot is not even wasted for `region`: the null-region
    /// coalesce puts real empty strings in that column, and they intern straight
    /// onto index 0.
    fn default() -> Self {
        let mut positions = std::collections::HashMap::new();
        positions.insert(String::new(), 0);
        Self {
            entries: vec![String::new()],
            positions,
            indexes: Vec::new(),
        }
    }
}

impl Dictionary {
    /// Records one value, adding it to the dictionary the first time it appears.
    fn push(&mut self, value: &str) {
        let at = if let Some(at) = self.positions.get(value) {
            *at
        } else {
            let at = self.entries.len() as u64;
            self.entries.push(value.to_owned());
            self.positions.insert(value.to_owned(), at);
            at
        };
        self.indexes.push(at);
    }

    /// Appends the dictionary-and-indexes group: the index-type word, the
    /// dictionary size, the dictionary entries, the index count, then the
    /// indexes.
    ///
    /// The two `u64` counts are fixed-width, not varints. Everything inside a
    /// column's data is fixed-width; the varints in a Native block are confined
    /// to the block header's column and row counts and to the string lengths.
    ///
    /// A group with no indexes at all writes **nothing** — not even the flag
    /// word. That is the shape the server expects when an
    /// `Array(LowCardinality(String))` block holds only empty arrays, and it is
    /// what `spate-clickhouse` emits for the same case. It cannot arise from this
    /// corpus, whose `tags` are non-empty on three rows in four, and it is
    /// mirrored anyway: an encoder that is right only for the inputs it happens
    /// to be handed is the kind that fails once it is reused.
    fn write(&self, out: &mut Vec<u8>) {
        if self.indexes.is_empty() {
            return;
        }
        let width = IndexWidth::for_dictionary(self.entries.len());
        out.extend_from_slice(&(width.code() | LOW_CARDINALITY_INDEX_FLAGS).to_le_bytes());
        out.extend_from_slice(&(self.entries.len() as u64).to_le_bytes());
        for entry in &self.entries {
            put_string(out, entry);
        }
        out.extend_from_slice(&(self.indexes.len() as u64).to_le_bytes());
        for index in &self.indexes {
            width.put(out, *index);
        }
    }
}

/// One column of a Native block, accumulated in the shape that column is written
/// in.
///
/// Native is columnar, so the rig has to transpose. It transposes from
/// [`row_of`] rather than generating columns directly, which keeps one
/// definition of what a row is: the RowBinary encoder, the Native encoder and
/// the DDL-agreement test all read the same cells in the same order, and a
/// column list that drifted from the DDL would fail that test rather than
/// silently mis-write one format.
#[derive(Debug)]
enum Column {
    UInt64(Vec<u64>),
    UInt16(Vec<u16>),
    Int64(Vec<i64>),
    LowCardString(Dictionary),
    NullableFloat64 {
        /// One byte per row, `1` for null. Written for the whole column before
        /// any value.
        nulls: Vec<u8>,
        /// One value per row **including the null ones**, unlike RowBinary where
        /// a null occupies one byte and nothing else.
        values: Vec<f64>,
    },
    LowCardStringArray {
        /// Cumulative end offset per row.
        offsets: Vec<u64>,
        /// Every row's elements, flattened into one `LowCardinality` column.
        elements: Dictionary,
    },
    DateTime64(Vec<i64>),
}

impl Column {
    /// An empty column shaped for this cell.
    ///
    /// Derived from the first row's cells rather than from a second table of
    /// column types, so there is exactly one mapping from the DDL to a shape and
    /// `every_encoded_row_declares_the_columns_the_ddl_declares_in_order` is
    /// what guards it.
    fn empty_for(cell: &Cell) -> Self {
        match cell {
            Cell::UInt64(_) => Self::UInt64(Vec::new()),
            Cell::UInt16(_) => Self::UInt16(Vec::new()),
            Cell::Int64(_) => Self::Int64(Vec::new()),
            Cell::LowCardString(_) => Self::LowCardString(Dictionary::default()),
            Cell::NullableFloat64(_) => Self::NullableFloat64 {
                nulls: Vec::new(),
                values: Vec::new(),
            },
            Cell::LowCardStringArray(_) => Self::LowCardStringArray {
                offsets: Vec::new(),
                elements: Dictionary::default(),
            },
            Cell::DateTime64 { .. } => Self::DateTime64(Vec::new()),
        }
    }

    /// Appends one row's value.
    ///
    /// Panics on a shape mismatch, which cannot happen: every row comes from
    /// [`row_of`], whose cell shapes are fixed. A panic here would mean
    /// that stopped being true, and a Native block built from a row whose shape
    /// changed mid-block is not something to recover from — it would be a block
    /// whose columns hold different numbers of rows.
    fn push(&mut self, cell: Cell) {
        match (self, cell) {
            (Self::UInt64(v), Cell::UInt64(x)) => v.push(x),
            (Self::UInt16(v), Cell::UInt16(x)) => v.push(x),
            (Self::Int64(v), Cell::Int64(x)) => v.push(x),
            (Self::LowCardString(d), Cell::LowCardString(s)) => d.push(&s),
            (Self::NullableFloat64 { nulls, values }, Cell::NullableFloat64(q)) => {
                nulls.push(u8::from(q.is_none()));
                // A null row still occupies a slot in the values column. Zero is
                // what ClickHouse's own writer leaves there, and the null map is
                // what decides; writing nothing would shorten the column and
                // misalign every row after the first null.
                values.push(q.unwrap_or(0.0));
            }
            (Self::LowCardStringArray { offsets, elements }, Cell::LowCardStringArray(tags)) => {
                for tag in &tags {
                    elements.push(tag);
                }
                offsets.push(elements.indexes.len() as u64);
            }
            (Self::DateTime64(v), Cell::DateTime64 { ticks, .. }) => v.push(ticks),
            (column, cell) => {
                panic!("row shape changed mid-block: {cell:?} cannot append to {column:?}")
            }
        }
    }

    /// Appends this column's values in Native's layout.
    ///
    /// The layouts, and how each was confirmed:
    ///
    /// * `UInt64`, `UInt16`, `Int64` and `DateTime64(N)` are fixed-width
    ///   little-endian, contiguous, one after another. `DateTime64` travels as
    ///   its underlying `Int64` tick count and the scale is a property of the
    ///   column type in the header, never of the wire — the regression
    ///   `ddl.sql` warns about, where "every value silently lands in 1970", is
    ///   exactly a writer that rescales here. Confirmed by the `batch_ts` sum
    ///   and the `send_ts` bound in `corpus::run_gates`.
    /// * `Nullable(Float64)` is the null map for the **whole column** first, one
    ///   byte per row, then the values for **every** row including the nulls.
    ///   This is the one column whose Native layout is not a rearrangement of
    ///   its RowBinary layout: RowBinary writes a marker and, only when the
    ///   value is present, eight more bytes. Confirmed by the null-quality count
    ///   in `corpus::run_gates`, which fails if the map and the values disagree
    ///   by even one row.
    /// * `LowCardinality(String)` is the key version, then one
    ///   dictionary-and-indexes group: see [`Dictionary::write`] and
    ///   [`IndexWidth`]. Confirmed by the `sensor`, `region`, name and `unit`
    ///   fingerprint sums, which are the only checks that would notice an index
    ///   width one step too narrow.
    /// * `Array(LowCardinality(String))` is the nested key version, then one
    ///   cumulative end offset per row, then the flattened elements as a single
    ///   `LowCardinality` group. Offsets are **cumulative ends**, not lengths
    ///   and not starts: row `i` owns elements `offsets[i - 1] .. offsets[i]`.
    ///   Confirmed by `sum(length(tags))` and the tag fingerprint sum, the pair
    ///   that exists because an arm emitting `tags = []` skips this encode
    ///   entirely and would otherwise pass.
    ///
    /// # The dictionary is per block, and so is the arm's
    ///
    /// Each block carries its own dictionary, holding only the values that block
    /// uses plus the reserved default at index 0. It is self-contained —
    /// `HasAdditionalKeysBit | NeedUpdateDictionary`, never
    /// `NeedGlobalDictionaryBit` — so nothing is assumed about what the server
    /// has seen before.
    ///
    /// This was checked against the arm rather than assumed, because a ceiling
    /// measured on a shared-dictionary insert path would be the denominator for
    /// arms that do not use one. `spate-clickhouse`'s `LowCard` writer resets its
    /// map, its dictionary buffer and its key run at every block boundary, its
    /// per-shard clones start empty, and no dictionary state is held on the
    /// encoder, the writer, the endpoint or the sink pool. It sets the same two
    /// flag bits and seeds the same reserved index 0. The two agree.
    ///
    /// # What this encoder still does not reproduce
    ///
    /// Two differences remain between this block and the bytes a Spate Native
    /// arm puts on the socket, and neither is smoothed over here because a
    /// ceiling that quietly describes a different insert is the failure this
    /// whole module exists to prevent:
    ///
    /// * **Transport compression.** The arm's sink defaults to LZ4, so each
    ///   block travels inside ClickHouse's compressed frame — a CityHash128
    ///   checksum, a method byte, the two sizes, then the LZ4 payload — with
    ///   `decompress=1` on the request. This rig POSTs the block raw. The server
    ///   therefore reads more bytes and does no decompression, and which way
    ///   that moves the ceiling has not been measured.
    /// * **Block size.** This rig sends one block per request, covering the
    ///   whole pre-encoded batch range. The arm seals a block at its sink's
    ///   chunk threshold and concatenates many of them into one request, so its
    ///   blocks are far smaller and its dictionaries far shorter. Larger blocks
    ///   amortise per-block work, which points this difference in the *lenient*
    ///   direction.
    ///
    /// Both are recorded on every measured Native ceiling by [`measure`], so a
    /// reader of the committed file sees them beside the number rather than
    /// having to find this comment.
    fn write_native(&self, out: &mut Vec<u8>) {
        match self {
            Self::UInt64(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Self::UInt16(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Self::Int64(v) | Self::DateTime64(v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Self::LowCardString(d) => {
                out.extend_from_slice(&LOW_CARDINALITY_KEY_VERSION.to_le_bytes());
                d.write(out);
            }
            Self::NullableFloat64 { nulls, values } => {
                out.extend_from_slice(nulls);
                for x in values {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Self::LowCardStringArray { offsets, elements } => {
                out.extend_from_slice(&LOW_CARDINALITY_KEY_VERSION.to_le_bytes());
                for offset in offsets {
                    out.extend_from_slice(&offset.to_le_bytes());
                }
                elements.write(out);
            }
        }
    }
}

/// Encodes one Native block over `batches` batches of the workload's rows.
///
/// The framing is the server's own: a varuint column count, a varuint row count,
/// then per column a name string, a type string and that column's values
/// contiguously. No block-info preamble and no per-column serialisation-kind
/// byte — both are gated on a client protocol revision, and the `FORMAT Native`
/// input path reads a block at revision zero, where neither is present. Writing
/// either one puts bytes in front of the column count and the server rejects the
/// whole block.
///
/// A zero-row block writes its header and no column data at all, which is what
/// ClickHouse's own writer does. It cannot arise from a ceiling pass — every
/// batch yields rows after the filters — but a header whose column count
/// promised data that was not there would be an odd shape to leave lying around.
fn encode_native_block(lo: u64, batches: u64) -> Block {
    let declared = corpus::COLUMNS;
    let mut columns: Vec<Column> = Vec::new();
    let mut rows = 0u64;
    for batch_id in lo..lo + batches {
        for seq in 0..corpus::EVENTS_PER_BATCH {
            if !corpus::keeps(batch_id, seq) {
                continue;
            }
            let row = row_of(batch_id, seq);
            if columns.is_empty() {
                columns = row.iter().map(Column::empty_for).collect();
            }
            assert_eq!(
                columns.len(),
                row.len(),
                "a row carries {} cells but the block has {} columns",
                row.len(),
                columns.len()
            );
            for (column, cell) in columns.iter_mut().zip(row) {
                column.push(cell);
            }
            rows += 1;
        }
    }
    assert!(
        columns.is_empty() || columns.len() == declared.len(),
        "the encoder built {} columns but the target declares {}",
        columns.len(),
        declared.len()
    );

    let mut body = Vec::new();
    put_varint(&mut body, declared.len() as u64);
    put_varint(&mut body, rows);
    for (i, (name, ty)) in declared.iter().enumerate() {
        put_string(&mut body, name);
        put_string(&mut body, ty);
        if let Some(column) = columns.get(i) {
            column.write_native(&mut body);
        }
    }
    Block { body, rows }
}

/// One flattened row, in the declared column order.
///
/// Every value comes from the corpus's own derivation functions rather than from
/// a copy of them, so the rows this rig inserts are the rows an arm would insert
/// and the ceiling is measured against real `LowCardinality` cardinality, real
/// array lengths and the real null pattern.
fn row_of(batch_id: u64, seq: u32) -> Vec<Cell> {
    let value = corpus::value_of(batch_id, seq);
    vec![
        Cell::UInt64(batch_id),
        Cell::UInt16(u16::try_from(seq).unwrap_or(u16::MAX)),
        Cell::LowCardString(corpus::sensor_of(batch_id)),
        // The specified null-region coalesce: the target column is
        // LowCardinality(String), not LowCardinality(Nullable(String)).
        Cell::LowCardString(corpus::region_of(batch_id).unwrap_or_default()),
        Cell::LowCardString(corpus::ascii_upper(&corpus::name_of(batch_id, seq))),
        Cell::LowCardString(corpus::unit_of(batch_id, seq).to_owned()),
        Cell::Int64(value),
        Cell::Int64(corpus::value_scaled_of(value, seq)),
        Cell::NullableFloat64(corpus::quality_of(batch_id, seq)),
        Cell::LowCardStringArray(corpus::tags_of(batch_id, seq)),
        Cell::DateTime64 {
            ticks: corpus::batch_ts_ms_of(batch_id),
            scale: 3,
        },
        Cell::DateTime64 {
            ticks: corpus::send_ts_us_prefill(batch_id),
            scale: 6,
        },
    ]
}

// ---------------------------------------------------------------------------
// JSONEachRow encoding
// ---------------------------------------------------------------------------

/// Appends `s` as a JSON string, escaped per RFC 8259's minimum: `"`, `\`, and
/// every control character below 0x20 as `\u00XX`.
///
/// Every string this corpus emits is an ASCII identifier (`sensor-17`,
/// `METRIC_4`, `tag-9`), so on today's inputs this is a quote, the bytes, and a
/// quote. It escapes anyway, because an encoder that is correct only for the
/// inputs it happens to be handed is the kind that fails once it is reused —
/// the same reason [`Dictionary::write`] handles the empty group this corpus
/// cannot produce.
///
/// Hand-rolled rather than routed through `serde_json`, deliberately, even
/// though `serde_json` is already a dependency. Three reasons: the block is
/// accumulated into one growing `String` and a per-cell `serde_json::to_string`
/// would allocate per value; `serde_json::Map` does not preserve insertion
/// order without the `preserve_order` feature, and this block's key order is
/// asserted byte-for-byte by its tests; and the escaping rule is four lines
/// whose output those same tests pin — a dependency would not make it more
/// proven, only less visible.
fn push_json_string(out: &mut String, s: &str) {
    use std::fmt::Write as _;
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Appends one `DateTime64` value as a JSON **string** holding Unix epoch
/// seconds with exactly `scale` fractional digits, e.g. ticks `1772000000123`
/// at scale 3 as `"1772000000.123"`.
///
/// # Why numeric epoch and not a formatted date string
///
/// ClickHouse parses `YYYY-MM-DD hh:mm:ss.fff` text **in the column's
/// timezone**, which for this DDL is the server's — so the same block would
/// land different ticks on servers configured differently, and a ceiling pass
/// must not depend on how a container's `TZ` happens to be set. A Unix epoch
/// names an instant with no timezone to consult. This is the same
/// server-timezone independence the ArrowStream encoder buys with an explicit
/// `"UTC"` on its timestamps.
///
/// # Why a quoted string and not a bare JSON number
///
/// Measured, not assumed: `clickhouse-server:26.3` **refused** the bare form.
/// Its JSONEachRow reader accepts only an integer number for a `DateTime64`
/// column — the live test's first run answered
/// `Cannot parse input: expected ',' before: '.000,...'` at the first
/// `batch_ts` — and an integer is epoch *seconds*, which cannot carry the
/// sub-second digits. The quoted form routes through the `DateTime64` text
/// parser instead, which accepts a decimal epoch in full. The cost is two
/// bytes per timestamp; the alternative — bare integer ticks and a cast in
/// the INSERT — would change the statement per column and measure an insert
/// no arm sends.
///
/// # Why the round trip is exact and not merely close
///
/// A `DateTime64(S)` is a `Decimal64(S)` underneath, and ClickHouse's text
/// reader parses the integer and fractional digits directly into that `Int64`
/// tick count — the value never passes through a float. That matters at scale
/// 6: `1772000000.000123` is not representable in an `f64` (31 bits of integer
/// part leave the mantissa ~0.24µs of resolution), so a reader that went
/// through `Float64` could misround the last microsecond digit. Digit-by-digit
/// parsing cannot. That the whole chain holds — quoted decimal epoch, parsed
/// exactly, timezone never consulted — is what the live test's `batch_ts` sum
/// and `send_ts` bound prove against a real server.
///
/// # Why the fraction is fixed-width
///
/// Exactly `scale` digits, zero-padded, trailing zeros kept: `seconds * 10^S +
/// fraction == ticks` then holds by construction, which is what lets a test
/// assert the serialised form and a reader recompute the ticks without
/// thinking about shortening rules. ClickHouse accepts trailing zeros; a
/// whole-second value serialises as `"1772000000.000"`, not `"1772000000"`.
///
/// # Why a negative tick count is refused rather than serialised
///
/// Measured, not assumed, like the quoting above: the server does not reject
/// the signed form loudly, it misparses it silently — which is worse. Against
/// a fresh `clickhouse-server:26.3`, `CREATE TABLE t (ts DateTime64(3))` and
/// an `INSERT … FORMAT JSONEachRow` of `{"ts":"-0.001"}`, the server ACCEPTS
/// the row and lands `toUnixTimestamp64Milli(ts) = 1` — *plus* one
/// millisecond, the sign dropped on the floor. An encoder that emitted the
/// sign would therefore corrupt every pre-epoch value with no error anywhere
/// to notice it. The corpus is never pre-epoch, so refusing loses nothing
/// legitimate; the assert turns a measured silent-corruption path into a loud
/// panic at the encoder, before a byte reaches the wire.
///
/// # Panics
///
/// If `scale` is 0 or above 9 — a zero scale would emit a trailing `.` with no
/// digits, not a decimal the text parser reads, and nothing in ClickHouse goes
/// finer than nanoseconds (the committed DDL uses 3 and 6). And if `ticks` is
/// negative, for the measured reason above.
fn push_json_datetime64(out: &mut String, ticks: i64, scale: u8) {
    use std::fmt::Write as _;
    assert!(
        (1..=9).contains(&scale),
        "DateTime64 scale {scale} has no JSON epoch form this encoder emits"
    );
    assert!(
        ticks >= 0,
        "a pre-epoch DateTime64 ({ticks} ticks at scale {scale}) is refused: \
         clickhouse-server:26.3 accepts \"-0.001\" and lands +0.001 — the sign is \
         silently dropped, not rejected — so emitting it would corrupt rather than \
         fail loudly"
    );
    let per_second = 10u64.pow(u32::from(scale));
    let ticks = ticks.unsigned_abs();
    let _ = write!(
        out,
        "\"{}.{:0width$}\"",
        ticks / per_second,
        ticks % per_second,
        width = usize::from(scale)
    );
}

impl Cell {
    /// Appends this cell as a JSONEachRow value.
    ///
    /// The integer cells — including the `UInt64` — are bare JSON numbers.
    /// JSON the grammar puts no width limit on a number; the familiar 2^53
    /// hazard is a property of consumers that parse into IEEE-754 doubles, and
    /// ClickHouse's `JSONEachRow` reader is not one: it parses the digit string
    /// directly into the column's own integer type, full-width. Quoting the
    /// value would also be accepted (ClickHouse reads numbers from strings on
    /// input), but it would measure a parse-plus-unquote nobody's arm performs.
    ///
    /// A present `quality` uses Rust's `Display` for `f64`, which prints the
    /// shortest decimal that parses back to the identical bits — so the `f64`
    /// the server stores equals the one the corpus generated, and the gate's
    /// quality checks hold. Asserted finite because the corpus cannot produce
    /// a NaN or an infinity and JSON cannot carry one; a silent `null` there
    /// would corrupt the null-count gate instead of failing loudly here.
    fn write_json(&self, out: &mut String) {
        use std::fmt::Write as _;
        match self {
            Self::UInt64(v) => {
                let _ = write!(out, "{v}");
            }
            Self::UInt16(v) => {
                let _ = write!(out, "{v}");
            }
            Self::Int64(v) => {
                let _ = write!(out, "{v}");
            }
            Self::LowCardString(s) => push_json_string(out, s),
            Self::NullableFloat64(None) => out.push_str("null"),
            Self::NullableFloat64(Some(v)) => {
                assert!(
                    v.is_finite(),
                    "a non-finite quality ({v}) has no JSON form and cannot come from the corpus"
                );
                let _ = write!(out, "{v}");
            }
            Self::LowCardStringArray(vs) => {
                out.push('[');
                for (i, s) in vs.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    push_json_string(out, s);
                }
                out.push(']');
            }
            Self::DateTime64 { ticks, scale } => push_json_datetime64(out, *ticks, *scale),
        }
    }
}

/// Encodes one JSONEachRow block over `batches` batches of the workload's rows:
/// one JSON object per row, keys the committed column names, newline-separated.
///
/// The keys are emitted in `corpus::COLUMNS` order. ClickHouse matches
/// JSONEachRow fields by name rather than position, so the order is not a wire
/// contract the way it is for RowBinary — it is kept anyway so that a block
/// diffed against the DDL reads in one order, and so the exact-text tests can
/// pin whole lines rather than sets of fragments.
///
/// The per-value decisions — numeric epoch timestamps, bare full-width
/// integers, shortest-round-trip floats, the minimal escaper — live on
/// [`Cell::write_json`] and [`push_json_datetime64`], beside the code that
/// takes them.
fn encode_json_each_row_block(lo: u64, batches: u64) -> Block {
    // Hoisted out of the row loop because the key fragments are properties of
    // the *column list*: `,"name":` is byte-identical on every row, so pushing
    // each name through the escaper per row would multiply twelve escape scans
    // by every row of a thousand-batch block for no extra bytes of output.
    // Built through `push_json_string` all the same, so a column name that one
    // day needed escaping would take the same path the values do.
    let keys: Vec<String> = corpus::COLUMNS
        .iter()
        .enumerate()
        .map(|(i, (name, _))| {
            let mut key = String::from(if i > 0 { "," } else { "" });
            push_json_string(&mut key, name);
            key.push(':');
            key
        })
        .collect();
    let mut body = String::new();
    let mut rows = 0u64;
    for batch_id in lo..lo + batches {
        for seq in 0..corpus::EVENTS_PER_BATCH {
            if !corpus::keeps(batch_id, seq) {
                continue;
            }
            let row = row_of(batch_id, seq);
            assert_eq!(
                row.len(),
                keys.len(),
                "a row carries {} cells but the target declares {} columns",
                row.len(),
                keys.len()
            );
            body.push('{');
            for (key, cell) in keys.iter().zip(&row) {
                body.push_str(key);
                cell.write_json(&mut body);
            }
            body.push_str("}\n");
            rows += 1;
        }
    }
    Block {
        body: body.into_bytes(),
        rows,
    }
}

// ---------------------------------------------------------------------------
// ArrowStream encoding
// ---------------------------------------------------------------------------

/// The Arrow field one committed ClickHouse column maps to.
///
/// One match from the DDL's own type spelling, driven by [`corpus::COLUMNS`],
/// so a column cannot be added to the workload without this mapping being
/// decided — an unmapped type panics here rather than silently landing as
/// whatever a generic converter guessed. The choices, and why each:
///
/// * `UInt64` / `UInt16` / `Int64` → the same-width Arrow integer. Identical
///   bits, no conversion for the server to price.
/// * `LowCardinality(String)` → plain `Utf8`, **not** an Arrow dictionary.
///   ClickHouse casts to `LowCardinality` on insert, so the server pays the
///   dictionary build — deliberately the same shape as RowBinary, where
///   `LowCardinality` is transparent on the wire. Sending pre-built Arrow
///   dictionaries would presuppose an arm that builds them client-side and
///   would err in the *lenient* direction for arms that do not.
/// * `Nullable(Float64)` → nullable `Float64` — the only nullable field,
///   mirroring the DDL exactly so the schema states the null contract rather
///   than leaving every column formally nullable the way lazy Arrow writers
///   do.
/// * `Array(LowCardinality(String))` → `List` of non-nullable `Utf8` items.
///   The corpus has no null tag elements and the target's array elements are
///   not `Nullable`, so the item field says so.
/// * `DateTime64(3)` → `Timestamp(Millisecond)` and `DateTime64(6)` →
///   `Timestamp(Microsecond)`: the tick unit matches the column scale, so the
///   value on the wire is the same `Int64` tick count the binary formats
///   carry, converted by nobody. Both carry an explicit `"UTC"` timezone. An
///   Arrow timestamp *without* a timezone is defined as a wall-clock reading
///   ("naive" time), which a consumer is entitled to interpret in its own
///   local zone — the same server-timezone dependence the JSONEachRow encoder
///   avoids with numeric epochs, avoided here with metadata: with `"UTC"` the
///   ticks are instants and land identically on any server.
fn arrow_field(name: &str, clickhouse_type: &str) -> arrow_schema::Field {
    use arrow_schema::{DataType, Field, TimeUnit};
    let data_type = match clickhouse_type {
        "UInt64" => DataType::UInt64,
        "UInt16" => DataType::UInt16,
        "Int64" => DataType::Int64,
        "LowCardinality(String)" => DataType::Utf8,
        "Nullable(Float64)" => DataType::Float64,
        "Array(LowCardinality(String))" => DataType::List(std::sync::Arc::new(Field::new(
            "item",
            DataType::Utf8,
            false,
        ))),
        "DateTime64(3)" => DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        "DateTime64(6)" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        other => panic!("no Arrow mapping has been decided for ClickHouse type {other}"),
    };
    Field::new(name, data_type, clickhouse_type == "Nullable(Float64)")
}

/// The Arrow schema of one encoded block: [`arrow_field`] over the committed
/// column list, in DDL order.
fn arrow_block_schema() -> arrow_schema::Schema {
    arrow_schema::Schema::new(
        corpus::COLUMNS
            .iter()
            .map(|(name, ty)| arrow_field(name, ty))
            .collect::<Vec<_>>(),
    )
}

/// Encodes one ArrowStream block over `batches` batches of the workload's rows:
/// the IPC stream framing around a single `RecordBatch`.
///
/// One record batch per block rather than many, mirroring the other encoders:
/// a pre-encoded block covers its whole batch range in one piece, and the
/// block-size caveat recorded on Native ceilings (one large block amortises
/// per-block work an arm's smaller blocks would pay) applies here identically.
///
/// Columnar like Native, so it transposes from [`row_of`] the same way and for
/// the same reason: one definition of what a row is, guarded by the
/// DDL-agreement test. The destructuring below is deliberately total — twelve
/// cells, each matched by variant, the two timestamps by their scale as well —
/// so a reordered or reshaped `row_of` fails loudly here rather than filling a
/// builder with the wrong column's values.
///
/// # Panics
///
/// If a row's shape stops matching the committed columns, or if the assembled
/// arrays disagree with [`arrow_block_schema`] — both of which are encoder
/// defects, not runtime conditions, exactly as with [`Column::push`].
fn encode_arrow_stream_block(lo: u64, batches: u64) -> Block {
    use arrow_array::builder::{
        Float64Builder, Int64Builder, ListBuilder, StringBuilder, TimestampMicrosecondBuilder,
        TimestampMillisecondBuilder, UInt16Builder, UInt64Builder,
    };
    use arrow_array::{ArrayRef, RecordBatch};
    use std::sync::Arc;

    let mut batch_ids = UInt64Builder::new();
    let mut event_seqs = UInt16Builder::new();
    let mut sensors = StringBuilder::new();
    let mut regions = StringBuilder::new();
    let mut name_uppers = StringBuilder::new();
    let mut units = StringBuilder::new();
    let mut values = Int64Builder::new();
    let mut values_scaled = Int64Builder::new();
    let mut qualities = Float64Builder::new();
    // The list builder is told its item field up front so the finished array's
    // type — name and non-nullability included — is byte-identical to what
    // [`arrow_field`] declares; `RecordBatch::try_new` rejects the block
    // otherwise, which is the loud failure this module prefers.
    let mut tags_lists = ListBuilder::new(StringBuilder::new()).with_field(
        arrow_schema::Field::new("item", arrow_schema::DataType::Utf8, false),
    );
    // `with_timezone` here and in the schema: the builders produce the array's
    // data type, and the two spellings of "UTC" must agree or try_new refuses.
    let mut batch_tss = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut send_tss = TimestampMicrosecondBuilder::new().with_timezone("UTC");

    let mut rows = 0u64;
    for batch_id in lo..lo + batches {
        for seq in 0..corpus::EVENTS_PER_BATCH {
            if !corpus::keeps(batch_id, seq) {
                continue;
            }
            let cells: [Cell; 12] =
                row_of(batch_id, seq)
                    .try_into()
                    .unwrap_or_else(|row: Vec<Cell>| {
                        panic!(
                            "a row carries {} cells but the target declares 12",
                            row.len()
                        )
                    });
            let [
                Cell::UInt64(id),
                Cell::UInt16(event_seq),
                Cell::LowCardString(sensor),
                Cell::LowCardString(region),
                Cell::LowCardString(name_upper),
                Cell::LowCardString(unit),
                Cell::Int64(value),
                Cell::Int64(value_scaled),
                Cell::NullableFloat64(quality),
                Cell::LowCardStringArray(tags),
                Cell::DateTime64 {
                    ticks: batch_ts,
                    scale: 3,
                },
                Cell::DateTime64 {
                    ticks: send_ts,
                    scale: 6,
                },
            ] = cells
            else {
                panic!("row shape changed: row_of no longer matches the committed columns")
            };
            batch_ids.append_value(id);
            event_seqs.append_value(event_seq);
            sensors.append_value(sensor);
            regions.append_value(region);
            name_uppers.append_value(name_upper);
            units.append_value(unit);
            values.append_value(value);
            values_scaled.append_value(value_scaled);
            qualities.append_option(quality);
            for tag in &tags {
                tags_lists.values().append_value(tag);
            }
            tags_lists.append(true);
            batch_tss.append_value(batch_ts);
            send_tss.append_value(send_ts);
            rows += 1;
        }
    }

    let schema = Arc::new(arrow_block_schema());
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(batch_ids.finish()),
        Arc::new(event_seqs.finish()),
        Arc::new(sensors.finish()),
        Arc::new(regions.finish()),
        Arc::new(name_uppers.finish()),
        Arc::new(units.finish()),
        Arc::new(values.finish()),
        Arc::new(values_scaled.finish()),
        Arc::new(qualities.finish()),
        Arc::new(tags_lists.finish()),
        Arc::new(batch_tss.finish()),
        Arc::new(send_tss.finish()),
    ];
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
        .expect("the assembled arrays match the declared schema");
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &schema)
        .expect("an IPC stream writer over a Vec cannot fail to construct");
    writer
        .write(&batch)
        .expect("writing one record batch to a Vec cannot fail");
    writer
        .finish()
        .expect("finishing an IPC stream over a Vec cannot fail");
    let body = writer
        .into_inner()
        .expect("unwrapping a finished IPC stream cannot fail");
    Block { body, rows }
}

/// Pre-encodes one insert block covering batches `lo..lo + INSERT_BLOCK_BATCHES`.
///
/// Encoded before the clock starts on purpose. The arms are measured with their
/// encoding inside the window because encoding is their work; the *ceiling* is
/// meant to be a property of ClickHouse, so as much of this rig's own cost as
/// possible is moved outside it.
fn encode_block(format: Format, lo: u64) -> Block {
    encode_batches(format, lo, INSERT_BLOCK_BATCHES)
}

/// [`encode_block`] over an arbitrary number of batches.
///
/// Split out so the encoder's tests can assert over ten batches rather than a
/// thousand: the block size is chosen for the measurement, and paying it in
/// every `cargo test` would make a correctness test slow enough to be skipped.
fn encode_batches(format: Format, lo: u64, batches: u64) -> Block {
    // Native and ArrowStream are columnar, and JSONEachRow is text; none of
    // the three shares anything with the row-oriented binary path below beyond
    // `row_of`, which all of them read so that none can drift from the DDL.
    // One exhaustive match rather than early-return guards, so that a future
    // `Format` variant is a missing arm the compiler refuses — not a silent
    // fall-through that encodes it as RowBinary bytes.
    let with_header = match format {
        Format::Native => return encode_native_block(lo, batches),
        Format::JsonEachRow => return encode_json_each_row_block(lo, batches),
        Format::ArrowStream => return encode_arrow_stream_block(lo, batches),
        Format::RowBinary => false,
        Format::RowBinaryWithNamesAndTypes => true,
    };
    let mut body = Vec::new();
    if with_header {
        let columns = corpus::COLUMNS;
        put_varint(&mut body, columns.len() as u64);
        for (name, _) in columns {
            put_string(&mut body, name);
        }
        for (_, ty) in columns {
            put_string(&mut body, ty);
        }
    }
    let mut rows = 0u64;
    for batch_id in lo..lo + batches {
        for seq in 0..corpus::EVENTS_PER_BATCH {
            if !corpus::keeps(batch_id, seq) {
                continue;
            }
            for cell in row_of(batch_id, seq) {
                cell.write(&mut body);
            }
            rows += 1;
        }
    }
    Block { body, rows }
}

// ---------------------------------------------------------------------------
// The encoder, for the test that proves it
// ---------------------------------------------------------------------------

/// Encodes one insert block of `batches` batches of the workload's rows in
/// `format`, exactly as a ceiling pass would POST it.
///
/// Public for one caller: `harness/tests/native_encoder_matches_clickhouse.rs`,
/// which sends these bytes at a live ClickHouse and checks what lands against
/// [`corpus::run_gates`]. Exporting the pass's own encoder rather than a test
/// double is the whole point — a test that proved something about a second
/// encoder would prove nothing about the one the measurement uses.
///
/// `lo` is the first `batch_id`. Deriving the rows from the corpus means the
/// live test's oracle is [`corpus::expected_range`] over the same range, with no
/// expectation written by hand.
#[must_use]
pub fn encode_insert_block(format: Format, lo: u64, batches: u64) -> Block {
    encode_batches(format, lo, batches)
}

/// POSTs one encoded block into the target table, behind the same statement a
/// ceiling pass sends.
///
/// # Errors
///
/// If the connection fails, the server answers anything but 200, or the body
/// carries a `DB::Exception` — which for a Native block is how a mis-encoded
/// column presents when it is malformed rather than merely wrong.
pub fn insert_encoded_block(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    format: Format,
    block: &Block,
) -> Result<(), String> {
    insert(host, port, user, password, &insert_sql(format), &block.body)
}

// ---------------------------------------------------------------------------
// The insert transport
// ---------------------------------------------------------------------------

/// POSTs one pre-encoded block.
///
/// Written here rather than through [`crate::http`] because that module's body
/// is a `&str` and RowBinary is not UTF-8. Widening it is the right eventual
/// fix; duplicating the request shape for one binary caller is the smaller
/// change, and it keeps a module every other call site depends on out of this
/// one's blast radius.
///
/// # Errors
///
/// If the connection fails, the server answers anything but 200, or the body
/// carries a `DB::Exception`. A refused insert is never counted as absorbed
/// rows: a ceiling inflated by inserts the server rejected would be the most
/// expensive possible number to publish.
fn insert(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    sql: &str,
    body: &[u8],
) -> Result<(), String> {
    use std::io::{Read, Write};

    let path = format!(
        "/?user={}&password={}&query={}",
        query_escaped(user),
        query_escaped(password),
        query_escaped(sql)
    );
    let mut stream = std::net::TcpStream::connect((host, port))
        .map_err(|e| format!("connect {host}:{port}: {e}"))?;
    let timeout = Some(Duration::from_secs(INSERT_TIMEOUT_S));
    let _ = stream.set_read_timeout(timeout);
    let _ = stream.set_write_timeout(timeout);

    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\
         Content-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    stream
        .write_all(head.as_bytes())
        .map_err(|e| format!("write insert head: {e}"))?;
    stream
        .write_all(body)
        .map_err(|e| format!("write insert body: {e}"))?;
    stream.flush().map_err(|e| format!("flush insert: {e}"))?;

    let mut raw = Vec::new();
    stream
        .read_to_end(&mut raw)
        .map_err(|e| format!("read insert response: {e}"))?;
    let response = String::from_utf8_lossy(&raw);
    let status = response.lines().next().unwrap_or_default();
    if !status.contains(" 200 ") {
        return Err(format!("insert refused: {}", response.trim()));
    }
    if response.contains("DB::Exception") {
        return Err(format!("insert refused: {}", response.trim()));
    }
    Ok(())
}

/// Percent-encodes a query-string value.
///
/// The SQL travels in the URL because the body carries the rows. Hand-rolled for
/// the same reason [`crate::http`] is: this is one local request shape, and the
/// unreserved set is four characters long.
fn query_escaped(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

/// How a recorded client location reads in a report line.
///
/// A ceiling that does not say gets `"UNSTATED"` rather than a blank, because a
/// blank in a column is read as "nothing to report" and this is the opposite: it
/// is the field whose absence [`Ceilings::gate`] refuses over.
fn described_location(recorded: &str) -> &str {
    match location_named(recorded) {
        Some(l) => l.name(),
        None if recorded.is_empty() => "UNSTATED",
        None => recorded,
    }
}

/// How a recorded infrastructure envelope reads in a refusal.
///
/// The counterpart of [`described_location`] and it exists for the same reason: a
/// ceiling that records no envelope at all is not a ceiling measured under an
/// envelope called `""`, and printing an empty pair of quotes says the wrong one.
fn described_envelope(recorded: &str) -> String {
    if recorded.is_empty() {
        "no envelope recorded".to_owned()
    } else {
        format!("envelope {recorded}")
    }
}

/// Everything `bench ceiling` prints about one environment's ceilings.
///
/// Built here rather than in the binary so that the refusal a reader is shown
/// and the refusal the driver acts on are produced by one piece of code. The
/// previous version of this command printed a stored constant and a paragraph
/// explaining that it had not measured anything, which is the shape of report
/// that a reader trusts and a gate ignores.
#[must_use]
pub fn describe(ceilings: &Ceilings, gate: &Ceiling) -> String {
    let mut out = String::new();
    match &ceilings.consume {
        None => out.push_str("consume:    not measured\n"),
        Some(c) => out.push_str(&format!(
            "consume:    {} msgs/s, {:.1} MB/s at {} B/message, {} partitions, {} consumers \
             (measured {}, client {})\n",
            c.msgs_per_s,
            c.mb_per_s,
            c.message_bytes,
            c.partitions,
            c.threads,
            c.provenance.date,
            described_location(&c.client),
        )),
    }
    if ceilings.clickhouse.is_empty() {
        out.push_str("clickhouse: not measured for any insert format\n");
    }
    for c in &ceilings.clickhouse {
        out.push_str(&format!(
            "clickhouse: {} — {} rows/s, {:.1} MB/s at {} B/row, {} inserters \
             (measured {}, client {})\n",
            c.format,
            c.rows_per_s,
            c.mb_per_s,
            c.row_bytes,
            c.threads,
            c.provenance.date,
            described_location(&c.client),
        ));
    }
    out.push_str(&format!(
        "\ncorpus:     {} B/message\n",
        gate.corpus_message_bytes
    ));
    if gate.refusals.is_empty() {
        out.push_str(&format!(
            "gate:       an arm above {:.0}% of a ceiling is infra-bound and is recorded as \
             such rather than published\n",
            HEADROOM_LIMIT * 100.0
        ));
    } else {
        out.push_str("\nREFUSED — these ceilings are not gateable as they stand:\n");
        for r in &gate.refusals {
            out.push_str(&format!("  - {r}\n"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provenance(digest: &str) -> Provenance {
        Provenance {
            date: "2026-07-25".to_owned(),
            dataset_version: "d2-test".to_owned(),
            infra_digest: digest.to_owned(),
            host: "test".to_owned(),
            rig: "test".to_owned(),
        }
    }

    fn consume_at(message_bytes: u64, msgs_per_s: u64, digest: &str) -> ConsumeCeiling {
        ConsumeCeiling {
            msgs_per_s,
            mb_per_s: (msgs_per_s * message_bytes) as f64 / 1e6,
            message_bytes,
            partitions: 8,
            broker: "test".to_owned(),
            threads: 4,
            // Every fixture below is a ceiling that is supposed to REACH the
            // gate, so each one says where its client ran. A fixture that left
            // this empty would be refused for that rather than for whatever the
            // test is about, and the test would pass for the wrong reason.
            client: Location::Inside.name().to_owned(),
            window: None,
            broker_cgroup: None,
            provenance: provenance(digest),
        }
    }

    fn ingest_of(format: &str, rows_per_s: u64, digest: &str) -> IngestCeiling {
        IngestCeiling {
            format: format.to_owned(),
            rows_per_s,
            mb_per_s: 1.0,
            row_bytes: 91,
            threads: 16,
            clickhouse: "test".to_owned(),
            client: Location::Inside.name().to_owned(),
            sweep: Vec::new(),
            target_cgroup: None,
            landed: None,
            parts: None,
            network: None,
            settle: None,
            stopped_at: None,
            provenance: provenance(digest),
        }
    }

    /// Drives a sweep with a throughput curve, the way a live pass would, and
    /// returns it for inspection. `curve` answers what the target absorbs at a
    /// given concurrency.
    fn run_sweep(max: u64, curve: impl Fn(u64) -> u64) -> Sweep {
        let mut sweep = Sweep::new(INGEST_CONCURRENCY_FROM, INGEST_CONCURRENCY_STEP, max);
        while let Some(c) = sweep.next_rung() {
            sweep.observe(c, curve(c));
        }
        sweep
    }

    /// A pass with no restriction measures the whole rig, which is what every
    /// committed ceiling has to have been produced by.
    #[test]
    fn a_pass_with_no_restriction_measures_every_format() {
        let all = select_combinations(&[]).expect("an empty restriction selects everything");
        assert_eq!(all, all_combinations());
        assert_eq!(all.len(), FORMATS.len());
        // And it says nothing about itself in the rig string, so a full pass's
        // provenance reads exactly as it did before the flag existed.
        assert_eq!(describe_restriction(&all), "");
    }

    /// The narrowing the envelope search needed: one insert format, because the
    /// combination that binds is a format.
    #[test]
    fn naming_a_format_selects_it_and_records_the_restriction() {
        let only = select_combinations(&["rowbinary".to_owned()]).expect("rowbinary is measurable");
        assert_eq!(only, vec![Format::RowBinary]);
        assert_eq!(describe_restriction(&only), " --only rowbinary");
    }

    /// A restriction is a set, so asking for the same work twice is asking for it
    /// once — and the canonical order is the rig's rather than the operator's, so
    /// two equivalent command lines produce one rig string.
    #[test]
    fn a_restriction_collapses_duplicates_and_keeps_the_rigs_own_order() {
        let typed = [
            "rowbinary".to_owned(),
            "native".to_owned(),
            "rowbinary".to_owned(),
        ];
        assert_eq!(
            select_combinations(&typed).expect("every name is measurable"),
            vec![Format::Native, Format::RowBinary]
        );
    }

    /// A name this rig cannot emit is a refusal naming what it can, not an empty
    /// selection: a typo that measured nothing would look exactly like a pass
    /// that had nothing to measure. `jsoneachrow` is the sharpest case now that
    /// `json_each_row` IS measurable: the near-miss spelling must be refused
    /// rather than forgiven, or the descriptor grammar would have two homes.
    #[test]
    fn a_restriction_naming_a_format_this_rig_cannot_emit_is_refused_with_the_ones_it_can() {
        let e = select_combinations(&["jsoneachrow".to_owned()]).expect_err("must refuse");
        assert!(
            e.contains("cannot \nemit") || e.contains("cannot emit"),
            "{e}"
        );
        assert!(e.contains("native"), "{e}");
        assert!(e.contains("rowbinary_nt"), "{e}");

        // A ':' does not name an axis; the whole argument is read as a format
        // name and refused as one nobody can emit.
        let e = select_combinations(&["rowbinary:c".to_owned()]).expect_err("must refuse");
        assert!(e.contains("cannot"), "{e}");
    }

    /// The state `--only` made reachable, and the reason writing it is refused:
    /// a file whose entries were taken under two envelopes cannot describe the
    /// environment it names.
    #[test]
    fn a_file_holding_ceilings_from_two_envelopes_names_the_ones_that_do_not_belong() {
        let mut c = Ceilings {
            consume: Some(consume_at(4056, 1_600_000, "new")),
            clickhouse: vec![
                ingest_of("rowbinary", 5_000_000, "new"),
                ingest_of("native", 15_000_000, "old"),
            ],
        };
        let stale = c.measured_under_other_envelopes("new");
        assert_eq!(stale.len(), 1, "{stale:?}");
        assert!(stale[0].contains("native"), "{stale:?}");
        assert!(stale[0].contains("envelope old"), "{stale:?}");

        // A full pass leaves nothing behind, which is what makes the refusal
        // actionable rather than permanent.
        c.clickhouse[1] = ingest_of("native", 15_000_000, "new");
        assert!(c.measured_under_other_envelopes("new").is_empty());

        // A ceiling that records no envelope at all is not one measured under an
        // envelope called "", and the refusal has to read that way.
        c.consume = Some(consume_at(4056, 1_600_000, ""));
        assert!(
            c.measured_under_other_envelopes("new")[0].contains("no envelope recorded"),
            "{:?}",
            c.measured_under_other_envelopes("new")
        );
    }

    #[test]
    fn the_headroom_limit_is_the_documented_seventy_percent() {
        // Named rather than inlined at the call site so the methodology and the
        // code cannot state different limits.
        assert!((HEADROOM_LIMIT - 0.70).abs() < f64::EPSILON);
    }

    /// The size the whole refusal turns on. Tied to `golden_corpus.rs`, which
    /// pins 1000 batches at 4,051,124 datum bytes; five bytes of Confluent
    /// framing per message make the mean 4056.
    #[test]
    fn the_corpus_message_is_four_thousand_and_fifty_six_bytes() {
        assert_eq!(corpus_message_bytes(), 4056);
    }

    /// The defect, as a test. The committed figure was taken at 840 bytes — the
    /// corpus's size when a message carried twenty events — and kept as the
    /// denominator after the array grew to a hundred. Gating against it asserts
    /// a byte rate 4.8x what the same pass sustained.
    #[test]
    fn a_ceiling_measured_at_the_old_message_size_is_refused_rather_than_extrapolated() {
        let ceilings = Ceilings {
            consume: Some(consume_at(840, 305_554, "envelope")),
            clickhouse: Vec::new(),
        };
        let gate = ceilings.gate(corpus_message_bytes(), "envelope");
        assert_eq!(
            gate.consume_msgs_per_s, 0,
            "a refused ceiling gates nothing"
        );
        assert_eq!(gate.refusals().len(), 1, "{:?}", gate.refusals());
        assert!(gate.refusals()[0].contains("840"), "{:?}", gate.refusals());
        assert!(gate.refusals()[0].contains("4056"), "{:?}", gate.refusals());
    }

    /// The rule has to be a band rather than an equality, or re-encoding a
    /// timestamp one byte wider would invalidate a perfectly good ceiling.
    #[test]
    fn a_ceiling_measured_within_the_tolerance_band_still_gates() {
        let current = corpus_message_bytes();
        let ceilings = Ceilings {
            consume: Some(consume_at(current * 102 / 100, 300_000, "envelope")),
            clickhouse: Vec::new(),
        };
        let gate = ceilings.gate(current, "envelope");
        assert!(gate.refusals().is_empty(), "{:?}", gate.refusals());
        assert_eq!(gate.consume_msgs_per_s, 300_000);
    }

    /// Inside the band the two readings disagree slightly, and the stricter one
    /// has to win — otherwise widening the tolerance would be a way to make
    /// every arm look further from the ceiling than it is.
    #[test]
    fn inside_the_tolerance_band_the_stricter_of_the_two_readings_binds() {
        let current = corpus_message_bytes();
        // Measured against messages 4% smaller, so the same message rate is a
        // smaller byte rate and the byte reading is the harsher of the two.
        let ceilings = Ceilings {
            consume: Some(consume_at(current * 96 / 100, 100_000, "envelope")),
            clickhouse: Vec::new(),
        };
        let gate = ceilings.gate(current, "envelope");
        let headroom = gate.headroom(Achieved {
            msgs_per_s: 50_000.0,
            rows_per_s: 5_000_000.0,
            wire_format: "rowbinary",
            server_side_transform: false,
        });
        let consume = headroom
            .shares()
            .iter()
            .find(|s| s.against == "broker consume")
            .expect("the consume ceiling was checked");
        assert!(
            consume.share > 0.50,
            "the byte reading must exceed the naive 50% message reading, got {}",
            consume.share
        );
    }

    /// The old file recorded no envelope at all, which is the other half of "the
    /// figure was silently wrong because nothing recorded what it was measured
    /// against".
    #[test]
    fn a_ceiling_that_records_no_infrastructure_envelope_is_refused() {
        let ceilings = Ceilings {
            consume: Some(consume_at(corpus_message_bytes(), 300_000, "")),
            clickhouse: Vec::new(),
        };
        let gate = ceilings.gate(corpus_message_bytes(), "envelope");
        assert_eq!(gate.consume_msgs_per_s, 0);
        assert!(
            gate.refusals()[0].contains("envelope"),
            "{:?}",
            gate.refusals()
        );
    }

    /// A ceiling taken under other broker caps is not this environment's
    /// ceiling, and scaling one to the other is not something this harness does.
    #[test]
    fn a_ceiling_measured_under_another_envelope_is_refused() {
        let ceilings = Ceilings {
            consume: Some(consume_at(corpus_message_bytes(), 300_000, "old-envelope")),
            clickhouse: vec![ingest_of("rowbinary", 6_000_000, "old-envelope")],
        };
        let gate = ceilings.gate(corpus_message_bytes(), "new-envelope");
        assert_eq!(gate.consume_msgs_per_s, 0);
        assert_eq!(gate.refusals().len(), 2, "{:?}", gate.refusals());
    }

    /// The fifth defect, as a test, and the reason it is a field rather than a
    /// note. Every ceiling this repository committed before the field existed was
    /// taken from the host through Docker's published ports, and on this host
    /// that path is worth an order of magnitude in both directions — so a figure
    /// that cannot say which side it came from is not a slightly stale ceiling,
    /// it is an unknown one.
    #[test]
    fn a_ceiling_that_does_not_say_where_its_client_ran_is_refused_rather_than_assumed_inside() {
        let mut consume = consume_at(corpus_message_bytes(), 300_000, "envelope");
        let mut ingest = ingest_of("rowbinary", 6_000_000, "envelope");
        consume.client = String::new();
        ingest.client = String::new();
        let gate = Ceilings {
            consume: Some(consume),
            clickhouse: vec![ingest],
        }
        .gate(corpus_message_bytes(), "envelope");

        assert_eq!(
            gate.consume_msgs_per_s, 0,
            "a refused ceiling gates nothing"
        );
        assert_eq!(gate.refusals().len(), 2, "{:?}", gate.refusals());
        for why in gate.refusals() {
            assert!(why.contains("predates the field"), "{why}");
            assert!(why.contains(crate::docker::NETWORK), "{why}");
        }
        // And no ClickHouse ceiling survived to gate the arm with, which is the
        // consequence that matters: a refused ingest ceiling must not silently
        // become "the arm cleared it".
        let headroom = gate.headroom(Achieved {
            msgs_per_s: 40_562.0,
            rows_per_s: 4_056_210.0,
            wire_format: "rowbinary",
            server_side_transform: false,
        });
        assert!(headroom.shares().is_empty(), "{:?}", headroom.shares());
        assert!(!headroom.is_proven());
    }

    /// The same refusal for a ceiling that says plainly that it was taken
    /// outside. Recording the fact honestly does not make the figure gateable —
    /// it makes the reason it is dropped legible.
    #[test]
    fn a_ceiling_measured_from_outside_the_bench_network_is_refused_by_the_gate() {
        let mut consume = consume_at(corpus_message_bytes(), 68_000, "envelope");
        consume.client = Location::Outside.name().to_owned();
        let mut ingest = ingest_of("native", 2_400_000, "envelope");
        ingest.client = "somewhere".to_owned();
        let gate = Ceilings {
            consume: Some(consume),
            clickhouse: vec![ingest],
        }
        .gate(corpus_message_bytes(), "envelope");

        assert_eq!(gate.consume_msgs_per_s, 0);
        assert_eq!(gate.refusals().len(), 2, "{:?}", gate.refusals());
        assert!(
            gate.refusals()[0].contains("OUTSIDE"),
            "{:?}",
            gate.refusals()
        );
        assert!(
            gate.refusals()[1].contains("\"somewhere\""),
            "a value that names no side is named back at the reader: {:?}",
            gate.refusals()
        );
    }

    /// An empty file is not a passing gate. It has to say that no ceiling
    /// exists, or `bench validate` would report an environment as gateable on
    /// the strength of having nothing to check.
    #[test]
    fn an_environment_with_no_measured_ceiling_refuses_rather_than_passes() {
        let gate = Ceilings::default().gate(corpus_message_bytes(), "envelope");
        assert_eq!(gate.consume_msgs_per_s, 0);
        assert!(!gate.refusals().is_empty());
        let headroom = gate.headroom(Achieved {
            msgs_per_s: 1.0,
            rows_per_s: 1.0,
            wire_format: "rowbinary",
            server_side_transform: false,
        });
        assert!(!headroom.is_proven());
        assert!(!headroom.infra_bound(), "an unproven gate is not a breach");
    }

    /// The methodology says *either* ceiling. An arm comfortably inside the
    /// consume ceiling and over the ClickHouse one is infra-bound, and reading
    /// only the consume figure is how it would have been published.
    #[test]
    fn an_arm_bound_by_clickhouse_and_not_by_the_broker_is_still_infra_bound() {
        let current = corpus_message_bytes();
        let ceilings = Ceilings {
            consume: Some(consume_at(current, 1_000_000, "envelope")),
            clickhouse: vec![ingest_of("rowbinary", 5_000_000, "envelope")],
        };
        let gate = ceilings.gate(current, "envelope");
        let headroom = gate.headroom(Achieved {
            // 5% of the consume ceiling, 90% of the ClickHouse one.
            msgs_per_s: 50_000.0,
            rows_per_s: 4_500_000.0,
            wire_format: "rowbinary",
            server_side_transform: false,
        });
        assert!(headroom.is_proven());
        assert!(headroom.infra_bound());
        assert_eq!(
            headroom.binding().expect("a binding ceiling").against,
            "clickhouse ingest (rowbinary)"
        );
    }

    /// Rule 5 says the insert format materially changes server-side work, so an
    /// arm is never gated against a format it does not write. `protobuf` is a
    /// live instance: a format arms could declare and this rig does not encode.
    /// (`jsoneachrow` was the example here until the `json_each_row` encoder
    /// landed; the near-miss spelling still resolves to nothing, which the
    /// last assertion keeps true.)
    #[test]
    fn an_arm_whose_insert_format_has_no_ceiling_is_not_gated_against_another_format() {
        let current = corpus_message_bytes();
        let ceilings = Ceilings {
            consume: Some(consume_at(current, 1_000_000, "envelope")),
            clickhouse: vec![ingest_of("rowbinary", 5_000_000, "envelope")],
        };
        let headroom = ceilings.gate(current, "envelope").headroom(Achieved {
            msgs_per_s: 50_000.0,
            // Far over the RowBinary ceiling, and deliberately not gated by it.
            rows_per_s: 50_000_000.0,
            wire_format: "protobuf",
            server_side_transform: false,
        });
        assert!(!headroom.infra_bound());
        assert!(!headroom.is_proven());
        assert!(
            headroom.unproven().iter().any(|u| u.contains("protobuf")),
            "{:?}",
            headroom.unproven()
        );
        assert!(Format::parse("protobuf").is_none());
        assert!(Format::parse("jsoneachrow").is_none());
        assert_eq!(Format::parse("json_each_row"), Some(Format::JsonEachRow));
        assert_eq!(Format::parse("arrow_stream"), Some(Format::ArrowStream));
    }

    /// The gap this encoder closes, as a test. Every headline arm of this
    /// benchmark's own vendor declares `wire_format = "native"`, and until the
    /// rig could emit a Native block those were the only arms not gated against
    /// ClickHouse at all. A measured Native ceiling must now reach them.
    #[test]
    fn a_native_arm_is_gated_against_a_native_ceiling_rather_than_left_unproven() {
        let current = corpus_message_bytes();
        let ceilings = Ceilings {
            consume: Some(consume_at(current, 1_000_000, "envelope")),
            clickhouse: vec![ingest_of("native", 5_000_000, "envelope")],
        };
        let headroom = ceilings.gate(current, "envelope").headroom(Achieved {
            msgs_per_s: 50_000.0,
            rows_per_s: 4_500_000.0,
            wire_format: "native",
            server_side_transform: false,
        });
        assert!(headroom.is_proven(), "{:?}", headroom.unproven());
        assert!(
            headroom.infra_bound(),
            "90% of the ceiling is over the limit"
        );
        assert_eq!(headroom.applied_ingest_rows_per_s(), Some(5_000_000));
        assert_eq!(Format::parse("native"), Some(Format::Native));
    }

    /// Every format the rig claims to measure has to name a ClickHouse format
    /// and a descriptor value, and the two must not collide.
    #[test]
    fn every_measurable_format_names_a_distinct_descriptor_value() {
        let mut seen = std::collections::BTreeSet::new();
        for f in FORMATS {
            assert!(
                seen.insert(f.wire_format()),
                "{f:?} duplicates a wire format"
            );
            assert_eq!(Format::parse(f.wire_format()), Some(f));
            assert!(!f.clickhouse_name().is_empty());
        }
    }

    /// The column order is the RowBinary wire contract. If a row's cells and the
    /// committed column list ever disagreed, every measured row would be
    /// misread by the server — and a ceiling taken against rejected inserts is
    /// the most expensive possible number to publish.
    #[test]
    fn every_encoded_row_declares_the_columns_the_ddl_declares_in_order() {
        let declared: Vec<String> = corpus::COLUMNS
            .iter()
            .map(|(_, ty)| (*ty).to_owned())
            .collect();
        for batch_id in [0u64, 1, 9, 10, 137] {
            for seq in [0u32, 1, 7] {
                let row: Vec<String> = row_of(batch_id, seq)
                    .iter()
                    .map(Cell::declared_type)
                    .collect();
                assert_eq!(row, declared, "row {batch_id}/{seq}");
            }
        }
    }

    /// ClickHouse's varint is unsigned LEB128, and a string is one of those
    /// followed by its bytes. Pinned because a block whose lengths are wrong is
    /// rejected wholesale, which reads as "ClickHouse is slow" rather than as an
    /// encoder bug.
    #[test]
    fn varints_and_strings_encode_the_way_clickhouse_reads_them() {
        let mut out = Vec::new();
        put_varint(&mut out, 0);
        put_varint(&mut out, 127);
        put_varint(&mut out, 128);
        put_varint(&mut out, 300);
        assert_eq!(out, vec![0x00, 0x7f, 0x80, 0x01, 0xac, 0x02]);

        let mut s = Vec::new();
        put_string(&mut s, "ms");
        assert_eq!(s, vec![0x02, b'm', b's']);
    }

    /// A null and a present value are different widths on the wire, so a row's
    /// size depends on the corpus's null pattern rather than being constant.
    #[test]
    fn a_null_quality_writes_one_byte_and_a_present_one_writes_nine() {
        let mut null = Vec::new();
        Cell::NullableFloat64(None).write(&mut null);
        assert_eq!(null, vec![1]);

        let mut present = Vec::new();
        Cell::NullableFloat64(Some(0.5)).write(&mut present);
        assert_eq!(present.len(), 9);
        assert_eq!(present[0], 0);
    }

    /// The name-and-type header is what makes the server validate the column
    /// contract instead of trusting position, and it is generated from the same
    /// committed list the rows are.
    #[test]
    fn the_names_and_types_header_carries_every_declared_column() {
        let plain = encode_batches(Format::RowBinary, 0, 10);
        let headed = encode_batches(Format::RowBinaryWithNamesAndTypes, 0, 10);
        assert_eq!(plain.rows, headed.rows);
        assert!(headed.body.len() > plain.body.len());
        let header = String::from_utf8_lossy(&headed.body[..headed.body.len() - plain.body.len()])
            .into_owned();
        for (name, ty) in corpus::COLUMNS {
            assert!(header.contains(name), "header omits {name}");
            assert!(header.contains(ty), "header omits type {ty}");
        }
    }

    /// The workload filters, so a block carries fewer rows than the batches'
    /// event count — and exactly the count the corpus's own oracle expects. A
    /// block that carried every event would be measuring an insert nobody
    /// performs.
    #[test]
    fn a_block_carries_exactly_the_rows_the_workload_keeps() {
        for format in FORMATS {
            let block = encode_batches(format, 0, 10);
            assert_eq!(block.rows, corpus::expected_rows(10), "{format:?}");
            assert!(
                block.rows < 10 * u64::from(corpus::EVENTS_PER_BATCH),
                "{format:?}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // The JSONEachRow encoder
    //
    // Exact-text assertions, because the whole defence of this encoder is that
    // its serialised forms are pinned: a timestamp that drifted into a
    // formatted date string, or a float that grew digits, would still be
    // "valid JSON" and would land different values on a differently configured
    // server. The live proof against a real ClickHouse is in
    // `harness/tests/native_encoder_matches_clickhouse.rs`.
    // -----------------------------------------------------------------------

    /// The serialised form the timestamp decision promises: a quoted decimal
    /// epoch — the quotes are load-bearing, 26.3's number path refused a
    /// fractional bare number — with exactly the column scale of fractional
    /// digits, zero-padded, trailing zeros kept, so `seconds * 10^scale +
    /// fraction == ticks` by construction and no timezone is consulted
    /// anywhere.
    #[test]
    fn a_datetime64_serialises_as_epoch_seconds_with_exactly_the_column_scale_of_digits() {
        let mut out = String::new();

        // Sub-second digits at scale 3: the millisecond lands in the fraction.
        push_json_datetime64(&mut out, 1_772_000_000_123, 3);
        assert_eq!(out, "\"1772000000.123\"");

        // A whole second keeps its zeros rather than shortening: the fraction
        // width is the scale, always.
        out.clear();
        push_json_datetime64(&mut out, 1_772_000_000_000, 3);
        assert_eq!(out, "\"1772000000.000\"");

        // Scale 6, with sub-second digits an f64 could not carry exactly —
        // the case that makes digit-by-digit parsing load-bearing.
        out.clear();
        push_json_datetime64(&mut out, 1_772_000_000_000_123, 6);
        assert_eq!(out, "\"1772000000.000123\"");

        out.clear();
        push_json_datetime64(&mut out, 1_772_000_000_000_000, 6);
        assert_eq!(out, "\"1772000000.000000\"");
    }

    /// Before the epoch there is no serialised form at all. The refusal is a
    /// measured decision, not caution: 26.3 accepts `"-0.001"` and lands
    /// `+0.001` — the sign silently dropped, the row kept — so an encoder
    /// that emitted the sign would corrupt values the server never complains
    /// about. See [`push_json_datetime64`] for the measurement.
    #[test]
    #[should_panic(expected = "pre-epoch")]
    fn a_pre_epoch_datetime64_is_refused_rather_than_landed_with_its_sign_dropped() {
        let mut out = String::new();
        push_json_datetime64(&mut out, -1, 3);
    }

    /// The escaper's contract: quotes, backslashes and control characters, and
    /// nothing else — the corpus's ASCII identifiers pass through untouched.
    #[test]
    fn json_strings_escape_quotes_backslashes_and_control_characters() {
        let mut plain = String::new();
        push_json_string(&mut plain, "sensor-17");
        assert_eq!(plain, "\"sensor-17\"");

        let mut escaped = String::new();
        push_json_string(&mut escaped, "a\"b\\c\n");
        assert_eq!(escaped, "\"a\\\"b\\\\c\\u000a\"");
    }

    /// The block's first lines, byte for byte.
    ///
    /// Hand-derived from the corpus's committed generator constants rather
    /// than recomputed through the encoder's own helpers, so this test cannot
    /// agree with a shared mistake. Batch 0 keeps seqs 0, 4 and 5 first: 1 and
    /// 2 fall to the quality floor (0.07 and 0.14 < 0.2), 3 to the `drop`
    /// unit. Batch 0 also pins the two coalesces the gate cares about — the
    /// null region landing as `""` and the null quality landing as JSON
    /// `null` — plus an empty and a non-empty tags array, a present quality
    /// (0.28, shortest-round-trip), and both timestamp scales at the corpus
    /// base timestamp.
    ///
    /// If a generator constant in `workload.toml` moves, these literals move
    /// with it — the same coupling `DATASET_VERSION` exists to make loud.
    #[test]
    fn the_first_json_lines_are_byte_for_byte_the_corpus_rows_they_encode() {
        let block = encode_insert_block(Format::JsonEachRow, 0, 1);
        let text = String::from_utf8(block.body).expect("a JSON block is UTF-8");
        assert!(text.ends_with('\n'), "every line is newline-terminated");
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(block.rows as usize, lines.len());

        assert_eq!(
            lines[0],
            "{\"batch_id\":0,\"event_seq\":0,\"sensor\":\"sensor-0\",\"region\":\"\",\
             \"name_upper\":\"METRIC_0\",\"unit\":\"count\",\"value\":0,\"value_scaled\":0,\
             \"quality\":null,\"tags\":[],\"batch_ts\":\"1772000000.000\",\
             \"send_ts\":\"1772000000.000000\"}"
        );
        assert_eq!(
            lines[1],
            "{\"batch_id\":0,\"event_seq\":4,\"sensor\":\"sensor-0\",\"region\":\"\",\
             \"name_upper\":\"METRIC_4\",\"unit\":\"ratio\",\"value\":388,\
             \"value_scaled\":77600,\"quality\":0.28,\"tags\":[],\
             \"batch_ts\":\"1772000000.000\",\"send_ts\":\"1772000000.000000\"}"
        );
        assert_eq!(
            lines[2],
            "{\"batch_id\":0,\"event_seq\":5,\"sensor\":\"sensor-0\",\"region\":\"\",\
             \"name_upper\":\"METRIC_5\",\"unit\":\"celsius\",\"value\":485,\
             \"value_scaled\":80833,\"quality\":null,\"tags\":[\"tag-5\"],\
             \"batch_ts\":\"1772000000.000\",\"send_ts\":\"1772000000.000000\"}"
        );
    }

    /// Every line of a multi-batch block through an independent parser —
    /// `serde_json`, which shares no code with the hand-rolled writer — and
    /// field-by-field against the corpus's own derivations.
    ///
    /// The timestamps come back as the strings they are sent as — the quoted
    /// form exists precisely so no float sits on the path — and their ticks
    /// are recovered here by deleting the point, which the fixed-width
    /// fraction makes an exact inversion.
    #[test]
    fn a_json_block_agrees_with_the_corpus_through_an_independent_parser() {
        let batches = 3;
        let block = encode_insert_block(Format::JsonEachRow, 0, batches);
        let text = String::from_utf8(block.body).expect("a JSON block is UTF-8");

        let mut kept = Vec::new();
        for batch_id in 0..batches {
            for seq in 0..corpus::EVENTS_PER_BATCH {
                if corpus::keeps(batch_id, seq) {
                    kept.push((batch_id, seq));
                }
            }
        }
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), kept.len());

        // A property of the column list rather than of any line, so derived
        // once rather than rebuilt and re-sorted per row.
        let mut sorted: Vec<&str> = corpus::COLUMNS.iter().map(|(n, _)| *n).collect();
        sorted.sort_unstable();

        for (line, &(batch_id, seq)) in lines.iter().zip(&kept) {
            let v: serde_json::Value =
                serde_json::from_str(line).unwrap_or_else(|e| panic!("{line}: {e}"));
            let obj = v.as_object().expect("a row is an object");
            let keys: Vec<&str> = obj.keys().map(String::as_str).collect();
            assert_eq!(keys, sorted, "serde_json sorts keys; the sets must match");

            assert_eq!(obj["batch_id"].as_u64(), Some(batch_id));
            assert_eq!(obj["event_seq"].as_u64(), Some(u64::from(seq)));
            assert_eq!(obj["sensor"].as_str(), Some(&*corpus::sensor_of(batch_id)));
            assert_eq!(
                obj["region"].as_str(),
                Some(&*corpus::region_of(batch_id).unwrap_or_default())
            );
            assert_eq!(
                obj["name_upper"].as_str(),
                Some(&*corpus::ascii_upper(&corpus::name_of(batch_id, seq)))
            );
            assert_eq!(obj["unit"].as_str(), Some(corpus::unit_of(batch_id, seq)));
            let value = corpus::value_of(batch_id, seq);
            assert_eq!(obj["value"].as_i64(), Some(value));
            assert_eq!(
                obj["value_scaled"].as_i64(),
                Some(corpus::value_scaled_of(value, seq))
            );
            // Shortest-round-trip printing means the parsed f64 is bit-equal
            // to the generated one, so exact comparison is the correct check.
            assert_eq!(obj["quality"].as_f64(), corpus::quality_of(batch_id, seq));
            let tags: Vec<&str> = obj["tags"]
                .as_array()
                .expect("tags is an array")
                .iter()
                .map(|t| t.as_str().expect("a tag is a string"))
                .collect();
            assert_eq!(tags, corpus::tags_of(batch_id, seq));
            // The quoted decimal epoch is fixed-width, so deleting the point
            // must recover the exact tick count — an independent inversion of
            // the encoder's `seconds * 10^scale + fraction` construction.
            let ticks_of = |v: &serde_json::Value| -> i64 {
                v.as_str()
                    .expect("a timestamp is a quoted decimal epoch")
                    .replace('.', "")
                    .parse()
                    .expect("digits either side of one point")
            };
            assert_eq!(ticks_of(&obj["batch_ts"]), corpus::batch_ts_ms_of(batch_id));
            assert_eq!(
                ticks_of(&obj["send_ts"]),
                corpus::send_ts_us_prefill(batch_id)
            );
        }
    }

    // -----------------------------------------------------------------------
    // The ArrowStream encoder
    //
    // Structural round-trip rather than exact bytes: the IPC framing carries
    // flatbuffer padding this crate has no business pinning. What is pinned is
    // everything ClickHouse acts on — the schema, field for field, and the
    // values — read back through arrow-ipc's own reader, which shares no code
    // with the builder path that wrote them.
    // -----------------------------------------------------------------------

    /// The declared schema, field for field: names in DDL order, the mapped
    /// types, `quality` alone nullable, non-nullable list items, and the
    /// explicit `"UTC"` on both timestamps — the field whose absence would let
    /// a server read the ticks as wall-clock time.
    #[test]
    fn the_arrow_schema_maps_every_committed_column_the_documented_way() {
        use arrow_schema::{DataType, Field, TimeUnit};

        let schema = arrow_block_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let declared: Vec<&str> = corpus::COLUMNS.iter().map(|(n, _)| *n).collect();
        assert_eq!(names, declared);

        for field in schema.fields() {
            assert_eq!(
                field.is_nullable(),
                field.name() == "quality",
                "{} must mirror the DDL's null contract",
                field.name()
            );
        }
        assert_eq!(
            schema
                .field_with_name("batch_id")
                .expect("batch_id")
                .data_type(),
            &DataType::UInt64
        );
        assert_eq!(
            schema
                .field_with_name("sensor")
                .expect("sensor")
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema
                .field_with_name("quality")
                .expect("quality")
                .data_type(),
            &DataType::Float64
        );
        assert_eq!(
            schema.field_with_name("tags").expect("tags").data_type(),
            &DataType::List(std::sync::Arc::new(Field::new(
                "item",
                DataType::Utf8,
                false
            )))
        );
        assert_eq!(
            schema
                .field_with_name("batch_ts")
                .expect("batch_ts")
                .data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert_eq!(
            schema
                .field_with_name("send_ts")
                .expect("send_ts")
                .data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    /// The stream read back through arrow-ipc's own reader: the schema
    /// travels, the row count is the workload's, and the first rows hold the
    /// corpus's values — including the null quality, the present quality, the
    /// empty and non-empty tags rows and both timestamps' raw tick counts.
    #[test]
    fn an_arrow_stream_block_round_trips_through_the_ipc_reader() {
        use arrow_array::{
            Array, Float64Array, ListArray, StringArray, TimestampMicrosecondArray,
            TimestampMillisecondArray, UInt16Array, UInt64Array,
        };

        let batches = 10;
        let block = encode_insert_block(Format::ArrowStream, 0, batches);
        let reader =
            arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(&block.body[..]), None)
                .expect("the body opens as an IPC stream");
        assert_eq!(*reader.schema(), arrow_block_schema());

        let read: Vec<arrow_array::RecordBatch> = reader
            .collect::<Result<_, _>>()
            .expect("every message in the stream decodes");
        // One record batch per block is part of the encoding's shape — see
        // `encode_arrow_stream_block` — not an accident of this input.
        assert_eq!(read.len(), 1);
        let batch = &read[0];
        assert_eq!(batch.num_rows() as u64, block.rows);
        assert_eq!(block.rows, corpus::expected_rows(batches));

        let batch_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("batch_id is UInt64");
        let event_seqs = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("event_seq is UInt16");
        let sensors = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("sensor is Utf8");
        let qualities = batch
            .column(8)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("quality is Float64");
        let tags = batch
            .column(9)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("tags is a list");
        let batch_tss = batch
            .column(10)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("batch_ts is Timestamp(ms)");
        let send_tss = batch
            .column(11)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("send_ts is Timestamp(us)");

        // Rows 0..3 are batch 0's kept seqs 0, 4 and 5 — the same rows the
        // JSON exact-text test derives by hand, checked against the corpus
        // here so the two encoder tests cannot drift apart.
        assert_eq!(batch_ids.value(0), 0);
        assert_eq!(event_seqs.value(0), 0);
        assert_eq!(sensors.value(0), "sensor-0");
        assert!(qualities.is_null(0), "quality (0,0) is the corpus's null");
        assert_eq!(tags.value(0).len(), 0, "tags (0,0) is empty");
        assert_eq!(batch_tss.value(0), corpus::batch_ts_ms_of(0));
        assert_eq!(send_tss.value(0), corpus::send_ts_us_prefill(0));

        assert_eq!(event_seqs.value(1), 4);
        // The Arrow wire carries the raw f64 bits, so the round trip is
        // bit-exact and the comparison is too — the same reason the JSON twin
        // compares exactly after shortest-round-trip printing.
        assert_eq!(qualities.value(1), 0.28);

        assert_eq!(event_seqs.value(2), 5);
        let row2_tags = tags.value(2);
        let row2_tags = row2_tags
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("tag items are Utf8");
        assert_eq!(row2_tags.len(), 1);
        assert_eq!(row2_tags.value(0), "tag-5");
    }

    // -----------------------------------------------------------------------
    // The ingest concurrency sweep
    //
    // The rule that decides whether a measured figure is a ceiling or a floor,
    // exercised here against synthetic throughput curves rather than against a
    // server. That is not a convenience: the live pass takes minutes and one
    // shape of curve per run, so a rule proven only by running it would be a
    // rule proven for whichever curve the target happened to produce that day.
    // -----------------------------------------------------------------------

    /// The defect, as a test. The consume pass's partition bound is correct and
    /// stays; what was wrong was that the same number reached the ingest pass,
    /// where partitions mean nothing — so ClickHouse was measured at eight-way
    /// concurrency because the topic had eight partitions, and every published
    /// arm exceeded the result.
    #[test]
    fn the_consume_pass_refuses_more_threads_than_partitions_and_the_ingest_sweep_is_not_bound_by_them()
     {
        assert!(consume_threads_fit_partitions(16, 8).is_err());
        assert!(consume_threads_fit_partitions(9, 8).is_err());
        assert!(consume_threads_fit_partitions(8, 8).is_ok());
        assert!(consume_threads_fit_partitions(4, 8).is_ok());

        // The ingest ladder is a function of the target's own answers and of
        // nothing else, so it climbs past a partition count it never sees.
        let sweep = run_sweep(INGEST_CONCURRENCY_MAX, |c| c * 100_000);
        assert!(
            sweep.points().iter().any(|p| p.concurrency > 8),
            "the sweep must be able to exceed the topic's partition count: {:?}",
            sweep.points()
        );
    }

    /// The split is what the partition bound above is a refusal about, so the two
    /// have to agree: within the bound every consumer gets partitions, and they
    /// are dealt round-robin rather than in contiguous runs.
    #[test]
    fn the_partition_split_deals_every_partition_round_robin_and_leaves_no_consumer_idle() {
        assert_eq!(
            partition_split(4, 8),
            vec![vec![0, 4], vec![1, 5], vec![2, 6], vec![3, 7]]
        );
        assert_eq!(
            partition_split(8, 8),
            (0..8).map(|p| vec![p]).collect::<Vec<_>>()
        );
        assert_eq!(partition_split(1, 3), vec![vec![0, 1, 2]]);
        // Uneven, but every consumer still has something: the bound refuses the
        // case where one would not.
        assert_eq!(
            partition_split(3, 8),
            vec![vec![0, 3, 6], vec![1, 4, 7], vec![2, 5]]
        );
        for split in [partition_split(4, 8), partition_split(3, 8)] {
            assert!(split.iter().all(|c| !c.is_empty()), "{split:?}");
        }
    }

    /// The consume pass's new constraint, and it only exists because the client
    /// moved inside the network: at 1.72M messages a second the whole corpus is
    /// 0.87s of backlog, so a window taken at face value from `--seconds 8`
    /// would spend seven of its eight seconds measuring an idle broker. The
    /// window is derived from the backlog instead, and the operator's number is
    /// still an upper bound on it.
    #[test]
    fn a_window_longer_than_the_backlog_is_shortened_to_fit_it_rather_than_left_to_drain() {
        let depths = vec![187_500u64; 8];
        let split = partition_split(8, 8);

        // 1.64M/s over 1.5M messages is 0.915s of backlog, and the window is
        // the share of that the margin allows. Derived from the constant rather
        // than typed, so tightening the margin does not turn this into a test
        // somebody edits.
        let fast = backlog_window(&depths, &split, 1_640_000.0, Duration::from_secs(8));
        assert!(
            (fast.as_secs_f64() - 1_500_000.0 / 1_640_000.0 * CONSUME_BACKLOG_SHARE).abs() < 0.005,
            "{:?}",
            fast.as_secs_f64()
        );

        // A slow client has backlog to spare, and then the operator's window is
        // what runs: the rule shortens a window, it never lengthens one.
        let slow = backlog_window(&depths, &split, 68_000.0, Duration::from_secs(8));
        assert_eq!(slow, Duration::from_secs(8));

        // The shallowest consumer decides, because it is the first to run dry
        // and its partition ending the window ends it for everybody.
        let uneven = vec![187_500, 187_500, 187_500, 20_000];
        let short = backlog_window(
            &uneven,
            &partition_split(4, 4),
            1_000_000.0,
            Duration::from_secs(8),
        );
        assert!(
            short.as_secs_f64() < 0.2,
            "the 20,000-message partition has to bound it: {short:?}"
        );

        // A rate nobody measured is not a reason to invent a window.
        assert_eq!(
            backlog_window(&depths, &split, 0.0, Duration::from_secs(8)),
            Duration::from_secs(8)
        );
    }

    /// A target that keeps absorbing more has not been shown a ceiling, and the
    /// pass says so instead of publishing the largest number it happened to
    /// reach. This is the whole difference between a ceiling and a floor.
    #[test]
    fn a_sweep_still_climbing_at_its_bound_is_refused_rather_than_reported() {
        let sweep = run_sweep(INGEST_CONCURRENCY_MAX, |c| c * 100_000);
        assert_eq!(
            sweep.points().last().map(|p| p.concurrency),
            Some(INGEST_CONCURRENCY_MAX),
            "it must climb all the way to the bound"
        );
        assert!(sweep.still_climbing());
        assert_eq!(
            sweep.best().map(|p| p.concurrency),
            Some(INGEST_CONCURRENCY_MAX)
        );
    }

    /// The ordinary case: throughput flattens, the sweep spends its patience
    /// proving it and reports the rung that won rather than the last rung tried.
    #[test]
    fn a_sweep_that_flattens_reports_the_rung_that_won_and_stops_two_rungs_later() {
        let sweep = run_sweep(INGEST_CONCURRENCY_MAX, |c| c.min(8) * 100_000);
        assert!(!sweep.still_climbing());
        let best = sweep.best().expect("a winning rung");
        assert_eq!(best.concurrency, 8);
        assert_eq!(best.rows_per_s, 800_000);
        assert_eq!(
            sweep
                .points()
                .iter()
                .map(|p| p.concurrency)
                .collect::<Vec<_>>(),
            vec![2, 4, 8, 16, 32],
            "two rungs past the winner is a plateau; a third would be paid for nothing"
        );
    }

    /// Run-to-run spread on this host reaches 14.5%, so a single rung landing
    /// below its predecessor is not evidence of anything. A rule that stopped at
    /// the first non-improvement would end this sweep at 4 and report less than
    /// half of what the target absorbs.
    #[test]
    fn a_single_rung_lost_to_noise_does_not_end_a_sweep_that_is_still_climbing() {
        let sweep = run_sweep(32, |c| match c {
            2 => 1_000_000,
            4 => 900_000,
            8 => 2_000_000,
            _ => 2_000_000,
        });
        let best = sweep.best().expect("a winning rung");
        assert_eq!(best.concurrency, 8);
        assert_eq!(best.rows_per_s, 2_000_000);
        assert!(!sweep.still_climbing());
        assert!(
            sweep.points().iter().any(|p| p.concurrency == 8),
            "the sweep must survive the dip at 4: {:?}",
            sweep.points()
        );
    }

    /// A gain inside the margin is not a gain. It neither continues the sweep
    /// nor becomes the figure, so the ceiling is understated by at most the
    /// margin — the strict direction, which costs results rather than
    /// publishing infra-bound ones.
    #[test]
    fn a_gain_smaller_than_the_margin_neither_continues_the_sweep_nor_becomes_the_ceiling() {
        let sweep = run_sweep(INGEST_CONCURRENCY_MAX, |c| match c {
            2 => 1_000_000,
            _ => 1_020_000,
        });
        assert_eq!(
            sweep
                .points()
                .iter()
                .map(|p| p.concurrency)
                .collect::<Vec<_>>(),
            vec![2, 4, 8]
        );
        assert_eq!(sweep.best().map(|p| p.rows_per_s), Some(1_000_000));
        assert!(!sweep.still_climbing());
    }

    /// The ladder doubles, and it tries the operator's bound exactly rather than
    /// stopping at the last power of two below it — a bound that silently became
    /// a smaller number would be a flag that does not do what it says.
    #[test]
    fn the_sweep_ladder_doubles_from_its_start_and_tries_the_operators_bound_exactly() {
        let rungs = |max: u64| {
            run_sweep(max, |c| c * 100_000)
                .points()
                .iter()
                .map(|p| p.concurrency)
                .collect::<Vec<_>>()
        };
        assert_eq!(rungs(64), vec![2, 4, 8, 16, 32, 64]);
        assert_eq!(rungs(24), vec![2, 4, 8, 16, 24]);
        assert_eq!(rungs(2), vec![2]);
        assert_eq!(rungs(1), vec![1], "a bound below the start still runs once");
    }

    /// A rig that POSTs the same blocks in a loop into a table with a
    /// deduplication window does not have its rows absorbed; it has them
    /// deduplicated, and it is then charged for everything except the merging of
    /// rows that stay. That is why the ceiling table turns the window off, and
    /// this is the check that the fix is in force rather than believed: a
    /// shortfall refuses the pass instead of adding a caveat to it.
    #[test]
    fn a_rung_whose_rows_did_not_all_land_refuses_rather_than_reporting_a_lenient_figure() {
        let posted = Burst {
            rows: 28_000_000,
            bytes: 1_600_000_000,
            elapsed_s: 8.0,
            landed: Some(800_000),
            server: None,
            settle_s: 2.0,
        };
        let e = check_landed(posted, "ceiling_sensor_events")
            .expect_err("a shortfall is a refusal, not a footnote");
        assert!(e.starts_with("REFUSED"), "{e}");
        assert!(e.contains("28000000") && e.contains("800000"), "{e}");
        assert!(describe_landed(posted).contains("deduplicated"));

        // The expected case: every row stayed, and the reading records both
        // counts so a later reader can check the claim rather than take it.
        let kept = Burst {
            landed: Some(28_000_000),
            ..posted
        };
        let landed =
            check_landed(kept, "ceiling_sensor_events").expect("landing every row is fine");
        assert_eq!(landed.posted, 28_000_000);
        assert_eq!(landed.counted, Some(28_000_000));
        assert!(describe_landed(kept).is_empty());

        // A count that could not be read is a missing reading rather than a
        // failed measurement, and must not be read as a shortfall of every row.
        let unknown = Burst {
            landed: None,
            ..posted
        };
        let landed =
            check_landed(unknown, "ceiling_sensor_events").expect("unknown is not a shortfall");
        assert_eq!(landed.counted, None);
        assert!(describe_landed(unknown).is_empty());
    }

    /// The ceiling table has to be the arms' table in every respect that costs
    /// the server work, and has to differ in exactly one: it may not
    /// deduplicate the rig's repeated blocks away.
    #[test]
    fn the_ceiling_table_carries_the_committed_columns_and_no_deduplication_window() {
        let ddl = ceiling_table_ddl().expect("the committed DDL yields a ceiling table");
        assert!(
            ddl.contains(&format!("CREATE TABLE IF NOT EXISTS {}", ceiling_table())),
            "{ddl}"
        );
        assert!(
            !ddl.contains(&format!("EXISTS {}\n", corpus::TABLE)),
            "the ceiling table must not be the arms' own table: {ddl}"
        );
        assert!(
            ddl.contains(&format!("{DEDUPLICATION_WINDOW} = 0")),
            "{ddl}"
        );
        // Every column, with its type, exactly as the arms' table declares
        // it: the column types are most of the server-side work, so a
        // ceiling measured against anything else describes another insert.
        for (name, ty) in corpus::COLUMNS {
            assert!(ddl.contains(name), "{ddl} omits {name}");
            assert!(ddl.contains(ty), "{ddl} omits {ty}");
        }
        // The materialised column is server-side work per row and is part of
        // the schema whether or not the rig writes it.
        assert!(ddl.contains("MATERIALIZED now64(6)"), "{ddl}");
        assert!(ddl.contains("ENGINE = MergeTree"), "{ddl}");
        assert!(
            ddl.contains("ORDER BY (sensor, batch_ts, batch_id, event_seq)"),
            "{ddl}"
        );
    }

    /// The window is parsed rather than string-matched against the value the DDL
    /// happens to carry today. A replacement that quietly matched nothing would
    /// restore the defect and say nothing about it — and a deduplication window
    /// is exactly the kind of setting somebody tunes.
    #[test]
    fn a_deduplication_window_this_cannot_turn_off_is_refused_rather_than_left_on() {
        let with = |settings: &str| {
            without_deduplication(&format!(
                "CREATE TABLE t (a UInt64) ENGINE = MergeTree {settings}"
            ))
        };
        assert!(
            with("SETTINGS non_replicated_deduplication_window = 1000")
                .expect("the committed shape")
                .ends_with("SETTINGS non_replicated_deduplication_window = 0")
        );
        // A different value, spelled differently, and the rewrite still lands.
        assert!(
            with("SETTINGS non_replicated_deduplication_window=7, index_granularity = 8192")
                .expect("a tuned value")
                .contains("non_replicated_deduplication_window = 0, index_granularity = 8192")
        );
        // And a DDL that no longer names it at all is a refusal: the pass cannot
        // show that the table it is about to measure does not deduplicate.
        let e = with("SETTINGS index_granularity = 8192").expect_err("nothing to turn off");
        assert!(e.starts_with("REFUSED"), "{e}");
        assert!(with("SETTINGS non_replicated_deduplication_window").is_err());
        assert!(with("SETTINGS non_replicated_deduplication_window = ").is_err());
    }

    /// The ladder's top rung can end for two opposite reasons, and a reader who
    /// cannot tell them apart learns nothing from either. "Too many parts" is
    /// the target declining work it cannot merge fast enough — the ceiling
    /// working — while a socket the rig could not open says nothing about the
    /// target at all.
    #[test]
    fn a_response_that_never_got_past_its_headers_is_not_a_target_refusal() {
        // The regression this pins, found by running the rig rather than reading
        // it: the inserter truncated the whole HTTP response to 400 bytes before
        // reporting it, and ClickHouse's headers alone exceed that. So a genuine
        // `MEMORY_LIMIT_EXCEEDED` arrived here as a header block, matched
        // nothing, and was filed as a limit of the rig — the opposite finding.
        // The rig had never once classified a target refusal correctly.
        let headers_only = "HTTP/1.1 500 Internal Server Error\r\nDate: Sat, 26 Jul 2026 \
             01:00:00 GMT\r\nConnection: Close\r\nX-ClickHouse-Server-Display-Name: \
             spate-bench-clickhouse\r\nX-ClickHouse-Exception-Code: 241\r\n";
        assert!(
            !target_refused(headers_only),
            "a header block names no code and must not be read as the target refusing"
        );

        // What the inserter now reports: the status line, then the body.
        let with_body = "HTTP/1.1 500 Internal Server Error | Code: 241. DB::Exception: \
             Memory limit (total) exceeded: would use 10.81 GiB, maximum: 10.80 GiB. \
             (MEMORY_LIMIT_EXCEEDED)";
        assert!(
            target_refused(with_body),
            "the exception body carries the code, so it must reach the classifier"
        );
    }

    #[test]
    fn a_rung_the_target_refused_is_reported_as_the_target_and_not_as_the_rig() {
        assert!(target_refused(
            "RuntimeError: Code: 252. DB::Exception: Too many parts (3000). \
             Merges are processing significantly slower than inserts: TOO_MANY_PARTS"
        ));
        assert!(target_refused(
            "Code: 241. DB::Exception: MEMORY_LIMIT_EXCEEDED"
        ));
        assert!(!target_refused(
            "ConnectionResetError: [Errno 104] Connection reset by peer"
        ));
        assert!(!target_refused("TimeoutError: timed out"));
    }

    /// The figure has to carry the shape it was obtained at, exactly as the
    /// consume ceiling carries the message size — and for the same reason: the
    /// defect was invisible because nothing recorded what the number described.
    /// The winning rung survives a rewrite in the field, and the ladder that
    /// chose it survives in the provenance the same rewrite carries.
    #[test]
    fn a_rewritten_ingest_ceiling_keeps_the_concurrency_and_the_ladder_it_was_measured_at() {
        let mut measured = ingest_of("native", 4_400_000, "envelope");
        measured.sweep = vec![
            SweepPoint {
                concurrency: 8,
                rows_per_s: 3_400_000,
            },
            SweepPoint {
                concurrency: 16,
                rows_per_s: 4_400_000,
            },
            SweepPoint {
                concurrency: 32,
                rows_per_s: 4_300_000,
            },
        ];
        measured.target_cgroup = Some(Cgroup {
            cores: 9.06,
            cap_cores: 9.0,
            user_share: 0.85,
            nr_throttled: 89,
            throttled_us: 72_900_000,
        });
        let json = serde_json::to_string(&measured).expect("serialise");
        let back: IngestCeiling = serde_json::from_str(&json).expect("round trip");
        assert_eq!(
            back.threads, 16,
            "the concurrency the figure was obtained at must survive a rewrite; its \
             absence is the same defect the message size closed for the consume ceiling"
        );
        // The ladder reaches a reader as numbers rather than as a sentence, so
        // it can be read, plotted and compared without parsing prose.
        assert_eq!(
            back.sweep, measured.sweep,
            "the ladder must survive a rewrite"
        );
        assert_eq!(
            back.target_cgroup.expect("cgroup").nr_throttled,
            89,
            "a target at its cap is what makes the figure a ceiling, so the reading \
             that says so has to survive with it"
        );
    }

    // -----------------------------------------------------------------------
    // The Native encoder
    //
    // A reference decoder, and every test below is the same assertion through
    // it: a block must read back as exactly the rows it was built from. That is
    // a different proof from the live one in
    // `harness/tests/native_encoder_matches_clickhouse.rs` and neither
    // substitutes for the other — this one says the bytes are internally
    // consistent and runs on every `cargo test`, that one says ClickHouse agrees
    // with our reading of them and needs a daemon. An encoder checked only
    // against its own decoder can be self-consistently wrong; an encoder checked
    // only against a server has nothing to say about which column broke.
    // -----------------------------------------------------------------------

    /// Walks a Native block. Panics on anything it cannot read, which is the
    /// right behaviour in a test: a truncated block means the encoder wrote a
    /// length that disagrees with the bytes after it.
    struct Reader<'a> {
        bytes: &'a [u8],
        at: usize,
    }

    impl<'a> Reader<'a> {
        fn new(bytes: &'a [u8]) -> Self {
            Self { bytes, at: 0 }
        }

        fn take(&mut self, n: usize) -> &'a [u8] {
            let end = self.at + n;
            assert!(end <= self.bytes.len(), "block ends mid-value");
            let out = &self.bytes[self.at..end];
            self.at = end;
            out
        }

        fn varint(&mut self) -> u64 {
            let (mut v, mut shift) = (0u64, 0u32);
            loop {
                let byte = self.take(1)[0];
                v |= u64::from(byte & 0x7f) << shift;
                if byte & 0x80 == 0 {
                    return v;
                }
                shift += 7;
            }
        }

        fn string(&mut self) -> String {
            let len = usize::try_from(self.varint()).expect("length fits usize");
            String::from_utf8(self.take(len).to_vec()).expect("valid UTF-8")
        }

        fn u8(&mut self) -> u8 {
            self.take(1)[0]
        }

        fn u16(&mut self) -> u16 {
            u16::from_le_bytes(self.take(2).try_into().expect("two bytes"))
        }

        fn u32(&mut self) -> u32 {
            u32::from_le_bytes(self.take(4).try_into().expect("four bytes"))
        }

        fn u64(&mut self) -> u64 {
            u64::from_le_bytes(self.take(8).try_into().expect("eight bytes"))
        }

        fn i64(&mut self) -> i64 {
            i64::from_le_bytes(self.take(8).try_into().expect("eight bytes"))
        }

        fn f64(&mut self) -> f64 {
            f64::from_le_bytes(self.take(8).try_into().expect("eight bytes"))
        }

        /// One `LowCardinality` group, returned as the value each index names.
        ///
        /// The flag word and the key version are asserted here rather than
        /// returned, because they are the two constants a reader cannot infer
        /// from the payload and they are precisely what a wrong encoder gets
        /// wrong.
        fn low_cardinality_group(&mut self) -> Vec<String> {
            let flags = self.u64();
            assert_eq!(
                flags & !0xff,
                LOW_CARDINALITY_INDEX_FLAGS,
                "the index-type word must set HasAdditionalKeys|NeedUpdateDictionary and \
                 nothing else above the width"
            );
            let dictionary: Vec<String> = {
                let size = usize::try_from(self.u64()).expect("dictionary fits usize");
                (0..size).map(|_| self.string()).collect()
            };
            assert_eq!(
                dictionary.first().map(String::as_str),
                Some(""),
                "index 0 is reserved for the inner default"
            );
            let expected_width = IndexWidth::for_dictionary(dictionary.len());
            assert_eq!(
                flags & 0xff,
                expected_width.code(),
                "the declared index width must be the one the dictionary size needs"
            );
            let indexes = usize::try_from(self.u64()).expect("index count fits usize");
            (0..indexes)
                .map(|_| {
                    let at = match expected_width {
                        IndexWidth::U8 => u64::from(self.u8()),
                        IndexWidth::U16 => u64::from(self.u16()),
                        IndexWidth::U32 => u64::from(self.u32()),
                        IndexWidth::U64 => self.u64(),
                    };
                    dictionary[usize::try_from(at).expect("index fits usize")].clone()
                })
                .collect()
        }
    }

    /// Reads a Native block back into its declared columns and one cell list per
    /// row, in the declared column order.
    fn decode_native(body: &[u8]) -> (Vec<(String, String)>, Vec<Vec<Cell>>) {
        let mut r = Reader::new(body);
        let columns = usize::try_from(r.varint()).expect("column count fits usize");
        let rows = usize::try_from(r.varint()).expect("row count fits usize");
        let mut declared = Vec::with_capacity(columns);
        let mut cells: Vec<Vec<Cell>> = vec![Vec::with_capacity(columns); rows];

        for _ in 0..columns {
            let name = r.string();
            let ty = r.string();
            let mut column: Vec<Cell> = Vec::with_capacity(rows);
            match ty.as_str() {
                "UInt64" => column.extend((0..rows).map(|_| Cell::UInt64(r.u64()))),
                "UInt16" => column.extend((0..rows).map(|_| Cell::UInt16(r.u16()))),
                "Int64" => column.extend((0..rows).map(|_| Cell::Int64(r.i64()))),
                "Nullable(Float64)" => {
                    // The null map for the whole column, then a value for every
                    // row including the null ones. Reading it the RowBinary way
                    // would desynchronise on the first null.
                    let nulls: Vec<bool> = (0..rows).map(|_| r.u8() == 1).collect();
                    let values: Vec<f64> = (0..rows).map(|_| r.f64()).collect();
                    column.extend(
                        nulls.into_iter().zip(values).map(|(null, v)| {
                            Cell::NullableFloat64(if null { None } else { Some(v) })
                        }),
                    );
                }
                "LowCardinality(String)" => {
                    assert_eq!(r.u64(), LOW_CARDINALITY_KEY_VERSION, "{name} key version");
                    let values = r.low_cardinality_group();
                    assert_eq!(values.len(), rows, "{name} carries one index per row");
                    column.extend(values.into_iter().map(Cell::LowCardString));
                }
                "Array(LowCardinality(String))" => {
                    // The nested key version comes FIRST, before the offsets.
                    assert_eq!(r.u64(), LOW_CARDINALITY_KEY_VERSION, "{name} key version");
                    let offsets: Vec<u64> = (0..rows).map(|_| r.u64()).collect();
                    let total = offsets.last().copied().unwrap_or(0);
                    let flat = if total == 0 {
                        Vec::new()
                    } else {
                        r.low_cardinality_group()
                    };
                    assert_eq!(
                        flat.len() as u64,
                        total,
                        "{name}'s final offset must be its element count"
                    );
                    let mut start = 0usize;
                    for end in offsets {
                        let end = usize::try_from(end).expect("offset fits usize");
                        column.push(Cell::LowCardStringArray(flat[start..end].to_vec()));
                        start = end;
                    }
                }
                other if other.starts_with("DateTime64(") => {
                    let scale = other
                        .trim_start_matches("DateTime64(")
                        .trim_end_matches(')')
                        .parse()
                        .expect("a DateTime64 scale");
                    column.extend((0..rows).map(|_| Cell::DateTime64 {
                        ticks: r.i64(),
                        scale,
                    }));
                }
                other => panic!("the reference decoder does not know {other}"),
            }
            for (row, cell) in cells.iter_mut().zip(column) {
                row.push(cell);
            }
            declared.push((name, ty));
        }
        assert_eq!(r.at, body.len(), "the block has trailing bytes");
        (declared, cells)
    }

    /// The rows a block was built from, so a decoded block can be compared
    /// against them without a second derivation of what a row is.
    fn rows_of(lo: u64, batches: u64) -> Vec<Vec<Cell>> {
        let mut rows = Vec::new();
        for batch_id in lo..lo + batches {
            for seq in 0..corpus::EVENTS_PER_BATCH {
                if !corpus::keeps(batch_id, seq) {
                    continue;
                }
                rows.push(row_of(batch_id, seq));
            }
        }
        rows
    }

    fn debug_rows(rows: &[Vec<Cell>]) -> Vec<String> {
        rows.iter().map(|r| format!("{r:?}")).collect()
    }

    /// The load-bearing one. A Native block must read back as exactly the rows
    /// it was built from — every column, every row — or the ceiling
    /// describes an insert of something else.
    ///
    /// The range starts at 0 and runs long enough to carry `sensor` past 256
    /// distinct values, which is the boundary where the `LowCardinality` index
    /// width steps from one byte to two. A block that never crosses it would
    /// leave the single most dangerous branch in the encoder untested.
    #[test]
    fn a_native_block_reads_back_as_exactly_the_rows_it_was_built_from() {
        let block = encode_batches(Format::Native, 0, 300);
        let (declared, decoded) = decode_native(&block.body);
        assert_eq!(
            declared,
            corpus::COLUMNS
                .iter()
                .map(|(n, t)| ((*n).to_owned(), (*t).to_owned()))
                .collect::<Vec<_>>(),
            "column header"
        );
        let built = rows_of(0, 300);
        assert_eq!(block.rows as usize, built.len(), "row count");
        assert_eq!(debug_rows(&decoded), debug_rows(&built), "rows");
    }

    /// A block that does not span 256 sensors still has to be readable. The
    /// narrow-dictionary path is the common one at short block sizes,
    /// and a width selector that only worked above the boundary
    /// would pass the test above and corrupt every short block.
    #[test]
    fn a_native_block_narrow_enough_for_one_byte_indexes_reads_back_unchanged() {
        let block = encode_batches(Format::Native, 0, 4);
        let (_, decoded) = decode_native(&block.body);
        assert_eq!(debug_rows(&decoded), debug_rows(&rows_of(0, 4)));

        // The dictionary for `sensor` over four batches is four sensors plus the
        // reserved default, so the indexes really are one byte wide here.
        let sensors = decoded
            .iter()
            .filter_map(|row| match &row[2] {
                Cell::LowCardString(s) => Some(s.clone()),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(sensors.len(), 4);
        assert_eq!(
            IndexWidth::for_dictionary(sensors.len() + 1),
            IndexWidth::U8
        );
    }

    /// The width is chosen from the largest index, `entries - 1`. One step too
    /// narrow does not fail loudly — it truncates every index past the boundary
    /// and lands the wrong dictionary entry in those rows.
    #[test]
    fn the_low_cardinality_index_width_is_chosen_from_the_largest_index() {
        assert_eq!(IndexWidth::for_dictionary(0), IndexWidth::U8);
        assert_eq!(IndexWidth::for_dictionary(1), IndexWidth::U8);
        assert_eq!(IndexWidth::for_dictionary(256), IndexWidth::U8);
        assert_eq!(IndexWidth::for_dictionary(257), IndexWidth::U16);
        assert_eq!(IndexWidth::for_dictionary(65_536), IndexWidth::U16);
        assert_eq!(IndexWidth::for_dictionary(65_537), IndexWidth::U32);
        assert_eq!((0, 1, 2, 3), {
            let c = |w: IndexWidth| w.code();
            (
                c(IndexWidth::U8),
                c(IndexWidth::U16),
                c(IndexWidth::U32),
                c(IndexWidth::U64),
            )
        });
    }

    /// The exact bytes of one small block, hand-checked, because a reference
    /// decoder written beside the encoder can agree with it about a layout
    /// ClickHouse does not use. One batch makes `sensor` constant, so the whole
    /// `LowCardinality` group is short enough to write out in full.
    #[test]
    fn a_low_cardinality_column_writes_its_key_version_flags_dictionary_and_indexes() {
        let block = encode_batches(Format::Native, 0, 1);
        // The workload filters, so one batch yields the kept rows rather than
        // every event.
        let rows = corpus::expected_rows(1);
        assert_eq!(block.rows, rows);

        // Header, then past `batch_id` and `event_seq` to `sensor`.
        let mut want = Vec::new();
        put_varint(&mut want, corpus::COLUMNS.len() as u64);
        put_varint(&mut want, rows);
        assert_eq!(&block.body[..want.len()], &want[..], "block header");

        // A column costs a length-prefixed name, a length-prefixed type and its
        // values; both prefixes are one byte at these lengths.
        let mut at = want.len();
        at += 1 + "batch_id".len() + 1 + "UInt64".len() + 8 * rows as usize;
        at += 1 + "event_seq".len() + 1 + "UInt16".len() + 2 * rows as usize;

        let mut sensor = Vec::new();
        put_string(&mut sensor, "sensor");
        put_string(&mut sensor, "LowCardinality(String)");
        sensor.extend_from_slice(&LOW_CARDINALITY_KEY_VERSION.to_le_bytes());
        // Width code 0 (one byte) beside both flag bits, then a two-entry
        // dictionary: the reserved default and the batch's one sensor.
        sensor.extend_from_slice(&LOW_CARDINALITY_INDEX_FLAGS.to_le_bytes());
        sensor.extend_from_slice(&2u64.to_le_bytes());
        put_string(&mut sensor, "");
        put_string(&mut sensor, &corpus::sensor_of(0));
        sensor.extend_from_slice(&rows.to_le_bytes());
        sensor.extend(std::iter::repeat_n(1u8, rows as usize));
        assert_eq!(
            &block.body[at..at + sensor.len()],
            &sensor[..],
            "the sensor column"
        );
    }

    /// The nested key version comes before the offsets, not after them. It is
    /// the array serialisation's state prefix, and the state prefix runs before
    /// any data — so a writer that emits the offsets first has the server read
    /// the first eight bytes of an offset as a version.
    #[test]
    fn an_array_of_low_cardinality_writes_the_nested_key_version_before_its_offsets() {
        let block = encode_batches(Format::Native, 0, 1);
        let mut header = Vec::new();
        put_string(&mut header, "tags");
        put_string(&mut header, "Array(LowCardinality(String))");
        let at = block
            .body
            .windows(header.len())
            .position(|w| w == header)
            .expect("the tags column header");
        let after_header = at + header.len();
        assert_eq!(
            u64::from_le_bytes(
                block.body[after_header..after_header + 8]
                    .try_into()
                    .expect("eight bytes")
            ),
            LOW_CARDINALITY_KEY_VERSION,
            "the eight bytes after the type name are the nested key version"
        );

        // And the offsets that follow are cumulative ends, so the first row's is
        // its own tag count rather than zero.
        let first = u64::from_le_bytes(
            block.body[after_header + 8..after_header + 16]
                .try_into()
                .expect("eight bytes"),
        );
        assert_eq!(first, corpus::tags_of(0, 0).len() as u64);
    }

    /// A null and a present quality are the same width in Native and different
    /// widths in RowBinary, which is the one column whose two encodings are not
    /// a rearrangement of each other. A Native null map that skipped the value
    /// slot would misalign every row after the first null.
    #[test]
    fn a_native_nullable_column_writes_a_value_slot_for_a_null_row_and_rowbinary_does_not() {
        let native = encode_batches(Format::Native, 0, 1);
        let row_binary = encode_batches(Format::RowBinary, 0, 1);
        assert_eq!(native.rows, row_binary.rows);

        let (_, decoded) = decode_native(&native.body);
        let nulls = decoded
            .iter()
            .filter(|row| matches!(row[8], Cell::NullableFloat64(None)))
            .count();
        assert!(nulls > 0, "the corpus nulls one quality in five");
        assert_eq!(
            nulls,
            (0..corpus::EVENTS_PER_BATCH)
                .filter(|seq| { corpus::keeps(0, *seq) && corpus::quality_of(0, *seq).is_none() })
                .count(),
            "the decoded null pattern is the corpus's, over the rows the workload keeps"
        );
    }

    /// A Native block is columnar and a RowBinary block is not, so the two are
    /// different sizes for the same rows — and `LowCardinality` is only paid for
    /// once per distinct value in Native against once per row in RowBinary. If
    /// they ever came out the same size, one of the encoders would not be doing
    /// what its name says.
    #[test]
    fn a_native_block_is_smaller_than_the_rowbinary_block_carrying_the_same_rows() {
        let native = encode_batches(Format::Native, 0, 50);
        let row_binary = encode_batches(Format::RowBinary, 0, 50);
        assert_eq!(native.rows, row_binary.rows);
        assert!(
            native.body.len() < row_binary.body.len(),
            "native {} vs rowbinary {}",
            native.body.len(),
            row_binary.body.len()
        );
    }

    /// The statement is one definition, so the pass and the encoder's live test
    /// cannot POST different inserts.
    #[test]
    fn every_format_inserts_behind_the_target_table_and_its_full_column_list() {
        for format in FORMATS {
            let sql = insert_sql(format);
            assert!(sql.starts_with(&format!("INSERT INTO {} (", corpus::TABLE)));
            assert!(sql.ends_with(&format!("FORMAT {}", format.clickhouse_name())));
            for (name, _) in corpus::COLUMNS {
                assert!(sql.contains(name), "{sql} omits {name}");
            }
        }
    }

    /// A re-measured pass replaces the entry it re-measures and leaves every
    /// other one alone, so measuring one format cannot delete another.
    #[test]
    fn merging_a_pass_replaces_only_the_keys_it_measured() {
        let mut ceilings = Ceilings {
            consume: Some(consume_at(840, 305_554, "old")),
            clickhouse: vec![
                ingest_of("rowbinary", 1, "old"),
                ingest_of("json_each_row", 2, "old"),
            ],
        };
        ceilings.merge(Pass {
            consume: consume_at(4056, 60_000, "new"),
            ingest: vec![ingest_of("rowbinary", 3, "new")],
        });
        assert_eq!(
            ceilings.consume.as_ref().expect("consume").msgs_per_s,
            60_000
        );
        assert_eq!(ceilings.clickhouse.len(), 2);
        let rb = ceilings
            .clickhouse
            .iter()
            .find(|c| c.format == "rowbinary")
            .expect("rowbinary survived");
        assert_eq!(rb.rows_per_s, 3);
        let je = ceilings
            .clickhouse
            .iter()
            .find(|c| c.format == "json_each_row")
            .expect("an unmeasured format is not deleted");
        assert_eq!(je.rows_per_s, 2);
    }

    /// What a figure was measured against is what defends it, and a rewrite that
    /// silently dropped those readings would leave the values with nothing
    /// behind them. They are fields rather than prose so that a consumer can act
    /// on them rather than parse them.
    #[test]
    fn a_rewritten_ceilings_file_keeps_the_readings_that_travel_with_it() {
        let mut consume = consume_at(4056, 60_000, "envelope");
        consume.window = Some(ConsumeWindow {
            requested_s: 8.0,
            actual_s: 0.72,
            messages_read: 1_277_917,
            topic_depth: 1_500_000,
            calibrated_msgs_per_s: 1_498_121,
        });
        consume.broker_cgroup = Some(Cgroup {
            cores: 2.11,
            cap_cores: 4.0,
            user_share: 0.55,
            nr_throttled: 0,
            throttled_us: 0,
        });
        let ceilings = Ceilings {
            consume: Some(consume),
            clickhouse: Vec::new(),
        };
        let json = serde_json::to_string(&ceilings).expect("serialise");
        let back: Ceilings = serde_json::from_str(&json).expect("round trip");
        let c = back.consume.expect("consume");
        assert_eq!(
            c.message_bytes, 4056,
            "the message size must survive a rewrite; its absence is the defect"
        );
        assert_eq!(c.window.expect("window").messages_read, 1_277_917);
        assert_eq!(
            c.broker_cgroup.expect("cgroup").nr_throttled,
            0,
            "a broker under its cap makes the figure a FLOOR on the broker, which is \
             the strict direction and has to survive with the number"
        );
    }

    /// A key the harness ignores is how a number comes to sit in a file looking
    /// authoritative while reaching nothing. The previous shape's
    /// `consume_msgs_per_s` is the concrete example.
    #[test]
    fn a_ceilings_file_with_an_unrecognised_key_fails_to_parse() {
        let err = serde_json::from_str::<Ceilings>(r#"{"consume_msgs_per_s": 305554}"#)
            .expect_err("an unknown key must not be ignored");
        assert!(err.to_string().contains("consume_msgs_per_s"), "{err}");
    }

    /// Every committed ceiling, checked against the rule rather than against a
    /// value.
    ///
    /// Pinning today's figures here would make this something to edit whenever a
    /// maintainer legitimately re-measures, and a test people edit is a test
    /// people stop reading. What is pinned instead is the property that failed:
    /// a consume ceiling reaches the gate only if it records the message size,
    /// the envelope and the side of the bench network it was taken on, and the
    /// size matches this corpus. The assertion is an equivalence, so it holds
    /// whether a committed file currently satisfies the rule or is being
    /// correctly refused until a pass is re-run.
    ///
    /// Discovered rather than named. This used to open one path spelled out in
    /// full, which made retiring that environment fail a test about a rule the
    /// environment had nothing to do with — and the fix a maintainer reaches for
    /// under that pressure is to edit the literal, which is how the rule quietly
    /// stops covering the file that replaced it. Iterating the directory means a
    /// new ceiling is covered the moment it is committed, by nobody's decision.
    ///
    /// An empty directory passes, and that is correct rather than a hole: the
    /// archive genuinely has no ceilings between retiring one environment and
    /// measuring the next, and a test that failed then would be reporting the
    /// absence of a measurement as a defect in a rule.
    #[test]
    fn every_committed_ceiling_reaches_the_gate_only_if_it_says_what_it_measured() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../environments/ceilings");
        let Ok(entries) = std::fs::read_dir(&dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let ceilings =
                Ceilings::load(&path).unwrap_or_else(|e| panic!("{} parses: {e}", path.display()));
            let Some(consume) = ceilings.consume.as_ref() else {
                continue;
            };
            assert_ne!(
                consume.message_bytes,
                0,
                "{}: a ceiling that does not record the message size it was measured at is \
                 the defect this file's shape exists to prevent",
                path.display()
            );

            // Its own recorded envelope, so the only thing left to disagree is
            // the message size.
            let gate = ceilings.gate(corpus_message_bytes(), &consume.provenance.infra_digest);
            assert_eq!(
                gate.consume_msgs_per_s > 0,
                !consume.provenance.infra_digest.is_empty()
                    && location_named(&consume.client) == Some(Location::Inside)
                    && !size_differs_materially(consume.message_bytes, corpus_message_bytes()),
                "{}: a committed ceiling must reach the gate exactly when it says what it \
                 was measured against: {:?}",
                path.display(),
                gate.refusals()
            );
        }
    }

    #[test]
    fn a_query_value_is_escaped_down_to_the_unreserved_set() {
        assert_eq!(
            query_escaped("INSERT INTO t FORMAT RowBinary"),
            "INSERT%20INTO%20t%20FORMAT%20RowBinary"
        );
        assert_eq!(query_escaped("a-b_c.d~e"), "a-b_c.d~e");
    }
}
