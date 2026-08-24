//! The measurement protocol.
//!
//! Everything here exists to answer one objection: *you measured your own
//! framework with your own instrumentation*. Nothing a system reports about
//! itself reaches a published number. Throughput is a row count in ClickHouse,
//! CPU and memory are cgroup counters read by a sidecar, and the envelope is
//! read back out of the kernel rather than trusted from the request.
//!
//! The protocol is a sequence of refusals as much as a sequence of measurements.
//! An arm that exits, that produces too few samples, that outruns the
//! infrastructure's proven ceiling, or that loses or corrupts rows, produces a
//! record with a failed status and **no metrics** — never a plausible-looking
//! number. Getting this wrong is not a crash; it is a publishable-looking figure
//! that is quietly false, which is the only outcome this suite genuinely cannot
//! afford.
//!
//! There are two modes and they measure different things. Drain replays a
//! prefilled topic and reports throughput and efficiency; sustained offers load
//! at a fixed rate during the window and is the only mode in which latency
//! exists, because it is the only mode in which `send_ts` is a live schedule
//! rather than a prefill timestamp. `methodology/` makes drain the default and
//! says sustained "has to be asked for", and the split is forced by the host:
//! see [`Mode`] for why the two may never be drawn on one axis, and
//! [`Mode::Sustained`] for why saturation is the expected result here rather
//! than an error.
//!
//! That paragraph used to be a description of an intention. Every refusal below
//! returned `Err` and appended nothing, `Status::Failed` was constructed nowhere
//! outside the tests, and `run` printed the refusal and moved on — so an arm
//! that exited mid-drain, blew its deadline, ran under the wrong cgroup caps or
//! lost rows left no trace in `results/` at all. See `measure` for where the
//! line between a reported refusal and a recorded one now falls, and why.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::ceiling::{self, Ceiling};
use crate::corpus;
use crate::entrant::{Container, Role, Variant};
use crate::environment::{Environment, HEADROOM_LIMIT};
use crate::infra::{self, Endpoints};
use crate::jvm;
use crate::report::{Flag, Infra, Kind, Metric, Report, RunMeta, Status, Sut, Trigger};
use crate::results;
use crate::sampler::{self, ArmLock, SutCost, SutSpec};
use crate::select::Arm;
use crate::serverside;

/// How the arm is loaded.
///
/// # A sustained record and a drain record may not share an axis
///
/// This is a conclusion rather than a preference, and it rests on three legs.
///
/// **`rows_per_s` is not the same quantity in the two modes.** In drain it is
/// the arm's *capacity*: the corpus is already on the broker, nothing throttles
/// the arm, and the number is a property of the system. In sustained it is
/// bounded above by the offered rate, so an arm that keeps up reports the rate
/// *we chose* and two arms of wildly different capacity report the same figure.
/// Ranking those against each other ranks the experimenter's knob.
///
/// **The efficiency figures were taken under different conditions.** A sustained
/// window has the broker serving writes and reads at once and a multi-threaded
/// generator competing for the same cores. That is the whole of
/// `methodology/`'s argument for drain being the default — widening the Spate
/// arm's egress concurrency moved its drain throughput substantially and its
/// sustained throughput not at all, because the sustained result was host
/// contention rather than a property of the system — and `cores_used` and
/// `cpu_us_per_row` inherit that contention directly.
///
/// **Latency exists in one mode only,** so a latency axis is single-mode by
/// construction; see [`Load`] for how that is made structural rather than
/// conventional.
///
/// `variant.mode` is therefore recorded on every record, and it has to be a
/// component of whatever key decides what shares an axis: the site's `groupKey`
/// in `website/plugins/bench-data/index.js` carries a
/// `` `mode-${rec.variant?.mode ?? '?'}` `` component for exactly this reason.
/// `variantKey` already separates the two modes into different *rows* because
/// `mode` is in the variant map, but a row is not an axis, and it is the axis
/// that misleads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Replay a prefilled topic to exhaustion and time the whole drain.
    ///
    /// The headline throughput measurement, and the only one this host can make
    /// honestly. Under sustained load the generator, the broker, ClickHouse and
    /// the arm together oversubscribe the machine, and the contention shows up
    /// as the arm's number: widening egress concurrency from 2 to 32 appeared to
    /// change throughput not at all, which reads exactly like "egress does not
    /// matter" and was host contention. Measured in drain it gives 3.25M → 4.81M
    /// rows/s.
    ///
    /// A full drain also removes the two things a windowed measurement has to
    /// get right and silently fails at: sizing a window, and detecting steady
    /// state inside it. There is no window — the drain is the measurement.
    Drain,
    /// Offer load at a fixed rate *during* the measurement and see what the arm
    /// does with it.
    ///
    /// The only mode in which latency means anything. In drain, `send_ts` is
    /// [`corpus::send_ts_us_prefill`] — a timestamp derived from `batch_id` and
    /// written into the topic hours earlier — so `ingest_ts - send_ts` measures
    /// how long the backlog had been sitting there. Here the producer stamps the
    /// time at which each message was *due*, so the same subtraction is
    /// end-to-end pipeline latency, corrected for coordinated omission.
    ///
    /// It has to be asked for, and drain is the default, because on the
    /// reference host this mode is expected to saturate. `methodology/` does
    /// the arithmetic: the box has 18 vCPU, the arm takes 4, the broker 8,
    /// ClickHouse 5, and a generator wide enough to offer millions of rows a
    /// second takes several more, plus the driver. That sum exceeds 18 before
    /// anything has been measured. [`oversubscription`] prints it before every
    /// sustained run rather than leaving it as a document a reader has to go and
    /// find.
    ///
    /// Saturation is therefore the *expected* outcome and not an error. What the
    /// harness must not do is pretend an oversubscribed host produced a clean
    /// latency distribution, which is what [`Sustained::kept_up`] and
    /// [`Flag::Saturated`] exist to prevent. The one thing that is refused is a
    /// producer that could not keep to its own schedule: at that point the
    /// offered rate was set by the harness and the record would describe the
    /// harness. See [`OFFERED_RATE_MIN_SHARE`].
    Sustained {
        /// Messages per second the producer is scheduled to offer. Messages, not
        /// rows: the workload filters, so a message's row yield is a property of
        /// the transform and a message rate is the figure that does not depend
        /// on it.
        offered_msgs_per_s: u64,
        /// Seconds the measurement window is held open once the arm has proved
        /// it is consuming.
        window_s: u64,
    },
}

impl Mode {
    /// The `mode` variant value recorded on every measurement.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Mode::Drain => "drain",
            Mode::Sustained { .. } => "sustained",
        }
    }

    /// What `auto.offset.reset` an arm in this mode must be configured with.
    ///
    /// Not cosmetic, and the wrong value fails silently in the worst available
    /// way. Drain replays a prefilled corpus, so it needs `earliest` or a fresh
    /// consumer group starts at the tail of a full topic and consumes nothing
    /// until the drain deadline.
    ///
    /// Sustained needs `latest` for two separate reasons. Its topic accumulates
    /// across runs, so `earliest` would replay every previous sustained run
    /// before reaching the current producer — a drain wearing sustained mode's
    /// name, whose `send_ts` values are minutes or hours old and whose latency
    /// figures would be published under `latency_*` while describing backlog
    /// age, which is precisely the confusion this whole mode exists to remove.
    /// And it discards whatever the producer offered while the arm was still
    /// starting up, so the window opens on an arm at the tail rather than on one
    /// working off a start-up backlog it would clear at faster than the offered
    /// rate — a backlog burn-off inside the window reads as `kept_up_share`
    /// above 1.0 and would hide a genuinely saturating arm.
    #[must_use]
    pub fn offset_reset(self) -> &'static str {
        match self {
            Mode::Drain => "earliest",
            Mode::Sustained { .. } => "latest",
        }
    }
}

/// Everything `bench run` was asked to do.
#[derive(Debug, Clone)]
pub struct RunOptions {
    /// Repetitions per arm.
    pub reps: u32,
    /// How the arm is loaded.
    pub mode: Mode,
    /// Environment id.
    pub env_id: String,
    /// What caused the run.
    pub trigger: Trigger,
    /// Print the plan and stop.
    pub dry_run: bool,
    /// Recreate the infrastructure instead of reusing what is running.
    ///
    /// Off by default, and that default is load-bearing rather than a
    /// convenience: **the prefilled corpus lives inside the broker**, so
    /// recreating it destroys the thing every arm is about to replay. Doing so
    /// once cost a sweep ten minutes of Flink failing against a topic that no
    /// longer existed.
    ///
    /// Reuse is safe here precisely because it is not trusted: the caps are read
    /// back from the running containers' cgroups and asserted, and the broker and
    /// ClickHouse versions are recorded on every record. A container running
    /// under a different envelope fails the run rather than quietly producing a
    /// number.
    pub fresh_infra: bool,
    /// Abandon the sweep on the first refusal. Off by default: one bad arm must
    /// not cost a thirty-hour sweep.
    pub fail_fast: bool,
    /// Topic to consume.
    pub topic: String,
    /// Corpus size, in messages.
    pub batches: u64,
    /// Knob values that replace the selected variants' declared ones, for this
    /// invocation only.
    ///
    /// This is how a configuration search walks a product of knob values without
    /// editing a committed descriptor per cell. It is also, on its own, a reason
    /// a record can never be published: the record names a `variant_id`, that
    /// variant's declared knobs are in the descriptor for anyone to read, and an
    /// overridden run did not use them. `bench run` therefore refuses an
    /// override unless the run's [`Trigger`] already bars publication — the flag
    /// that makes a record misdescribe its own variant cannot be reached without
    /// the marking that keeps it out of the archive.
    ///
    /// An override naming a knob the variant does not declare is refused rather
    /// than applied. The typo `--knob paralellism=4` would otherwise resolve
    /// nothing, leave the real knob at its declared value, and write the
    /// misspelling into the record's variant map — a cell that reports a
    /// configuration it did not run.
    pub knobs: BTreeMap<String, toml::Value>,
}

impl RunOptions {
    /// The knob values one arm will actually run with.
    ///
    /// One function, called by both the substitution that configures the
    /// container and the variant map the record publishes, because those two are
    /// the same claim seen from either end. Two derivations of "what did this
    /// arm run at" is precisely how a record comes to report a value that was
    /// never applied.
    #[must_use]
    pub fn knobs_for(&self, variant: &Variant) -> BTreeMap<String, toml::Value> {
        let mut out = variant.knobs.clone();
        for (k, v) in &self.knobs {
            out.insert(k.clone(), v.clone());
        }
        out
    }
}

/// Refuses a sweep whose knobs no arm could run, before anything is started.
///
/// Every problem across every arm, in one message. A search walks dozens of
/// cells; discovering one bound per attempt, each attempt costing a container
/// start and a JVM, is how an operator learns to stop reading refusals.
///
/// Both rules here are the entrant's own. `--knob` may only name a knob the
/// variant declares, and the entrant's `[[constraints]]` decide which
/// combinations are runnable — see `entrant::Constraint` for why that lives in
/// the descriptor rather than in this file.
fn assert_knobs_are_runnable(arms: &[Arm<'_>], opts: &RunOptions) -> Result<(), String> {
    let mut problems = Vec::new();
    for arm in arms {
        problems.extend(unrunnable_knobs(
            &format!("{}:{}", arm.entrant.id(), arm.variant.id),
            arm.variant,
            &arm.entrant.spec.constraints,
            opts,
        ));
    }
    if problems.is_empty() {
        return Ok(());
    }
    Err(format!(
        "REFUSED: {} unrunnable knob combination(s):\n  - {}",
        problems.len(),
        problems.join("\n  - ")
    ))
}

/// Every reason one arm could not run at this invocation's knobs.
fn unrunnable_knobs(
    at: &str,
    variant: &Variant,
    constraints: &[crate::entrant::Constraint],
    opts: &RunOptions,
) -> Vec<String> {
    let mut problems = Vec::new();
    for name in opts.knobs.keys() {
        if !variant.knobs.contains_key(name) {
            problems.push(format!(
                "{at}: --knob {name} names a knob this variant does not declare. \
                 Declared: {}. An override that resolves nothing would leave the real \
                 knob at its declared value and write the misspelling into the record \
                 anyway, so the cell would report a configuration it never ran.",
                if variant.knobs.is_empty() {
                    "none".to_owned()
                } else {
                    variant
                        .knobs
                        .keys()
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            ));
        }
    }
    for why in crate::entrant::knob_violations(constraints, &opts.knobs_for(variant)) {
        problems.push(format!("{at}: {why}"));
    }
    problems
}

/// Seconds a drain of the reference corpus may take before it is abandoned.
///
/// 2.5x the slowest drain any arm has needed at 1,500,000 batches, so a working
/// one cannot reach it. [`drain_max_s`] scales it with the corpus, which is a
/// variant knob.
const DRAIN_MAX_REFERENCE_S: u64 = 600;

/// Batches [`DRAIN_MAX_REFERENCE_S`] was calibrated against.
const DRAIN_MAX_REFERENCE_BATCHES: u64 = 1_500_000;

/// Milliseconds between row-count polls while a drain runs.
///
/// The poll decides when the window closes, so its period is post-drain idle
/// inside the window. It is also a query against the system under test, so a
/// tighter poll competes with the arm — which is why it is not the sampler's
/// interval. Against [`MIN_WINDOW_S`] this contributes at most 0.2%.
const DRAIN_POLL_MS: u64 = 250;

/// Seconds a drain window must reach for the reading to be treated as precise.
///
/// The window is `corpus / throughput`, so it shrinks as arms get faster and has
/// no lower bound of its own. Below this, [`Flag::ShortWindow`] marks the record
/// and `window_resolution` says what it was read at.
pub const MIN_WINDOW_S: f64 = 120.0;

/// The drain deadline for a corpus of `batches`.
fn drain_max_s(batches: u64) -> u64 {
    DRAIN_MAX_REFERENCE_S
        .saturating_mul(batches.max(1))
        .saturating_div(DRAIN_MAX_REFERENCE_BATCHES)
        .max(DRAIN_MAX_REFERENCE_S)
}
/// Seconds to wait for the pipeline to settle before gating.
const QUIESCE_MAX_S: u64 = 900;

/// Consecutive row-count probes that may fail before an arm is abandoned.
///
/// A probe reads `SELECT count()` over HTTP, so it can fail for reasons that
/// have nothing to do with the arm — a refused connection, a timeout, a
/// truncated chunked body. One or two of those in a drain that runs for minutes
/// is noise; five in a row is ClickHouse being gone, and continuing past it
/// would gate the arm against a frontier nobody read.
const ROW_PROBE_MAX_FAILURES: u32 = 5;

/// Separates a refusal's reason from the arm's logs, in the one shape
/// [`note_for`] knows how to split.
const LOG_SEPARATOR: &str = "\nLogs:\n";

/// Characters of a refusal that reach a record's `note`.
const NOTE_MAX_CHARS: usize = 400;

/// Share of the corpus the correctness gate examines, counted down from the top
/// of the range.
///
/// A share rather than a count. The count this replaces was calibrated against
/// a 1,500,000-batch corpus, where it covered 6.7%; the corpus is a variant
/// knob, so a fixed count means the gate covers less of a longer one — and a
/// longer corpus is exactly what raises the number of rows an arm could lose
/// without the gate noticing.
const GATE_SHARE: f64 = 0.067;

/// Bytes of ClickHouse memory one batch of the gate's exact-distinct costs.
///
/// Exact-distinct needs a hash set proportional to cardinality, and the gate's
/// is `(batch_id, event_seq)` over the rows in its window. One observation
/// backs this: `uniqExact` over the full 150M-row, 1,500,000-batch corpus asked
/// for 10.45 GiB, which is ~7.5 KiB per batch.
const GATE_BYTES_PER_BATCH: u64 = 7_500;

/// Fraction of ClickHouse's memory the gate may plan to occupy.
///
/// The run that established [`GATE_BYTES_PER_BATCH`] was killed at 10.45 GiB
/// against a 10.8 GiB limit, taking a completed, valid measurement with it. The
/// gate is a check on a measurement already taken, so it is sized to lose that
/// race rather than to win it.
const GATE_MEMORY_SHARE: f64 = 0.25;

/// Batches the correctness gate examines for a corpus of `batches`, against a
/// ClickHouse allocation of `ch_memory_bytes`.
///
/// The slice is taken from the top of the range because that is the part
/// produced during and after the measurement window. The window is recorded in
/// the record's note, so the gate is visibly a sample rather than silently one.
fn gate_window_batches(batches: u64, ch_memory_bytes: u64) -> u64 {
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "batch counts and byte budgets are far below f64's exact range"
    )]
    let want = (batches as f64 * GATE_SHARE) as u64;
    #[expect(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "as above"
    )]
    let affordable = ((ch_memory_bytes as f64 * GATE_MEMORY_SHARE) as u64) / GATE_BYTES_PER_BATCH;
    want.min(affordable).max(1)
}

/// Seconds a sustained measurement window is held open, unless asked otherwise.
///
/// Short, and short for a reason worth stating because it looks like an
/// oversight. A longer window would buy nothing statistically: at the rates this
/// mode is used at, thirty seconds is already of the order of a hundred million
/// rows — comparable to the entire committed corpus, and orders of magnitude
/// more than a p999 needs. What a longer window does buy is broker disk, because
/// the producer writes roughly four kilobytes per message for the whole time it
/// runs; and on a saturating arm it also buys a proportionally longer post-window
/// drain before the correctness gate can see a settled frontier. The binding
/// constraints here are storage and drain time, not sample size.
///
/// The default is chosen so that the default three repetitions fit inside
/// [`SUSTAINED_TOPIC_BYTES_MAX`] at a rate high enough to saturate the fastest
/// published drain figure. A default that the default `--reps` cannot run is a
/// default that is always overridden.
///
/// Public because `bench run` is where it is applied: the flag parser must not
/// carry a second copy of this number, or the two drift and a record says it ran
/// for a window it did not.
pub const SUSTAINED_WINDOW_S: u64 = 30;

/// Seconds the arm may take to prove it is consuming before the window opens.
///
/// The window opens on a *warmed* arm, unlike drain, and the asymmetry is
/// deliberate rather than an inconsistency. `sampler::SutCost` argues that drain
/// should charge an arm for its own start-up because "the corpus is prefilled
/// and sitting on the broker from the moment the arm starts, so every second an
/// arm spends coming up is a second in which it drains nothing". In sustained
/// mode that premise is false: no load has been offered yet that the arm is
/// failing to consume, so charging start-up here would deflate `kept_up_share`
/// by the arm's start-up time over the window and report a Flink arm as
/// saturated for taking twenty seconds to schedule a job.
const SUSTAINED_WARMUP_MAX_S: u64 = 120;

/// Seconds of warm-up production the disk projection charges per run.
///
/// Deliberately **not** [`SUSTAINED_WARMUP_MAX_S`], and the distinction is the
/// difference between a guard that is obeyed and one that gets deleted. The
/// producer does run throughout the warm-up, so those bytes are real; but an arm
/// that takes the full two-minute deadline to start consuming is the
/// pathological case rather than the ordinary one — the Spate arm warms in
/// seconds and the Flink arm in tens of them — and charging every run the
/// deadline makes the projection four to twenty times the reality. A guard that
/// refuses every sweep the host can in fact run teaches its operator to remove
/// it, which leaves no guard at all.
///
/// The residual risk is stated rather than engineered away: a sweep of arms that
/// all warm slowly can overrun the projection. The driver prints the projection
/// before the sweep and the messages each run actually produced afterwards, so
/// the gap is visible rather than silent.
const SUSTAINED_WARMUP_BUDGET_S: u64 = 45;

/// Consecutive advancing row-count polls that count as "the arm is consuming".
///
/// Three rather than one, for the reason [`quiesce`] wants three: a single
/// advancing poll can be one sink chunk landing, and a window opened on that is
/// a window opened on an arm that has not started steadily.
const SUSTAINED_WARMUP_POLLS: u32 = 3;

/// Messages per second one producer thread is budgeted to offer.
///
/// Measured on the reference host at roughly 73k messages/s for a single
/// producer thread, and budgeted below it on purpose. A thread run at its own
/// ceiling becomes the pacer — it stops tracking the schedule and starts
/// tracking its own throughput — and the arm is then credited with keeping up
/// with a load nobody offered. Budgeting at 60k leaves each thread room to be
/// waiting on the clock rather than on itself.
const PRODUCER_THREAD_MSGS_PER_S: u64 = 60_000;

/// Fraction of the requested rate the producer must actually have offered.
///
/// Below this the *harness* set the offered rate, so the record would describe
/// our load generator rather than the system, and the run is refused. This is
/// the one thing sustained mode refuses on rather than flags: a saturated arm is
/// a genuine ceiling measurement, but a producer-bound one is a measurement of
/// the producer, and there is no reading of it that says anything about the arm.
///
/// Weakening the producer until it stops falling behind would satisfy this
/// constant and destroy the experiment — a generator too slow to saturate the
/// arm measures nothing at all. The correct response to a refusal here is a
/// lower `--rate`, more host, or the acceptance that this host cannot offer that
/// rate.
const OFFERED_RATE_MIN_SHARE: f64 = 0.99;

/// Consumed-over-offered below which a sustained arm is [`Flag::Saturated`].
///
/// Not 1.0. The row count and the producer's schedule are measured against the
/// same window but not by the same instrument, and an arm that is keeping up
/// perfectly still has rows in flight at both edges of it, so a share a fraction
/// below unity is window-edge accounting rather than the arm falling behind. Two
/// percent is comfortably above that and far below any real shortfall: an arm
/// that cannot keep up on this host does not miss by 2%, it misses by half.
const KEPT_UP_MIN: f64 = 0.98;

/// Bytes the sustained topic may be projected to hold before a run is refused.
///
/// A disk guard, not a claim about physics, and it is stated as a constant so
/// that the refusal can show its arithmetic. Sustained mode writes a fresh
/// corpus for the whole time it runs — at 40,000 messages/s that is about
/// 170 MB/s — and the topic is not truncated between runs.
///
/// It is not truncated between runs on purpose. The obvious alternative, a short
/// `retention.ms` on the sustained topic, has a failure mode this suite cannot
/// accept: a saturated arm falls behind by design, retention would then delete
/// log the consumer had not read, the consumer would reset to the tail, and the
/// correctness gate would report the skipped range as the framework losing rows.
/// That is a false accusation about somebody else's software, published. A run
/// refused for want of disk is loud, wrong about nothing, and fixed by deleting
/// a topic.
///
/// The figure itself is a judgement about the reference host's Docker volume,
/// which no environment profile declares, and it is the number in this file most
/// likely to want changing the first time this mode is run in anger. It is
/// stated as a constant with its arithmetic printed so that changing it is a
/// decision somebody makes rather than a limit they discover.
const SUSTAINED_TOPIC_BYTES_MAX: u64 = 48 << 30;

/// Prepares infrastructure, schema, target tables and corpus.
///
/// # Errors
///
/// If infrastructure cannot be brought up, or the corpus does not verify.
pub fn prefill(root: &Path, opts: &RunOptions) -> Result<(), String> {
    let env = Environment::load(&root.join("environments"), &opts.env_id)?;
    let (ep, _infra, _flags) = infra::bring_up(&env, !opts.fresh_infra)?;

    let schema_id = corpus::register_schema(&ep.registry_host, ep.registry_port);
    eprintln!("registered {} as schema id {schema_id}", corpus::SUBJECT);

    for stmt in corpus::ddl_statements() {
        crate::docker::clickhouse_sql(&ep.ch_host, ep.ch_port, &ep.ch_user, &ep.ch_password, &stmt)
            .map_err(|e| format!("DDL failed: {e}"))?;
    }
    eprintln!("target tables applied");

    let report = corpus::prefill(
        &ep.bootstrap,
        &opts.topic,
        env.spec.infra.partitions,
        opts.batches,
        schema_id,
    );
    eprintln!("prefill: {} messages on {}", report.batches, opts.topic);

    // Re-read the bytes actually sitting in Kafka and re-derive every field from
    // `batch_id`. The round-trip unit tests only prove the encoder and decoder
    // agree with each other; this proves the wire matches the contract, which is
    // what every competitor arm actually reads.
    let verified = corpus::verify_corpus(&ep.bootstrap, &opts.topic, schema_id, 64);
    eprintln!("verified {verified} messages against the contract");

    // The integer-only count. The full `expected` formats a string
    // fingerprint per row, which is right for a once-per-arm gate over a
    // bounded window and is ~15s of pure waste when all that is wanted is
    // how many rows a complete drain must produce.
    let rows = corpus::expected_rows(opts.batches);
    eprintln!("expected: {rows} rows");
    Ok(())
}

/// Runs the selected arms and appends one record per repetition.
///
/// One record per repetition **including a refused one**, as long as the arm got
/// far enough to be identified: that record carries `Status::Failed`, the reason
/// in its `note`, and no metrics. See [`measure`].
///
/// # Errors
///
/// If setup fails. Individual arm refusals are recorded and reported, and do not
/// stop the sweep unless `fail_fast` is set.
pub fn run(root: &Path, arms: &[Arm<'_>], opts: &RunOptions) -> Result<(), String> {
    let env = Environment::load(&root.join("environments"), &opts.env_id)?;

    // Before the plan is printed and long before anything is started, because a
    // sweep's operator has to be able to ask "is this cell runnable?" for the
    // price of a `--dry-run` rather than for the price of a container start, a
    // job submission and a JVM.
    assert_knobs_are_runnable(arms, opts)?;

    // What this sweep may hold an arm to, resolved once. Here rather than per
    // Loaded once per sweep so a ceilings file that does not parse refuses it
    // before a container starts, rather than after the first arm has run.
    //
    // Nothing downstream needs the committed file. `Ceilings::gate` resolves
    // which ceilings may be applied to this corpus, drops the rest with their
    // reasons, and `Headroom` then carries the figure each arm was actually
    // held to — so no call site re-derives that decision from the file and no
    // two derivations can disagree.
    let _ceilings = env.ceilings()?;
    let ceiling = env.ceiling()?;

    // The plan, printed before anything is spent. A full sweep costs hours, so
    // "which arms will this actually run?" has to be answerable in advance
    // rather than inferred afterwards from what appeared.
    eprintln!(
        "plan: {} arm(s) x {} rep(s) = {} run(s), interleaved, in {} mode, on {} [{}]",
        arms.len(),
        opts.reps,
        arms.len() * opts.reps as usize,
        opts.mode.name(),
        env.spec.id,
        format!("{:?}", env.spec.class).to_lowercase()
    );
    if let Mode::Sustained {
        offered_msgs_per_s,
        window_s,
    } = opts.mode
    {
        // Printed with the plan and not buried in the run, because it is the
        // reason drain is the default and the reason these records will very
        // probably say SATURATED. A reader who sees the arithmetic before the
        // sweep starts is not surprised by the result.
        let (demand, vcpus) = oversubscription(&env, arms, offered_msgs_per_s);
        eprintln!(
            "  sustained: {offered_msgs_per_s} msgs/s offered for {window_s}s per run, from \
             {} producer thread(s)",
            producer_threads(offered_msgs_per_s)
        );
        eprintln!(
            "  host: this asks for ~{demand:.0} vCPU of a {vcpus}-vCPU box (arm + broker + \
             ClickHouse + generator + driver){}",
            if demand > f64::from(vcpus) {
                " — OVERSUBSCRIBED, which is exactly why drain is the default"
            } else {
                ""
            }
        );
    }
    // A run whose trigger bars publication says so before it spends anything,
    // and says what stops it reaching readers rather than merely that it must
    // not. The same sentence is prefixed to every record's note, and
    // `validate::results_are_valid` is what makes the claim true.
    if let Some(bar) = opts.trigger.publication_bar() {
        eprintln!(
            "\nNOT PUBLISHABLE: {bar}.\nEvery record carries trigger={:?}, and `bench \
             validate` refuses such a record under results/ — committing one fails \
             the build instead of publishing a number.\n",
            format!("{:?}", opts.trigger).to_lowercase()
        );
    }
    for a in arms {
        // The knob values, on the plan line, because they are what a sweep
        // varies and a log of fifty cells that does not say which cell it ran is
        // a log of one number fifty times.
        let knobs = opts.knobs_for(a.variant);
        let cell: Vec<String> = knobs
            .iter()
            .map(|(k, v)| format!("{k}={}", knob_text(v)))
            .collect();
        eprintln!(
            "  {}:{}  {}{}",
            a.entrant.id(),
            a.variant.id,
            a.variant
                .reports
                .get("wire_format")
                .map_or("-", String::as_str),
            if cell.is_empty() {
                String::new()
            } else {
                format!("  [{}]", cell.join(" "))
            }
        );
    }
    if opts.dry_run {
        eprintln!("dry run: nothing was started");
        return Ok(());
    }

    // One arm at a time, across the whole host. Two arms sharing this machine
    // would each measure the other.
    let _lock = ArmLock::acquire("bench run").map_err(|e| format!("REFUSED: {e}"))?;

    let (ep, infra, base_flags) = infra::bring_up(&env, !opts.fresh_infra)?;

    // An environment whose class bars publication still runs — that is what a
    // fixture environment is for — but every record it produces has to say so.
    //
    // What used to happen here was worse than saying nothing: the run pushed
    // `Flag::ThirdPartyHardware`, which means "produced on hardware we do not
    // control". A fixture run is synthetic development data produced on the
    // reference machine, so the record made a false claim about its provenance
    // in place of the true claim about its worth, and then appended to
    // `results/` alongside real measurements. Every record it writes carries
    // `Flag::UnpublishableEnvironment`, which a consumer can filter on.
    if let Some(bar) = env.publication_bar() {
        eprintln!(
            "\nNOT PUBLISHABLE: {bar}.\nEvery record from this run carries the \
             `unpublishable_environment` flag and must never reach the site.\n"
        );
    }

    let schema_id = corpus::register_schema(&ep.registry_host, ep.registry_port);
    for stmt in corpus::ddl_statements() {
        crate::docker::clickhouse_sql(&ep.ch_host, ep.ch_port, &ep.ch_user, &ep.ch_password, &stmt)
            .map_err(|e| format!("DDL failed: {e}"))?;
    }
    // Verify the load source BEFORE starting anything. Without this, a missing
    // or short topic is discovered one arm at a time, each failing only at the
    // drain deadline — ten minutes to learn what a metadata call answers
    // instantly.
    match opts.mode {
        Mode::Drain => {
            let depth =
                corpus::topic_message_count(&ep.bootstrap, &opts.topic, env.spec.infra.partitions);
            if depth != opts.batches {
                return Err(format!(
                    "REFUSED: topic {:?} holds {depth} messages, expected {}. Run \
                     `bench prefill` first.\n\nIf you just recreated the infrastructure, \
                     that is why: the corpus lives inside the broker, so `--fresh-infra` \
                     discards it and the prefill has to be repeated.",
                    opts.topic, opts.batches
                ));
            }
            eprintln!("corpus: {depth} messages on {}", opts.topic);
        }
        Mode::Sustained {
            offered_msgs_per_s,
            window_s,
        } => {
            let topic = sustained_topic(&opts.topic);
            crate::kafka::ensure_topic(&ep.bootstrap, &topic, env.spec.infra.partitions);
            let depth =
                corpus::topic_message_count(&ep.bootstrap, &topic, env.spec.infra.partitions);
            let per_msg = message_bytes(schema_id);
            let runs = u64::try_from(arms.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(u64::from(opts.reps));
            let projected =
                projected_topic_bytes(depth, offered_msgs_per_s, window_s, runs, per_msg);
            eprintln!(
                "sustained topic {topic}: {depth} message(s) already present, \
                 ~{per_msg} bytes each; {runs} arm-run(s) project up to {:.1} GiB on it",
                bytes_gib(projected)
            );
            if projected > SUSTAINED_TOPIC_BYTES_MAX {
                return Err(format!(
                    "REFUSED: {runs} arm-run(s) at {offered_msgs_per_s} messages/s would put \
                     up to {:.1} GiB on topic {topic:?}, over the {:.1} GiB budget. The \
                     projection is (existing {depth} + runs x rate x (window {window_s}s + \
                     warm-up allowance {SUSTAINED_WARMUP_BUDGET_S}s)) x {per_msg} \
                     bytes/message.\n\nLower --rate, shorten --window, run fewer arms or \
                     --reps, or delete the topic. It is deliberately never trimmed under a \
                     running consumer: a saturated arm falls behind by design, and \
                     retention that deleted log it had not read would make the correctness \
                     gate report the skipped range as the framework losing rows.",
                    bytes_gib(projected),
                    bytes_gib(SUSTAINED_TOPIC_BYTES_MAX),
                ));
            }
        }
    }

    // Integer-only: see the note in `prefill`. This is the drain target,
    // not the gate, so the closed-form fingerprints are not wanted here.
    let corpus_rows = corpus::expected_rows(opts.batches);

    let mut refusals = Vec::new();
    let mut emitted = 0usize;

    // Interleaved, not batched. Running all of one arm and then all of another
    // has already manufactured a fake 30% difference in a related project: the
    // machine is not in the same state at the end of a long run as at the start,
    // and batching aliases that drift onto whichever arm went last.
    for rep in 1..=opts.reps {
        for arm in arms {
            let expected_rows = corpus_rows;
            eprintln!(
                "\n=== rep {rep}/{} — {}:{} ===",
                opts.reps,
                arm.entrant.id(),
                arm.variant.id
            );
            match measure(
                root,
                &env,
                &ep,
                &infra,
                &ceiling,
                arm,
                opts,
                rep,
                expected_rows,
                schema_id,
                &base_flags,
            ) {
                Ok(m) => {
                    emitted += 1;
                    // The refusal is printed and counted exactly as an
                    // unrecorded one is. A recorded refusal is not a quiet one:
                    // the record exists so a consumer can see the gap, not so
                    // the operator stops being told.
                    if let Some(why) = m.refusal {
                        eprintln!("REFUSED {}:{}: {why}", arm.entrant.id(), arm.variant.id);
                        refusals.push(format!("{}:{} — {why}", arm.entrant.id(), arm.variant.id));
                        eprintln!("recorded as {:?} in {}", m.status, m.path.display());
                        if opts.fail_fast {
                            return Err(format!("stopping after the first refusal: {why}"));
                        }
                    } else {
                        eprintln!("recorded in {}", m.path.display());
                    }
                }
                Err(why) => {
                    eprintln!("REFUSED {}:{}: {why}", arm.entrant.id(), arm.variant.id);
                    refusals.push(format!("{}:{} — {why}", arm.entrant.id(), arm.variant.id));
                    if opts.fail_fast {
                        return Err(format!("stopping after the first refusal: {why}"));
                    }
                }
            }
        }
    }

    eprintln!(
        "\n{emitted} record(s) written, {} refusal(s)",
        refusals.len()
    );
    for r in &refusals {
        eprintln!("  {r}");
    }
    Ok(())
}

/// What one repetition appended, and whether what it appended was a refusal.
struct Measured {
    /// The results file the record went into.
    path: std::path::PathBuf,
    /// The status the record carries.
    status: Status,
    /// The refusal that made this a [`Status::Failed`] record, if it is one.
    refusal: Option<String>,
}

/// What one measurement window produced, when it produced anything at all.
struct Measurement {
    /// The arm's cost over the one measurement window.
    ///
    /// Its `peak_anon_bytes` has been replaced with the arm's **simultaneous**
    /// peak; see [`arm_peak_anon`] for why `SutCost::sum`'s own figure is not
    /// the one a record publishes.
    cost: SutCost,
    /// The data-plane container's share of that cost, when one is declared.
    data_plane: Option<SutCost>,
    /// Rows landed inside the window.
    rows: f64,
    /// Rows per second, over the same window `cost` rests on. Carried rather
    /// than recomputed, so there is one division and not two.
    rows_per_s: f64,
    /// Duplicate rows the correctness gate counted in its window.
    duplicates: u64,
    /// Seconds the target needed to have no active parts and no running merges
    /// after this repetition's truncate, before the window opened.
    settle_s: f64,
    /// `Ok`, or `InfraBound` when the arm outran a proven ceiling.
    status: Status,
    /// What the record's `note` should say about how it was measured.
    note: String,
    /// Typed caveats the measurement itself established, as opposed to the ones
    /// the environment or the infrastructure did.
    flags: Vec<Flag>,
    /// The half of the measurement that only one mode can produce.
    load: Load,
    /// What the arm's inserts cost ClickHouse, when the server could be asked.
    ///
    /// `None` is "not measured" and never "cost nothing": every refusal
    /// `crate::serverside` can return lands here as an absence, because a
    /// record carrying `ch_cpu_us_per_row: 0` would be the most flattering
    /// possible wrong answer.
    server: Option<serverside::ServerSideCost>,
    /// Merge work ClickHouse completed over the same window, when the server
    /// could be asked.
    ///
    /// `None` is "not measured". A window in which nothing merged reads as a
    /// zero, which the server did answer, and is a different fact from a
    /// `part_log` that could not be read at all.
    merges: Option<serverside::MergeActivity>,
    /// What the arm's JVMs reported about their own collectors, if it has any.
    gc: Gc,
    /// The ClickHouse ingest ceiling this arm was held to, rows per second, or
    /// `0` when none applies to its insert format.
    ceiling_rows_per_s: u64,
}

/// What an arm's JVMs reported about their own collectors.
///
/// Two containers at most, and they are kept apart rather than summed: a pause
/// total is time during which the arm was stopped, and adding a JobManager's
/// pauses to a TaskManager's would name an interval in which neither was
/// entirely stopped. The data plane's figures are the headline and the control
/// plane's are published beside them, for the same reason
/// `data_plane_cores_used` exists — so that nobody can claim we taxed a
/// multi-process system for its control plane, nor that we hid what that plane
/// cost.
///
/// Both `None` for an arm with no collector, and both `None` for a JVM arm whose
/// log could not be read. Those are different facts and neither of them is a
/// pause total of zero; [`gc_metrics`] emits nothing in either case, so a
/// consumer sees an absence and has to render it as one.
#[derive(Debug, Default)]
struct Gc {
    /// The data-plane JVM's summary, where one could be read.
    data_plane: Option<jvm::GcSummary>,
    /// The control-plane JVM's summary, where one could be read.
    control_plane: Option<jvm::GcSummary>,
}

/// The part of a measurement that depends on how the arm was loaded.
///
/// **This enum is what makes a latency-free drain record structural rather than
/// conventional.** [`Mode::Drain`]'s variant carries no data at all, so there is
/// no value of any latency-shaped type anywhere on the drain path for a future
/// edit to reach for; [`mode_metrics`] can only emit a latency figure by
/// matching [`Load::Sustained`], and a `Sustained` can only be built by
/// [`hold_sustained_window`]. Adding `latency_p99_us` to a drain record is not a
/// thing someone might forget not to do — it does not typecheck.
///
/// The alternative, `Option<Latency>` on one struct, was rejected precisely
/// because it does typecheck: `None` on the drain path is a convention, and a
/// convention is what the measurement window used to be before `SutCost` was
/// given sole ownership of the interval. `methodology/` is unambiguous that in
/// drain mode `send_ts` is a prefill timestamp and the subtraction measures
/// backlog age, so a drain-mode latency figure would be a wrong number wearing
/// the name of a right one — the single failure this suite says it cannot
/// afford.
enum Load {
    /// Drained a prefilled topic. Throughput only, and deliberately nothing else.
    Drain,
    /// Consumed a live producer at a fixed offered rate.
    Sustained(Sustained),
}

/// What a sustained window measured that a drain window cannot.
struct Sustained {
    /// Offered rows per second: the producer's scheduled **message** rate at the
    /// workload's own row yield.
    ///
    /// Converted at the measured yield rather than at `EVENTS_PER_BATCH`,
    /// because the filters drop about a quarter of every message's events:
    /// comparing the consumed row rate against the raw event count would
    /// understate an arm's share by 1.36x and report a saturating arm as
    /// keeping up. `rows_per_message` is the one conversion, used by both this
    /// and the headroom gate.
    offered_rows_per_s: f64,
    /// Consumed rows per second over offered rows per second.
    kept_up_share: f64,
    /// The worst any producer thread ran behind its own schedule.
    ///
    /// Published because it is the evidence for the refusal that did *not*
    /// happen: a run whose `achieved_share` cleared [`OFFERED_RATE_MIN_SHARE`]
    /// but whose worst lag is large caught up in bursts rather than tracking the
    /// schedule, and that burstiness is in the latency figures.
    max_schedule_lag_ms: f64,
    /// The end-to-end latency distribution over the window.
    latency: Latency,
}

impl Sustained {
    /// Whether the arm tracked the offered rate.
    ///
    /// **One predicate, three consequences**, and they are deliberately not
    /// three independent decisions: this decides whether [`Flag::Saturated`] is
    /// set, whether the record's note leads with the warning, and — the one that
    /// cannot be missed by accident — which metric keys the latency figures are
    /// published under. Splitting them would let a record carry `latency_p99_us`
    /// without the flag, or the flag without the renaming, and a consumer would
    /// then have to know which of the two to believe.
    fn kept_up(&self) -> bool {
        self.kept_up_share >= KEPT_UP_MIN
    }

    /// The metric-name prefix the latency figures may be published under.
    ///
    /// `methodology/` allows a saturated record to keep its latency figures —
    /// it is a genuine ceiling measurement — but says they "describe backlog age
    /// and must not be read as latency at the offered rate". A flag and a note
    /// say so to a person. Neither says so to the site's aggregator, which
    /// medians metrics by name: three repetitions of one arm where one saturated
    /// would otherwise be medianed together under `latency_p99_us`, mixing a
    /// pipeline latency with a queue depth and captioning the result as
    /// run-to-run spread.
    ///
    /// Renaming makes that arithmetic impossible. A consumer plotting
    /// `latency_p99_us` finds no such metric on a saturated record rather than
    /// finding a number it has no reason to distrust, which is the same argument
    /// `Metric::bytes` makes about a value and its unit: the figure and the name
    /// that says what it is have to be produced together or they drift.
    fn latency_prefix(&self) -> &'static str {
        if self.kept_up() {
            "latency"
        } else {
            "backlog_age"
        }
    }
}

/// `ingest_ts - send_ts`, in microseconds, as ClickHouse computed it.
///
/// Percentiles and a maximum, never a mean, and that is a choice about what a
/// queueing system's latency distribution is. It is heavy-tailed and usually
/// multi-modal — a service time with a sink's flush interval superimposed on it
/// — so its mean names a value that few rows actually experienced and moves with
/// the tail rather than describing the typical case. Two arms with identical
/// means can have p999s an order of magnitude apart, and it is the p999 that a
/// reader deploying either of them will meet.
///
/// The set is p50, p99, p999 and max. p50 is what a typical row experienced. p99
/// and p999 are where a system under load actually lives. The maximum is the one
/// figure that a percentile cannot substitute for: over a window holding tens of
/// millions of rows, a single multi-second stall is invisible at p999 and is
/// exactly what a reader wants to know about, so the worst observation is
/// reported as itself.
struct Latency {
    /// Median.
    p50_us: f64,
    /// 99th percentile.
    p99_us: f64,
    /// 99.9th percentile.
    p999_us: f64,
    /// Worst observation. Exact, unlike the percentiles — see
    /// [`latency_in_window`].
    max_us: f64,
    /// Rows the distribution rests on.
    rows: u64,
}

/// The containers of one arm, removed when this value leaves scope — however it
/// leaves.
///
/// Defect this closes: a refusal after the arm had started — the cgroup cap
/// assertion is the one that bit — returned `Err` from [`measure`] with the
/// arm's containers still running, and `run` went straight on to the next arm,
/// which then measured itself against them on a host `methodology/` documents
/// as oversubscribed. Nothing about that is visible in the result: the next arm
/// simply produces a slower number, and a slower number is what a benchmark is
/// for.
///
/// A guard on the `Err` path alone would not close it. `docker::clickhouse_sql`
/// asserts on a server exception, so the truncate and the gate queries can leave
/// this module by panic, and a panic unwinds through `Drop` rather than through
/// a `match`.
struct ArmContainers {
    names: Vec<String>,
}

impl ArmContainers {
    /// Start every container of the arm, registering the names **before** the
    /// first `docker run`: `start_sut` asserts on a missing image, and a panic
    /// halfway through a multi-container arm must still take down the containers
    /// that did start.
    fn start(specs: &[SutSpec]) -> Self {
        let this = Self {
            names: specs.iter().map(|s| s.name.clone()).collect(),
        };
        sampler::start_arm(specs);
        this
    }

    /// Stop and remove every container, returning their logs for diagnosis.
    ///
    /// Idempotent: the names are taken, so a later drop is a no-op and a refusal
    /// that already collected logs does not remove twice.
    fn stop(&mut self) -> String {
        std::mem::take(&mut self.names)
            .iter()
            .map(|n| format!("--- {n} ---\n{}", sampler::stop_sut(n)))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for ArmContainers {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

/// One repetition of one arm.
///
/// # Which refusals are recorded, and which are only reported
///
/// The boundary is [`resolve_sut`]. Above it there is nothing for a record to be
/// attributed to: an image that cannot be inspected has no digest, an arm that
/// reports neither version nor commit has no identity, and
/// `validate::results_are_valid` requires both of those on every record —
/// because a tag can be re-pushed under the same name and a digest cannot lie.
/// A refusal there is reported and nothing is written.
///
/// Below it the arm is identified, so every refusal appends a
/// [`Status::Failed`] record carrying the reason in `note` and **no metrics**;
/// `Status::carries_metrics` is the authority on the second half of that, and
/// the validator enforces it in both directions.
///
/// Defect this closes: none of them did. `Status::Failed` was constructed
/// nowhere outside the tests, so an arm that exited mid-drain, blew its drain
/// deadline, ran under cgroup caps that did not match its envelope, produced too
/// few samples or failed a correctness gate left a line on somebody's terminal
/// and nothing in `results/`. That makes "we ran Flink and it lost rows"
/// indistinguishable from "we never ran Flink", and the first of those is a
/// finding — the argument is written out in `report.rs`, and the site already
/// renders these records as explicit gaps.
#[expect(
    clippy::too_many_arguments,
    reason = "splitting this would hide the order of operations, which is the part that has to be right"
)]
fn measure(
    root: &Path,
    env: &Environment,
    ep: &Endpoints,
    infra: &Infra,
    ceiling: &Ceiling,
    arm: &Arm<'_>,
    opts: &RunOptions,
    rep: u32,
    expected_rows: u64,
    schema_id: u32,
    base_flags: &[Flag],
) -> Result<Measured, String> {
    let image = arm
        .image
        .clone()
        .or_else(|| arm.entrant.spec.build.as_ref().map(|b| b.image.clone()))
        .ok_or("no image for this entrant")?;

    // Resolve what is about to run BEFORE running it. A digest that cannot be
    // read is a refusal, not an optional field: version strings can be
    // re-pushed under the same tag, and a record that cannot say what produced
    // it is not evidence.
    let sut = resolve_sut(arm, &image)?;

    // Everything from here on can name what failed, so it is recorded.
    let outcome = assert_pinned_version(
        arm.entrant.id(),
        arm.entrant
            .spec
            .version
            .as_ref()
            .map_or("", |v| v.pinned.as_str()),
        sut.version.as_deref(),
    )
    .and_then(|()| {
        run_arm(
            ep,
            env,
            ceiling,
            arm,
            opts,
            &image,
            expected_rows,
            schema_id,
        )
    });

    let (status, measured_note) = match &outcome {
        Ok(d) => (d.status, d.note.clone()),
        Err(why) => (Status::Failed, note_for(why)),
    };
    // Every unpublishability marker goes first, before anything a reader might
    // stop after. `environment.rs` documents that the driver puts it there; it
    // did not, and the typed flag below was carrying the claim alone.
    //
    // There are two sources of such a marker and they are independent: the
    // environment can bar publication because its data is synthetic, and the
    // trigger can bar it because the run is a point in a configuration search.
    // A run can be both. Neither is the enforcement — `validate::results_are_valid`
    // refuses the record on the typed fields — but a line of JSONL that says so
    // in its own prose is what a person reading one sees.
    let bars: Vec<&str> = [opts.trigger.publication_bar(), env.publication_bar()]
        .into_iter()
        .flatten()
        .collect();
    let note = if bars.is_empty() {
        measured_note
    } else {
        format!("{}; {measured_note}", bars.join("; "))
    };

    let mut flags = base_flags.to_vec();
    if outcome.as_ref().is_ok_and(|d| d.cost.was_throttled()) {
        flags.push(Flag::CpuCapThrottled);
    }
    if outcome
        .as_ref()
        .is_ok_and(|d| d.cost.window_s < MIN_WINDOW_S)
    {
        flags.push(Flag::ShortWindow);
    }
    // Whatever the measurement itself established — today only
    // `Flag::Saturated`, decided by `Sustained::kept_up` and by nothing else.
    if let Ok(d) = &outcome {
        flags.extend(d.flags.iter().copied());
    }
    // Typed rather than prose, because a consumer has to be able to filter on
    // it: a note explaining that a number is synthetic cannot stop the number
    // being drawn on a chart, and this is the one caveat that must.
    if env.publication_bar().is_some() {
        flags.push(Flag::UnpublishableEnvironment);
    }

    // This record's own copy of the infrastructure. Every other field of it is
    // shared by the whole sweep, but the ClickHouse ceiling is measured per
    // insert format, so `infra::bring_up` — which runs before any
    // arm is chosen — leaves it zero and it is filled in here, where the arm is
    // known. See `report::Infra::ceiling_rows_per_s`.
    let mut infra_for_record = infra.clone();
    if let Ok(d) = &outcome {
        infra_for_record.ceiling_rows_per_s = d.ceiling_rows_per_s;
    }

    let mut report = Report::new(
        "kafka_avro_clickhouse",
        Kind::Measurement,
        status,
        sut,
        RunMeta::new(&env.spec.id, &env.digest, opts.trigger, infra_for_record),
    )
    .rep(rep, opts.reps)
    .variant(
        "approach",
        format!("{:?}", arm.variant.approach).to_lowercase(),
    )
    // Recorded from the mode the run was actually asked for, not a literal. A
    // hard-coded "drain" here would let a sustained record claim to be a drain
    // one and be drawn on the same axis as one; see [`Mode`] for why the two may
    // never share one.
    .variant("mode", opts.mode.name())
    .variant("partitions", i64::from(env.spec.infra.partitions))
    .variant("batches", i64::try_from(opts.batches).unwrap_or(i64::MAX));

    // The data-plane envelope the arm ran under. In the variant map, so two
    // envelopes are never medianed together as run-to-run spread — the same
    // reason the sustained rate and window are there.
    //
    // The declared totals, and they are proof rather than a claim: validation
    // asserts the data-plane containers sum to them, and `assert_arm_caps` reads
    // each container's cap back out of its cgroup, so a record carrying these
    // ran under them or did not run.
    if let Some(e) = arm.entrant.spec.envelope.as_ref() {
        report = report
            .variant("envelope_cpus", e.cpus.clone())
            .variant("envelope_memory", e.memory.clone());
    }

    // The offered rate and the window length are configuration, not measurement:
    // two sustained runs of one arm at different rates are different
    // experiments, and the site's `variantKey` is built from this map precisely
    // so that they are never medianed together as run-to-run spread.
    if let Mode::Sustained {
        offered_msgs_per_s,
        window_s,
    } = opts.mode
    {
        report = report
            .variant(
                "offered_msgs_per_s",
                i64::try_from(offered_msgs_per_s).unwrap_or(i64::MAX),
            )
            .variant("window_s", i64::try_from(window_s).unwrap_or(i64::MAX));
    }

    for (k, v) in &arm.variant.reports {
        report = report.variant(k.clone(), v.clone());
    }
    // The EFFECTIVE knobs, not the descriptor's. A record produced with
    // `--knob` overrides has to report the values that were applied, or the one
    // published artefact of a configuration search would name the configuration
    // it did not run. The overriding is what makes such a record unpublishable;
    // recording it accurately is what makes the search readable afterwards.
    for (k, v) in &opts.knobs_for(arm.variant) {
        if let Some(n) = v.as_integer() {
            report = report.variant(k.clone(), n);
        } else if let Some(s) = v.as_str() {
            report = report.variant(k.clone(), s.to_owned());
        }
    }

    // The variant map is written for a refused arm too. A gap that cannot say
    // which arm and which configuration it is a gap in is not much better than
    // no gap at all.
    if let Ok(d) = &outcome
        && status.carries_metrics()
    {
        report = report
            .metric("rows_per_s", Metric::maximize(d.rows_per_s, "records/s"))
            .metric(
                "cpu_us_per_row",
                Metric::minimize(d.cost.cpu_us_per_row(d.rows), "us"),
            )
            .metric("cores_used", Metric::minimize(d.cost.cores_used, "cores"))
            .metric(
                "rows_per_s_per_core",
                Metric::maximize(
                    if d.cost.cores_used > 0.0 {
                        d.rows_per_s / d.cost.cores_used
                    } else {
                        0.0
                    },
                    "records/s",
                ),
            )
            .metric("peak_anon_bytes", Metric::bytes(d.cost.peak_anon_bytes))
            .metric(
                "peak_charged_bytes",
                Metric::bytes(d.cost.peak_charged_bytes),
            )
            .metric("throttled_us", Metric::minimize(d.cost.throttled_us, "us"))
            .metric(
                "duplicate_rows",
                // Reported, never suppressed: these are at-least-once systems and
                // some duplication is legitimate. Hiding it would misrepresent the
                // guarantee being compared.
                #[expect(clippy::cast_precision_loss, reason = "counts stay small")]
                Metric::minimize(d.duplicates as f64, "rows"),
            );
        // The contract promises a data-plane figure alongside the total, so that
        // nobody can claim we taxed a multi-process system for its control plane.
        if let Some(dp) = d.data_plane {
            report = report
                .metric(
                    "data_plane_cores_used",
                    Metric::minimize(dp.cores_used, "cores"),
                )
                .metric(
                    "data_plane_peak_anon_bytes",
                    Metric::bytes(dp.peak_anon_bytes),
                );
        }
        // What the arm's inserts cost the shared target. An addition to a
        // record rather than its reason for existing, so a ClickHouse that
        // could not be asked costs the run these metrics and nothing else —
        // and costs them as an absence, never as a zero.
        if let Some(server) = &d.server {
            for (key, metric) in server_metrics(server, d.rows) {
                report = report.metric(key, metric);
            }
        }
        // Merge work that ran against the same window. Separate from the cost
        // figures above because it is charged to nobody: `ch_cpu_us` excludes
        // it by construction, and it competes with the arm regardless.
        if let Some(merges) = &d.merges {
            for (key, metric) in merge_metrics(*merges) {
                report = report.metric(key, metric);
            }
        }
        // How long the target took to go quiet before this window opened.
        // Minimised: it is merge work the previous repetition left, and a rising
        // figure across a sweep is the drift `ch_rows_merged` exists to expose.
        report = report.metric("ch_settle_us", Metric::minimize(d.settle_s * 1e6, "us"));
        // One sampler tick as a fraction of the window this reading was taken
        // over. A reader cannot otherwise tell a figure read at 0.1% from one
        // read at 7%, and the two are not the same evidence.
        report = report.metric(
            "window_resolution",
            Metric::minimize(
                crate::sampler::INTERVAL_S / d.cost.window_s.max(f64::EPSILON),
                "ratio",
            ),
        );
        // GC pauses and heap, for the JVM arms and no others. Emitted only
        // where the quantity exists: a Rust binary has no collector, and a bar
        // of length zero would say it paused for 0 ms, which is a claim about a
        // measurement nobody made.
        for (key, metric) in gc_metrics(&d.gc) {
            report = report.metric(key, metric);
        }
        // Everything a drain cannot produce. `mode_metrics` returns nothing at
        // all for `Load::Drain`, and it is the only place a latency figure can
        // be attached to a record.
        for (key, metric) in mode_metrics(&d.load) {
            report = report.metric(key, metric);
        }
    }

    for f in flags {
        report = report.flag(f);
    }
    report = report.note(note);

    // `results/` for a publishable run, `tuning/` for one whose trigger bars
    // publication. The routing is not the enforcement — `validate` refuses such
    // a record wherever it appears under `results/` — it is what stops a sweep's
    // fifty measurements landing in the same file as the arm's published ones,
    // where clearing them would mean a hand-edit of the one file this module
    // deliberately cannot rewrite. See `results::root_for`.
    let path = results::append(&results::root_for(root, opts.trigger), &report)
        .map_err(|e| format!("append record: {e}"))?;
    Ok(Measured {
        path,
        status,
        refusal: outcome.err(),
    })
}

/// Reads one of an arm's committed SQL files, by the path its descriptor
/// declares relative to the entrant directory.
///
/// A read failure is an `Err` naming the file rather than a panic, because it
/// happens per repetition inside a sweep: the arm is identified by then, so the
/// refusal is recorded against it like any other.
fn read_arm_sql(arm: &Arm<'_>, rel: &str) -> Result<String, String> {
    let path = arm.entrant.dir.join(rel);
    std::fs::read_to_string(&path).map_err(|e| format!("read arm SQL {}: {e}", path.display()))
}

/// Executes one file's worth of entrant-authored SQL, split by
/// [`corpus::split_sql`], failing softly.
///
/// Built on [`crate::docker::try_clickhouse_sql`] and never on the asserting
/// variant, and the distinction is who owns the statement. A bad statement in
/// an arm's own `arm_sql`/`arm_teardown_sql` is a defect of that arm — it gets
/// a `Failed` record like any other refusal, because the `Err` propagates to
/// [`measure`] with the arm already identified. `docker::clickhouse_sql`
/// asserts on `DB::Exception`, which is the right behaviour for the SQL the
/// harness owns (the `TRUNCATE`, the workload DDL — those failing means the
/// bench itself is broken) and the wrong one here: a panic over one entrant's
/// typo takes a multi-hour sweep down with every other arm's remaining
/// repetitions. `try_clickhouse_sql`'s own doc states the principle; this is
/// its application to SQL the harness runs but did not write.
fn apply_arm_sql(ep: &Endpoints, sql: &str, what: &str) -> Result<(), String> {
    apply_arm_statements(ep, &corpus::split_sql(sql), what)
}

/// Executes already-split entrant-authored statements; see [`apply_arm_sql`].
///
/// Split out so [`ArmObjects`], which holds its teardown pre-split, runs its
/// statements through exactly the same execution and error path as the create
/// side does.
fn apply_arm_statements(ep: &Endpoints, stmts: &[String], what: &str) -> Result<(), String> {
    for stmt in stmts {
        let body = crate::docker::try_clickhouse_sql(
            &ep.ch_host,
            ep.ch_port,
            &ep.ch_user,
            &ep.ch_password,
            stmt,
        )
        .map_err(|e| format!("{what}: {stmt:?}: {e}"))?;
        // The body is checked here rather than asserted on: a server that
        // answers with an exception is reporting the entrant's statement bad,
        // which is the arm's refusal to record, not the harness's panic.
        if body.contains("DB::Exception") {
            return Err(format!("{what}: {stmt:?}: {body}"));
        }
    }
    Ok(())
}

/// The arm's own ClickHouse objects, guaranteed torn down when the repetition
/// ends — on **every** path.
///
/// The sweep loop is interleaved (`for rep { for arm }`), so an object that
/// outlives its repetition is live through every *other* arm's measured window
/// until this arm's next turn — a declaring arm's materialized view would tax
/// the shared target during its competitors' measurements, and would still be
/// installed after the sweep exits. Tearing down only at the *start* of a
/// repetition (the defensive pass in [`run_arm`]) cannot fix that: it cleans up
/// for this arm, not for the arms measured in between.
///
/// Constructed in [`run_arm`] **before** [`ArmContainers`], so that when the
/// function unwinds — `Err` or panic — drop order (reverse of declaration)
/// tears the containers down first and the SQL objects after: nothing is still
/// inserting through a view while the view is being dropped. The success path
/// consumes the guard via [`ArmObjects::finish`] after all measurement,
/// server-side attribution included, so the teardown's own cost can never land
/// inside anything that was measured. The `Drop` impl is the backstop for the
/// failure paths only.
struct ArmObjects {
    /// A clone rather than a borrow, so the `Drop` backstop owes nothing to
    /// `run_arm`'s locals when it fires during an unwind.
    ep: Endpoints,
    /// The teardown statements, read and split at construction. The failure
    /// paths must not depend on re-reading a file mid-unwind, and the split is
    /// then done once, by the same splitter, for every path that runs them.
    teardown: Vec<String>,
    /// Armed until [`ArmObjects::finish`] has run; `Drop` is a no-op after.
    armed: bool,
}

impl ArmObjects {
    /// Builds the guard when — and only when — the arm declares teardown SQL.
    ///
    /// # Errors
    ///
    /// If the declared file cannot be read; the arm is identified by then, so
    /// the refusal is recorded against it.
    fn new(ep: &Endpoints, arm: &Arm<'_>) -> Result<Option<Self>, String> {
        let Some(ch) = arm.entrant.spec.clickhouse.as_ref() else {
            return Ok(None);
        };
        if ch.arm_teardown_sql.trim().is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            ep: ep.clone(),
            teardown: corpus::split_sql(&read_arm_sql(arm, &ch.arm_teardown_sql)?),
            armed: true,
        }))
    }

    /// Runs the teardown on the success path and disarms the backstop.
    ///
    /// Explicit and consuming, rather than leaving everything to `Drop`,
    /// because a teardown failure on the success path is a *finding* — the
    /// arm's objects are still installed on the shared server — and a `Drop`
    /// cannot propagate it. Here it becomes the arm's `Err` like any other.
    ///
    /// # Errors
    ///
    /// If a teardown statement cannot be executed or the server refuses it.
    fn finish(mut self) -> Result<(), String> {
        // Disarmed before running, not after: these statements are about to be
        // attempted once, and a failure is being *reported*, so the backstop
        // re-attempting the same failing statements on drop would only bury
        // the propagated error under a second copy of itself.
        self.armed = false;
        apply_arm_statements(&self.ep, &self.teardown, "arm teardown DDL")
    }
}

impl Drop for ArmObjects {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Best-effort by necessity: a Drop cannot propagate, and this path is
        // already unwinding out of a failed or panicked repetition. Loud by
        // choice: a teardown that failed here has left the arm's objects live
        // on the shared server, where they will tax every subsequent arm's
        // measurement — an operator must see that even though no record can
        // carry it.
        for stmt in &self.teardown {
            let outcome = crate::docker::try_clickhouse_sql(
                &self.ep.ch_host,
                self.ep.ch_port,
                &self.ep.ch_user,
                &self.ep.ch_password,
                stmt,
            );
            match outcome {
                Ok(body) if body.contains("DB::Exception") => eprintln!(
                    "ARM TEARDOWN FAILED (backstop; cannot propagate from Drop): {stmt:?}: \
                     {body}\nThe arm's ClickHouse objects may still be live and will tax \
                     every subsequent arm until removed."
                ),
                Err(e) => eprintln!(
                    "ARM TEARDOWN FAILED (backstop; cannot propagate from Drop): {stmt:?}: \
                     {e}\nThe arm's ClickHouse objects may still be live and will tax \
                     every subsequent arm until removed."
                ),
                Ok(_) => {}
            }
        }
    }
}

/// Runs one arm under the requested load and measures it.
///
/// Every `Err` from here names something that happened to an identified arm, so
/// [`measure`] records it. The arm's containers are torn down on every path,
/// including a panic, by [`ArmContainers`].
///
/// The two modes differ only in how the window is opened, held and closed — that
/// is [`hold_drain_window`] and [`hold_sustained_window`]. Everything after it,
/// from the cgroup cap assertion to the correctness gate, is shared and has to
/// stay shared: an envelope check or a loss gate that ran in one mode and not
/// the other would make the modes differ in what they *prove* as well as in what
/// they measure.
#[expect(
    clippy::too_many_arguments,
    reason = "splitting this would hide the order of operations, which is the part that has to be right"
)]
fn run_arm(
    ep: &Endpoints,
    env: &Environment,
    ceiling: &Ceiling,
    arm: &Arm<'_>,
    opts: &RunOptions,
    image: &str,
    expected_rows: u64,
    schema_id: u32,
) -> Result<Measurement, String> {
    // The arm's own objects go first, and the lifecycle is: teardown
    // (defensive) → TRUNCATE → create at the start, teardown again when the
    // repetition ends — on every path, success, `Err` or panic. Teardown
    // before the truncate so a materialized view left by a previous *process*
    // (a killed driver, a crash before its backstop could run) is never live
    // while the table it targets is truncated; creation after it so the fresh
    // objects observe an empty target; and the end-of-repetition teardown —
    // [`ArmObjects`] — so the objects exist only inside their own arm's
    // repetition. That last leg is what makes the invariant sweep-wide: the
    // loop is interleaved (`for rep { for arm }`), so without it a declaring
    // arm's MV and landing table stayed live through every OTHER arm's
    // measured window and after the sweep — "an MV is never live across its
    // target's truncate" held only for this arm's own truncates, which is to
    // say it did not hold. `arm_teardown_sql` is written to be idempotent
    // (`DROP … IF EXISTS`), so the defensive pass, which usually has nothing
    // to drop, runs the same statements as every other.
    //
    // The guard is constructed before `build_specs`/`ArmContainers`, so on an
    // unwind drop order tears the containers down first and the SQL objects
    // after: nothing is still inserting through a view while it is dropped.
    let arm_objects = ArmObjects::new(ep, arm)?;
    if let Some(objs) = &arm_objects {
        apply_arm_statements(ep, &objs.teardown, "arm teardown DDL (defensive)")?;
    }

    // A clean table per repetition. Without this the gate would see the previous
    // repetition's rows and the row delta would be meaningless. Harness-owned
    // SQL, so it stays on the asserting variant: this statement failing means
    // the bench is broken, not the arm.
    crate::docker::clickhouse_sql(
        &ep.ch_host,
        ep.ch_port,
        &ep.ch_user,
        &ep.ch_password,
        &format!("TRUNCATE TABLE {}", corpus::TABLE),
    )
    .map_err(|e| format!("truncate failed: {e}"))?;

    // Wait for the server to finish acting on that truncate before the window
    // opens. `TRUNCATE` drops parts asynchronously and leaves a merge queue, and
    // `system.part_log` records a merge at COMPLETION — so merges started by one
    // repetition and finishing inside the next are charged to the next. The loop
    // is `for rep { for arm }`, so the debt lands on whichever arm runs first in
    // each repetition, every time.
    //
    // Here rather than at the top of the sweep: this truncate is the largest
    // single source of the churn, and no arm container exists yet, so the wait is
    // outside the window at both ends.
    let settle_s = crate::serverside::wait_until_settled(
        &ep.ch_host,
        ep.ch_port,
        &ep.ch_user,
        &ep.ch_password,
        corpus::TABLE,
    );
    eprintln!("  settled in {settle_s:.1}s");

    // The arm's objects, recreated from the committed file every repetition so
    // no state — not a stale view definition, not an implicitly kept setting —
    // survives from one repetition into the next. The workload's own DDL is
    // deliberately not touched here: it is applied once per run and hashed into
    // `dataset_version`, and arm objects must never move that.
    if let Some(ch) = arm.entrant.spec.clickhouse.as_ref()
        && !ch.arm_sql.trim().is_empty()
    {
        apply_arm_sql(ep, &read_arm_sql(arm, &ch.arm_sql)?, "arm DDL")?;
    }

    let specs = build_specs(arm, ep, opts, image)?;
    let names: Vec<String> = specs.iter().map(|s| s.name.clone()).collect();
    for s in &specs {
        eprintln!(
            "  container {} ({}, --cpus={}, --memory={}) {}",
            s.name,
            s.image,
            s.cpus,
            s.memory,
            s.args.join(" ")
        );
    }

    // `None` is "the count could not be read", which is not the count zero.
    //
    // Defect this closes: the closure was `.ok().and_then(parse).unwrap_or(0)`,
    // so a refused connection, a ten-second HTTP timeout or a truncated chunked
    // body all became the number 0. `quiesce` breaks after three EQUAL
    // consecutive polls, so three timed-out polls read `0, 0, 0` and satisfied
    // it; the gate then ran against a frontier nobody had read and recorded a
    // correct arm as having lost rows.
    let rows_now = || -> Option<u64> {
        crate::docker::clickhouse_sql(
            &ep.ch_host,
            ep.ch_port,
            &ep.ch_user,
            &ep.ch_password,
            &format!("SELECT count() FROM {}", corpus::TABLE),
        )
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
    };

    let data_plane_name = arm
        .entrant
        .data_plane()
        .map(|c| format!("spate-bench-sut-{}", c.name));

    let mut containers = ArmContainers::start(&specs);

    // Open, hold and close the one measurement window. Which function does it is
    // the whole of the difference between the two modes.
    let (costs, held) = match opts.mode {
        Mode::Drain => hold_drain_window(
            &names,
            data_plane_name.as_ref(),
            &mut containers,
            &rows_now,
            expected_rows,
            opts.batches,
        )?,
        Mode::Sustained {
            offered_msgs_per_s,
            window_s,
        } => hold_sustained_window(
            ep,
            env,
            &names,
            data_plane_name.as_ref(),
            &mut containers,
            &rows_now,
            opts,
            schema_id,
            offered_msgs_per_s,
            window_s,
        )?,
    };

    // Defect this closes: the sampler already reads `cpu.max` and `memory.max`
    // back out of the arm's cgroup — the literal proof the envelope was applied
    // — and the previous harness discarded it in `summarise()`. Asserting it
    // here is what makes the methodology's claim true for the arms as well as
    // for the infrastructure.
    for (name, s) in &costs {
        let declared = arm
            .entrant
            .spec
            .envelope
            .as_ref()
            .and_then(|e| e.containers.iter().find(|c| name.ends_with(&c.name)));
        if let Some(c) = declared
            && let Err(why) = assert_arm_caps(name, c, &s.meta)
        {
            let logs = containers.stop();
            return Err(with_logs(&why, &logs));
        }
    }

    let parts: Vec<(String, Option<SutCost>)> = costs
        .iter()
        .map(|(n, s)| (n.clone(), s.summarise()))
        .collect();
    for (name, c) in &parts {
        if let Some(c) = c {
            eprintln!(
                "  {name}: {:.2} cores, peak anon {:.1} MB{}",
                c.cores_used,
                c.peak_anon_bytes / 1e6,
                if c.was_throttled() { " THROTTLED" } else { "" }
            );
        }
    }

    // A summary for EVERY declared container. Defect this closes: the missing
    // ones were filtered out and the rest were summed, so "fewer than two
    // sampler samples" was an exact refusal for a single-container arm and a
    // vacuous one for a multi-container arm — a Flink arm whose TaskManager
    // sampler produced one sample was published with the JobManager's ~0.067
    // cores as the whole arm's cost, a ~25x efficiency win carrying
    // `status: ok`. A partial arm is not a cheap arm.
    let missing: Vec<&str> = parts
        .iter()
        .filter(|(_, c)| c.is_none())
        .map(|(n, _)| n.as_str())
        .collect();
    if !missing.is_empty() {
        let logs = containers.stop();
        return Err(with_logs(
            &format!(
                "the cgroup sampler produced no usable summary for {} — fewer than two \
                 readable samples, so there is no CPU delta and no measurement for that \
                 container, and an arm is only measurable whole",
                missing.join(", ")
            ),
            &logs,
        ));
    }

    let summaries: Vec<Option<SutCost>> = parts.iter().map(|(_, c)| *c).collect();
    let data_plane = parts
        .iter()
        .find(|(n, _)| Some(n) == data_plane_name.as_ref())
        .and_then(|(_, c)| *c);

    // The producer round-robins across partitions and consumers drain them
    // independently, so at any instant the consumed frontier is RAGGED: the most
    // advanced partition sets the maximum while slower ones leave holes below it.
    // Gating on that snapshot reports those holes as data loss, which is how the
    // problem was originally found. The metrics still come from the window above;
    // only the gate sees these extra rows, which is exactly right — the question
    // the gate asks is "did everything produced eventually arrive?".
    //
    // In sustained mode this is also where a saturated arm works off the backlog
    // its window built, which is why `Quiesced::settled` is carried rather than
    // discarded: a run that did not settle may hand the gate a ragged frontier,
    // and a gate failure on such a run has to say so instead of accusing the arm
    // of losing rows it had not been given time to land.
    let settled = match quiesce(&rows_now) {
        Ok(q) => q,
        Err(why) => {
            let logs = containers.stop();
            return Err(with_logs(&why, &logs));
        }
    };

    // The GC logs, read while the containers still exist. `sampler::stop_sut`
    // runs `docker rm -f`, and `docker cp` against a removed container has
    // nothing to read — so this is the last moment the figures are obtainable,
    // and it is after the samplers have stopped, so whatever the copy costs
    // falls outside the measurement window.
    let gc = read_gc(arm, &parts);

    let logs = containers.stop();

    let Some(mut cost) = SutCost::sum(&summaries) else {
        return Err(with_logs(
            "the arm's containers did not summarise into one cost",
            &logs,
        ));
    };
    // The arm's footprint is what it held at one instant, not the sum of the
    // moments its containers each peaked at. See [`arm_peak_anon`].
    cost.peak_anon_bytes = arm_peak_anon(&costs, cost.peak_anon_bytes);

    let mut flags = Vec::new();
    let mut note = format!(
        "{} sampler samples over {:.1}s",
        cost.samples, cost.window_s
    );
    if cost.unreadable > 0 {
        note.push_str(&format!(
            "; {} unreadable samples discarded",
            cost.unreadable
        ));
    }
    // What the collector was and what it was allowed, in the record rather than
    // only in the metrics: "the arm lost 4 seconds to GC" is not readable
    // without knowing which collector, over what heap, and across how many
    // pauses.
    if let Some(s) = &gc.data_plane {
        note.push_str(&format!("; {}", s.provenance()));
    }
    if let Some(s) = &gc.control_plane {
        note.push_str(&format!("; control plane {}", s.provenance()));
    }

    // Rows landed INSIDE the window, and the two modes read that off different
    // instruments for a reason.
    //
    // A drain's window ends when the corpus is exhausted, so the count at that
    // instant is the whole corpus and needs no bounding. A sustained window is a
    // slice out of the middle of a continuous stream, so the count has to be
    // bounded to it — and it is bounded by `ingest_ts`, the server-side
    // `now64(6)` the target table already stamps on every row, against the
    // sampler's own first and last timestamps. That keeps the numerator and the
    // denominator on one interval, which is `SutCost`'s whole argument: both the
    // sampler and ClickHouse read the same VM kernel clock, so no cross-clock
    // join is needed to make them agree.
    let (rows, load) = match held {
        Held::Drain { rows } => (rows, Load::Drain),
        Held::Sustained { report, threads } => {
            let Some((from_ms, to_ms)) = sampler_window_ms(&costs) else {
                return Err(with_logs(
                    "the sampler produced no readable timestamps, so there is no window to \
                     count rows or latency over",
                    &logs,
                ));
            };
            let rows = rows_in_window(ep, from_ms, to_ms).map_err(|e| with_logs(&e, &logs))?;
            let latency =
                latency_in_window(ep, from_ms, to_ms).map_err(|e| with_logs(&e, &logs))?;

            #[expect(
                clippy::cast_precision_loss,
                reason = "row and message counts stay far below f64's exact range"
            )]
            let offered_rows_per_s =
                report.target_rate as f64 * rows_per_message(expected_rows, opts.batches);
            #[expect(
                clippy::cast_precision_loss,
                reason = "row counts stay far below f64's exact range"
            )]
            let consumed_rows_per_s = cost.rows_per_s(rows as f64);
            let s = Sustained {
                offered_rows_per_s,
                kept_up_share: if offered_rows_per_s > 0.0 {
                    consumed_rows_per_s / offered_rows_per_s
                } else {
                    0.0
                },
                max_schedule_lag_ms: report.max_schedule_lag_ms,
                latency,
            };
            eprintln!(
                "  offered {offered_rows_per_s:.0} rows/s from {threads} thread(s); kept up \
                 {:.3}; latency p50 {:.1}ms p99 {:.1}ms p999 {:.1}ms max {:.1}ms over {} rows",
                s.kept_up_share,
                s.latency.p50_us / 1000.0,
                s.latency.p99_us / 1000.0,
                s.latency.p999_us / 1000.0,
                s.latency.max_us / 1000.0,
                s.latency.rows,
            );
            note.push_str(&format!(
                "; sustained {} msgs/s offered from {threads} producer thread(s), \
                 {} messages produced, achieved {:.3} of schedule, worst schedule lag \
                 {:.0}ms; kept up {:.3}",
                report.target_rate,
                report.sent,
                report.achieved_share,
                report.max_schedule_lag_ms,
                s.kept_up_share
            ));
            if !s.kept_up() {
                // The warning goes in the note as well as in the flag and the
                // metric names, and it goes in upper case, because this is the
                // one sentence a reader must not skim past: the figures are real
                // and they are not what their obvious reading says.
                note.push_str(&format!(
                    "; SATURATED — the arm consumed {:.0}% of the offered rate, so the \
                     latency figures are published as backlog_age_* and describe how far \
                     behind it fell, NOT latency at the offered rate",
                    s.kept_up_share * 100.0
                ));
                flags.push(Flag::Saturated);
                eprintln!(
                    "  SATURATED at {:.0}% of the offered rate — this is a ceiling \
                     measurement, and its latency figures are backlog age",
                    s.kept_up_share * 100.0
                );
            }
            (rows, Load::Sustained(s))
        }
    };

    #[expect(
        clippy::cast_precision_loss,
        reason = "row counts stay far below f64's exact range"
    )]
    let rows_f = rows as f64;
    let rows_per_s = cost.rows_per_s(rows_f);

    // The headroom rule, enforced rather than checked by hand, and enforced
    // against BOTH ceilings `methodology/` names rather than only the broker's.
    // Above the limit we are measuring the shared infrastructure and not the
    // system, so the record is marked `Status::InfraBound` — and it keeps its
    // full metric set, because `InfraBound::carries_metrics()` is true. Emitting
    // nothing would make "we ran this arm and it blew the headroom limit"
    // indistinguishable from "we never ran it", and the first of those is a
    // finding. A consumer filters on the typed status instead; the argument is
    // in `report.rs`.
    //
    // One call, taking the arm's achievement in the units the ceilings are
    // stated in, so that a third ceiling changes `ceiling::Achieved` and the
    // body of `Ceiling::headroom` and leaves this call site alone. The
    // rows-to-messages conversion stays here and only here, because only the
    // driver knows the row yield it actually measured — see `rows_per_message`
    // for the yield defect that reasoning exists to prevent.
    let wire_format = arm
        .variant
        .reports
        .get("wire_format")
        .map_or("", String::as_str);
    let mut status = Status::Ok;
    let per_message = rows_per_message(expected_rows, opts.batches);
    let headroom = ceiling.headroom(ceiling::Achieved {
        msgs_per_s: if per_message > 0.0 {
            rows_per_s / per_message
        } else {
            0.0
        },
        rows_per_s,
        wire_format,
        // Arm-owned DDL means every insert also runs the arm's objects — the
        // ingest ceilings were measured against the bare target, so the gate
        // refuses them for such arms rather than proving headroom against
        // work the ceiling never performed.
        server_side_transform: arm
            .entrant
            .spec
            .clickhouse
            .as_ref()
            .is_some_and(|ch| !ch.arm_sql.trim().is_empty()),
    });
    eprintln!("  headroom: {}", headroom.summary());
    note.push_str(&format!("; headroom {}", headroom.summary()));
    if headroom.infra_bound() {
        status = Status::InfraBound;
        // Said on the terminal for the operator watching, and in the note for
        // everyone after: a share above the limit only reads as a demotion to
        // someone who remembers the rule, and `status` only to someone who
        // knows the schema, while the note is what the site renders beside the
        // number and what one line of JSONL shows.
        eprintln!("  {}", infra_bound_notice());
        note.push_str(&format!("; {}", infra_bound_notice()));
    }
    // "Not gated" must never read as "cleared the gate". A ceiling the
    // methodology names that could not be checked at all — because none was
    // measured, or because the one that was does not describe this corpus, or
    // because no ClickHouse figure exists for this arm's insert format — leaves
    // the arm's headroom *unknown* rather than satisfied, and that difference
    // has to reach the record as its own typed caveat or a reader cannot tell
    // the two apart.
    if !headroom.is_proven() {
        flags.push(Flag::HeadroomUnproven);
    }
    // Asked of the gate, which is what made the decision. Deriving it a second
    // time from the committed file — or recognising the share by the prose
    // name it carries — is two derivations of one choice, and that is how
    // they come to disagree.
    let ceiling_rows_per_s = headroom.applied_ingest_rows_per_s().unwrap_or(0);

    // What those inserts cost ClickHouse, from ClickHouse's own accounting.
    //
    // Here, after `quiesce` and before the correctness gate, and both halves of
    // that are load-bearing. After the quiesce, so every insert the arm issued
    // has finished and is in `system.query_log` to be attributed. Before the
    // gate, whose `uniqExact` scans charge CPU-seconds apiece to the same log —
    // they are excluded by `query_kind = 'Insert'`, but running the read first
    // keeps the figure independent of how strict the gate happens to be.
    let (server, merges) = measure_server_side(ep, arm, &costs, &mut note);

    // Correctness gates. An arm that loses rows is faster for the wrong reason,
    // and one that computes different values did different work.
    let gate_batches = gate_window_batches(
        opts.batches,
        crate::entrant::parse_memory(&env.spec.infra.clickhouse.memory).unwrap_or(0),
    );
    let gates = corpus::run_gates(
        &ep.ch_host,
        ep.ch_port,
        &ep.ch_user,
        &ep.ch_password,
        gate_batches,
    )
    .map_err(|e| with_logs(&e, &logs))?;
    if let Some(why) = gates.failure() {
        // A gate failure on a pipeline that never settled is a different claim
        // from a gate failure on one that did, and only the second of them is
        // evidence about the arm. Without this sentence a saturated sustained run
        // whose backlog outlasted the quiesce budget would be recorded as the
        // framework losing rows — a false accusation about somebody else's
        // software, published.
        let caveat = if settled.settled {
            String::new()
        } else {
            format!(
                " (the pipeline had NOT settled after {QUIESCE_MAX_S}s, so the gate may have \
                 read a frontier that was still filling in; re-run with a shorter --window or \
                 a lower --rate before believing this)"
            )
        };
        return Err(with_logs(
            &format!("correctness gate failed: {why}{caveat}"),
            &logs,
        ));
    }
    note.push_str(&format!(
        "; gate window {gate_batches} batches, quiesced at {} rows{}",
        settled.rows,
        if settled.settled {
            ""
        } else {
            " (NOT settled — the quiesce budget ran out)"
        }
    ));

    eprintln!(
        "  {rows} rows in {:.1}s = {rows_per_s:.0} rows/s; {:.2} cores; {:.3} us/row",
        cost.window_s,
        cost.cores_used,
        cost.cpu_us_per_row(rows_f)
    );

    // The repetition is over: every measurement, the server-side attribution
    // and the gates included, has read what it needed. The arm's objects come
    // down NOW, explicitly, so a failure is this arm's `Err` — the `Drop`
    // backstop only covers the paths that cannot report one.
    if let Some(objs) = arm_objects {
        objs.finish()?;
    }

    Ok(Measurement {
        cost,
        data_plane,
        rows: rows_f,
        rows_per_s,
        duplicates: gates.duplicates,
        settle_s,
        status,
        note,
        flags,
        load,
        server,
        merges,
        gc,
        ceiling_rows_per_s,
    })
}

/// What holding the window produced, beyond the sampler series itself.
enum Held {
    /// The row count at the instant the corpus was exhausted.
    Drain { rows: u64 },
    /// What the producer actually offered, and how many threads offered it.
    Sustained {
        report: corpus::LoadReport,
        threads: u64,
    },
}

/// Holds the window open for a drain: from container start to the instant the
/// corpus is exhausted.
///
/// The window is the drain, and it is the SAMPLER's window — the `Instant` here
/// bounds the deadline and nothing else. Every rate in the record comes from
/// `SutCost`, which owns the one interval; see its docs for what the second
/// interval cost.
fn hold_drain_window(
    names: &[String],
    data_plane_name: Option<&String>,
    containers: &mut ArmContainers,
    rows_now: &dyn Fn() -> Option<u64>,
    expected_rows: u64,
    batches: u64,
) -> Result<(Vec<(String, crate::sampler::Samples)>, Held), String> {
    let deadline_s = drain_max_s(batches);
    let samplers = sampler::sample_arm(names, sampler::INTERVAL_S);
    let started = Instant::now();
    let mut rows = 0u64;
    let mut probe_failures = 0u32;
    loop {
        std::thread::sleep(Duration::from_millis(DRAIN_POLL_MS));

        if let Some(why) = dead_container(names, data_plane_name, "the drain") {
            let logs = containers.stop();
            return Err(with_logs(&why, &logs));
        }

        match rows_now() {
            Some(n) => {
                probe_failures = 0;
                rows = n;
            }
            None => {
                probe_failures += 1;
                if probe_failures >= ROW_PROBE_MAX_FAILURES {
                    let logs = containers.stop();
                    return Err(with_logs(
                        &format!(
                            "the row count could not be read {probe_failures} times running \
                             ({rows} rows at the last reading), so there is no way to tell a \
                             drained corpus from an unreachable ClickHouse"
                        ),
                        &logs,
                    ));
                }
            }
        }

        if rows >= expected_rows {
            break;
        }
        if started.elapsed() > Duration::from_secs(deadline_s) {
            let logs = containers.stop();
            return Err(with_logs(
                &format!(
                    "the drain did not finish within {deadline_s}s \
                     ({rows} of {expected_rows} rows)"
                ),
                &logs,
            ));
        }
    }

    Ok((stop_samplers(samplers, names), Held::Drain { rows }))
}

/// Starts a live producer, waits for the arm to warm up, then holds the window
/// open for a fixed span.
///
/// # Why the producer starts before the window and not with it
///
/// The producer runs first and the samplers start only once the arm is
/// demonstrably consuming, which is the opposite of drain's rule and is
/// justified by drain's own argument. `sampler::SutCost` charges a drain for the
/// arm's start-up "because the corpus is prefilled and sitting on the broker
/// from the moment the arm starts, so every second an arm spends coming up is a
/// second in which it drains nothing". Here that premise does not hold: there is
/// no backlog waiting, so start-up is not time the arm spent failing to keep up,
/// and charging it would deflate `kept_up_share` by the arm's start-up time over
/// the window — enough to report a Flink arm as saturated for taking twenty
/// seconds to schedule a job.
///
/// Opening the window late is also what makes the memory figure the right one:
/// the sampler resets `memory.peak` through its held fd when it starts, so the
/// peak covers the measurement rather than a JVM's start-up allocation.
///
/// The arm is on `latest`, so whatever the producer offered while the arm was
/// starting is skipped rather than backlogged. That matters more than it looks:
/// with `earliest` the window would open on an arm working off a start-up
/// backlog, it would clear that backlog at faster than the offered rate, and
/// `kept_up_share` would come out above 1.0 on an arm that is in fact saturating.
#[expect(
    clippy::too_many_arguments,
    reason = "splitting this would hide the order of operations, which is the part that has to be right"
)]
fn hold_sustained_window(
    ep: &Endpoints,
    env: &Environment,
    names: &[String],
    data_plane_name: Option<&String>,
    containers: &mut ArmContainers,
    rows_now: &dyn Fn() -> Option<u64>,
    opts: &RunOptions,
    schema_id: u32,
    offered_msgs_per_s: u64,
    window_s: u64,
) -> Result<(Vec<(String, crate::sampler::Samples)>, Held), String> {
    let topic = sustained_topic(&opts.topic);
    let partitions = env.spec.infra.partitions;
    let threads = producer_threads(offered_msgs_per_s);

    // `batch_id`s disjoint from the prefilled corpus AND from every earlier
    // sustained run on this topic. `(batch_id, event_seq)` is the row identity
    // the correctness gate is built on, so a producer that re-used a range would
    // make a legitimate replay indistinguishable from an arm emitting
    // duplicates — one of the few ways an arm can look fast for a dishonest
    // reason.
    let existing = corpus::topic_message_count(&ep.bootstrap, &topic, partitions);
    let first_batch_id = opts.batches.saturating_add(existing);

    eprintln!(
        "  producing {offered_msgs_per_s} msgs/s to {topic} from {threads} thread(s), \
         batch_id from {first_batch_id}"
    );
    let load = corpus::SustainedLoad::start(
        &ep.bootstrap,
        &topic,
        partitions,
        schema_id,
        offered_msgs_per_s,
        first_batch_id,
        threads,
    );

    // Warm up: the window opens on an arm that is demonstrably landing rows, not
    // on one that is still resolving its consumer group.
    let mut advancing = 0u32;
    let mut prev = rows_now();
    let deadline = Instant::now() + Duration::from_secs(SUSTAINED_WARMUP_MAX_S);
    loop {
        std::thread::sleep(Duration::from_secs(1));
        if let Some(why) = dead_container(names, data_plane_name, "the sustained warm-up") {
            let logs = containers.stop();
            return Err(with_logs(&why, &logs));
        }
        let now = rows_now();
        if let (Some(a), Some(b)) = (prev, now)
            && b > a
        {
            advancing += 1;
            if advancing >= SUSTAINED_WARMUP_POLLS {
                break;
            }
        } else {
            advancing = 0;
        }
        prev = now;
        if Instant::now() > deadline {
            let logs = containers.stop();
            return Err(with_logs(
                &format!(
                    "the arm did not start consuming within {SUSTAINED_WARMUP_MAX_S}s of the \
                     producer starting ({} rows at the last reading), so there is no steady \
                     state to open a window on",
                    prev.map_or_else(|| "unreadable".to_owned(), |n| n.to_string())
                ),
                &logs,
            ));
        }
    }

    // The window. Fixed length, because a sustained stream has no natural end —
    // this is the one place in the harness where a window has to be *sized*, and
    // `SUSTAINED_WINDOW_S` records why the size is what it is.
    let samplers = sampler::sample_arm(names, sampler::INTERVAL_S);
    let opened = Instant::now();
    let mut probe_failures = 0u32;
    while opened.elapsed() < Duration::from_secs(window_s) {
        std::thread::sleep(Duration::from_secs(1));
        if let Some(why) = dead_container(names, data_plane_name, "the sustained window") {
            let logs = containers.stop();
            return Err(with_logs(&why, &logs));
        }
        if rows_now().is_none() {
            probe_failures += 1;
            if probe_failures >= ROW_PROBE_MAX_FAILURES {
                let logs = containers.stop();
                return Err(with_logs(
                    &format!(
                        "the row count could not be read {probe_failures} times running during \
                         the sustained window, so the window cannot be shown to have measured \
                         a running pipeline"
                    ),
                    &logs,
                ));
            }
        } else {
            probe_failures = 0;
        }
    }

    // Close the window BEFORE stopping the producer: the offered rate has to
    // have been offered for the whole of the interval the samplers covered.
    let costs = stop_samplers(samplers, names);
    let report = load.stop();

    if report.failed > 0 {
        let logs = containers.stop();
        return Err(with_logs(
            &format!(
                "the load generator failed to deliver {} of {} messages. Every failure is a \
                 hole in the batch_id sequence, and the correctness gate cannot tell one from \
                 the framework losing a row — so this is the harness's defect and is refused \
                 rather than charged to the arm",
                report.failed, report.sent
            ),
            &logs,
        ));
    }
    if report.achieved_share < OFFERED_RATE_MIN_SHARE {
        let logs = containers.stop();
        return Err(with_logs(
            &format!(
                "the load generator offered only {:.1}% of the {offered_msgs_per_s} messages/s \
                 it was asked for, across {threads} thread(s), with a worst schedule lag of \
                 {:.0}ms. Below {:.0}% the offered rate was set by the harness rather than \
                 requested of the arm, so the record would describe our load generator.\n\n\
                 Do NOT respond by lowering the rate until the generator keeps up: a producer \
                 too weak to saturate the arm measures nothing. `methodology/` records that \
                 this host cannot fit the arm's 4 vCPU, the broker's, ClickHouse's, a wide \
                 enough generator and the driver into 18, and this refusal is that arithmetic \
                 arriving.",
                report.achieved_share * 100.0,
                report.max_schedule_lag_ms,
                OFFERED_RATE_MIN_SHARE * 100.0
            ),
            &logs,
        ));
    }

    Ok((costs, Held::Sustained { report, threads }))
}

/// Stops every sampler and pairs its series with the container it watched.
fn stop_samplers(
    samplers: Vec<sampler::Sampler>,
    names: &[String],
) -> Vec<(String, crate::sampler::Samples)> {
    samplers
        .into_iter()
        .zip(names)
        .map(|(s, n)| (n.clone(), s.stop()))
        .collect()
}

/// Names the arm's first dead container, if any, and says what it died during.
///
/// EVERY declared container, not merely one of them. Defect this closes: the
/// test was `!names.iter().any(sut_alive)`, so an arm counted as alive while any
/// single container of it was — and an OOM-killed Flink TaskManager beside a
/// healthy JobManager burned the whole drain deadline and was recorded as
/// slowness. That attributes a data-plane crash to the framework being slow,
/// which is the wrong finding about the wrong thing.
fn dead_container(
    names: &[String],
    data_plane_name: Option<&String>,
    during: &str,
) -> Option<String> {
    let dead = names.iter().find(|n| !sampler::sut_alive(n.as_str()))?;
    let role = if Some(dead) == data_plane_name {
        "data-plane"
    } else {
        "control-plane"
    };
    Some(format!(
        "the arm's {role} container {dead} exited during {during}"
    ))
}

/// Rows one message yields under the workload.
///
/// The filters drop events, so a message lands about
/// 73.5 of its 100. Derived from the corpus expectation the run already computed
/// rather than from `EVENTS_PER_BATCH`, so the two cannot drift.
#[expect(
    clippy::cast_precision_loss,
    reason = "row and message counts stay far below f64's exact range"
)]
fn rows_per_message(expected_rows: u64, batches: u64) -> f64 {
    if batches == 0 {
        return 0.0;
    }
    expected_rows as f64 / batches as f64
}

/// The arm's peak anonymous memory: what it held **at one instant**.
///
/// Not `SutCost::sum`'s figure, which adds each container's own maximum. That
/// answers "how much could they have used between them" — an upper bound only
/// reached if every container peaked simultaneously, which nothing makes them
/// do. A JobManager that spikes during job submission and a TaskManager that
/// spikes late in the drain are charged as though they had spiked together.
///
/// The error is in the wrong direction. It over-reports the arm total, so it
/// penalises exactly the multi-process arms the envelope rule already goes out
/// of its way not to penalise, on the one panel where a JVM looks worst.
///
/// Each container's own published figure stays its own maximum, which is right:
/// that number *is* one container's peak, and `data_plane_peak_anon_bytes` is
/// answering a different question from the arm total.
///
/// `summed` is the fallback and is unreachable from here: `simultaneous_peak_anon`
/// returns `None` only when no series has a readable sample, and an arm in that
/// state has already been refused for having no summary at all.
fn arm_peak_anon(costs: &[(String, sampler::Samples)], summed: f64) -> f64 {
    let series: Vec<&[sampler::Sample]> = costs.iter().map(|(_, s)| s.rows.as_slice()).collect();
    sampler::simultaneous_peak_anon(&series).unwrap_or(summed)
}

// ---------------------------------------------------------------------------
// What the arm cost the shared target
// ---------------------------------------------------------------------------

/// The database this harness writes into, as `system.query_log` spells it.
///
/// `corpus::ddl_statements` creates the target table unqualified, so it lands
/// in ClickHouse's default database; the log always writes the qualified name,
/// and a predicate built from `corpus::TABLE` alone would match nothing at all —
/// which looks exactly like an arm that issued no inserts.
const TARGET_DATABASE: &str = "default";

/// Reads what an arm's inserts cost ClickHouse over the sampler's own window.
///
/// **The window comes from the sampler's `t_ms` and not from `report::now_ms`.**
/// The sampler runs inside a container on the Docker VM, which is the kernel
/// ClickHouse's own container runs on, so its timestamps and
/// `event_time_microseconds` are the same clock. The driver's process runs on
/// macOS, outside that VM, and its wall clock agrees with the VM's only as well
/// as Docker Desktop happens to keep them in step — a window taken on it would
/// be off by that drift, silently, and would shave inserts off one end.
///
/// A failure here **does not fail the arm**. The server-side figure is an
/// addition to a record rather than its reason for existing, so a disabled
/// `system.query_log` or a client that took the asynchronous insert path costs
/// the run this one family of metrics and nothing else. The refusal is printed
/// and no metric is attached; it is never turned into a zero, which would read
/// as "this arm cost the server nothing".
fn measure_server_side(
    ep: &Endpoints,
    arm: &Arm<'_>,
    costs: &[(String, sampler::Samples)],
    note: &mut String,
) -> (
    Option<serverside::ServerSideCost>,
    Option<serverside::MergeActivity>,
) {
    let series: Vec<&sampler::Samples> = costs.iter().map(|(_, s)| s).collect();
    // The workload's target first, then whatever the entrant declared — the
    // MV-flatten arm's landing table rides here, so the parent insert that
    // carries the view's cost is attributed even if a ClickHouse version stops
    // naming the view's target in `tables`. Qualified in this one place, so the
    // descriptor's bare names and the log's spelling cannot drift apart.
    let mut tables = vec![serverside::qualify(TARGET_DATABASE, corpus::TABLE)];
    let ch = arm.entrant.spec.clickhouse.as_ref();
    if let Some(ch) = ch {
        for t in &ch.attribution_tables {
            tables.push(serverside::qualify(TARGET_DATABASE, t));
        }
    }
    let table_refs: Vec<&str> = tables.iter().map(String::as_str).collect();
    let forwarded = ch.is_some_and(|c| c.forwarded_inserts);
    let window = match serverside::Window::spanning(&series) {
        Ok(w) => w,
        Err(why) => {
            eprintln!("  no server-side figure: {why}");
            return (None, None);
        }
    };

    let cost = match serverside::measure(
        &ep.ch_host,
        ep.ch_port,
        &ep.ch_user,
        &ep.ch_password,
        &table_refs,
        window,
        forwarded,
    ) {
        Ok(cost) => {
            eprintln!("  {}", cost.provenance());
            // The caveats travel with the number. A reader told "0.67 us/row of
            // server-side cost" without being told it excludes the background
            // merges the insert made inevitable has been told something
            // slightly false, and small-batch arms push more work there.
            note.push_str(&format!("; {}", cost.provenance()));
            Some(cost)
        }
        Err(why) => {
            eprintln!("  no server-side figure: {why}");
            None
        }
    };

    let merges = match serverside::measure_merges(
        &ep.ch_host,
        ep.ch_port,
        &ep.ch_user,
        &ep.ch_password,
        &table_refs,
        window,
    ) {
        Ok(m) => {
            eprintln!(
                "  merges: {} row(s) over {} ms, completed in the window",
                m.rows_merged, m.duration_ms
            );
            Some(m)
        }
        Err(why) => {
            eprintln!("  no merge figure: {why}");
            None
        }
    };

    (cost, merges)
}

/// Merge work against an arm's measurement window, as record metrics.
///
/// Minimised, both of them: the corpus is fixed, so within one window more
/// merge work is more unaccounted contention against the same output. Neither
/// is charged to the arm or to `ch_cpu_us`.
fn merge_metrics(merges: serverside::MergeActivity) -> Vec<(String, Metric)> {
    #[expect(
        clippy::cast_precision_loss,
        reason = "row counts and millisecond spans stay far below f64's exact range"
    )]
    let rows = merges.rows_merged as f64;
    #[expect(
        clippy::cast_precision_loss,
        reason = "row counts and millisecond spans stay far below f64's exact range"
    )]
    let duration_us = (merges.duration_ms as f64) * 1000.0;
    vec![
        ("ch_rows_merged".to_owned(), Metric::minimize(rows, "rows")),
        (
            "ch_merge_duration_us".to_owned(),
            Metric::minimize(duration_us, "us"),
        ),
    ]
}

/// What an arm's inserts cost ClickHouse, as record metrics.
///
/// Two per-row figures rather than one, because they have different
/// denominators and answer different questions. `ch_cpu_us_per_row` divides by
/// the rows the driver counted in the target table, so it is the one that may be
/// added to the arm's own `cpu_us_per_row`; `ch_cpu_us_per_written_row` divides
/// by what the server says it wrote, so it is the one that describes the insert
/// format. They coincide for an arm that inserts each row exactly once,
/// and diverge for one that duplicated.
///
/// Every counter the server did not report is omitted rather than defaulted.
/// `ProfileEvents` omits any counter whose value is zero, so `crate::serverside`
/// keeps absence representable precisely to stop a counter a container was
/// denied arriving here as a measured zero.
fn server_metrics(cost: &serverside::ServerSideCost, rows: f64) -> Vec<(String, Metric)> {
    let mut out = Vec::new();
    if let Ok(per_landed) = cost.cpu_us_per_landed_row(rows) {
        out.push((
            "ch_cpu_us_per_row".to_owned(),
            Metric::minimize(per_landed, "us"),
        ));
    }
    if let Ok(per_written) = cost.cpu_us_per_written_row() {
        out.push((
            "ch_cpu_us_per_written_row".to_owned(),
            Metric::minimize(per_written, "us"),
        ));
    }
    out.push(("ch_cpu_us".to_owned(), Metric::minimize(cost.cpu_us, "us")));
    #[expect(
        clippy::cast_precision_loss,
        reason = "row counts stay far below f64's exact range"
    )]
    let written_rows = cost.written_rows as f64;
    out.push((
        "ch_written_rows".to_owned(),
        // Minimised, because the comparison is against the rows that landed:
        // writing more of them for the same result is duplication or a filter
        // applied server-side, and both are work the arm asked for.
        Metric::minimize(written_rows, "rows"),
    ));
    out.push((
        // The arm's effective batch size as the SERVER saw it, rather than as
        // its configuration claims. Larger is better: it is the same insert
        // overhead spread over more rows, and it is what explains a per-row cost
        // without asking the arm anything.
        "ch_rows_per_insert".to_owned(),
        Metric::maximize(cost.rows_per_insert(), "rows"),
    ));
    if let Some(wait_us) = cost.os_cpu_wait_us {
        out.push((
            // Runnable but off-CPU: the server's counterpart to the sampler's
            // `throttled_us`, and the evidence for "why was it X and not 2X?"
            // under a capped ClickHouse.
            "ch_cpu_wait_us".to_owned(),
            Metric::minimize(wait_us, "us"),
        ));
    }
    if let Some(bytes) = cost.inserted_bytes {
        #[expect(
            clippy::cast_precision_loss,
            reason = "byte counts stay far below f64's exact range"
        )]
        let bytes = bytes as f64;
        out.push(("ch_inserted_bytes".to_owned(), Metric::bytes(bytes)));
    }
    out
}

// ---------------------------------------------------------------------------
// What the arm's runtime cost it
// ---------------------------------------------------------------------------

/// The `[entrant].runtime` value that has a collector to measure.
const JVM_RUNTIME: &str = "jvm";

/// Reads every JVM container's GC log, immediately before the containers go.
///
/// Nothing at all for a non-JVM arm, which is not an oversight: a Rust binary
/// has no collector, so it has no pause distribution and no configured heap, and
/// the absence of a GC number is not a GC number of zero.
///
/// A log that cannot be read costs the record its `gc_*` metrics and nothing
/// else. The refusal is printed and the record simply carries none — the same
/// shape the Spate arm's record has, for the same reason: no measurement was
/// made. The alternative, an empty summary, would publish
/// `gc_pause_total_us: 0` for an arm whose instrumentation broke, flattering
/// exactly the run that went wrong.
fn read_gc(arm: &Arm<'_>, parts: &[(String, Option<SutCost>)]) -> Gc {
    let mut gc = Gc::default();
    if arm.entrant.spec.entrant.runtime != JVM_RUNTIME {
        return gc;
    }
    let Some(envelope) = arm.entrant.spec.envelope.as_ref() else {
        return gc;
    };
    for c in &envelope.containers {
        let Some((name, Some(cost))) = parts.iter().find(|(n, _)| n.ends_with(&c.name)) else {
            continue;
        };
        // The path is the descriptor's, because only the entrant's own
        // configuration knows where its JVM writes — Flink's `env.java.opts.*`
        // and Connect's `KAFKA_OPTS` put it in different places, and a harness
        // that guessed would either read nothing or read another JVM's file. A
        // JVM container that declares no `gc_log` gets no `gc_*` metrics: an
        // absence, stated on the terminal, never a zero.
        let Some(gc_log) = c.gc_log.as_deref() else {
            eprintln!("  no GC figures for {name}: its [[envelope.container]] declares no gc_log");
            continue;
        };
        // Bounded by that container's OWN window, so the GC figures cover the
        // interval every other number on its record is divided by. A GC log
        // covers the JVM's whole life and the copy is taken after the pipeline
        // has quiesced, so it runs past the window at both ends; `[0, window]`
        // charges the arm for its own start-up exactly as the sampler's window
        // does. The mapping is approximate and `GcSummary::from_uptime_s` says
        // what was actually covered.
        match jvm::measure(name, gc_log, Some((0.0, cost.window_s))) {
            Ok(summary) => match c.role {
                Role::DataPlane => gc.data_plane = Some(summary),
                Role::ControlPlane => gc.control_plane = Some(summary),
            },
            Err(why) => eprintln!("  no GC figures for {name}: {why}"),
        }
    }
    gc
}

/// The JVM arms' pause distribution and heap, as record metrics.
///
/// **Empty when there is nothing to publish**, and that covers two different
/// facts: an arm with no collector, and a JVM arm whose log could not be read.
/// Neither of them is a pause total of zero. A consumer that finds no `gc_*`
/// metric on a record has to render "not applicable"; one that found a zero
/// would draw a bar saying the arm paused for 0 ms, which is a claim about a
/// measurement nobody made.
///
/// The data plane's figures are the headline and the control plane's are
/// published beside them under a `control_plane_` prefix, mirroring the
/// `data_plane_*` treatment of CPU and footprint — the same argument in the
/// other direction. There, the arm total leads and the data plane's share is the
/// qualifier; here the pauses that stopped the ingestion lead, and the
/// coordinator's are the qualifier, because a JobManager's pause did not stop
/// the pipeline.
fn gc_metrics(gc: &Gc) -> Vec<(String, Metric)> {
    let mut out = Vec::new();
    if let Some(s) = &gc.data_plane {
        out.extend(gc_metrics_for("", s));
    }
    if let Some(s) = &gc.control_plane {
        out.extend(gc_metrics_for("control_plane_", s));
    }
    out
}

/// One JVM's figures, under `prefix`.
///
/// The pause set is total, max, p99 and p999 and deliberately not the mean: a
/// collector that pauses ten thousand times for 1 ms and once for 900 ms has a
/// mean of 1.09 ms, which describes nothing anybody deploying it will meet. The
/// total is the part of the window in which the arm did no work; the maximum is
/// the figure a percentile cannot substitute for.
///
/// Each heap figure is emitted only where it exists. The gap between configured
/// and committed is the whole quantity `methodology/` asks for, and a missing
/// side makes it undefined rather than large — ZGC's pause lines carry no
/// occupancy at all, so an arm on it has a configured heap and no committed one.
fn gc_metrics_for(prefix: &str, s: &jvm::GcSummary) -> Vec<(String, Metric)> {
    let mut out = vec![
        (
            format!("{prefix}gc_pause_total_us"),
            Metric::minimize(s.total_us, "us"),
        ),
        (
            format!("{prefix}gc_pause_max_us"),
            Metric::minimize(s.max_us, "us"),
        ),
        (
            format!("{prefix}gc_pause_p99_us"),
            Metric::minimize(s.p99_us, "us"),
        ),
        (
            format!("{prefix}gc_pause_p999_us"),
            Metric::minimize(s.p999_us, "us"),
        ),
    ];
    if let Some(v) = s.configured.max_bytes {
        out.push((format!("{prefix}jvm_heap_configured_bytes"), heap_bytes(v)));
    }
    if let Some(v) = s.peak_committed_bytes {
        out.push((
            format!("{prefix}jvm_heap_committed_peak_bytes"),
            heap_bytes(v),
        ));
    }
    if let Some(v) = s.peak_live_bytes {
        out.push((format!("{prefix}jvm_heap_live_peak_bytes"), heap_bytes(v)));
    }
    out
}

/// A heap size as a metric: bytes, unscaled, for the reason `Metric::bytes`
/// exists.
#[expect(
    clippy::cast_precision_loss,
    reason = "heap sizes stay far below f64's exact range"
)]
fn heap_bytes(bytes: u64) -> Metric {
    Metric::bytes(bytes as f64)
}

// ---------------------------------------------------------------------------
// Sustained mode
// ---------------------------------------------------------------------------

/// The topic a sustained run produces to and consumes from.
///
/// **Not the prefilled topic**, and the separation is worth the extra name.
/// Producing into the corpus would grow it past the exact depth `run` asserts
/// before every drain, so the next drain sweep would refuse until somebody
/// re-produced six gigabytes of corpus — a ten-minute penalty for having run a
/// one-minute experiment. It would also mean a sustained run could never be told
/// from the corpus by anyone reading the topic.
fn sustained_topic(base: &str) -> String {
    format!("{base}-sustained")
}

/// Framed bytes of one message, measured rather than assumed.
///
/// Measured because the number is load-bearing for a refusal: the disk
/// projection in `run` is the only thing standing between a sustained sweep and
/// a full broker volume, and a guessed constant would drift the moment
/// `events_per_batch` or the tag alphabet changed. Four batches rather than one
/// because tag length cycles with `(batch_id + seq) % 4`; a hundred events per
/// batch already covers every residue, so four batches is belt and braces at a
/// cost of four encodes.
fn message_bytes(schema_id: u32) -> u64 {
    let total: usize = (0..4u64)
        .map(|b| corpus::frame_confluent(schema_id, &corpus::encode_batch(b, 0)).len())
        .sum();
    u64::try_from(total / 4).unwrap_or(0)
}

/// Bytes the sustained topic is projected to hold once this sweep has finished.
///
/// Every run produces for its warm-up as well as its window, so both are
/// charged; the messages already on the topic are charged once. See
/// [`SUSTAINED_WARMUP_BUDGET_S`] for why the warm-up term is an allowance rather
/// than the deadline.
fn projected_topic_bytes(
    existing_msgs: u64,
    rate: u64,
    window_s: u64,
    runs: u64,
    message_bytes: u64,
) -> u64 {
    let per_run = rate.saturating_mul(window_s.saturating_add(SUSTAINED_WARMUP_BUDGET_S));
    existing_msgs
        .saturating_add(per_run.saturating_mul(runs))
        .saturating_mul(message_bytes)
}

/// Gibibytes, for a message a human reads about a disk.
#[expect(
    clippy::cast_precision_loss,
    reason = "byte counts stay far below f64's exact range"
)]
fn bytes_gib(bytes: u64) -> f64 {
    bytes as f64 / f64::from(1u32 << 30)
}

/// Producer threads for a target offered rate.
///
/// Deliberately has no upper bound. Capping it would be the one thing
/// `methodology/` forbids in this mode — a generator too weak to saturate the
/// arm measures nothing — so if a rate needs more threads than the host has
/// cores, the answer is that the host cannot offer that rate, and
/// [`oversubscription`] says so in the plan rather than the harness quietly
/// under-producing and the arm looking better than it is.
fn producer_threads(offered_msgs_per_s: u64) -> u64 {
    offered_msgs_per_s
        .div_ceil(PRODUCER_THREAD_MSGS_PER_S)
        .max(1)
}

/// vCPU this sustained sweep asks the host for, and how many it has.
///
/// This is `methodology/`'s host caveat as arithmetic instead of prose: "the
/// system's 4, plus the broker's, plus ClickHouse's, plus a load generator wide
/// enough to offer millions of rows/s, plus the driver, exceeds 18". It is
/// computed from the environment profile and the arms actually selected rather
/// than from the quoted figures, so it stays true when either changes.
///
/// The widest arm is used rather than the sum of them, because arms run one at a
/// time under `ArmLock`. The driver counts as one: it polls a row count, runs
/// the gate queries and hosts the generator threads' bookkeeping.
fn oversubscription(env: &Environment, arms: &[Arm<'_>], offered_msgs_per_s: u64) -> (f64, u32) {
    let cpus = |s: &str| s.parse::<f64>().unwrap_or(0.0);
    let arm_cpus = arms
        .iter()
        .filter_map(|a| a.entrant.spec.envelope.as_ref())
        .map(|e| e.containers.iter().map(|c| cpus(&c.cpus)).sum::<f64>())
        .fold(0.0_f64, f64::max);
    #[expect(
        clippy::cast_precision_loss,
        reason = "thread counts stay far below f64's exact range"
    )]
    let generator = producer_threads(offered_msgs_per_s) as f64;
    let demand = arm_cpus
        + cpus(&env.spec.infra.broker.cpus)
        + cpus(&env.spec.infra.clickhouse.cpus)
        + generator
        + 1.0;
    (demand, env.spec.host.vm_cpus)
}

/// The wall-clock interval the samplers actually covered, in epoch milliseconds.
///
/// Taken from the sampler's own `t_ms` values and from readable rows only, so it
/// is the same interval `SutCost::window_s` measures rather than a second one on
/// the driver's clock. `sample.py` writes `int(time.time() * 1000)` inside its
/// container and ClickHouse's `ingest_ts` is `now64(6)` inside its own; both
/// containers read the same VM kernel clock, which is what lets a `WHERE
/// ingest_ts BETWEEN` bound the row count and the latency to exactly this
/// window with no cross-clock correction.
///
/// For a single-container arm this interval *is* `cost.window_s`. For a
/// multi-container arm it is the union of the parts' intervals while
/// `SutCost::sum` defines the arm's window as the longest part's, so the two can
/// differ by the samplers' start skew — sub-second, against a window measured in
/// minutes, and taking the union is the conservative choice because a row that
/// landed while any container of the arm was being sampled is a row that arm
/// produced.
fn sampler_window_ms(costs: &[(String, crate::sampler::Samples)]) -> Option<(u64, u64)> {
    let mut from = u64::MAX;
    let mut to = 0u64;
    for (_, s) in costs {
        let mut readable = s.rows.iter().filter(|r| r.readable()).map(|r| r.t_ms);
        let first = readable.next()?;
        let last = readable.next_back().unwrap_or(first);
        from = from.min(first);
        to = to.max(last);
    }
    (from < to).then_some((from, to))
}

/// Rows whose server-side ingest timestamp falls inside the measurement window.
///
/// Compared on the integer rather than on the `DateTime64` so that no scale
/// coercion is involved: `ingest_ts` is `DateTime64(6)`, the bounds are
/// milliseconds, and a comparison between two different `DateTime64` scales is
/// one more thing that has to be right at the single moment it is most expensive
/// to discover — after an arm has finished running. `ingest_ts` is not in the
/// table's `ORDER BY`, so the predicate costs a full scan either way and the
/// integer form gives up nothing.
fn rows_in_window(ep: &Endpoints, from_ms: u64, to_ms: u64) -> Result<u64, String> {
    let sql = format!(
        "SELECT count() FROM {} WHERE toUnixTimestamp64Milli(ingest_ts) >= {from_ms} \
         AND toUnixTimestamp64Milli(ingest_ts) < {to_ms}",
        corpus::TABLE
    );
    let out =
        crate::docker::clickhouse_sql(&ep.ch_host, ep.ch_port, &ep.ch_user, &ep.ch_password, &sql)
            .map_err(|e| format!("windowed row count failed: {e}"))?;
    out.trim()
        .parse::<u64>()
        .map_err(|e| format!("windowed row count returned {out:?}: {e}"))
}

/// The latency distribution over the measurement window, computed in ClickHouse.
///
/// `ingest_ts - send_ts`, both `DateTime64(6)`, differenced as integer
/// microseconds. Nothing about it comes from the arm: `ingest_ts` is a
/// `MATERIALIZED now64(6)` column the server stamps at insert, identically for
/// every framework and every wire format, and `send_ts` is the schedule the
/// producer wrote into the message. There is one definition of "arrived" and one
/// definition of "sent", and neither is a framework's own instrumentation.
///
/// # Why approximate percentiles and an exact maximum
///
/// `quantile` is reservoir sampling: fixed memory, approximate answer.
/// `quantileExact` sorts the whole column, so its memory is proportional to the
/// row count — the same shape as the `uniqExact` that asked ClickHouse for
/// 10.45 GiB against a 10.8 GiB limit, was killed, and took a completed, valid
/// measurement down with it. A window here holds tens of millions of rows, so
/// the exact form is the version of this query that loses an hour.
///
/// `max` is exact and costs one accumulator, and it earns its place precisely
/// because the percentiles are sampled: a single multi-second stall is invisible
/// at p999 over ten million rows and is the thing a reader most wants to know
/// about, so the worst observation is reported as itself rather than as the top
/// of a reservoir.
///
/// Four scalar aggregates rather than one `quantiles(…)` call, because
/// `quantiles` renders as `[a,b,c]` in the tab-separated response this module
/// already splits on tabs and newlines, and a second grammar to parse is a
/// second thing that can be wrong about a number after the arm has gone.
fn latency_in_window(ep: &Endpoints, from_ms: u64, to_ms: u64) -> Result<Latency, String> {
    let sql = format!(
        "SELECT quantile(0.5)(lat), quantile(0.99)(lat), quantile(0.999)(lat), max(lat), \
                count() \
         FROM (SELECT toUnixTimestamp64Micro(ingest_ts) - toUnixTimestamp64Micro(send_ts) \
                      AS lat \
               FROM {} \
               WHERE toUnixTimestamp64Milli(ingest_ts) >= {from_ms} \
                 AND toUnixTimestamp64Milli(ingest_ts) < {to_ms})",
        corpus::TABLE
    );
    let out =
        crate::docker::clickhouse_sql(&ep.ch_host, ep.ch_port, &ep.ch_user, &ep.ch_password, &sql)
            .map_err(|e| format!("latency query failed: {e}"))?;
    let fields: Vec<&str> = out.trim().split(['\t', '\n']).collect();
    if fields.len() < 5 {
        return Err(format!("latency query returned {out:?}"));
    }
    let num = |i: usize| -> Result<f64, String> {
        fields[i]
            .parse::<f64>()
            .map_err(|e| format!("latency query field {i} was {:?}: {e}", fields[i]))
    };
    let rows = fields[4]
        .parse::<u64>()
        .map_err(|e| format!("latency query row count was {:?}: {e}", fields[4]))?;
    if rows == 0 {
        return Err(
            "no rows landed inside the measurement window, so there is no latency \
             distribution — the arm consumed nothing while the window was open"
                .to_owned(),
        );
    }
    Ok(Latency {
        p50_us: num(0)?,
        p99_us: num(1)?,
        p999_us: num(2)?,
        max_us: num(3)?,
        rows,
    })
}

/// The metrics only one mode can produce, keyed by the name they publish under.
///
/// **The one place a latency figure can reach a record.** [`Load::Drain`] has no
/// fields, so the drain arm of this match has nothing to emit and cannot be made
/// to emit anything without changing the type — which is the difference between
/// "we remembered not to publish latency in drain mode" and "drain mode has no
/// latency to publish". `methodology/` requires the second: in drain,
/// `send_ts` is a prefill timestamp and the subtraction measures backlog age.
///
/// The saturated case renames rather than suppresses. Suppressing would throw
/// away a real measurement — a saturated point is a genuine ceiling measurement
/// and its backlog growth is exactly what a ceiling looks like — and keeping the
/// name would let the site's aggregator median a backlog age together with a
/// latency under one key and caption the result as run-to-run spread. See
/// [`Sustained::latency_prefix`].
fn mode_metrics(load: &Load) -> Vec<(String, Metric)> {
    let Load::Sustained(s) = load else {
        return Vec::new();
    };
    let p = s.latency_prefix();
    #[expect(
        clippy::cast_precision_loss,
        reason = "row counts stay far below f64's exact range"
    )]
    let latency_rows = s.latency.rows as f64;
    vec![
        ("kept_up_share".to_owned(), Metric::share(s.kept_up_share)),
        (
            "offered_rows_per_s".to_owned(),
            // The experiment's independent variable rather than a result, which
            // `Metric` has no third direction for. `maximize` is the honest of
            // the two available readings — of two sustained records, the one
            // that offered more is the harder experiment — but nothing here
            // should be read as the arm having achieved it. `kept_up_share`
            // above is what says whether it did.
            Metric::maximize(s.offered_rows_per_s, "records/s"),
        ),
        (
            // Named for the unit it is stored in, not for the magnitude a human
            // expects, because the name and the unit have to agree — a value in
            // one scale wearing another scale's name is the defect
            // `Metric::bytes` exists for.
            "max_schedule_lag_us".to_owned(),
            Metric::minimize(s.max_schedule_lag_ms * 1000.0, "us"),
        ),
        (
            format!("{p}_p50_us"),
            Metric::minimize(s.latency.p50_us, "us"),
        ),
        (
            format!("{p}_p99_us"),
            Metric::minimize(s.latency.p99_us, "us"),
        ),
        (
            format!("{p}_p999_us"),
            Metric::minimize(s.latency.p999_us, "us"),
        ),
        (
            format!("{p}_max_us"),
            Metric::minimize(s.latency.max_us, "us"),
        ),
        (
            format!("{p}_rows"),
            // How many rows the distribution rests on, so a percentile taken over
            // a nearly-empty window is visible rather than merely small.
            Metric::maximize(latency_rows, "rows"),
        ),
    ]
}

/// Asserts a descriptor's pinned version against what the image reported.
///
/// Split out of [`resolve_sut`] because the two refusals are not the same kind
/// of thing. A version that cannot be *read* leaves the arm unidentified and can
/// only be reported; a version that *disagrees* with the descriptor identifies
/// the arm perfectly well and is a finding about the image, so it is recorded
/// against that identity as [`Status::Failed`].
///
/// A pinned version is asserted, not assumed. A base-image bump that moved the
/// version silently would otherwise publish the old label against new code.
fn assert_pinned_version(entrant: &str, pinned: &str, found: Option<&str>) -> Result<(), String> {
    if pinned.is_empty() {
        return Ok(());
    }
    match found {
        Some(found) if found != pinned => Err(format!(
            "REFUSED: {entrant} declares version {pinned:?} but the image reports \
             {found:?}. Update the descriptor in the same change that moved the image."
        )),
        _ => Ok(()),
    }
}

/// Joins a refusal to the arm's logs, in the one shape [`note_for`] can split.
fn with_logs(reason: &str, logs: &str) -> String {
    format!("{reason}{LOG_SEPARATOR}{logs}")
}

/// What a demoted record says about itself, in prose.
///
/// One spelling for the terminal and the note, so the line an operator reads
/// during a sweep and the line a reader finds in `results/` months later cannot
/// disagree about why the number was demoted.
fn infra_bound_notice() -> String {
    format!(
        "INFRA-BOUND: above {:.0}% of a measured ceiling, so this number describes \
         the shared infrastructure rather than the system",
        HEADROOM_LIMIT * 100.0
    )
}

/// The part of a refusal that belongs in a record's `note`.
///
/// A refusal carries the arm's last forty log lines per container, which is what
/// the person watching the terminal needs and not what a published record should
/// hold: the site renders `note` beside the gap, and forty lines of a JVM stack
/// trace there is a wall rather than a finding. The full text still goes to
/// stderr.
fn note_for(refusal: &str) -> String {
    let reason = refusal
        .split(LOG_SEPARATOR)
        .next()
        .unwrap_or(refusal)
        .trim();
    if reason.chars().count() <= NOTE_MAX_CHARS {
        return reason.to_owned();
    }
    let head: String = reason.chars().take(NOTE_MAX_CHARS).collect();
    format!("{head}…")
}

/// Where a pipeline got to, and whether it actually stopped moving.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Quiesced {
    /// The row count it settled — or gave up — at.
    rows: u64,
    /// Whether three consecutive polls agreed before the budget ran out.
    ///
    /// Carried rather than warned about and forgotten. A gate failure on a
    /// pipeline that never settled is not evidence about the arm, and sustained
    /// mode makes that case ordinary rather than exotic: a saturated arm spends
    /// the whole post-window quiesce draining the backlog its own window built,
    /// and a badly saturated one will still be draining when the budget expires.
    /// Without this the run would be recorded as the framework losing rows.
    settled: bool,
}

/// Waits for the pipeline to settle so the gate sees a complete frontier, and
/// returns where it got to.
///
/// # Errors
///
/// If the row-count probe fails [`ROW_PROBE_MAX_FAILURES`] times in a row, or
/// never succeeds at all. Proceeding would hand the gate a frontier nobody read,
/// and the gate would report the rows it could not see as the arm losing them.
fn quiesce(rows_now: &dyn Fn() -> Option<u64>) -> Result<Quiesced, String> {
    let mut stable = 0u32;
    let mut failures = 0u32;
    let mut prev: Option<u64> = None;
    let mut settled = false;
    let deadline = Instant::now() + Duration::from_secs(QUIESCE_MAX_S);
    loop {
        std::thread::sleep(Duration::from_secs(1));
        match rows_now() {
            // An unreadable count is not a count. Three failed polls in a row
            // used to read `0, 0, 0` and satisfy the stability test below on a
            // pipeline that had not settled at all.
            None => {
                failures += 1;
                stable = 0;
                if failures >= ROW_PROBE_MAX_FAILURES {
                    return Err(format!(
                        "the row count could not be read {failures} times running while \
                         waiting for the pipeline to settle, so the frontier is unknown \
                         and the correctness gate would read what it cannot see as loss"
                    ));
                }
            }
            Some(n) => {
                failures = 0;
                if prev == Some(n) {
                    stable += 1;
                    // Three consecutive unchanged polls: one is not enough,
                    // because a batch in flight can straddle a single poll
                    // interval.
                    if stable >= 3 {
                        settled = true;
                        break;
                    }
                } else {
                    stable = 0;
                }
                prev = Some(n);
            }
        }
        if Instant::now() > deadline {
            eprintln!(
                "WARNING: still draining after {QUIESCE_MAX_S}s; the gate may read a \
                 ragged frontier as loss."
            );
            break;
        }
    }
    let rows = prev.ok_or_else(|| {
        "no row-count probe succeeded while waiting for the pipeline to settle".to_owned()
    })?;
    eprintln!(
        "  quiesced at {rows} rows{}",
        if settled { "" } else { " (NOT settled)" }
    );
    Ok(Quiesced { rows, settled })
}

/// Reads what an arm actually is, by running it.
fn resolve_sut(arm: &Arm<'_>, image: &str) -> Result<Sut, String> {
    let digest =
        crate::docker::docker_try(&["image", "inspect", "-f", "{{.Id}}", image]).map_err(|e| {
            format!(
                "cannot read the image digest for {image}: {e}. The image must be built \
                 before it can be measured (`bench build {}`), and a run whose digest \
                 cannot be read is refused rather than published without one.",
                arm.entrant.id()
            )
        })?;

    let (version, commit, toolchain) = match arm.entrant.spec.version.as_ref() {
        Some(v) if v.strategy == "command" && !v.command.is_empty() => {
            let mut argv: Vec<&str> = vec!["run", "--rm", "--entrypoint", &v.command[0], image];
            for a in &v.command[1..] {
                argv.push(a);
            }
            let out = crate::docker::docker_try(&argv)
                .map_err(|e| format!("version command failed for {image}: {e}"))?;
            parse_version(&out)
        }
        _ => (None, None, None),
    };

    // The pinned-version assertion is deliberately NOT here; it is
    // `assert_pinned_version`, called by `measure` on the recorded side of the
    // line. This function establishes identity, and a refusal inside it means
    // there is no identity to attach a record to.
    if version.is_none() && commit.is_none() {
        return Err(format!(
            "REFUSED: could not resolve a version or commit for {}. Every published \
             number has to say what produced it.",
            arm.entrant.id()
        ));
    }

    Ok(Sut {
        entrant: arm.entrant.id().to_owned(),
        variant_id: arm.variant.id.clone(),
        version,
        commit,
        image_digest: digest,
        image: image.to_owned(),
        toolchain,
    })
}

/// Extracts `(version, commit, toolchain)` from an arm's `--version` output.
///
/// The contract is a line containing a version-like token, optionally followed
/// by a parenthesised commit, and optionally a `toolchain:` line. Deliberately
/// not a regex, for two reasons that are worth separating because the first used
/// to be stated in a form that is wrong.
///
/// A regex engine would be a dependency of *this crate*, which is the driver and
/// runs on the host — it is not, in general, compiled into an arm image, and
/// Flink's arm is a JVM that links none of this. It would nonetheless reach one
/// image: `entrants/spate/Cargo.toml` depends on this crate for the corpus, so
/// the Spate arm carries whatever is added here. That is the same argument
/// `results::year_month` makes when it declines a date crate, and it is a real
/// cost paid by exactly one arm rather than by every arm.
///
/// The reason that does the work is the second: the shape is fixed by us rather
/// than discovered. Every arm is told to print `<name> <version> (<commit>)`,
/// the output is one line we control, and what a version *means* is asserted
/// separately against `[version].pinned`. There is no dialect here for a grammar
/// to earn its keep on.
fn parse_version(out: &str) -> (Option<String>, Option<String>, Option<String>) {
    let mut version = None;
    let mut commit = None;
    let mut toolchain = None;

    for line in out.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("toolchain:") {
            toolchain = Some(rest.trim().to_owned());
            continue;
        }
        if version.is_none() {
            version = line
                .split_whitespace()
                .find(|t| {
                    t.starts_with(|c: char| c.is_ascii_digit())
                        && t.contains('.')
                        && t.chars()
                            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
                })
                .map(str::to_owned);
        }
        if commit.is_none()
            && let Some(open) = line.find('(')
            && let Some(close) = line[open..].find(')')
        {
            let inner = &line[open + 1..open + close];
            if inner.len() >= 7 && inner.chars().all(|c| c.is_ascii_hexdigit()) {
                commit = Some(inner.to_owned());
            }
        }
    }
    (version, commit, toolchain)
}

/// Builds the container specs for one arm from its descriptor.
fn build_specs(
    arm: &Arm<'_>,
    ep: &Endpoints,
    opts: &RunOptions,
    image: &str,
) -> Result<Vec<SutSpec>, String> {
    let envelope = arm
        .entrant
        .spec
        .envelope
        .as_ref()
        .ok_or("entrant has no envelope")?;

    // A FRESH consumer group per run, not a stable one per arm.
    //
    // Drain replays the prefilled corpus from offset zero, which `earliest` only
    // does for a group with no committed offsets. A stable group id would commit
    // at the end of rep 1, and rep 2 would then resume at the tail, consume
    // nothing, and sit there until the drain deadline — reporting a timeout for
    // an arm that is working perfectly.
    let group_id = format!(
        "comparison-{}-{}-{}",
        arm.entrant.id(),
        arm.variant.id,
        uuid::Uuid::now_v7().simple()
    );
    let container_names: BTreeMap<&str, String> = envelope
        .containers
        .iter()
        .map(|c| (c.name.as_str(), format!("spate-bench-sut-{}", c.name)))
        .collect();

    let volumes: Vec<String> = arm
        .entrant
        .spec
        .volumes
        .as_ref()
        .map(|v| v.named.clone())
        .unwrap_or_default();

    let mut specs = Vec::new();
    for c in &envelope.containers {
        let name = container_names[c.name.as_str()].clone();
        let mut env: Vec<(String, String)> = Vec::new();

        // The entrant's own vocabulary, with the driver-owned values substituted.
        // Sending one system's variable names to another would leave it on its
        // defaults while the record claimed knob values that were never applied —
        // a silent misreport rather than a visible failure.
        for (k, raw) in arm.entrant.spec.env.iter().chain(arm.variant.env.iter()) {
            let v = substitute(raw, arm.variant, ep, opts, &group_id, &container_names)?;
            env.push((k.clone(), v));
        }

        specs.push(SutSpec {
            name,
            image: image.to_owned(),
            cpus: c.cpus.clone(),
            memory: c.memory.clone(),
            env,
            args: c.args.clone(),
            volumes: volumes.clone(),
        });
    }

    // Control plane first: a TaskManager that starts before its JobManager spends
    // its first seconds retrying a connection, which lands inside the measurement.
    specs.sort_by_key(|s| {
        let role = envelope
            .containers
            .iter()
            .find(|c| s.name.ends_with(&c.name))
            .map(|c| c.role);
        u8::from(role != Some(Role::ControlPlane))
    });
    Ok(specs)
}

fn substitute(
    raw: &str,
    variant: &Variant,
    ep: &Endpoints,
    opts: &RunOptions,
    group_id: &str,
    containers: &BTreeMap<&str, String>,
) -> Result<String, String> {
    use crate::entrant::Placeholder;

    // The EFFECTIVE knobs, which are the variant's with this invocation's
    // overrides applied. `RunOptions::knobs_for` is also what the record's
    // variant map is built from, so what is substituted into the container and
    // what the record claims cannot be two different answers.
    let effective = opts.knobs_for(variant);

    // The vocabulary is `entrant::Placeholder`, shared with descriptor
    // validation: one definition, so a spelling validation accepts and this
    // function does not — or the reverse — cannot be written.
    let mut out = raw.to_owned();
    for (token, inner) in crate::entrant::placeholder_tokens(raw) {
        let resolved = match inner.and_then(Placeholder::parse) {
            Some(Placeholder::BrokerInternal) => Some(ep.bootstrap_internal.clone()),
            Some(Placeholder::RegistryInternal) => Some(ep.registry_internal.clone()),
            Some(Placeholder::ClickhouseInternal) => Some(ep.ch_internal.clone()),
            // Sustained mode reads a different topic from the one drain replays;
            // see `sustained_topic` for why the two are kept apart.
            Some(Placeholder::Topic) => Some(match opts.mode {
                Mode::Drain => opts.topic.clone(),
                Mode::Sustained { .. } => sustained_topic(&opts.topic),
            }),
            Some(Placeholder::GroupId) => Some(group_id.to_owned()),
            // The ClickHouse password the infra actually started the server
            // with. Before this existed every arm baked the same literal into
            // its image and the harness repeated it in two places of its own —
            // five copies of one secret, tied together by nothing. An arm that
            // takes it from here cannot drift when the infra's value changes.
            Some(Placeholder::ClickhousePassword) => Some(ep.ch_password.clone()),
            // Drain replays a prefilled corpus from the beginning; sustained
            // starts at the tail. `Mode::offset_reset` carries the argument, and
            // it is not a preference — the wrong value here fails silently and
            // publishes a drain's backlog age under a latency metric's name.
            Some(Placeholder::OffsetReset) => Some(opts.mode.offset_reset().to_owned()),
            Some(Placeholder::Knob(k)) => effective.get(k).map(knob_text),
            Some(Placeholder::Container(c)) => containers.get(c).cloned(),
            // Left in place for the refusal below, never silently dropped.
            None => None,
        };
        if let Some(v) = resolved {
            out = out.replace(token, &v);
        }
    }

    // An unresolved placeholder would reach the container verbatim and be read as
    // a literal, so the arm would run misconfigured while the record claimed the
    // intended value. Fail instead. Descriptor validation refuses the spellings
    // it can see; this guard covers what only run time knows.
    if out.contains("{{") {
        return Err(format!(
            "REFUSED: unresolved placeholder in {raw:?} (produced {out:?}). A \
             placeholder that reaches the container is read as a literal, so the \
             arm would run misconfigured while the record claimed otherwise."
        ));
    }
    Ok(out)
}

/// One knob value as the container will see it.
///
/// Integers and strings only, which is what a knob is: a value substituted into
/// an environment variable. Anything else — a TOML table, an array — has no
/// meaning on the far side of `docker run -e`, and rendering it to the empty
/// string is the same refusal the previous inline copy of this made, kept in one
/// place so the plan line, the substitution and the record cannot disagree about
/// what a knob's text is.
fn knob_text(v: &toml::Value) -> String {
    v.as_integer().map_or_else(
        || v.as_str().unwrap_or_default().to_owned(),
        |n| n.to_string(),
    )
}

/// Asserts an arm's applied cgroup caps against what its descriptor declares.
fn assert_arm_caps(name: &str, declared: &Container, meta: &str) -> Result<(), String> {
    // `# cgroup=… cpu.max=<quota>/<period> memory.max=<bytes> …`
    let field =
        |key: &str| -> Option<&str> { meta.split_whitespace().find_map(|t| t.strip_prefix(key)) };
    let cpu = field("cpu.max=").ok_or_else(|| format!("{name}: sampler reported no cpu.max"))?;
    let mem =
        field("memory.max=").ok_or_else(|| format!("{name}: sampler reported no memory.max"))?;

    let cores = {
        let mut it = cpu.split('/');
        let q = it.next().unwrap_or_default();
        let p: f64 = it.next().unwrap_or("0").parse().unwrap_or(0.0);
        if q == "max" || p <= 0.0 {
            None
        } else {
            q.parse::<f64>().ok().map(|q| q / p)
        }
    };
    let want_cores: f64 = declared.cpus.parse().unwrap_or(f64::NAN);
    if !cores.is_some_and(|c| (c - want_cores).abs() < 0.01) {
        return Err(format!(
            "REFUSED: {name} declares cpus={} but is running under cpu.max={cpu}. \
             The envelope is what every published number is described by.",
            declared.cpus
        ));
    }

    // `entrant::parse_memory`, not a copy. This assertion had its own, and it was
    // the one that had drifted: with no `k` arm, a descriptor declaring
    // `memory = "1048576k"` validated, was applied by Docker as exactly the
    // gibibyte it asked for, and then failed here with a message reporting a
    // mismatch that did not exist.
    let want_bytes = crate::entrant::parse_memory(&declared.memory);
    if want_bytes != mem.parse::<u64>().ok() {
        return Err(format!(
            "REFUSED: {name} declares memory={} but is running under memory.max={mem}.",
            declared.memory
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// At the corpus the old fixed count was calibrated against, the share
    /// reproduces it — so this changes coverage only where the corpus moved.
    #[test]
    fn the_gate_window_is_a_share_of_the_corpus_bounded_by_memory() {
        let sixteen_gib = 16 * 1024 * 1024 * 1024;
        let reference = gate_window_batches(1_500_000, sixteen_gib);
        assert!(
            (99_000..=102_000).contains(&reference),
            "expected ~100,000 at the reference corpus, got {reference}"
        );

        // Twenty times the corpus does not mean a twentieth of the coverage.
        let long = gate_window_batches(30_000_000, 32 * 1024 * 1024 * 1024);
        assert!(long > reference * 10, "{long} vs {reference}");

        // Memory is the binding constraint, not the share, once the corpus is
        // long enough — the gate is a check on a measurement already taken, so
        // it loses that race rather than winning it.
        assert!(gate_window_batches(u64::MAX, sixteen_gib) < 600_000);

        // A window of zero batches would gate nothing while reporting a gate.
        assert_eq!(gate_window_batches(1, 0), 1);
    }

    /// The deadline bounds what a broken drain wastes, so it has to move with
    /// the corpus a working one has to get through.
    #[test]
    fn the_drain_deadline_scales_with_the_corpus() {
        assert_eq!(
            drain_max_s(DRAIN_MAX_REFERENCE_BATCHES),
            DRAIN_MAX_REFERENCE_S
        );
        assert_eq!(
            drain_max_s(DRAIN_MAX_REFERENCE_BATCHES * 20),
            DRAIN_MAX_REFERENCE_S * 20
        );
        // Never below the calibrated floor, however small the corpus.
        assert_eq!(drain_max_s(1), DRAIN_MAX_REFERENCE_S);
        assert_eq!(drain_max_s(0), DRAIN_MAX_REFERENCE_S);
    }

    /// A variant carrying the Flink arm's sink knobs, for the tuning tests.
    fn tunable_variant() -> Variant {
        Variant {
            id: "rowbinary-nt".to_owned(),
            label: "rowbinary-nt".to_owned(),
            approach: crate::entrant::Approach::Realistic,
            unshipped: Vec::new(),
            default: true,
            env: BTreeMap::new(),
            knobs: [
                ("max_rows", 25_000),
                ("buffered_rows", 50_000),
                ("parallelism", 8),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_owned(), toml::Value::Integer(v)))
            .collect(),
            reports: BTreeMap::new(),
        }
    }

    fn tuning_opts(overrides: &[(&str, i64)]) -> RunOptions {
        RunOptions {
            reps: 1,
            mode: Mode::Drain,
            env_id: "test-env".to_owned(),
            trigger: Trigger::Tuning,
            dry_run: true,
            fresh_infra: false,
            fail_fast: false,
            topic: crate::corpus::TOPIC.to_owned(),
            batches: 1_500_000,
            knobs: overrides
                .iter()
                .map(|(k, v)| ((*k).to_owned(), toml::Value::Integer(*v)))
                .collect(),
        }
    }

    fn buffered_exceeds_batch() -> crate::entrant::Constraint {
        crate::entrant::Constraint {
            knob: "buffered_rows".to_owned(),
            exceeds: Some("max_rows".to_owned()),
            at_least: None,
            why: "AsyncSinkWriter refuses to construct otherwise.".to_owned(),
        }
    }

    /// One function answers "what did this arm run at", and the container
    /// configuration and the published record both ask it. Two derivations is
    /// how a record comes to report a value that was never applied.
    #[test]
    fn an_overridden_knob_replaces_the_declared_one_and_leaves_the_rest_alone() {
        let v = tunable_variant();
        let effective = tuning_opts(&[("max_rows", 262_144)]).knobs_for(&v);
        assert_eq!(effective["max_rows"].as_integer(), Some(262_144));
        assert_eq!(effective["parallelism"].as_integer(), Some(8));
        assert_eq!(effective["buffered_rows"].as_integer(), Some(50_000));
        // The descriptor is not mutated: the next arm of the same sweep, and the
        // next sweep, still start from what is committed.
        assert_eq!(v.knobs["max_rows"].as_integer(), Some(25_000));
    }

    #[test]
    fn a_knob_combination_the_entrant_rules_out_is_refused_before_a_container_starts() {
        // The first cell a sweep reaches for: raise the batch size towards the
        // single-process arms' 262,144 and leave the buffer where it is. Flink's
        // AsyncSinkWriter would refuse to construct, minutes in, with a message
        // naming neither knob.
        let problems = unrunnable_knobs(
            "flink:rowbinary-nt",
            &tunable_variant(),
            &[buffered_exceeds_batch()],
            &tuning_opts(&[("max_rows", 262_144)]),
        );
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(
            problems[0].contains("flink:rowbinary-nt"),
            "{}",
            problems[0]
        );
        assert!(problems[0].contains("buffered_rows"), "{}", problems[0]);

        // Raising both together is the runnable cell, and nothing objects to it.
        assert!(
            unrunnable_knobs(
                "flink:rowbinary-nt",
                &tunable_variant(),
                &[buffered_exceeds_batch()],
                &tuning_opts(&[("max_rows", 262_144), ("buffered_rows", 1_048_576)]),
            )
            .is_empty()
        );
    }

    #[test]
    fn an_override_naming_a_knob_the_variant_does_not_declare_is_refused() {
        // `--knob paralellism=4` would otherwise resolve nothing, leave the real
        // knob at 8, and write the misspelling into the record's variant map —
        // a cell reporting a configuration it never ran.
        let problems = unrunnable_knobs(
            "flink:rowbinary-nt",
            &tunable_variant(),
            &[],
            &tuning_opts(&[("paralellism", 4)]),
        );
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("does not declare"), "{}", problems[0]);
        // The message has to list what IS declared, or the operator's next guess
        // is another typo.
        assert!(problems[0].contains("parallelism"), "{}", problems[0]);
    }

    #[test]
    fn parses_the_arms_version_line() {
        let (v, c, t) =
            parse_version("spate-arm 0.1.0-dev (6f28a8b8912e)\ntoolchain: rustc 1.97.1");
        assert_eq!(v.as_deref(), Some("0.1.0-dev"));
        assert_eq!(c.as_deref(), Some("6f28a8b8912e"));
        assert_eq!(t.as_deref(), Some("rustc 1.97.1"));
    }

    #[test]
    fn parses_a_bare_version() {
        let (v, c, _) = parse_version("2.2.1\n");
        assert_eq!(v.as_deref(), Some("2.2.1"));
        assert_eq!(c, None);
    }

    #[test]
    fn a_non_hex_parenthetical_is_not_a_commit() {
        // "(build 42)" is not a commit, and recording it as one would put a
        // fabricated provenance field on a published record.
        let (_, c, _) = parse_version("thing 1.2.3 (not a sha)");
        assert_eq!(c, None);
    }

    #[test]
    fn an_arm_cap_mismatch_is_refused() {
        let declared = Container {
            role: Role::DataPlane,
            name: "sut".to_owned(),
            cpus: "4".to_owned(),
            memory: "16g".to_owned(),
            args: vec![],
            gc_log: None,
        };
        let good = "# cgroup=/x cpu.max=400000/100000 memory.max=17179869184 x=1";
        assert!(assert_arm_caps("sut", &declared, good).is_ok());

        // The evidence the previous harness threw away: this is the sampler
        // proving the arm did NOT get the envelope it is described by.
        let bad = "# cgroup=/x cpu.max=200000/100000 memory.max=17179869184 x=1";
        let e = assert_arm_caps("sut", &declared, bad).expect_err("must refuse");
        assert!(e.starts_with("REFUSED"), "{e}");

        let uncapped = "# cgroup=/x cpu.max=max/100000 memory.max=max x=1";
        assert!(assert_arm_caps("sut", &declared, uncapped).is_err());
    }

    /// The digest a test's ceilings are measured under. Any value will do; what
    /// matters is that the gate compares it against the environment's, so the
    /// two have to be the same string or every ceiling below is refused.
    const TEST_DIGEST: &str = "e3b0c44298fc";
    /// Mean framed message size a test's consume ceiling was taken at.
    const TEST_MESSAGE_BYTES: u64 = 4056;

    fn provenance() -> ceiling::Provenance {
        ceiling::Provenance {
            date: "2026-07-25".to_owned(),
            dataset_version: "d2-test".to_owned(),
            infra_digest: TEST_DIGEST.to_owned(),
            host: "test".to_owned(),
            rig: "test".to_owned(),
        }
    }

    /// A gate over the ceilings a test states, resolved for this corpus.
    ///
    /// Built through `Ceilings::gate` rather than by hand because `Ceiling` has
    /// no public constructor, and that is the point of it: a caller cannot
    /// assemble a ceiling it was not given, so a test gates against exactly what
    /// a run would.
    ///
    /// The consume ceiling's byte rate is stated as its own message rate times
    /// its own message size, so the two readings `Ceiling::headroom` takes the
    /// larger of coincide and a test is about the arithmetic it is testing.
    fn gate(consume_msgs_per_s: Option<u64>, clickhouse: Vec<ceiling::IngestCeiling>) -> Ceiling {
        #[expect(clippy::cast_precision_loss, reason = "small integers")]
        let consume = consume_msgs_per_s.map(|msgs_per_s| ceiling::ConsumeCeiling {
            msgs_per_s,
            mb_per_s: (msgs_per_s * TEST_MESSAGE_BYTES) as f64 / 1e6,
            message_bytes: TEST_MESSAGE_BYTES,
            partitions: 8,
            broker: "test".to_owned(),
            threads: 4,
            // Inside the bench network, because that is where every arm's
            // consumer runs and `Ceilings::gate` drops a ceiling that says
            // otherwise. A fixture that left this empty would be refused for
            // that rather than for what the test is about.
            client: ceiling::Location::Inside.name().to_owned(),
            window: None,
            broker_cgroup: None,
            provenance: provenance(),
        });
        ceiling::Ceilings {
            consume,
            clickhouse,
        }
        .gate(TEST_MESSAGE_BYTES, TEST_DIGEST)
    }

    fn ingest(format: &str, rows_per_s: u64) -> ceiling::IngestCeiling {
        ceiling::IngestCeiling {
            format: format.to_owned(),
            rows_per_s,
            mb_per_s: 1.0,
            row_bytes: 91,
            threads: 4,
            clickhouse: "test".to_owned(),
            client: ceiling::Location::Inside.name().to_owned(),
            sweep: Vec::new(),
            target_cgroup: None,
            landed: None,
            parts: None,
            network: None,
            settle: None,
            stopped_at: None,
            provenance: provenance(),
        }
    }

    /// The headroom limit is stated against the ceiling pass's **message** rate,
    /// so an arm has to be converted into messages before the two are compared —
    /// and that conversion is the driver's risk, because only the driver knows
    /// the row yield it actually measured. Multiplying by `EVENTS_PER_BATCH`
    /// instead ignores the filters and makes the gate 1.36x too lenient:
    /// the arm below sits at 95% of the ceiling and would gate at
    /// 70%, i.e. published as `ok` while the number described the broker.
    #[test]
    fn an_arm_is_gated_against_the_message_ceiling_at_the_workloads_own_yield() {
        let batches = 10_000u64;
        let msgs_per_s = 300_000u64;
        let per_event = f64::from(corpus::EVENTS_PER_BATCH);
        let per_message = rows_per_message(corpus::expected_rows(batches), batches);

        // The filters drop about a quarter of every message's events.
        assert!(
            (70.0..80.0).contains(&per_message),
            "the workload yields {per_message} rows per message"
        );

        let gate = gate(Some(msgs_per_s), Vec::new());
        // An arm at 95% of the proven consume ceiling, in rows.
        #[expect(clippy::cast_precision_loss, reason = "small integers")]
        let rows_per_s = 0.95 * msgs_per_s as f64 * per_message;

        // The conversion the driver performs: rows back into messages at the
        // workload's own yield.
        let honest = gate.headroom(ceiling::Achieved {
            msgs_per_s: rows_per_s / per_message,
            rows_per_s,
            wire_format: "rowbinary",
            server_side_transform: false,
        });
        let share = honest.binding().expect("the consume ceiling applied").share;
        assert!((share - 0.95).abs() < 1e-9, "share was {share}");
        assert!(
            share > HEADROOM_LIMIT && honest.infra_bound(),
            "an arm at 95% of the ceiling must be infra-bound"
        );

        // The defect, reproduced: converting at the raw event count understates
        // the share by exactly the ratio of the two yields, which is enough to
        // pass an arm that should have been refused.
        let understated = gate.headroom(ceiling::Achieved {
            msgs_per_s: rows_per_s / per_event,
            rows_per_s,
            wire_format: "rowbinary",
            server_side_transform: false,
        });
        let wrong = understated
            .binding()
            .expect("the consume ceiling applied")
            .share;
        assert!(
            !understated.infra_bound(),
            "a gate converting at the raw event count would let this through, or the test \
             proves nothing"
        );
        assert!((share / wrong - per_event / per_message).abs() < 1e-9);
    }

    /// `methodology/` says an arm above 70% of **either** ceiling is
    /// infra-bound, and the driver reads that as one predicate over every share.
    ///
    /// The case that matters is the one below: an arm with plenty of broker
    /// headroom sitting at 90% of what ClickHouse absorbed for its insert
    /// format. Reading only the consume ceiling — which is what the driver did
    /// before `ceiling::Ceiling` existed — publishes it as a system comparison
    /// when the number describes the target.
    #[test]
    fn an_arm_bound_by_clickhouse_is_infra_bound_even_with_broker_headroom() {
        let gate = gate(Some(1_000_000), vec![ingest("rowbinary", 4_000_000)]);
        let arm = ceiling::Achieved {
            // A tenth of the consume ceiling: nothing to see on the broker.
            msgs_per_s: 100_000.0,
            // Nine tenths of what ClickHouse took for this format.
            rows_per_s: 3_600_000.0,
            wire_format: "rowbinary",
            server_side_transform: false,
        };
        let headroom = gate.headroom(arm);

        assert_eq!(headroom.shares().len(), 2, "{}", headroom.summary());
        assert!(headroom.infra_bound());
        assert!(
            headroom.is_proven(),
            "both ceilings were checked: {}",
            headroom.summary()
        );
        let binding = headroom.binding().expect("two shares were charged");
        assert!(
            binding.kind == ceiling::Against::ClickHouseIngest,
            "the ClickHouse share binds, not the broker's: {}",
            binding.against
        );
    }

    /// "Not gated" must never read as "cleared the gate".
    ///
    /// An arm whose insert format has no measured ClickHouse ceiling is
    /// deliberately not gated against the target — substituting another format's
    /// figure is the same unmeasured conversion that produced the message-size
    /// defect — so its headroom is *unknown* rather than satisfied, and the
    /// record has to carry [`Flag::HeadroomUnproven`] to say so. A share
    /// comfortably below the limit against the one ceiling that could be checked
    /// is not the same claim, and the flag is the only thing that distinguishes
    /// them to a consumer.
    #[test]
    fn an_arm_no_ceiling_covers_is_recorded_as_unproven_rather_than_as_cleared() {
        // Nothing measured at all: no broker ceiling, no ClickHouse ceiling.
        let nothing = gate(None, Vec::new()).headroom(ceiling::Achieved {
            msgs_per_s: 100_000.0,
            rows_per_s: 10_000_000.0,
            wire_format: "rowbinary",
            server_side_transform: false,
        });
        assert!(nothing.shares().is_empty());
        assert!(!nothing.is_proven());
        // And it is NOT infra-bound, which is exactly why the flag has to exist:
        // an ungated arm records `Status::Ok`.
        assert!(!nothing.infra_bound());
        assert!(nothing.summary().contains("no ceiling applied"));

        // The broker was measured; this arm's insert format was not.
        let partial = gate(Some(1_000_000), vec![ingest("rowbinary", 4_000_000)]).headroom(
            ceiling::Achieved {
                msgs_per_s: 100_000.0,
                rows_per_s: 10_000_000.0,
                wire_format: "native",
                server_side_transform: false,
            },
        );
        assert_eq!(partial.shares().len(), 1);
        assert!(!partial.infra_bound());
        assert!(
            !partial.is_proven(),
            "an arm gated against one of two ceilings is not a proven arm"
        );
        assert!(
            partial.summary().contains("UNPROVEN"),
            "{}",
            partial.summary()
        );
    }

    /// An arm that installs its own ClickHouse objects is never gated against
    /// the direct-insert ingest ceiling, even when a ceiling for its exact wire
    /// format exists.
    ///
    /// The ceilings were measured against the bare target. An arm whose every
    /// insert also runs a materialized view's flatten, filters and derived
    /// columns asks the server for more work per row than the ceiling ever
    /// measured, so gating it there would publish "headroom proven" for an arm
    /// that may be saturating ClickHouse. The broker share still applies —
    /// consume cost does not depend on what happens after the insert arrives.
    #[test]
    fn an_arm_with_its_own_ddl_is_not_gated_against_the_direct_insert_ceiling() {
        let gate = gate(Some(1_000_000), vec![ingest("rowbinary", 4_000_000)]);
        let headroom = gate.headroom(ceiling::Achieved {
            msgs_per_s: 100_000.0,
            // Ninety percent of the direct-insert figure: gated naively, this
            // arm would be refused as infra-bound; gated correctly it is
            // *unproven*, which is a different claim than either.
            rows_per_s: 3_600_000.0,
            wire_format: "rowbinary",
            server_side_transform: true,
        });
        assert_eq!(headroom.shares().len(), 1, "{}", headroom.summary());
        assert!(
            headroom
                .shares()
                .iter()
                .all(|s| s.kind == ceiling::Against::BrokerConsume),
            "only the broker share may be charged: {}",
            headroom.summary()
        );
        assert!(!headroom.infra_bound());
        assert!(
            !headroom.is_proven(),
            "refused is unproven, never silently cleared: {}",
            headroom.summary()
        );
        assert!(
            headroom.summary().contains("UNPROVEN"),
            "{}",
            headroom.summary()
        );
    }

    /// The record says which ClickHouse ceiling the arm was actually held to,
    /// and it says `0` for one that was not held to any.
    ///
    /// The gate keeps the decision. A ceiling present in the committed file but
    /// refused by the gate — measured under another envelope, say — must not be
    /// recorded as the figure this arm cleared, because "no ceiling applied" and
    /// "the ceiling was satisfied" are the two claims the whole mechanism exists
    /// to keep apart.
    #[test]
    fn the_record_names_the_clickhouse_ceiling_the_arm_was_actually_held_to() {
        let committed = ceiling::Ceilings {
            consume: None,
            clickhouse: vec![
                ingest("rowbinary", 4_000_000),
                ingest("rowbinary_nt", 3_100_000),
            ],
        };
        let gate = committed.gate(TEST_MESSAGE_BYTES, TEST_DIGEST);
        fn achieved(wire_format: &str) -> ceiling::Achieved<'_> {
            ceiling::Achieved {
                msgs_per_s: 10_000.0,
                rows_per_s: 1_000_000.0,
                wire_format,
                server_side_transform: false,
            }
        }

        // Each format is held to its own figure, never to the other's.
        assert_eq!(
            gate.headroom(achieved("rowbinary"))
                .applied_ingest_rows_per_s()
                .unwrap_or(0),
            4_000_000
        );
        assert_eq!(
            gate.headroom(achieved("rowbinary_nt"))
                .applied_ingest_rows_per_s()
                .unwrap_or(0),
            3_100_000
        );

        // A format with no measured ceiling is gated against nothing, and says
        // so with a zero rather than borrowing a neighbour's number.
        assert_eq!(
            gate.headroom(achieved("native"))
                .applied_ingest_rows_per_s()
                .unwrap_or(0),
            0
        );
        // And a ceiling the gate refused is not the ceiling the arm cleared. The
        // file still holds it; the gate dropped it for a different envelope.
        let stale = committed.gate(TEST_MESSAGE_BYTES, "0123456789ab");
        assert_eq!(
            stale
                .headroom(achieved("rowbinary"))
                .applied_ingest_rows_per_s()
                .unwrap_or(0),
            0
        );
    }

    /// A pinned version that disagrees with the image identifies the arm
    /// perfectly, so it is a finding about the image rather than a reason to
    /// write nothing. `measure` records it as `Status::Failed`.
    #[test]
    fn an_image_that_disagrees_with_its_pinned_version_is_refused() {
        assert!(assert_pinned_version("flink", "2.2.1", Some("2.2.1")).is_ok());
        // No pin declared, and nothing to assert against.
        assert!(assert_pinned_version("flink", "", Some("2.3.0")).is_ok());
        // Pinned, but the image reports no version at all: `resolve_sut` has
        // already refused unless a commit identified it, and a commit-identified
        // arm has nothing to compare.
        assert!(assert_pinned_version("flink", "2.2.1", None).is_ok());

        let e = assert_pinned_version("flink", "2.2.1", Some("2.3.0")).expect_err("must refuse");
        assert!(e.starts_with("REFUSED"), "{e}");
        assert!(e.contains("2.2.1") && e.contains("2.3.0"), "{e}");
    }

    /// A refusal is two things at once: a diagnosis for whoever is watching, and
    /// a caveat that travels with a published gap. The forty log lines per
    /// container belong to the first only — the site renders `note` beside the
    /// gap, and a JVM stack trace there is a wall rather than a finding.
    /// A demoted record must say so in prose, not only in `status`. The share
    /// alone reads as a demotion only to someone holding the rule, and the note
    /// is what the site renders beside the number.
    #[test]
    fn the_infra_bound_notice_names_the_limit_it_broke() {
        let notice = infra_bound_notice();
        assert!(notice.starts_with("INFRA-BOUND:"), "{notice}");
        assert!(
            notice.contains(&format!("{:.0}%", HEADROOM_LIMIT * 100.0)),
            "the notice must quote the limit it is enforcing: {notice}"
        );
        assert!(
            notice.contains("shared infrastructure"),
            "the notice must say what the number describes instead: {notice}"
        );
        // Survives `note_for`'s truncation, so a refusal carrying it keeps it.
        assert!(notice.chars().count() < NOTE_MAX_CHARS, "{notice}");
    }

    #[test]
    fn a_recorded_refusal_keeps_its_reason_and_drops_the_container_logs() {
        let refusal = with_logs(
            "the arm's data-plane container spate-bench-sut-taskmanager exited during the drain",
            "--- spate-bench-sut-taskmanager ---\njava.lang.OutOfMemoryError\n\tat x\n\tat y",
        );
        let note = note_for(&refusal);
        assert!(note.starts_with("the arm's data-plane container"), "{note}");
        assert!(!note.contains("OutOfMemoryError"), "{note}");
        assert!(!note.contains("Logs:"), "{note}");

        // A refusal with no logs at all is its own note.
        assert_eq!(
            note_for("truncate failed: connection refused"),
            "truncate failed: connection refused"
        );

        // And a reason long enough to be a wall in its own right is cut, on a
        // character boundary rather than a byte one.
        let long = format!("{}…tail", "é".repeat(NOTE_MAX_CHARS + 50));
        let cut = note_for(&long);
        assert_eq!(cut.chars().count(), NOTE_MAX_CHARS + 1);
        assert!(cut.ends_with('…'));
    }

    /// One `Sustained` with a stated kept-up share, for the tests below to vary
    /// the one field they are about.
    fn sustained(kept_up_share: f64) -> Sustained {
        Sustained {
            offered_rows_per_s: 4_000_000.0,
            kept_up_share,
            max_schedule_lag_ms: 12.5,
            latency: Latency {
                p50_us: 3_100.0,
                p99_us: 41_000.0,
                p999_us: 260_000.0,
                max_us: 1_900_000.0,
                rows: 91_000_000,
            },
        }
    }

    fn keys(load: &Load) -> Vec<String> {
        mode_metrics(load).into_iter().map(|(k, _)| k).collect()
    }

    /// The structural half of `methodology/`'s rule that drain reports
    /// throughput only.
    ///
    /// `Load::Drain` carries no fields, so there is no latency-shaped value on
    /// the drain path at all — this test pins the consequence, and the type pins
    /// the cause. The distinction matters because in drain mode `send_ts` is a
    /// prefill timestamp written into the topic hours earlier, so
    /// `ingest_ts - send_ts` measures how long the backlog sat there. A
    /// drain-mode latency figure would be a wrong number wearing a right one's
    /// name.
    #[test]
    fn a_drain_measurement_emits_no_latency_metric_at_all() {
        assert!(mode_metrics(&Load::Drain).is_empty());
    }

    /// An arm that tracked the offered rate publishes latency, and publishes it
    /// under the name a consumer plots.
    #[test]
    fn a_sustained_arm_that_kept_up_publishes_latency_and_never_backlog_age() {
        let load = Load::Sustained(sustained(0.997));
        let Load::Sustained(s) = &load else {
            unreachable!()
        };
        assert!(s.kept_up());
        assert_eq!(s.latency_prefix(), "latency");

        let keys = keys(&load);
        assert!(keys.iter().any(|k| k == "latency_p999_us"), "{keys:?}");
        assert!(keys.iter().any(|k| k == "kept_up_share"), "{keys:?}");
        assert!(
            !keys.iter().any(|k| k.starts_with("backlog_age")),
            "{keys:?}"
        );
    }

    /// The one that must not regress: a saturated arm keeps its numbers and
    /// loses the name.
    ///
    /// `methodology/` allows a saturated record to carry latency figures — it
    /// is a genuine ceiling measurement — but says they "describe backlog age
    /// and must not be read as latency at the offered rate". A flag and a note
    /// say that to a person; renaming the metric says it to the site's
    /// aggregator, which medians metrics by name and would otherwise average a
    /// backlog age together with a latency across repetitions and caption the
    /// result as run-to-run spread.
    #[test]
    fn a_saturated_sustained_arm_publishes_backlog_age_and_never_latency() {
        let load = Load::Sustained(sustained(0.41));
        let Load::Sustained(s) = &load else {
            unreachable!()
        };
        assert!(!s.kept_up());
        assert_eq!(s.latency_prefix(), "backlog_age");

        let keys = keys(&load);
        for suffix in ["p50_us", "p99_us", "p999_us", "max_us", "rows"] {
            assert!(
                keys.iter().any(|k| k == &format!("backlog_age_{suffix}")),
                "{keys:?}"
            );
        }
        assert!(!keys.iter().any(|k| k.starts_with("latency")), "{keys:?}");

        // The numbers themselves are kept, not suppressed: throwing them away
        // would discard the measurement a saturating host is best placed to make.
        let by_key: std::collections::BTreeMap<String, f64> = mode_metrics(&load)
            .into_iter()
            .map(|(k, m)| (k, m.value))
            .collect();
        assert!((by_key["backlog_age_max_us"] - 1_900_000.0).abs() < f64::EPSILON);
    }

    /// One predicate decides the flag, the note and the metric names, and the
    /// boundary is where the constant says it is.
    #[test]
    fn the_saturation_threshold_is_the_only_thing_that_decides_the_metric_names() {
        assert!(sustained(KEPT_UP_MIN).kept_up());
        assert!(!sustained(KEPT_UP_MIN - 0.001).kept_up());
        // A share above 1.0 is legitimate — the arm cleared a backlog inside the
        // window — and must not be mistaken for saturation.
        assert!(sustained(1.04).kept_up());
    }

    /// The offered rate is a **message** rate and the consumed rate is a row
    /// rate, so the conversion has to happen at the workload's own yield. This
    /// is the same defect the headroom gate guards against: the filters land
    /// about 73.5 rows per message, so converting at the raw 100 would make a
    /// saturating arm look as though it kept up.
    #[test]
    fn the_offered_row_rate_is_the_message_rate_at_the_workloads_own_yield() {
        let batches = 10_000u64;
        let offered_msgs_per_s = 40_000.0;
        let per_event = f64::from(corpus::EVENTS_PER_BATCH);
        let per_message = rows_per_message(corpus::expected_rows(batches), batches);

        // An arm consuming exactly what is offered.
        let consumed = offered_msgs_per_s * per_message;
        let honest = consumed / (offered_msgs_per_s * per_message);
        assert!((honest - 1.0).abs() < 1e-9);

        // The defect, reproduced: dividing by the raw event count overstates the
        // denominator by the ratio of the yields, so an arm that kept up
        // perfectly is recorded as having managed only three quarters of the
        // offered rate — and flagged SATURATED for it.
        let understated = consumed / (offered_msgs_per_s * per_event);
        assert!((honest / understated - per_event / per_message).abs() < 1e-9);
        assert!(
            understated < KEPT_UP_MIN,
            "the wrong yield should have flagged this arm, or the test proves nothing"
        );
    }

    /// The generator is sized from the rate and is never capped, because a
    /// producer too weak to saturate the arm measures nothing.
    #[test]
    fn the_generator_is_sized_from_the_offered_rate_and_is_never_capped() {
        assert_eq!(producer_threads(1), 1);
        assert_eq!(producer_threads(PRODUCER_THREAD_MSGS_PER_S), 1);
        assert_eq!(producer_threads(PRODUCER_THREAD_MSGS_PER_S + 1), 2);
        // A rate that needs more threads than the reference host has cores gets
        // them. That is the host caveat becoming visible rather than the harness
        // quietly under-producing and the arm looking better than it is.
        assert_eq!(producer_threads(1_200_000), 20);
    }

    /// The disk guard has to project the worst case the run can reach, not the
    /// one it will probably hit: a guard that under-projects is a guard that
    /// lets the broker's volume fill halfway through a sweep.
    #[test]
    fn the_topic_projection_charges_every_run_for_its_warm_up_deadline() {
        let per_msg = 4_200u64;
        let rate = 40_000u64;
        let one = projected_topic_bytes(0, rate, 60, 1, per_msg);
        assert_eq!(one, rate * (60 + SUSTAINED_WARMUP_BUDGET_S) * per_msg);

        // Every run produces; the messages already on the topic are counted once.
        let three = projected_topic_bytes(1_000, rate, 60, 3, per_msg);
        assert_eq!(
            three,
            (1_000 + 3 * rate * (60 + SUSTAINED_WARMUP_BUDGET_S)) * per_msg
        );
        assert!(three > one);

        // A three-repetition sweep of one arm at a rate high enough to saturate
        // the fastest published drain figure has to fit, or this mode cannot be
        // used on the reference host at all and the guard is the reason.
        assert!(
            projected_topic_bytes(0, 40_000, 30, 3, per_msg) < SUSTAINED_TOPIC_BYTES_MAX,
            "a 3-rep sustained sweep at 40k msgs/s over a 30s window must be allowed"
        );
    }

    /// The warm-up allowance and the warm-up deadline are different numbers on
    /// purpose: charging the deadline would make the projection several times
    /// the reality and refuse sweeps this host can run, and a guard that always
    /// says no is a guard that gets removed.
    #[test]
    fn the_disk_projection_charges_a_warm_up_allowance_and_not_the_warm_up_deadline() {
        const { assert!(SUSTAINED_WARMUP_BUDGET_S < SUSTAINED_WARMUP_MAX_S) };
    }

    /// The window a sustained run counts rows and latency over is the sampler's
    /// own, expressed in the wall clock ClickHouse's `ingest_ts` also reads.
    ///
    /// Unreadable rows are excluded, which is not a detail: `sample.py` writes
    /// `-1` into every field of a row it could not read, and a window opened on
    /// one of those would be a window whose endpoint no counter agrees with.
    #[test]
    fn the_row_and_latency_window_is_the_samplers_own_readable_span() {
        // `-1` is what `sample.py` writes into every field of a row it could not
        // read, and the fields do not fail independently.
        let sample = |t_ms: u64, readable: bool| {
            let v = if readable { 1i64 } else { -1i64 };
            crate::sampler::Sample {
                t_ms,
                usage_usec: v,
                user_usec: v,
                system_usec: v,
                nr_throttled: v,
                throttled_usec: v,
                mem_current: v,
                mem_peak: v,
                anon: v,
                file: v,
                slab: v,
                kernel_stack: v,
                sock: v,
            }
        };
        let series = |rows: Vec<crate::sampler::Sample>| crate::sampler::Samples {
            meta: String::new(),
            rows,
            wall_s: 0.0,
        };

        // Two containers whose samplers started a second apart. The union is
        // taken, because a row that landed while any container of the arm was
        // being sampled is a row that arm produced.
        let costs = vec![
            (
                "tm".to_owned(),
                series(vec![sample(1_000, true), sample(5_000, true)]),
            ),
            (
                "jm".to_owned(),
                series(vec![sample(1_100, true), sample(6_000, true)]),
            ),
        ];
        assert_eq!(sampler_window_ms(&costs), Some((1_000, 6_000)));

        // An unreadable row contributes no endpoint. A window whose edge came
        // from a sentinel would be a window no counter agrees with, and the row
        // count and the latency taken over it would describe a different
        // interval from `cores_used`.
        let ragged = vec![(
            "tm".to_owned(),
            series(vec![
                sample(1_000, false),
                sample(2_000, true),
                sample(9_000, true),
                sample(9_500, false),
            ]),
        )];
        assert_eq!(sampler_window_ms(&ragged), Some((2_000, 9_000)));

        // A series with nothing readable in it has no window at all, rather than
        // a window built on sentinels.
        let none = vec![(
            "tm".to_owned(),
            series(vec![sample(1_000, false), sample(2_000, false)]),
        )];
        assert_eq!(sampler_window_ms(&none), None);
    }

    #[test]
    fn a_kibibyte_memory_declaration_reaches_the_cap_assertion_intact() {
        // The drift this closes. This assertion carried its own memory parser
        // with no `k` arm, while the descriptor validator had one — so
        // `memory = "1048576k"` passed validation, was applied by Docker as
        // exactly one gibibyte, and then failed here, refusing a run over a
        // mismatch that did not exist and naming a limit that was correct.
        let declared = Container {
            role: Role::DataPlane,
            name: "sut".to_owned(),
            cpus: "4".to_owned(),
            memory: "1048576k".to_owned(),
            args: vec![],
            gc_log: None,
        };
        let meta = "# cgroup=/x cpu.max=400000/100000 memory.max=1073741824 x=1";
        assert!(assert_arm_caps("sut", &declared, meta).is_ok());
    }

    /// One server-side figure, in the shape the reference ClickHouse produced
    /// for three real inserts from the Spate arm.
    fn server_side() -> serverside::ServerSideCost {
        serverside::ServerSideCost {
            window: serverside::Window::new(1_784_979_298_378, 1_784_979_598_378)
                .expect("a forward window"),
            tables: vec!["default.sensor_events".to_owned()],
            queries: 3,
            queries_started: 3,
            failed_queries: 0,
            written_rows: 786_984,
            inserted_rows: Some(786_984),
            inserted_bytes: Some(50_773_537),
            cpu_us: 514_218.0,
            user_us: 504_865.0,
            system_us: 9_353.0,
            real_us: Some(1_301_238.0),
            os_cpu_virtual_us: Some(514_215.0),
            os_cpu_wait_us: Some(786_278.0),
            queries_without_cpu: 0,
        }
    }

    /// The two per-row figures are not the same quantity, and a record carries
    /// both because they answer different questions: one shares the arm's own
    /// denominator and may be added to `cpu_us_per_row`, the other divides by
    /// what the server says it wrote and describes the insert format. They
    /// coincide only for an arm that inserted each landed row exactly once.
    #[test]
    fn a_server_side_figure_publishes_cpu_against_both_denominators() {
        let cost = server_side();
        let by_key: BTreeMap<String, Metric> =
            server_metrics(&cost, 786_984.0).into_iter().collect();

        let per_landed = &by_key["ch_cpu_us_per_row"];
        assert_eq!(per_landed.unit, "us");
        assert!(!per_landed.higher_is_better);
        assert!(
            (per_landed.value - by_key["ch_cpu_us_per_written_row"].value).abs() < 1e-12,
            "the two coincide when every written row landed"
        );

        // Half the rows landed: the arm duplicated, and the cost per landed row
        // doubles while the cost per written row does not move.
        let duplicated: BTreeMap<String, Metric> =
            server_metrics(&cost, 393_492.0).into_iter().collect();
        assert!(
            (duplicated["ch_cpu_us_per_row"].value - 2.0 * per_landed.value).abs() < 1e-9,
            "{}",
            duplicated["ch_cpu_us_per_row"].value
        );
        assert!((duplicated["ch_cpu_us_per_written_row"].value - per_landed.value).abs() < 1e-12);

        // The batch size the SERVER saw, which is the figure that explains a
        // per-row cost without asking the arm anything.
        assert!(by_key["ch_rows_per_insert"].higher_is_better);
        assert!((by_key["ch_rows_per_insert"].value - 262_328.0).abs() < 1.0);
    }

    /// A counter the server never mentioned is absent from the record rather
    /// than present as a zero. `ProfileEvents` omits any counter whose value is
    /// zero, so "the server measured nothing" and "this build has no such
    /// counter" are indistinguishable once a default has been substituted.
    #[test]
    fn a_counter_the_server_did_not_report_reaches_no_metric_at_all() {
        let mut cost = server_side();
        cost.os_cpu_wait_us = None;
        cost.inserted_bytes = None;
        let keys: Vec<String> = server_metrics(&cost, 786_984.0)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert!(!keys.iter().any(|k| k == "ch_cpu_wait_us"), "{keys:?}");
        assert!(!keys.iter().any(|k| k == "ch_inserted_bytes"), "{keys:?}");
        // The counters that were reported are still published.
        assert!(keys.iter().any(|k| k == "ch_cpu_us"), "{keys:?}");
    }

    /// One JVM's figures, in the shape a G1 log summarises to.
    fn gc_summary(total_us: f64) -> jvm::GcSummary {
        jvm::GcSummary {
            pauses: 5,
            total_us,
            max_us: 2_739.0,
            max_label: "Young (Normal) (G1 Evacuation Pause)".to_owned(),
            p50_us: 2_182.0,
            p99_us: 2_739.0,
            p999_us: 2_739.0,
            mean_us: total_us / 5.0,
            from_uptime_s: Some(0.228),
            to_uptime_s: Some(0.267),
            configured: jvm::HeapConfig {
                collector: Some("G1".to_owned()),
                version: Some("25.0.3+9-LTS (release)".to_owned()),
                initial_bytes: Some(256 * 1024 * 1024),
                min_bytes: Some(8 * 1024 * 1024),
                max_bytes: Some(256 * 1024 * 1024),
            },
            peak_committed_bytes: Some(256 * 1024 * 1024),
            peak_occupancy_bytes: Some(174 * 1024 * 1024),
            peak_live_bytes: Some(39 * 1024 * 1024),
        }
    }

    /// The asymmetry that must never be rendered as a zero.
    ///
    /// A Rust binary has no collector, so it has no pause distribution and no
    /// configured heap — a real difference between the runtimes and worth
    /// showing. But the absence of a GC number is not a GC number of zero: a
    /// chart drawing a missing pause total as a bar of length zero says "Spate
    /// paused for 0 ms", which is a claim about a measurement nobody made. The
    /// record therefore carries no `gc_*` key at all, so a consumer has to
    /// render the gap rather than a value.
    #[test]
    fn an_arm_with_no_collector_publishes_no_gc_metric_at_all() {
        assert!(gc_metrics(&Gc::default()).is_empty());
        // And the same for a JVM arm whose log could not be read, which is a
        // different fact and is likewise not a pause total of zero.
        assert!(
            gc_metrics(&Gc {
                data_plane: None,
                control_plane: None,
            })
            .is_empty()
        );
    }

    /// The data plane's pauses are the ones that stopped the ingestion, so they
    /// are the headline; the control plane's are published beside them under
    /// their own prefix, exactly as `data_plane_cores_used` sits beside the arm
    /// total. Folding the two together would name an interval in which neither
    /// JVM was entirely stopped.
    #[test]
    fn the_data_planes_gc_figures_are_the_headline_and_the_control_planes_are_secondary() {
        let gc = Gc {
            data_plane: Some(gc_summary(9_666.0)),
            control_plane: Some(gc_summary(1_200.0)),
        };
        let by_key: BTreeMap<String, Metric> = gc_metrics(&gc).into_iter().collect();

        assert!((by_key["gc_pause_total_us"].value - 9_666.0).abs() < 1e-9);
        assert!((by_key["control_plane_gc_pause_total_us"].value - 1_200.0).abs() < 1e-9);
        assert!(!by_key["gc_pause_total_us"].higher_is_better);
        assert_eq!(by_key["gc_pause_p999_us"].unit, "us");

        // The gap `methodology/` asks for, in bytes and unscaled.
        assert_eq!(by_key["jvm_heap_configured_bytes"].unit, "bytes");
        assert!((by_key["jvm_heap_configured_bytes"].value - 268_435_456.0).abs() < f64::EPSILON);
        assert!((by_key["jvm_heap_live_peak_bytes"].value - 40_894_464.0).abs() < f64::EPSILON);

        // A control plane with no readable log leaves the headline intact.
        let alone = Gc {
            data_plane: Some(gc_summary(9_666.0)),
            control_plane: None,
        };
        let keys: Vec<String> = gc_metrics(&alone).into_iter().map(|(k, _)| k).collect();
        assert!(keys.iter().any(|k| k == "gc_pause_total_us"), "{keys:?}");
        assert!(
            !keys.iter().any(|k| k.starts_with("control_plane_")),
            "{keys:?}"
        );
    }

    /// A side of the heap comparison that was never observed stays absent. ZGC's
    /// pause lines carry no occupancy, so an arm on it has a configured heap and
    /// no committed one, and `jvm_heap_committed_peak_bytes: 0` would say the
    /// runtime committed nothing at all.
    #[test]
    fn a_heap_figure_the_collector_never_reported_is_absent_and_not_zero() {
        let mut summary = gc_summary(9_666.0);
        summary.peak_committed_bytes = None;
        summary.peak_live_bytes = None;
        let keys: Vec<String> = gc_metrics(&Gc {
            data_plane: Some(summary),
            control_plane: None,
        })
        .into_iter()
        .map(|(k, _)| k)
        .collect();

        assert!(keys.iter().any(|k| k == "jvm_heap_configured_bytes"));
        assert!(
            !keys.iter().any(|k| k.contains("committed_peak")),
            "{keys:?}"
        );
        assert!(!keys.iter().any(|k| k.contains("live_peak")), "{keys:?}");
    }

    /// An arm's footprint is what it held at one instant, not the sum of the
    /// moments its containers each peaked at.
    ///
    /// `SutCost::sum` adds the parts' maxima, which answers "how much could they
    /// have used between them" — an upper bound only reached if every container
    /// peaked simultaneously. The error over-reports the arm total, penalising
    /// exactly the multi-process arms the envelope rule goes out of its way not
    /// to penalise, on the one panel where a JVM already looks worst.
    #[test]
    fn an_arms_published_footprint_is_what_it_held_at_once() {
        let series = |t0: u64, anon: &[i64]| crate::sampler::Samples {
            meta: String::new(),
            rows: anon
                .iter()
                .enumerate()
                .map(|(i, &a)| crate::sampler::Sample {
                    t_ms: t0 + (i as u64) * 1000,
                    usage_usec: 0,
                    user_usec: 0,
                    system_usec: 0,
                    nr_throttled: 0,
                    throttled_usec: 0,
                    mem_current: a,
                    mem_peak: a,
                    anon: a,
                    file: 0,
                    slab: 0,
                    kernel_stack: 0,
                    sock: 0,
                })
                .collect(),
            wall_s: 3.0,
        };

        // A JobManager that spikes during job submission beside a TaskManager
        // that spikes late in the drain. Their maxima add to 300; the arm never
        // held more than 210 at once, and 210 is what the machine saw.
        let costs = vec![
            ("jm".to_owned(), series(1_000, &[100, 10, 10])),
            ("tm".to_owned(), series(1_000, &[10, 10, 200])),
        ];
        assert!((arm_peak_anon(&costs, 300.0) - 210.0).abs() < f64::EPSILON);

        // A single-container arm's peak is unchanged, which is the common case
        // and must not move.
        let one = vec![("sut".to_owned(), series(1_000, &[10, 900, 400]))];
        assert!((arm_peak_anon(&one, 900.0) - 900.0).abs() < f64::EPSILON);
    }
}
