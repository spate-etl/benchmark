//! The versioned record every measurement emits.
//!
//! One JSON object per line, appended to a file under `results/`. Forked from
//! `spate/benchmarks/src/report.rs` at `6f28a8b8912e` and immediately taken to
//! schema 2. **Fixes do not flow between the two copies.** That repository stays
//! on schema 1 for its two dozen self-comparison datasets, which have no system
//! under test, no environment registry and no comparability rules; sharing a
//! crate would force either a pointless migration there or a union schema
//! describing neither well.
//!
//! [`Metric`] carries its own `unit` and `higher_is_better`, which is the single
//! best property inherited from schema 1: a consumer plotting these records
//! cannot silently draw a lower-is-better quantity as a taller bar, because the
//! direction travels with the number rather than living in the plotting code.
//!
//! What schema 2 adds is provenance strong enough to publish:
//!
//! - [`Sut`] — *what was actually run*, including an image digest that is not
//!   optional, because version strings lie and digests do not.
//! - [`RunMeta::env_id`] — an interned hardware profile rather than a hostname.
//!   `Marcuss-MBP.kainth.co.uk` is not a hardware disclosure and cannot be
//!   compared across machines.
//! - [`RunMeta::harness_version`] / [`RunMeta::dataset_version`] — the two
//!   quantities that invalidate an entire result set.
//! - [`RunMeta::invocation_id`] — which sitting a record belongs to, so
//!   repetitions can be grouped exactly rather than by the day they landed on.
//! - [`Status`] — so "we ran it and it failed the headroom gate" is
//!   distinguishable from "we never ran it".

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Schema version of the emitted records. Bump on any breaking field change.
pub const SCHEMA_VERSION: u32 = 2;

/// Version of the measurement **protocol**.
///
/// Records with different values are not comparable, and the site refuses to
/// place them on one axis rather than averaging across the change.
///
/// Bump when the protocol changes in a way that **moves numbers**: the
/// definition of the measurement window, the drain protocol, sampler interval
/// semantics, the gate set, envelope enforcement. Do **not** bump for a log
/// message, a refactor, or a new field that no measurement depends on.
///
/// Hand-maintained rather than derived, deliberately. "Did this change move
/// numbers?" is a judgement; a content hash would answer yes to every typo fix
/// and shatter every comparability group in the archive. `methodology/`
/// carries a row per version and CI asserts the two stay in step.
pub const HARNESS_VERSION: u32 = 1;

/// Version of the **corpus**: the Avro schema, the ClickHouse DDL, and the
/// generator constants.
///
/// Derived rather than hand-maintained, because unlike the protocol this is
/// fully determined by files. `build.rs` hashes them, so a change to what the
/// data *is* cannot be made without the version moving.
pub const DATASET_VERSION: &str = env!("SPATE_BENCH_DATASET_VERSION");

/// Whether a record reports a measurement or a decision derived from them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// An observed quantity for one arm.
    Measurement,
    /// A conclusion drawn across arms — a ceiling pass, a go/no-go gate.
    Verdict,
}

/// Whether a record carries publishable numbers, and if not, why not.
///
/// Schema 1 had no counterpart: a refused arm called `exit(3)` and emitted
/// nothing, which was right when a free-text `note` was the only marker
/// available — a note cannot stop a consumer averaging the record in. With a
/// typed status that a loader filters on, the argument inverts: emitting nothing
/// makes "we ran Flink and it blew the headroom limit" indistinguishable from
/// "we never ran Flink", and the first of those is a finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Ran to completion and passed every gate. Publishable.
    Ok,
    /// Exceeded the infrastructure headroom limit. The number describes
    /// ClickHouse or the broker, not the system under test.
    InfraBound,
    /// The system cannot express this variant at all — no fan-out operator, no
    /// Native writer. Carries no metrics, and exists so the site can render an
    /// explicit gap rather than an absence a reader would read as "not tried".
    Unsupported,
    /// Started but produced no valid measurement. Reason in `note`.
    Failed,
}

impl Status {
    /// Whether this status permits metrics to be attached.
    #[must_use]
    pub fn carries_metrics(self) -> bool {
        matches!(self, Self::Ok | Self::InfraBound)
    }
}

/// What caused a run to happen. Recorded so a measurement taken for a reason
/// that bars publication can never be mistaken for a published one.
///
/// # Two of these mean "never published", and that is now enforced
///
/// [`Trigger::Pr`] has carried the words "Never published" since schema 2 was
/// written, and nothing set it and nothing refused it. A second field,
/// `Flag::PrRun`, carried the same words and was likewise set by nothing and
/// filtered by nothing. That is the state this type exists in the archive to
/// prevent: a vocabulary for an intention, with no enforcement anywhere, which
/// reads to the next person as though the rule is already in force.
///
/// `Flag::PrRun` is gone rather than wired, and the argument is that it restated
/// this field and added nothing. `trigger` is mandatory on every record and
/// typed; a flag derived from it is a second place to look and a second place to
/// forget, and the two could disagree. No committed record ever carried
/// `pr_run`, so removing it costs the archive nothing. What replaces both is
/// [`Trigger::bars_publication`] — one predicate, on the field that is already
/// required, checked by `validate::results_are_valid` so that committing such a
/// record fails the build.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Trigger {
    /// A scheduled full-matrix re-run.
    Nightly,
    /// Invoked by hand.
    Manual,
    /// Produced by a pull request, on code nobody has reviewed. Never published.
    Pr,
    /// A **configuration search**: dozens of measurements taken to find out
    /// which knob setting an arm should be published at. Never published.
    ///
    /// A distinct trigger rather than a reuse of [`Trigger::Pr`], and the reason
    /// is that they are different causes with different lifetimes. A PR run
    /// happens in CI on untrusted code; a tuning sweep happens on the reference
    /// rig, by hand, on code that is already committed, and its numbers are
    /// *good* numbers — they are simply not the arm's published configuration.
    /// Folding one into the other would leave `pr` meaning "unpublishable for
    /// some reason", which is what a status or a flag is for, and would make
    /// "which of these came out of CI?" unanswerable afterwards.
    ///
    /// It is not an addition to unenforced vocabulary, because the same change
    /// that added it enforced both members: `bench validate` refuses any record
    /// whose trigger bars publication, so the archive cannot contain one.
    ///
    /// What a search concludes on is a **descriptor**: the knob values it
    /// settled on, declared where the driver reads them, and re-measured as an
    /// ordinary run. A search left as fifty committed records says only "here
    /// are fifty numbers", and invites exactly the failure this suite cannot
    /// afford — running until the number is liked, and then recording that one.
    Tuning,
    /// Pinned to a release of the system under test.
    Release,
}

impl Trigger {
    /// The marker a record produced under this trigger must carry, if any.
    ///
    /// Shaped like `Environment::publication_bar` on purpose: the driver
    /// prefixes the record's `note` with whichever of the two applies, so a
    /// record that must never be published says so in its own prose as well as
    /// in a typed field, and a person reading one line of JSONL sees it without
    /// knowing the schema.
    #[must_use]
    pub fn publication_bar(self) -> Option<&'static str> {
        match self {
            Self::Pr => Some("pull-request run on unreviewed code, never published"),
            Self::Tuning => Some(
                "TUNING RUN: a point in a configuration search, never published — the \
                 search belongs in a document with its rejected points, not in results/",
            ),
            Self::Nightly | Self::Manual | Self::Release => None,
        }
    }

    /// Whether a record produced under this trigger may reach `results/`.
    ///
    /// The single authority, in the same sense [`Status::carries_metrics`] is
    /// one: the validator, the driver's banner and the driver's note prefix all
    /// ask this rather than each matching on the variants, so a trigger added
    /// later cannot be unpublishable in one of the three and publishable in the
    /// other two.
    #[must_use]
    pub fn bars_publication(self) -> bool {
        self.publication_bar().is_some()
    }
}

/// A machine-readable caveat. `note` is prose for humans; these are filterable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Flag {
    /// The arm hit its cgroup CPU cap during the measurement window.
    CpuCapThrottled,
    /// No ceiling measurement was available to gate against, or no ceiling
    /// applies to this arm's insert format.
    ///
    /// Both halves matter, and the second was added when the ClickHouse ingest
    /// ceiling arrived. That ceiling is measured **per insert
    /// format**, because Native, RowBinary and `JSONEachRow` are not the same
    /// amount of server-side work; an arm whose format has no measured figure is
    /// therefore deliberately not gated against the target at all rather than
    /// gated against another format's number. See `crate::ceiling`.
    ///
    /// The flag exists so that "we did not gate this arm" can never be read as
    /// "this arm cleared the gate". A record whose status is [`Status::Ok`] and
    /// whose share sat comfortably below the limit against the one ceiling that
    /// was checked is a different claim from one that was held to every ceiling
    /// `methodology/` names, and a consumer has to be able to tell them apart
    /// without reading prose.
    HeadroomUnproven,
    /// A sustained arm could not keep up with the offered rate.
    ///
    /// A flag and not a [`Status`], deliberately. `methodology/` is explicit
    /// that such a point "is a genuine ceiling measurement": the arm consumed at
    /// its capacity for the whole window, and its throughput, CPU and footprint
    /// figures are exactly as sound as any other sustained record's. Demoting it
    /// to [`Status::Failed`] would throw away the one number a saturating host
    /// is best placed to produce.
    ///
    /// What is *not* sound is its latency. Once the arm is behind, the gap
    /// between a row's scheduled send time and its `ingest_ts` is dominated by
    /// how far the backlog had grown by then, so the distribution describes
    /// backlog age and grows without bound for as long as the window is held —
    /// it is a property of how long we ran the experiment, not of the pipeline.
    /// The driver therefore does three things at once and none of them alone:
    /// it sets this flag, it leads the record's `note` with the warning, and it
    /// publishes the figures under `backlog_age_*` metric keys instead of
    /// `latency_*`. The third is the one that cannot be ignored by accident — a
    /// consumer plotting `latency_p99_us` finds no such metric on a saturated
    /// record rather than finding a number it has no reason to distrust, and a
    /// median taken across repetitions cannot mix the two quantities under one
    /// name. See `driver::Sustained`.
    Saturated,
    /// Produced on hardware we do not control.
    ThirdPartyHardware,
    /// Produced in an environment whose class bars publication — a `fixture`
    /// profile, whose data is synthetic.
    ///
    /// Distinct from [`Flag::ThirdPartyHardware`], which is a claim about
    /// *hardware we do not control*. A fixture run happens on our own machine
    /// and the objection to it is the data, not the host. The driver used to
    /// conflate the two: it set `ThirdPartyHardware` on fixture runs and on
    /// nothing else, so the one flag meaning "not our hardware" was applied
    /// exclusively to runs on our hardware, and the record made a false
    /// provenance claim in place of a true worthlessness claim.
    UnpublishableEnvironment,
    /// Infrastructure containers were reused rather than recreated.
    ReusedInfra,
    /// The measurement window fell below the floor the protocol declares.
    ///
    /// A drain's window is `corpus / throughput`, so it shrinks as arms get
    /// faster and has no lower bound of its own. Both ends of the CPU delta are
    /// sampler readings while the row count is the whole corpus, so a short
    /// window reads flatteringly low. `window_resolution` on the same record
    /// says what it was read at.
    ShortWindow,
}

/// One measured quantity, carrying its unit and its direction of goodness.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    /// The measured value, in `unit`.
    pub value: f64,
    /// Unit of `value`. Constrained to a known set by `results_are_valid`.
    pub unit: String,
    /// `true` when a larger `value` is a better result.
    pub higher_is_better: bool,
    /// 95% confidence interval `(low, high)` when repetitions were taken.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci95: Option<(f64, f64)>,
    /// Sample count behind `value` (repetitions, not inner iterations).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub n: Option<u64>,
}

impl Metric {
    /// A metric where more is better — throughput, rows written.
    pub fn maximize(value: f64, unit: impl Into<String>) -> Self {
        Self {
            value,
            unit: unit.into(),
            higher_is_better: true,
            ci95: None,
            n: None,
        }
    }

    /// A metric where less is better — latency, CPU per row, bytes resident.
    pub fn minimize(value: f64, unit: impl Into<String>) -> Self {
        Self {
            value,
            unit: unit.into(),
            higher_is_better: false,
            ci95: None,
            n: None,
        }
    }

    /// A footprint in **bytes**, unscaled.
    ///
    /// This helper exists because its absence caused a real defect. The previous
    /// harness emitted `peak_anon_mb` holding megabytes while tagging the unit
    /// `"bytes"`; the site's formatter, correctly trusting the unit, divided by
    /// 1e6 again and rendered 1010 MB as "1.0 KB". The value and its label must
    /// be produced together or they drift.
    ///
    /// Scaling for display is the consumer's job — it is the only party that
    /// knows how much space it has.
    pub fn bytes(bytes: f64) -> Self {
        Self::minimize(bytes, "bytes")
    }

    /// A byte throughput, recorded as `MB/s` in the SI sense — 10^6 bytes, not
    /// 2^20. One divisor, so two rigs cannot drift onto different conventions
    /// while emitting the same unit string.
    pub fn bytes_per_s(bytes_per_s: f64) -> Self {
        Self::maximize(bytes_per_s / 1e6, "MB/s")
    }

    /// A dimensionless **fraction**, where 1.0 is the whole — `kept_up_share`
    /// being the first of them.
    ///
    /// A ratio still needs a unit string, and it needs its own rather than
    /// borrowing one, for the reason `ALLOWED_UNITS` exists: the unit is the
    /// only thing that tells a consumer what to do with a number, and the site's
    /// formatter branches on it. A share tagged `"records/s"` because that was
    /// the nearest allowed string would be rendered with an SI suffix and read
    /// as a rate; a share left untagged would fall through whichever branch a
    /// formatter ends on. That seam has already produced one published defect —
    /// a value in megabytes tagged `"bytes"` rendered 1010 MB as "1.0 KB" — and
    /// [`Metric::bytes`] exists because of it.
    ///
    /// **Never scaled by a consumer.** `0.62` means 62%, and a formatter that
    /// multiplies it by anything is wrong. Rendering it as a percentage is a
    /// display choice; changing the stored value is not.
    ///
    /// `higher_is_better`, because these are shares of a target that is being
    /// aimed at rather than avoided. A value above 1.0 is legitimate and is not
    /// clamped: for `kept_up_share` it means the arm consumed faster than the
    /// producer offered over the window, i.e. it was clearing a backlog, which
    /// is information a reader wants rather than an error to be tidied to 1.0.
    pub fn share(share: f64) -> Self {
        Self::maximize(share, "ratio")
    }

    /// Attaches a 95% confidence interval.
    #[must_use]
    pub fn with_ci(mut self, low: f64, high: f64) -> Self {
        self.ci95 = Some((low, high));
        self
    }

    /// Attaches the repetition count behind the value.
    #[must_use]
    pub fn with_n(mut self, n: u64) -> Self {
        self.n = Some(n);
        self
    }
}

/// The system under test: what was **actually** run.
///
/// Every field here is resolved at run time from the descriptor plus runtime
/// interrogation of the image and process. None of it is typed by a human into a
/// results file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Sut {
    /// Entrant directory name — joins to `entrants/<id>/`.
    pub entrant: String,
    /// Variant id from the descriptor. Stable for the life of the entrant.
    pub variant_id: String,
    /// Released version, resolved by the descriptor's `[version].strategy`.
    ///
    /// `None` only when the system has no release concept, in which case
    /// `commit` must be present — asserted by `results_are_valid`. Between them
    /// they discharge the requirement that every published number says what was
    /// measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Upstream commit, where there is no release or alongside a pre-release.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    /// `sha256:…` of the image actually run, from `docker inspect`.
    ///
    /// Not an `Option`, deliberately. Version strings lie — a tag can be
    /// re-pushed under the same name — and an `Option` here would invite a code
    /// path that skips it on the day it is least convenient. If the digest
    /// cannot be read the run is [`Status::Failed`].
    pub image_digest: String,
    /// The image tag the driver was told to run. Human orientation only.
    pub image: String,
    /// Compiler or runtime that built the arm (`rustc 1.97.0`,
    /// `temurin-17.0.19+10`). Codegen moves throughput, so this is part of a
    /// number's provenance rather than trivia.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub toolchain: Option<String>,
}

/// The shared infrastructure, as **read back** from the running containers.
///
/// Every field is observed, never taken from the request that created it. The
/// previous harness asked for one envelope, warned when it got another, and
/// carried on — which is how three different infrastructure envelopes ended up
/// in one results file with nothing in the records to say which was in force.
/// Two components cannot disagree if only one of them is allowed to speak.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Infra {
    /// Stable hash over the envelope-defining subset only — cpus, memory,
    /// partitions, broker family, storage layout. Deliberately excludes
    /// versions, so a ClickHouse patch release does not split a comparability
    /// group.
    pub digest: String,
    /// Broker family, e.g. `redpanda`.
    pub broker: String,
    /// Broker version, read from the broker.
    pub broker_version: String,
    /// `sha256:…` of the broker image.
    pub broker_image_digest: String,
    /// Broker CPU quota, from the container's cgroup `cpu.max`.
    pub broker_cpus: String,
    /// Broker memory limit, from the container's cgroup `memory.max`.
    pub broker_memory: String,
    /// ClickHouse version, from `SELECT version()`.
    pub clickhouse_version: String,
    /// `sha256:…` of the ClickHouse image.
    pub clickhouse_image_digest: String,
    /// ClickHouse CPU quota, from cgroup `cpu.max`.
    pub clickhouse_cpus: String,
    /// ClickHouse memory limit, from cgroup `memory.max`.
    pub clickhouse_memory: String,
    /// Topic partition count.
    pub partitions: i32,
    /// What the measured data paths sat on: `shared-root` or `local-nvme`.
    /// Part of `digest` above.
    ///
    /// Additive: [`SCHEMA_VERSION`] stays at 2 and this is `#[serde(default)]`,
    /// so a record without the field still deserialises, with an empty string
    /// meaning "not stated".
    #[serde(default)]
    pub storage: String,
    /// Schema Registry implementation, e.g. `redpanda-builtin`.
    pub registry: String,
    /// The measured consume ceiling this run was gated against, in **messages**
    /// per second. `0` means no ceiling was available, which sets
    /// [`Flag::HeadroomUnproven`].
    pub ceiling_msgs_per_s: u64,
    /// The same consume ceiling in **bytes** per second.
    ///
    /// Both, because neither transfers on its own. A messages-per-second figure
    /// does not survive a change of message size, and `crate::ceiling` records
    /// what that cost: a ceiling measured against 840-byte messages was kept as
    /// the denominator after the corpus grew to 4056, which asserted a byte rate
    /// 4.8x the one the rig had actually sustained. A byte rate alone cannot be
    /// compared against an arm whose work is per message. A record carrying only
    /// one of the two cannot be audited for that mistake after the fact, which
    /// is exactly the position the archive was in when it was found.
    ///
    /// Additive: [`SCHEMA_VERSION`] stays at 2 and this is `#[serde(default)]`,
    /// so every record already committed still deserialises, with a zero a
    /// consumer reads as "not stated".
    #[serde(default)]
    pub ceiling_bytes_per_s: u64,
    /// The ClickHouse ingest ceiling this arm was actually gated against, in
    /// rows per second. `0` means none applies to its insert format.
    ///
    /// Per **arm** rather than per run, unlike every other field on this type,
    /// and the exception is forced by what the ceiling is: it is measured per
    /// insert format, so a single figure for the whole sweep would
    /// gate every arm against work it does not do — and would err leniently for
    /// exactly the arms whose format is cheapest server-side. `crate::infra`
    /// therefore leaves this zero, because bringing infrastructure up happens
    /// before any arm is known, and `driver::measure` fills it in on each
    /// record, where the arm's declared `wire_format` is.
    ///
    /// `0` is "not gated against the target" — a visible gap rather than a
    /// cleared gate, and the same condition that raises
    /// [`Flag::HeadroomUnproven`].
    ///
    /// Additive on the same terms as [`Self::ceiling_bytes_per_s`].
    #[serde(default)]
    pub ceiling_rows_per_s: u64,
}

/// Provenance for a run: when, where, under what protocol, on what data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunMeta {
    /// Unix epoch milliseconds at which the record was built.
    pub ts_ms: u64,
    /// The `bench run` invocation that produced this record — one UUIDv7 minted
    /// per sweep and written identically onto every record that sweep appends.
    ///
    /// A sitting is the unit repetitions belong to: three repetitions of one arm
    /// are comparable because one invocation produced them under one set of
    /// conditions. Nothing in schema 2 said so, and the site approximated it by
    /// UTC calendar day — which splits a sweep that straddles midnight into two
    /// published rows, and merges two sweeps run on the same day into one row
    /// captioned as run-to-run spread. Both are silent, and the second is the
    /// worse of the two because it presents a configuration change as noise.
    ///
    /// Additive: [`SCHEMA_VERSION`] stays at 2 and this is `#[serde(default)]`,
    /// so every record already committed still deserialises, with an empty id
    /// that a consumer reads as "not stated" and falls back to the old
    /// approximation for.
    #[serde(default)]
    pub invocation_id: String,
    /// Interned hardware profile — the file stem in `environments/`.
    pub env_id: String,
    /// Content hash of the resolved environment profile, so a later edit to
    /// `environments/<env_id>.toml` cannot retroactively re-describe old runs.
    pub env_digest: String,
    /// Measurement protocol version. See [`HARNESS_VERSION`].
    pub harness_version: u32,
    /// Corpus version. See [`DATASET_VERSION`].
    pub dataset_version: String,
    /// Commit of *this* repository, for reproducing the run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit: Option<String>,
    /// What caused the run.
    pub trigger: Trigger,
    /// Shared infrastructure, read back from the running containers.
    pub infra: Infra,
}

/// The static half of [`RunMeta`], resolved once per process.
struct StaticMeta {
    commit: Option<String>,
    invocation_id: String,
}

/// Provenance that is fixed for the life of the process.
///
/// The invocation id is minted here rather than passed down from `bench run`
/// deliberately. One process is one sweep, so a `OnceLock` makes it impossible
/// for two records of the same invocation to disagree and impossible for a new
/// call site to forget to thread it through — which is the only way this field
/// can fail, and it fails silently.
///
/// UUIDv7 rather than v4 so that sittings sort by time in the same way
/// [`Report::run_id`] does.
fn static_meta() -> &'static StaticMeta {
    static META: OnceLock<StaticMeta> = OnceLock::new();
    META.get_or_init(|| StaticMeta {
        commit: detect_commit(),
        invocation_id: uuid::Uuid::now_v7().to_string(),
    })
}

fn trimmed_stdout(cmd: &str, args: &[&str]) -> Option<String> {
    let out = std::process::Command::new(cmd).args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8_lossy(&out.stdout).trim().to_owned();
    (!s.is_empty()).then_some(s)
}

fn detect_commit() -> Option<String> {
    if let Ok(c) = std::env::var("GIT_COMMIT")
        && !c.is_empty()
    {
        return Some(c);
    }
    trimmed_stdout("git", &["rev-parse", "--short=12", "HEAD"])
}

/// Milliseconds since the Unix epoch.
#[must_use]
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

impl RunMeta {
    /// Builds run provenance around an environment and the infrastructure that
    /// was observed for it.
    pub fn new(
        env_id: impl Into<String>,
        env_digest: impl Into<String>,
        trigger: Trigger,
        infra: Infra,
    ) -> Self {
        Self {
            ts_ms: now_ms(),
            invocation_id: static_meta().invocation_id.clone(),
            env_id: env_id.into(),
            env_digest: env_digest.into(),
            harness_version: HARNESS_VERSION,
            dataset_version: DATASET_VERSION.to_owned(),
            commit: static_meta().commit.clone(),
            trigger,
            infra,
        }
    }
}

/// One emitted measurement record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Report {
    /// Schema version; always [`SCHEMA_VERSION`] on write.
    pub schema: u32,
    /// The suite: `kafka_avro_clickhouse` for the workload, `ceiling` for the
    /// infrastructure characterisation pass.
    pub bench: String,
    /// Measurement or verdict.
    pub kind: Kind,
    /// Whether this record carries publishable numbers.
    pub status: Status,
    /// UUIDv7 — time-ordered, so sorting by id sorts by time. One per
    /// (entrant, variant, rep) execution; never repeats.
    pub run_id: String,
    /// 1-based repetition index within one `bench run` invocation.
    pub rep: u32,
    /// Repetitions the invocation asked for, so a reader can see that rep 2 of 3
    /// is *missing* rather than having to infer it from a gap.
    pub reps: u32,
    /// What was actually run.
    pub sut: Sut,
    /// Provenance of the run.
    pub run: RunMeta,
    /// The arm's configuration. Never a measured quantity: two records sharing a
    /// variant identity are repetitions and may be aggregated.
    pub variant: BTreeMap<String, Value>,
    /// Measured quantities, keyed by metric name.
    pub metrics: BTreeMap<String, Metric>,
    /// Free-text caveat carried alongside the numbers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Machine-readable caveats.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub flags: Vec<Flag>,
}

impl Report {
    /// A new record for one arm of one repetition.
    pub fn new(
        bench: impl Into<String>,
        kind: Kind,
        status: Status,
        sut: Sut,
        run: RunMeta,
    ) -> Self {
        Self {
            schema: SCHEMA_VERSION,
            bench: bench.into(),
            kind,
            status,
            run_id: uuid::Uuid::now_v7().to_string(),
            rep: 1,
            reps: 1,
            sut,
            run,
            variant: BTreeMap::new(),
            metrics: BTreeMap::new(),
            note: None,
            flags: Vec::new(),
        }
    }

    /// Records which repetition of how many this is.
    #[must_use]
    pub fn rep(mut self, rep: u32, reps: u32) -> Self {
        self.rep = rep;
        self.reps = reps;
        self
    }

    /// Adds one dimension of the arm's configuration.
    #[must_use]
    pub fn variant(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.variant.insert(key.into(), value.into());
        self
    }

    /// Adds one measured quantity.
    #[must_use]
    pub fn metric(mut self, key: impl Into<String>, metric: Metric) -> Self {
        self.metrics.insert(key.into(), metric);
        self
    }

    /// Attaches a caveat that travels with the numbers.
    #[must_use]
    pub fn note(mut self, note: impl Into<String>) -> Self {
        self.note = Some(note.into());
        self
    }

    /// Attaches a machine-readable caveat, idempotently.
    #[must_use]
    pub fn flag(mut self, flag: Flag) -> Self {
        if !self.flags.contains(&flag) {
            self.flags.push(flag);
        }
        self
    }

    /// Serializes to the single JSON line that goes into a results file.
    ///
    /// # Errors
    ///
    /// Returns the underlying `serde_json` error if the record cannot be
    /// serialized.
    pub fn to_line(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sut() -> Sut {
        Sut {
            entrant: "spate".to_owned(),
            variant_id: "native".to_owned(),
            version: Some("0.1.0-dev".to_owned()),
            commit: Some("6f28a8b8912e".to_owned()),
            image_digest: format!("sha256:{}", "a".repeat(64)),
            image: "spate-bench-spate".to_owned(),
            toolchain: Some("rustc 1.97.0".to_owned()),
        }
    }

    fn infra() -> Infra {
        Infra {
            digest: "e3b0c44298fc".to_owned(),
            broker: "redpanda".to_owned(),
            broker_version: "v26.1.13".to_owned(),
            broker_image_digest: format!("sha256:{}", "b".repeat(64)),
            broker_cpus: "800000 100000".to_owned(),
            broker_memory: "8589934592".to_owned(),
            clickhouse_version: "26.3.1.1".to_owned(),
            clickhouse_image_digest: format!("sha256:{}", "c".repeat(64)),
            clickhouse_cpus: "500000 100000".to_owned(),
            clickhouse_memory: "12884901888".to_owned(),
            partitions: 8,
            storage: "local-nvme".to_owned(),
            registry: "redpanda-builtin".to_owned(),
            ceiling_msgs_per_s: 305_554,
            ceiling_bytes_per_s: 256_700_000,
            ceiling_rows_per_s: 3_100_000,
        }
    }

    fn report() -> Report {
        Report::new(
            "kafka_avro_clickhouse",
            Kind::Measurement,
            Status::Ok,
            sut(),
            RunMeta::new("test-env", "deadbeef", Trigger::Manual, infra()),
        )
    }

    #[test]
    fn round_trips_through_json_on_one_line() {
        let rep = report()
            .rep(2, 3)
            .variant("format", "native")
            .metric("rows_per_s", Metric::maximize(4_383_663.0, "records/s"))
            .metric("cpu_us_per_row", Metric::minimize(0.6187, "us").with_n(3))
            .flag(Flag::CpuCapThrottled)
            .note("330 sampler samples");

        let line = rep.to_line().expect("serialize");
        assert!(!line.contains('\n'), "a record must be one JSON line");

        let back: Report = serde_json::from_str(&line).expect("deserialize");
        assert_eq!(back, rep);
        assert_eq!(back.schema, 2);
        assert_eq!(back.run.harness_version, HARNESS_VERSION);
    }

    #[test]
    fn optional_fields_are_omitted_when_absent() {
        let line = report().to_line().expect("serialize");
        assert!(!line.contains("note"), "{line}");
        assert!(!line.contains("flags"), "{line}");
        assert!(line.contains(r#""status":"ok""#), "{line}");
    }

    /// A sitting has to be identifiable exactly. Every record one invocation
    /// writes carries the same id, so a consumer can group repetitions without
    /// approximating the sweep by the calendar day it landed on.
    #[test]
    fn every_record_of_one_invocation_carries_the_same_invocation_id() {
        let a = report();
        let b = report();
        assert!(!a.run.invocation_id.is_empty());
        assert_eq!(a.run.invocation_id, b.run.invocation_id);
        // Distinct from the per-record id, which must still differ.
        assert_ne!(a.run_id, b.run_id);
        assert_ne!(a.run.invocation_id, a.run_id);
    }

    /// The additive half of the change: `SCHEMA_VERSION` stays at 2, so a record
    /// written before the field existed must still load — as "not stated"
    /// rather than as a parse error that would make the archive unreadable.
    #[test]
    fn a_record_written_without_an_invocation_id_still_loads() {
        let line = report().to_line().expect("serialize");
        let mut v: Value = serde_json::from_str(&line).expect("parse");
        v.get_mut("run")
            .and_then(|r| r.as_object_mut())
            .expect("a record carries a run object")
            .remove("invocation_id")
            .expect("the field was written");
        let older = serde_json::to_string(&v).expect("re-serialize");

        let back: Report = serde_json::from_str(&older).expect("an older record deserialises");
        assert!(back.run.invocation_id.is_empty());
    }

    /// The second consume ceiling and the ClickHouse one are additions to a
    /// schema that stays at 2, so the eight-arm dataset already committed has to
    /// keep loading — as "not stated" rather than as a parse error that would
    /// make the archive unreadable.
    ///
    /// A zero read as "not stated" is safe here for the same reason it is safe
    /// on a live record: zero already means "not gated against this ceiling",
    /// which is the conservative reading, and [`Flag::HeadroomUnproven`] is what
    /// distinguishes it from a gate that was cleared.
    #[test]
    fn a_record_written_before_the_second_ceiling_existed_still_loads() {
        let line = report().to_line().expect("serialize");
        let mut v: Value = serde_json::from_str(&line).expect("parse");
        let infra = v
            .get_mut("run")
            .and_then(|r| r.get_mut("infra"))
            .and_then(|i| i.as_object_mut())
            .expect("a record carries an infra object");
        infra
            .remove("ceiling_bytes_per_s")
            .expect("the field was written");
        infra
            .remove("ceiling_rows_per_s")
            .expect("the field was written");
        let older = serde_json::to_string(&v).expect("re-serialize");

        let back: Report = serde_json::from_str(&older).expect("an older record deserialises");
        assert_eq!(back.run.infra.ceiling_bytes_per_s, 0);
        assert_eq!(back.run.infra.ceiling_rows_per_s, 0);
        // The figure that was always there is untouched, so an old record is
        // still gated-against-something rather than gated-against-nothing.
        assert_eq!(back.run.infra.ceiling_msgs_per_s, 305_554);
    }

    #[test]
    fn run_ids_are_unique_and_time_ordered() {
        // UUIDv7 sorts lexicographically by creation time, which is what lets a
        // results file be scanned in order without parsing timestamps.
        let a = report().run_id;
        let b = report().run_id;
        assert_ne!(a, b);
        assert!(a < b, "v7 ids must sort by time: {a} !< {b}");
    }

    #[test]
    fn footprint_bytes_are_unscaled_and_labelled_bytes() {
        // The regression this helper exists for: a value in megabytes tagged
        // "bytes" renders 1010 MB as "1.0 KB" in a consumer that trusts the unit.
        let m = Metric::bytes(1_059_481_600.0);
        assert_eq!(m.unit, "bytes");
        assert!(!m.higher_is_better);
        assert!(m.value > 1e9, "must be raw bytes, got {}", m.value);
    }

    #[test]
    fn byte_rates_are_si_megabytes() {
        let m = Metric::bytes_per_s(1_048_576.0);
        assert_eq!(m.unit, "MB/s");
        assert!((m.value - 1.048576).abs() < 1e-12);
    }

    /// A dimensionless share still carries a unit of its own, because the unit
    /// is what a consumer's formatter branches on. `validate::ALLOWED_UNITS` has
    /// to carry `"ratio"` or every sustained record fails validation — that
    /// coupling is deliberate and is why adding a unit is a deliberate act.
    #[test]
    fn a_share_is_a_fraction_of_one_and_is_never_rescaled() {
        let m = Metric::share(0.62);
        assert_eq!(m.unit, "ratio");
        assert!(m.higher_is_better, "a share of a target is aimed at");
        assert!((m.value - 0.62).abs() < f64::EPSILON, "{}", m.value);

        // Not clamped at 1.0. An arm that cleared a backlog inside the window
        // consumed more than was offered over it, and saying so is information
        // rather than an error to be tidied away.
        assert!((Metric::share(1.04).value - 1.04).abs() < f64::EPSILON);
    }

    #[test]
    fn direction_travels_with_the_number() {
        assert!(Metric::maximize(1.0, "records/s").higher_is_better);
        assert!(!Metric::minimize(1.0, "ns").higher_is_better);
    }

    /// The predicate the validator, the driver's banner and the driver's note
    /// prefix all defer to. Enumerated exhaustively rather than spot-checked, so
    /// that a trigger added later has to decide which side of the line it is on
    /// here rather than defaulting to publishable by omission.
    #[test]
    fn a_trigger_that_bars_publication_says_so_in_exactly_one_place() {
        for t in [Trigger::Pr, Trigger::Tuning] {
            assert!(t.bars_publication(), "{t:?}");
            assert!(
                t.publication_bar().is_some_and(|b| !b.trim().is_empty()),
                "{t:?} bars publication but says nothing a reader could act on"
            );
        }
        for t in [Trigger::Nightly, Trigger::Manual, Trigger::Release] {
            assert!(!t.bars_publication(), "{t:?}");
            assert_eq!(t.publication_bar(), None, "{t:?}");
        }
    }

    /// A tuning sweep is not a pull-request run, and the record has to be able to
    /// say which it was. Conflating them would leave `pr` meaning "unpublishable
    /// for some reason" and make "which of these came out of CI?" unanswerable.
    #[test]
    fn a_tuning_run_and_a_pull_request_run_are_distinguishable_in_the_record() {
        let tuning = serde_json::to_string(&Trigger::Tuning).expect("serialize");
        let pr = serde_json::to_string(&Trigger::Pr).expect("serialize");
        assert_eq!(tuning, r#""tuning""#);
        assert_ne!(tuning, pr);

        let back: Trigger = serde_json::from_str(&tuning).expect("deserialize");
        assert_eq!(back, Trigger::Tuning);
    }

    #[test]
    fn only_successful_statuses_carry_metrics() {
        assert!(Status::Ok.carries_metrics());
        assert!(Status::InfraBound.carries_metrics());
        assert!(!Status::Failed.carries_metrics());
        assert!(!Status::Unsupported.carries_metrics());
    }
}
