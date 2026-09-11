//! The entrant contract: one TOML file per system, and nothing else to edit.
//!
//! There is deliberately **no central registry**. The driver and the site both
//! enumerate `entrants/*/entrant.toml` and derive every filter facet from the
//! union of what they find. That is the property worth protecting as the number
//! of systems grows: adding entrant N+1 touches one new directory and zero
//! shared files, so two concurrent contributions conflict in nothing. The moment
//! a shared list exists, every entrant pull request conflicts with every other
//! one, and the marginal cost of a system stops being constant.
//!
//! TOML rather than JSON because the most valuable content in a competitor's
//! configuration is the *reasons*, and a format without comments loses them.
//! Rather than YAML because there is exactly one way to write an array of tables,
//! no significant whitespace to change meaning under copy-paste, and no
//! `DESER: no` parsing as a boolean.
//!
//! Everything the site would otherwise hardcode — label, ordering, colour, "is
//! this ours" — lives in `[display]` and `[entrant]`, so the rendering code never
//! needs to know that any particular system exists.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Descriptor schema version, independent of the result schema.
pub const DESCRIPTOR_SCHEMA: u32 = 1;

/// A parsed `entrant.toml`, plus where it was found.
#[derive(Debug, Clone)]
pub struct Entrant {
    /// Directory holding the descriptor.
    pub dir: PathBuf,
    /// The descriptor itself.
    pub spec: Spec,
}

/// The descriptor's contents.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    /// Descriptor schema version.
    pub schema: u32,
    /// Identity and classification.
    pub entrant: Identity,
    /// Who to contact, and whether the config has been reviewed upstream.
    pub maintainer: Maintainer,
    /// Presentation metadata, so the site hardcodes none.
    pub display: Display,
    /// How to learn what version actually ran. Absent for `planned` entrants.
    #[serde(default)]
    pub version: Option<VersionSource>,
    /// How to build the image. Absent for `planned` entrants.
    #[serde(default)]
    pub build: Option<Build>,
    /// The resource envelope. Absent for `planned` entrants.
    #[serde(default)]
    pub envelope: Option<Envelope>,
    /// Delivery semantics, published beside the numbers.
    #[serde(default)]
    pub guarantees: Option<Guarantees>,
    /// Named docker volumes. Never host bind mounts.
    #[serde(default)]
    pub volumes: Option<Volumes>,
    /// Environment passed to the container, in the entrant's own vocabulary.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// The published arms this entrant provides.
    #[serde(default)]
    pub variants: Vec<Variant>,
    /// Knob combinations the arm cannot run, declared so a sweep is refused
    /// before it starts a container.
    #[serde(default, rename = "constraints")]
    pub constraints: Vec<Constraint>,
    /// Rule 4: every deviation from the common shape, declared where the site
    /// can render it.
    #[serde(default, rename = "deviations")]
    pub deviations: Vec<Deviation>,
    /// Arm-specific ClickHouse objects and attribution facts.
    #[serde(default)]
    pub clickhouse: Option<ClickhouseSpec>,
    /// Why an entrant is not yet built.
    #[serde(default)]
    pub planned: Option<Planned>,
}

/// Identity and the facets the site filters on.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Identity {
    /// Must equal the directory name.
    pub id: String,
    /// Display name.
    pub name: String,
    #[serde(default)]
    pub homepage: String,
    #[serde(default)]
    pub repo: String,
    #[serde(default)]
    pub docs: String,
    /// SPDX identifier.
    pub licence: String,
    /// Who publishes the system. `self` marks our own entry and is what drives
    /// the vendor-run disclosure on the site.
    pub vendor: String,
    /// Broad category, e.g. `stream-processor`.
    pub kind: String,
    /// Implementation languages. An array because polyglot systems are real.
    pub language: Vec<String>,
    /// `native`, `jvm`, `go`, … — a genuine grouping axis for these results.
    pub runtime: String,
    /// Lifecycle.
    pub status: Status,
}

/// Whether an entrant is measured, planned, or retired.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Built and measured.
    Active,
    /// Declared but not yet implemented. Validation is relaxed.
    Planned,
    /// Was measured; no longer re-run. Results stay published and visible.
    Historical,
    /// Removed at the maintainer's request or because it no longer applies.
    Withdrawn,
}

impl Status {
    /// Whether this entrant must carry a buildable, runnable definition.
    #[must_use]
    pub fn is_runnable(self) -> bool {
        matches!(self, Self::Active | Self::Historical)
    }
}

/// Contact and upstream-review state.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Maintainer {
    /// `spate-benchmark` means we wrote the arm; otherwise a contact.
    pub who: String,
    /// Rule 7: has the configuration been shown to the project?
    pub reviewed_upstream: bool,
    #[serde(default)]
    pub review_url: String,
}

/// Presentation metadata. The site reads this instead of hardcoding anything.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Display {
    /// Stable sort key for equal-ranked rows.
    pub order: i64,
    /// Hue **angle** in degrees, not a hex colour: the site owns contrast in
    /// both themes, and a supplied hex would fail one of them — while a site
    /// that overrode it would make the field a lie.
    pub hue: u16,
    /// Short label for dense layouts.
    pub short: String,
}

/// The `[version].strategy` values `driver::resolve_sut` implements.
///
/// Checked at validation rather than left to fail at run time. A descriptor
/// naming a strategy nobody wrote resolves no version at all, and the only
/// symptom is a refusal after the image has been built and the sweep has
/// started — which is the most expensive moment to learn about a typo.
///
/// `image-label` used to be listed here and was never implemented.
/// `entrants/flink/entrant.toml` records why it should not be: `flink:2.2.1`
/// carries no Flink version label, only `org.opencontainers.image.version:
/// 24.04` for the base operating system, and a descriptor trusting that label
/// would have recorded every Flink result as version 24.04 entirely plausibly.
/// `pinned` was listed too, and is not a strategy but an assertion — a version a
/// human typed into a descriptor is not evidence of what ran.
pub const VERSION_STRATEGIES: [&str; 1] = ["command"];

/// How to discover what version actually ran.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VersionSource {
    /// How the version is resolved. One of [`VERSION_STRATEGIES`].
    pub strategy: String,
    /// For `command`: argv executed inside the image.
    #[serde(default)]
    pub command: Vec<String>,
    /// Expected value. Asserted against what is resolved; a mismatch refuses the
    /// run rather than publishing a mislabelled number.
    #[serde(default)]
    pub pinned: String,
}

/// How to build the entrant's image.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Build {
    /// Build context, relative to the entrant directory.
    pub context: String,
    /// Dockerfile, relative to the entrant directory.
    pub dockerfile: String,
    /// Local image tag. The **digest** is what gets recorded.
    pub image: String,
    /// BuildKit secret ids the build needs.
    #[serde(default)]
    pub secrets: Vec<String>,
}

/// The resource envelope, and how it splits across containers.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    /// Data-plane CPU total.
    pub cpus: String,
    /// Data-plane memory total.
    pub memory: String,
    /// The containers this entrant runs.
    #[serde(default, rename = "container")]
    pub containers: Vec<Container>,
}

/// One container of a possibly multi-container arm.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Container {
    /// `data-plane` or `control-plane`.
    pub role: Role,
    /// Suffix for the container name.
    pub name: String,
    /// CPU quota.
    pub cpus: String,
    /// Memory limit. Swap is pinned equal to it by the driver.
    pub memory: String,
    /// Command arguments, for images that dispatch on argv.
    #[serde(default)]
    pub args: Vec<String>,
    /// Where this container's JVM writes its GC log, for `docker cp`.
    ///
    /// Descriptor knowledge, not harness knowledge: the path is set by the
    /// entrant's own configuration (Flink's `env.java.opts.*`, Connect's
    /// `KAFKA_OPTS`), so only the descriptor can state it truthfully. A JVM
    /// container that declares none gets no `gc_*` metrics — an absence, never
    /// another JVM's numbers and never a zero.
    #[serde(default)]
    pub gc_log: Option<String>,
}

/// Whether a container does the work or coordinates it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Role {
    /// Does the ingestion. Charged against the envelope.
    DataPlane,
    /// Coordinates. Allocated on top, with measured cost published.
    ControlPlane,
}

/// The delivery guarantee every arm is expected to run under.
///
/// `methodology/`: "Every arm runs **at-least-once**." Validation refuses any
/// other value, because an exactly-once arm sitting in the same chart as five
/// at-least-once arms is paying for a stronger guarantee than they are and the
/// numbers are not on one axis.
pub const DELIVERY_CONTRACT: &str = "at-least-once";

/// Delivery semantics, published beside the numbers so the comparison is
/// guarantee-for-guarantee rather than throughput-for-throughput.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Guarantees {
    /// e.g. `at-least-once`.
    pub delivery: String,
    /// Mechanism: `offset-commit`, `checkpoint`, …
    pub durability: String,
    /// Cadence of that mechanism.
    pub interval_ms: u64,
}

/// Named docker volumes.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Volumes {
    /// `name:/path` entries. Named volumes only — a host bind mount would be
    /// served over VirtioFS on the reference environment, taxing exactly the
    /// paths under measurement.
    pub named: Vec<String>,
}

/// One published arm.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Variant {
    /// Stable for the life of the entrant: it is recorded on every result.
    pub id: String,
    /// Human label.
    pub label: String,
    /// The anti-gaming valve.
    pub approach: Approach,
    /// Components this variant selects that the entrant's project does not ship:
    /// code we wrote, a patched build, a fork.
    ///
    /// Declaring one binds the variant to [`Approach::Stripped`], and that
    /// binding is the whole point of the field. Before it, the only thing keeping
    /// the Flink arm that runs our own `ReusingAvroDeserializationSchema` out of
    /// the headline set was the word `stripped` on one hand-written line;
    /// editing that one word to `realistic` passed every check in the repository
    /// and put hand-written-decoder numbers into the default view of the site.
    /// `methodology/` calls this valve "not hypothetical", and a valve that one
    /// word defeats is not one.
    ///
    /// Naming the component rather than setting a boolean is deliberate: the
    /// declaration is rendered, so a reader can see *what* the arm substituted,
    /// and deleting it to dodge the rule is a visible removal of a disclosure
    /// rather than a one-word edit.
    #[serde(default)]
    pub unshipped: Vec<String>,
    /// Exactly one variant per active entrant is the default.
    #[serde(default)]
    pub default: bool,
    /// Extra environment for this arm.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Values substituted into `{{knob:…}}` placeholders.
    #[serde(default)]
    pub knobs: BTreeMap<String, toml::Value>,
    /// Facts the arm asserts about itself that the site publishes, notably
    /// `wire_format` (rule 5).
    #[serde(default)]
    pub reports: BTreeMap<String, String>,
}

/// How realistic a variant's configuration is. Defined against our own rules in
/// `methodology/`, not borrowed wholesale from another benchmark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Approach {
    /// Rules 1 and 3 satisfied: what a competent user would deploy.
    /// Headline-eligible, and the class every arm must have one of, because a
    /// tuned best must never be the only figure an arm publishes.
    Realistic,
    /// Rule-1 compliant but not what a typical user would deploy.
    /// Headline-eligible, and labelled wherever it appears: the label is a
    /// claim about deployability, not about the measurement.
    Tuned,
    /// Uses code the project does not ship, or drops a guarantee. Shown and
    /// filterable, never ranked; exists to quantify a specific effect.
    Stripped,
}

/// A knob combination the arm cannot run.
///
/// # Why this exists in the descriptor rather than in the driver
///
/// A tuning sweep walks a product of knob values, and some cells of that product
/// are not runnable at all. The Flink arm's sink is the case that forced this:
/// `AsyncSinkWriter` requires its buffered-request bound to be **strictly
/// greater** than its batch size, so `buffered_rows = max_rows` is not a slow
/// configuration, it is a job that refuses to start. Discovering that after two
/// containers, a job submission and a JVM start-up is minutes per cell across
/// dozens of cells, and the failure arrives as a stack trace in a container log
/// rather than as a sentence about the sweep.
///
/// The alternative was to teach `driver` about Flink's sink, which would put one
/// system's internals into the code that measures every system — the thing
/// [`Spec`] exists so that nobody has to do. The entrant states its own rule and
/// the driver applies it without knowing what it means.
///
/// # Two relations, deliberately — and no third without a case
///
/// A constraint states exactly one of: `knob` strictly greater than another
/// knob (`exceeds`), or `knob` at or above a literal floor (`at_least`). A
/// general expression language here would be a configuration DSL: unreviewable,
/// and a place for a rule to be written that nobody can evaluate by reading it.
/// Each relation was added by an arm that needed it — `exceeds` by Flink's
/// sink (`buffered_rows` strictly above `max_rows`, or the job refuses to
/// start), `at_least` by Kafka Connect's sink (`buffer_flush_ms` at least 1,
/// or a zero flush time strands a sub-`bufferCount` tail and a drain never
/// completes). A third real case is a third field with its own name and its
/// own doc comment, added by someone who has one.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Constraint {
    /// The knob the relation binds.
    pub knob: String,
    /// The knob it must strictly exceed. Exactly one of this and `at_least`.
    #[serde(default)]
    pub exceeds: Option<String>,
    /// The literal it must be at least equal to. Exactly one of this and
    /// `exceeds`.
    #[serde(default)]
    pub at_least: Option<i64>,
    /// What breaks when the relation does not hold. Quoted verbatim in the
    /// refusal, so the operator of a sweep is told why the cell is impossible
    /// rather than merely that it is.
    pub why: String,
}

/// Every way a set of knob values breaks the entrant's declared constraints.
///
/// Every problem, never the first, for the reason `validate::results_are_valid`
/// reports every problem: a sweep operator fixing a cell should see the whole
/// list rather than discovering one bound per attempt.
///
/// A knob a constraint names and the values do not carry is itself a violation.
/// The tempting reading is "not applicable, skip it", and it is wrong here: an
/// unset knob means the image's own `ENV` default applies, which is a value the
/// descriptor never stated and the record would not report — so the constraint
/// would be evaluated against a number nobody can see, or not at all.
#[must_use]
pub fn knob_violations(
    constraints: &[Constraint],
    knobs: &BTreeMap<String, toml::Value>,
) -> Vec<String> {
    let mut out = Vec::new();
    for c in constraints {
        let value = |name: &str| knobs.get(name).and_then(toml::Value::as_integer);
        // Reflowed onto one line. `why` is a TOML multi-line string, so it
        // arrives hard-wrapped at the width of the descriptor, and a refusal
        // printed with those breaks in the middle of an indented list reads as
        // two problems rather than one.
        let why = || c.why.split_whitespace().collect::<Vec<_>>().join(" ");
        let stated = || {
            if knobs.is_empty() {
                "none".to_owned()
            } else {
                knobs
                    .keys()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            }
        };
        match (&c.exceeds, c.at_least) {
            (Some(exceeds), None) => match (value(&c.knob), value(exceeds)) {
                (Some(a), Some(b)) if a > b => {}
                (Some(a), Some(b)) => out.push(format!(
                    "{} = {a} must strictly exceed {exceeds} = {b}. {}",
                    c.knob,
                    why()
                )),
                _ => out.push(format!(
                    "the constraint \"{} must exceed {exceeds}\" cannot be checked: one \
                     of them is unset or is not an integer here (knobs: {}). An unset \
                     knob leaves the image's own default in force, which is a value \
                     this descriptor never states and no record reports.",
                    c.knob,
                    stated()
                )),
            },
            (None, Some(floor)) => match value(&c.knob) {
                Some(a) if a >= floor => {}
                Some(a) => out.push(format!(
                    "{} = {a} must be at least {floor}. {}",
                    c.knob,
                    why()
                )),
                None => out.push(format!(
                    "the constraint \"{} must be at least {floor}\" cannot be checked: \
                     it is unset or is not an integer here (knobs: {}). An unset knob \
                     leaves the image's own default in force, which is a value this \
                     descriptor never states and no record reports.",
                    c.knob,
                    stated()
                )),
            },
            // Malformed relations are refused by `validate_constraints` before
            // any run; this arm of the match exists so a caller that skipped
            // validation still gets a refusal rather than a silently-ignored
            // rule.
            _ => out.push(format!(
                "the constraint on {:?} states both `exceeds` and `at_least`, or \
                 neither; exactly one relation must be stated",
                c.knob
            )),
        }
    }
    out
}

/// One placeholder in an `[env]` value, parsed from the text between `{{ }}`.
///
/// This enum is the **entire** placeholder vocabulary: `driver::substitute`
/// resolves exactly these and refuses anything left over, and descriptor
/// validation refuses any other spelling where it is written. One definition shared by
/// both, deliberately — a second list is how the two drift, and a spelling
/// validation accepted but the driver did not would pass every gate and fail at
/// the sweep's first live repetition, the most expensive place to learn about a
/// typo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placeholder<'a> {
    /// Kafka bootstrap servers, on the bench network.
    BrokerInternal,
    /// Schema Registry base URL.
    RegistryInternal,
    /// ClickHouse HTTP base URL.
    ClickhouseInternal,
    /// The topic this run consumes. Mode-dependent: drain replays the corpus
    /// topic, sustained reads its own.
    Topic,
    /// The per-run consumer group.
    GroupId,
    /// `earliest` or `latest`, decided by the run mode, not by the descriptor.
    OffsetReset,
    /// The ClickHouse password the infrastructure actually started the server
    /// with. An arm that takes it from here cannot drift when the infra's value
    /// changes; before it existed, every arm baked the same literal into its
    /// image and the harness repeated it in two places of its own.
    ClickhousePassword,
    /// The named knob's effective value for the variant being started.
    Knob(&'a str),
    /// The named `[[envelope.container]]`'s DNS name on the bench network.
    Container(&'a str),
}

impl<'a> Placeholder<'a> {
    /// Every supported spelling, quoted into refusals so the fix is in the
    /// message rather than in this file's source.
    pub const GRAMMAR: &'static str = "{{broker_internal}}, {{registry_internal}}, \
         {{clickhouse_internal}}, {{clickhouse_password}}, {{topic}}, {{group_id}}, \
         {{offset_reset}}, {{knob:NAME}}, {{container:NAME}}";

    /// Parses the text between `{{` and `}}`. `None` is a spelling the driver
    /// resolves no value for.
    #[must_use]
    pub fn parse(inner: &'a str) -> Option<Self> {
        match inner {
            "broker_internal" => Some(Self::BrokerInternal),
            "registry_internal" => Some(Self::RegistryInternal),
            "clickhouse_internal" => Some(Self::ClickhouseInternal),
            "clickhouse_password" => Some(Self::ClickhousePassword),
            "topic" => Some(Self::Topic),
            "group_id" => Some(Self::GroupId),
            "offset_reset" => Some(Self::OffsetReset),
            _ => {
                if let Some(k) = inner.strip_prefix("knob:") {
                    Some(Self::Knob(k))
                } else {
                    inner.strip_prefix("container:").map(Self::Container)
                }
            }
        }
    }
}

/// Every `{{…}}` token in a raw `[env]` value: the token as written, and the
/// text between the braces — `None` when the `{{` never closes, so a truncated
/// spelling is reported rather than silently skipped.
#[must_use]
pub fn placeholder_tokens(raw: &str) -> Vec<(&str, Option<&str>)> {
    let mut out = Vec::new();
    let mut rest = raw;
    while let Some(start) = rest.find("{{") {
        let after = &rest[start..];
        match after.find("}}") {
            Some(end) => {
                out.push((&after[..end + 2], Some(&after[2..end])));
                rest = &after[end + 2..];
            }
            None => {
                out.push((after, None));
                rest = "";
            }
        }
    }
    out
}

/// One declared deviation from the common shape (rule 4).
///
/// `methodology/` and `CONTRIBUTING.md` have required this table since before
/// it was parseable, and the site has typed and rendered it for as long
/// (`website/src/components/Results/data.ts`) — the field names here are that
/// type's, deliberately, so the descriptor and the page cannot disagree about
/// what a deviation is made of. Until this struct existed, `deny_unknown_fields`
/// turned any attempt to comply with rule 4 into a parse error, which is the
/// exact inversion of what a disclosure rule is for.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Deviation {
    /// What differs, stated as a fact about this arm.
    pub what: String,
    /// Why the difference exists — the part a reader weighs.
    #[serde(default)]
    pub why: String,
    /// Which published quantities the difference touches, e.g. `envelope`,
    /// `server-side-cpu`, `latency`. Free-form: the site prints them verbatim.
    #[serde(default)]
    pub affects: Vec<String>,
}

/// Arm-specific ClickHouse objects and how the arm's inserts reach the log.
///
/// Exists for the arms whose pipeline puts SQL objects on the shared server or
/// changes how their inserts appear in `system.query_log`. The Kafka Connect
/// arm lands nested batches in a landing table and flattens with a materialized
/// view; the ClickHouse Kafka engine arm's inserts arrive as
/// Distributed-forwarded queries. Neither fact belongs in the driver — the code
/// that measures every system must not know any one system's shape — so the
/// entrant states it and the driver applies it without knowing what it means,
/// exactly as [`Constraint`] does for knobs.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickhouseSpec {
    /// SQL applied to the shared server after the workload DDL, relative to the
    /// entrant directory. Re-applied at the start of every repetition, after
    /// [`Self::arm_teardown_sql`] has cleared any leftovers, so no state
    /// survives from one repetition into the next — and the objects it creates
    /// are removed again when the repetition ends, so they exist only inside
    /// their own arm's repetition. See [`Self::arm_teardown_sql`] for the full
    /// lifecycle.
    ///
    /// The workload's own `ddl.sql` is deliberately untouched by this
    /// mechanism: `build.rs` hashes it into `dataset_version`, so arm objects
    /// living there would re-key every published record over a change that
    /// alters no other arm's bytes.
    #[serde(default)]
    pub arm_sql: String,
    /// SQL that removes everything `arm_sql` creates, relative to the entrant
    /// directory. Explicit rather than derived by parsing `arm_sql`, because a
    /// DROP generated from a parse of somebody's CREATE is a guess with
    /// privileges.
    ///
    /// The driver runs it at **both ends** of every repetition: once at the
    /// start — a defensive pass against a previous *process's* leftovers,
    /// before the `TRUNCATE` and the `arm_sql` re-create — and once when the
    /// repetition ends, on every path including failure and panic. The
    /// end-of-repetition leg is the load-bearing one: the sweep interleaves
    /// arms (`for rep { for arm }`), so an object torn down only at its own
    /// arm's next start would be live through every other arm's measured
    /// window in between, taxing the shared target during measurements it has
    /// nothing to do with. With both legs, a materialized view is never live
    /// while the table it targets is truncated — by any arm, sweep-wide, not
    /// merely by its own. Write it idempotent (`DROP … IF EXISTS`): the
    /// defensive pass usually has nothing to drop and runs the same statements
    /// regardless.
    #[serde(default)]
    pub arm_teardown_sql: String,
    /// Extra **unqualified** table names whose inserts are attributed to this
    /// arm in `system.query_log`, beside the workload's target table. The
    /// landing table of an MV-flatten arm goes here: its parent insert is the
    /// row that carries the view's cost.
    #[serde(default)]
    pub attribution_tables: Vec<String>,
    /// Whether the arm's inserts arrive at the shared server as forwarded
    /// distributed queries rather than as initial ones.
    ///
    /// The attribution query normally requires `is_initial_query` so that a
    /// distributed sub-query is never counted beside the query that spawned it.
    /// An arm that pushes through a `Distributed` table inverts the situation:
    /// **every** insert it lands is `is_initial_query = 0`, and the unmodified
    /// predicate would match nothing — which reads as an arm that never
    /// inserted. Declaring the fact keeps the strict predicate for every arm
    /// that does not need the exception.
    #[serde(default)]
    pub forwarded_inserts: bool,
}

/// Why an entrant is declared but not yet built.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Planned {
    #[serde(default)]
    pub tracking: String,
    #[serde(default)]
    pub blockers: String,
    /// Present when publication is gated on a licence review rather than on
    /// engineering.
    #[serde(default)]
    pub licence_gate: String,
}

impl Entrant {
    /// The entrant's id.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.spec.entrant.id
    }

    /// The variant marked `default`, else the first.
    #[must_use]
    pub fn default_variant(&self) -> Option<&Variant> {
        self.spec
            .variants
            .iter()
            .find(|v| v.default)
            .or_else(|| self.spec.variants.first())
    }

    /// A variant by id.
    #[must_use]
    pub fn variant(&self, id: &str) -> Option<&Variant> {
        self.spec.variants.iter().find(|v| v.id == id)
    }

    /// The single data-plane container.
    #[must_use]
    pub fn data_plane(&self) -> Option<&Container> {
        self.spec
            .envelope
            .as_ref()?
            .containers
            .iter()
            .find(|c| c.role == Role::DataPlane)
    }
}

/// Loads and validates every descriptor under `dir`.
///
/// # Errors
///
/// Returns every problem found, not just the first: a contributor fixing an
/// entrant should see the whole list rather than discovering one issue per
/// iteration.
pub fn load_all(dir: &Path) -> Result<Vec<Entrant>, Vec<String>> {
    let mut entrants = Vec::new();
    let mut errors = Vec::new();

    let mut dirs: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.is_dir())
            .collect(),
        Err(e) => return Err(vec![format!("read {}: {e}", dir.display())]),
    };
    dirs.sort();

    for d in dirs {
        let path = d.join("entrant.toml");
        if !path.is_file() {
            errors.push(format!("{} has no entrant.toml", d.display()));
            continue;
        }
        match load_one(&d, &path) {
            Ok(e) => entrants.push(e),
            Err(mut errs) => errors.append(&mut errs),
        }
    }

    cross_check(&entrants, &mut errors);

    if errors.is_empty() {
        Ok(entrants)
    } else {
        Err(errors)
    }
}

fn load_one(dir: &Path, path: &Path) -> Result<Entrant, Vec<String>> {
    let src =
        std::fs::read_to_string(path).map_err(|e| vec![format!("read {}: {e}", path.display())])?;
    let spec: Spec = toml::from_str(&src).map_err(|e| vec![format!("{}: {e}", path.display())])?;
    let entrant = Entrant {
        dir: dir.to_path_buf(),
        spec,
    };
    let errors = validate(&entrant);
    if errors.is_empty() {
        Ok(entrant)
    } else {
        Err(errors)
    }
}

/// Per-entrant validation. Status-gated: `planned` relaxes everything that
/// requires an implementation, so a roadmap entry does not have to lie about
/// having an envelope in order to be listed.
fn validate(e: &Entrant) -> Vec<String> {
    let mut errs = Vec::new();
    let id = e.id();
    let at = |m: String| format!("{id}: {m}");

    if e.spec.schema != DESCRIPTOR_SCHEMA {
        errs.push(at(format!(
            "descriptor schema {}, expected {DESCRIPTOR_SCHEMA}",
            e.spec.schema
        )));
    }

    // The id IS the join key: results, directories and the site all use it, so a
    // divergence between the two would silently orphan every record.
    let dir_name = e
        .dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    if dir_name != id {
        errs.push(at(format!("id does not match directory name {dir_name:?}")));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        errs.push(at(
            "id must be lowercase ascii, digits and hyphens".to_owned()
        ));
    }
    if e.spec.entrant.licence.trim().is_empty() {
        errs.push(at("licence is empty".to_owned()));
    }
    if e.spec.entrant.language.is_empty() {
        errs.push(at("language must list at least one language".to_owned()));
    }
    if e.spec.display.hue >= 360 {
        errs.push(at(format!(
            "display.hue {} is not an angle in 0..360",
            e.spec.display.hue
        )));
    }

    if !e.spec.entrant.status.is_runnable() {
        // A planned entrant must say why, or it is an empty promise.
        if e.spec.planned.is_none() {
            errs.push(at(
                "status is not runnable but [planned] is absent".to_owned()
            ));
        }
        return errs;
    }

    // From here on the entrant claims to be measurable.
    if e.spec.build.is_none() {
        errs.push(at("active entrant has no [build]".to_owned()));
    }
    if !e.dir.join("README.md").is_file() {
        errs.push(at("active entrant has no README.md".to_owned()));
    }
    if let Some(b) = &e.spec.build
        && !e.dir.join(&b.dockerfile).is_file()
    {
        errs.push(at(format!("build.dockerfile {} not found", b.dockerfile)));
    }

    validate_version(e, &mut errs, &at);
    validate_guarantees(e, &mut errs, &at);
    validate_envelope(e, &mut errs, &at);
    validate_variants(e, &mut errs, &at);
    validate_env(e, &mut errs, &at);
    validate_constraints(e, &mut errs, &at);
    validate_deviations(e, &mut errs, &at);
    validate_clickhouse(e, &mut errs, &at);

    errs
}

/// Checks that every declared deviation actually discloses something, and that
/// the shapes which *require* a disclosure carry one.
///
/// A deviation with an empty `what` satisfies rule 4's letter while telling a
/// reader nothing, which is the same shape as an empty `unshipped` entry and is
/// refused for the same reason.
///
/// The control-plane check enforces `CONTRIBUTING.md`'s normative rule: "A
/// `control-plane` container is allowed and is budgeted on top, but it requires
/// a `[[deviations]]` entry with `\"envelope\"` in `affects`." The container
/// declaration and the deviation are two halves of one disclosure — the first
/// allocates resources on top of the budget every other arm gets, the second is
/// where a reader learns that happened — and until this check existed the first
/// half validated alone, so the site could publish an arm consuming beyond the
/// envelope with nothing on the page saying so.
fn validate_deviations(e: &Entrant, errs: &mut Vec<String>, at: &dyn Fn(String) -> String) {
    for (i, d) in e.spec.deviations.iter().enumerate() {
        if d.what.trim().is_empty() {
            errs.push(at(format!(
                "[[deviations]] entry {i} has an empty `what`; a deviation that does \
                 not say what differs discloses nothing"
            )));
        }
    }

    let has_control_plane = e
        .spec
        .envelope
        .as_ref()
        .is_some_and(|env| env.containers.iter().any(|c| c.role == Role::ControlPlane));
    let discloses_envelope = e
        .spec
        .deviations
        .iter()
        .any(|d| d.affects.iter().any(|a| a == "envelope"));
    if has_control_plane && !discloses_envelope {
        errs.push(at(
            "declares a control-plane container but no [[deviations]] entry with \
             \"envelope\" in `affects`. CONTRIBUTING.md: \"A `control-plane` \
             container is allowed and is budgeted on top, but it requires a \
             `[[deviations]]` entry with `\"envelope\"` in `affects`.\" The \
             container is resources allocated on top of the envelope every other \
             arm runs inside, and the deviation is where a reader is told so."
                .to_owned(),
        ));
    }
}

/// Checks the arm's ClickHouse declaration for the shapes that fail at run time.
///
/// The pair rule is the load-bearing one. `arm_sql` without `arm_teardown_sql`
/// leaves the previous repetition's materialized view live while the driver
/// truncates its target, and the first symptom is a correctness gate failing on
/// rows that survived a TRUNCATE — a defect diagnosed hours from its cause.
fn validate_clickhouse(e: &Entrant, errs: &mut Vec<String>, at: &dyn Fn(String) -> String) {
    let Some(ch) = &e.spec.clickhouse else {
        return;
    };
    match (ch.arm_sql.trim(), ch.arm_teardown_sql.trim()) {
        ("", "") => {}
        (sql, "") | ("", sql) => errs.push(at(format!(
            "[clickhouse] declares {:?} without its pair. arm_sql and arm_teardown_sql \
             travel together: creation without teardown leaks objects across \
             repetitions, and teardown without creation drops objects nothing made.",
            sql
        ))),
        (sql, teardown) => {
            for rel in [sql, teardown] {
                // "Relative to the entrant directory" is a containment claim,
                // and `dir.join(rel)` does not enforce it: join *replaces* the
                // directory outright when `rel` is absolute, and `..` walks
                // above it. Either shape would make the descriptor run SQL
                // from outside the files this repository reviews, so both are
                // refused where they are written.
                let path = Path::new(rel);
                let escapes = path.is_absolute()
                    || path
                        .components()
                        .any(|c| matches!(c, std::path::Component::ParentDir));
                if escapes {
                    errs.push(at(format!(
                        "[clickhouse] path {rel:?} escapes the entrant directory. \
                         arm_sql and arm_teardown_sql are relative to the entrant \
                         directory: an absolute path replaces it outright and a \
                         `..` component walks above it, so either would run SQL \
                         that lives outside the entrant's own reviewed files."
                    )));
                } else if !e.dir.join(rel).is_file() {
                    errs.push(at(format!("[clickhouse] file {rel:?} not found")));
                }
            }
        }
    }
    for t in &ch.attribution_tables {
        if t.trim().is_empty() {
            errs.push(at(
                "[clickhouse].attribution_tables lists an empty name".to_owned()
            ));
        } else if t.trim() != t.as_str() {
            // The driver qualifies the declared string verbatim into the
            // attribution query, so " sensor_batches_landing" becomes
            // "default. sensor_batches_landing" — a name `system.query_log`
            // never writes. The predicate is `hasAny`, so it would not fail:
            // it would silently match nothing, and the arm's inserts on that
            // table would vanish from the server-side figure.
            errs.push(at(format!(
                "[clickhouse].attribution_tables entry {t:?} has surrounding \
                 whitespace. The name is qualified verbatim into the attribution \
                 query, so the padded form matches nothing in system.query_log \
                 and the table's inserts silently go unattributed."
            )));
        }
        // Unqualified here, qualified at the query site, so the database is
        // spelled in exactly one place (`driver::TARGET_DATABASE`) and cannot
        // disagree with how `system.query_log` writes it.
        if t.contains('.') {
            errs.push(at(format!(
                "[clickhouse].attribution_tables entry {t:?} is qualified; declare the \
                 bare table name and let the driver qualify it the way \
                 system.query_log does"
            )));
        }
    }
}

/// Checks every `[env]` value's placeholders against the vocabulary the driver
/// resolves.
///
/// The failure this moves earlier: `driver::substitute` refuses an unresolved
/// placeholder at run time, so a stale `{{knob:threads}}` left behind by a
/// variant rename passed `bench validate` and CI, and failed at the sweep's
/// first live repetition. Every fact needed to catch it is in the descriptor.
///
/// The entrant-wide `[env]` is substituted for **every** variant, so a knob it
/// names must exist in every variant's `knobs` — one variant without it is one
/// arm of the sweep refused mid-run. A variant's own `env` binds only to that
/// variant's knobs.
fn validate_env(e: &Entrant, errs: &mut Vec<String>, at: &dyn Fn(String) -> String) {
    let mut check = |place: &str, key: &str, raw: &str, variants: &[&Variant]| {
        for (token, inner) in placeholder_tokens(raw) {
            let Some(inner) = inner else {
                errs.push(at(format!(
                    "{place} {key} = {raw:?}: {token:?} opens a placeholder that never closes"
                )));
                continue;
            };
            match Placeholder::parse(inner) {
                None => errs.push(at(format!(
                    "{place} {key} = {raw:?}: {token:?} is not a placeholder the driver \
                     resolves. Known: {}. Run time refuses an unresolved placeholder, so \
                     this descriptor would pass every gate and fail at the sweep's first \
                     live repetition — the most expensive place to learn about a typo.",
                    Placeholder::GRAMMAR
                ))),
                Some(Placeholder::Knob(k)) => {
                    for v in variants.iter().filter(|v| !v.knobs.contains_key(k)) {
                        errs.push(at(format!(
                            "{place} {key} names {{{{knob:{k}}}}} but variant {:?} declares \
                             no knob {k:?}. Substitution is per variant, so that variant's \
                             arm would be refused mid-sweep with an unresolved placeholder.",
                            v.id
                        )));
                    }
                }
                Some(Placeholder::Container(c)) => {
                    let declared = e
                        .spec
                        .envelope
                        .as_ref()
                        .is_some_and(|env| env.containers.iter().any(|ct| ct.name == c));
                    if !declared {
                        errs.push(at(format!(
                            "{place} {key} names {{{{container:{c}}}}} but no \
                             [[envelope.container]] is named {c:?}"
                        )));
                    }
                }
                Some(_) => {}
            }
        }
    };

    let all: Vec<&Variant> = e.spec.variants.iter().collect();
    for (key, raw) in &e.spec.env {
        check("[env]", key, raw, &all);
    }
    for v in &e.spec.variants {
        for (key, raw) in &v.env {
            check(&format!("variant {:?} env", v.id), key, raw, &[v]);
        }
    }
}

/// Checks the declared constraints, and checks every committed variant against
/// them.
///
/// Both halves are needed and they catch different mistakes. The first catches a
/// constraint that names a knob nothing sets, which would be a rule about
/// nothing. The second catches a *committed* variant that violates its own
/// entrant's rule — a descriptor that cannot run, which would otherwise be
/// discovered by whoever next ran the arm rather than by whoever wrote it.
fn validate_constraints(e: &Entrant, errs: &mut Vec<String>, at: &dyn Fn(String) -> String) {
    for c in &e.spec.constraints {
        if c.knob.trim().is_empty() || c.exceeds.as_deref().is_some_and(|n| n.trim().is_empty()) {
            errs.push(at(
                "a [[constraints]] entry names an empty knob; a constraint over \
                 nothing is a rule that can never fire"
                    .to_owned(),
            ));
        }
        match (&c.exceeds, c.at_least) {
            (Some(_), None) | (None, Some(_)) => {}
            _ => errs.push(at(format!(
                "constraint on {:?} must state exactly one relation: `exceeds` (a \
                 knob it strictly exceeds) or `at_least` (a literal it must meet). \
                 Both, or neither, is a rule nobody can evaluate.",
                c.knob
            ))),
        }
        if c.exceeds.as_deref() == Some(c.knob.as_str()) {
            errs.push(at(format!(
                "constraint {:?} must exceed itself, which nothing can satisfy",
                c.knob
            )));
        }
        if c.why.trim().is_empty() {
            errs.push(at(format!(
                "the constraint on {:?} does not say what breaks when it does not \
                 hold. The text is quoted verbatim into the refusal a sweep sees, \
                 and a refusal that only says `no` costs whoever hits it the \
                 investigation this field exists to have already done.",
                c.knob
            )));
        }
    }

    for v in &e.spec.variants {
        for why in knob_violations(&e.spec.constraints, &v.knobs) {
            errs.push(at(format!("variant {:?}: {why}", v.id)));
        }
    }
}

/// Checks that the version can actually be resolved by the code that resolves it.
///
/// The failure this moves earlier: an unimplemented strategy, or a `command`
/// strategy with no argv, resolves nothing at all, and `driver::resolve_sut`
/// refuses the run — after the image has been built and the sweep has started.
/// Every fact needed to catch it is in the descriptor.
fn validate_version(e: &Entrant, errs: &mut Vec<String>, at: &dyn Fn(String) -> String) {
    let Some(v) = &e.spec.version else {
        errs.push(at("active entrant has no [version]".to_owned()));
        return;
    };
    if !VERSION_STRATEGIES.contains(&v.strategy.as_str()) {
        errs.push(at(format!(
            "[version].strategy {:?} is not implemented. Known: {}",
            v.strategy,
            VERSION_STRATEGIES.join(", ")
        )));
    }
    if v.strategy == "command" && v.command.is_empty() {
        errs.push(at(
            "[version].strategy is \"command\" but [version].command is empty, so \
             nothing would be run and no version resolved"
                .to_owned(),
        ));
    }
}

/// Checks the delivery guarantee against the one the methodology fixes.
///
/// Every arm is compared guarantee-for-guarantee, so an arm declaring something
/// other than [`DELIVERY_CONTRACT`] is paying for something different from every
/// other arm on the same chart and cannot be drawn beside them.
fn validate_guarantees(e: &Entrant, errs: &mut Vec<String>, at: &dyn Fn(String) -> String) {
    let Some(g) = &e.spec.guarantees else {
        errs.push(at(
            "active entrant has no [guarantees]. Delivery semantics are published \
             beside the numbers so the comparison is guarantee-for-guarantee; an arm \
             that declares none cannot be compared at all."
                .to_owned(),
        ));
        return;
    };
    if g.delivery != DELIVERY_CONTRACT {
        errs.push(at(format!(
            "declares delivery {:?} rather than {DELIVERY_CONTRACT:?}. Every arm is \
             compared guarantee-for-guarantee, so an arm paying for a different \
             guarantee is not on the same axis as the rest.",
            g.delivery
        )));
    }
}

fn validate_envelope(e: &Entrant, errs: &mut Vec<String>, at: &dyn Fn(String) -> String) {
    let Some(env) = &e.spec.envelope else {
        errs.push(at("active entrant has no [envelope]".to_owned()));
        return;
    };

    let data: Vec<&Container> = env
        .containers
        .iter()
        .filter(|c| c.role == Role::DataPlane)
        .collect();
    if data.len() != 1 {
        errs.push(at(format!(
            "expected exactly one data-plane container, found {}",
            data.len()
        )));
        return;
    }

    // The declared totals must equal what the data-plane containers actually get.
    // Without this the envelope is decorative: the driver would apply per-container
    // caps while the record published a total nothing enforced.
    let sum_cpus: f64 = data.iter().filter_map(|c| c.cpus.parse::<f64>().ok()).sum();
    let want_cpus = env.cpus.parse::<f64>().unwrap_or(f64::NAN);
    if (sum_cpus - want_cpus).abs() > f64::EPSILON {
        errs.push(at(format!(
            "data-plane containers total {sum_cpus} CPUs but [envelope].cpus is {}",
            env.cpus
        )));
    }
    let sum_mem: u64 = data.iter().filter_map(|c| parse_memory(&c.memory)).sum();
    match parse_memory(&env.memory) {
        Some(want) if want == sum_mem => {}
        Some(want) => errs.push(at(format!(
            "data-plane containers total {sum_mem} bytes but [envelope].memory is {} ({want})",
            env.memory
        ))),
        None => errs.push(at(format!(
            "[envelope].memory {:?} unparseable",
            env.memory
        ))),
    }
}

/// Reduces a wire-format spelling to lowercase letters and digits, so that
/// `"JSONEachRow"`, `"json-each-row"` and `"json_each_row"` all fold to
/// `"jsoneachrow"`.
///
/// Used only to *detect* a near-miss of a canonical spelling and refuse it with
/// the canonical form named — never to accept one. `ceiling::Format::parse`
/// stays the single definition of what a descriptor may say; a matcher that
/// forgave a spelling would be a second one.
fn fold_wire_format(s: &str) -> String {
    s.chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

fn validate_variants(e: &Entrant, errs: &mut Vec<String>, at: &dyn Fn(String) -> String) {
    if e.spec.variants.is_empty() {
        errs.push(at("active entrant declares no variants".to_owned()));
        return;
    }

    let mut seen = std::collections::BTreeSet::new();
    for v in &e.spec.variants {
        if !seen.insert(v.id.as_str()) {
            errs.push(at(format!("duplicate variant id {:?}", v.id)));
        }
        if !v
            .id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            errs.push(at(format!(
                "variant id {:?} must be lowercase ascii, digits and hyphens",
                v.id
            )));
        }
        // Rule 5: the insert format is not the same server-side work across
        // systems, so a results table that omits it is indefensible.
        match v.reports.get("wire_format") {
            None => errs.push(at(format!(
                "variant {:?} does not report a wire_format",
                v.id
            ))),
            Some(declared) => {
                // A near-miss spelling of a format the rig CAN measure is
                // refused here, because `ceiling::Format::parse` deliberately
                // matches the canonical spelling exactly and nothing else —
                // the descriptor grammar is defined in one place, not in a
                // forgiving matcher. The failure a near-miss produces at run
                // time is the quiet kind: the arm is not gated against the
                // measured ceiling for its format and every record it emits
                // carries HeadroomUnproven, hours after the typo was written.
                // The normative docs' own prose spelling ("JSONEachRow") is
                // exactly this trap.
                //
                // A declared format with no near-miss stays allowed: an arm
                // may report a format the rig has no ceiling for — that is
                // rule 5 doing its job, and HeadroomUnproven is the honest
                // record of it.
                let folded = fold_wire_format(declared);
                for f in crate::ceiling::FORMATS {
                    let canonical = f.wire_format();
                    if declared != canonical && folded == fold_wire_format(canonical) {
                        errs.push(at(format!(
                            "variant {:?} reports wire_format {declared:?} — did you \
                             mean {canonical:?}? The ceiling gate matches the \
                             canonical spelling exactly, so this near-miss would not \
                             fail anything: every record the arm emits would silently \
                             carry HeadroomUnproven, ungated against the ceiling that \
                             was measured for its own format.",
                            v.id
                        )));
                    }
                }
            }
        }
        // Rule 1's valve. A variant that selects code the project does not ship
        // and is labelled `realistic` is the direction the rule exists to stop,
        // and `unshipped` is what makes that direction detectable at all:
        // without a declaration there is nothing in any file the harness reads
        // that distinguishes "runs the shipped deserializer" from "runs ours".
        for u in &v.unshipped {
            if u.trim().is_empty() {
                errs.push(at(format!(
                    "variant {:?} lists an empty `unshipped` entry; name the component",
                    v.id
                )));
            }
        }
        if !v.unshipped.is_empty() && v.approach != Approach::Stripped {
            errs.push(at(format!(
                "variant {:?} declares unshipped {:?} but has approach {:?}. Code the \
                 project does not ship is `stripped` under rule 1 and is never the \
                 headline. If the arm no longer uses it, remove the declaration in \
                 the same change — the two move together or the label is decoration.",
                v.id, v.unshipped, v.approach
            )));
        }
    }

    let defaults = e.spec.variants.iter().filter(|v| v.default).count();
    if defaults != 1 {
        errs.push(at(format!(
            "expected exactly one variant marked default, found {defaults}"
        )));
    }
    // This check carries more weight since rule 3 made `tuned` headline-eligible.
    // It is what guarantees every arm still publishes the figure a user would
    // actually get: without it an arm could present only its tuned best, and
    // "how does this behave out of the box" would stop being answerable from the
    // page. A `stripped` default would additionally put a deliberately
    // unrepresentative arm in front — the failure the valve exists to prevent.
    if let Some(d) = e.spec.variants.iter().find(|v| v.default)
        && d.approach != Approach::Realistic
    {
        errs.push(at(format!(
            "default variant {:?} has approach {:?}; the default must be realistic",
            d.id, d.approach
        )));
    }
}

/// Checks that only make sense across the whole set.
fn cross_check(entrants: &[Entrant], errs: &mut Vec<String>) {
    let mut orders: BTreeMap<i64, &str> = BTreeMap::new();
    let mut hues: Vec<(u16, &str)> = Vec::new();

    for e in entrants {
        if let Some(prev) = orders.insert(e.spec.display.order, e.id()) {
            errs.push(format!(
                "display.order {} is used by both {prev} and {}",
                e.spec.display.order,
                e.id()
            ));
        }
        hues.push((e.spec.display.hue, e.id()));
    }

    // Hues must be far enough apart to stay distinguishable. Checked here rather
    // than left to the site, because the site derives its colours from these and
    // cannot invent separation that the data does not have.
    hues.sort_unstable();
    for w in hues.windows(2) {
        let (a, ida) = w[0];
        let (b, idb) = w[1];
        if b - a < 20 {
            errs.push(format!(
                "display.hue {a} ({ida}) and {b} ({idb}) are within 20 degrees"
            ));
        }
    }
}

/// Parses a declared memory limit — `16g`, `512m`, `1048576k`, `1024` (bytes) —
/// into bytes.
///
/// Public, and the **only** copy, because there were four and they had drifted.
/// This module and `infra` accepted `k`; the copy in `driver` did not, and the
/// copy in `tests/entrants_are_valid.rs` returned MiB and rejected a bare byte
/// count. The `driver` copy is the one that asserts an arm's *applied* cgroup
/// cap, which the methodology says fails a run rather than warning, so the
/// drift landed on the strictest check: a descriptor declaring
/// `memory = "1048576k"` validated here, was applied by Docker exactly as asked,
/// and then failed the cap assertion with a message reporting that the container
/// ran under a limit which was in fact the one requested.
///
/// It lives here because descriptors are where these strings originate.
#[must_use]
pub fn parse_memory(s: &str) -> Option<u64> {
    let s = s.trim();
    // Only ASCII suffixes are sliced off, so `s.len() - 1` is always a character
    // boundary — a trailing multi-byte character falls through to the bare-count
    // arm and fails to parse.
    let (digits, mult) = match s.chars().last()? {
        'g' | 'G' => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        'm' | 'M' => (&s[..s.len() - 1], 1024 * 1024),
        'k' | 'K' => (&s[..s.len() - 1], 1024),
        _ => (s, 1),
    };
    // Checked rather than wrapping: an absurd declaration has to be unparseable,
    // because a wrapped product would be a plausible small number that could
    // compare equal to an unrelated cap and pass the assertion it exists to fail.
    digits.trim().parse::<u64>().ok()?.checked_mul(mult)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variant(id: &str, approach: Approach, unshipped: &[&str]) -> Variant {
        Variant {
            id: id.to_owned(),
            label: id.to_owned(),
            approach,
            default: false,
            env: BTreeMap::new(),
            knobs: BTreeMap::new(),
            reports: BTreeMap::from([("wire_format".to_owned(), "native".to_owned())]),
            unshipped: unshipped.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    fn entrant_with(variants: Vec<Variant>) -> Entrant {
        Entrant {
            dir: PathBuf::from("entrants/probe"),
            spec: Spec {
                schema: DESCRIPTOR_SCHEMA,
                entrant: Identity {
                    id: "probe".to_owned(),
                    name: "Probe".to_owned(),
                    homepage: String::new(),
                    repo: String::new(),
                    docs: String::new(),
                    licence: "Apache-2.0".to_owned(),
                    vendor: "probe".to_owned(),
                    kind: "stream-processor".to_owned(),
                    language: vec!["rust".to_owned()],
                    runtime: "native".to_owned(),
                    status: Status::Active,
                },
                maintainer: Maintainer {
                    who: "spate-benchmark".to_owned(),
                    reviewed_upstream: false,
                    review_url: String::new(),
                },
                display: Display {
                    order: 1,
                    hue: 0,
                    short: "Probe".to_owned(),
                },
                version: None,
                build: None,
                envelope: None,
                guarantees: None,
                volumes: None,
                env: BTreeMap::new(),
                variants,
                constraints: Vec::new(),
                deviations: Vec::new(),
                clickhouse: None,
                planned: None,
            },
        }
    }

    fn errors_from(variants: Vec<Variant>) -> Vec<String> {
        let e = entrant_with(variants);
        let mut errs = Vec::new();
        let at = |m: String| format!("probe: {m}");
        validate_variants(&e, &mut errs, &at);
        errs
    }

    /// The Flink sink's rule, in the shape the descriptor states it.
    fn buffered_exceeds_batch() -> Constraint {
        Constraint {
            knob: "buffered_rows".to_owned(),
            exceeds: Some("max_rows".to_owned()),
            at_least: None,
            why: "AsyncSinkWriter refuses to construct otherwise.".to_owned(),
        }
    }

    fn knobs(pairs: &[(&str, i64)]) -> BTreeMap<String, toml::Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), toml::Value::Integer(*v)))
            .collect()
    }

    #[test]
    fn every_memory_suffix_a_descriptor_may_use_parses_to_bytes() {
        assert_eq!(parse_memory("4g"), Some(4 * 1024 * 1024 * 1024));
        assert_eq!(parse_memory("512m"), Some(512 * 1024 * 1024));
        assert_eq!(parse_memory("1024"), Some(1024));
        assert_eq!(parse_memory("nonsense"), None);
        assert_eq!(parse_memory(" 8G "), Some(8 * 1024 * 1024 * 1024));
    }

    #[test]
    fn a_kibibyte_suffix_parses_because_one_copy_of_this_used_to_reject_it() {
        // The drift this closes. `driver`'s copy had no `k` arm, so a descriptor
        // declaring `1048576k` validated here, was applied by Docker as exactly
        // one gibibyte, and then failed the cap assertion claiming the container
        // was running under a limit that was precisely the one requested.
        assert_eq!(parse_memory("1048576k"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory("1048576K"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_memory("1g"), parse_memory("1048576k"));
    }

    #[test]
    fn an_overflowing_declaration_is_unparseable_rather_than_wrapped() {
        // A wrapped product is the dangerous outcome: it is a plausible small
        // number that could compare equal to some unrelated cap and pass the
        // assertion this parser feeds.
        assert_eq!(parse_memory("99999999999999999999g"), None);
        assert_eq!(parse_memory("18014398509481985k"), None);
    }

    #[test]
    fn a_variant_selecting_unshipped_code_must_be_labelled_stripped() {
        // The valve's missing converse. `entrants/flink/entrant.toml` marks the
        // arm running our own `ReusingAvroDeserializationSchema` as `stripped` on
        // one hand-written line; before this rule, editing that word to
        // `realistic` passed every check and moved hand-written-decoder numbers
        // into the site's default view.
        let errs = errors_from(vec![
            variant("shipped", Approach::Realistic, &[]),
            variant(
                "ours",
                Approach::Realistic,
                &["ReusingAvroDeserializationSchema"],
            ),
        ]);
        assert!(
            errs.iter().any(|e| e.contains("declares unshipped")),
            "{errs:?}"
        );

        // Labelled correctly, the same descriptor raises nothing about approach.
        let errs = errors_from(vec![
            variant("shipped", Approach::Realistic, &[]),
            variant(
                "ours",
                Approach::Stripped,
                &["ReusingAvroDeserializationSchema"],
            ),
        ]);
        assert!(
            !errs.iter().any(|e| e.contains("declares unshipped")),
            "{errs:?}"
        );
    }

    #[test]
    fn an_unshipped_declaration_must_name_the_component() {
        // `unshipped = [""]` would otherwise satisfy the rule above while
        // disclosing nothing, which is the shape a reader cannot act on.
        let errs = errors_from(vec![variant("ours", Approach::Stripped, &[" "])]);
        assert!(
            errs.iter().any(|e| e.contains("empty `unshipped` entry")),
            "{errs:?}"
        );
    }

    #[test]
    fn a_knob_combination_the_arm_cannot_run_is_a_violation_quoting_the_reason() {
        // The cell a sweep will reach for first: raise the batch size to match
        // the single-process arms' and leave the buffer where it was. Flink's
        // AsyncSinkWriter refuses to construct, minutes into the cell, with a
        // message naming neither knob.
        let v = knob_violations(
            &[buffered_exceeds_batch()],
            &knobs(&[("max_rows", 262_144), ("buffered_rows", 50_000)]),
        );
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("262144"), "{}", v[0]);
        assert!(v[0].contains("50000"), "{}", v[0]);
        assert!(
            v[0].contains("AsyncSinkWriter"),
            "the entrant's own reason: {}",
            v[0]
        );

        // Strictly greater, not greater-or-equal: equality is exactly the case
        // the sink rejects, and an off-by-one here would let the sweep spend a
        // cell proving it.
        assert_eq!(
            knob_violations(
                &[buffered_exceeds_batch()],
                &knobs(&[("max_rows", 50_000), ("buffered_rows", 50_000)]),
            )
            .len(),
            1
        );
        assert!(
            knob_violations(
                &[buffered_exceeds_batch()],
                &knobs(&[("max_rows", 50_000), ("buffered_rows", 50_001)]),
            )
            .is_empty()
        );
    }

    #[test]
    fn a_constraint_over_a_knob_the_variant_does_not_set_is_a_violation_and_not_a_pass() {
        // "Not applicable, skip it" is the tempting reading and the wrong one:
        // an unset knob leaves the image's own ENV default in force, so the
        // constraint would be evaluated against a number the descriptor never
        // states and no record reports.
        let v = knob_violations(&[buffered_exceeds_batch()], &knobs(&[("max_rows", 25_000)]));
        assert_eq!(v.len(), 1, "{v:?}");
        assert!(v[0].contains("cannot be checked"), "{}", v[0]);
        assert!(v[0].contains("max_rows"), "{}", v[0]);
    }

    #[test]
    fn a_committed_variant_that_breaks_its_own_entrants_constraint_fails_validation() {
        // A descriptor that cannot run must fail where it is written rather than
        // where it is next used.
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.constraints = vec![buffered_exceeds_batch()];
        e.spec.variants[0].knobs = knobs(&[("max_rows", 25_000), ("buffered_rows", 25_000)]);

        let mut errs = Vec::new();
        let at = |m: String| format!("probe: {m}");
        validate_constraints(&e, &mut errs, &at);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("variant \"only\""), "{}", errs[0]);
    }

    #[test]
    fn a_constraint_that_explains_nothing_is_rejected() {
        // The `why` is quoted verbatim into the refusal a sweep operator reads,
        // so an empty one costs whoever hits it the investigation the field
        // exists to have already done.
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.constraints = vec![Constraint {
            knob: "buffered_rows".to_owned(),
            exceeds: Some("buffered_rows".to_owned()),
            at_least: None,
            why: "  ".to_owned(),
        }];
        e.spec.variants[0].knobs = knobs(&[("buffered_rows", 1)]);

        let mut errs = Vec::new();
        let at = |m: String| format!("probe: {m}");
        validate_constraints(&e, &mut errs, &at);
        assert!(
            errs.iter().any(|x| x.contains("must exceed itself")),
            "{errs:?}"
        );
        assert!(
            errs.iter().any(|x| x.contains("does not say what breaks")),
            "{errs:?}"
        );
    }

    /// The `at_least` relation: the floor itself is admitted, the value below
    /// it is refused, and a constraint stating both relations (or neither) is
    /// a descriptor error rather than a rule that silently half-applies.
    #[test]
    fn an_at_least_floor_admits_the_floor_and_refuses_below_it() {
        let floor = Constraint {
            knob: "buffer_flush_ms".to_owned(),
            exceeds: None,
            at_least: Some(1),
            why: "a zero flush time strands a sub-buffer tail forever.".to_owned(),
        };
        let floor_only = std::slice::from_ref(&floor);
        assert!(knob_violations(floor_only, &knobs(&[("buffer_flush_ms", 1)])).is_empty());
        let refused = knob_violations(floor_only, &knobs(&[("buffer_flush_ms", 0)]));
        assert_eq!(refused.len(), 1, "{refused:?}");
        assert!(refused[0].contains("must be at least 1"), "{}", refused[0]);

        let mut both = floor;
        both.exceeds = Some("other".to_owned());
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.constraints = vec![both];
        e.spec.variants[0].knobs = knobs(&[("buffer_flush_ms", 1), ("other", 0)]);
        let mut errs = Vec::new();
        let at = |m: String| m;
        validate_constraints(&e, &mut errs, &at);
        assert!(
            errs.iter().any(|x| x.contains("exactly one relation")),
            "{errs:?}"
        );
    }

    #[test]
    fn a_deviation_that_does_not_say_what_differs_is_refused() {
        // Rule 4 is a disclosure rule; an entry with an empty `what` complies
        // with its letter while telling a reader nothing.
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.deviations = vec![Deviation {
            what: "  ".to_owned(),
            why: "reasons".to_owned(),
            affects: vec!["envelope".to_owned()],
        }];
        let mut errs = Vec::new();
        let at = |m: String| format!("probe: {m}");
        validate_deviations(&e, &mut errs, &at);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("empty `what`"), "{}", errs[0]);

        e.spec.deviations[0].what = "the transform runs server-side".to_owned();
        let mut errs = Vec::new();
        validate_deviations(&e, &mut errs, &at);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// CONTRIBUTING.md's normative rule: a control-plane container is budgeted
    /// on top of the envelope, so it requires a `[[deviations]]` entry with
    /// "envelope" in `affects`. The container and the deviation are two halves
    /// of one disclosure; a descriptor carrying only the first half is an arm
    /// consuming beyond the shared budget with nothing on the page saying so.
    #[test]
    fn a_control_plane_container_requires_an_envelope_deviation() {
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.envelope = Some(Envelope {
            cpus: "6".to_owned(),
            memory: "24g".to_owned(),
            containers: vec![
                Container {
                    role: Role::DataPlane,
                    name: "sut".to_owned(),
                    cpus: "6".to_owned(),
                    memory: "24g".to_owned(),
                    args: Vec::new(),
                    gc_log: None,
                },
                Container {
                    role: Role::ControlPlane,
                    name: "jm".to_owned(),
                    cpus: "1".to_owned(),
                    memory: "2g".to_owned(),
                    args: Vec::new(),
                    gc_log: None,
                },
            ],
        });
        let at = |m: String| format!("probe: {m}");

        let mut errs = Vec::new();
        validate_deviations(&e, &mut errs, &at);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("control-plane"), "{}", errs[0]);
        assert!(errs[0].contains("\"envelope\""), "{}", errs[0]);

        // With the promised disclosure in place, the same shape passes.
        e.spec.deviations = vec![Deviation {
            what: "runs a JobManager control-plane container on top of the envelope".to_owned(),
            why: "cannot run a standalone job without one".to_owned(),
            affects: vec!["envelope".to_owned()],
        }];
        let mut errs = Vec::new();
        validate_deviations(&e, &mut errs, &at);
        assert!(errs.is_empty(), "{errs:?}");
    }

    /// A near-miss of a measurable format's canonical spelling is refused with
    /// the canonical form named. `ceiling::Format::parse` matches exactly, so
    /// at run time the near-miss would fail nothing — it would silently publish
    /// every record as HeadroomUnproven. The normative docs' own prose spelling
    /// "JSONEachRow" is the canonical example of the trap.
    #[test]
    fn a_near_miss_wire_format_is_refused_with_the_canonical_spelling() {
        let mut v = variant("only", Approach::Realistic, &[]);
        v.reports = BTreeMap::from([("wire_format".to_owned(), "JSONEachRow".to_owned())]);
        let errs = errors_from(vec![v]);
        assert!(
            errs.iter()
                .any(|e| e.contains("did you mean \"json_each_row\"")),
            "{errs:?}"
        );
    }

    /// The exact canonical spelling raises nothing; a genuinely foreign format
    /// with no near-miss stays allowed too — an arm may report a format the rig
    /// has no ceiling for (rule 5), and HeadroomUnproven is the honest record
    /// of that, not a validation failure.
    #[test]
    fn canonical_and_genuinely_foreign_wire_formats_pass() {
        let mut canonical = variant("only", Approach::Realistic, &[]);
        canonical.reports =
            BTreeMap::from([("wire_format".to_owned(), "json_each_row".to_owned())]);
        let errs = errors_from(vec![canonical]);
        assert!(!errs.iter().any(|e| e.contains("did you mean")), "{errs:?}");

        let mut foreign = variant("only", Approach::Realistic, &[]);
        foreign.reports = BTreeMap::from([("wire_format".to_owned(), "go_sql".to_owned())]);
        let errs = errors_from(vec![foreign]);
        assert!(!errs.iter().any(|e| e.contains("did you mean")), "{errs:?}");
    }

    #[test]
    fn arm_sql_and_its_teardown_travel_together() {
        // Creation without teardown leaks a live materialized view into the
        // next repetition's TRUNCATE; teardown without creation drops objects
        // nothing made. Both are refused where they are written.
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.clickhouse = Some(ClickhouseSpec {
            arm_sql: "clickhouse/arm.sql".to_owned(),
            arm_teardown_sql: String::new(),
            attribution_tables: Vec::new(),
            forwarded_inserts: false,
        });
        let mut errs = Vec::new();
        let at = |m: String| format!("probe: {m}");
        validate_clickhouse(&e, &mut errs, &at);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("without its pair"), "{}", errs[0]);
    }

    /// The mirror of the test above: teardown without creation is the same
    /// pairing violation and must hit the same refusal, or the or-pattern in
    /// `validate_clickhouse` could regress on one side without a test noticing.
    #[test]
    fn teardown_without_arm_sql_is_refused_like_its_mirror() {
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.clickhouse = Some(ClickhouseSpec {
            arm_sql: String::new(),
            arm_teardown_sql: "clickhouse/teardown.sql".to_owned(),
            attribution_tables: Vec::new(),
            forwarded_inserts: false,
        });
        let mut errs = Vec::new();
        let at = |m: String| format!("probe: {m}");
        validate_clickhouse(&e, &mut errs, &at);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("without its pair"), "{}", errs[0]);
    }

    /// An absolute path is not "relative to the entrant directory":
    /// `dir.join(rel)` replaces the directory outright, so the descriptor
    /// would run SQL from anywhere on the host.
    #[test]
    fn an_absolute_arm_sql_path_is_refused() {
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.clickhouse = Some(ClickhouseSpec {
            arm_sql: "/etc/arm.sql".to_owned(),
            arm_teardown_sql: "/etc/teardown.sql".to_owned(),
            attribution_tables: Vec::new(),
            forwarded_inserts: false,
        });
        let mut errs = Vec::new();
        let at = |m: String| format!("probe: {m}");
        validate_clickhouse(&e, &mut errs, &at);
        assert_eq!(errs.len(), 2, "{errs:?}");
        assert!(
            errs.iter()
                .all(|x| x.contains("escapes the entrant directory")),
            "{errs:?}"
        );
    }

    /// `..` walks above the entrant directory, which is the same escape by a
    /// different spelling — and unlike the absolute form it survives a casual
    /// review because it still looks relative.
    #[test]
    fn a_parent_traversal_arm_sql_path_is_refused() {
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.clickhouse = Some(ClickhouseSpec {
            arm_sql: "../other/arm.sql".to_owned(),
            arm_teardown_sql: "../other/teardown.sql".to_owned(),
            attribution_tables: Vec::new(),
            forwarded_inserts: false,
        });
        let mut errs = Vec::new();
        let at = |m: String| format!("probe: {m}");
        validate_clickhouse(&e, &mut errs, &at);
        assert_eq!(errs.len(), 2, "{errs:?}");
        assert!(
            errs.iter()
                .all(|x| x.contains("escapes the entrant directory")),
            "{errs:?}"
        );
    }

    /// A padded name is qualified verbatim into the attribution query, where
    /// `hasAny` silently matches nothing — the arm's inserts on that table
    /// vanish from the server-side figure rather than failing anything.
    #[test]
    fn an_attribution_table_with_surrounding_whitespace_is_refused() {
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.clickhouse = Some(ClickhouseSpec {
            arm_sql: String::new(),
            arm_teardown_sql: String::new(),
            attribution_tables: vec![" sensor_batches_landing".to_owned()],
            forwarded_inserts: false,
        });
        let mut errs = Vec::new();
        let at = |m: String| format!("probe: {m}");
        validate_clickhouse(&e, &mut errs, &at);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("surrounding"), "{}", errs[0]);
        assert!(errs[0].contains("verbatim"), "{}", errs[0]);
    }

    #[test]
    fn an_attribution_table_is_declared_unqualified() {
        // The database is spelled in exactly one place, at the query site, so
        // the declaration and system.query_log's spelling cannot drift apart.
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.clickhouse = Some(ClickhouseSpec {
            arm_sql: String::new(),
            arm_teardown_sql: String::new(),
            attribution_tables: vec!["default.sensor_batches_landing".to_owned()],
            forwarded_inserts: true,
        });
        let mut errs = Vec::new();
        let at = |m: String| format!("probe: {m}");
        validate_clickhouse(&e, &mut errs, &at);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("is qualified"), "{}", errs[0]);
    }

    fn env_errors(e: &Entrant) -> Vec<String> {
        let mut errs = Vec::new();
        let at = |m: String| format!("probe: {m}");
        validate_env(e, &mut errs, &at);
        errs
    }

    /// A single-container envelope for the placeholder tests.
    fn sut_envelope() -> Envelope {
        Envelope {
            cpus: "6".to_owned(),
            memory: "24g".to_owned(),
            containers: vec![Container {
                role: Role::DataPlane,
                name: "sut".to_owned(),
                cpus: "6".to_owned(),
                memory: "24g".to_owned(),
                args: Vec::new(),
                gc_log: None,
            }],
        }
    }

    #[test]
    fn an_env_using_only_the_supported_vocabulary_validates() {
        // Every placeholder kind at once: a driver-owned value, a knob present
        // in both variants, a declared container, and a variant-scoped knob
        // that exists only in the variant whose env names it.
        let mut a = variant("a", Approach::Realistic, &[]);
        a.knobs = knobs(&[("threads", 6), ("only_a", 1)]);
        a.env = BTreeMap::from([("ONLY_A".to_owned(), "{{knob:only_a}}".to_owned())]);
        let mut b = variant("b", Approach::Realistic, &[]);
        b.knobs = knobs(&[("threads", 8)]);

        let mut e = entrant_with(vec![a, b]);
        e.spec.envelope = Some(sut_envelope());
        e.spec.env = BTreeMap::from([
            ("BOOTSTRAP".to_owned(), "{{broker_internal}}".to_owned()),
            ("THREADS".to_owned(), "{{knob:threads}}".to_owned()),
            ("PEER".to_owned(), "{{container:sut}}".to_owned()),
            ("PLAIN".to_owned(), "no placeholder at all".to_owned()),
        ]);
        let errs = env_errors(&e);
        assert!(errs.is_empty(), "{errs:?}");
    }

    #[test]
    fn an_unknown_placeholder_fails_validation_rather_than_the_sweep() {
        // The gap this closes: `driver::substitute` refuses an unresolved
        // placeholder at RUN time, so a spelling outside the vocabulary passed
        // `bench validate` and CI and failed at the first live repetition.
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.env = BTreeMap::from([("TOPIC".to_owned(), "{{topics}}".to_owned())]);
        let errs = env_errors(&e);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("{{topics}}"), "{}", errs[0]);
        assert!(
            errs[0].contains("{{knob:NAME}}"),
            "the fix belongs in the message: {}",
            errs[0]
        );
    }

    #[test]
    fn a_knob_placeholder_absent_from_one_variant_names_that_variant() {
        // The stale-rename case. The entrant-wide [env] is substituted for
        // every variant, so a knob one variant lost in a retune is that
        // variant's arm refused mid-sweep — and the message must say which.
        let mut with = variant("with", Approach::Realistic, &[]);
        with.knobs = knobs(&[("threads", 6)]);
        let without = variant("without", Approach::Realistic, &[]);

        let mut e = entrant_with(vec![with, without]);
        e.spec.env = BTreeMap::from([("THREADS".to_owned(), "{{knob:threads}}".to_owned())]);
        let errs = env_errors(&e);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("\"without\""), "{}", errs[0]);
        assert!(errs[0].contains("threads"), "{}", errs[0]);
    }

    #[test]
    fn a_container_placeholder_must_name_a_declared_container() {
        // `{{container:jm}}` with no such [[envelope.container]] resolves
        // nothing; the flink arm's JOB_MANAGER_RPC_ADDRESS is the live use.
        let mut e = entrant_with(vec![variant("only", Approach::Realistic, &[])]);
        e.spec.envelope = Some(sut_envelope());
        e.spec.env = BTreeMap::from([("RPC".to_owned(), "{{container:jm}}".to_owned())]);
        let errs = env_errors(&e);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("{{container:jm}}"), "{}", errs[0]);
        assert!(errs[0].contains("[[envelope.container]]"), "{}", errs[0]);
    }

    #[test]
    fn an_unterminated_placeholder_is_refused_where_it_is_written() {
        // `{{knob:threads` never closes, so no replacement ever matches it; at
        // run time it survives every substitution and hits the driver's
        // unresolved-placeholder refusal mid-sweep.
        let mut only = variant("only", Approach::Realistic, &[]);
        only.knobs = knobs(&[("threads", 6)]);
        let mut e = entrant_with(vec![only]);
        e.spec.env = BTreeMap::from([("THREADS".to_owned(), "{{knob:threads".to_owned())]);
        let errs = env_errors(&e);
        assert_eq!(errs.len(), 1, "{errs:?}");
        assert!(errs[0].contains("never closes"), "{}", errs[0]);
    }

    #[test]
    fn hue_separation_is_enforced() {
        // Two entrants whose colours a reader could not tell apart is a data
        // problem, not a rendering problem: the site derives hue from here and
        // cannot invent separation the descriptors did not provide.
        let mut errs = Vec::new();
        let hues = [(10u16, "a"), (25u16, "b")];
        let mut v: Vec<(u16, &str)> = hues.to_vec();
        v.sort_unstable();
        for w in v.windows(2) {
            if w[1].0 - w[0].0 < 20 {
                errs.push("too close".to_owned());
            }
        }
        assert_eq!(errs.len(), 1);
    }
}
