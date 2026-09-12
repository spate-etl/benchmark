//! GC pauses and configured-versus-used heap, for the JVM arms only.
//!
//! `methodology/` promises two things this module produces and nothing else
//! currently does: a GC row sourced from `-Xlog:gc*`, and "configured versus
//! actually-used heap, so that the gap between allocation and use is visible
//! rather than implied". An earlier review of the Flink arm found the first half
//! of that already half-built — `entrants/flink/config.yaml` configures the JVM
//! to write `/opt/flink/log/gc.log`, its comment says "the driver reads
//! /opt/flink/log/gc.log", and no code anywhere read it. Flink has been writing
//! GC logs that nothing ever read.
//!
//! # Why this is not the arm reporting on itself
//!
//! The rule is absolute — "nothing a system reports about itself is used for any
//! published number" — and this module reads a log file out of the arm's own
//! container, so the distinction has to be exact rather than convenient.
//!
//! The system under test is **Flink**, and Flink reports nothing here. A GC log
//! is the *runtime's* account of itself: the HotSpot VM writes it whether or not
//! Flink exists, its content is decided by JVM flags rather than by any code in
//! the arm, and no line of the job's Java could change a figure in it except by
//! allocating differently — which is the thing being measured. It stands in the
//! same relation to Flink as `system.query_log` does to an arm's inserts: a
//! shared runtime's accounting of what it was asked to do.
//!
//! `methodology/` also names `-Xlog:gc*` as the source outright, so the
//! question is settled by the normative document rather than by this argument.
//! The argument is written down anyway, because the day somebody proposes
//! reading Flink's own `taskmanager.Status.JVM.GarbageCollector` metrics instead
//! — which report the same quantity and would be far easier to fetch — the
//! reason that is not allowed has to already be on the page.
//!
//! One further property makes this safe where a framework metric would not be:
//! the JVM runs **inside the arm's cgroup**, so every microsecond in this log is
//! already inside the CPU the sampler measured from outside. These figures are a
//! *decomposition* of a number obtained externally, never an addition to it. If
//! the GC log and the cgroup disagreed, the cgroup would be right.
//!
//! # The asymmetry, which must not be presented as a zero
//!
//! There is no equivalent of this for the Spate arm, and there never will be: a
//! Rust binary has no collector, so it has no pause distribution and no
//! configured heap. That is a real difference between the two runtimes and it is
//! worth showing — but **the absence of a GC number is not a GC number of
//! zero**. A chart that renders a missing pause total as a bar of length zero
//! says "Spate paused for 0 ms", which is a claim about a measurement nobody
//! made rather than about a collector that does not exist.
//!
//! So: this module produces values only where the quantity exists, refuses
//! rather than returning an empty summary everywhere else, and a consumer that
//! finds no `gc_*` metric on a record must render "not applicable" and not "0".
//! The driver must omit the metrics entirely for a non-JVM arm rather than
//! emitting them as zeroes.
//!
//! # Why a JVM-only mechanism is acceptable where `docker exec` was not
//!
//! `crate::sampler` refuses to `docker exec` into an arm, because "an arm's
//! image may have no shell — Spate's is distroless — and the same measurement
//! must work for every arm regardless of base image". That reasoning is about a
//! quantity **every arm must produce identically**: CPU and memory exist for all
//! of them, and a mechanism that worked on some images would silently yield a
//! different measurement for different arms, which destroys the comparison the
//! numbers rest on.
//!
//! GC pauses are not that kind of quantity. They do not exist for a non-GC arm,
//! so there is no cross-arm ratio for a JVM-only mechanism to distort; the
//! asymmetry is in the runtimes and not in the instrument.
//!
//! And the objection does not actually apply anyway. [`read_gc_log`] uses
//! `docker cp`, which reads the container's filesystem through the daemon's
//! archive endpoint and needs no shell, no `PATH` and no utilities in the image.
//! That was checked against the arm the objection is about rather than assumed:
//! `docker cp` reads a file out of the Spate arm's distroless image, and
//! `docker run --entrypoint /bin/sh` on the same image fails with
//! `stat /bin/sh: no such file or directory`. The constraint that really binds
//! is that the file has to be there, which is a property of the arm being a JVM
//! rather than of its base image. For an arm that logs GC to its console
//! instead, [`parse_gc_log`] takes the text from anywhere, so a
//! [`gc_log_from_console`] capture parses identically.
//!
//! **Copy the log before the container is removed.** `sampler::stop_sut` runs
//! `docker logs` and then `docker rm -f`; a `docker cp` after that has nothing to
//! read. Copy it after the samplers have stopped and before the containers go,
//! so that whatever cost the copy has lands outside the measurement window.
//!
//! # Parsing defensively
//!
//! Unified logging's shape varies between JDK releases, and the failure mode of
//! a lenient parser here is the worst one available: a parser that skips lines it
//! does not recognise reports "no GC" for an arm that spent seconds paused, and
//! "no GC" is the most flattering possible answer. So the rules are:
//!
//! * A line with no `[...]` decorations at all is foreign — an application log
//!   line sharing the console — and is counted, not parsed.
//! * A `gc`-tagged line that announces a pause and whose duration cannot be read
//!   is an **error**, never a skipped line. See
//!   [`JvmError::UnparseablePause`].
//! * A log carrying no evidence that a collector ever initialised is an error,
//!   even if it contains pauses. This is also what catches a **rotated** log: the
//!   JVM's default file output is `filecount=5,filesize=20M`, so a long run
//!   silently discards its own beginning, and a log that lost its initialisation
//!   block is a log whose pause total is missing an unknown number of pauses.
//! * Zero pauses is a legitimate answer, but only from a log that positively
//!   shows a collector coming up. "The JVM never collected" and "we could not
//!   read the log" must not produce the same output.

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use crate::docker::docker_try;

/// The unified-logging levels, used to tell a level decoration from a tag one.
const LEVELS: [&str; 6] = ["off", "trace", "debug", "info", "warning", "error"];

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

/// Why a GC measurement could not be made.
///
/// Typed, and returned in place of every empty summary this module could have
/// produced instead. The value of the type is entirely in what it makes
/// impossible: a record carrying `"gc_pause_total_us": {"value": 0.0}` for an
/// arm whose log could not be read, which is indistinguishable from an arm that
/// genuinely never paused and flatters exactly the arm whose instrumentation
/// broke.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JvmError {
    /// The GC log could not be fetched out of the container.
    LogUnavailable {
        /// Container the log was sought in.
        container: String,
        /// Path inside that container.
        path: String,
        /// What `docker` said.
        why: String,
    },
    /// The log was fetched but could not be read off disk.
    ReadFailed(String),
    /// Nothing in the text carries unified-logging decorations.
    ///
    /// Either the file is empty, or the JVM was configured with `:none`
    /// decorators, or this is not a GC log at all.
    NotUnifiedLogging {
        /// Lines examined.
        lines: usize,
    },
    /// The text is unified logging, but no line carries the `gc` tag.
    NoGcTaggedLines {
        /// Decorated lines examined.
        decorated: usize,
    },
    /// No line shows a collector initialising.
    ///
    /// The completeness check, and the one that catches a rotated log: the JVM's
    /// default file output keeps five 20 MiB files, so a long run discards its
    /// own beginning and leaves a log whose pause total silently omits an
    /// unknown number of pauses.
    NoCollectorInitialised {
        /// Lines carrying the `gc` tag.
        gc_lines: usize,
        /// Pauses found despite the missing initialisation block.
        pauses: usize,
    },
    /// A line announced a pause and its duration could not be read.
    UnparseablePause {
        /// The offending line, verbatim.
        line: String,
        /// What was wrong with it.
        why: String,
    },
    /// A size could not be read where one was required.
    UnparseableSize {
        /// The offending line, verbatim.
        line: String,
        /// The token that failed.
        token: String,
    },
    /// A window was asked for over a log whose lines carry no uptime.
    NoUptimeDecoration,
}

impl fmt::Display for JvmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LogUnavailable {
                container,
                path,
                why,
            } => write!(
                f,
                "no GC log at {path} in {container}: {why}. It is copied with `docker cp`, \
                 which needs no shell in the image but does need the container to still \
                 exist — copy it before `docker rm -f`, and check that the arm's JVM was \
                 given -Xlog:gc*"
            ),
            Self::ReadFailed(e) => write!(f, "the copied GC log could not be read: {e}"),
            Self::NotUnifiedLogging { lines } => write!(
                f,
                "none of the {lines} line(s) carries unified-logging decorations, so this \
                 is not a -Xlog:gc* log. Reporting no pauses from it would say the arm \
                 never collected, which is a claim about a measurement nobody made"
            ),
            Self::NoGcTaggedLines { decorated } => write!(
                f,
                "{decorated} decorated line(s) and not one tagged `gc`. The output is \
                 unified logging from some other subsystem; -Xlog:gc* was not in force"
            ),
            Self::NoCollectorInitialised { gc_lines, pauses } => write!(
                f,
                "{gc_lines} gc-tagged line(s) and {pauses} pause(s), but nothing showing a \
                 collector initialising. The log is incomplete — most likely rotated away, \
                 since the JVM's default file output keeps five 20 MiB files and discards \
                 the beginning of a long run — so its pause total omits an unknown number \
                 of pauses. Set filecount=0 on the -Xlog output to disable rotation"
            ),
            Self::UnparseablePause { line, why } => write!(
                f,
                "a GC pause line did not parse ({why}): {line:?}. Skipping it would report \
                 a shorter pause total for an arm whose log format this binary has not \
                 caught up with, and a shorter pause total is a better-looking result"
            ),
            Self::UnparseableSize { line, token } => write!(
                f,
                "the size {token:?} in {line:?} did not parse. Heap figures are published \
                 in bytes, and a size read wrongly is worse than one not read at all"
            ),
            Self::NoUptimeDecoration => write!(
                f,
                "the GC log carries no uptime decoration, so pauses cannot be bounded to \
                 the measurement window. Add `uptime` to the -Xlog decorators, or take the \
                 summary over the whole log and say so"
            ),
        }
    }
}

impl std::error::Error for JvmError {}

// ---------------------------------------------------------------------------
// Lines
// ---------------------------------------------------------------------------

/// One unified-logging line, split into its decorations and its message.
#[derive(Clone, Debug, PartialEq)]
struct Decorated<'a> {
    /// Seconds since JVM start, from the `uptime` decoration.
    uptime_s: Option<f64>,
    /// The tag list, in order, unpadded.
    tags: Vec<&'a str>,
    /// Everything after the decorations.
    message: &'a str,
}

impl<'a> Decorated<'a> {
    /// Splits a line into leading `[...]` decorations and a message.
    ///
    /// The decorator **order is fixed by the JVM** (`logDecorators.hpp`) and tags
    /// are always last, whatever subset is enabled — which is what makes "the
    /// last bracket group is the tags" a rule rather than a guess. The uptime is
    /// found by shape (a decoration that parses as a number of seconds) rather
    /// than by position, so a log configured with a different decorator set still
    /// yields one.
    ///
    /// The tag field is space-padded to the width of the widest tag set seen so
    /// far, and that width **grows part-way through a file** — `[gc]` on the
    /// first line becomes `[gc     ]` and then `[gc          ]` as longer tag sets
    /// appear. Every field is trimmed for that reason; a fixed-width reader would
    /// work on the JDK 17 log this arm produces and fail on the JDK 25 one.
    fn parse(line: &'a str) -> Option<Self> {
        let mut rest = line;
        let mut fields: Vec<&str> = Vec::new();
        while let Some(tail) = rest.strip_prefix('[') {
            let (field, after) = tail.split_once(']')?;
            fields.push(field.trim());
            rest = after;
        }
        let tags = fields.pop()?;
        Some(Self {
            uptime_s: fields.iter().find_map(|f| parse_uptime(f)),
            tags: tags.split(',').map(str::trim).collect(),
            message: rest.trim(),
        })
    }

    /// Whether the tag list contains `tag` exactly.
    ///
    /// Exactly, not as a prefix: `gc` and `gcold` are different tags, and
    /// `-Xlog:gc*` matches a tag *set* containing `gc` rather than a tag whose
    /// name starts with it.
    fn has_tag(&self, tag: &str) -> bool {
        self.tags.contains(&tag)
    }
}

/// Reads an uptime decoration such as `0.228s`, rejecting a level or a tag list.
fn parse_uptime(field: &str) -> Option<f64> {
    if LEVELS.contains(&field) {
        return None;
    }
    field.strip_suffix('s')?.parse().ok()
}

/// Reads a JVM size token — `256M`, `9788K`, `8G`, `512B` — as bytes.
///
/// Returns `None` rather than guessing on anything else. The unit is mandatory
/// because the JVM always prints one here, and a bare number would be ambiguous
/// between bytes and kilobytes in a way that moves a published figure by 1024.
fn parse_size(token: &str) -> Option<u64> {
    let token = token.trim();
    let (digits, scale) = match token.chars().last()? {
        'B' => (&token[..token.len() - 1], 1_u64),
        'K' => (&token[..token.len() - 1], 1024),
        'M' => (&token[..token.len() - 1], 1024 * 1024),
        'G' => (&token[..token.len() - 1], 1024 * 1024 * 1024),
        'T' => (&token[..token.len() - 1], 1024_u64.pow(4)),
        _ => return None,
    };
    let value: f64 = digits.parse().ok()?;
    if value < 0.0 {
        return None;
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "heap sizes are small integers in their printed unit"
    )]
    Some((value * scale as f64) as u64)
}

// ---------------------------------------------------------------------------
// What a log contains
// ---------------------------------------------------------------------------

/// The heap occupancy a pause line reports: `48M->20M(256M)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeapTriple {
    /// Occupancy immediately before the collection.
    pub before_bytes: u64,
    /// Occupancy immediately after it — the live set.
    pub after_bytes: u64,
    /// Heap capacity **committed at that moment**, which is not the configured
    /// maximum: a JVM grows toward its maximum and can shrink again after a full
    /// collection.
    pub capacity_bytes: u64,
}

/// One stop-the-world pause.
#[derive(Clone, Debug, PartialEq)]
pub struct Pause {
    /// The `GC(n)` cycle this pause belongs to, where the line carried one.
    pub gc_id: Option<u64>,
    /// Seconds since JVM start.
    pub uptime_s: Option<f64>,
    /// ZGC's generation marker (`Y`, `O`), where the line carried one. Kept
    /// because ZGC logs a young and an old pause of the same name in one cycle,
    /// and without it they look like one pause logged twice.
    pub generation: Option<String>,
    /// What the collector called it — `Young (Normal) (G1 Evacuation Pause)`,
    /// `Full (System.gc())`, `Mark Start (Major)`.
    pub label: String,
    /// Duration, microseconds.
    pub us: f64,
    /// The heap occupancy the line reported, where it reported one. ZGC's pause
    /// lines carry none.
    pub heap: Option<HeapTriple>,
}

/// What the collector was configured with, from its initialisation block.
///
/// Every field is optional because different collectors print different subsets:
/// G1 writes `Heap Max Capacity`, ZGC writes `Max Capacity`, and a JDK old
/// enough writes neither. An absent field stays absent — the point of the whole
/// exercise is the gap between configured and used, and inventing either side of
/// it would defeat the measurement.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HeapConfig {
    /// The collector, from `Using G1` or `Initializing The Z Garbage Collector`.
    pub collector: Option<String>,
    /// The JVM version, from the initialisation block.
    pub version: Option<String>,
    /// `-Xms`, as the collector resolved it.
    pub initial_bytes: Option<u64>,
    /// The smallest the heap may shrink to.
    pub min_bytes: Option<u64>,
    /// `-Xmx`, as the collector resolved it. The **configured** side of
    /// "configured versus used".
    pub max_bytes: Option<u64>,
}

/// Everything one GC log yielded.
#[derive(Clone, Debug, PartialEq)]
pub struct GcLog {
    /// Every pause, in the order they were logged.
    pub pauses: Vec<Pause>,
    /// The configured heap.
    pub heap: HeapConfig,
    /// Lines carrying unified-logging decorations.
    pub decorated_lines: usize,
    /// Of those, lines tagged `gc`.
    pub gc_lines: usize,
    /// Lines with no decorations at all — an application sharing the console.
    /// Counted rather than silently dropped, so a record can say the GC figures
    /// were read out of a mixed stream.
    pub foreign_lines: usize,
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parses `-Xlog:gc*` output, from a file or from a console capture.
///
/// # Errors
///
/// [`JvmError::NotUnifiedLogging`], [`JvmError::NoGcTaggedLines`],
/// [`JvmError::NoCollectorInitialised`], [`JvmError::UnparseablePause`] and
/// [`JvmError::UnparseableSize`]. None of them can be turned into a summary: see
/// the module docs for why an unreadable log must not become a quiet zero.
pub fn parse_gc_log(text: &str) -> Result<GcLog, JvmError> {
    let mut log = GcLog {
        pauses: Vec::new(),
        heap: HeapConfig::default(),
        decorated_lines: 0,
        gc_lines: 0,
        foreign_lines: 0,
    };
    let mut lines = 0usize;

    for raw in text.lines() {
        if raw.trim().is_empty() {
            continue;
        }
        lines += 1;
        let Some(line) = Decorated::parse(raw) else {
            log.foreign_lines += 1;
            continue;
        };
        log.decorated_lines += 1;
        if !line.has_tag("gc") {
            continue;
        }
        log.gc_lines += 1;

        read_heap_config(&line, raw, &mut log.heap)?;

        // A `gc,start` line is the *announcement* of a pause — the same text
        // without a duration, logged before the pause happens. Counting it would
        // double every G1 pause; treating its missing duration as unparseable
        // would refuse every G1 log.
        if line.has_tag("start") {
            continue;
        }
        if let Some(p) = parse_pause(&line, raw)? {
            log.pauses.push(p);
        }
    }

    if log.decorated_lines == 0 {
        return Err(JvmError::NotUnifiedLogging { lines });
    }
    if log.gc_lines == 0 {
        return Err(JvmError::NoGcTaggedLines {
            decorated: log.decorated_lines,
        });
    }
    // Positive evidence that this log begins where the JVM did. Without it a
    // pause total is a lower bound of unknown tightness, and a rotated log is
    // exactly that.
    if log.heap.collector.is_none() && log.heap.max_bytes.is_none() {
        return Err(JvmError::NoCollectorInitialised {
            gc_lines: log.gc_lines,
            pauses: log.pauses.len(),
        });
    }
    Ok(log)
}

/// Reads the collector's initialisation block into `heap`.
///
/// Matched by key **suffix** rather than by exact text, because G1 prints
/// `Heap Max Capacity: 1863M` and ZGC prints `Max Capacity: 512M` for the same
/// quantity, and a parser keyed on either one alone reports "no configured heap"
/// for the other collector.
fn read_heap_config(
    line: &Decorated<'_>,
    raw: &str,
    heap: &mut HeapConfig,
) -> Result<(), JvmError> {
    if let Some(name) = line.message.strip_prefix("Using ") {
        heap.collector = Some(name.trim().to_owned());
        return Ok(());
    }
    if let Some(rest) = line.message.strip_prefix("Initializing ") {
        heap.collector = Some(rest.trim().to_owned());
        return Ok(());
    }
    if !line.has_tag("init") {
        return Ok(());
    }
    let Some((key, value)) = line.message.split_once(':') else {
        return Ok(());
    };
    let (key, value) = (key.trim(), value.trim());
    if key == "Version" {
        heap.version = Some(value.to_owned());
        return Ok(());
    }
    // ZGC prints `Soft Max Capacity` directly after `Max Capacity`, and the
    // suffix match below would take the soft limit as the configured heap — the
    // later line wins. They are equal only while `-Xmx` is explicit; wherever
    // the JVM derives its heap, the soft limit is the lower number, and the gap
    // between configured and used is the whole point of this module.
    if key.starts_with("Soft ") {
        return Ok(());
    }
    let slot = if key.ends_with("Max Capacity") {
        &mut heap.max_bytes
    } else if key.ends_with("Initial Capacity") {
        &mut heap.initial_bytes
    } else if key.ends_with("Min Capacity") {
        &mut heap.min_bytes
    } else {
        return Ok(());
    };
    // A capacity line whose value does not parse is an error, not an omission:
    // the whole point of this module is the gap between configured and used, and
    // a silently absent configured side collapses the comparison.
    *slot = Some(parse_size(value).ok_or_else(|| JvmError::UnparseableSize {
        line: raw.to_owned(),
        token: value.to_owned(),
    })?);
    Ok(())
}

/// Reads a pause from a `gc`-tagged line, or `None` if the line is not one.
///
/// The grammar, which holds across every collector shipped in JDK 17 and JDK 25
/// and was checked against G1 and ZGC output from this rig:
///
/// ```text
/// GC(<id>) [<generation>:] Pause <label...> [<before>-><after>(<capacity>)] <d>ms
/// ```
///
/// A line is a pause when, after the optional `GC(n)` and generation markers,
/// the message begins with `Pause`. G1 and Parallel log one summary line per
/// pause under the bare `gc` tag; ZGC logs each of its pauses under `gc,phases`
/// and has no summary line, so both tags are accepted and the `Pause` keyword
/// rather than the tag is what decides. G1's own `gc,phases` lines name sub-phases
/// (`Evacuate Collection Set: 6.0ms`) and never say `Pause`, so they do not
/// collide.
///
/// Once a line has announced a pause, **its duration is mandatory**. That is the
/// rule that stops a format change from being reported as a shorter pause total.
fn parse_pause(line: &Decorated<'_>, raw: &str) -> Result<Option<Pause>, JvmError> {
    let bad = |why: &str| JvmError::UnparseablePause {
        line: raw.to_owned(),
        why: why.to_owned(),
    };

    let mut rest = line.message;
    let mut gc_id = None;
    if let Some(tail) = rest.strip_prefix("GC(")
        && let Some((id, after)) = tail.split_once(')')
    {
        gc_id = id.parse().ok();
        rest = after.trim_start();
    }
    // ZGC's generation marker. Two characters and a colon, before `Pause`.
    let mut generation = None;
    if let Some((head, after)) = rest.split_once(": ")
        && head.len() <= 2
        && !head.is_empty()
        && head.chars().all(char::is_alphanumeric)
    {
        generation = Some(head.to_owned());
        rest = after.trim_start();
    }
    let Some(body) = rest.strip_prefix("Pause") else {
        return Ok(None);
    };
    let body = body.trim();

    let mut tokens: Vec<&str> = body.split_whitespace().collect();
    let duration = tokens
        .pop()
        .and_then(|t| t.strip_suffix("ms"))
        .and_then(|t| t.parse::<f64>().ok())
        .ok_or_else(|| bad("the line announces a pause but ends in no <n>ms duration"))?;

    // A token containing `->` is a heap triple, and having decided that it is
    // one, failing to read it is an error rather than an absence.
    let heap = match tokens.last().filter(|t| t.contains("->")) {
        Some(t) => {
            let triple = parse_heap_triple(t).ok_or_else(|| JvmError::UnparseableSize {
                line: raw.to_owned(),
                token: (*t).to_owned(),
            })?;
            tokens.pop();
            Some(triple)
        }
        None => None,
    };

    Ok(Some(Pause {
        gc_id,
        uptime_s: line.uptime_s,
        generation,
        label: tokens.join(" "),
        us: duration * 1000.0,
        heap,
    }))
}

/// Reads `48M->20M(256M)`.
fn parse_heap_triple(token: &str) -> Option<HeapTriple> {
    let (before, rest) = token.split_once("->")?;
    let (after, capacity) = rest.split_once('(')?;
    let capacity = capacity.strip_suffix(')')?;
    Some(HeapTriple {
        before_bytes: parse_size(before)?,
        after_bytes: parse_size(after)?,
        capacity_bytes: parse_size(capacity)?,
    })
}

// ---------------------------------------------------------------------------
// Summarising
// ---------------------------------------------------------------------------

/// The pause distribution and the heap comparison, for one JVM over one window.
#[derive(Clone, Debug, PartialEq)]
pub struct GcSummary {
    /// Pauses counted.
    pub pauses: usize,
    /// Total time stopped, microseconds. The figure that matters most, because
    /// it is the part of the window in which the arm did no work.
    pub total_us: f64,
    /// The longest single pause, microseconds.
    pub max_us: f64,
    /// The name of the longest pause, so "why was it X and not 2X?" has an
    /// answer with a collector phase attached to it.
    pub max_label: String,
    /// Median pause, microseconds.
    pub p50_us: f64,
    /// 99th percentile pause, microseconds.
    pub p99_us: f64,
    /// 99.9th percentile pause, microseconds.
    pub p999_us: f64,
    /// Mean pause, microseconds. Published last and never alone: a collector
    /// that pauses ten thousand times for 1 ms and once for 900 ms has a mean of
    /// 1.09 ms, which describes nothing anybody cares about.
    pub mean_us: f64,
    /// Earliest pause's uptime, seconds — what interval these figures cover.
    pub from_uptime_s: Option<f64>,
    /// Latest pause's uptime, seconds.
    pub to_uptime_s: Option<f64>,
    /// The configured heap, from the collector's initialisation block.
    pub configured: HeapConfig,
    /// The largest heap capacity the JVM actually committed. This is the "used"
    /// side of "configured versus used" in the sense `methodology/` means it —
    /// what the runtime *chose to take* when nothing forced it to economise.
    pub peak_committed_bytes: Option<u64>,
    /// The highest occupancy observed, immediately before a collection.
    pub peak_occupancy_bytes: Option<u64>,
    /// The highest live set observed, immediately after a collection.
    pub peak_live_bytes: Option<u64>,
}

impl GcSummary {
    /// Configured maximum against the largest capacity actually committed.
    ///
    /// `None` when either side is missing, and deliberately not a pair with a
    /// zero in it: the gap between allocation and use is the whole quantity, and
    /// a missing side makes it undefined rather than large.
    #[must_use]
    pub fn configured_versus_committed(&self) -> Option<(u64, u64)> {
        Some((self.configured.max_bytes?, self.peak_committed_bytes?))
    }

    /// The share of the configured maximum heap the JVM actually committed.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "heap sizes stay far below f64's exact range"
    )]
    pub fn committed_share(&self) -> Option<f64> {
        let (configured, committed) = self.configured_versus_committed()?;
        (configured > 0).then(|| committed as f64 / configured as f64)
    }

    /// A one-line account for a record's note.
    #[must_use]
    pub fn provenance(&self) -> String {
        let collector = self.configured.collector.as_deref().unwrap_or("unknown");
        // Absent rather than guessed where the log carries no version line.
        let runtime = self
            .configured
            .version
            .as_deref()
            .map_or(String::new(), |v| format!(" on JVM {v}"));
        let mut s = format!(
            "GC: {collector}{runtime}, {} pause(s) totalling {:.1}ms, max {:.1}ms ({})",
            self.pauses,
            self.total_us / 1000.0,
            self.max_us / 1000.0,
            if self.max_label.is_empty() {
                "-"
            } else {
                &self.max_label
            }
        );
        if let Some((configured, committed)) = self.configured_versus_committed() {
            s.push_str(&format!(
                "; heap {:.0} MiB committed of {:.0} MiB configured",
                committed as f64 / (1024.0 * 1024.0),
                configured as f64 / (1024.0 * 1024.0)
            ));
        }
        s
    }
}

impl GcLog {
    /// Summarises the pauses, optionally bounded to an uptime interval.
    ///
    /// The bound exists because a GC log covers the JVM's whole life while the
    /// measurement window is the sampler's — container start to drain complete —
    /// and the copy is taken later still, after the pipeline has quiesced. Pauses
    /// during the quiesce are real pauses but they are outside the window every
    /// other figure on the record is divided by.
    ///
    /// The mapping is approximate and stated rather than hidden: JVM uptime
    /// starts a little after container start, so bounding by
    /// `[0, window_seconds]` charges the arm for its own start-up exactly as the
    /// sampler's window does. `from_uptime_s` and `to_uptime_s` on the result say
    /// what was actually covered, so a record never has to assume.
    ///
    /// # Errors
    ///
    /// [`JvmError::NoUptimeDecoration`] when a bound is asked for and the log's
    /// lines carry no uptime. Silently ignoring the bound would attribute
    /// out-of-window pauses to the window.
    pub fn summarise(&self, uptime_window: Option<(f64, f64)>) -> Result<GcSummary, JvmError> {
        let mut chosen: Vec<&Pause> = Vec::new();
        for p in &self.pauses {
            match uptime_window {
                None => chosen.push(p),
                Some((from, to)) => {
                    let u = p.uptime_s.ok_or(JvmError::NoUptimeDecoration)?;
                    if u >= from && u < to {
                        chosen.push(p);
                    }
                }
            }
        }

        let mut sorted: Vec<f64> = chosen.iter().map(|p| p.us).collect();
        sorted.sort_by(f64::total_cmp);
        let total: f64 = sorted.iter().sum();
        let worst = chosen
            .iter()
            .max_by(|a, b| a.us.total_cmp(&b.us))
            .map(|p| p.label.clone())
            .unwrap_or_default();

        #[expect(
            clippy::cast_precision_loss,
            reason = "pause counts stay far below f64's exact range"
        )]
        let mean = if sorted.is_empty() {
            0.0
        } else {
            total / sorted.len() as f64
        };

        let triples: Vec<HeapTriple> = chosen.iter().filter_map(|p| p.heap).collect();
        let peak = |f: fn(&HeapTriple) -> u64| triples.iter().map(f).max();

        Ok(GcSummary {
            pauses: sorted.len(),
            total_us: total,
            max_us: sorted.last().copied().unwrap_or(0.0),
            max_label: worst,
            p50_us: percentile(&sorted, 0.50),
            p99_us: percentile(&sorted, 0.99),
            p999_us: percentile(&sorted, 0.999),
            mean_us: mean,
            from_uptime_s: chosen.first().and_then(|p| p.uptime_s),
            to_uptime_s: chosen.last().and_then(|p| p.uptime_s),
            configured: self.heap.clone(),
            peak_committed_bytes: peak(|h| h.capacity_bytes),
            peak_occupancy_bytes: peak(|h| h.before_bytes),
            peak_live_bytes: peak(|h| h.after_bytes),
        })
    }

    /// How many pauses each collector phase contributed, for diagnosis.
    ///
    /// Not published: it exists so that "the arm lost 4 seconds to GC" can be
    /// followed by "to what", which is the question a reader asks next and the
    /// one a single total cannot answer.
    #[must_use]
    pub fn by_label(&self) -> BTreeMap<String, (usize, f64)> {
        let mut out: BTreeMap<String, (usize, f64)> = BTreeMap::new();
        for p in &self.pauses {
            let e = out.entry(p.label.clone()).or_insert((0, 0.0));
            e.0 += 1;
            e.1 += p.us;
        }
        out
    }
}

/// Nearest-rank percentile over an ascending slice.
///
/// Nearest-rank rather than interpolated, deliberately: every value it can
/// return is a pause that actually happened. An interpolated p99 of a pause
/// distribution is a duration no collector ever spent, and this is a suite that
/// publishes measurements rather than fits.
#[must_use]
#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a rank is bounded by the sample count, which is small"
)]
pub fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = (q * sorted.len() as f64).ceil() as usize;
    sorted[rank.max(1).min(sorted.len()) - 1]
}

// ---------------------------------------------------------------------------
// I/O
// ---------------------------------------------------------------------------

/// Copies a GC log out of a running or stopped container.
///
/// `docker cp` reads the container's filesystem through the daemon rather than
/// running anything inside it, so it needs no shell and works against a
/// distroless image. It does need the container to exist: call this **before**
/// `sampler::stop_sut`, which removes it.
///
/// The copy goes to a temp file and is deleted again, unless `keep` names a file
/// to leave it at. A bind mount would have avoided the round trip and is
/// forbidden here — on macOS it crosses VirtioFS, and the rule for this harness
/// is never to bind-mount a measured path.
///
/// `keep` exists because the parsed `gc_*` metrics answer only the questions
/// this module already asks. Sizing a heap needs the log itself — post-Full-GC
/// occupancy, whether compressed oops came up — and on a tuning box nothing but
/// `results/` is uploaded, so a discarded copy is gone with the instance.
///
/// # Errors
///
/// [`JvmError::LogUnavailable`] if the container or the path is not there, and
/// [`JvmError::ReadFailed`] if the copy cannot be read back.
pub fn read_gc_log(container: &str, path: &str, keep: Option<&Path>) -> Result<String, JvmError> {
    let dest = match keep {
        Some(at) => {
            if let Some(parent) = at.parent() {
                std::fs::create_dir_all(parent).map_err(|e| JvmError::ReadFailed(e.to_string()))?;
            }
            at.to_path_buf()
        }
        None => std::env::temp_dir().join(format!(
            "spate-bench-gc-{}-{}.log",
            std::process::id(),
            crate::report::now_ms()
        )),
    };
    let dest_str = dest.to_string_lossy().into_owned();
    let source = format!("{container}:{path}");
    docker_try(&["cp", &source, &dest_str]).map_err(|why| JvmError::LogUnavailable {
        container: container.to_owned(),
        path: path.to_owned(),
        why,
    })?;
    let text = std::fs::read_to_string(&dest).map_err(|e| JvmError::ReadFailed(e.to_string()));
    if keep.is_none() {
        let _ = std::fs::remove_file(&dest);
    }
    text
}

/// The container's console, for an arm that logs GC to its console rather than
/// to a file.
///
/// Offered beside [`read_gc_log`] because the two are interchangeable to
/// [`parse_gc_log`]: an arm configured with `-Xlog:gc*:stdout:uptime,level,tags`
/// interleaves its GC lines with its application logging, and the parser counts
/// the application's lines as foreign rather than choking on them.
///
/// **Both** streams, joined, rather than `docker::docker_try`'s stdout. That
/// helper returns stdout on success and stderr only on failure, which is right
/// for a CLI call whose output *is* the answer and wrong here: `-Xlog` writes to
/// whichever stream the flags name, a JVM's default is stdout but Flink's log4j
/// console appender can be pointed at either, and a GC log read from the wrong
/// stream is empty rather than short. Interleaving between the two streams is
/// lost and does not matter — each stream keeps its own order, and every line
/// carries its own uptime.
///
/// # Errors
///
/// [`JvmError::LogUnavailable`] if the container's logs cannot be read.
pub fn gc_log_from_console(container: &str) -> Result<String, JvmError> {
    let out = std::process::Command::new("docker")
        .args(["logs", container])
        .output()
        .map_err(|e| JvmError::LogUnavailable {
            container: container.to_owned(),
            path: "<console>".to_owned(),
            why: e.to_string(),
        })?;
    if !out.status.success() {
        return Err(JvmError::LogUnavailable {
            container: container.to_owned(),
            path: "<console>".to_owned(),
            why: String::from_utf8_lossy(&out.stderr).trim().to_owned(),
        });
    }
    Ok(format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    ))
}

/// Reads and summarises one JVM container's GC log.
///
/// The one entry point the driver needs per JVM container. Call it after the
/// samplers have stopped and before the containers are removed.
///
/// # Errors
///
/// Every variant of [`JvmError`]. None of them is recoverable into a summary; a
/// caller that cannot get one records the arm **without** GC metrics rather than
/// with zeroes. See the module docs.
pub fn measure(
    container: &str,
    path: &str,
    uptime_window: Option<(f64, f64)>,
    keep: Option<&Path>,
) -> Result<GcSummary, JvmError> {
    parse_gc_log(&read_gc_log(container, path, keep)?)?.summarise(uptime_window)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verbatim `-Xlog:gc*:file=…:uptime,level,tags` output from a G1 JVM,
    /// abridged to the lines that carry information. The tag column widens from
    /// `[gc     ]` to `[gc             ]` part-way through, exactly as the real
    /// file does once a longer tag set appears — which is why nothing here may be
    /// read by column position.
    const G1_LOG: &str = "\
[0.003s][info][gc,init] CardTable entry size: 512
[0.003s][info][gc     ] Using G1
[0.003s][info][gc,init] Version: 25.0.3+9-LTS (release)
[0.003s][info][gc,init] CPUs: 18 total, 18 available
[0.003s][info][gc,init] Memory: 128G
[0.003s][info][gc,init] Heap Region Size: 1M
[0.003s][info][gc,init] Heap Min Capacity: 8M
[0.003s][info][gc,init] Heap Initial Capacity: 256M
[0.003s][info][gc,init] Heap Max Capacity: 256M
[0.003s][info][gc,init] Pre-touch: Disabled
[0.225s][info][gc,start    ] GC(0) Pause Young (Normal) (G1 Evacuation Pause)
[0.226s][info][gc,task     ] GC(0) Using 6 workers of 14 for evacuation
[0.228s][info][gc,phases   ] GC(0)   Pre Evacuate Collection Set: 0.06ms
[0.228s][info][gc,phases   ] GC(0)   Evacuate Collection Set: 1.79ms
[0.228s][info][gc,heap     ] GC(0) Eden regions: 47->0(96)
[0.228s][info][gc,metaspace] GC(0) Metaspace: 9788K(9984K)->9788K(9984K) NonClass: 8592K(8704K)->8592K(8704K)
[0.228s][info][gc          ] GC(0) Pause Young (Normal) (G1 Evacuation Pause) 48M->20M(256M) 2.739ms
[0.228s][info][gc,cpu      ] GC(0) User=0.01s Sys=0.00s Real=0.00s
[0.239s][info][gc          ] GC(1) Pause Young (Normal) (G1 Evacuation Pause) 116M->30M(256M) 1.341ms
[0.252s][info][gc          ] GC(2) Pause Young (Normal) (G1 Evacuation Pause) 156M->39M(256M) 2.478ms
[0.263s][info][gc          ] GC(3) Pause Young (Normal) (G1 Evacuation Pause) 174M->37M(256M) 0.926ms
[0.267s][info][gc             ] GC(4) Pause Full (System.gc()) 62M->3M(20M) 2.182ms
[0.267s][info][gc,cpu         ] GC(4) User=0.00s Sys=0.00s Real=0.00s
";

    /// Verbatim generational-ZGC output. Its pauses are logged under `gc,phases`
    /// with a `Y:`/`O:` generation marker and no heap triple, and it has no
    /// single summary line per cycle — the shape a parser written only against G1
    /// silently reports as "no GC".
    const ZGC_LOG: &str = "\
[0.003s][info][gc,init] Initializing The Z Garbage Collector
[0.003s][info][gc,init] Version: 25.0.3+9-LTS (release)
[0.003s][info][gc,init] Min Capacity: 8M
[0.003s][info][gc,init] Initial Capacity: 512M
[0.003s][info][gc,init] Max Capacity: 512M
[0.003s][info][gc,init] Soft Max Capacity: 460M
[0.188s][info][gc,phases   ] GC(0) Y: Pause Mark Start (Major) 0.004ms
[0.191s][info][gc,phases   ] GC(0) Y: Pause Mark End 0.004ms
[0.192s][info][gc,phases   ] GC(0) Y: Pause Relocate Start 0.007ms
[0.193s][info][gc,phases   ] GC(0) O: Pause Mark End 0.005ms
[0.195s][info][gc,phases   ] GC(0) O: Pause Relocate Start 0.003ms
";

    #[test]
    fn zgcs_soft_limit_is_not_mistaken_for_its_configured_heap() {
        // `Soft Max Capacity` is printed straight after `Max Capacity` and ends
        // with the same words, so a suffix match takes the soft limit — a heap
        // this arm was never configured with, and the smaller of the two.
        let log = parse_gc_log(ZGC_LOG).expect("a real ZGC log parses");
        assert_eq!(log.heap.max_bytes, Some(512 * 1024 * 1024));
    }

    #[test]
    fn provenance_names_the_runtime_that_produced_the_pauses() {
        let summary = parse_gc_log(ZGC_LOG)
            .expect("a real ZGC log parses")
            .summarise(None)
            .expect("a summary");
        assert!(
            summary
                .provenance()
                .contains("on JVM 25.0.3+9-LTS (release)"),
            "{}",
            summary.provenance()
        );
    }

    #[test]
    fn parses_a_real_g1_log_from_the_arms_own_collector() {
        let log = parse_gc_log(G1_LOG).expect("a real G1 log parses");
        assert_eq!(log.pauses.len(), 5);
        assert_eq!(log.heap.collector.as_deref(), Some("G1"));
        assert_eq!(log.heap.version.as_deref(), Some("25.0.3+9-LTS (release)"));
        assert_eq!(log.foreign_lines, 0);

        let first = &log.pauses[0];
        assert_eq!(first.gc_id, Some(0));
        assert_eq!(first.label, "Young (Normal) (G1 Evacuation Pause)");
        assert!((first.us - 2739.0).abs() < 1e-9);
        assert_eq!(
            first.heap,
            Some(HeapTriple {
                before_bytes: 48 * 1024 * 1024,
                after_bytes: 20 * 1024 * 1024,
                capacity_bytes: 256 * 1024 * 1024,
            })
        );
        // The last pause's tag column is four characters wider than the first's,
        // and both must read the same.
        assert_eq!(log.pauses[4].label, "Full (System.gc())");
    }

    /// G1 logs every pause twice: once under `gc,start` when it begins, without a
    /// duration, and once under `gc` when it ends. Counting the announcement
    /// would double every pause count; treating its missing duration as a parse
    /// failure would refuse every G1 log ever written.
    #[test]
    fn a_pause_announcement_is_not_itself_a_pause() {
        let log = parse_gc_log(G1_LOG).expect("a real G1 log parses");
        assert_eq!(log.pauses.len(), 5, "one pause per GC(n), not two");
        assert!(G1_LOG.contains("[gc,start    ] GC(0) Pause Young"));
    }

    /// A parser written only against G1's summary line reports "no GC" for a ZGC
    /// arm that paused every cycle, because ZGC logs its pauses under `gc,phases`
    /// and writes no summary. The keyword rather than the tag decides.
    #[test]
    fn a_zgc_pause_logged_under_a_phases_tag_is_still_a_pause() {
        let log = parse_gc_log(ZGC_LOG).expect("a real ZGC log parses");
        assert_eq!(log.pauses.len(), 5);
        assert_eq!(log.pauses[0].generation.as_deref(), Some("Y"));
        assert_eq!(log.pauses[0].label, "Mark Start (Major)");
        assert!((log.pauses[0].us - 4.0).abs() < 1e-9);
        // Its pause lines carry no occupancy, which is an absence rather than a
        // zero-byte heap.
        assert!(log.pauses[0].heap.is_none());
        // The young and old pauses of one cycle share a name; without the
        // generation marker they read as one pause logged twice.
        assert_eq!(log.pauses[1].label, log.pauses[3].label);
        assert_ne!(log.pauses[1].generation, log.pauses[3].generation);
    }

    /// G1 writes `Heap Max Capacity` and ZGC writes `Max Capacity` for the same
    /// quantity. A parser keyed on either exact string publishes "no configured
    /// heap" for the other collector, which reads as a JVM without a limit.
    #[test]
    fn reads_configured_heap_from_either_collectors_initialisation_block() {
        let g1 = parse_gc_log(G1_LOG).expect("a real G1 log parses");
        assert_eq!(g1.heap.max_bytes, Some(256 * 1024 * 1024));
        assert_eq!(g1.heap.initial_bytes, Some(256 * 1024 * 1024));
        assert_eq!(g1.heap.min_bytes, Some(8 * 1024 * 1024));

        let z = parse_gc_log(ZGC_LOG).expect("a real ZGC log parses");
        assert_eq!(z.heap.max_bytes, Some(512 * 1024 * 1024));
        assert_eq!(z.heap.initial_bytes, Some(512 * 1024 * 1024));
        assert_eq!(z.heap.collector.as_deref(), Some("The Z Garbage Collector"));
    }

    /// The quantity `methodology/` promises: what the JVM was allowed against
    /// what it actually took. The committed figure is the maximum capacity the
    /// collector ever reported, not the capacity at the end — a full collection
    /// shrinks the heap, and reading the last line would report the JVM as having
    /// used 20 MiB of the 256 MiB it grew to.
    #[test]
    fn reports_the_gap_between_configured_and_committed_heap() {
        let summary = parse_gc_log(G1_LOG)
            .expect("a real G1 log parses")
            .summarise(None)
            .expect("pauses summarise");

        assert_eq!(
            summary.configured_versus_committed(),
            Some((256 * 1024 * 1024, 256 * 1024 * 1024))
        );
        assert!((summary.committed_share().expect("both sides known") - 1.0).abs() < 1e-9);
        // Occupancy peaked at 174 MiB and the live set at 39 MiB, and the last
        // pause shrank the heap to 20 MiB — which is why the maximum is taken.
        assert_eq!(summary.peak_occupancy_bytes, Some(174 * 1024 * 1024));
        assert_eq!(summary.peak_live_bytes, Some(39 * 1024 * 1024));
        assert_eq!(summary.peak_committed_bytes, Some(256 * 1024 * 1024));
    }

    /// An absent side of the comparison stays absent. `Some((max, 0))` would say
    /// the JVM committed nothing, which is a stronger claim than "we did not
    /// read it" and reads as a runtime with no footprint at all.
    #[test]
    fn a_missing_side_of_the_heap_comparison_is_absent_and_not_zero() {
        let summary = parse_gc_log(ZGC_LOG)
            .expect("a real ZGC log parses")
            .summarise(None)
            .expect("pauses summarise");
        // ZGC's pause lines carry no occupancy, so there is no committed figure.
        assert_eq!(summary.peak_committed_bytes, None);
        assert_eq!(summary.configured_versus_committed(), None);
        assert_eq!(summary.committed_share(), None);
        // The configured side is still known and still published.
        assert_eq!(summary.configured.max_bytes, Some(512 * 1024 * 1024));
    }

    /// A mean pause describes nothing anybody cares about. The distribution is
    /// the measurement, and the maximum is the number that answers "why was it X
    /// and not 2X?".
    #[test]
    fn summarises_pauses_by_distribution_rather_than_by_mean() {
        let summary = parse_gc_log(G1_LOG)
            .expect("a real G1 log parses")
            .summarise(None)
            .expect("pauses summarise");

        assert_eq!(summary.pauses, 5);
        let total = 2739.0 + 1341.0 + 2478.0 + 926.0 + 2182.0;
        assert!((summary.total_us - total).abs() < 1e-9);
        assert!((summary.max_us - 2739.0).abs() < 1e-9);
        assert_eq!(summary.max_label, "Young (Normal) (G1 Evacuation Pause)");
        assert!((summary.mean_us - total / 5.0).abs() < 1e-9);
        // Nearest-rank, so every published percentile is a pause that happened.
        assert!((summary.p50_us - 2182.0).abs() < 1e-9);
        assert!((summary.p99_us - 2739.0).abs() < 1e-9);
        assert!((summary.p999_us - 2739.0).abs() < 1e-9);

        // The phase breakdown that follows "the arm lost 9.7ms to GC" with "to
        // what".
        let by_label = parse_gc_log(G1_LOG)
            .expect("a real G1 log parses")
            .by_label();
        assert_eq!(by_label["Full (System.gc())"].0, 1);
        assert_eq!(by_label["Young (Normal) (G1 Evacuation Pause)"].0, 4);
    }

    #[test]
    fn percentiles_are_nearest_rank_over_the_pauses_that_happened() {
        let sorted = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        assert!((percentile(&sorted, 0.5) - 5.0).abs() < 1e-9);
        assert!((percentile(&sorted, 0.99) - 10.0).abs() < 1e-9);
        assert!((percentile(&sorted, 0.0) - 1.0).abs() < 1e-9);
        assert!((percentile(&sorted, 1.0) - 10.0).abs() < 1e-9);
        assert!((percentile(&[], 0.5) - 0.0).abs() < f64::EPSILON);
    }

    /// The failure this module is built around. A line that announces a pause and
    /// whose duration this binary cannot read is a format it has not caught up
    /// with — and skipping it publishes a shorter pause total, which is a better
    /// result for the arm whose log format changed.
    #[test]
    fn a_pause_whose_duration_cannot_be_read_is_an_error_and_never_a_zero() {
        let broken = "\
[0.003s][info][gc] Using G1
[0.228s][info][gc] GC(0) Pause Young (Normal) (G1 Evacuation Pause) 48M->20M(256M) 2.739 milliseconds
";
        let e = parse_gc_log(broken).expect_err("an unreadable duration must be refused");
        match &e {
            JvmError::UnparseablePause { line, .. } => assert!(line.contains("milliseconds")),
            other => panic!("wrong refusal: {other}"),
        }
        assert!(format!("{e}").contains("shorter pause total"));

        // A heap triple that has been decided to be one and then does not read is
        // the same kind of failure.
        let bad_size = "\
[0.003s][info][gc] Using G1
[0.228s][info][gc] GC(0) Pause Young (Normal) 48X->20M(256M) 2.739ms
";
        assert!(matches!(
            parse_gc_log(bad_size),
            Err(JvmError::UnparseableSize { .. })
        ));
    }

    /// Three ways of having nothing to read, each refused with its own reason,
    /// because the fix for each is different and none of them is "publish a zero".
    #[test]
    fn a_log_that_is_not_a_gc_log_is_refused_by_name() {
        assert!(matches!(
            parse_gc_log(""),
            Err(JvmError::NotUnifiedLogging { lines: 0 })
        ));
        assert!(matches!(
            parse_gc_log("2026-07-25 22:04:11,120 INFO  org.apache.flink.runtime - Starting"),
            Err(JvmError::NotUnifiedLogging { lines: 1 })
        ));
        assert!(matches!(
            parse_gc_log("[0.003s][info][safepoint] Safepoint \"G1CollectForAllocation\"\n"),
            Err(JvmError::NoGcTaggedLines { decorated: 1 })
        ));
    }

    /// The JVM's default file output is `filecount=5,filesize=20M`, so a long run
    /// silently discards the beginning of its own GC log. What is left parses
    /// perfectly and reports a pause total missing an unknown number of pauses —
    /// the one failure here that produces a plausible number rather than an
    /// obvious absence. Requiring positive evidence that the log starts where the
    /// JVM did is what catches it.
    #[test]
    fn a_rotated_log_that_lost_its_initialisation_block_is_refused() {
        let rotated = "\
[812.228s][info][gc          ] GC(9001) Pause Young (Normal) (G1 Evacuation Pause) 48M->20M(256M) 2.739ms
[812.239s][info][gc          ] GC(9002) Pause Young (Normal) (G1 Evacuation Pause) 116M->30M(256M) 1.341ms
";
        let e = parse_gc_log(rotated).expect_err("a rotated log must be refused");
        match e {
            JvmError::NoCollectorInitialised { pauses, .. } => assert_eq!(pauses, 2),
            other => panic!("wrong refusal: {other}"),
        }
        assert!(
            format!("{}", parse_gc_log(rotated).expect_err("refused")).contains("filecount=0"),
            "the refusal must say how to stop it happening again"
        );
    }

    /// "The JVM never collected" and "we could not read the log" must not produce
    /// the same output. A log that shows a collector coming up and contains no
    /// pause is a measurement of zero pauses; one that shows neither is a
    /// refusal.
    #[test]
    fn a_jvm_that_never_collected_is_a_measurement_and_not_an_absence() {
        let quiet = "\
[0.003s][info][gc     ] Using G1
[0.003s][info][gc,init] Heap Max Capacity: 1863M
[0.003s][info][gc,init] Heap Initial Capacity: 250M
";
        let summary = parse_gc_log(quiet)
            .expect("an initialised collector is evidence enough")
            .summarise(None)
            .expect("zero pauses summarise");
        assert_eq!(summary.pauses, 0);
        assert!((summary.total_us - 0.0).abs() < f64::EPSILON);
        assert_eq!(summary.configured.max_bytes, Some(1863 * 1024 * 1024));
        // And it cannot claim a committed heap it never observed.
        assert_eq!(summary.peak_committed_bytes, None);
    }

    /// A GC log covers the JVM's whole life, and the copy is taken after the
    /// pipeline has quiesced — so it runs past the sampler's window at both ends.
    /// Bounding by uptime is what keeps the GC figures on the same interval as
    /// every other number on the record.
    #[test]
    fn pauses_can_be_bounded_to_the_measurement_window_by_uptime() {
        let log = parse_gc_log(G1_LOG).expect("a real G1 log parses");
        let whole = log.summarise(None).expect("pauses summarise");
        assert_eq!(whole.pauses, 5);

        // Everything up to 0.25s: the first two pauses only.
        let bounded = log.summarise(Some((0.0, 0.25))).expect("pauses summarise");
        assert_eq!(bounded.pauses, 2);
        assert!((bounded.total_us - (2739.0 + 1341.0)).abs() < 1e-9);
        assert_eq!(bounded.from_uptime_s, Some(0.228));
        assert_eq!(bounded.to_uptime_s, Some(0.239));
        // And the heap peaks follow the bound, or the two halves of the record
        // would describe different intervals.
        assert_eq!(bounded.peak_occupancy_bytes, Some(116 * 1024 * 1024));

        // Silently ignoring a bound the log cannot honour would attribute
        // out-of-window pauses to the window.
        let undecorated = GcLog {
            pauses: vec![Pause {
                gc_id: Some(0),
                uptime_s: None,
                generation: None,
                label: "Young".to_owned(),
                us: 1000.0,
                heap: None,
            }],
            heap: HeapConfig::default(),
            decorated_lines: 1,
            gc_lines: 1,
            foreign_lines: 0,
        };
        assert!(matches!(
            undecorated.summarise(Some((0.0, 1.0))),
            Err(JvmError::NoUptimeDecoration)
        ));
    }

    /// An arm that logs GC to its console interleaves the two streams. The
    /// application's lines are counted as foreign rather than parsed, so the same
    /// parser serves `docker cp` of a file and `docker logs` of a console.
    #[test]
    fn an_application_sharing_the_console_does_not_disturb_the_gc_figures() {
        let mixed = "\
2026-07-25 22:04:11,120 INFO  org.apache.flink.runtime.taskexecutor.TaskExecutor - Starting
[0.003s][info][gc     ] Using G1
[0.003s][info][gc,init] Heap Max Capacity: 256M
2026-07-25 22:04:12,001 INFO  org.apache.flink.runtime.state - Checkpoint 1 completed
[0.228s][info][gc          ] GC(0) Pause Young (Normal) (G1 Evacuation Pause) 48M->20M(256M) 2.739ms
";
        let log = parse_gc_log(mixed).expect("a mixed stream parses");
        assert_eq!(log.foreign_lines, 2);
        assert_eq!(log.pauses.len(), 1);
        let summary = log.summarise(None).expect("pauses summarise");
        assert!((summary.total_us - 2739.0).abs() < 1e-9);
        assert!(
            summary.provenance().contains("G1"),
            "{}",
            summary.provenance()
        );
        assert!(
            summary.provenance().contains("256 MiB configured"),
            "{}",
            summary.provenance()
        );
    }

    #[test]
    fn reads_the_jvms_own_size_units_and_refuses_a_bare_number() {
        assert_eq!(parse_size("256M"), Some(256 * 1024 * 1024));
        assert_eq!(parse_size("9788K"), Some(9788 * 1024));
        assert_eq!(parse_size("8G"), Some(8 * 1024 * 1024 * 1024));
        assert_eq!(parse_size("512B"), Some(512));
        // A bare number is ambiguous between bytes and kilobytes, which is a
        // factor of 1024 on a published figure.
        assert_eq!(parse_size("256"), None);
        assert_eq!(parse_size(""), None);
    }
}
