//! What an arm's inserts cost ClickHouse, from ClickHouse's own `ProfileEvents`.
//!
//! `methodology/` lists this as "ClickHouse's own `ProfileEvents` CPU-per-row,
//! via `system.query_log`", and it sits close enough to the rule against
//! self-reported numbers that the distinction has to be argued rather than
//! assumed.
//!
//! The rule is "nothing a system reports about itself is used for any published
//! number", and the system under test is the **arm** — Spate, Flink, Kafka
//! Connect. ClickHouse is the shared target every arm writes into: identical for
//! all of them, outside the resource envelope, and no more under test than the
//! broker is. Its accounting of what an arm's inserts cost it is therefore an
//! *external* measurement of that arm, of the same kind as the cgroup counters
//! [`crate::sampler`] reads. The arm is not being asked how much work it did; a
//! third party is being asked how much work it was handed.
//!
//! The argument breaks the moment this number is read as a fact about
//! *ClickHouse*. It is not: it is a fact about what an arm's choice of insert
//! format, batch size and row shape costs the server, which is exactly the
//! quantity rule 5 of the contract ("report the insert format — Native,
//! RowBinary, `JSONEachRow` and a Go SQL driver are not the same amount of
//! server-side work") promises a reader. Nothing here belongs on a chart whose
//! axis is a ClickHouse version.
//!
//! # Attribution: which queries are the arm's
//!
//! `system.query_log` holds **every** query the server ran, and during a drain
//! most of them are the harness's own. The driver polls `SELECT count()` once a
//! second for the whole drain, and the correctness gate then runs `uniqExact`
//! scans over ten million rows. Those are not cheap: one gate scan observed on
//! the reference rig charged 2.89 CPU-seconds to `UserTimeMicroseconds` for a
//! single query. A figure that swept them in would be measuring the harness's
//! polling interval, and it would rise every time somebody made the gate
//! stricter.
//!
//! The base predicates narrow the log to the arm, and each one is doing
//! distinct work:
//!
//! * `query_kind = 'Insert'` — removes the driver's `SELECT count()` polls and
//!   the gate's scans outright, and also the `TRUNCATE` that precedes every
//!   repetition, which the log records as kind `Drop`.
//! * `hasAny(tables, [...])` — the workload's target table plus any
//!   `attribution_tables` the arm's descriptor declares, all **fully
//!   qualified** (`default.sensor_events`), because that is how
//!   `system.query_log` writes them. The extra names carry the MV-landing
//!   shape: an arm that lands nested rows in its own table and flattens with a
//!   materialized view is attributed through the parent insert on that landing
//!   table, which is the row that carries the view's cost.
//! * `query_start_time_microseconds` inside the measurement window — the
//!   sampler's own window, not the driver's clock. See [`Window`].
//! * `is_initial_query` — required by default, so a distributed sub-query is
//!   never counted beside the query that spawned it. When the descriptor
//!   declares `forwarded_inserts = true` the predicate is **inverted** to
//!   `NOT is_initial_query`, never dropped: such an arm's inserts arrive at
//!   the shared server as forwarded executions of an initial query that ran on
//!   the arm's own node, whose log this module never reads, so the strict form
//!   would match nothing — and *no* form would let an initial query and the
//!   forwarded execution it spawned both match, double-counting the arm's
//!   `written_rows` and CPU. One polarity is always present, so
//!   double-counting is impossible by construction.
//!
//! ## What can still leak in, and what leaks out
//!
//! This list is the honest part of the measurement, and it is long on purpose.
//!
//! 1. **Another client inserting into the same table in the same window.**
//!    Nothing in `system.query_log` says "this row was the arm". The arm lock in
//!    [`crate::sampler::ArmLock`] serialises arms across the host, so this is a
//!    protocol violation rather than a normal case — but it has happened once
//!    already (two drivers sharing one ClickHouse), and it would be invisible
//!    here.
//! 2. **Background merges are not counted at all.** A `MergeTree` merge appears
//!    in `system.part_log`, never in `system.query_log`, so this figure is the
//!    cost of the *insert* and excludes the merging that the insert made
//!    inevitable. That is a real asymmetry between arms rather than a rounding
//!    error: an arm writing 25k-row batches creates far more parts than one
//!    writing 262k-row batches, and the difference in merge cost lands nowhere
//!    in this number. Published records must say so.
//! 3. **Asynchronous inserts are not attributable.** ClickHouse 26.3 ships
//!    `async_insert = 1` as the compiled default (`changed = 0` on the live
//!    reference server). Under it the client's query returns once the data is
//!    buffered, so the query's own `ProfileEvents` carry the parse cost and not
//!    the flush cost, and `written_rows` on that row is zero. Spate's
//!    `clickhouse-rs` client sets `async_insert = 0` explicitly on every insert;
//!    a client that does not would be measured on a systematically smaller
//!    number than Spate's for the same work. [`ServerSideCost`] refuses rather
//!    than publishes when it sees the fingerprint — see
//!    [`ServerSideError::AsyncInsertsNotAttributable`].
//! 4. **Failed inserts wrote no rows but did cost CPU.** They are counted and
//!    reported separately rather than folded in, because folding them in would
//!    charge a per-row figure for work that produced no rows.
//! 5. **A materialized view's work rides on the parent insert.** The Kafka
//!    Connect deviation in `methodology/` lands nested rows and flattens with
//!    a view, which "moves CPU to the server and must be disclosed". Such an
//!    insert names the view's target in `tables` as well as its own landing
//!    table, so naming the workload's target table attributes it — believed, and
//!    **unverified until a Connect arm exists**. Pass the landing table too if
//!    in doubt; the predicate is `hasAny`, not equality.
//! 6. **Under the inverted predicate, any other non-initial query touching an
//!    attribution table inside the window is attributed to the arm.** The
//!    inversion trades one admission for another: the strict form admits
//!    stray *initial* inserts (item 1) and the inverted form admits stray
//!    *forwarded* ones — some other client's distributed write whose
//!    sub-query lands on an attributed table in the window. The arm lock
//!    makes that a protocol violation just as it does for item 1, and it
//!    would be exactly as invisible here.
//!
//! # Which counters constitute "CPU per row"
//!
//! `ProfileEvents` is a map with a few hundred keys in it, and four of them look
//! like candidates. They are not interchangeable, and on a container with a CPU
//! cap the difference is a factor of two and a half.
//!
//! Measured on the reference ClickHouse (26.3.17.4, capped at 5 CPUs at the time
//! of the reading — the envelope search has since moved it to 9 — on an
//! 18-vCPU host), one real 262 555-row insert from the Spate arm:
//!
//! ```text
//! UserTimeMicroseconds            170 850
//! SystemTimeMicroseconds            3 911   (sum: 174 761)
//! OSCPUVirtualTimeMicroseconds    174 760
//! OSCPUWaitMicroseconds           262 949
//! RealTimeMicroseconds            437 970
//! ```
//!
//! `UserTimeMicroseconds + SystemTimeMicroseconds` is what this module publishes
//! as CPU, for two reasons.
//!
//! It is **the same pair the arm is measured by**. `sampler::Sample` reads
//! `user_usec` and `system_usec` out of the arm's cgroup `cpu.stat`; taking user
//! plus system on the server side makes both halves of `arm CPU + server CPU`
//! the same quantity obtained the same way, rather than two different ideas of
//! cost that happen to share a unit.
//!
//! And it is **the only one of the four that is not a function of contention**.
//! `RealTimeMicroseconds` is thread-wall-time: in the row above it is almost
//! exactly `CPU + OSCPUWaitMicroseconds` (437 710 against 437 970, a 0.06%
//! difference), because under a 5-CPU cap ClickHouse's threads spent *more* time
//! waiting for a core than running on one. A "CPU per row" built on it would
//! have been 2.5x the truth, and would move with how busy the box was rather
//! than with what the arm asked the server to do — which is precisely the
//! failure `methodology/` describes for sustained-mode throughput.
//!
//! `OSCPUVirtualTimeMicroseconds` is the OS's own view of the same thing and
//! agrees to within a microsecond (174 760 against 174 761), so it is carried as
//! corroboration and never as the headline: it comes from taskstats/procfs,
//! which a container can be denied, whereas user and system time come from
//! `getrusage` and are always there. A counter that can silently vanish must not
//! be the one a published number rests on.
//!
//! `OSCPUWaitMicroseconds` is carried too, for the same reason the sampler
//! carries `nr_throttled`: it answers "why was it X and not 2X?" with evidence.
//!
//! # Flushing
//!
//! `system.query_log` is written through an in-memory buffer flushed every
//! `flush_interval_milliseconds` (7500 by default). A read taken the instant a
//! drain completes therefore misses the last several seconds of inserts — and
//! misses them *systematically at the end*, where the largest batches are. This
//! module issues `SYSTEM FLUSH LOGS` and waits for it before reading; see
//! [`flush_logs`]. It is not optional and there is no code path that skips it.
//!
//! # Per row, of what
//!
//! Two denominators, and they are different quantities:
//!
//! * [`ServerSideCost::cpu_us_per_written_row`] divides by `sum(written_rows)`
//!   from the log — the rows ClickHouse actually wrote, duplicates included.
//!   This is what the server's work was per unit of the server's own work, and
//!   it is the figure that describes the insert format.
//! * [`ServerSideCost::cpu_us_per_landed_row`] divides by the row count the
//!   driver observed in the target table. This is the one that may be added to
//!   the arm's own `cpu_us_per_row`, because it has the same denominator.
//!
//! They coincide for an arm that inserts each row exactly once. They
//! diverge for an at-least-once arm that duplicated, and for an arm that
//! lands raw rows and filters server-side — for which `written_rows` counts what
//! was landed while the driver counts what survived. `inserted_rows`, from the
//! `InsertedRows` counter, is carried beside `written_rows` for the same reason:
//! it counts rows inserted into *all* tables, so a divergence between the two is
//! the fingerprint of a materialized view doing work the arm pushed to the
//! server.
//!
//! Both refuse rather than return zero. A missing measurement must never become
//! "0 µs of server-side cost", which is the most flattering possible wrong
//! answer and the one this module exists to make unrepresentable.

use std::fmt;

use crate::docker::try_clickhouse_sql;
use crate::sampler::Samples;

/// The counters projected out of `ProfileEvents`.
///
/// A fixed list rather than the whole map, and the reason is not only response
/// size. Every one of these is a `UInt64` under an ASCII identifier, so the map's
/// text form cannot contain a comma or a tab and [`parse_profile_events`] can
/// split on both without a quoting rule. Widening this list to a counter whose
/// value is a string would silently break that.
const COUNTERS: [&str; 9] = [
    "UserTimeMicroseconds",
    "SystemTimeMicroseconds",
    "RealTimeMicroseconds",
    "OSCPUVirtualTimeMicroseconds",
    "OSCPUWaitMicroseconds",
    "InsertedRows",
    "InsertedBytes",
    "AsyncInsertQuery",
    "AsyncInsertRows",
];

/// Fully qualifies a table the way `system.query_log` writes it.
///
/// The log's `tables` column holds `default.sensor_events`, never
/// `sensor_events`, so a predicate built from `corpus::TABLE` alone matches
/// nothing at all — and matching nothing looks exactly like an arm that issued
/// no inserts.
#[must_use]
pub fn qualify(database: &str, table: &str) -> String {
    format!("{database}.{table}")
}

// ---------------------------------------------------------------------------
// The window
// ---------------------------------------------------------------------------

/// The interval a server-side figure is attributed over.
///
/// Epoch milliseconds, and they must come from the **sampler's** timestamps
/// rather than from `report::now_ms()`. Two reasons, and the second is the one
/// that bites.
///
/// `SutCost` owns the one measurement window, and every rate a record publishes
/// is derived from it; a server-side figure attributed over a different interval
/// would be a fourth view of a quantity that already has three consistent ones.
///
/// And the clocks differ. `sampler::Sample::t_ms` is read inside a container on
/// the Docker VM, which is the same kernel ClickHouse's container runs on, so it
/// and `event_time_microseconds` are the same clock. The driver's process runs
/// on macOS, outside that VM, and its wall clock is synchronised to the VM's
/// only as well as Docker Desktop happens to keep it. A window taken on the host
/// clock would be off by whatever that drift is, silently, and would shave
/// inserts off one end of the window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Window {
    /// Start of the window, epoch milliseconds, inclusive.
    pub from_ms: u64,
    /// End of the window, epoch milliseconds, exclusive.
    pub to_ms: u64,
}

impl Window {
    /// A window from explicit epoch milliseconds.
    ///
    /// # Errors
    ///
    /// [`ServerSideError::EmptyWindow`] if it does not run forwards. An empty
    /// window would match no queries, and "no queries matched" is reported as a
    /// refusal — so an inverted window would be diagnosed as an arm that never
    /// inserted anything.
    pub fn new(from_ms: u64, to_ms: u64) -> Result<Self, ServerSideError> {
        if to_ms <= from_ms {
            return Err(ServerSideError::EmptyWindow { from_ms, to_ms });
        }
        Ok(Self { from_ms, to_ms })
    }

    /// The window spanned by one arm's sampler series.
    ///
    /// Takes the earliest and latest **readable** sample across every container
    /// of the arm, which is by construction the same interval
    /// [`crate::sampler::SutCost`] divides by. An unreadable row carries a `-1`
    /// sentinel and is not evidence of anything, including of a time.
    ///
    /// # Errors
    ///
    /// [`ServerSideError::EmptyWindow`] if fewer than two readable samples
    /// landed across the whole arm, which is the same condition that makes
    /// `Samples::summarise` refuse.
    pub fn spanning(series: &[&Samples]) -> Result<Self, ServerSideError> {
        let times: Vec<u64> = series
            .iter()
            .flat_map(|s| s.rows.iter())
            .filter(|s| s.readable())
            .map(|s| s.t_ms)
            .collect();
        let from = times.iter().copied().min().unwrap_or(0);
        let to = times.iter().copied().max().unwrap_or(0);
        Self::new(from, to)
    }

    /// Length of the window in seconds, for provenance.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "a window of milliseconds stays far below f64's exact range"
    )]
    pub fn seconds(self) -> f64 {
        (self.to_ms - self.from_ms) as f64 / 1000.0
    }
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

/// Why a server-side measurement could not be made.
///
/// Typed, and returned in place of every value this module could plausibly have
/// invented. The failure this exists to prevent is not a crash: it is a record
/// carrying `"ch_cpu_us_per_row": {"value": 0.0, "unit": "us"}` with
/// `status: ok`, which reads as "this arm cost the server nothing" and is the
/// single most flattering wrong answer available here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerSideError {
    /// The window does not run forwards.
    EmptyWindow {
        /// Start that was asked for.
        from_ms: u64,
        /// End that was asked for.
        to_ms: u64,
    },
    /// The HTTP call to ClickHouse failed.
    Transport(String),
    /// ClickHouse answered with an exception.
    ///
    /// Separate from [`Self::Transport`] because the causes are different
    /// things to fix: a disabled `query_log`, a user without `SELECT` on
    /// `system`, or a server too old for `mapFilter` all land here, and none of
    /// them is a network problem.
    ServerException(String),
    /// A row of the response did not have the shape the projection asks for.
    Malformed {
        /// The offending line, verbatim.
        line: String,
        /// What was wrong with it.
        why: String,
    },
    /// The predicates matched no query at all.
    NoAttributedQueries {
        /// The tables that were looked for.
        tables: Vec<String>,
        /// The window that was looked in.
        window: Window,
    },
    /// Inserts were attributed, but not one of them carried the CPU counters.
    ///
    /// Distinct from [`Self::NoAttributedQueries`] on purpose: that one means
    /// the arm was not found, this one means it was found and the server did not
    /// say what it cost. Summing the counters that were present would produce a
    /// number smaller than the truth by an unknown factor.
    NoCpuCounters {
        /// How many inserts were attributed.
        queries: usize,
    },
    /// Some attributed inserts took the asynchronous path.
    ///
    /// Their cost is charged to a background flush that has no `query_log` row,
    /// so their `ProfileEvents` describe parsing and buffering only. Publishing
    /// the sum would under-report the arm, and under-report it by an amount that
    /// depends on which client library the arm happens to use.
    AsyncInsertsNotAttributable {
        /// Inserts carrying the `AsyncInsertQuery` counter.
        async_queries: usize,
        /// Inserts that finished having written no rows, which is the
        /// version-independent fingerprint of the same thing.
        wrote_nothing: usize,
        /// How many inserts were attributed in total.
        queries: usize,
    },
}

impl fmt::Display for ServerSideError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyWindow { from_ms, to_ms } => write!(
                f,
                "the server-side window [{from_ms}, {to_ms}) does not run forwards, so it \
                 would match no query and an arm that inserted nothing would be \
                 indistinguishable from a window nobody set"
            ),
            Self::Transport(e) => write!(f, "could not reach ClickHouse for its query log: {e}"),
            Self::ServerException(e) => write!(
                f,
                "ClickHouse refused the query-log read: {e}. system.query_log has to be \
                 enabled and readable for the server-side figure to exist at all; a run \
                 that cannot read it is recorded without one rather than with a zero"
            ),
            Self::Malformed { line, why } => write!(
                f,
                "a system.query_log row did not parse ({why}): {line:?}. The projection is \
                 fixed by this module, so a row that does not match it means the server's \
                 output changed shape and every figure derived from it is suspect"
            ),
            Self::NoAttributedQueries { tables, window } => write!(
                f,
                "no INSERT into {} was logged between {} and {}. Either the arm inserted \
                 nothing, the table name was not qualified with its database the way \
                 system.query_log writes it, or the window came from the host clock \
                 rather than the sampler's",
                tables.join(", "),
                window.from_ms,
                window.to_ms
            ),
            Self::NoCpuCounters { queries } => write!(
                f,
                "{queries} insert(s) were attributed to the arm and not one of them \
                 reported UserTimeMicroseconds or SystemTimeMicroseconds, so the server \
                 did not say what they cost. Summing what was there would under-report by \
                 an unknown factor"
            ),
            Self::AsyncInsertsNotAttributable {
                async_queries,
                wrote_nothing,
                queries,
            } => write!(
                f,
                "{async_queries} of {queries} attributed insert(s) took the asynchronous \
                 path ({wrote_nothing} finished having written no rows). An async insert's \
                 ProfileEvents cover parsing and buffering only — the write is charged to \
                 a background flush with no query_log row — so the sum would under-report \
                 this arm against one whose client sets async_insert=0. ClickHouse 26.3 \
                 defaults it ON: pin async_insert=0 in the arm's client settings"
            ),
        }
    }
}

impl std::error::Error for ServerSideError {}

// ---------------------------------------------------------------------------
// One logged query
// ---------------------------------------------------------------------------

/// The `type` column of `system.query_log`.
///
/// Kept as four variants rather than a boolean because each one means something
/// different about the row: a start is a duplicate of the row that follows it, a
/// finish is the measurement, and the two exception variants are work the server
/// did that produced no rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowType {
    /// `QueryStart` — logged when the query begins, with every counter zero.
    Start,
    /// `QueryFinish` — the only type that carries a measurement.
    Finish,
    /// `ExceptionBeforeStart` — rejected before it ran.
    ExceptionBeforeStart,
    /// `ExceptionWhileProcessing` — failed part-way through, having done work.
    ExceptionWhileProcessing,
}

impl RowType {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "QueryStart" => Some(Self::Start),
            "QueryFinish" => Some(Self::Finish),
            "ExceptionBeforeStart" => Some(Self::ExceptionBeforeStart),
            "ExceptionWhileProcessing" => Some(Self::ExceptionWhileProcessing),
            _ => None,
        }
    }

    /// Whether this row failed.
    #[must_use]
    pub fn is_exception(self) -> bool {
        matches!(
            self,
            Self::ExceptionBeforeStart | Self::ExceptionWhileProcessing
        )
    }
}

/// The counters projected out of one row's `ProfileEvents`.
///
/// Every field is an `Option`, and that is the whole design. `ProfileEvents` is
/// a map that omits any counter whose value is zero, so `ProfileEvents['X']`
/// returns `0` both for "the server measured zero" and for "the server does not
/// have that counter" — the two cases a published number must never conflate.
/// Projecting the map itself and parsing it here keeps absence representable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProfileCounters {
    /// `UserTimeMicroseconds` — user-mode CPU, summed over the query's threads.
    pub user_us: Option<u64>,
    /// `SystemTimeMicroseconds` — kernel-mode CPU, summed over the query's
    /// threads.
    pub system_us: Option<u64>,
    /// `RealTimeMicroseconds` — thread wall-clock. Not CPU; see the module docs.
    pub real_us: Option<u64>,
    /// `OSCPUVirtualTimeMicroseconds` — the OS's own view of the CPU time.
    pub os_cpu_virtual_us: Option<u64>,
    /// `OSCPUWaitMicroseconds` — runnable but off-CPU. Evidence about the cap.
    pub os_cpu_wait_us: Option<u64>,
    /// `InsertedRows` — rows inserted into **all** tables, views included.
    pub inserted_rows: Option<u64>,
    /// `InsertedBytes` — bytes inserted into all tables.
    pub inserted_bytes: Option<u64>,
    /// `AsyncInsertQuery` — non-zero when this insert took the async path.
    pub async_insert_query: Option<u64>,
    /// `AsyncInsertRows` — rows this insert handed to the async buffer.
    pub async_insert_rows: Option<u64>,
}

impl ProfileCounters {
    /// CPU microseconds this query cost the server, or `None` when the server
    /// reported neither half.
    ///
    /// `None` rather than zero when both are missing, and the sum of whichever
    /// half is present otherwise: a query with user time and no system time is a
    /// query that spent no measurable time in the kernel, which is a real answer
    /// on a short insert.
    #[must_use]
    pub fn cpu_us(self) -> Option<u64> {
        match (self.user_us, self.system_us) {
            (None, None) => None,
            (u, s) => Some(u.unwrap_or(0) + s.unwrap_or(0)),
        }
    }
}

/// One `system.query_log` row, attributed to the arm.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AttributedQuery {
    /// Which of the four log types this row is.
    pub row_type: RowType,
    /// `written_rows` — rows this query wrote to the table it inserted into.
    pub written_rows: u64,
    /// `query_duration_ms` — wall time from start to finish.
    pub duration_ms: u64,
    /// The projected counters.
    pub events: ProfileCounters,
}

// ---------------------------------------------------------------------------
// The published figure
// ---------------------------------------------------------------------------

/// What an arm's inserts cost ClickHouse over one measurement window.
///
/// Constructed only by [`summarise`], which refuses every case in which one of
/// these fields would have to be invented.
#[derive(Clone, Debug, PartialEq)]
pub struct ServerSideCost {
    /// The window the figures are attributed over.
    pub window: Window,
    /// The tables the inserts were attributed to, fully qualified.
    pub tables: Vec<String>,
    /// Inserts that completed. The denominator of nothing, the numerator's
    /// population.
    pub queries: usize,
    /// Inserts that *started* inside the window.
    ///
    /// Carried because the gap is evidence: an insert logged as started with no
    /// matching finish or exception was still in flight when the window closed,
    /// or the arm was killed under it. Reading only the finishes would make an
    /// arm that died mid-insert look like an arm that stopped inserting.
    pub queries_started: usize,
    /// Inserts that failed. Their CPU is **not** in `cpu_us`, because they
    /// produced no rows and a per-row figure cannot charge for them.
    pub failed_queries: usize,
    /// `sum(written_rows)` over the completed inserts.
    pub written_rows: u64,
    /// `sum(InsertedRows)` — rows written to all tables, views included. Equal
    /// to `written_rows` unless a materialized view is doing work.
    pub inserted_rows: Option<u64>,
    /// `sum(InsertedBytes)` over the completed inserts.
    pub inserted_bytes: Option<u64>,
    /// Server CPU microseconds: user plus system, summed.
    pub cpu_us: f64,
    /// User-mode share of `cpu_us`.
    pub user_us: f64,
    /// Kernel-mode share of `cpu_us`.
    pub system_us: f64,
    /// `RealTimeMicroseconds`, summed. Thread wall-clock, not CPU.
    pub real_us: Option<f64>,
    /// `OSCPUVirtualTimeMicroseconds`, summed. Corroborates `cpu_us`.
    pub os_cpu_virtual_us: Option<f64>,
    /// `OSCPUWaitMicroseconds`, summed. Runnable but off-CPU — the server's
    /// counterpart to the sampler's `throttled_us`.
    pub os_cpu_wait_us: Option<f64>,
    /// Completed inserts that reported no CPU counters at all. Carried rather
    /// than dropped, so a record can say its figure rests on a subset.
    pub queries_without_cpu: usize,
}

impl ServerSideCost {
    /// Server CPU microseconds per row **ClickHouse wrote**.
    ///
    /// The figure that describes the insert format: it divides the server's work
    /// by the server's own count of what it was asked to write, duplicates and
    /// filtered-away landing rows included.
    ///
    /// # Errors
    ///
    /// [`ServerSideError::NoAttributedQueries`] when nothing was written, which
    /// is the one case where a division would produce an infinity that reads
    /// like a measurement.
    pub fn cpu_us_per_written_row(&self) -> Result<f64, ServerSideError> {
        if self.written_rows == 0 {
            return Err(ServerSideError::NoAttributedQueries {
                tables: self.tables.clone(),
                window: self.window,
            });
        }
        #[expect(
            clippy::cast_precision_loss,
            reason = "row counts stay far below f64's exact range"
        )]
        Ok(self.cpu_us / self.written_rows as f64)
    }

    /// Server CPU microseconds per row the **driver** counted in the target
    /// table.
    ///
    /// The figure that may be added to the arm's own `cpu_us_per_row`, because
    /// it shares that denominator. Pass the same `rows` the record's
    /// `rows_per_s` and `cpu_us_per_row` were computed from, or the record will
    /// contradict itself.
    ///
    /// # Errors
    ///
    /// [`ServerSideError::NoAttributedQueries`] when `rows` is not positive.
    pub fn cpu_us_per_landed_row(&self, rows: f64) -> Result<f64, ServerSideError> {
        if rows <= 0.0 {
            return Err(ServerSideError::NoAttributedQueries {
                tables: self.tables.clone(),
                window: self.window,
            });
        }
        Ok(self.cpu_us / rows)
    }

    /// Mean rows per completed insert — the arm's effective batch size, as the
    /// server saw it rather than as the arm's configuration claims.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "row and query counts stay far below f64's exact range"
    )]
    pub fn rows_per_insert(&self) -> f64 {
        if self.queries == 0 {
            0.0
        } else {
            self.written_rows as f64 / self.queries as f64
        }
    }

    /// A one-line account of how this figure was obtained, for a record's note.
    ///
    /// Written here rather than at the call site because the caveats travel with
    /// the number: a reader who is told "0.67 µs/row of server-side cost"
    /// without being told it excludes background merges has been told something
    /// slightly false.
    #[must_use]
    pub fn provenance(&self) -> String {
        let mut s = format!(
            "server-side: {} insert(s) into {} over {:.1}s, {} rows written",
            self.queries,
            self.tables.join("+"),
            self.window.seconds(),
            self.written_rows
        );
        if self.failed_queries > 0 {
            s.push_str(&format!(
                "; {} failed insert(s) excluded",
                self.failed_queries
            ));
        }
        let accounted = self.queries + self.failed_queries;
        if self.queries_started > accounted {
            s.push_str(&format!(
                "; {} insert(s) still in flight at the end of the window",
                self.queries_started - accounted
            ));
        }
        if self.queries_without_cpu > 0 {
            s.push_str(&format!(
                "; {} insert(s) reported no CPU counters",
                self.queries_without_cpu
            ));
        }
        if self.inserted_rows.is_some_and(|i| i != self.written_rows) {
            s.push_str("; InsertedRows differs from written_rows, so a view wrote too");
        }
        s.push_str("; excludes background merges");
        s
    }
}

// ---------------------------------------------------------------------------
// Pure: the query, and the parse
// ---------------------------------------------------------------------------

/// The `SYSTEM FLUSH LOGS` statement, named so the reason travels with it.
///
/// `system.query_log` is buffered and flushed asynchronously, so a read taken
/// the instant a drain completes misses whatever has not been flushed — which is
/// the tail of the run, where an arm's largest batches are. Flushing first turns
/// a silent partial answer into a complete one.
pub const FLUSH_LOGS_SQL: &str = "SYSTEM FLUSH LOGS";

/// The attribution query, as a string, so that it can be read and tested without
/// a server.
///
/// The projection selects **every** log type rather than filtering to
/// `QueryFinish` in SQL, and the split is made in [`summarise`] instead. Two
/// reasons. The rule that a start row is not a measurement is then a property of
/// code with a test on it rather than of a `WHERE` clause nobody reads. And the
/// start rows are themselves evidence: an insert that started inside the window
/// with no matching finish was still running when the window closed, which is
/// what an arm killed mid-insert looks like from the server, and a `WHERE` that
/// discarded them would make that indistinguishable from an arm that stopped
/// inserting.
///
/// `event_date` is bounded a day either side of the window. That predicate does
/// no attribution work — `query_start_time_microseconds` already does it — and
/// exists so the read prunes `system.query_log`'s partitions instead of scanning
/// every month the server has been up. The day of slack is because `event_date`
/// is derived in the server's timezone while the bounds are epoch milliseconds,
/// and being generous costs one partition where being exact could cost a row.
/// `forwarded_inserts` **inverts** the fourth predicate for the one arm shape
/// that needs it — it never drops it. An arm pushing through a `Distributed`
/// table lands **every** insert on the shared server with
/// `is_initial_query = 0` — the initial query ran on the arm's own node, whose
/// log this module never reads — so the strict predicate would match nothing at
/// all, and nothing looks exactly like an arm that never inserted. Substituting
/// `NOT is_initial_query` matches exactly those forwarded executions. Dropping
/// the predicate instead would match both sides of a forward: if such an arm's
/// `Distributed` table ever lived on the shared server itself, the initial
/// query and the forwarded execution it spawned would each carry
/// `written_rows` and CPU, and the arm would be double-counted. With one form
/// or its negation always present, that is impossible by construction. The
/// exception is declared per-entrant (`[clickhouse].forwarded_inserts`) rather
/// than defaulted, so every arm that does not need it keeps the predicate that
/// stops a distributed sub-query being counted beside the query that spawned
/// it.
#[must_use]
pub fn attribution_sql(tables: &[&str], window: Window, forwarded_inserts: bool) -> String {
    let counters = COUNTERS
        .iter()
        .map(|c| format!("'{c}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let names = tables
        .iter()
        .map(|t| format!("'{t}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let initial = if forwarded_inserts {
        "AND NOT is_initial_query "
    } else {
        "AND is_initial_query "
    };
    format!(
        "SELECT type, written_rows, query_duration_ms, \
         mapFilter((k, v) -> k IN ({counters}), ProfileEvents) \
         FROM system.query_log \
         WHERE event_date >= toDate(fromUnixTimestamp64Milli({from})) - 1 \
           AND event_date <= toDate(fromUnixTimestamp64Milli({to})) + 1 \
           AND query_start_time_microseconds >= fromUnixTimestamp64Milli({from}) \
           AND query_start_time_microseconds < fromUnixTimestamp64Milli({to}) \
           AND query_kind = 'Insert' \
           {initial}\
           AND hasAny(tables, [{names}]) \
         ORDER BY event_time_microseconds \
         FORMAT TSV",
        from = window.from_ms,
        to = window.to_ms,
    )
}

/// Parses the `ProfileEvents` map as ClickHouse renders it in TSV.
///
/// The text form is `{'Key':123,'Other':456}`, or `{}` when every projected
/// counter was zero and therefore absent. Absence is the information: see
/// [`ProfileCounters`].
///
/// # Errors
///
/// [`ServerSideError::Malformed`] for anything that is not that shape. A key
/// that is not in the projected set is ignored rather than refused, so a future
/// ClickHouse that adds a counter to the projection does not fail the read; an
/// *entry* that is not `'key':integer` is refused, because that means the text
/// form changed and every value parsed out of it is a guess.
pub fn parse_profile_events(field: &str) -> Result<ProfileCounters, ServerSideError> {
    let malformed = |why: &str| ServerSideError::Malformed {
        line: field.to_owned(),
        why: why.to_owned(),
    };
    let body = field
        .trim()
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| malformed("a ProfileEvents map is rendered as {…}"))?;

    let mut out = ProfileCounters::default();
    if body.trim().is_empty() {
        return Ok(out);
    }
    for entry in body.split(',') {
        let (key, value) = entry
            .split_once(':')
            .ok_or_else(|| malformed("a map entry is rendered as 'key':value"))?;
        let key = key
            .trim()
            .strip_prefix('\'')
            .and_then(|k| k.strip_suffix('\''))
            .ok_or_else(|| malformed("a map key is rendered in single quotes"))?;
        let value: u64 = value
            .trim()
            .parse()
            .map_err(|_| malformed("a projected counter is a UInt64"))?;
        match key {
            "UserTimeMicroseconds" => out.user_us = Some(value),
            "SystemTimeMicroseconds" => out.system_us = Some(value),
            "RealTimeMicroseconds" => out.real_us = Some(value),
            "OSCPUVirtualTimeMicroseconds" => out.os_cpu_virtual_us = Some(value),
            "OSCPUWaitMicroseconds" => out.os_cpu_wait_us = Some(value),
            "InsertedRows" => out.inserted_rows = Some(value),
            "InsertedBytes" => out.inserted_bytes = Some(value),
            "AsyncInsertQuery" => out.async_insert_query = Some(value),
            "AsyncInsertRows" => out.async_insert_rows = Some(value),
            _ => {}
        }
    }
    Ok(out)
}

/// Parses the whole TSV response of [`attribution_sql`].
///
/// # Errors
///
/// [`ServerSideError::ServerException`] if the body is a ClickHouse exception
/// rather than a result set — checked here rather than left to
/// `docker::clickhouse_sql`, which **asserts** on `DB::Exception` and would take
/// a thirty-hour sweep down over a disabled system table.
///
/// [`ServerSideError::Malformed`] for a row that is not four tab-separated
/// fields of the declared types.
pub fn parse_response(body: &str) -> Result<Vec<AttributedQuery>, ServerSideError> {
    if body.contains("DB::Exception") {
        return Err(ServerSideError::ServerException(body.trim().to_owned()));
    }
    let mut out = Vec::new();
    for line in body.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let malformed = |why: &str| ServerSideError::Malformed {
            line: line.to_owned(),
            why: why.to_owned(),
        };
        let f: Vec<&str> = line.splitn(4, '\t').collect();
        if f.len() != 4 {
            return Err(malformed(
                "expected type, written_rows, duration and events",
            ));
        }
        out.push(AttributedQuery {
            row_type: RowType::parse(f[0]).ok_or_else(|| malformed("unknown query_log type"))?,
            written_rows: f[1]
                .trim()
                .parse()
                .map_err(|_| malformed("written_rows is a UInt64"))?,
            duration_ms: f[2]
                .trim()
                .parse()
                .map_err(|_| malformed("query_duration_ms is a UInt64"))?,
            events: parse_profile_events(f[3])?,
        });
    }
    Ok(out)
}

/// Summarises attributed rows into the figure a record publishes.
///
/// Only [`RowType::Finish`] rows are summed. A `QueryStart` row is a duplicate
/// of the finish that follows it with every counter zero, so counting it would
/// halve the per-row figure; an exception row did work but wrote no rows, so
/// charging it to a per-row figure would charge rows that do not exist for work
/// that did happen. Both are counted separately instead of being folded in or
/// thrown away.
///
/// # Errors
///
/// * [`ServerSideError::NoAttributedQueries`] when no insert completed.
/// * [`ServerSideError::AsyncInsertsNotAttributable`] when any completed insert
///   carries the async fingerprint. Refused rather than reported, because the
///   under-count is arm-dependent and therefore distorts the comparison rather
///   than merely the absolute value.
/// * [`ServerSideError::NoCpuCounters`] when not one completed insert reported
///   user or system time.
pub fn summarise(
    rows: &[AttributedQuery],
    tables: &[&str],
    window: Window,
) -> Result<ServerSideCost, ServerSideError> {
    let tables: Vec<String> = tables.iter().map(|t| (*t).to_owned()).collect();
    let finished: Vec<&AttributedQuery> = rows
        .iter()
        .filter(|r| r.row_type == RowType::Finish)
        .collect();
    let failed = rows.iter().filter(|r| r.row_type.is_exception()).count();

    if finished.is_empty() {
        return Err(ServerSideError::NoAttributedQueries { tables, window });
    }

    // The async fingerprint, read two ways because neither is sufficient alone.
    // `AsyncInsertQuery` is the counter that says so outright, and a counter
    // that is absent proves nothing; an insert that finished having written no
    // rows is what the async path looks like from the outside on any version.
    let async_queries = finished
        .iter()
        .filter(|r| r.events.async_insert_query.is_some_and(|n| n > 0))
        .count();
    let wrote_nothing = finished.iter().filter(|r| r.written_rows == 0).count();
    if async_queries > 0 {
        return Err(ServerSideError::AsyncInsertsNotAttributable {
            async_queries,
            wrote_nothing,
            queries: finished.len(),
        });
    }

    let queries_without_cpu = finished
        .iter()
        .filter(|r| r.events.cpu_us().is_none())
        .count();
    if queries_without_cpu == finished.len() {
        return Err(ServerSideError::NoCpuCounters {
            queries: finished.len(),
        });
    }

    // `None` where no completed insert carried the counter at all, and a sum
    // where any did. A counter the server never mentioned must not arrive as a
    // zero; a counter it mentioned on some queries and not others is summed over
    // the ones it mentioned, and `queries_without_cpu` says how many that was.
    let total = |f: fn(&ProfileCounters) -> Option<u64>| -> Option<u64> {
        let present: Vec<u64> = finished.iter().filter_map(|r| f(&r.events)).collect();
        (!present.is_empty()).then(|| present.iter().sum())
    };
    #[expect(
        clippy::cast_precision_loss,
        reason = "microsecond counts stay far below f64's exact range"
    )]
    let micros = |v: Option<u64>| -> Option<f64> { v.map(|n| n as f64) };

    let user_us = micros(total(|e| e.user_us)).unwrap_or(0.0);
    let system_us = micros(total(|e| e.system_us)).unwrap_or(0.0);
    Ok(ServerSideCost {
        window,
        tables,
        queries: finished.len(),
        queries_started: rows.iter().filter(|r| r.row_type == RowType::Start).count(),
        failed_queries: failed,
        written_rows: finished.iter().map(|r| r.written_rows).sum(),
        inserted_rows: total(|e| e.inserted_rows),
        inserted_bytes: total(|e| e.inserted_bytes),
        cpu_us: user_us + system_us,
        user_us,
        system_us,
        real_us: micros(total(|e| e.real_us)),
        os_cpu_virtual_us: micros(total(|e| e.os_cpu_virtual_us)),
        os_cpu_wait_us: micros(total(|e| e.os_cpu_wait_us)),
        queries_without_cpu,
    })
}

// ---------------------------------------------------------------------------
// I/O
// ---------------------------------------------------------------------------

/// Flushes ClickHouse's system logs so the query log is complete up to now.
///
/// # Errors
///
/// [`ServerSideError::Transport`] or [`ServerSideError::ServerException`].
/// Failing here is a refusal rather than a warning: reading an unflushed
/// `system.query_log` produces a smaller, entirely plausible number.
pub fn flush_logs(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
) -> Result<(), ServerSideError> {
    let body = try_clickhouse_sql(host, port, user, password, FLUSH_LOGS_SQL)
        .map_err(|e| ServerSideError::Transport(e.to_string()))?;
    if body.contains("DB::Exception") {
        return Err(ServerSideError::ServerException(body.trim().to_owned()));
    }
    Ok(())
}

/// Merge work ClickHouse completed over a measurement window.
///
/// Both figures cover merges that **finished** inside the window, because
/// `system.part_log` records a merge at completion. A merge still running when
/// the window closes contributes nothing here, so this is a lower bound on the
/// merge work that competed with the arm.
///
/// `duration_ms` sums each merge's own elapsed time and merges run
/// concurrently, so it can exceed the window's wall-clock length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MergeActivity {
    /// Rows read by merges that completed in the window.
    pub rows_merged: u64,
    /// Summed elapsed time of those merges.
    pub duration_ms: u64,
}

/// The projection [`measure_merges`] reads, over `tables` and `window`.
///
/// `event_date` bounds the partition scan either side of the window, matching
/// [`attribution_sql`]; `part_log` is partitioned by date and a window crossing
/// midnight would otherwise read one day only.
///
/// `tables` are **fully qualified** (`default.sensor_events`); `part_log` holds
/// `database` and `table` separately, so the predicate rebuilds the qualified
/// name rather than asking the caller for two lists.
#[must_use]
pub fn merge_sql(tables: &[&str], window: Window) -> String {
    let names = tables
        .iter()
        .map(|t| format!("'{t}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT sumIf(rows, event_type = 'MergeParts'),          sumIf(duration_ms, event_type = 'MergeParts')          FROM system.part_log          WHERE event_date >= toDate(fromUnixTimestamp64Milli({from})) - 1            AND event_date <= toDate(fromUnixTimestamp64Milli({to})) + 1            AND event_time_microseconds >= fromUnixTimestamp64Milli({from})            AND event_time_microseconds < fromUnixTimestamp64Milli({to})            AND concat(database, '.', table) IN ({names})          FORMAT TSV",
        from = window.from_ms,
        to = window.to_ms,
    )
}

/// Parses the two-column TSV [`merge_sql`] projects.
///
/// A window in which no merge completed is `0\t0`, which parses to a zero
/// reading rather than an error: the server answered, and the answer is that
/// nothing merged.
///
/// # Errors
///
/// [`ServerSideError::ServerException`] if the body carries one, and
/// [`ServerSideError::Malformed`] for anything that is not two integers.
pub fn parse_merge_activity(body: &str) -> Result<MergeActivity, ServerSideError> {
    if body.contains("DB::Exception") {
        return Err(ServerSideError::ServerException(body.trim().to_owned()));
    }
    let line = body.trim();
    let malformed = |why: &str| ServerSideError::Malformed {
        line: line.to_owned(),
        why: why.to_owned(),
    };
    let mut fields = line.split('\t');
    let mut next = |what: &str| -> Result<u64, ServerSideError> {
        fields
            .next()
            .ok_or_else(|| malformed(&format!("no {what} column")))?
            .trim()
            .parse()
            .map_err(|_| malformed(&format!("{what} is not an integer")))
    };
    let rows_merged = next("rows_merged")?;
    let duration_ms = next("duration_ms")?;
    if fields.next().is_some() {
        return Err(malformed("more than two columns"));
    }
    Ok(MergeActivity {
        rows_merged,
        duration_ms,
    })
}

/// Reads the merge work ClickHouse completed over `window`.
///
/// Flushes the system logs itself, so it carries no ordering contract with
/// [`measure`]. Both flushes happen after the measurement window has closed and
/// cannot perturb the number.
///
/// `tables` are **fully qualified** (`default.sensor_events`); see [`qualify`].
///
/// # Errors
///
/// Every variant of [`ServerSideError`] except
/// [`ServerSideError::NoAttributedQueries`], which cannot arise: an empty result
/// is a zero reading. A caller that cannot get a reading records the arm without
/// it rather than attaching a zero.
pub fn measure_merges(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    tables: &[&str],
    window: Window,
) -> Result<MergeActivity, ServerSideError> {
    flush_logs(host, port, user, password)?;
    let sql = merge_sql(tables, window);
    let body = try_clickhouse_sql(host, port, user, password, &sql)
        .map_err(|e| ServerSideError::Transport(e.to_string()))?;
    parse_merge_activity(&body)
}

/// Reads what an arm's inserts cost ClickHouse over `window`.
///
/// The one entry point the driver needs. Flushes the system logs, reads the
/// attributed rows, and summarises them — refusing at every step where a value
/// would otherwise have to be invented.
///
/// `tables` are **fully qualified** (`default.sensor_events`); see [`qualify`].
///
/// Deliberately built on [`crate::docker::try_clickhouse_sql`] rather than
/// `clickhouse_sql`: the latter asserts on `DB::Exception`, and a server-side
/// figure is an addition to a record rather than the record's reason for
/// existing. A disabled `system.query_log` must cost the run this one metric,
/// not the whole measurement.
///
/// # Errors
///
/// Every variant of [`ServerSideError`]. None of them is recoverable into a
/// number; a caller that cannot get one records the arm without it.
pub fn measure(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    tables: &[&str],
    window: Window,
    forwarded_inserts: bool,
) -> Result<ServerSideCost, ServerSideError> {
    flush_logs(host, port, user, password)?;
    let sql = attribution_sql(tables, window, forwarded_inserts);
    let body = try_clickhouse_sql(host, port, user, password, &sql)
        .map_err(|e| ServerSideError::Transport(e.to_string()))?;
    let rows = parse_response(&body)?;
    summarise(&rows, tables, window)
}

// ---------------------------------------------------------------------------
// Settling
// ---------------------------------------------------------------------------

/// Additional quiet imposed after the server reports no parts and no merges,
/// in milliseconds.
///
/// A part that has just become inactive is still being unlinked, and the
/// measurement about to start should not pay for that.
pub const SETTLE_QUIET_MS: u64 = 2_000;

/// Seconds [`wait_until_settled`] waits before giving up on the wait.
///
/// Bounded because the alternative is a pass that hangs, and generous because
/// the alternative to that is a measurement charged for its predecessor.
/// Exceeding it is recorded rather than swallowed: a target that cannot clear
/// its own merge queue inside this is a finding about the target.
pub const SETTLE_MAX_S: u64 = 120;

/// Milliseconds between polls while waiting.
pub const SETTLE_POLL_MS: u64 = 250;

/// Waits until `table` has no active parts and no running merges, and returns
/// the seconds waited.
///
/// The wait is on the server's own `system.parts` and `system.merges` rather
/// than on a clock. A `TRUNCATE` drops parts asynchronously and leaves a merge
/// queue, so a fixed sleep is a guess about work whose size is not known.
///
/// Returning past [`SETTLE_MAX_S`] is not an error, and an unreadable answer
/// ends the wait rather than extending it: the system tables are a diagnostic,
/// and a pass that hung because one stopped answering would be a worse failure
/// than a measurement that started slightly early.
#[must_use]
pub fn wait_until_settled(host: &str, port: u16, user: &str, password: &str, table: &str) -> f64 {
    let started = std::time::Instant::now();
    let deadline = started + std::time::Duration::from_secs(SETTLE_MAX_S);
    while std::time::Instant::now() < deadline {
        let quiet = crate::docker::try_clickhouse_sql(
            host,
            port,
            user,
            password,
            &format!(
                "SELECT (SELECT count() FROM system.parts WHERE table = '{table}' AND active) \
                 + (SELECT count() FROM system.merges WHERE table = '{table}') FORMAT TSV"
            ),
        )
        .ok()
        .and_then(|b| b.trim().parse::<u64>().ok());
        if quiet.is_none_or(|n| n == 0) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(SETTLE_POLL_MS));
    }
    std::thread::sleep(std::time::Duration::from_millis(SETTLE_QUIET_MS));
    started.elapsed().as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Three verbatim rows from the reference ClickHouse (26.3.17.4) — the Spate
    /// arm's own inserts into `default.sensor_events_t`, read back through
    /// exactly the projection [`attribution_sql`] builds.
    const REAL_INSERTS: &str = "\
QueryFinish\t262555\t437\t{'InsertedRows':262555,'InsertedBytes':16938179,'RealTimeMicroseconds':437970,'UserTimeMicroseconds':170850,'SystemTimeMicroseconds':3911,'OSCPUWaitMicroseconds':262949,'OSCPUVirtualTimeMicroseconds':174760}
QueryFinish\t262154\t427\t{'InsertedRows':262154,'InsertedBytes':16913682,'RealTimeMicroseconds':428015,'UserTimeMicroseconds':168868,'SystemTimeMicroseconds':951,'OSCPUWaitMicroseconds':257986,'OSCPUVirtualTimeMicroseconds':169818}
QueryFinish\t262275\t435\t{'InsertedRows':262275,'InsertedBytes':16921676,'RealTimeMicroseconds':435253,'UserTimeMicroseconds':165147,'SystemTimeMicroseconds':4491,'OSCPUWaitMicroseconds':265343,'OSCPUVirtualTimeMicroseconds':169637}
";

    fn window() -> Window {
        Window::new(1_784_979_298_378, 1_784_979_598_378).expect("a forward window")
    }

    fn summarised(body: &str) -> Result<ServerSideCost, ServerSideError> {
        let rows = parse_response(body)?;
        summarise(&rows, &["default.sensor_events_t"], window())
    }

    #[test]
    fn parses_a_merge_reading() {
        let m = parse_merge_activity("1966080\t2431\n").expect("two integers parse");
        assert_eq!(m.rows_merged, 1_966_080);
        assert_eq!(m.duration_ms, 2431);
    }

    /// A window in which nothing merged is a reading, not a refusal. The server
    /// answered; the answer is zero. A caller that turned this into an absence
    /// would lose the ability to say "no merges ran here".
    #[test]
    fn a_window_with_no_merges_reads_as_zero() {
        let m = parse_merge_activity("0\t0\n").expect("a zero row parses");
        assert_eq!(m.rows_merged, 0);
        assert_eq!(m.duration_ms, 0);
    }

    #[test]
    fn a_part_log_exception_is_not_a_zero_reading() {
        let err = parse_merge_activity(
            "Code: 60. DB::Exception: Table system.part_log does not exist. (UNKNOWN_TABLE)",
        )
        .expect_err("an exception refuses");
        assert!(
            matches!(err, ServerSideError::ServerException(_)),
            "{err:?}"
        );
    }

    #[test]
    fn a_malformed_merge_row_is_refused() {
        for body in ["", "1966080", "a\tb", "1\t2\t3"] {
            assert!(
                parse_merge_activity(body).is_err(),
                "{body:?} should not parse"
            );
        }
    }

    /// The predicate rebuilds the qualified name because `part_log` splits it,
    /// and bounds `event_date` either side so a window crossing midnight is not
    /// read as one day.
    #[test]
    fn merge_sql_qualifies_the_table_and_bounds_the_partition_scan() {
        let sql = merge_sql(&["default.sensor_events"], window());
        assert!(
            sql.contains("concat(database, '.', table) IN ('default.sensor_events')"),
            "{sql}"
        );
        assert!(sql.contains("event_type = 'MergeParts'"), "{sql}");
        assert!(
            sql.contains("toDate(fromUnixTimestamp64Milli(1784979298378)) - 1"),
            "{sql}"
        );
        assert!(
            sql.contains("toDate(fromUnixTimestamp64Milli(1784979598378)) + 1"),
            "{sql}"
        );
        assert!(
            sql.contains("event_time_microseconds >= fromUnixTimestamp64Milli(1784979298378)"),
            "{sql}"
        );
        assert!(
            sql.contains("event_time_microseconds < fromUnixTimestamp64Milli(1784979598378)"),
            "{sql}"
        );
    }

    #[test]
    fn parses_a_real_query_log_response() {
        let rows = parse_response(REAL_INSERTS).expect("the reference response parses");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].row_type, RowType::Finish);
        assert_eq!(rows[0].written_rows, 262_555);
        assert_eq!(rows[0].duration_ms, 437);
        assert_eq!(rows[0].events.user_us, Some(170_850));
        assert_eq!(rows[0].events.system_us, Some(3_911));
        assert_eq!(rows[0].events.inserted_bytes, Some(16_938_179));
        assert_eq!(rows[0].events.async_insert_query, None);
    }

    /// The counter choice, on the numbers that forced it. `RealTimeMicroseconds`
    /// is thread wall-clock and on this row is CPU plus the time the server's
    /// threads spent runnable but off-CPU under a 5-CPU cap — so a per-row figure
    /// built on it is two and a half times the truth and moves with how busy the
    /// host was rather than with what the arm asked the server to do.
    #[test]
    fn cpu_is_user_plus_system_and_not_thread_wall_clock() {
        let cost = summarised(REAL_INSERTS).expect("three real inserts summarise");
        let user_plus_system = 170_850.0 + 3_911.0 + 168_868.0 + 951.0 + 165_147.0 + 4_491.0;
        assert!((cost.cpu_us - user_plus_system).abs() < 1e-9);

        // The OS's own view of the same quantity, to within a microsecond a row.
        let os = cost.os_cpu_virtual_us.expect("the counter was present");
        assert!(
            (cost.cpu_us - os).abs() / cost.cpu_us < 1e-4,
            "user+system {} against OSCPUVirtualTime {os}",
            cost.cpu_us
        );

        // And the counter that must not be mistaken for CPU.
        let real = cost.real_us.expect("the counter was present");
        let wait = cost.os_cpu_wait_us.expect("the counter was present");
        assert!(
            real > cost.cpu_us * 2.0,
            "RealTime {real} should dwarf CPU {} under a cap",
            cost.cpu_us
        );
        assert!(
            (real - (cost.cpu_us + wait)).abs() / real < 0.01,
            "RealTime {real} should be CPU {} plus wait {wait}",
            cost.cpu_us
        );
    }

    #[test]
    fn divides_by_the_rows_the_server_wrote_and_by_the_rows_the_driver_counted() {
        let cost = summarised(REAL_INSERTS).expect("three real inserts summarise");
        assert_eq!(cost.written_rows, 262_555 + 262_154 + 262_275);
        // 0.66 microseconds of server CPU per row inserted, in RowBinary.
        let per_written = cost
            .cpu_us_per_written_row()
            .expect("rows were written, so the division is defined");
        assert!(
            (0.6..0.7).contains(&per_written),
            "server cost was {per_written} us/row"
        );

        // The comparable figure divides by what the driver counted instead, and
        // the two coincide only when the arm inserted each row exactly once.
        #[expect(clippy::cast_precision_loss, reason = "a test constant")]
        let landed = cost.written_rows as f64;
        let per_landed = cost
            .cpu_us_per_landed_row(landed)
            .expect("a positive row count");
        assert!((per_written - per_landed).abs() < 1e-12);

        let duplicated = cost
            .cpu_us_per_landed_row(landed / 2.0)
            .expect("a positive row count");
        assert!((duplicated - per_written * 2.0).abs() < 1e-9);
    }

    /// A `QueryStart` row is a duplicate of the finish that follows it with every
    /// counter zero — the live server renders its `ProfileEvents` as `{}`.
    /// Summing it would halve the per-row figure while leaving the result
    /// entirely plausible, which is the failure mode this module is built around.
    /// It is counted rather than discarded, because a start with no finish is an
    /// insert that was still running when the window closed.
    #[test]
    fn a_query_start_row_is_counted_but_is_never_a_measurement() {
        // The server logs a start for every insert, so a complete window has one
        // of each.
        let starts = "QueryStart\t0\t0\t{}\n".repeat(3);
        let cost = summarised(&format!("{starts}{REAL_INSERTS}")).expect("the finishes summarise");
        assert_eq!(cost.queries, 3);
        assert_eq!(cost.queries_started, 3);
        assert_eq!(cost.written_rows, 262_555 + 262_154 + 262_275);
        assert!(
            !cost.provenance().contains("in flight"),
            "{}",
            cost.provenance()
        );

        // A fourth start with no finish: the arm was killed under its own insert,
        // which must not read as an arm that simply stopped inserting.
        let killed = format!("{starts}QueryStart\t0\t0\t{{}}\n{REAL_INSERTS}");
        let cost = summarised(&killed).expect("the finishes still summarise");
        assert!(
            cost.provenance().contains("1 insert(s) still in flight"),
            "{}",
            cost.provenance()
        );
    }

    /// A failed insert did work and wrote nothing. Charging it to a per-row
    /// figure would charge rows that do not exist; dropping it silently would
    /// hide that the arm was failing. It is counted separately.
    #[test]
    fn a_failed_insert_is_counted_but_not_charged_per_row() {
        let body = format!(
            "{REAL_INSERTS}ExceptionWhileProcessing\t0\t12\t{{'UserTimeMicroseconds':9000}}\n"
        );
        let cost = summarised(&body).expect("the completed inserts still summarise");
        assert_eq!(cost.queries, 3);
        assert_eq!(cost.failed_queries, 1);
        let expected = 170_850.0 + 3_911.0 + 168_868.0 + 951.0 + 165_147.0 + 4_491.0;
        assert!((cost.cpu_us - expected).abs() < 1e-9, "{}", cost.cpu_us);
    }

    /// ClickHouse 26.3 defaults `async_insert` on. An insert that takes that path
    /// reports the cost of parsing and buffering, never the cost of the write,
    /// and the shortfall depends on which client library an arm happens to use —
    /// so it would distort the comparison and not merely the absolute figure.
    #[test]
    fn an_asynchronous_insert_is_refused_rather_than_under_reported() {
        let body = "QueryFinish\t0\t3\t{'AsyncInsertQuery':1,'AsyncInsertRows':25000,\
                    'UserTimeMicroseconds':1200,'SystemTimeMicroseconds':40}\n";
        let e = summarised(body).expect_err("an async insert must be refused");
        match e {
            ServerSideError::AsyncInsertsNotAttributable {
                async_queries,
                wrote_nothing,
                queries,
            } => {
                assert_eq!((async_queries, wrote_nothing, queries), (1, 1, 1));
            }
            other => panic!("wrong refusal: {other}"),
        }
        assert!(
            format!("{e}").contains("async_insert=0"),
            "the refusal must say how to fix it"
        );
    }

    /// The failure this module exists to prevent: a window that matched nothing
    /// becoming "this arm cost the server nothing".
    #[test]
    fn a_window_that_matched_nothing_is_a_refusal_and_never_a_zero() {
        let e = summarised("").expect_err("an empty response must be refused");
        assert!(matches!(e, ServerSideError::NoAttributedQueries { .. }));
        // And it names the two mistakes that produce it.
        let text = format!("{e}");
        assert!(text.contains("qualified"), "{text}");
        assert!(text.contains("sampler"), "{text}");
    }

    /// Absence and zero are different answers, and `ProfileEvents['X']` cannot
    /// tell them apart because the map omits every counter whose value is zero.
    /// Parsing the map itself is what keeps them distinguishable.
    #[test]
    fn a_counter_the_server_did_not_report_is_absent_rather_than_zero() {
        let empty = parse_profile_events("{}").expect("an empty map parses");
        assert_eq!(empty, ProfileCounters::default());
        assert_eq!(empty.cpu_us(), None);

        // Kernel time genuinely zero on a short insert is not the same as the
        // server never having said.
        let only_user =
            parse_profile_events("{'UserTimeMicroseconds':1200}").expect("one entry parses");
        assert_eq!(only_user.system_us, None);
        assert_eq!(only_user.cpu_us(), Some(1200));

        let body = "QueryFinish\t10\t1\t{}\n";
        let e = summarised(body).expect_err("no CPU counters at all must be refused");
        assert!(matches!(e, ServerSideError::NoCpuCounters { queries: 1 }));
    }

    /// A row whose shape changed is a refusal, because every value parsed out of
    /// a shape we no longer recognise is a guess.
    #[test]
    fn a_row_that_does_not_match_the_projection_is_refused() {
        assert!(matches!(
            parse_response("QueryFinish\t10\t1\n"),
            Err(ServerSideError::Malformed { .. })
        ));
        assert!(matches!(
            parse_response("Whatever\t10\t1\t{}\n"),
            Err(ServerSideError::Malformed { .. })
        ));
        assert!(matches!(
            parse_profile_events("UserTimeMicroseconds=5"),
            Err(ServerSideError::Malformed { .. })
        ));
        assert!(matches!(
            parse_profile_events("{'UserTimeMicroseconds':not-a-number}"),
            Err(ServerSideError::Malformed { .. })
        ));
        // A counter this binary does not know is ignored, so a future ClickHouse
        // adding one to the projection does not fail the read.
        let ok = parse_profile_events("{'SomethingNew':7,'UserTimeMicroseconds':5}")
            .expect("an unknown key is not a defect");
        assert_eq!(ok.user_us, Some(5));
    }

    /// `docker::clickhouse_sql` asserts on `DB::Exception`, which would take a
    /// thirty-hour sweep down over one disabled system table. This module reads
    /// through the non-asserting call and turns the exception into a refusal that
    /// costs the run one metric.
    #[test]
    fn a_server_exception_is_a_typed_refusal_rather_than_a_panic() {
        let body = "Code: 60. DB::Exception: Table system.query_log does not exist.";
        let e = parse_response(body).expect_err("an exception body must be refused");
        assert!(matches!(e, ServerSideError::ServerException(_)));
        assert!(format!("{e}").contains("query_log"));
    }

    /// The predicates that keep the harness's own queries out. Each one is doing
    /// distinct work, and the query is checked as text because it is the only
    /// artefact a reader can audit without a live server.
    #[test]
    fn the_attribution_query_excludes_the_harnesss_own_queries() {
        let sql = attribution_sql(&["default.sensor_events"], window(), false);
        assert!(sql.contains("query_kind = 'Insert'"), "{sql}");
        assert!(
            sql.contains("hasAny(tables, ['default.sensor_events'])"),
            "{sql}"
        );
        assert!(sql.contains("AND is_initial_query"), "{sql}");
        assert!(
            sql.contains(
                "query_start_time_microseconds >= fromUnixTimestamp64Milli(1784979298378)"
            ),
            "{sql}"
        );
        assert!(
            sql.contains("query_start_time_microseconds < fromUnixTimestamp64Milli(1784979598378)"),
            "{sql}"
        );
        // Partition pruning, a day either side, because `event_date` is derived
        // in the server's timezone and the bounds are epoch milliseconds.
        assert!(sql.contains("event_date >="), "{sql}");
        assert!(sql.contains("event_date <="), "{sql}");
        // Every counter the summary can read has to be in the projection, or it
        // is absent for a reason that has nothing to do with the server.
        for c in COUNTERS {
            assert!(sql.contains(c), "{c} is missing from the projection");
        }
        // Both tables of a multi-table arm.
        let two = attribution_sql(
            &["default.landing", "default.sensor_events"],
            window(),
            false,
        );
        assert!(
            two.contains("hasAny(tables, ['default.landing', 'default.sensor_events'])"),
            "{two}"
        );
    }

    /// The one arm shape that inverts `is_initial_query`: a Distributed-forwarded
    /// insert executes on the shared server as a non-initial query — the initial
    /// one ran on the arm's own node, whose log this module never reads — so the
    /// strict predicate would attribute nothing and the refusal would read as an
    /// arm that never inserted. The predicate is negated, never dropped: with no
    /// predicate at all, a `Distributed` table living on the shared server would
    /// have BOTH the initial query and its forwarded execution match, and the
    /// arm's `written_rows` and CPU would be double-counted.
    #[test]
    fn a_forwarded_inserts_arm_inverts_the_initial_query_predicate() {
        let strict = attribution_sql(&["default.sensor_events"], window(), false);
        let forwarded = attribution_sql(&["default.sensor_events"], window(), true);
        // The strict predicate appears exactly once, so the replace below is
        // provably a single edit and not a family of them.
        assert_eq!(
            strict.matches("AND is_initial_query ").count(),
            1,
            "{strict}"
        );
        // The whole transformation is that one negation and nothing else: the
        // forwarded query IS the strict query with the predicate inverted, so
        // every other predicate survives by equality rather than by a
        // hand-maintained list.
        assert_eq!(
            forwarded,
            strict.replace("AND is_initial_query ", "AND NOT is_initial_query "),
        );
    }

    /// The window is the sampler's, so that the server-side figure and the arm's
    /// own CPU rest on one interval — and so that it is taken on the clock inside
    /// the Docker VM rather than the host's.
    #[test]
    fn the_window_comes_from_the_samplers_own_timestamps() {
        let series = Samples {
            meta: String::new(),
            rows: vec![
                crate::sampler::Sample {
                    t_ms: 1_000,
                    usage_usec: 0,
                    user_usec: 0,
                    system_usec: 0,
                    nr_throttled: 0,
                    throttled_usec: 0,
                    mem_current: 1,
                    mem_peak: 1,
                    anon: 1,
                    file: 0,
                    slab: 0,
                    kernel_stack: 0,
                    sock: 0,
                },
                // Unreadable: a sentinel row is not evidence of a time either.
                crate::sampler::Sample {
                    t_ms: 9_999,
                    usage_usec: -1,
                    user_usec: -1,
                    system_usec: -1,
                    nr_throttled: -1,
                    throttled_usec: -1,
                    mem_current: 1,
                    mem_peak: 1,
                    anon: 1,
                    file: 0,
                    slab: 0,
                    kernel_stack: 0,
                    sock: 0,
                },
                crate::sampler::Sample {
                    t_ms: 5_000,
                    usage_usec: 10,
                    user_usec: 10,
                    system_usec: 0,
                    nr_throttled: 0,
                    throttled_usec: 0,
                    mem_current: 1,
                    mem_peak: 1,
                    anon: 1,
                    file: 0,
                    slab: 0,
                    kernel_stack: 0,
                    sock: 0,
                },
            ],
            wall_s: 4.0,
        };
        let w = Window::spanning(&[&series]).expect("two readable samples span a window");
        assert_eq!(
            w,
            Window {
                from_ms: 1_000,
                to_ms: 5_000
            }
        );
        assert!((w.seconds() - 4.0).abs() < 1e-9);

        // A window that does not run forwards is refused rather than silently
        // matching nothing, which would be diagnosed as an arm that never
        // inserted.
        assert!(matches!(
            Window::new(5, 5),
            Err(ServerSideError::EmptyWindow { .. })
        ));
    }

    /// The caveats travel with the number. A reader told the server-side cost
    /// without being told it excludes background merges has been told something
    /// slightly false, because an arm writing small batches pushes work into
    /// merges that this figure cannot see.
    #[test]
    fn the_provenance_line_states_what_the_figure_leaves_out() {
        let cost = summarised(REAL_INSERTS).expect("three real inserts summarise");
        let p = cost.provenance();
        assert!(p.contains("3 insert(s)"), "{p}");
        assert!(p.contains("default.sensor_events_t"), "{p}");
        assert!(p.contains("excludes background merges"), "{p}");
        assert!((cost.rows_per_insert() - 262_328.0).abs() < 1.0, "{p}");
    }
}
