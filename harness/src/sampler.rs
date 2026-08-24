//! Framework-neutral resource measurement for the cross-framework comparison.
//!
//! The comparison publishes numbers about other people's software, so nothing
//! here may depend on the thing being measured. Every quantity this module
//! produces is read from **outside** the framework under test — from its cgroup —
//! and is therefore obtained identically whether that framework is Spate, a
//! JVM, a Go binary, or ClickHouse consuming a topic by itself. No `etl_*`
//! metric family, and no competitor's own instrumentation, feeds a published
//! figure.
//!
//! The framework under test always runs in a container, including Spate: an
//! in-process host run would get every core on the box and make the resource
//! envelope meaningless.
//!
//! ## Why a sidecar rather than `docker stats`
//!
//! `docker stats` reports a CPU *percentage* computed over an interval it
//! chooses, with no cumulative microsecond counter, and its memory column folds
//! in page cache. Neither supports a defensible CPU-per-record figure. Reading
//! cgroup v2 directly gives monotonic `usage_usec` and a page-cache-free `anon`,
//! plus `nr_throttled`/`throttled_usec`, which answer "why was it X and not 2X?"
//! with evidence instead of inference.
//!
//! The sidecar is necessary because on Docker Desktop for macOS the cgroup
//! filesystem lives inside the Linux VM and cannot be read from the host at all.
//!
//! ## Two lessons paid for during bring-up, both encoded below
//!
//! * **`memory.peak`'s reset is scoped to the file descriptor.** Writing to it
//!   resets the value only for subsequent reads through that *same* fd; a fresh
//!   open still returns the cgroup's lifetime peak. The sampler holds the fd, so
//!   its peak is a true windowed peak. The driver starts the sampler when the
//!   arm's containers start and stops it the instant the drain completes, so the
//!   sampling window *is* the measurement window with no signalling between the
//!   two — and it is the **only** window: [`SutCost`] derives mean cores and
//!   throughput from that one interval, so the two cannot be computed against
//!   different ones.
//! * **Killing the `docker` CLI does not stop the container.** A `timeout` on
//!   `docker run` detaches the client and leaves the container alive holding the
//!   stdout pipe open. Every container started here is therefore named and
//!   removed by name, never signalled.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::docker::{NETWORK, docker, docker_try};

/// The sampler container's name. Fixed, so an orphan from an interrupted run is
/// always cleaned up by the next one rather than accumulating.
/// Milliseconds between cgroup readings.
///
/// Both ends of a window's CPU delta are sampler readings while a drain's row
/// count is the whole corpus, so a window clipped at either end understates CPU
/// against a full numerator and reads flatteringly low. The clipping is bounded
/// by this interval, so the interval bounds that error.
pub const INTERVAL_MS: u64 = 100;

/// The same interval in seconds, which is what the sampler process is given.
pub const INTERVAL_S: f64 = INTERVAL_MS as f64 / 1000.0;

const SAMPLER_CONTAINER: &str = "spate-bench-sampler";

/// Image used for the sampler. Chosen only because it has a Python interpreter;
/// nothing about it is under measurement.
const SAMPLER_IMAGE: &str = "python:3.12-alpine";

/// The sampler program, embedded at compile time and fed to the container on
/// stdin (`python3 -`). Passing it on stdin rather than mounting it keeps the
/// harness free of any bind mount, which on macOS would cross VirtioFS.
const SAMPLER_SRC: &str = include_str!("../../workload/sampler/sample.py");

/// Resolve a container's full 64-hex id from its name.
///
/// The cgroup directory is named after the full id, not the short form.
#[must_use]
pub fn container_id(name: &str) -> String {
    let id = docker(&["inspect", "-f", "{{.Id}}", name]);
    assert_eq!(
        id.len(),
        64,
        "expected a 64-hex container id for {name}, got {id:?}"
    );
    id
}

/// One sample row from the cgroup sampler. `-1` means the field was unreadable
/// at that instant, which is preserved rather than zeroed: a zero would read as
/// "idle" where the truth is "unknown".
///
/// Preserved here, and refused in [`Samples::summarise`] — see
/// [`Sample::readable`]. The sentinel is evidence about the sampler, never an
/// input to a published number.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sample {
    /// Wall-clock milliseconds since the epoch, taken inside the sampler.
    pub t_ms: u64,
    /// Cumulative CPU time charged to the cgroup, microseconds.
    pub usage_usec: i64,
    /// Cumulative user-mode CPU time, microseconds.
    pub user_usec: i64,
    /// Cumulative kernel-mode CPU time, microseconds.
    pub system_usec: i64,
    /// Number of CFS periods in which the cgroup was throttled.
    pub nr_throttled: i64,
    /// Cumulative time the cgroup spent throttled, microseconds.
    pub throttled_usec: i64,
    /// Current charged memory, including page cache.
    pub mem_current: i64,
    /// Peak charged memory **since the sampler started**, via the held fd.
    pub mem_peak: i64,
    /// Anonymous memory — the page-cache-free figure the comparison headlines.
    pub anon: i64,
    /// Page-cache memory charged to the cgroup.
    pub file: i64,
    /// Kernel slab memory.
    pub slab: i64,
    /// Kernel stack memory.
    pub kernel_stack: i64,
    /// Socket buffer memory.
    pub sock: i64,
}

impl Sample {
    fn parse(line: &str) -> Option<Self> {
        let f: Vec<i64> = line
            .split(',')
            .map(|s| s.trim().parse().ok())
            .collect::<Option<_>>()?;
        if f.len() != 13 {
            return None;
        }
        Some(Self {
            t_ms: u64::try_from(f[0]).ok()?,
            usage_usec: f[1],
            user_usec: f[2],
            system_usec: f[3],
            nr_throttled: f[4],
            throttled_usec: f[5],
            mem_current: f[6],
            mem_peak: f[7],
            anon: f[8],
            file: f[9],
            slab: f[10],
            kernel_stack: f[11],
            sock: f[12],
        })
    }

    /// Whether every counter in this row was actually read.
    ///
    /// `workload/sampler/sample.py` emits `-1` for any field it could not read,
    /// and the fields do not fail independently: an unreadable `memory.stat`
    /// takes every one of its keys with it, and an unresettable `memory.peak`
    /// makes `mem_peak` a sentinel for the whole run. So one `-1` anywhere in a
    /// row means the row is not evidence, and [`Samples::summarise`] drops it.
    ///
    /// Defect this closes: the sentinel used to be summarised like a
    /// measurement. Three consequences, all of which published rather than
    /// failed. `peak_anon_bytes` and `peak_charged_bytes` were passed straight
    /// through, so a record could carry
    /// `"peak_anon_bytes": {"value": -1.0, "unit": "bytes"}` with `status: ok`.
    /// Summed across a multi-container arm, a `-1` silently deducted a byte from
    /// an otherwise plausible total, which is worse — nothing about the number
    /// looks wrong. And because `delta` clamps a negative difference to zero, a
    /// `-1` in the **last** CPU sample produced `cores_used = 0`: an arm that
    /// appears to have consumed no CPU at all, the most flattering possible
    /// wrong answer.
    #[must_use]
    pub fn readable(&self) -> bool {
        [
            self.usage_usec,
            self.user_usec,
            self.system_usec,
            self.nr_throttled,
            self.throttled_usec,
            self.mem_current,
            self.mem_peak,
            self.anon,
            self.file,
            self.slab,
            self.kernel_stack,
            self.sock,
        ]
        .iter()
        .all(|v| *v >= 0)
    }
}

/// A running cgroup sampler for one container.
#[derive(Debug)]
pub struct Sampler {
    child: Child,
    lines: Arc<Mutex<Vec<String>>>,
    started: Instant,
    name: String,
    stopped: bool,
}

impl Sampler {
    /// Start sampling `target` at `interval_s`.
    ///
    /// Call this the moment the arm's container starts, and stop it the instant
    /// the drain completes: the sampler resets `memory.peak` on its own held fd
    /// at startup, so the sampling window *is* the measurement window, and
    /// [`SutCost`] is the only place a rate may be derived from it.
    ///
    /// This doc used to read "call this at the point steady state is detected,
    /// not at container start", while the driver called it at container start.
    /// The instruction was left over from a windowed protocol and described a
    /// plateau detector nothing called; a full drain has no window to place
    /// inside. See `driver::Mode::Drain`.
    ///
    /// # Panics
    /// If the sampler container cannot be started, or its stdin/stdout cannot be
    /// captured — a silent measurement failure would be worse than a loud one.
    #[must_use]
    pub fn start(target: &str, interval_s: f64) -> Self {
        Self::start_named(target, interval_s, SAMPLER_CONTAINER)
    }

    /// Like [`start`](Self::start) but with an explicit sampler container name.
    ///
    /// Needed because an arm can be several containers — a Flink arm is a
    /// JobManager plus a TaskManager — and each needs its own sampler. The fixed
    /// name would make the second sampler evict the first.
    ///
    /// # Panics
    /// As [`start`](Self::start).
    #[must_use]
    pub fn start_named(target: &str, interval_s: f64, sampler_name: &str) -> Self {
        // The sampler receives the container ID, not a cgroup path: where the
        // cgroup lives depends on the daemon's cgroup driver (cgroupfs puts it
        // at docker/<id>, systemd at system.slice/docker-<id>.scope), and only
        // the sampler — inside a container, with the tree mounted at /cg — can
        // probe which layout this host uses. See workload/sampler/sample.py.
        let id = container_id(target);
        // An orphan from an interrupted run would hold the name.
        let _ = docker_try(&["rm", "-f", sampler_name]);
        let sampler_container = sampler_name.to_owned();

        let interval = interval_s.to_string();
        let mut child = Command::new("docker")
            .args([
                "run",
                "--rm",
                "-i",
                "--name",
                &sampler_container,
                "--network",
                NETWORK,
                // The sampler must see the VM's cgroup tree, not its own
                // namespaced view, or the target's cgroup is invisible to it.
                "--cgroupns=host",
                // rw, because resetting `memory.peak` is a write.
                "-v",
                "/sys/fs/cgroup:/cg:rw",
                SAMPLER_IMAGE,
                "python3",
                "-",
                &id,
                &interval,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn cgroup sampler");

        child
            .stdin
            .take()
            .expect("sampler stdin")
            .write_all(SAMPLER_SRC.as_bytes())
            .expect("feed sampler program");

        let stdout = child.stdout.take().expect("sampler stdout");
        let lines = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&lines);
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                sink.lock().expect("sampler line lock").push(line);
            }
        });

        Self {
            child,
            lines,
            started: Instant::now(),
            name: sampler_container,
            stopped: false,
        }
    }

    /// Stop the sampler and return everything it collected.
    ///
    /// Stops by removing the container: killing the `docker` CLI leaves the
    /// container running and holding the pipe open.
    #[must_use]
    pub fn stop(mut self) -> Samples {
        self.shutdown();
        let elapsed = self.started.elapsed().as_secs_f64();
        let collected = self.lines.lock().expect("sampler line lock").clone();

        let mut meta = String::new();
        let mut rows = Vec::new();
        for line in &collected {
            if let Some(rest) = line.strip_prefix('#') {
                meta = rest.trim().to_owned();
            } else if let Some(s) = Sample::parse(line) {
                rows.push(s);
            }
        }
        Samples {
            meta,
            rows,
            wall_s: elapsed,
        }
    }

    /// Remove the sampler container and reap the `docker` client, exactly once.
    fn shutdown(&mut self) {
        if std::mem::replace(&mut self.stopped, true) {
            return;
        }
        let _ = docker_try(&["rm", "-f", &self.name]);
        let _ = self.child.wait();
    }
}

impl Drop for Sampler {
    /// Defect this closes: a sampler was only ever removed by [`stop`](Self::stop),
    /// so every refusal that abandoned a drain — an arm that exited, a drain that
    /// ran past its deadline — dropped its `Sampler` values without stopping
    /// them and left one container per arm still running, sampling a cgroup that
    /// no longer existed, on a host `methodology/` documents as
    /// oversubscribed. The next arm then measured against them.
    ///
    /// Dropping the value cannot stop the container by itself: killing the
    /// `docker` CLI detaches the client and leaves the container alive, which is
    /// why this removes by name.
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Everything one sampler run collected.
#[derive(Clone, Debug)]
pub struct Samples {
    /// The sampler's header line: cgroup path, `cpu.max`, `memory.max`.
    /// Recorded so a published arm can prove the envelope was actually applied
    /// rather than merely requested.
    pub meta: String,
    /// The sample series, in order.
    pub rows: Vec<Sample>,
    /// Wall-clock seconds the sampler was alive, from the driver's clock.
    pub wall_s: f64,
}

impl Samples {
    /// Summarise the series into the cost figures the comparison publishes.
    ///
    /// Only [readable](Sample::readable) rows are summarised. A row carrying a
    /// `-1` is discarded rather than arithmetic-ed, because the sentinel means
    /// "not read" and every use it had here — a peak, a delta endpoint, a
    /// published byte count — silently turned it into a measurement.
    ///
    /// Returns `None` when fewer than two readable samples landed: a single
    /// sample gives no CPU delta, and inventing one from a lifetime counter
    /// would silently charge the framework for its own startup. A sampler whose
    /// every row is unreadable — an unresettable `memory.peak`, a cgroup that
    /// vanished — therefore refuses the arm instead of publishing sentinels.
    #[must_use]
    pub fn summarise(&self) -> Option<SutCost> {
        let readable: Vec<&Sample> = self.rows.iter().filter(|s| s.readable()).collect();
        let unreadable = self.rows.len() - readable.len();
        let first = *readable.first()?;
        let last = *readable.last()?;
        if readable.len() < 2 {
            return None;
        }
        let window_s = (last.t_ms.saturating_sub(first.t_ms)) as f64 / 1000.0;
        if window_s <= 0.0 {
            return None;
        }
        let delta = |f: fn(&Sample) -> i64| (f(last) - f(first)).max(0) as f64;
        let cpu_us = delta(|s| s.usage_usec);
        Some(SutCost {
            window_s,
            cpu_us,
            user_us: delta(|s| s.user_usec),
            system_us: delta(|s| s.system_usec),
            // Mean cores occupied over the window. Directly comparable to the
            // container's `--cpus` cap, so a value at the cap says the arm is
            // CPU-bound without needing the throttle counters to say it.
            cores_used: cpu_us / (window_s * 1_000_000.0),
            throttled_us: delta(|s| s.throttled_usec),
            nr_throttled: delta(|s| s.nr_throttled),
            // The headline footprint: page-cache-free, so a framework is not
            // charged for the kernel caching its own input.
            peak_anon_bytes: readable.iter().map(|s| s.anon).max()? as f64,
            // Windowed, via the sampler's held fd — not a lifetime peak.
            peak_charged_bytes: last.mem_peak as f64,
            peak_current_bytes: readable.iter().map(|s| s.mem_current).max()? as f64,
            samples: readable.len(),
            unreadable,
        })
    }
}

/// The largest anonymous memory an arm occupied **at any one instant**.
///
/// Not the sum of each container's peak, which is what a multi-container arm
/// used to publish. Summing maxima answers "how much could they have used
/// between them" — an upper bound that is only reached if every container peaked
/// simultaneously, which nothing makes them do. A JobManager that spikes during
/// job submission and a TaskManager that spikes late in the drain would be
/// charged as though they had spiked together.
///
/// The error is small and it is in the wrong direction: it over-reports the
/// arm total, so it penalises exactly the multi-process arms the envelope rule
/// already goes out of its way not to penalise, on the one panel where a JVM
/// looks worst.
///
/// The series are sampled independently and are not synchronised, so
/// they are aligned into one-second buckets by their own timestamps. A container
/// with no sample in a bucket contributes its last known reading rather than
/// zero: these are levels, not events, and a gap means "not observed", not
/// "released its memory". Buckets before a container's first sample contribute
/// nothing from it, because it had not started.
///
/// Returns `None` if no series has a readable sample, which is the same
/// condition under which the arm has no cost at all.
#[must_use]
pub fn simultaneous_peak_anon(series: &[&[Sample]]) -> Option<f64> {
    /// Bucket width, equal to the sampler interval: finer would invent
    /// alignment the series cannot support.
    const BUCKET_MS: u64 = INTERVAL_MS;

    let readable: Vec<Vec<&Sample>> = series
        .iter()
        .map(|rows| rows.iter().filter(|s| s.readable()).collect())
        .collect();
    if readable.iter().all(Vec::is_empty) {
        return None;
    }

    let first = readable
        .iter()
        .filter_map(|rows| rows.first().map(|s| s.t_ms))
        .min()?;
    let last = readable
        .iter()
        .filter_map(|rows| rows.last().map(|s| s.t_ms))
        .max()?;

    let mut peak = 0_i128;
    let mut cursors = vec![0_usize; readable.len()];
    let mut held: Vec<Option<i64>> = vec![None; readable.len()];

    let mut bucket = first;
    while bucket <= last {
        let edge = bucket.saturating_add(BUCKET_MS);
        let mut total = 0_i128;
        for (i, rows) in readable.iter().enumerate() {
            // Advance to the last sample that falls at or before this bucket's
            // edge, so a container sampling slightly fast does not skip one.
            while cursors[i] < rows.len() && rows[cursors[i]].t_ms < edge {
                held[i] = Some(rows[cursors[i]].anon);
                cursors[i] += 1;
            }
            if let Some(v) = held[i] {
                total += i128::from(v);
            }
        }
        peak = peak.max(total);
        bucket = edge;
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "byte counts stay far below f64's exact range"
    )]
    Some(peak as f64)
}

/// Resource cost of one framework arm over one measurement window.
///
/// **This type owns the window, and every rate a record publishes is derived
/// from it here.** That is the point of putting [`rows_per_s`](Self::rows_per_s)
/// on a struct named for cost: `cores_used`, [`cpu_us_per_row`](Self::cpu_us_per_row)
/// and `rows_per_s` are three views of two quantities over one interval, and the
/// identity
///
/// ```text
/// cores_used == cpu_us_per_row(rows) * rows_per_s(rows) / 1e6
/// ```
///
/// holds by construction rather than by two call sites happening to agree.
///
/// Defect this closes: they did not agree. The driver timed the drain on its own
/// clock and divided rows by an interval that ran from the first landed row to
/// *after* `quiesce` (three or more stable polls) and `stop_all` (a `docker
/// logs` plus a `docker rm -f` per container), plus a `+1.0` fudge, while
/// `cores_used` rested on the sampler's window. The two forms of the identity
/// above disagreed by +5.9% to +11.2% on every published Spate record and by
/// −2.1% to +0.6% on every published Flink record — arm-dependent, so it
/// distorted the comparison and not merely the absolute values.
///
/// The window is the sampler's own, from container start to the instant the
/// drain completes. Two reasons for that endpoint rather than the first landed
/// row. It is the interval the CPU numerator is already measured over, so no
/// cross-clock join between the driver's `Instant` and the sampler's wall clock
/// is needed to make the identity exact. And it charges an arm for its own
/// startup, which is the honest reading of a drain: the corpus is prefilled and
/// sitting on the broker from the moment the arm starts, so every second an arm
/// spends coming up is a second in which it drains nothing. Opening the window
/// at the first landed row would have to exclude startup from the CPU numerator
/// too, which would hand a costly start-up back to the arm for free.
#[derive(Clone, Copy, Debug)]
pub struct SutCost {
    /// Length of the window, from the sampler's own timestamps. The one
    /// denominator; see the type docs.
    pub window_s: f64,
    /// CPU microseconds consumed in the window.
    pub cpu_us: f64,
    /// User-mode share of `cpu_us`.
    pub user_us: f64,
    /// Kernel-mode share of `cpu_us`.
    pub system_us: f64,
    /// Mean cores occupied (`cpu_us / window`). Compare against the `--cpus` cap.
    pub cores_used: f64,
    /// Microseconds spent throttled by the CPU cap.
    pub throttled_us: f64,
    /// CFS periods in which throttling occurred.
    pub nr_throttled: f64,
    /// Peak anonymous memory — the published footprint figure.
    pub peak_anon_bytes: f64,
    /// Peak charged memory over the window (includes page cache).
    pub peak_charged_bytes: f64,
    /// Peak `memory.current` seen in the series (includes page cache).
    pub peak_current_bytes: f64,
    /// Number of readable samples the summary rests on.
    pub samples: usize,
    /// Samples discarded as unreadable. Carried rather than dropped so a record
    /// can say that its numbers rest on a series with holes in it.
    pub unreadable: usize,
}

impl SutCost {
    /// Sum the cost of several containers into one arm's cost.
    ///
    /// This is what makes a multi-container arm measurable against a
    /// single-container one. A Flink arm is a JobManager plus a TaskManager, and
    /// the resource envelope is defined over the arm as a whole — so CPU,
    /// footprint and throttling **add**, while the window is the longest of them
    /// (they are sampled concurrently, so it is one shared window, not a sum).
    ///
    /// Reporting only the data-plane container would quietly under-report a
    /// framework that needs a control plane, and hand us a win we had not earned.
    ///
    /// **`None` unless every part is present**, which is why the argument is a
    /// slice of `Option`. Defect this closes: the previous signature took
    /// `&[Self]` and returned `None` only for an empty slice, so the driver
    /// filtered its missing summaries out and summed what was left. The refusal
    /// "fewer than two sampler samples" was therefore exact for a
    /// single-container arm and vacuous for a multi-container one: a Flink arm
    /// whose TaskManager sampler yielded one sample was published with the
    /// JobManager's ~0.067 cores as the arm's entire cost — a ~25× efficiency
    /// win, carrying `status: ok`. A partial arm is not a cheap arm.
    ///
    /// `cores_used` is recomputed from the summed CPU over the shared window
    /// rather than summed from the parts. Summing per-part means would divide
    /// each part by its own slightly different window and break the identity in
    /// the type docs for exactly the multi-container arms this function exists
    /// for.
    #[must_use]
    pub fn sum(parts: &[Option<Self>]) -> Option<Self> {
        if parts.is_empty() || parts.iter().any(Option::is_none) {
            return None;
        }
        let present: Vec<Self> = parts.iter().filter_map(|p| *p).collect();
        let add = |f: fn(&Self) -> f64| present.iter().map(f).sum::<f64>();
        let window_s = present.iter().map(|p| p.window_s).fold(0.0_f64, f64::max);
        if window_s <= 0.0 {
            return None;
        }
        let cpu_us = add(|p| p.cpu_us);
        Some(Self {
            window_s,
            cpu_us,
            user_us: add(|p| p.user_us),
            system_us: add(|p| p.system_us),
            cores_used: cpu_us / (window_s * 1_000_000.0),
            throttled_us: add(|p| p.throttled_us),
            nr_throttled: add(|p| p.nr_throttled),
            peak_anon_bytes: add(|p| p.peak_anon_bytes),
            peak_charged_bytes: add(|p| p.peak_charged_bytes),
            peak_current_bytes: add(|p| p.peak_current_bytes),
            samples: present.iter().map(|p| p.samples).min().unwrap_or(0),
            unreadable: present.iter().map(|p| p.unreadable).sum(),
        })
    }

    /// CPU microseconds per row landed — the efficiency metric the comparison
    /// leads with, and the one that is meaningful across languages.
    #[must_use]
    pub fn cpu_us_per_row(&self, rows: f64) -> f64 {
        if rows <= 0.0 {
            f64::NAN
        } else {
            self.cpu_us / rows
        }
    }

    /// Rows per second over **this** window — the headline throughput figure.
    ///
    /// On the cost type rather than at the call site on purpose. A throughput is
    /// a count over an interval, and the interval has to be the one the CPU
    /// figures were measured over or the record contradicts itself; the only way
    /// to make that impossible by accident is to give the caller no other
    /// interval to divide by. See the type docs for what happened when there
    /// were two.
    #[must_use]
    pub fn rows_per_s(&self, rows: f64) -> f64 {
        if self.window_s > 0.0 {
            rows / self.window_s
        } else {
            0.0
        }
    }

    /// Whether the CPU cap was a binding constraint. Reported rather than
    /// inferred: an arm that throttled was cap-bound, which is the honest answer
    /// to "why was it X and not 2X?".
    #[must_use]
    pub fn was_throttled(&self) -> bool {
        self.nr_throttled > 0.0
    }
}

// ---------------------------------------------------------------------------
// Serialising arms
// ---------------------------------------------------------------------------

/// Path of the cross-arm advisory lock. A fixed, boring path so that a shell
/// script or a Java harness can take the same lock with `set -o noclobber`; the
/// lock is not Rust-specific and must not be.
pub const LOCK_PATH: &str = "/tmp/spate-bench-comparison.lock";

/// Exclusive right to run one arm against the shared infrastructure.
///
/// This exists because its absence already cost a measurement. Two arms ran
/// concurrently against the same Redpanda and ClickHouse, and one driver
/// `TRUNCATE`d the shared target table five times inside the other's run — so the
/// second arm's throughput numbers were unusable and its correctness had to be
/// re-verified on separate tables. Nothing about that failure was visible while it
/// was happening, which is exactly why it needs a lock rather than a convention.
///
/// Acquisition is atomic (`create_new`, i.e. `O_EXCL`). The holder writes its pid
/// and a description so a refusal can say who is running and since when.
#[derive(Debug)]
pub struct ArmLock {
    path: std::path::PathBuf,
}

impl ArmLock {
    /// Take the lock, or return the current holder's description.
    ///
    /// A lock whose recorded pid is no longer alive is treated as stale and
    /// reclaimed: a crashed run must not block the suite forever. `FORCE_UNLOCK=1`
    /// overrides a live holder, which is a deliberate foot-gun for when a holder
    /// is wedged.
    pub fn acquire(description: &str) -> Result<Self, String> {
        let path = std::path::PathBuf::from(LOCK_PATH);
        if std::env::var("FORCE_UNLOCK").is_ok_and(|v| v == "1") {
            let _ = std::fs::remove_file(&path);
        }
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut f) => {
                    let _ = std::io::Write::write_all(
                        &mut f,
                        format!("{} {description}\n", std::process::id()).as_bytes(),
                    );
                    return Ok(Self { path });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    let held = std::fs::read_to_string(&path).unwrap_or_default();
                    let holder_pid = held
                        .split_whitespace()
                        .next()
                        .and_then(|p| p.parse::<u32>().ok());
                    // A dead holder is stale; reclaim and retry once.
                    if holder_pid.is_some_and(|pid| !pid_alive(pid)) {
                        eprintln!("reclaiming stale arm lock from dead pid {holder_pid:?}");
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                    return Err(format!(
                        "another arm holds {LOCK_PATH}: {}. Arms MUST run one at a \
                         time — they share one Redpanda and one ClickHouse, and the \
                         driver truncates the target table. Wait, or pass \
                         FORCE_UNLOCK=1 if that holder is wedged.",
                        held.trim()
                    ));
                }
                Err(e) => return Err(format!("could not take {LOCK_PATH}: {e}")),
            }
        }
    }
}

impl Drop for ArmLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Whether a pid is alive, via `kill -0`. Shelling out avoids a `libc`
/// dependency for one probe, and this is not on any hot path.
fn pid_alive(pid: u32) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .output()
        .is_ok_and(|o| o.status.success())
}

// ---------------------------------------------------------------------------
// The framework under test
// ---------------------------------------------------------------------------

/// How to launch one framework arm.
#[derive(Clone, Debug)]
pub struct SutSpec {
    /// Container name; removed before and after the run.
    pub name: String,
    /// Image to run.
    pub image: String,
    /// `--cpus` value. The whole budget, control plane included.
    pub cpus: String,
    /// `--memory` value; `--memory-swap` is set to the same, so memory pressure
    /// surfaces instead of moving into swap where we are not measuring.
    pub memory: String,
    /// Environment passed to the arm.
    pub env: Vec<(String, String)>,
    /// Command arguments after the image. Flink's image dispatches on these
    /// (`standalone-job`, `taskmanager`); a single-container arm leaves it empty.
    pub args: Vec<String>,
    /// `-v` arguments. Named volumes only — never a host bind mount, which on
    /// macOS crosses VirtioFS. Flink needs the same checkpoint volume visible in
    /// both its containers.
    pub volumes: Vec<String>,
}

/// Start a framework arm, replacing any container of the same name.
///
/// # Panics
/// If the image is missing or `docker run` is rejected — a benchmark that
/// silently measured nothing would be worse than a loud failure.
pub fn start_sut(spec: &SutSpec) {
    assert!(
        docker_try(&["image", "inspect", &spec.image]).is_ok(),
        "image {} is not built. Build the arm's Dockerfile first.",
        spec.image
    );
    let _ = docker_try(&["rm", "-f", &spec.name]);

    let cpus = format!("--cpus={}", spec.cpus);
    let mem = format!("--memory={}", spec.memory);
    // Equal to `--memory`: with swap left at its default the arm would silently
    // swap instead of feeling its cap, and the footprint figure would record a
    // limit being respected while the real cost moved somewhere unmeasured.
    let swap = format!("--memory-swap={}", spec.memory);
    let mut args: Vec<&str> = vec![
        "run",
        "-d",
        "--name",
        &spec.name,
        "--network",
        NETWORK,
        &cpus,
        &mem,
        &swap,
    ];
    let env_args: Vec<String> = spec.env.iter().map(|(k, v)| format!("{k}={v}")).collect();
    for e in &env_args {
        args.push("-e");
        args.push(e);
    }
    for v in &spec.volumes {
        args.push("-v");
        args.push(v);
    }
    args.push(&spec.image);
    for a in &spec.args {
        args.push(a);
    }
    docker(&args);
}

/// Start every container of a multi-container arm, in order.
///
/// Order matters for Flink: the JobManager must be up before a TaskManager can
/// register with it.
pub fn start_arm(specs: &[SutSpec]) {
    for spec in specs {
        start_sut(spec);
    }
}

/// Sample every container of an arm concurrently, one sampler each.
///
/// Returns the per-container costs in the order given, so the driver can publish
/// both the arm total and each part — the contract promises a TaskManager-only
/// figure alongside the total, so that nobody can claim we taxed Flink for its
/// JobManager, and so the control plane's real cost is a published fact rather
/// than an allocation we chose to charge.
#[must_use]
pub fn sample_arm(names: &[String], interval_s: f64) -> Vec<Sampler> {
    names
        .iter()
        .enumerate()
        .map(|(i, n)| Sampler::start_named(n, interval_s, &format!("{SAMPLER_CONTAINER}-{i}")))
        .collect()
}

/// Stop and remove a framework arm, returning its last log lines for diagnosis.
pub fn stop_sut(name: &str) -> String {
    let logs = crate::docker::container_logs(name, 40);
    let _ = docker_try(&["rm", "-f", name]);
    logs
}

/// Whether the arm container is still running.
#[must_use]
pub fn sut_alive(name: &str) -> bool {
    docker_try(&["inspect", "-f", "{{.State.Running}}", name]).is_ok_and(|s| s == "true")
}

// ---------------------------------------------------------------------------
// Steady state
// ---------------------------------------------------------------------------
//
// There is deliberately nothing here, and its absence is the fix rather than an
// omission.
//
// This module carried a plateau detector — `detect_steady_state`, with a
// `SteadyStateConfig` of flatness, slope and floor thresholds and five tests of
// its own — that **nothing outside those tests ever called**. `Sampler::start`
// documented "call this at the point steady state is detected"; the driver
// called it at container start. `methodology/`'s harness-v1 row listed
// "plateau-detected steady state" as part of what defined the protocol version,
// which was a claim about code that ran in no measurement.
//
// It was deleted rather than wired in because `driver::Mode::Drain` already
// makes the argument against it: a full drain "removes the two things a windowed
// measurement has to get right and silently fails at: sizing a window, and
// detecting steady state inside it. There is no window — the drain is the
// measurement." Drain is the only mode the harness implements. Wiring the
// detector in would mean starting the sampler late, which excludes an arm's
// startup CPU from the numerator while the rows it landed meanwhile still count
// — the exact asymmetry `SutCost`'s one-window rule exists to prevent — and
// would reset `memory.peak` mid-run so that earlier allocations vanished from
// the footprint.
//
// If sustained mode is ever implemented it will need a plateau detector, and
// this one is in the history under `6f28a8b8912e`. What it must not do is sit
// here uncalled while a normative document says the protocol uses it.

#[cfg(test)]
mod tests {
    use super::*;

    /// A series from CSV rows, so a test reads as the sampler's own output.
    fn series(rows: &[&str]) -> Samples {
        Samples {
            meta: String::new(),
            rows: rows
                .iter()
                .map(|r| Sample::parse(r).expect("row parses"))
                .collect(),
            wall_s: 1.0,
        }
    }

    #[test]
    fn parses_a_sampler_row() {
        let row = "1784979298378,129007508,129003510,3998,660,602643,659456,659456,\
                   135168,0,399080,16384,0";
        let s = Sample::parse(row).expect("row parses");
        assert_eq!(s.t_ms, 1_784_979_298_378);
        assert_eq!(s.usage_usec, 129_007_508);
        assert_eq!(s.anon, 135_168);
        assert_eq!(s.sock, 0);
    }

    #[test]
    fn rejects_a_row_of_the_wrong_width() {
        assert!(Sample::parse("1,2,3").is_none());
        assert!(Sample::parse("not,a,row").is_none());
    }

    /// A single sample cannot yield a CPU delta, and treating its lifetime
    /// counter as the window's usage would charge the framework for its startup.
    #[test]
    fn a_single_sample_summarises_to_nothing() {
        let one = Sample::parse("1000,5,5,0,0,0,10,10,10,0,0,0,0").expect("row parses");
        let s = Samples {
            meta: String::new(),
            rows: vec![one],
            wall_s: 1.0,
        };
        assert!(s.summarise().is_none());
    }

    #[test]
    fn summarises_cpu_as_a_delta_over_the_window() {
        let rows = vec![
            Sample::parse("1000,1000000,900000,100000,0,0,100,100,80,20,0,0,0")
                .expect("row parses"),
            // +2 CPU-seconds over 1 wall-second: two cores' worth.
            Sample::parse("2000,3000000,2700000,300000,3,500,300,400,250,50,0,0,0")
                .expect("row parses"),
        ];
        let cost = Samples {
            meta: String::new(),
            rows,
            wall_s: 1.0,
        }
        .summarise()
        .expect("two samples summarise");

        assert!((cost.window_s - 1.0).abs() < 1e-9);
        assert!((cost.cpu_us - 2_000_000.0).abs() < 1e-9);
        assert!((cost.cores_used - 2.0).abs() < 1e-9);
        // Peak anon is the max over the series, not the last value.
        assert!((cost.peak_anon_bytes - 250.0).abs() < 1e-9);
        // Charged peak comes from the held fd (the last row), which is what
        // makes it a windowed rather than lifetime figure.
        assert!((cost.peak_charged_bytes - 400.0).abs() < 1e-9);
        assert!(cost.was_throttled());
        assert!((cost.cpu_us_per_row(1_000_000.0) - 2.0).abs() < 1e-9);
        assert_eq!(cost.unreadable, 0);
    }

    /// The sampler writes `-1` for a counter it could not read, and every field
    /// of a row fails together — so a row carrying one is not a sample.
    #[test]
    fn a_row_carrying_an_unreadable_counter_is_not_evidence() {
        let good = Sample::parse("1000,5,5,0,0,0,10,10,10,0,0,0,0").expect("row parses");
        assert!(good.readable());
        // `memory.peak` could not be reset, so the sampler reports the sentinel
        // for the whole run.
        let no_peak = Sample::parse("1000,5,5,0,0,0,10,-1,10,0,0,0,0").expect("row parses");
        assert!(!no_peak.readable());
        // `cpu.stat` could not be read at this instant.
        let no_cpu = Sample::parse("1000,-1,-1,-1,-1,-1,10,10,10,0,0,0,0").expect("row parses");
        assert!(!no_cpu.readable());
    }

    /// The most flattering possible wrong answer, and the one this closes: the
    /// last sample is unreadable, `delta` clamps its negative difference to
    /// zero, and the arm is published as having consumed no CPU at all.
    #[test]
    fn an_unreadable_last_sample_cannot_report_zero_cores() {
        let cost = series(&[
            "1000,1000000,900000,100000,0,0,100,100,80,20,0,0,0",
            "2000,3000000,2700000,300000,0,0,300,400,250,50,0,0,0",
            // `cpu.stat` vanished under the sampler on the way out.
            "3000,-1,-1,-1,-1,-1,300,400,250,50,0,0,0",
        ])
        .summarise()
        .expect("two readable samples summarise");

        assert!((cost.cores_used - 2.0).abs() < 1e-9, "{}", cost.cores_used);
        assert_eq!(cost.samples, 2);
        assert_eq!(cost.unreadable, 1);
    }

    /// A sentinel must never reach a published byte count. When every row
    /// carries one there is nothing left to summarise, and the arm is refused
    /// rather than recorded with `peak_anon_bytes: -1` and `status: ok`.
    #[test]
    fn a_series_that_is_never_readable_summarises_to_nothing() {
        let s = series(&[
            "1000,5,5,0,0,0,10,-1,10,0,0,0,0",
            "2000,9,9,0,0,0,10,-1,10,0,0,0,0",
            "3000,13,13,0,0,0,10,-1,10,0,0,0,0",
        ]);
        assert!(s.summarise().is_none());
    }

    /// The identity that makes a record internally consistent. Two of these
    /// three numbers are published side by side, and they disagreed by up to
    /// 11% because throughput was divided by the driver's drain-plus-teardown
    /// clock while cores rested on the sampler's.
    #[test]
    fn throughput_and_cores_rest_on_the_same_window() {
        let cost = series(&[
            "1000,1000000,900000,100000,0,0,100,100,80,20,0,0,0",
            "5000,9000000,8100000,900000,0,0,300,400,250,50,0,0,0",
        ])
        .summarise()
        .expect("two samples summarise");

        let rows = 12_345_678.0;
        let derived = cost.cpu_us_per_row(rows) * cost.rows_per_s(rows) / 1e6;
        assert!(
            (cost.cores_used - derived).abs() < 1e-9,
            "cores_used {} but cpu_us_per_row x rows_per_s / 1e6 is {derived}",
            cost.cores_used
        );
    }

    /// A multi-container arm's cost is its summed CPU over the one shared
    /// window, not the sum of per-container means over their own windows — or
    /// the identity above holds for exactly the arms it is not needed for.
    #[test]
    fn an_arms_cores_are_its_summed_cpu_over_one_shared_window() {
        // A TaskManager over 4s, and a JobManager whose sampler saw 3.9s.
        let tm = series(&[
            "1000,0,0,0,0,0,100,100,80,20,0,0,0",
            "5000,8000000,8000000,0,0,0,300,400,250,50,0,0,0",
        ])
        .summarise()
        .expect("summarises");
        let jm = series(&[
            "1100,0,0,0,0,0,10,10,8,2,0,0,0",
            "5000,390000,390000,0,0,0,30,40,25,5,0,0,0",
        ])
        .summarise()
        .expect("summarises");

        let arm = SutCost::sum(&[Some(tm), Some(jm)]).expect("both parts present");
        assert!((arm.window_s - 4.0).abs() < 1e-9);
        assert!((arm.cpu_us - 8_390_000.0).abs() < 1e-9);
        let expected = 8_390_000.0 / (4.0 * 1e6);
        assert!(
            (arm.cores_used - expected).abs() < 1e-9,
            "{}",
            arm.cores_used
        );

        let rows = 500_000.0;
        let derived = arm.cpu_us_per_row(rows) * arm.rows_per_s(rows) / 1e6;
        assert!((arm.cores_used - derived).abs() < 1e-9);
    }

    /// A partial arm is not a cheap arm. With the TaskManager's summary missing,
    /// the arm used to be published carrying the JobManager's ~0.067 cores as
    /// its whole cost — a ~25x efficiency win, with `status: ok`.
    #[test]
    fn an_arm_missing_one_containers_summary_has_no_cost_at_all() {
        let jm = series(&[
            "1000,0,0,0,0,0,10,10,8,2,0,0,0",
            "2000,67000,67000,0,0,0,30,40,25,5,0,0,0",
        ])
        .summarise()
        .expect("summarises");

        assert!(SutCost::sum(&[Some(jm), None]).is_none());
        assert!(SutCost::sum(&[None, Some(jm)]).is_none());
        assert!(SutCost::sum(&[]).is_none());
        assert!(SutCost::sum(&[Some(jm)]).is_some());
    }

    /// A 1 Hz series from `t0` carrying only the anon readings that matter here.
    fn anon_at(t0: u64, anon: &[i64]) -> Vec<Sample> {
        anon.iter()
            .enumerate()
            .map(|(i, &a)| Sample {
                t_ms: t0 + (i as u64) * 1000,
                usage_usec: 0,
                user_usec: 0,
                system_usec: 0,
                nr_throttled: 0,
                throttled_usec: 0,
                mem_current: a.max(0),
                mem_peak: a.max(0),
                anon: a,
                file: 0,
                slab: 0,
                kernel_stack: 0,
                sock: 0,
            })
            .collect()
    }

    #[test]
    fn an_arms_peak_is_what_it_held_at_once_not_the_sum_of_separate_peaks() {
        // Two containers peaking at different moments. Summing their maxima
        // gives 300, which neither the arm nor the machine ever saw.
        let a = anon_at(1_000, &[100, 10, 10]);
        let b = anon_at(1_000, &[10, 10, 200]);
        assert_eq!(simultaneous_peak_anon(&[&a, &b]), Some(210.0));
    }

    #[test]
    fn a_container_with_no_sample_in_a_bucket_holds_its_last_reading() {
        // Levels, not events: a gap means the container was not observed, not
        // that it released its memory.
        let dense = anon_at(1_000, &[10, 10, 10, 10]);
        let mut sparse = anon_at(1_000, &[500]);
        sparse.extend(anon_at(4_000, &[500]));
        assert_eq!(simultaneous_peak_anon(&[&dense, &sparse]), Some(510.0));
    }

    #[test]
    fn a_container_contributes_nothing_before_it_has_started() {
        // Charging an arm for a TaskManager that has not started yet would
        // invent memory: the JobManager runs alone for the first two seconds.
        let early = anon_at(1_000, &[100, 100, 100, 100]);
        let late = anon_at(3_000, &[400, 400]);
        assert_eq!(simultaneous_peak_anon(&[&early, &late]), Some(500.0));
    }

    #[test]
    fn a_single_container_arms_peak_is_unchanged_by_the_alignment() {
        // The common case must not move: one container's simultaneous peak is
        // exactly its own maximum.
        let only = anon_at(1_000, &[10, 900, 400]);
        assert_eq!(simultaneous_peak_anon(&[&only]), Some(900.0));
    }

    #[test]
    fn an_unreadable_sample_cannot_become_an_arms_peak() {
        let mut rows = anon_at(1_000, &[10, 20]);
        rows.extend(anon_at(3_000, &[-1]));
        assert_eq!(simultaneous_peak_anon(&[&rows]), Some(20.0));
    }

    #[test]
    fn a_series_with_nothing_readable_has_no_peak() {
        assert_eq!(simultaneous_peak_anon(&[&Vec::<Sample>::new()]), None);
    }
}
