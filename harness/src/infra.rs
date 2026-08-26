//! Bringing up the shared infrastructure, from the environment profile.
//!
//! There is exactly **one** path to a running broker and ClickHouse, and it
//! takes its caps from the environment profile rather than from ambient
//! variables. That is not tidiness: the previous harness had the caps come from
//! environment variables, and a runner script set one pair while the driver's
//! defaults declared another and the written methodology stated a third — with
//! every recorded number silent about which had been in force. Two components
//! cannot disagree if only one of them is allowed to speak.
//!
//! Having applied the caps, this module **reads them back out of the running
//! containers' cgroups and asserts they match**. A mismatch fails the run. The
//! previous harness warned and carried on, which is how the disagreement above
//! survived long enough to reach published records.
//!
//! Infrastructure is **recreated by default**, not reused. The framework
//! repository's equivalent reuses a healthy container so repeated runs are
//! cheap, which is right for a development rig and wrong here: silently reusing
//! a warm ClickHouse of the wrong version would be a published-number defect,
//! not an inconvenience. Reuse is opt-in and sets a flag on every record it
//! produces.

use std::time::{Duration, Instant};

use crate::docker::{self, NETWORK};
use crate::environment::Environment;
use crate::report::{Flag, Infra};

/// Where the running infrastructure can be reached, from the host and from
/// inside the bench network.
#[derive(Debug, Clone)]
pub struct Endpoints {
    /// Host-side Kafka bootstrap.
    pub bootstrap: String,
    /// Bootstrap as a container on [`NETWORK`] must dial it.
    ///
    /// What the ceiling pass's consumer is given, and never
    /// [`Endpoints::bootstrap`]. The published port is the thing the consume pass
    /// stopped using: measured on this host, the same corpus, broker, partition
    /// count and window served 72,349 messages a second through it and 1,719,373
    /// container-to-container.
    pub bootstrap_internal: String,
    /// The broker container's name, which is also the hostname a container on
    /// [`NETWORK`] reaches it by.
    ///
    /// Carried beside [`Endpoints::bootstrap_internal`] rather than parsed back
    /// out of it, for the same reason [`Endpoints::ch_container`] is carried
    /// beside `ch_internal`: it is the container whose cgroup the consume pass
    /// reads, via [`cgroup_cpu`], to say whether the broker was at its cap while
    /// it was being measured — which is what tells a ceiling from a floor.
    pub broker_container: String,
    /// Host-side registry.
    pub registry_host: String,
    /// Host-side registry port.
    pub registry_port: u16,
    /// Registry URL for a container on [`NETWORK`].
    pub registry_internal: String,
    /// Host-side ClickHouse.
    pub ch_host: String,
    /// Host-side ClickHouse HTTP port.
    pub ch_port: u16,
    /// ClickHouse user.
    pub ch_user: String,
    /// ClickHouse password.
    pub ch_password: String,
    /// ClickHouse URL for a container on [`NETWORK`].
    pub ch_internal: String,
    /// The ClickHouse container's name, which is also the hostname a container
    /// on [`NETWORK`] reaches it by.
    ///
    /// Carried beside [`Endpoints::ch_internal`] rather than parsed back out of
    /// it. A client that opens its own socket needs a host and a port, not a
    /// URL, and the ceiling pass's inserter is exactly that client: it runs in a
    /// container on [`NETWORK`] so that its POSTs cross the same boundary an
    /// arm's inserts cross, which is none. It is also the container whose cgroup
    /// that pass reads, via [`cgroup_cpu`], to say whether the target was at its
    /// cap while it was being measured.
    pub ch_container: String,
    /// ClickHouse's HTTP port as seen from inside [`NETWORK`].
    ///
    /// Not the published one. The published port is the thing the ingest pass
    /// stopped using: measured on this host, the same statement at the same
    /// concurrency ran roughly ten times faster container-to-container than
    /// through it.
    pub ch_internal_port: u16,
}

const BROKER: &str = "spate-bench-redpanda";
const CLICKHOUSE: &str = "spate-bench-clickhouse";
/// Where ClickHouse keeps the data an arm's inserts land in.
const CLICKHOUSE_DATA: &str = "/var/lib/clickhouse";
/// Where Redpanda keeps the segments the prefilled corpus is read out of.
const BROKER_DATA: &str = "/var/lib/redpanda/data";
/// ClickHouse's HTTP port inside the container. Published as 18123 on the host,
/// and reached unpublished at this number from anywhere on [`NETWORK`].
const CLICKHOUSE_HTTP_PORT: u16 = 8123;

/// Brings up the infrastructure this environment declares.
///
/// # Errors
///
/// If a container does not become reachable, or if the caps read back from a
/// running container disagree with the profile.
///
/// # Panics
///
/// If the docker CLI itself fails.
pub fn bring_up(env: &Environment, reuse: bool) -> Result<(Endpoints, Infra, Vec<Flag>), String> {
    let mut flags = Vec::new();
    assert_storage(&env.spec.infra.storage)?;
    docker::ensure_network();

    let b = &env.spec.infra.broker;
    let c = &env.spec.infra.clickhouse;

    // Reuse is decided **per container**, not for the pair.
    //
    // It used to ask one question — are both running? — and recreate both when the
    // answer was no. Changing ClickHouse's caps means removing the ClickHouse
    // container, because `assert_cap` below correctly refuses to run against a
    // container whose applied caps disagree with the profile; so under the old
    // shape the only way to move ClickHouse's cap also recreated the broker and
    // **destroyed the prefilled corpus inside it**. Six gigabytes and several
    // minutes, lost to a command that had nothing to do with the broker, and lost
    // precisely during an envelope search — the one job whose whole method is to
    // move one container's caps at a time.
    //
    // Splitting the question weakens nothing. Both containers' caps are read back
    // out of their cgroups and asserted below whether they were reused or freshly
    // started, so a container left running under a stale envelope still fails the
    // run rather than quietly producing a number. What changes is only which
    // containers a stale one takes down with it: itself, rather than its
    // neighbour's data.
    let reused_broker = reuse && running(BROKER);
    let reused_clickhouse = reuse && running(CLICKHOUSE);
    if reused_broker {
        docker::attach_to_network(BROKER);
    } else {
        start_broker(b, &env.spec.infra.storage)?;
    }
    if reused_clickhouse {
        docker::attach_to_network(CLICKHOUSE);
    } else {
        start_clickhouse(c, &env.spec.infra.storage)?;
    }
    if reused_broker || reused_clickhouse {
        flags.push(Flag::ReusedInfra);
        eprintln!(
            "reusing the running {}. Caps are still read back and asserted below, so a \
             container started under a different envelope will fail the run rather than \
             quietly produce a number.",
            reused_description(reused_broker, reused_clickhouse)
        );
    }

    let endpoints = Endpoints {
        bootstrap: "localhost:9092".to_owned(),
        bootstrap_internal: format!("{BROKER}:29092"),
        broker_container: BROKER.to_owned(),
        registry_host: "localhost".to_owned(),
        registry_port: 18081,
        registry_internal: format!("http://{BROKER}:8081"),
        ch_host: "localhost".to_owned(),
        ch_port: 18123,
        ch_user: "default".to_owned(),
        ch_password: "bench".to_owned(),
        ch_internal: format!("http://{CLICKHOUSE}:{CLICKHOUSE_HTTP_PORT}"),
        ch_container: CLICKHOUSE.to_owned(),
        ch_internal_port: CLICKHOUSE_HTTP_PORT,
    };

    wait_for_registry(&endpoints)?;

    // Read back and assert. Declared-versus-applied is checked here, once, for
    // both containers — the only place in the harness that gets to decide the
    // infrastructure was what the profile said it was.
    let (broker_cpus, broker_memory) = cgroup_caps(BROKER)?;
    assert_cap(BROKER, "cpus", &b.cpus, &broker_cpus)?;
    assert_cap(BROKER, "memory", &b.memory, &broker_memory)?;

    let (ch_cpus, ch_memory) = cgroup_caps(CLICKHOUSE)?;
    assert_cap(CLICKHOUSE, "cpus", &c.cpus, &ch_cpus)?;
    assert_cap(CLICKHOUSE, "memory", &c.memory, &ch_memory)?;

    // Both consume ceilings, and every reason one of them was refused.
    //
    // The refusals are printed rather than discarded because **a refused ceiling
    // is the mechanism working**. `crate::ceiling` drops a figure that does not
    // describe this corpus or this envelope instead of scaling it, and the
    // reason it dropped it is the only thing that tells an operator what to do
    // about it — "re-measure with `bench ceiling --measure`" is actionable,
    // whereas a flag on a record somebody reads a week later is not.
    //
    // What this replaced was `env.ceiling().map_or(0, |c| c.consume_msgs_per_s)`,
    // which threw `Ceiling::refusals` away entirely. That made "no ceiling has
    // ever been measured for this environment" and "the measured ceiling
    // describes a message this corpus no longer produces" produce identical
    // output: a zero, and a flag that says neither.
    let (ceiling_msgs_per_s, ceiling_bytes_per_s) = match env.ceiling() {
        Ok(c) => {
            for why in c.refusals() {
                eprintln!("REFUSED ceiling: {why}");
            }
            (c.consume_msgs_per_s, c.consume_bytes_per_s)
        }
        // A ceilings file that does not parse is a different problem from a
        // stale measurement in one that does, and it is reported as one rather
        // than failing the bring-up: `bench prefill` needs no ceiling at all,
        // and `driver::run` refuses the sweep over it before a container starts.
        Err(why) => {
            eprintln!("REFUSED ceiling: {why}");
            (0, 0)
        }
    };
    if ceiling_msgs_per_s == 0 {
        flags.push(Flag::HeadroomUnproven);
    }

    let infra = Infra {
        digest: env.infra_digest(),
        broker: b.kind.clone(),
        broker_version: broker_version(),
        broker_image_digest: image_digest(BROKER),
        broker_cpus,
        broker_memory,
        clickhouse_version: clickhouse_version(&endpoints),
        clickhouse_image_digest: image_digest(CLICKHOUSE),
        clickhouse_cpus: ch_cpus,
        clickhouse_memory: ch_memory,
        partitions: env.spec.infra.partitions,
        storage: env.spec.infra.storage.kind.as_str().to_owned(),
        registry: b.registry.clone(),
        ceiling_msgs_per_s,
        ceiling_bytes_per_s,
        // Left at zero here, and that is the honest value at this point rather
        // than a placeholder: the ClickHouse ceiling is measured per insert
        // format, and no arm has been chosen yet. `driver::measure`
        // fills it in on each record, where the arm's declared `wire_format` is
        // known. See `report::Infra::ceiling_rows_per_s`.
        ceiling_rows_per_s: 0,
    };

    Ok((endpoints, infra, flags))
}

/// Fails the run unless each declared data path is its own mount, distinct from
/// the root filesystem and from the other.
///
/// Read on the host. Inside a container a bind mount reports a different device
/// from the overlay root whether or not the host path is its own device, so the
/// in-container reading cannot tell a mounted NVMe device from an ordinary
/// directory — which is what Docker creates when the source is missing.
///
/// An unreadable answer is a refusal.
fn assert_storage(storage: &crate::environment::Storage) -> Result<(), String> {
    if storage.kind != crate::environment::Kind::LocalNvme {
        return Ok(());
    }
    let source = |path: &str| -> Result<String, String> {
        let out = std::process::Command::new("findmnt")
            .args(["-no", "SOURCE", "--target", path])
            .output()
            .map_err(|e| {
                format!(
                    "[infra.storage] declares kind = \"local-nvme\", which this harness \
                     verifies with findmnt(8), and it could not be run ({e}). Either the \
                     host is not the one the profile describes, or the profile should \
                     declare kind = \"shared-root\"."
                )
            })?;
        if !out.status.success() {
            return Err(format!("findmnt could not resolve {path}"));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
    };

    let root = source("/")?;
    let ch = source(&storage.clickhouse_data)?;
    let broker = source(&storage.broker_data)?;

    let refuse = |what: &str| {
        Err(format!(
            "REFUSED: {what}\n  /                      -> {root}\n  {} -> {ch}\n  {} -> {broker}\n\
             The profile declares kind = \"local-nvme\" and infra_digest records that, so \
             every number from this run would be described as measured on separate local \
             devices. Check that the box formatted and mounted its instance store before \
             Docker started, or declare kind = \"shared-root\".",
            storage.clickhouse_data, storage.broker_data
        ))
    };
    if ch == root {
        return refuse("ClickHouse's data path is on the root filesystem.");
    }
    if broker == root {
        return refuse("the broker's data path is on the root filesystem.");
    }
    if ch == broker {
        return refuse("ClickHouse and the broker are on the SAME device.");
    }

    eprintln!("storage: clickhouse {ch}, broker {broker}, root {root}");
    Ok(())
}

/// The `-v host:container` argument for a measured data path. `None` under
/// `shared-root`, where the container writes to its own layer.
fn data_volume(
    storage: &crate::environment::Storage,
    host_path: &str,
    container_path: &str,
) -> Option<String> {
    if storage.kind != crate::environment::Kind::LocalNvme {
        return None;
    }
    Some(format!("{host_path}:{container_path}"))
}

fn running(name: &str) -> bool {
    docker::docker_try(&["inspect", "-f", "{{.State.Running}}", name]).is_ok_and(|s| s == "true")
}

/// Which containers were reused, for the line an operator reads.
///
/// Named rather than counted, because "reusing the running infrastructure" and
/// "reusing the running broker while ClickHouse was recreated under new caps" are
/// different states and the second is what an envelope search spends its whole
/// time in.
fn reused_description(broker: bool, clickhouse: bool) -> &'static str {
    match (broker, clickhouse) {
        (true, true) => "infrastructure",
        (true, false) => "broker, with ClickHouse recreated",
        (false, true) => "ClickHouse, with the broker recreated",
        // Not reached: the caller only asks when at least one was reused.
        (false, false) => "infrastructure",
    }
}

fn start_broker(
    b: &crate::environment::Broker,
    storage: &crate::environment::Storage,
) -> Result<(), String> {
    let _ = docker::docker_try(&["rm", "-f", BROKER]);
    eprintln!("starting {BROKER} ({}, --cpus={}) ...", b.image, b.cpus);

    let cpus = format!("--cpus={}", b.cpus);
    let mem = format!("--memory={}", b.memory);
    let swap = format!("--memory-swap={}", b.memory);
    // Redpanda's own memory budget, separate from the cgroup limit: left below
    // the container cap so the process reserves less than the kernel allows and
    // an overshoot surfaces as Redpanda backpressure rather than an OOM kill.
    let rp_memory = "4G";
    let advertise = format!("EXTERNAL://localhost:9092,INTERNAL://{BROKER}:29092");
    let smp = b.cpus.clone();
    let volume = data_volume(storage, &storage.broker_data, BROKER_DATA);

    let mut args: Vec<&str> = vec![
        "run",
        "-d",
        "--name",
        BROKER,
        "--network",
        NETWORK,
        "-p",
        "9092:9092",
        // Redpanda's built-in, Confluent-compatible Schema Registry, published
        // for the host-side prefill. Using this rather than a separate Confluent
        // container keeps a second JVM out of the measurement environment and
        // returns a CPU and a GiB to the infra budget.
        "-p",
        "18081:8081",
        &cpus,
        &mem,
        &swap,
    ];
    if let Some(v) = volume.as_deref() {
        args.extend_from_slice(&["-v", v]);
    }
    args.extend_from_slice(&[
        &b.image,
        "redpanda",
        "start",
        "--node-id",
        "0",
        "--check=false",
        // Load-bearing rather than cosmetic: by default Redpanda busy-polls one
        // core per shard, which on a co-located host burns CPU the system under
        // test needs and turns the measurement into a scheduling contest.
        "--overprovisioned",
        "--kafka-addr",
        "EXTERNAL://0.0.0.0:9092,INTERNAL://0.0.0.0:29092",
        "--advertise-kafka-addr",
        &advertise,
        "--smp",
        &smp,
        "--memory",
        rp_memory,
        "--reserve-memory",
        "0M",
    ]);
    docker::docker(&args);

    let deadline = Instant::now() + Duration::from_secs(90);
    while !tcp_open("localhost", 9092) {
        if Instant::now() >= deadline {
            return Err(format!("{BROKER} did not become reachable"));
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    // The port opens before the broker is ready to serve metadata.
    std::thread::sleep(Duration::from_secs(2));
    Ok(())
}

fn start_clickhouse(
    c: &crate::environment::ClickHouse,
    storage: &crate::environment::Storage,
) -> Result<(), String> {
    let _ = docker::docker_try(&["rm", "-f", CLICKHOUSE]);
    eprintln!("starting {CLICKHOUSE} ({}, --cpus={}) ...", c.image, c.cpus);

    let cpus = format!("--cpus={}", c.cpus);
    let mem = format!("--memory={}", c.memory);
    let swap = format!("--memory-swap={}", c.memory);
    let volume = data_volume(storage, &storage.clickhouse_data, CLICKHOUSE_DATA);
    let mut args: Vec<&str> = vec![
        "run",
        "-d",
        "--name",
        CLICKHOUSE,
        "--network",
        NETWORK,
        "-p",
        "18123:8123",
        "-p",
        "19000:9000",
        "-e",
        "CLICKHOUSE_PASSWORD=bench",
        "--ulimit",
        "nofile=262144:262144",
        &cpus,
        &mem,
        &swap,
    ];
    if let Some(v) = volume.as_deref() {
        args.extend_from_slice(&["-v", v]);
    }
    args.push(&c.image);
    docker::docker(&args);

    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        if crate::http::get("localhost", 18123, "/ping").is_ok_and(|b| b.contains("Ok")) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!("{CLICKHOUSE} did not become reachable"));
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn wait_for_registry(e: &Endpoints) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        // `/subjects` on an empty registry returns `[]`, which is a positive
        // answer — probe for a well-formed response, not for non-emptiness.
        if crate::http::get(&e.registry_host, e.registry_port, "/subjects")
            .is_ok_and(|b| b.contains('['))
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("the schema registry did not answer".to_owned());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// The cgroup v2 caps a container is actually running under, as
/// `(cpu.max, memory.max)` verbatim.
///
/// Read from inside the container rather than from `docker inspect`: inspect
/// reports what Docker was *asked* for, and the question here is what the kernel
/// is *enforcing*. Infrastructure images have a shell, so `docker exec` is
/// available — the systems under test may not, which is why their sampling goes
/// through a sidecar instead.
fn cgroup_caps(container: &str) -> Result<(String, String), String> {
    let cpu = docker::docker_try(&["exec", container, "cat", "/sys/fs/cgroup/cpu.max"])
        .map_err(|e| format!("read cpu.max from {container}: {e}"))?;
    let mem = docker::docker_try(&["exec", container, "cat", "/sys/fs/cgroup/memory.max"])
        .map_err(|e| format!("read memory.max from {container}: {e}"))?;
    Ok((cpu.trim().to_owned(), mem.trim().to_owned()))
}

/// What a container's cgroup says it has spent, read at one instant.
///
/// The counters `/sys/fs/cgroup/cpu.stat` carries, unaltered. They are
/// cumulative, so a single reading says nothing and a *pair* of readings across
/// a known interval says how many cores the container occupied over it — which
/// is the one question a measurement pass has to be able to answer about the
/// system it is measuring: was the target at its cap, or was something else the
/// constraint?
///
/// Read here rather than through [`crate::sampler::Sampler`] because the ceiling
/// pass needs the interval to be **exactly** one rung of its concurrency sweep.
/// A sidecar samples on its own timer and starts when its container boots, so
/// the seconds before the rung began would land inside the window and drag the
/// mean down; two `docker exec`s taken either side of the rung cannot. The
/// sidecar remains right for an arm, whose window is the sampler's own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuStat {
    /// Cumulative CPU time charged to the cgroup, microseconds.
    pub usage_us: u64,
    /// User-mode share of it.
    pub user_us: u64,
    /// Kernel-mode share of it.
    pub system_us: u64,
    /// CFS periods the cgroup has been scheduled in.
    pub nr_periods: u64,
    /// Periods in which it was throttled by its own cap.
    pub nr_throttled: u64,
    /// Cumulative time spent throttled, microseconds.
    pub throttled_us: u64,
}

impl CpuStat {
    /// Mean cores occupied between two readings taken `wall_s` apart.
    ///
    /// Directly comparable to the container's `--cpus` cap, so a figure at the
    /// cap says the target was CPU-bound without any inference. A negative or
    /// zero interval yields zero rather than an absurdity: the counters are
    /// monotonic, so the only way to get one is a clock or a caller that read
    /// them out of order, and neither is evidence of a busy server.
    #[must_use]
    pub fn cores_between(before: Self, after: Self, wall_s: f64) -> f64 {
        if wall_s <= 0.0 {
            return 0.0;
        }
        after.usage_us.saturating_sub(before.usage_us) as f64 / (wall_s * 1e6)
    }

    /// The delta between two readings, field by field.
    #[must_use]
    pub fn since(self, before: Self) -> Self {
        Self {
            usage_us: self.usage_us.saturating_sub(before.usage_us),
            user_us: self.user_us.saturating_sub(before.user_us),
            system_us: self.system_us.saturating_sub(before.system_us),
            nr_periods: self.nr_periods.saturating_sub(before.nr_periods),
            nr_throttled: self.nr_throttled.saturating_sub(before.nr_throttled),
            throttled_us: self.throttled_us.saturating_sub(before.throttled_us),
        }
    }
}

/// A container's cgroup CPU counters right now, or `None` if they cannot be
/// read.
///
/// `None` rather than an error, and deliberately: this is evidence *about* a
/// measurement, not the measurement itself, and a pass that failed because a
/// diagnostic was unavailable would trade a number for a footnote.
#[must_use]
pub fn cgroup_cpu(container: &str) -> Option<CpuStat> {
    let raw = docker::docker_try(&["exec", container, "cat", "/sys/fs/cgroup/cpu.stat"]).ok()?;
    parse_cpu_stat(&raw)
}

/// Parses `cpu.stat`'s `key value` lines. Absent keys read as zero, because a
/// kernel that reports no throttling omits the throttling keys entirely.
fn parse_cpu_stat(raw: &str) -> Option<CpuStat> {
    let field = |key: &str| -> Option<u64> {
        raw.lines()
            .find_map(|l| l.strip_prefix(key)?.trim().parse().ok())
    };
    Some(CpuStat {
        usage_us: field("usage_usec ")?,
        user_us: field("user_usec ").unwrap_or(0),
        system_us: field("system_usec ").unwrap_or(0),
        nr_periods: field("nr_periods ").unwrap_or(0),
        nr_throttled: field("nr_throttled ").unwrap_or(0),
        throttled_us: field("throttled_usec ").unwrap_or(0),
    })
}

/// Fails the run when the applied cap disagrees with the declared one.
///
/// Memory goes through `entrant::parse_memory` rather than through a copy kept
/// here. There were four copies of that parser and they had drifted, which is
/// how a suffix this one accepted became a suffix the arm's own cap assertion
/// refused.
fn assert_cap(container: &str, what: &str, declared: &str, applied: &str) -> Result<(), String> {
    let ok = match what {
        "cpus" => cpu_max_cores(applied)
            .is_some_and(|c| declared.parse::<f64>().is_ok_and(|d| (c - d).abs() < 0.01)),
        _ => crate::entrant::parse_memory(declared)
            .is_some_and(|d| applied.parse::<u64>().is_ok_and(|a| a == d)),
    };
    if ok {
        return Ok(());
    }
    Err(format!(
        "REFUSED: {container} declares {what}={declared} but is running under \
         {what}={applied}. The envelope in the environment profile is what every \
         published number is described by, so a container running under a \
         different one cannot produce a publishable result. Recreate the \
         infrastructure (drop --reuse-infra) or correct the profile."
    ))
}

/// Cores from a cgroup v2 `cpu.max` line (`"<quota> <period>"`, or `"max …"`).
fn cpu_max_cores(cpu_max: &str) -> Option<f64> {
    let mut it = cpu_max.split_whitespace();
    let quota = it.next()?;
    let period: f64 = it.next()?.parse().ok()?;
    if quota == "max" {
        return None;
    }
    Some(quota.parse::<f64>().ok()? / period)
}

/// The image a container is running, by content id. A tag can be re-pushed; an
/// id cannot.
fn image_digest(container: &str) -> String {
    docker::docker_try(&["inspect", "-f", "{{.Image}}", container]).unwrap_or_default()
}

fn broker_version() -> String {
    docker::docker_try(&["exec", BROKER, "rpk", "version"])
        .map(|s| {
            s.lines()
                .find_map(|l| l.split_whitespace().last().map(str::to_owned))
                .unwrap_or_else(|| s.trim().to_owned())
        })
        .unwrap_or_else(|_| "unknown".to_owned())
}

fn clickhouse_version(e: &Endpoints) -> String {
    docker::clickhouse_sql(
        &e.ch_host,
        e.ch_port,
        &e.ch_user,
        &e.ch_password,
        "SELECT version()",
    )
    .map(|v| v.trim().to_owned())
    .unwrap_or_else(|_| "unknown".to_owned())
}

fn tcp_open(host: &str, port: u16) -> bool {
    use std::net::ToSocketAddrs;
    (host, port)
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
        .is_some_and(|addr| {
            std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_ok()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_max_converts_quota_and_period_to_cores() {
        assert_eq!(cpu_max_cores("800000 100000"), Some(8.0));
        assert_eq!(cpu_max_cores("400000 100000"), Some(4.0));
        // An uncapped container cannot satisfy a declared cap, and returning
        // None is what makes the assertion fail rather than pass vacuously.
        assert_eq!(cpu_max_cores("max 100000"), None);
    }

    #[test]
    fn a_mismatched_cap_is_refused() {
        // The whole point of the module: declared 8, running 4, and the run
        // stops. The previous harness printed a warning here and carried on.
        let e = assert_cap("c", "cpus", "8", "400000 100000").expect_err("must refuse");
        assert!(e.starts_with("REFUSED"), "{e}");
        assert!(assert_cap("c", "cpus", "8", "800000 100000").is_ok());
    }

    /// The counters the ingest pass reads either side of every rung, so it can
    /// say whether the target was at its cap while it was being measured.
    #[test]
    fn a_cgroup_cpu_stat_parses_into_its_counters_and_tolerates_absent_throttling_keys() {
        let full = "usage_usec 129007508\nuser_usec 129003510\nsystem_usec 3998\n\
                    nr_periods 660\nnr_throttled 12\nthrottled_usec 602643\n";
        let s = parse_cpu_stat(full).expect("cpu.stat parses");
        assert_eq!(s.usage_us, 129_007_508);
        assert_eq!(s.user_us, 129_003_510);
        assert_eq!(s.nr_throttled, 12);
        assert_eq!(s.throttled_us, 602_643);

        // A kernel with nothing to report omits the throttling keys, which is
        // not the same as failing to read them.
        let quiet = parse_cpu_stat("usage_usec 5\nuser_usec 4\nsystem_usec 1\n")
            .expect("a cpu.stat without throttling keys still parses");
        assert_eq!(quiet.nr_throttled, 0);
        assert_eq!(quiet.throttled_us, 0);

        // No usage at all is not a reading of zero usage.
        assert!(parse_cpu_stat("nr_periods 4\n").is_none());
    }

    /// Mean cores over the rung's own interval, which is the whole point of
    /// taking the two readings by hand instead of from a sidecar's timer.
    #[test]
    fn mean_cores_are_the_cpu_delta_over_the_interval_the_rung_actually_ran_for() {
        let zero = CpuStat {
            usage_us: 0,
            user_us: 0,
            system_us: 0,
            nr_periods: 0,
            nr_throttled: 0,
            throttled_us: 0,
        };
        // Four CPU-seconds over one wall second is four cores.
        let after = CpuStat {
            usage_us: 4_000_000,
            user_us: 3_000_000,
            system_us: 1_000_000,
            nr_periods: 10,
            nr_throttled: 3,
            throttled_us: 500,
        };
        assert!((CpuStat::cores_between(zero, after, 1.0) - 4.0).abs() < 1e-9);
        assert!((CpuStat::cores_between(zero, after, 8.0) - 0.5).abs() < 1e-9);
        // A window of no length is not a busy server.
        assert!((CpuStat::cores_between(zero, after, 0.0)).abs() < f64::EPSILON);
        assert_eq!(after.since(zero), after);
        assert_eq!(after.since(after).usage_us, 0);
    }

    /// The line an operator reads has to distinguish the state an envelope search
    /// lives in — broker reused, ClickHouse recreated under new caps — from a
    /// wholesale reuse, because under the previous shape that state did not exist:
    /// recreating ClickHouse recreated the broker and destroyed the corpus.
    #[test]
    fn reusing_one_container_says_which_one_rather_than_claiming_the_whole_infrastructure() {
        assert_eq!(reused_description(true, true), "infrastructure");
        assert_eq!(
            reused_description(true, false),
            "broker, with ClickHouse recreated"
        );
        assert_eq!(
            reused_description(false, true),
            "ClickHouse, with the broker recreated"
        );
    }

    #[test]
    fn a_shared_root_profile_mounts_nothing_and_local_nvme_mounts_both() {
        use crate::environment::{Kind, Storage};

        let shared = Storage::default();
        assert_eq!(data_volume(&shared, "", CLICKHOUSE_DATA), None);

        let nvme = Storage {
            kind: Kind::LocalNvme,
            clickhouse_data: "/mnt/bench-clickhouse".to_owned(),
            broker_data: "/mnt/bench-broker".to_owned(),
        };
        assert_eq!(
            data_volume(&nvme, &nvme.clickhouse_data, CLICKHOUSE_DATA).as_deref(),
            Some("/mnt/bench-clickhouse:/var/lib/clickhouse")
        );
        assert_eq!(
            data_volume(&nvme, &nvme.broker_data, BROKER_DATA).as_deref(),
            Some("/mnt/bench-broker:/var/lib/redpanda/data")
        );
    }

    #[test]
    fn only_a_declared_nvme_layout_is_verified() {
        use crate::environment::{Kind, Storage};

        assert!(assert_storage(&Storage::default()).is_ok());

        // `/` resolves to the root source on any host, so this fails wherever
        // the test runs.
        let bogus = Storage {
            kind: Kind::LocalNvme,
            clickhouse_data: "/".to_owned(),
            broker_data: "/".to_owned(),
        };
        let e = assert_storage(&bogus).expect_err("must refuse");
        assert!(e.starts_with("REFUSED") || e.contains("findmnt"), "{e}");
    }

    #[test]
    fn memory_caps_compare_in_bytes() {
        assert!(assert_cap("c", "memory", "8g", "8589934592").is_ok());
        assert!(assert_cap("c", "memory", "8g", "8589934591").is_err());
        // "max" is not a number, so an uncapped container fails the assertion.
        assert!(assert_cap("c", "memory", "8g", "max").is_err());
        // Every suffix the shared parser accepts has to reach this comparison,
        // which is what the four drifted copies could not guarantee.
        assert!(assert_cap("c", "memory", "8388608k", "8589934592").is_ok());
    }
}
