#!/usr/bin/env python3
"""Host-side diagnostics for a tuning run.

Runs on the box beside `bench run --trigger tuning` and writes one directory of
evidence for locating whatever bounds an arm's drain. Its ClickHouse polls add
to the server-wide network counters the harness records as
`ch_network_receive_bytes`, so run-bench.sh starts it only for a tuning run,
whose records are never published.

  events.jsonl     container starts and stops, so each repetition's window can be
                   cut out of every other stream by timestamp.
  cgroup.jsonl     every bench container's cgroup v2 counters, each second: CPU,
                   throttling, pressure, memory and per-device I/O.
  threads.jsonl    per-thread CPU and run-queue wait from /proc/<tid>/schedstat,
                   grouped by thread name, each second. Redpanda runs one reactor
                   thread per shard (shard 0 on the main thread, `redpanda`) and
                   librdkafka a thread per broker connection, so a single
                   saturated thread shows here while its cgroup does not.
  host.jsonl       per-CPU /proc/stat, softirqs, NVMe diskstats, interface
                   counters and host pressure, each second. Bridge networking is
                   softirq work charged to no container.
  clickhouse.jsonl landed rows and a few server metrics and events, each second.
  redpanda.txt.gz  Redpanda's /metrics and /public_metrics, whole, every five
                   seconds, so per-shard reactor, I/O queue, batch cache and
                   fetch series need no name guessed in advance.
  admin.jsonl      Redpanda's partition-to-shard placement and fetch
                   configuration, read when the collector first sees a broker
                   (on the box, the one prefill left running) and shortly after
                   each arm starts. Placement comes from the shard label on the
                   per-partition metrics: `replicas[].core` from
                   /v1/partitions/kafka/<topic> reads 0 for every partition on a
                   broker whose metrics spread them over all shards.
  logs/            every arm container's stdout and stderr, streamed with
                   timestamps. The harness discards them when a repetition
                   succeeds.

Every stream writes absolute counters except threads.jsonl, which writes deltas
because thread ids come and go between samples.

Usage: diag.py <out-dir>. Runs until SIGTERM or SIGINT.
"""

import base64
import gzip
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
import urllib.request

OUT = sys.argv[1]
STOP = threading.Event()

BROKER = "spate-bench-redpanda"
CLICKHOUSE = "spate-bench-clickhouse"
INFRA = (BROKER, CLICKHOUSE)
CH_URL = "http://localhost:18123/"
CH_AUTH = "Basic " + base64.b64encode(b"default:bench").decode()
TOPIC = "comparison-sensor-batches"

CH_METRICS = (
    "Query", "QueryNonInternal", "HTTPConnection", "TCPConnection",
    "BackgroundMergesAndMutationsPoolTask", "PartsActive", "DelayedInserts",
)
CH_EVENTS = (
    "InsertedRows", "InsertedBytes", "InsertQuery", "DelayedInserts",
    "RejectedInserts", "InsertQueryTimeMicroseconds", "NetworkReceiveBytes",
    "NetworkReceiveElapsedMicroseconds", "OSCPUWaitMicroseconds",
)

# Containers known to the watcher: name -> {"id", "cg", "ip"}. Readers take a
# snapshot under the lock and never hold it while sampling.
LOCK = threading.Lock()
CONTAINERS = {}


def now_ms():
    return time.time_ns() // 1_000_000


class Jsonl:
    """Append-only JSONL writer, flushed per line so a killed box keeps what it wrote."""

    def __init__(self, name):
        self.fh = open(os.path.join(OUT, name), "a", buffering=1)
        self.lock = threading.Lock()

    def write(self, obj):
        line = json.dumps(obj, separators=(",", ":"))
        with self.lock:
            self.fh.write(line + "\n")


def read(path):
    try:
        with open(path) as fh:
            return fh.read()
    except OSError:
        return None


def keyed(text, wanted=None):
    out = {}
    for line in (text or "").splitlines():
        parts = line.split()
        if len(parts) >= 2 and (wanted is None or parts[0] in wanted):
            try:
                out[parts[0]] = int(parts[1])
            except ValueError:
                pass
    return out


def pressure(text):
    """`some avg10=.. total=N` lines to {"some": N, "full": N} microseconds."""
    out = {}
    for line in (text or "").splitlines():
        parts = line.split()
        for p in parts[1:]:
            if p.startswith("total="):
                out[parts[0]] = int(p[6:])
    return out


def docker(*args, timeout=10):
    r = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        print(f"diag: docker {args[0]} exited {r.returncode}: {r.stderr.strip()[:300]}", file=sys.stderr, flush=True)
        return None
    return r.stdout.strip()


def cgroup_dir(cid):
    for p in (f"/sys/fs/cgroup/system.slice/docker-{cid}.scope", f"/sys/fs/cgroup/docker/{cid}"):
        if os.path.isdir(p):
            return p
    return None


def http_get(url, timeout=4.0):
    """GET `url`, giving up once `timeout` seconds have passed in total.

    urllib's timeout bounds each socket operation, so a server that trickles a
    body would otherwise hold the call open indefinitely.
    """
    deadline = time.monotonic() + timeout
    chunks = []
    with urllib.request.urlopen(url, timeout=timeout) as r:
        while chunk := r.read(65536):
            chunks.append(chunk)
            if time.monotonic() > deadline or STOP.is_set():
                raise TimeoutError(f"{url}: body not complete within {timeout}s")
    return b"".join(chunks).decode("utf-8", "replace")


def every(seconds, fn):
    """Run `fn` on a fixed cadence until STOP, logging an exception rather than dying."""

    def loop():
        next_t = time.monotonic()
        while not STOP.is_set():
            try:
                fn()
            except Exception as e:  # noqa: BLE001 - a diagnostic stream must outlive one bad read
                print(f"diag: {fn.__name__}: {e!r}", file=sys.stderr, flush=True)
            # A tick that overran its slot reschedules from now, so a stall is
            # not followed by a burst of the ticks it missed.
            next_t = max(next_t + seconds, time.monotonic())
            STOP.wait(max(0.0, next_t - time.monotonic()))

    t = threading.Thread(target=loop, name=fn.__name__, daemon=True)
    t.start()
    return t


# --- containers -----------------------------------------------------------

EVENTS = None
ADMIN = None
LOG_PROCS = []
TIMERS = []


def later(delay, fn, *args):
    """Run `fn` after `delay` seconds on a daemon timer that shutdown cancels."""
    t = threading.Timer(delay, fn, args)
    t.daemon = True
    TIMERS.append(t)
    t.start()


def admin_snapshot(reason):
    with LOCK:
        broker = CONTAINERS.get(BROKER)
    if not broker or not broker["ip"]:
        return
    base = f"http://{broker['ip']}:9644"
    snap = {"t": now_ms(), "reason": reason}
    for key, path in (
        ("partitions", f"/v1/partitions/kafka/{TOPIC}"),
        ("cluster_config", "/v1/cluster_config"),
    ):
        try:
            body = json.loads(http_get(base + path))
            if key == "cluster_config" and isinstance(body, dict):
                body = {k: v for k, v in body.items() if "fetch" in k or "cache" in k or "read" in k}
            snap[key] = body
        except Exception as e:  # noqa: BLE001
            snap[key] = {"error": repr(e)}
    try:
        placement = {}
        for line in http_get(base + "/metrics").splitlines():
            if line.startswith("vectorized_cluster_partition_records_produced{") and f'topic="{TOPIC}"' in line:
                labels = dict(re.findall(r'(\w+)="([^"]*)"', line[line.index("{"):line.index("}")]))
                placement[labels["partition"]] = int(labels["shard"])
        snap["shard_by_partition"] = placement
    except Exception as e:  # noqa: BLE001
        snap["shard_by_partition"] = {"error": repr(e)}
    ADMIN.write(snap)


def watch():
    listing = docker("ps", "--no-trunc", "--format", "{{.ID}}\t{{.Names}}")
    if listing is None:
        return
    seen = {}
    for line in listing.splitlines():
        cid, _, name = line.partition("\t")
        if name.startswith("spate-bench-"):
            seen[name] = cid

    with LOCK:
        known = dict(CONTAINERS)
    for name, cid in seen.items():
        if name in known and known[name]["id"] == cid:
            # A failed inspect or a cgroup not yet created is retried on the
            # next poll rather than left empty for the container's lifetime.
            if known[name]["ip"] is None or known[name]["cg"] is None:
                ip = docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", name)
                with LOCK:
                    if name in CONTAINERS:
                        CONTAINERS[name]["ip"] = (ip or "").split()[0] if ip else None
                        CONTAINERS[name]["cg"] = cgroup_dir(cid)
            continue
        if name in known:
            # Removed and recreated under the same name between two polls.
            EVENTS.write({"t": now_ms(), "event": "stop", "name": name, "id": known[name]["id"]})
        ip = docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}} {{end}}", name)
        info = {"id": cid, "cg": cgroup_dir(cid), "ip": (ip or "").split()[0] if ip else None}
        with LOCK:
            CONTAINERS[name] = info
        t = now_ms()
        EVENTS.write({"t": t, "event": "start", "name": name, "id": cid, "cg": info["cg"], "ip": info["ip"]})
        if name == BROKER:
            later(5, admin_snapshot, "broker_seen")
        elif name not in INFRA and "sampler" not in name:
            path = os.path.join(OUT, "logs", f"{name}.{t}.log")
            fh = open(path, "w")
            LOG_PROCS.append(subprocess.Popen(
                ["docker", "logs", "-f", "--timestamps", name],
                stdout=fh, stderr=subprocess.STDOUT,
            ))
            # The arm sets the broker's read concurrency as it starts, so the
            # read-back waits for that to have happened.
            later(15, admin_snapshot, f"arm_start:{name}")

    for name in known:
        if name not in seen:
            with LOCK:
                CONTAINERS.pop(name, None)
            EVENTS.write({"t": now_ms(), "event": "stop", "name": name, "id": known[name]["id"]})

    for p in list(LOG_PROCS):
        if p.poll() is not None:
            LOG_PROCS.remove(p)


# --- per-container cgroup -------------------------------------------------

CGROUP = None
MEM_KEYS = ("anon", "file", "shmem", "file_dirty", "file_writeback", "active_file", "inactive_file", "sock")


def io_stat(text):
    out = {}
    for line in (text or "").splitlines():
        parts = line.split()
        if parts:
            out[parts[0]] = {k: int(v) for k, v in (p.split("=", 1) for p in parts[1:] if "=" in p)}
    return out


def sample_cgroups():
    with LOCK:
        snap = {n: c["cg"] for n, c in CONTAINERS.items() if c["cg"]}
    for name, cg in snap.items():
        CGROUP.write({
            "t": now_ms(),
            "name": name,
            "cpu": keyed(read(f"{cg}/cpu.stat")),
            "cpu_max": (read(f"{cg}/cpu.max") or "").strip(),
            "pressure": {
                "cpu": pressure(read(f"{cg}/cpu.pressure")),
                "io": pressure(read(f"{cg}/io.pressure")),
                "memory": pressure(read(f"{cg}/memory.pressure")),
            },
            "mem": keyed(read(f"{cg}/memory.stat"), MEM_KEYS),
            "mem_current": int((read(f"{cg}/memory.current") or "-1").strip() or -1),
            "io": io_stat(read(f"{cg}/io.stat")),
        })


# --- per-thread CPU and run-queue wait ------------------------------------

THREADS = None
PREV_THREAD = {}


def thread_stat(tid):
    stat = read(f"/proc/{tid}/stat")
    sched = read(f"/proc/{tid}/schedstat")
    if not stat or not sched:
        return None
    # comm sits in parentheses and may itself contain spaces or parentheses.
    comm = stat[stat.index("(") + 1:stat.rindex(")")]
    rest = stat[stat.rindex(")") + 2:].split()
    run_ns, wait_ns, _ = (int(x) for x in sched.split())
    # Field 39 of /proc/<tid>/stat is the CPU the thread last ran on; `rest`
    # starts at field 3.
    return comm, run_ns, wait_ns, int(rest[36])


def sample_threads():
    with LOCK:
        snap = {n: c["cg"] for n, c in CONTAINERS.items() if c["cg"]}
    t = now_ms()
    live = set()
    for name, cg in snap.items():
        groups = {}
        for tid in (read(f"{cg}/cgroup.threads") or "").split():
            s = thread_stat(tid)
            if s is None:
                continue
            comm, run_ns, wait_ns, cpu = s
            key = (name, tid)
            live.add(key)
            prev = PREV_THREAD.get(key)
            PREV_THREAD[key] = (t, run_ns, wait_ns)
            if prev is None:
                continue
            d_run, d_wait = run_ns - prev[1], wait_ns - prev[2]
            g = groups.setdefault(comm, {"n": 0, "run_ns": 0, "max_run_ns": 0, "wait_ns": 0, "cpus": []})
            g["n"] += 1
            g["run_ns"] += d_run
            g["wait_ns"] += d_wait
            g["max_run_ns"] = max(g["max_run_ns"], d_run)
            if cpu not in g["cpus"]:
                g["cpus"].append(cpu)
        if groups:
            THREADS.write({"t": t, "name": name, "groups": groups})
    for key in list(PREV_THREAD):
        if key not in live:
            del PREV_THREAD[key]


# --- host -----------------------------------------------------------------

HOST = None


def sample_host():
    cpus = {}
    for line in (read("/proc/stat") or "").splitlines():
        if line.startswith("cpu"):
            parts = line.split()
            cpus[parts[0]] = [int(x) for x in parts[1:]]
    softirqs = {}
    for line in (read("/proc/softirqs") or "").splitlines()[1:]:
        parts = line.split()
        if parts and parts[0].rstrip(":") in ("NET_RX", "NET_TX", "BLOCK"):
            softirqs[parts[0].rstrip(":")] = [int(x) for x in parts[1:]]
    disks = {}
    for line in (read("/proc/diskstats") or "").splitlines():
        parts = line.split()
        if len(parts) > 3 and parts[2].startswith("nvme"):
            disks[parts[2]] = [int(x) for x in parts[3:]]
    nets = {}
    for line in (read("/proc/net/dev") or "").splitlines()[2:]:
        name, _, rest = line.partition(":")
        nets[name.strip()] = [int(x) for x in rest.split()]
    HOST.write({
        "t": now_ms(),
        "cpu": cpus,
        "softirqs": softirqs,
        "diskstats": disks,
        "net": nets,
        "pressure": {k: pressure(read(f"/proc/pressure/{k}")) for k in ("cpu", "io", "memory")},
        "meminfo": keyed((read("/proc/meminfo") or "").replace(":", ""), ("MemFree", "Cached", "Dirty", "Writeback")),
    })


# --- ClickHouse -----------------------------------------------------------

CLICKHOUSE_OUT = None


def ch_query(sql):
    req = urllib.request.Request(CH_URL, data=sql.encode(), headers={"Authorization": CH_AUTH})
    with urllib.request.urlopen(req, timeout=4.0) as r:
        return r.read().decode()


def sample_clickhouse():
    with LOCK:
        up = CLICKHOUSE in CONTAINERS
    if not up:
        return
    t = now_ms()
    out = {"t": t}
    try:
        out["rows"] = int(ch_query("SELECT count() FROM default.sensor_events").strip())
    except Exception as e:  # noqa: BLE001 - a failed count is recorded, the sample still written
        out["rows_error"] = repr(e)[:200]
    names = ", ".join(f"'{m}'" for m in CH_METRICS)
    events = ", ".join(f"'{m}'" for m in CH_EVENTS)
    try:
        body = ch_query(
            f"SELECT 'm', metric, toInt64(value) FROM system.metrics WHERE metric IN ({names}) "
            f"UNION ALL SELECT 'e', event, toInt64(value) FROM system.events WHERE event IN ({events}) "
            # Without it a counter still at zero has no row, which reads the
            # same as one that was never collected.
            "SETTINGS system_events_show_zero_values = 1 FORMAT TSV"
        )
        out["metrics"], out["events"] = {}, {}
        for line in body.splitlines():
            kind, name, value = line.split("\t")
            out["metrics" if kind == "m" else "events"][name] = int(value)
    except Exception as e:  # noqa: BLE001
        out["metrics_error"] = repr(e)[:200]
    CLICKHOUSE_OUT.write(out)


# --- Redpanda metrics -----------------------------------------------------

RP_PATH = None
RP_LOCK = threading.Lock()


def scrape_redpanda():
    with LOCK:
        broker = CONTAINERS.get(BROKER)
    if not broker or not broker["ip"]:
        return
    for path in ("/metrics", "/public_metrics"):
        t0 = now_ms()
        try:
            body = http_get(f"http://{broker['ip']}:9644{path}")
        except Exception as e:  # noqa: BLE001
            body = f"# error {e!r}\n"
        # One complete gzip member per scrape, so a box killed mid-run still
        # leaves a file every reader can decompress to its last scrape.
        with RP_LOCK, gzip.open(RP_PATH, "ab") as fh:
            fh.write(f"# scrape t={t0} took_ms={now_ms() - t0} path={path}\n".encode())
            fh.write(body.encode())


# --------------------------------------------------------------------------

def main():
    global EVENTS, ADMIN, CGROUP, THREADS, HOST, CLICKHOUSE_OUT, RP_PATH
    os.makedirs(os.path.join(OUT, "logs"), exist_ok=True)
    EVENTS = Jsonl("events.jsonl")
    ADMIN = Jsonl("admin.jsonl")
    CGROUP = Jsonl("cgroup.jsonl")
    THREADS = Jsonl("threads.jsonl")
    HOST = Jsonl("host.jsonl")
    CLICKHOUSE_OUT = Jsonl("clickhouse.jsonl")
    RP_PATH = os.path.join(OUT, "redpanda.txt.gz")

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: STOP.set())

    EVENTS.write({
        "t": now_ms(),
        "event": "collector_start",
        "clk_tck": os.sysconf("SC_CLK_TCK"),
        "mounts": subprocess.run(
            ["findmnt", "-rno", "TARGET,SOURCE"], capture_output=True, text=True,
        ).stdout.splitlines(),
        "lsblk": subprocess.run(["lsblk", "-rno", "NAME,MAJ:MIN,MOUNTPOINT"], capture_output=True, text=True).stdout,
    })

    threads = [
        every(1.0, watch),
        every(1.0, sample_cgroups),
        every(1.0, sample_threads),
        every(1.0, sample_host),
        every(1.0, sample_clickhouse),
        every(5.0, scrape_redpanda),
    ]
    STOP.wait()
    for t in TIMERS:
        t.cancel()
    for t in threads:
        t.join(timeout=5)
    for p in LOG_PROCS:
        p.terminate()
    EVENTS.write({"t": now_ms(), "event": "collector_stop"})


if __name__ == "__main__":
    main()
