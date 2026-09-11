#!/usr/bin/env python3
"""Bounded Flink tuning session. All runs use the normal harness and tuning quarantine.

No infrastructure profile or corpus changes. Run with --dry-run to inspect the
initial matrix without Docker, AWS, file writes, or a benchmark invocation.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import statistics
import tarfile
import subprocess
import threading
import time
import tomllib
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
DESCRIPTOR = ROOT / "entrants/flink/entrant.toml"
ENV_ID = "c8gd-metal-24xl-ec2-docker"
BENCH = ROOT / "target/release/bench"


def initial_cases(defaults):
    def case(name, **knobs):
        return name, dict(defaults, **knobs)
    return [
        case("baseline"),
        case("batch50k-one", inflight=1),
        case("batch128k", max_rows=131072, buffered_rows=262144, inflight=1, max_batch_bytes=67108864),
        case("batch256k", max_rows=262144, buffered_rows=524288, inflight=1, max_batch_bytes=67108864),
        case("batch256k-fill", max_rows=262144, buffered_rows=524288, inflight=1, max_batch_bytes=67108864, linger_ms=5000),
        case("small-buffer", max_rows=12500, buffered_rows=25000, inflight=1),
        case("heap34g", process_mib=34816),
        case("batch256k-heap64g", process_mib=65536, max_rows=262144, buffered_rows=524288,
             inflight=1, max_batch_bytes=67108864, linger_ms=5000),
        case("spate-rowbinary-settings", process_mib=65536, max_rows=262144, buffered_rows=524288,
             inflight=4, max_batch_bytes=67108864, linger_ms=500),
    ]


def descriptor_text(original, cases, taskmanagers=1):
    """Only replace the variant list and optionally split the existing TM envelope."""
    head = original[:original.index("[[variants]]")]
    if taskmanagers == 4:
        start = head.index('[[envelope.container]]\nrole   = "data-plane"')
        end = head.index("\n# The disclosure", start)
        template = head[start:end]
        head = head[:start] + "\n".join(template.replace('name   = "tm"', f'name   = "tm{i}"')
            .replace('cpus   = "32"', 'cpus   = "8"').replace('memory = "96g"', 'memory = "24g"')
            for i in range(4)) + head[end:]
    for i, (name, knobs) in enumerate(cases):
        knobs = dict(knobs, taskmanagers=taskmanagers)
        head += '\n[[variants]]\n' + f'id = {json.dumps(name)}\nlabel = {json.dumps("Flink review " + name)}\n'
        head += 'approach = "realistic"\n' + f'default = {str(i == 0).lower()}\n'
        head += 'reports = { wire_format = "rowbinary_nt" }\n'
        head += 'knobs = { ' + ', '.join(f'{k} = {json.dumps(v)}' for k, v in knobs.items()) + ' }\n'
    return head


def run_command(args, **kwargs):
    return subprocess.run([str(x) for x in args], check=True, **kwargs)


def verify_gc_flags(directory, cases):
    """One-configuration trials must prove requested worker/collector settings."""
    if len(cases) != 1:
        return None
    import re
    options = cases[0][1].get("jvm_opts", "")
    expected = []
    for option, label in (("ParallelGCThreads", "Parallel Workers"), ("ConcGCThreads", "Concurrent Workers")):
        match = re.search(rf"-XX:{option}=(\d+)(?:\s|$)", options)
        if match:
            expected.append(f"{label}: {match[1]}")
    if "-XX:+UseZGC" in options:
        expected.append("Using The Z Garbage Collector")
    if not expected:
        return True
    logs = list(directory.glob("*-sut-tm*-gc.log"))
    return bool(logs) and all(all(item in log.read_text() for item in expected) for log in logs)


def docker_output(*args):
    return subprocess.check_output(["docker", *args], text=True, timeout=20).strip()


def copy_recording(name, recording, target):
    # JFR creates its destination before finishing. Keep refreshing, and never
    # let an empty/failed copy replace an earlier completed recording.
    pending = target.with_suffix(".jfr.partial")
    copied = subprocess.run(["docker", "cp", f"{name}:/opt/flink/log/{recording}", str(pending)],
                            capture_output=True, timeout=20)
    if copied.returncode == 0 and pending.stat().st_size > 0:
        pending.replace(target)
    else:
        pending.unlink(missing_ok=True)


def confirmation_summary(records):
    """Both configurations must have independent repeats and their own A/A trials.

    CPU efficiency cannot silently substitute for throughput or correctness.
    Throttling and downstream CPU are disclosed, not interpreted as CPU work
    or used to hide a noisy baseline by choosing the quieter A/A control.
    """
    groups = {name: [r for r in records if r["kind"] == "measurement" and r["status"] == "ok"
                    and r["sut"]["variant_id"] == name and "aa_control" not in r.get("flags", [])]
              for name in ("baseline", "candidate")}
    aa = {name: [r["metrics"]["aa_spread"]["value"] for r in records
                 if r["kind"] == "verdict" and r.get("sut", {}).get("variant_id") == name
                 and "aa_spread" in r["metrics"]] for name in groups}
    summary = dict(accepted=False, repetitions={n: len(g) for n, g in groups.items()}, aa_spread=aa,
                   reason="Three correct repetitions and three A/A comparisons per configuration are required")
    if any(len(g) < 3 for g in groups.values()) or any(len(a) < 3 for a in aa.values()):
        return summary
    required = ("rows_per_s_per_core", "rows_per_s", "duplicate_rows", "throttled_us", "ch_cpu_us_per_row")
    if any(any(key not in r["metrics"] for key in required) for g in groups.values() for r in g):
        summary["reason"] = "Required throughput, correctness or cost evidence is missing"
        return summary
    medians, spreads, diagnostics = {}, {}, {}
    for name, group in groups.items():
        values = [r["metrics"]["rows_per_s_per_core"]["value"] for r in group]
        medians[name] = statistics.median(values)
        spreads[name] = (max(values) - min(values)) / medians[name]
        diagnostics[name] = {key: statistics.median(r["metrics"][key]["value"] for r in group)
                             for key in required}
        diagnostics[name]["max_duplicate_rows"] = max(r["metrics"]["duplicate_rows"]["value"] for r in group)
        diagnostics[name]["max_throttled_us"] = max(r["metrics"]["throttled_us"]["value"] for r in group)
    threshold = max(0.02, *spreads.values(), *(x for a in aa.values() for x in a))
    gain = medians["candidate"] / medians["baseline"] - 1
    throughput_gain = diagnostics["candidate"]["rows_per_s"] / diagnostics["baseline"]["rows_per_s"] - 1
    no_extra_duplicates = diagnostics["candidate"]["max_duplicate_rows"] <= diagnostics["baseline"]["max_duplicate_rows"]
    accepted = all(max(a) <= 0.02 for a in aa.values()) and gain > threshold and throughput_gain >= -0.02 and no_extra_duplicates
    summary.update(accepted=accepted, median_rows_per_s_per_core=medians, repetition_spread=spreads,
                   gain=gain, threshold=threshold, throughput_gain=throughput_gain, diagnostics=diagnostics,
                   reason="Improvement exceeds bilateral A/A and repetition noise without >2% throughput loss or more duplicates"
                   if accepted else "Bilateral noise, throughput or correctness gate did not pass")
    return summary


class Observe(threading.Thread):
    """Read shipped diagnostics only; enabled for diagnostic/screening runs."""
    def __init__(self, directory, process, recovery=False):
        super().__init__(daemon=True)
        self.directory = directory
        self.process = process
        self.done = threading.Event()
        self.recovery = recovery
        self.restarted = False
        self.recovered = False
        self.captured = set()

    def request(self, address, path):
        with urllib.request.urlopen(f"http://{address}:8081{path}", timeout=5) as response:
            return json.load(response)

    def run(self):
        while not self.done.is_set():
            try:
                names = docker_output("ps", "--filter", "name=^/spate-bench-sut-", "--format", "{{.Names}}").splitlines()
                for name in names:
                    cid = docker_output("inspect", "--format", "{{.Id}}", name)
                    if cid not in self.captured:
                        self.captured.add(cid)
                        target = self.directory / cid[:12]
                        target.mkdir(exist_ok=True)
                        (target / "inspect.json").write_text(docker_output("inspect", name))
                        if name.endswith("-jm") or "-tm" in name:
                            subprocess.run(["docker", "cp", f"{name}:/opt/flink/conf/config.yaml", str(target / "config.yaml")], capture_output=True, timeout=20)
                            subprocess.run(["docker", "cp", f"{name}:/opt/flink/usrlib/dependencies.txt", str(target / "dependencies.txt")], capture_output=True, timeout=20)
                    if "-tm" in name:
                        for recording in ("early.jfr", "late.jfr"):
                            target = self.directory / f"{cid[:12]}-{recording}"
                            copy_recording(name, recording, target)
                jm = next((n for n in names if n.endswith("-jm")), None)
                if jm:
                    networks = json.loads(docker_output("inspect", "--format", "{{json .NetworkSettings.Networks}}", jm))
                    address = next(iter(networks.values()))["IPAddress"]
                    stamp = str(time.time_ns())
                    overview = self.request(address, "/jobs/overview")
                    for job in overview.get("jobs", []):
                        jid = job["jid"]
                        for suffix in ("", "/plan", "/checkpoints", "/exceptions"):
                            data = self.request(address, f"/jobs/{jid}{suffix}")
                            (self.directory / f"{stamp}-{jid}-{suffix.strip('/') or 'job'}.json").write_text(json.dumps(data))
                            if suffix == "/checkpoints":
                                if self.recovery and not self.restarted and data.get("counts", {}).get("completed", 0) >= 3:
                                    # Pause the diagnostic harness so its crash detector does not
                                    # remove the cluster while Flink exercises normal recovery.
                                    # This run is never eligible for performance selection.
                                    self.restarted = True
                                    os.kill(self.process.pid, signal.SIGSTOP)
                                    try:
                                        tm = next(n for n in names if "-tm" in n)
                                        run_command(["docker", "restart", "--time", "0", tm], timeout=60)
                                        until = time.monotonic() + 180
                                        while time.monotonic() < until and not self.done.wait(5):
                                            restored = self.request(address, f"/jobs/{jid}/checkpoints")
                                            if restored.get("counts", {}).get("restored", 0) > 0:
                                                self.recovered = True
                                                (self.directory / "restored.json").write_text(json.dumps(restored))
                                                break
                                    finally:
                                        os.kill(self.process.pid, signal.SIGCONT)
                        detail = self.request(address, f"/jobs/{jid}")
                        for vertex in detail.get("vertices", []):
                            path = f"/jobs/{jid}/vertices/{vertex['id']}/metrics"
                            available = self.request(address, path)
                            ids = [m["id"] for m in available if any(k in m["id"] for k in (
                                "busyTime", "backPressured", "numBytes", "actualRecords", "triggeredBy", "numRequest", "writeLatency"))]
                            if ids:
                                metrics = self.request(address, path + "?get=" + ",".join(ids))
                                (self.directory / f"{stamp}-{jid}-{vertex['id']}-metrics.json").write_text(json.dumps(metrics))
            except (subprocess.SubprocessError, OSError, ValueError, StopIteration) as error:
                with (self.directory / "observer.log").open("a") as out:
                    out.write(f"{time.time()} {error}\n")
            self.done.wait(30)


class Session:
    def __init__(self):
        self.original = DESCRIPTOR.read_text()
        self.defaults = tomllib.loads(self.original)["variants"][0]["knobs"]
        self.directory = ROOT / "tuning/flink-review" / os.environ.get("RUN_ID", time.strftime("%Y%m%d-%H%M%S"))
        self.directory.mkdir(parents=True, exist_ok=True)
        self.started = time.monotonic()
        # The parent payload has a 10-hour timeout including provisioning/build.
        # Leave one hour here for upload and shutdown even on slow provisioning.
        self.screen_deadline = self.started + 4.5 * 3600
        self.deadline = self.started + 7 * 3600
        self.results = []

    def upload(self):
        if os.environ.get("S3_RUN"):
            # The instance role is PutObject-only: sync needs ListBucket, and
            # multipart KMS uploads need Decrypt. Use a single PUT of a bundle.
            archive = self.directory.parent / (self.directory.name + ".tar.gz")
            with tarfile.open(archive, "w:gz") as out:
                out.add(self.directory, arcname="flink-review")
            config = self.directory.parent / "upload-config"
            config.write_text("[default]\nregion = eu-west-2\ns3 =\n    multipart_threshold = 4GB\n")
            environment = dict(os.environ, AWS_CONFIG_FILE=str(config), AWS_DEFAULT_REGION="eu-west-2")
            environment.pop("AWS_PROFILE", None)
            environment.pop("AWS_DEFAULT_PROFILE", None)
            if archive.stat().st_size >= 4 * 1024**3:
                raise RuntimeError("Diagnostic archive exceeds single-PUT upload budget")
            prefix = os.environ["S3_RUN"] + "/logs/flink-review/"
            run_command(["aws", "s3", "cp", str(archive),
                         prefix + "artifacts.tar.gz", "--only-show-errors"], env=environment, timeout=240)
            for name in ("session.json", "candidate.json"):
                if (self.directory / name).exists():
                    run_command(["aws", "s3", "cp", str(self.directory / name),
                                 prefix + name, "--only-show-errors"], env=environment, timeout=60)

    def heartbeat(self, label, directory):
        """Small progress artifact, also available during unprofiled runs."""
        if not os.environ.get("S3_RUN"):
            return
        status = self.directory / "progress.json"
        log = directory / "bench.log"
        tail = log.read_text(errors="replace")[-16000:] if log.exists() else ""
        status.write_text(json.dumps({"phase": label, "time": time.time(), "log_tail": tail}))
        # The remote payload uses instance-role credentials, without account
        # identifiers or a user-specific named profile in this public repository.
        try:
            subprocess.run(["aws", "s3", "cp", str(status),
                            os.environ["S3_RUN"] + "/logs/flink-review/progress.json", "--only-show-errors"],
                           env=dict(os.environ, AWS_DEFAULT_REGION="eu-west-2"), timeout=30, check=False)
        except (OSError, subprocess.TimeoutExpired) as error:
            print(f"Progress upload unavailable: {error}", flush=True)

    def sweep(self, label, cases, *, reps=1, taskmanagers=1, observe=True, recovery=False, deadline=None, image=None):
        deadline = deadline or self.screen_deadline
        remaining = deadline - time.monotonic()
        if remaining < 180:
            return []
        directory = self.directory / label
        directory.mkdir()
        text = descriptor_text(self.original, cases, taskmanagers)
        if image:
            text = text.replace('image      = "spate-bench-flink"', f'image      = "{image}"')
            text = text.replace('approach = "realistic"', 'approach = "tuned"')
        DESCRIPTOR.write_text(text)
        (directory / "entrant.toml").write_text(text)
        command = [str(BENCH), "run", *["flink:" + name for name, _ in cases], "--reps", str(reps), "--trigger", "tuning", "--env", ENV_ID]
        environment = dict(os.environ, BENCH_DIAGNOSTICS_DIR=str(directory))
        before = set()
        for file in (ROOT / "tuning" / ENV_ID / "flink").glob("*.jsonl"):
            before.update(json.loads(line)["run_id"] for line in file.read_text().splitlines())
        run_command([*command, "--dry-run"], env=environment, timeout=30)
        print(f"=== Flink review: {label}; {int(remaining)}s available ===", flush=True)
        observer = None
        with (directory / "bench.log").open("w") as log:
            process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=environment, start_new_session=True)
            if observe:
                observer = Observe(directory, process, recovery)
                observer.start()
            timed_out = False
            try:
                while True:
                    wait = deadline - time.monotonic()
                    if wait <= 0:
                        raise subprocess.TimeoutExpired(command, remaining)
                    try:
                        code = process.wait(timeout=min(60, wait))
                        break
                    except subprocess.TimeoutExpired:
                        self.heartbeat(label, directory)
            except subprocess.TimeoutExpired:
                timed_out = True
                os.kill(process.pid, signal.SIGCONT)
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                code = 124
            finally:
                if observer:
                    observer.done.set()
                    observer.join(timeout=210)
                if timed_out:
                    ids = docker_output("ps", "-aq", "--filter", "name=^/spate-bench-(sut|sampler)").splitlines()
                    if ids:
                        run_command(["docker", "rm", "-f", *ids], timeout=60)
        records = []
        for file in (ROOT / "tuning" / ENV_ID / "flink").glob("*.jsonl"):
            records.extend(r for line in file.read_text().splitlines() if (r := json.loads(line))["run_id"] not in before)
        (directory / "records.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
        outcome = {"label": label, "exit_code": code, "timed_out": timed_out, "taskmanagers": taskmanagers,
                   "recovery_attempted": bool(observer and observer.restarted), "recovery_restored": bool(observer and observer.recovered),
                   "gc_flags_verified": verify_gc_flags(directory, cases)}
        (directory / "outcome.json").write_text(json.dumps(outcome, indent=2))
        self.results.append(outcome)
        (self.directory / "session.json").write_text(json.dumps(self.results, indent=2))
        DESCRIPTOR.write_text(self.original)
        self.upload()
        return records



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    defaults = tomllib.loads(DESCRIPTOR.read_text())["variants"][0]["knobs"]
    if args.dry_run:
        print(json.dumps(initial_cases(defaults), indent=2))
        return
    raise SystemExit("Historical screening matrix only; use flink-confirm.py for the predeclared follow-up")


if __name__ == "__main__":
    main()
