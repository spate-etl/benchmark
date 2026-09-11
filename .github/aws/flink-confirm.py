#!/usr/bin/env python3
"""Predeclared Java 21/ZGC confirmation, with recovery and bilateral A/A first.

The earlier four-hour runner remains reproducible at commit 093a97b. This
replacement refuses to begin unless the existing instance budget fits the plan.
It never launches or extends an instance itself.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import shutil
import time

spec = importlib.util.spec_from_file_location("review", Path(__file__).with_name("flink-review.py"))
review = importlib.util.module_from_spec(spec)
spec.loader.exec_module(review)

RUNTIME = "flink:2.2.1-java21"
IMAGE = "spate-bench-flink-java21"
# Six baseline drains at up to 30 minutes, six candidate drains at 15 minutes,
# recovery plus its mandatory A/A twin at 30, build/prefill at 30, and 30 slack.
REQUIRED_SECONDS = 6 * 3600


def baseline_knobs(defaults):
    return dict(defaults, parallelism=32, slots=32, max_rows=50000,
                buffered_rows=100000, inflight=2, jvm_opts="", avro_mode="generic",
                runtime_image="flink:2.2.1-java17")


def candidate_knobs(defaults):
    return dict(baseline_knobs(defaults), max_rows=12500, buffered_rows=25000, inflight=1,
                runtime_image=RUNTIME, jvm_opts="-XX:+UseZGC -XX:+ZGenerational")


def screen_cases(defaults):
    """Independent mechanism probes; not a ladder ranked by its noisiest maximum."""
    small = dict(baseline_knobs(defaults), max_rows=12500, buffered_rows=25000, inflight=1)
    return [
        ("g1-eight-workers-width32", dict(baseline_knobs(defaults), jvm_opts="-XX:ParallelGCThreads=8 -XX:ConcGCThreads=2")),
        ("small-buffer-width32", small),
        ("network256m-width32", dict(small, network_memory_max="256m")),
        ("specific-avro-width32", dict(small, avro_mode="specific")),
    ]


def plan(defaults):
    return dict(required_seconds=REQUIRED_SECONDS, candidate=candidate_knobs(defaults),
                baseline=baseline_knobs(defaults),
                stages=["build/prefill (30 min)", "ZGC recovery + A/A (30 min)",
                        "three alternating baseline/candidate pairs, each with its own A/A (270 min)",
                        "upload/shutdown margin (30 min)"],
                deferred_mechanism_probes=screen_cases(defaults),
                deferred_contract_probes=[
                    ("width16-large-batch", dict(baseline_knobs(defaults), parallelism=16, slots=16,
                        max_rows=262144, buffered_rows=524288, inflight=1, max_batch_bytes=67108864)),
                    ("width16-25000-rows", dict(baseline_knobs(defaults), parallelism=16, slots=16,
                        max_rows=25000, buffered_rows=50000)),
                    ("four-taskmanagers", dict(baseline_knobs(defaults), slots=8, taskmanagers=4,
                        max_rows=12500, buffered_rows=25000, inflight=1))],
                controls="Fresh Spate RowBinary/Native/ClickHouse Kafka require a separately budgeted sweep")


def execute(session):
    # Read the remaining payload budget, not the requested original TTL: time
    # already spent provisioning is not available to confirmation.
    seconds = float(os.environ.get("FLINK_REVIEW_REMAINING_SECONDS", "0"))
    if seconds < REQUIRED_SECONDS:
        raise RuntimeError(f"Refusing an undersized session: need {REQUIRED_SECONDS}s remaining, have {seconds}s")
    session.deadline = session.started + seconds - 15 * 60
    review.run_command([review.BENCH, "ceiling", "--env", review.ENV_ID])
    review.run_command([review.BENCH, "build", "flink"])
    review.run_command(["docker", "build", "--build-arg", f"FLINK_RUNTIME_IMAGE={RUNTIME}",
                        "-f", "entrants/flink/Dockerfile", "-t", IMAGE, "."])
    review.run_command([review.BENCH, "prefill", "--env", review.ENV_ID])
    (session.directory / "plan.json").write_text(json.dumps(plan(session.defaults), indent=2))
    knobs = candidate_knobs(session.defaults)
    jfr = " -XX:StartFlightRecording=name=early,filename=/opt/flink/log/early.jfr,settings=profile,delay=30s,duration=60s"
    recovered = session.sweep("recovery-first", [("recovery", dict(knobs, jvm_opts=knobs["jvm_opts"] + jfr))],
                              recovery=True, image=IMAGE, deadline=min(session.deadline, time.monotonic() + 30 * 60))
    recovery_passed = bool(session.results and session.results[-1]["recovery_restored"]
        and session.results[-1]["gc_flags_verified"]
        and any(r["kind"] == "measurement" and r["status"] == "ok" for r in recovered))
    verdict = dict(accepted=False, recovery_passed=recovery_passed)
    if not recovery_passed:
        verdict["reason"] = "Recovery/correctness failed; do not spend confirmation budget"
    else:
        confirmed = []
        # Reverse the pair order every repetition. Each one-arm invocation gets
        # the harness's mandatory twin, so both configurations face the same gate.
        for rep in range(3):
            cases = [("baseline", baseline_knobs(session.defaults), None, 30 * 60),
                     ("candidate", knobs, IMAGE, 15 * 60)]
            if rep % 2:
                cases.reverse()
            for name, case, image, drain_budget in cases:
                remaining = session.deadline - time.monotonic()
                if remaining < 2 * drain_budget:
                    verdict["budget_exhausted"] = True
                    break
                records = session.sweep(f"{name}-{rep}", [(name, case)], observe=False,
                                        image=image, deadline=min(session.deadline, time.monotonic() + 2 * drain_budget))
                confirmed += records
                if not session.results[-1]["gc_flags_verified"]:
                    verdict["runtime_configuration_failed"] = True
                    break
            if verdict.get("budget_exhausted") or verdict.get("runtime_configuration_failed"):
                break
        verdict.update(review.confirmation_summary(confirmed))
        if verdict.get("runtime_configuration_failed"):
            verdict.update(accepted=False, reason="Requested runtime flags were not verified in the GC logs")
        (session.directory / "confirmation.jsonl").write_text("".join(json.dumps(r) + "\n" for r in confirmed))
    (session.directory / "verdict.json").write_text(json.dumps(verdict, indent=2))
    shutil.copytree(review.ROOT / "tuning" / review.ENV_ID, session.directory / "all-records", dirs_exist_ok=True)
    session.upload()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.dry_run:
        import tomllib
        print(json.dumps(plan(tomllib.loads(review.DESCRIPTOR.read_text())["variants"][0]["knobs"]), indent=2))
        return
    if os.environ.get("ENV_ID") != review.ENV_ID or os.environ.get("TRIGGER") not in ("manual", "tuning"):
        raise SystemExit("Use the fixed-profile tuning launcher")
    session = review.Session()
    try:
        execute(session)
    finally:
        review.DESCRIPTOR.write_text(session.original)
        session.upload()


if __name__ == "__main__":
    main()
