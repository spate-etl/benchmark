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
REFERENCE = "tuned-ref"

# Phase caps, seconds, spent in this order. Confirmation is reserved off the far
# end of the budget before the screen is given anything, so an overrunning screen
# loses its own cells and never the repetitions that decide the verdict.
SETUP_S = 35 * 60
SCREEN_S = 120 * 60
RECOVERY_S = 30 * 60
CONFIRM_S = 220 * 60
CONTROLS_S = 30 * 60
REQUIRED_SECONDS = SETUP_S + SCREEN_S + RECOVERY_S + CONFIRM_S + CONTROLS_S + 15 * 60

# `G1NewSizePercent` is an experimental HotSpot flag and the unlock must precede
# it. The entrypoint sets `-XX:-IgnoreUnrecognizedVMOptions`, so without this the
# JVM refuses to start and the cell reads as a TaskManager that exited during the
# drain rather than as a flag that was never valid.
G1_YOUNG_FLOOR = "-XX:+UnlockExperimentalVMOptions -XX:G1NewSizePercent=40 -XX:MaxGCPauseMillis=500"

# A screening cell has one repetition, and the session-1 control drifted 1.7x
# across seven hours. Nothing below this margin is a mechanism.
SCREEN_FLOOR = 0.05


def baseline_knobs(defaults):
    """The published default, unchanged: the arm confirmation measures against."""
    return dict(defaults, parallelism=32, slots=32, max_rows=50000,
                buffered_rows=100000, inflight=2, jvm_opts="", avro_mode="generic",
                runtime_image="flink:2.2.1-java17")


def reference_knobs(defaults):
    """Smaller sink buffers on the published default. Every probe varies this."""
    return dict(baseline_knobs(defaults), max_rows=12500, buffered_rows=25000, inflight=1)


def java17_cases(defaults):
    """One factor each against the reference, which carries the sweep's A/A twin."""
    reference = reference_knobs(defaults)
    return [
        (REFERENCE, reference),
        ("g1-young-floor", dict(reference, jvm_opts=G1_YOUNG_FLOOR)),
        ("g1-young-floor-big-buffers", dict(baseline_knobs(defaults), jvm_opts=G1_YOUNG_FLOOR)),
        ("network256m", dict(reference, network_memory_max="256m")),
        ("specific-avro", dict(reference, avro_mode="specific")),
    ]


def java21_cases(defaults):
    """A second invocation because `[build].image` is per descriptor, not per variant."""
    reference = dict(reference_knobs(defaults), runtime_image=RUNTIME)
    return [
        ("java21-g1", reference),
        ("java21-zgc", dict(reference, jvm_opts="-XX:+UseZGC -XX:+ZGenerational")),
    ]


def candidate_knobs(defaults):
    """The fallback candidate when no probe clears the screen's noise."""
    return reference_knobs(defaults)


def screen_cases(defaults):
    return java17_cases(defaults) + java21_cases(defaults)


def medians(records, flag="aa_control"):
    """Median primary metric per variant, over correct non-A/A measurements."""
    out = {}
    for r in records:
        if r["kind"] != "measurement" or r["status"] != "ok" or flag in r.get("flags", []):
            continue
        out.setdefault(r["sut"]["variant_id"], []).append(r["metrics"]["rows_per_s_per_core"]["value"])
    import statistics
    return {k: statistics.median(v) for k, v in out.items()}


def select_candidate(records, defaults):
    """The probe whose margin over the reference clears the screen's own noise.

    Not the maximum of the cells: a cell has one repetition, so ranking them
    against each other ranks their noise. Each is compared against the reference
    it varies by one factor, and a cell that does not clear both the A/A spread
    and `SCREEN_FLOOR` is not a mechanism. With nothing clearing it the reference
    is the candidate, which is still the session-1 result.
    """
    scored = medians(records)
    aa = [r["metrics"]["aa_spread"]["value"] for r in records
          if r["kind"] == "verdict" and "aa_spread" in r.get("metrics", {})]
    threshold = max([SCREEN_FLOOR, *aa])
    reference = scored.get(REFERENCE)
    cases = dict(screen_cases(defaults))
    qualifying = {}
    if reference:
        for name, value in scored.items():
            if name != REFERENCE and value / reference - 1 > threshold:
                qualifying[name] = value / reference - 1
    chosen = max(qualifying, key=qualifying.get) if qualifying else REFERENCE
    return dict(name=chosen, knobs=cases.get(chosen, reference_knobs(defaults)),
                image=IMAGE if chosen.startswith("java21") else None,
                threshold=threshold, reference_rows_per_s_per_core=reference,
                margins=qualifying, screened=scored)


def plan(defaults):
    return dict(
        required_seconds=REQUIRED_SECONDS,
        phases=[
            {"phase": "setup", "cap_s": SETUP_S, "drains": 0,
             "does": "ceiling, build both images, prefill"},
            {"phase": "screen", "cap_s": SCREEN_S, "drains": 5 + 1 + 2 + 1,
             "does": "one Java 17 invocation and one Java 21 invocation, each interleaved with its own A/A twin"},
            {"phase": "recovery", "cap_s": RECOVERY_S, "drains": 2,
             "does": "TaskManager restart on the SELECTED candidate; failing it stops the session"},
            {"phase": "confirmation", "cap_s": CONFIRM_S, "drains": 12,
             "does": "three repetitions, candidate against the published baseline, each arm carrying its own A/A twin, pair order reversed on alternate repetitions"},
            {"phase": "controls", "cap_s": CONTROLS_S, "drains": 3,
             "does": "fresh Spate RowBinary, only if reached"},
        ],
        baseline=baseline_knobs(defaults),
        reference=reference_knobs(defaults),
        screen=screen_cases(defaults),
        selection=f"margin over {REFERENCE} greater than both the sweep A/A spread and {SCREEN_FLOOR}",
        deferred_contract_probes=[
            ("width16-large-batch", dict(baseline_knobs(defaults), parallelism=16, slots=16,
                max_rows=262144, buffered_rows=524288, inflight=1, max_batch_bytes=67108864)),
            ("width16-25000-rows", dict(baseline_knobs(defaults), parallelism=16, slots=16,
                max_rows=25000, buffered_rows=50000)),
            ("four-taskmanagers", dict(baseline_knobs(defaults), slots=8, taskmanagers=4,
                max_rows=12500, buffered_rows=25000, inflight=1))],
        controls="Fresh Spate Native and ClickHouse Kafka need a separately budgeted sweep")


# `maxsize` is load-bearing, not tidiness. JFR writes the destination file only
# when the recording stops, so its size is not observable while it runs: a 60s
# `settings=profile` recording measured 331 MB on two CPUs under this workload's
# allocation rate, and a 32-subtask TaskManager allocates far harder. Unbounded,
# it threatens the container's writable layer and the single-PUT upload budget
# the session asserts against. JFR honours the cap by dropping its oldest chunks
# rather than by failing.
JFR = (" -XX:StartFlightRecording=name=early,filename=/opt/flink/log/early.jfr"
       ",settings=profile,delay=30s,duration=60s,maxsize=256m")


def execute(session):
    # The remaining payload budget, not the requested TTL: time already spent
    # provisioning is not available to the phases below.
    seconds = float(os.environ.get("FLINK_REVIEW_REMAINING_SECONDS", "0"))
    if seconds < REQUIRED_SECONDS:
        raise RuntimeError(f"Refusing an undersized session: need {REQUIRED_SECONDS}s remaining, have {seconds}s")
    session.deadline = session.started + seconds - 15 * 60

    # Every window is measured back from the deadline, so the phases that decide
    # the verdict are reserved before the screen is offered anything.
    controls_start = session.deadline - CONTROLS_S
    confirm_start = controls_start - CONFIRM_S
    recovery_start = confirm_start - RECOVERY_S

    review.run_command([review.BENCH, "ceiling", "--env", review.ENV_ID])
    review.run_command([review.BENCH, "build", "flink"])
    review.run_command(["docker", "build", "--build-arg", f"FLINK_RUNTIME_IMAGE={RUNTIME}",
                        "-f", "entrants/flink/Dockerfile", "-t", IMAGE, "."])
    review.run_command([review.BENCH, "prefill", "--env", review.ENV_ID])
    (session.directory / "plan.json").write_text(json.dumps(plan(session.defaults), indent=2))

    # Two invocations because `[build].image` is a descriptor field, so one sweep
    # runs one runtime. Each carries its own A/A twin on its first arm.
    # Capped forward as well as reserved backwards. The launcher gives a tuning
    # run twelve hours whatever the plan asks for, so a window derived only from
    # the deadline would hand the screen most of the session.
    screen_end = min(recovery_start, time.monotonic() + SCREEN_S)
    screened = session.sweep("screen-java17", java17_cases(session.defaults), deadline=screen_end)
    screened += session.sweep("screen-java21", java21_cases(session.defaults),
                              image=IMAGE, deadline=screen_end)
    candidate = select_candidate(screened, session.defaults)
    (session.directory / "candidate.json").write_text(json.dumps(candidate, indent=2))
    knobs, image = candidate["knobs"], candidate["image"]

    verdict = dict(accepted=False, candidate=candidate["name"], screen=candidate)
    recovered = session.sweep("recovery", [("recovery", dict(knobs, jvm_opts=knobs["jvm_opts"] + JFR))],
                              recovery=True, image=image,
                              deadline=min(confirm_start, time.monotonic() + RECOVERY_S))
    verdict["recovery_passed"] = bool(
        session.results and session.results[-1]["recovery_restored"]
        and session.results[-1]["gc_flags_verified"]
        and any(r["kind"] == "measurement" and r["status"] == "ok"
                and "aa_control" not in r.get("flags", []) for r in recovered))
    if not verdict["recovery_passed"]:
        verdict["reason"] = "Recovery or correctness failed on the selected candidate"
    else:
        confirm_end = min(controls_start, time.monotonic() + CONFIRM_S)
        confirmed = []
        # Reverse the pair order every repetition. Each one-arm invocation gets
        # the harness's mandatory twin, so both configurations face the same gate
        # rather than the quieter one setting the noise floor for both.
        for rep in range(3):
            cases = [("baseline", baseline_knobs(session.defaults), None, 30 * 60),
                     ("candidate", knobs, image, 15 * 60)]
            if rep % 2:
                cases.reverse()
            for name, case, case_image, drain_budget in cases:
                if confirm_end - time.monotonic() < 2 * drain_budget:
                    verdict["budget_exhausted"] = True
                    break
                confirmed += session.sweep(
                    f"{name}-{rep}", [(name, case)], observe=False, image=case_image,
                    deadline=min(confirm_end, time.monotonic() + 2 * drain_budget))
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
    review.DESCRIPTOR.write_text(session.original)
    if session.deadline - time.monotonic() > 180:
        import subprocess
        with (session.directory / "controls.log").open("w") as log:
            try:
                review.run_command([review.BENCH, "run", "spate:rowbinary", "--reps", "3",
                                    "--trigger", "tuning", "--env", review.ENV_ID],
                                   stdout=log, stderr=subprocess.STDOUT,
                                   timeout=session.deadline - time.monotonic())
            except subprocess.TimeoutExpired:
                print("RowBinary control reached the session deadline", flush=True)
    shutil.copytree(review.ROOT / "tuning" / review.ENV_ID, session.directory / "all-records", dirs_exist_ok=True)
    session.upload()


def execute_priority(session):
    """Use the remaining original budget for the requested single-TM ZGC work.

    This is screening, not a shortened substitute for three-repeat confirmation.
    Every measured configuration still receives the harness's A/A twin.
    """
    seconds = float(os.environ.get("FLINK_REVIEW_REMAINING_SECONDS", "0"))
    if seconds < 60 * 60:
        raise RuntimeError("Priority screening requires at least one hour remaining")
    session.deadline = session.started + seconds - 5 * 60
    review.run_command([review.BENCH, "ceiling", "--env", review.ENV_ID])
    review.run_command([review.BENCH, "build", "flink"])
    review.run_command(["docker", "build", "--build-arg", f"FLINK_RUNTIME_IMAGE={RUNTIME}",
                        "-f", "entrants/flink/Dockerfile", "-t", IMAGE, "."])
    review.run_command([review.BENCH, "prefill", "--env", review.ENV_ID])
    knobs = candidate_knobs(session.defaults)
    baseline = dict(baseline_knobs(session.defaults), max_rows=12500, buffered_rows=25000, inflight=1)
    recovered = session.sweep("priority-recovery", [("recovery", dict(knobs, jvm_opts=knobs["jvm_opts"] + JFR))],
                              recovery=True, image=IMAGE, deadline=min(session.deadline, time.monotonic() + 25 * 60))
    recovery_passed = bool(session.results and session.results[-1]["recovery_restored"]
        and session.results[-1]["gc_flags_verified"]
        and any(r["kind"] == "measurement" and r["status"] == "ok"
                and "aa_control" not in r.get("flags", []) for r in recovered))
    verdict = dict(accepted=False, recovery_passed=recovery_passed,
                   reason="Priority screening only; independent three-repeat confirmation is still required")
    if recovery_passed:
        # The first pair proves the requested collector on top of the already
        # smaller buffers. The Java 17 pair isolates the additional runtime gain.
        for label, name, case, image in [
            ("priority-zgc", "candidate", knobs, IMAGE),
            ("priority-g1", "baseline", baseline, None),
            ("priority-network", "network256m", dict(knobs, network_memory_max="256m"), IMAGE),
            ("priority-specific", "specific-avro", dict(knobs, avro_mode="specific"), IMAGE),
        ]:
            if session.deadline - time.monotonic() < 18 * 60:
                break
            session.sweep(label, [(name, case)], observe=False, image=image,
                          deadline=min(session.deadline, time.monotonic() + 25 * 60))
    (session.directory / "verdict.json").write_text(json.dumps(verdict, indent=2))
    shutil.copytree(review.ROOT / "tuning" / review.ENV_ID, session.directory / "all-records", dirs_exist_ok=True)
    session.upload()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--priority", action="store_true", help="bounded ZGC screening within the remaining original budget")
    args = parser.parse_args()
    if args.dry_run:
        import tomllib
        print(json.dumps(plan(tomllib.loads(review.DESCRIPTOR.read_text())["variants"][0]["knobs"]), indent=2))
        return
    if os.environ.get("ENV_ID") != review.ENV_ID or os.environ.get("TRIGGER") not in ("manual", "tuning"):
        raise SystemExit("Use the fixed-profile tuning launcher")
    session = review.Session()
    try:
        if args.priority:
            execute_priority(session)
        else:
            execute(session)
    finally:
        review.DESCRIPTOR.write_text(session.original)
        session.upload()


if __name__ == "__main__":
    main()
