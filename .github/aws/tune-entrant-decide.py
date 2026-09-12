#!/usr/bin/env python3
"""Scoring and winner-selection for tune-entrant.sh (issue #86's Flink
sink-batch ladder).

Kept as plain Python rather than jq one-liners threaded through bash
quoting — tune.sh already shells out to python3 for its own fiddly bit
(set_key), so this follows the same precedent. tune-entrant.sh calls this
for three things and treats it as a black box otherwise:

  score <tuning-jsonl-file>              -> prints one JSON object
  is_safe <candidate-json> <base-json>   -> exit 0 (safe) / 1 (not)
  decide <base-json> <candidates-file>   -> prints the winning candidate

Thresholds are fixed here, in the same commit as the ladder they gate, and
are not read from the environment — see the module docstring in
tune-entrant.sh for why (issue #86, fixed before seeing a single result).
"""
import json
import statistics
import sys

# The harness's LEAD_METRIC (harness/src/driver.rs): the metric it differences
# for aa_spread, and the one the benchmark publishes.
LEAD = "rows_per_s_per_core"

GC_MAX_RATIO = 1.5
GC_FLOOR_US = 5_000_000
THROTTLE_MAX_RATIO = 1.5
# Periods per second. nr_throttled is a raw CFS-period counter differenced over
# the window, so an unnormalised comparison partly measures duration: a cell
# that drains at half speed accrues twice the periods at identical pressure.
THROTTLE_FLOOR_PER_S = 0.25


def load_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def score(path):
    records = load_jsonl(path)
    reps = [
        r for r in records
        if r.get("kind") == "measurement"
        and r.get("variant", {}).get("aa_label") is None
    ]
    verdicts = [r for r in records if r.get("kind") == "verdict"]

    def med(key):
        vals = [
            r["metrics"][key]["value"]
            for r in reps
            if key in r.get("metrics", {})
        ]
        return statistics.median(vals) if vals else 0.0

    aa_metrics = verdicts[0].get("metrics", {}) if verdicts else {}

    def throttle_rates():
        """nr_throttled per second of drain. There is no window metric, so the
        window is ch_written_rows / rows_per_s — both published per rep."""
        for r in reps:
            m = r.get("metrics", {})
            rows = m.get("ch_written_rows", {}).get("value", 0.0)
            rate = m.get("rows_per_s", {}).get("value", 0.0)
            thr = m.get("nr_throttled", {}).get("value")
            if rows > 0 and rate > 0 and thr is not None:
                yield thr / (rows / rate)

    rates = list(throttle_rates())

    return {
        "n": len(reps),
        "rows_per_s": med("rows_per_s"),
        "rows_per_s_per_core": med("rows_per_s_per_core"),
        "gc_pause_total_us": med("gc_pause_total_us"),
        "nr_throttled": med("nr_throttled"),
        # Rate, not count: see THROTTLE_FLOOR_PER_S. Absent inputs give None,
        # which is_safe treats as a cell it cannot clear rather than a pass.
        "throttled_per_s": statistics.median(rates) if rates else None,
        "jvm_heap_live_peak_bytes": med("jvm_heap_live_peak_bytes"),
        "jvm_heap_configured_bytes": med("jvm_heap_configured_bytes"),
        "ch_cpu_us_per_row": med("ch_cpu_us_per_row"),
        # See tune-entrant.sh: the check that max_rows, not max_batch_bytes
        # or linger_ms, is the cap actually binding a cell's batch.
        "ch_rows_per_insert": med("ch_rows_per_insert"),
        "aa_spread": aa_metrics.get("aa_spread", {}).get("value", 0.0),
        "aa_spread_declared": aa_metrics.get("aa_spread_declared", {}).get("value"),
    }


def is_safe(candidate, base):
    """GC pause total and cgroup throttle count both within the fixed
    ratio+floor of baseline. The floor keeps a near-zero baseline reading
    from making any nonzero candidate reading a trip."""
    gc_limit = max(base["gc_pause_total_us"] * GC_MAX_RATIO, GC_FLOOR_US)
    if candidate["throttled_per_s"] is None or base["throttled_per_s"] is None:
        return False
    thr_limit = max(base["throttled_per_s"] * THROTTLE_MAX_RATIO, THROTTLE_FLOOR_PER_S)
    return (
        candidate["gc_pause_total_us"] <= gc_limit
        and candidate["throttled_per_s"] <= thr_limit
    )


def is_credible(candidate, base):
    """The candidate's gain over baseline exceeds ITS OWN A/A spread.

    Scored on rows_per_s_per_core because that is what `aa_spread` measures —
    the harness differences its LEAD_METRIC (driver.rs), which is this one — and
    what the benchmark publishes. Gating a rows_per_s gain with a
    rows_per_s_per_core spread compares two different quantities, and they move
    in opposite directions on this arm.

    A cell that did not produce three reps and an A/A verdict is not a
    measurement: a partial cell medians only its surviving reps, and an absent
    verdict leaves aa_spread at 0.0, which would reduce this test to gain > 0.
    """
    if base[LEAD] <= 0 or candidate["n"] < 3 or candidate["aa_spread"] <= 0.0:
        return False
    declared = candidate.get("aa_spread_declared")
    if declared is not None and candidate["aa_spread"] > declared:
        # The rig itself says this cell's twin did not hold still, at which
        # point a difference against it is not evidence either way.
        return False
    gain = (candidate[LEAD] - base[LEAD]) / base[LEAD]
    return gain > candidate["aa_spread"]


def decide(base, candidates, base_name="baseline", base_knobs=""):
    """The best of the candidates actually run that are both safe and credible,
    by the lead metric. Falls back to the base cell when none clear both bars.

    The base is whatever the ladder measured its candidates against, which is
    not always the committed default — name it so the caller cannot confirm the
    wrong knobs."""
    scored = [
        {**c, "safe": is_safe(c["cell"], base), "credible": is_credible(c["cell"], base)}
        for c in candidates
    ]
    eligible = [c for c in scored if c["safe"] and c["credible"]]
    if not eligible:
        return {"name": base_name, "knobs": base_knobs, "cell": base,
                "safe": True, "credible": True}
    return max(eligible, key=lambda c: c["cell"][LEAD])


def max_rows_in(knobs):
    """The max_rows value out of a "k=v,k=v" knob spec, or None."""
    for kv in knobs.split(","):
        if kv.startswith("max_rows="):
            return int(kv.split("=", 1)[1])
    return None


def check_batch_caps(candidates):
    """Warn (to stdout — the caller tees it into the log) wherever a cell's
    achieved rows/INSERT landed well under the max_rows it was given: that
    means max_batch_bytes or linger_ms capped the batch before max_rows did,
    and raising max_rows accomplished nothing for that cell."""
    for c in candidates:
        want = max_rows_in(c["knobs"])
        got = c["cell"]["ch_rows_per_insert"]
        if want and got < want * 0.8:
            print(
                f"=== tune-entrant: {c['name']} landed {got:.0f} rows/INSERT "
                f"against a {want} max_rows cap — check max_batch_bytes/"
                f"linger_ms before trusting this cell ==="
            )


def main(argv):
    if len(argv) < 2:
        sys.exit(f"usage: {argv[0]} score|add_candidate|check_batch_caps|is_safe|decide ...")
    cmd = argv[1]
    if cmd == "score":
        print(json.dumps(score(argv[2])))
    elif cmd == "add_candidate":
        name, knobs, cell_json = argv[2], argv[3], argv[4]
        print(json.dumps({"name": name, "knobs": knobs, "cell": json.loads(cell_json)}))
    elif cmd == "check_batch_caps":
        check_batch_caps(load_jsonl(argv[2]))
    elif cmd == "is_safe":
        candidate = json.loads(argv[2])
        base = json.loads(argv[3])
        sys.exit(0 if is_safe(candidate, base) else 1)
    elif cmd == "decide":
        base = json.loads(argv[2])
        candidates = load_jsonl(argv[3])
        print(json.dumps(decide(base, candidates, *argv[4:6])))
    else:
        sys.exit(f"unknown command {cmd!r}")


if __name__ == "__main__":
    main(sys.argv)
