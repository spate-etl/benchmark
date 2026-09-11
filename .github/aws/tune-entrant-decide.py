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

GC_MAX_RATIO = 1.5
GC_FLOOR_US = 5_000_000
THROTTLE_MAX_RATIO = 1.5
THROTTLE_FLOOR = 100


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

    return {
        "n": len(reps),
        "rows_per_s": med("rows_per_s"),
        "rows_per_s_per_core": med("rows_per_s_per_core"),
        "gc_pause_total_us": med("gc_pause_total_us"),
        "nr_throttled": med("nr_throttled"),
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
    thr_limit = max(base["nr_throttled"] * THROTTLE_MAX_RATIO, THROTTLE_FLOOR)
    return (
        candidate["gc_pause_total_us"] <= gc_limit
        and candidate["nr_throttled"] <= thr_limit
    )


def is_credible(candidate, base):
    """The candidate's rows_per_s gain over baseline exceeds ITS OWN A/A
    spread — not any other sweep's, and not a bare positive delta."""
    if base["rows_per_s"] <= 0:
        return False
    gain = (candidate["rows_per_s"] - base["rows_per_s"]) / base["rows_per_s"]
    return gain > candidate["aa_spread"]


def decide(base, candidates):
    """The candidate with the highest rows_per_s among those actually run
    that are both safe and credible. Falls back to baseline — the committed
    default stands — when none clear both bars."""
    scored = [
        {**c, "safe": is_safe(c["cell"], base), "credible": is_credible(c["cell"], base)}
        for c in candidates
    ]
    eligible = [c for c in scored if c["safe"] and c["credible"]]
    if not eligible:
        return {"name": "baseline", "knobs": "", "cell": base, "safe": True, "credible": True}
    return max(eligible, key=lambda c: c["cell"]["rows_per_s"])


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
        print(json.dumps(decide(base, candidates)))
    else:
        sys.exit(f"unknown command {cmd!r}")


if __name__ == "__main__":
    main(sys.argv)
