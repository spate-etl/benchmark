#!/usr/bin/env bash
# The entrant-knob tuning box's payload: walk a fixed ladder of `bench run
# --knob` cells for one arm, at fixed decision thresholds, and confirm the
# winner at the full corpus. Nothing here is published — every cell carries
# --trigger tuning, which routes its records to tuning/ and is what
# `bench validate` refuses to see under results/.
#
# Companion to tune.sh, which walks the INFRASTRUCTURE ladder instead and
# never touches an entrant. run-bench.sh dispatches here for MODE=tuning
# whenever SELECTOR names a specific entrant rather than '*'.
#
# The collector's tuning path reads only manifest.json for a mode=tuning run
# and commits nothing back (bench-collect.yml: "nothing published"), so
# anything worth keeping after this box terminates has to leave via S3 here,
# the same way tune.sh ships its rungs.
#
# There is no way to reach this box interactively mid-run (no SSM, console
# output is a stale snapshot), so the whole ladder — screen, decide, confirm —
# has to be a single unattended pass with its thresholds fixed in advance.
#
# The ladder and thresholds below are issue #86's. A later entrant-knob search
# rewrites this file (and its Python-side thresholds in tune-entrant-decide.py)
# for its own cells rather than accreting a flag-driven framework onto it.
#
# Scoring, the safety/credibility rules and winner selection all live in
# tune-entrant-decide.py alongside this script — plain Python for the JSON
# work instead of jq threaded through bash quoting, the same reasoning that
# already has tune.sh shelling out to python3 for its own fiddly bit
# (set_key). This script is orchestration only: it sequences `bench` calls
# and passes JSON blobs between them and the Python helper.
set -euo pipefail

: "${REPO:?}" "${ENV_ID:?}" "${SELECTOR:?}" "${S3_RUN:?}"

cd "$REPO"
BENCH=./target/release/bench
DECIDE="python3 $REPO/.github/aws/tune-entrant-decide.py"
ENTRANT=${SELECTOR%%:*}
TOPIC=comparison-sensor-batches
# ~1.7x MIN_WINDOW_S (120s) at this arm's published rate — long enough that a
# cell isn't flagged short_window, short enough to afford the whole ladder.
SCREEN_BATCHES=16000000
CONFIRM_BATCHES=40000000
LOG=/tmp/tune-entrant.log
RUNGS=/tmp/entrant-rungs.jsonl
TUNING_FILE="tuning/$ENV_ID/$ENTRANT/$(date -u +%Y-%m).jsonl"

note() { echo "=== tune-entrant: $* ==="; }
lines_in() { [ -f "$1" ] && awk 'END{print NR+0}' "$1" || echo 0; }

knob_args_for() { # comma-separated k=v,k=v (may be empty) -> --knob k=v ...
  local spec=$1 kv
  [ -z "$spec" ] && return 0
  IFS=, read -ra kv <<< "$spec"
  for kv in "${kv[@]}"; do printf '%s\n' "--knob" "$kv"; done
}

# Runs one cell (reps=3, plus the harness's own A/A control on the — here
# only — arm), against $BATCHES, and captures its slice of $TUNING_FILE's new
# lines into $2.
cell() { # label outfile knob_spec
  local label=$1 outfile=$2 spec=$3
  local -a knob_args=()
  mapfile -t knob_args < <(knob_args_for "$spec")
  local before after
  before=$(lines_in "$TUNING_FILE")
  note "cell $label (batches=$BATCHES, knobs=${spec:-<committed default>})"
  "$BENCH" run "$SELECTOR" --batches "$BATCHES" --reps 3 --trigger tuning \
    "${knob_args[@]}" >>"$LOG" 2>&1
  after=$(lines_in "$TUNING_FILE")
  tail -n "+$((before + 1))" "$TUNING_FILE" > "$outfile"
  note "cell $label: $((after - before)) record(s) appended"
}

# The topic is fixed depth per prefill (harness/src/corpus.rs asserts depth is
# either 0 or exactly what was asked for), so moving from screen depth to
# confirm depth needs the old corpus gone first.
drop_topic() {
  docker exec spate-bench-redpanda rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
  local depth
  for _ in $(seq 1 60); do
    depth=$(docker exec spate-bench-redpanda rpk topic describe -p "$TOPIC" 2>/dev/null \
      | awk 'NR>1{s+=$6} END{print s+0}')
    [ "${depth:-0}" = 0 ] && return 0
    sleep 2
  done
  return 1
}

note "building $SELECTOR"
"$BENCH" build "$SELECTOR" >>"$LOG" 2>&1

note "ceiling gate"
"$BENCH" ceiling --env "$ENV_ID" >>"$LOG" 2>&1

BATCHES=$SCREEN_BATCHES
note "prefilling $BATCHES batches for the screen"
"$BENCH" prefill --env "$ENV_ID" --batches "$BATCHES" >>"$LOG" 2>&1

# Both cells dry-run first: --dry-run checks the entrant's [[constraints]]
# without starting a container, and this is the last chance to catch a
# typo'd knob before spending box time on it.
for spec in "" "sink_parallelism=8,max_rows=50000,buffered_rows=100000"; do
  mapfile -t args < <(knob_args_for "$spec")
  "$BENCH" run "$SELECTOR" --batches "$BATCHES" --trigger tuning --dry-run "${args[@]}" >>"$LOG" 2>&1
done
note "both cells validated dry-run"

CANDIDATES=/tmp/candidates.jsonl
: > "$CANDIDATES"
add_candidate() { $DECIDE add_candidate "$1" "$2" "$3" >> "$CANDIDATES"; }

cell baseline /tmp/cell-baseline.jsonl ""
cell sink-8 /tmp/cell-sink-8.jsonl "sink_parallelism=8,max_rows=50000,buffered_rows=100000"

base=$($DECIDE score /tmp/cell-baseline.jsonl)
sink8=$($DECIDE score /tmp/cell-sink-8.jsonl)
# max_rows=50000/buffered_rows=100000 at sink_parallelism=8 gives the SAME
# 8x(100000+50000)=1.2M total retained payloads as baseline's
# 32x(25000+12500)=1.2M — the point of this cell is to hold that total fixed
# while each sink instance's own batch grows 4x, isolating "does fewer/larger
# buffers help" from "does the total retained set matter", which the batch-
# size-only ladder measured earlier this session could not separate (see
# issue #86's own follow-up discussion).
add_candidate sink_parallelism_8 "sink_parallelism=8,max_rows=50000,buffered_rows=100000" "$sink8"

note "screen results (baseline, then the candidate actually run):"
echo "$base" | tee -a "$LOG" >> "$RUNGS"
cat "$CANDIDATES" | tee -a "$LOG" >> "$RUNGS"

# Diagnostic only — not a gate. If the candidate's achieved rows/INSERT sits
# well under the max_rows it was given, max_batch_bytes or linger_ms is
# capping the batch before max_rows gets to, and raising max_rows accomplished
# nothing — worth knowing before writing the promotion PR either way.
$DECIDE check_batch_caps "$CANDIDATES" | tee -a "$LOG"

decision=$($DECIDE decide "$base" "$CANDIDATES")
echo "$decision" | tee -a "$LOG"
IFS=$'\t' read -r winner winner_knobs < <(
  python3 -c "import json,sys; d=json.load(sys.stdin); print(d['name'] + '\t' + d['knobs'])" <<< "$decision"
)

if [ "$winner" = baseline ]; then
  note "no candidate cleared its own A/A spread within the GC/throttle budget; the committed default stands. Skipping the confirm run."
else
  note "winner: $winner ($winner_knobs); confirming at the full corpus"
  drop_topic || { note "topic drop timed out; skipping confirm"; winner=baseline; }
fi

if [ "$winner" != baseline ]; then
  BATCHES=$CONFIRM_BATCHES
  note "prefilling $BATCHES batches for the confirm run"
  "$BENCH" prefill --env "$ENV_ID" --batches "$BATCHES" >>"$LOG" 2>&1
  cell "confirm-$winner" /tmp/cell-confirm.jsonl "$winner_knobs"
  confirm=$($DECIDE score /tmp/cell-confirm.jsonl)
  $DECIDE add_candidate "confirm-$winner" "$winner_knobs" "$confirm" | tee -a "$RUNGS" "$LOG"
fi

if [ -f "$TUNING_FILE" ]; then
  aws s3 cp "$TUNING_FILE" "$S3_RUN/tuning/$TUNING_FILE" || note "tuning records upload refused"
fi
aws s3 cp "$RUNGS" "$S3_RUN/logs/entrant-rungs.jsonl" || note "rungs upload refused"
aws s3 cp "$LOG" "$S3_RUN/logs/tune-entrant.log" || note "tune-entrant.log upload refused"

note "done"
