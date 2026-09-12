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
BACKPRESSURE="python3 $REPO/.github/aws/tune-entrant-backpressure.py"
ENTRANT=${SELECTOR%%:*}
TOPIC=comparison-sensor-batches
# The fastest cell is what has to clear MIN_WINDOW_S (120s): at 73.5 rows/batch
# this holds 155s even at 9.5M rows/s, against 253s at baseline's 5.81M.
SCREEN_BATCHES=20000000
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

# Flink's own busy/backpressure/idle metrics, per job vertex, polled every 15s
# for the duration of one cell and appended to $1 — diagnostic only, never part
# of a published or tuning record (no validate.rs/ALLOWED_UNITS concern). A
# poll failure (no job running between reps, the JobManager container not
# found) is recorded as an error line, not a script failure: this must never
# be able to break the actual measurement it runs alongside.
poll_backpressure() { # outfile label
  local outfile=$1 label=$2
  while true; do
    $BACKPRESSURE "$label" >> "$outfile" 2>&1 || true
    sleep 15
  done
}

# The TaskManager's GC log, copied out while a rep is up. The harness reads this
# file into gc_* metrics and deletes its copy (jvm.rs read_gc_log), so without
# this the run's only evidence of what the JVM actually did leaves with the box.
# It carries the two things the tuning records cannot answer: whether compressed
# references survived the heap and the 16-byte alignment ("Compressed Oops:
# Enabled"), and post-mixed-cycle occupancy, which is a live set where
# jvm_heap_live_peak_bytes is a max over every pause.
capture_gc() { # outfile
  local outfile=$1
  while true; do
    docker cp "spate-bench-sut-tm:/opt/flink/log/gc.log" "$outfile" >/dev/null 2>&1 || true
    sleep 15
  done
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
  local bp_log="/tmp/backpressure-$label.jsonl"
  : > "$bp_log"
  poll_backpressure "$bp_log" "$label" &
  local poll_pid=$!
  local gc_log="/tmp/gc-$label.log"
  capture_gc "$gc_log" &
  local gc_pid=$!
  # A cell that dies (TM OOM-kill at the top of the ladder is the expected way)
  # must not take the run with it: under `set -e` that would skip every later
  # cell AND the S3 uploads at the bottom, losing the cells that did succeed.
  local rc=0
  "$BENCH" run "$SELECTOR" --batches "$BATCHES" --reps 3 --trigger tuning \
    "${knob_args[@]}" >>"$LOG" 2>&1 || rc=$?
  [ "$rc" -eq 0 ] || note "cell $label FAILED (exit $rc) — continuing the ladder"
  kill "$poll_pid" "$gc_pid" >/dev/null 2>&1 || true
  wait "$poll_pid" "$gc_pid" 2>/dev/null || true
  after=$(lines_in "$TUNING_FILE")
  # Absent when the very first cell dies before writing a record. An empty
  # slice scores zero on every metric, which `decide` rejects as not credible —
  # the outcome a dead cell should have.
  : > "$outfile"
  [ -f "$TUNING_FILE" ] && tail -n "+$((before + 1))" "$TUNING_FILE" > "$outfile"
  note "cell $label: $((after - before)) record(s) appended, $(lines_in "$bp_log") backpressure poll(s)"
  note "cell $label oops: $(grep -m1 'Compressed Oops' "$gc_log" 2>/dev/null || echo 'no gc.log captured')"
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

# --dry-run checks the entrant's [[constraints]] without starting a container:
# the last chance to catch a typo'd knob before spending box time on it.
#
# 73728 derives a 64563 MiB heap, the most that keeps compressed references at
# ObjectAlignmentInBytes=16 (they stop at 65505, measured). The alignment is not
# optional above a 32 GiB heap and entrypoint.sh refuses the pairing without it.
HEAP_KNOB="process_mib=73728,jvm_opts=-XX:ObjectAlignmentInBytes=16"
rows_knob() { echo "$HEAP_KNOB,max_rows=$1,buffered_rows=$(( $1 * 2 ))"; }
ROWS_25K=$(rows_knob 25000)
ROWS_50K=$(rows_knob 50000)
for spec in "$HEAP_KNOB" "$ROWS_25K" "$ROWS_50K"; do
  mapfile -t args < <(knob_args_for "$spec")
  "$BENCH" run "$SELECTOR" --batches "$BATCHES" --trigger tuning --dry-run "${args[@]}" >>"$LOG" 2>&1
done
note "all three cells validated dry-run"

# Surfaces a broken poller at the top of the log instead of as an artefact of
# error lines after the box is gone. Never gates the run.
note "backpressure preflight: $($BACKPRESSURE preflight 2>&1 || true)"

CANDIDATES=/tmp/candidates.jsonl
: > "$CANDIDATES"
add_candidate() { $DECIDE add_candidate "$1" "$2" "$3" >> "$CANDIDATES"; }

cell heaponly /tmp/cell-heaponly.jsonl "$HEAP_KNOB"
cell rows25k /tmp/cell-rows25k.jsonl "$ROWS_25K"
cell rows50k /tmp/cell-rows50k.jsonl "$ROWS_50K"

base=$($DECIDE score /tmp/cell-heaponly.jsonl)
# heaponly is the control, not a rung: it shares the heap and the alignment with
# both candidates, so the only thing left varying is max_rows. Comparing them
# against the committed default instead would confound batch size with a 3.4x
# heap and an object-alignment change.
#
# Retention is parallelism x (buffered_rows + inflight x max_rows), ~1.2M / 2.4M
# / 4.8M payloads across these three. Its per-payload cost is the open question:
# the previous run's two cells at one heap imply ~7.3 KiB, a synthetic replica
# of the payload implies ~1.1 KiB, and jvm_heap_live_peak_bytes cannot settle it
# because it is a max over every pause, not a live set. The captured gc.log is
# what answers it, from post-mixed-cycle occupancy.
add_candidate rows25k "$ROWS_25K" "$($DECIDE score /tmp/cell-rows25k.jsonl)"
add_candidate rows50k "$ROWS_50K" "$($DECIDE score /tmp/cell-rows50k.jsonl)"

note "screen results (heaponly, then the candidates):"
echo "$base" | tee -a "$LOG" >> "$RUNGS"
cat "$CANDIDATES" | tee -a "$LOG" >> "$RUNGS"

# Diagnostic only — not a gate. If the candidate's achieved rows/INSERT sits
# well under the max_rows it was given, max_batch_bytes or linger_ms is
# capping the batch before max_rows gets to, and raising max_rows accomplished
# nothing — worth knowing before writing the promotion PR either way.
$DECIDE check_batch_caps "$CANDIDATES" | tee -a "$LOG"

decision=$($DECIDE decide "$base" "$CANDIDATES" heaponly "$HEAP_KNOB")
echo "$decision" | tee -a "$LOG"
IFS=$'\t' read -r winner winner_knobs < <(
  python3 -c "import json,sys; d=json.load(sys.stdin); print(d['name'] + '\t' + d['knobs'])" <<< "$decision"
)

# The winner is confirmed even when it is heaponly: unlike the committed
# default, heaponly is itself a configuration change and worth a full-corpus
# number. Only a failed topic drop skips the confirm.
confirm_ok=1
if [ "$winner" = heaponly ]; then
  note "no batch size cleared its own A/A spread within the GC/throttle budget; heaponly stands"
else
  note "winner: $winner ($winner_knobs)"
fi
note "confirming $winner at the full corpus"
drop_topic || { note "topic drop timed out; skipping confirm"; confirm_ok=0; }

if [ "$confirm_ok" = 1 ]; then
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
for extra in /tmp/backpressure-*.jsonl /tmp/gc-*.log; do
  [ -f "$extra" ] || continue
  aws s3 cp "$extra" "$S3_RUN/logs/$(basename "$extra")" || note "$(basename "$extra") upload refused"
done

note "done"
