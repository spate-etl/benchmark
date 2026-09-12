#!/usr/bin/env bash
# The tuning box's payload: bring the infrastructure up, walk the ladder that
# sizes the profile — partitions, then the broker cap, then ClickHouse — and
# hold the box open for the session that spends what the ladder found.
#
# Nothing this box does may be published. The ladder edits
# environments/$ENV_ID.toml, which moves env_digest and infra_digest, so every
# ceiling measured here describes a profile that exists only on this box.
set -euo pipefail

: "${REPO:?}" "${ENV_ID:?}" "${S3_RUN:?}" "${TTL_HOURS:?}"

cd "$REPO"
BENCH=./target/release/bench
PROFILE=environments/$ENV_ID.toml
CEIL=environments/ceilings/$ENV_ID.json
TOPIC=comparison-sensor-batches
# Ceiling-mode depth: the consume pass refuses (DRAINED) when the backlog
# cannot outlast its window, and this host consumes at over 1.1M msgs/s.
PREFILL_BATCHES=30000000
RUNGS=/tmp/rungs.jsonl
LOG=/tmp/ladder.log

note() { echo "=== tune: $* ==="; }

set_key() { # section key value
  python3 - "$PROFILE" "$1" "$2" "$3" <<'PY'
import re, sys
path, section, key, value = sys.argv[1:5]
lines = open(path).read().splitlines(keepends=True)
here = done = False
for i, line in enumerate(lines):
    s = line.strip()
    if s.startswith("[") and s.endswith("]"):
        here = s == f"[{section}]"; continue
    if here and re.match(rf"\s*{re.escape(key)}\s*=", line):
        q = '"' if '"' in line.split("=", 1)[1] else ""
        lines[i] = f"{key} = {q}{value}{q}\n"; done = True; break
if not done: raise SystemExit(f"no {key} in [{section}]")
open(path, "w").write("".join(lines))
PY
}

ceil_val() { jq -r "$1 // 0" "$CEIL" 2>/dev/null || echo 0; }

emit() { # phase partitions broker_cpus ch_cpus secs ok
  jq -nc --arg ph "$1" --argjson p "$2" --argjson b "$3" --argjson c "$4" \
     --argjson secs "$5" --argjson ok "$6" \
     --argjson cons "$(ceil_val '.consume.msgs_per_s')" \
     --argjson consmb "$(ceil_val '.consume.mb_per_s')" \
     --argjson bcore "$(ceil_val '.consume.broker_cgroup.cores')" \
     --argjson rb "$(ceil_val '[.clickhouse[]|select(.format=="rowbinary")|.rows_per_s][0]')" \
     --argjson nat "$(ceil_val '[.clickhouse[]|select(.format=="native")|.rows_per_s][0]')" \
     --argjson ccore "$(ceil_val '.clickhouse[0].target_cgroup.cores')" \
     --argjson thr "$(ceil_val '.clickhouse[0].target_cgroup.throttled_us')" \
     '{phase:$ph,partitions:$p,broker_cpus:$b,ch_cpus:$c,secs:$secs,ok:($ok==1),
       consume_msgs_per_s:$cons,consume_mb_per_s:$consmb,broker_cores:$bcore,
       rowbinary_rows_per_s:$rb,native_rows_per_s:$nat,ch_cores:$ccore,throttled_us:$thr}' >> "$RUNGS"
}

measure() { # phase partitions broker_cpus ch_cpus [--only fmt ...]
  local ph=$1 p=$2 b=$3 c=$4
  shift 4
  local t0=$SECONDS rc=0
  "$BENCH" ceiling --measure --write "$@" --env "$ENV_ID" >>"$LOG" 2>&1 || rc=1
  # One retry after a pause: a freshly recreated container can outlast
  # bring_up's readiness wait while it replays its data directory.
  if [ "$rc" = 1 ]; then
    sleep 30
    rc=0
    "$BENCH" ceiling --measure --write "$@" --env "$ENV_ID" >>"$LOG" 2>&1 || rc=1
  fi
  emit "$ph" "$p" "$b" "$c" "$((SECONDS - t0))" "$((1 - rc))"
  note "rung $ph p=$p b=$b ch=$c ok=$((1 - rc)) ($((SECONDS - t0))s)"
  # The box terminates with its filesystem, so a failed rung's evidence has to
  # reach the payload log to survive.
  if [ "$rc" = 1 ]; then tail -n 25 "$LOG" | sed 's/^/    /'; fi
}

# `assert_cap` refuses a running container whose applied cap disagrees with the
# profile, and `bring_up` reuses a running container rather than recreating it,
# so a cap change must remove the container it re-caps. The broker's data dir
# is an NVMe bind mount, so its corpus outlives the container — but only under
# an equal or larger cap: Redpanda refuses to boot a data directory laid out
# for more shards than it has, so a cap decrease needs wipe_broker first.
set_broker() {
  set_key infra.broker cpus "$1"
  docker rm -f spate-bench-redpanda >/dev/null 2>&1 || true
  sleep 2
}
wipe_broker() {
  local bdata
  bdata=$(grep -E '^broker_data' "$PROFILE" | sed 's/.*"\(.*\)"/\1/')
  [ -d "$bdata" ] || { note "no broker_data dir at '$bdata'"; return 1; }
  docker rm -f spate-bench-redpanda >/dev/null 2>&1 || true
  rm -rf "${bdata:?}"/* "${bdata:?}"/.[!.]* 2>/dev/null || true
}
set_ch() {
  set_key infra.clickhouse cpus "$1"
  docker rm -f spate-bench-clickhouse >/dev/null 2>&1 || true
  sleep 2
}

reprefill() { # partitions
  # The topic delete needs a broker to talk to, and set_broker removes it: a
  # throwaway prefill brings the infrastructure back up first. It exits
  # non-zero when a corpus is already present — exactly when the delete below
  # is needed — so its failure is not one.
  docker ps --format '{{.Names}}' | grep -q '^spate-bench-redpanda$' \
    || "$BENCH" prefill --env "$ENV_ID" --batches 1 >>"$LOG" 2>&1 || true
  docker exec spate-bench-redpanda rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
  # The delete is asynchronous. Prefilling before it lands leaves messages
  # spread over the OLD partition count, which skews per-partition depth and
  # refuses the consume pass as DRAINED.
  local depth
  for _ in $(seq 1 60); do
    depth=$(docker exec spate-bench-redpanda rpk topic describe -p "$TOPIC" 2>/dev/null \
      | awk 'NR>1{s+=$6} END{print s+0}')
    [ "${depth:-0}" = 0 ] && break
    sleep 2
  done
  set_key infra partitions "$1"
  "$BENCH" prefill --env "$ENV_ID" --batches "$PREFILL_BATCHES" >>"$LOG" 2>&1
}

walk_ladder() {
  local p b c best_p best_b

  # Phase 1 — partitions, at a broker cap generous enough not to bind. Each
  # rung re-prefills: a topic's partition count is fixed at creation.
  set_broker 8
  for p in 8 16 24 32; do
    reprefill "$p" || { note "prefill at p=$p failed; see $LOG"; return 1; }
    measure partitions "$p" 8 16 --only rowbinary
  done

  # The largest partition count within 3% of the best consume rate: partitions
  # bound every arm's consume parallelism, so where the ceiling is flat the
  # widest topic wins.
  best_p=$(jq -s '[.[] | select(.phase=="partitions" and .ok)] as $r
    | ($r | map(.consume_msgs_per_s) | max) as $top
    | [$r[] | select(.consume_msgs_per_s >= $top * 0.97)]
    | sort_by(-.partitions)[0].partitions' "$RUNGS")
  if [ -z "$best_p" ] || [ "$best_p" = null ]; then best_p=8; fi
  note "phase 1 settles on p=$best_p"

  # Phase 2 — broker rungs ascend from the smallest cap over a fresh data
  # directory, because a shard-count decrease refuses to boot. One re-prefill
  # buys all four rungs.
  wipe_broker || return 1
  set_key infra.broker cpus 3
  reprefill "$best_p" || { note "prefill at p=$best_p failed; see $LOG"; return 1; }
  for b in 3 4 6 8; do
    set_broker "$b"
    measure broker "$best_p" "$b" 16 --only rowbinary
  done

  # The smallest cap within 3% of the best consume rate: the cap is not the
  # constraint there, so a smaller broker buys the envelope a core for free.
  best_b=$(jq -s '[.[] | select(.phase=="broker" and .ok)] as $r
    | ($r | map(.consume_msgs_per_s) | max) as $top
    | [$r[] | select(.consume_msgs_per_s >= $top * 0.97)]
    | sort_by(.broker_cpus)[0].broker_cpus' "$RUNGS")
  if [ -z "$best_b" ] || [ "$best_b" = null ]; then best_b=3; fi
  note "phase 2 settles on b=$best_b"

  # Phase 3 — ClickHouse up, with the broker left at 8: shrinking it here
  # would be a shard decrease, and its cap is not what these rungs measure.
  for c in 16 24 32 48 64; do
    set_ch "$c"
    measure clickhouse "$best_p" 8 "$c" --only rowbinary --only native
  done
}

note "host: $(nproc) cpus, $(free -g | awk '/^Mem:/{print $2}') GiB, up $(awk '{print $1}' /proc/uptime)s"
lsblk -o NAME,SIZE,MODEL,MOUNTPOINT | sed 's/^/    /'
for target in / /mnt/bench-clickhouse /mnt/bench-broker /var/lib/docker; do
  printf '    %-24s %s\n' "$target" "$(findmnt -no SOURCE --target "$target" 2>/dev/null || echo '(no mount)')"
done

# Brings the infrastructure up and refuses if the declared storage layout is
# not the one this host has, before any of the box time below is spent.
note "bringing infrastructure up"
"$BENCH" prefill --env "$ENV_ID" --batches 1

# A clean gate means the committed ceilings describe this profile already, so
# the ladder has nothing to size and the box is a held session from the start.
if "$BENCH" ceiling --env "$ENV_ID" >/dev/null 2>&1; then
  note "committed ceilings gate clean against this profile; holding without the ladder"
else
  note "walking the ladder"
  walk_ladder || note "ladder aborted; the box stays up for diagnosis"
fi

# Under logs/ because that is the one prefix the instance role can write
# besides the run markers. Guarded: an upload refusal must not kill a held
# session whose evidence is also in the payload log above.
if [ -f "$RUNGS" ]; then
  note "rungs measured:"
  cat "$RUNGS"
  aws s3 cp "$RUNGS" "$S3_RUN/logs/rungs.jsonl" || note "rungs upload refused"
fi
if [ -f "$LOG" ]; then aws s3 cp "$LOG" "$S3_RUN/logs/ladder.log" || note "ladder.log upload refused"; fi
aws s3 cp "$PROFILE" "$S3_RUN/logs/$ENV_ID.toml" || note "profile upload refused"
if [ -f "$CEIL" ]; then aws s3 cp "$CEIL" "$S3_RUN/logs/$ENV_ID.ceilings.json" || note "ceilings upload refused"; fi

# ---------------------------------------------------------------------------
# The kafka-connect GC screen.
#
# Scripted rather than typed into a held session, because the console freezes
# and a session nobody can see is a box burning $5/hour blind. Every cell
# uploads its records, its GC logs and one progress line BEFORE the next starts,
# so `aws s3 cp $S3_RUN/logs/progress.jsonl -` is the whole monitoring story and
# it survives the agent dying.
#
# Screening only. What this settles is declared in the descriptor and
# re-measured as an ordinary published run — see methodology/comparability.md.
# ---------------------------------------------------------------------------
ARM=kafka-connect
# 12M batches is ~894M landed rows: 11 minutes a drain at the 1.33M rows/s this
# arm last published, under 3 at the rate the diagnosis predicts. It stays above
# MIN_WINDOW_S until about 7.4M rows/s, past which cells carry ShortWindow — a
# result worth re-measuring at the full corpus anyway.
SCREEN_BATCHES=12000000
PROGRESS=/tmp/progress.jsonl
# The anchor cell reproduces p500-tuned's 4,696,111 rows/s. Materially below that
# means this box is not the one the curve is being compared against.
GATE_ROWS_PER_S=4000000

records() { ls "tuning/$ENV_ID/$ARM/"*.jsonl 2>/dev/null | head -1; }
record_lines() { local f; f=$(records); [ -n "$f" ] && wc -l < "$f" || echo 0; }

cell() { # id reps batches knob...
  local id=$1 reps=$2 batches=$3; shift 3
  local log=/tmp/cell-$id.log t0=$SECONDS rc=0 before after
  before=$(record_lines)
  : > /tmp/.cellmark

  note "cell $id: reps=$reps batches=$batches $*"
  "$BENCH" run "$ARM" --reps "$reps" --batches "$batches" --env "$ENV_ID" \
    --trigger tuning "$@" > "$log" 2>&1 || rc=$?
  after=$(record_lines)

  aws s3 cp "$log" "$S3_RUN/logs/cell-$id.log" || note "cell log upload refused"
  # The GC logs the harness now keeps for a trigger that bars publication, plus
  # the cell's own records. `cp` and not `sync`: the instance role has no
  # ListBucket, by design, so nothing here may compare against the bucket.
  find tuning -type f -newer /tmp/.cellmark 2>/dev/null | while read -r f; do
    aws s3 cp "$f" "$S3_RUN/tuning/$id/${f#tuning/}" || note "upload refused: $f"
  done

  python3 - "$id" "$rc" "$((SECONDS - t0))" "$before" "$after" "$(records)" "$*" \
    >> "$PROGRESS" <<'PY'
import json, statistics, sys
cell, rc, secs, before, after, path, knobs = sys.argv[1:8]
out = {"cell": cell, "rc": int(rc), "secs": int(secs), "knobs": knobs.strip(),
       "records": int(after) - int(before)}
try:
    with open(path) as fh:
        new = fh.read().splitlines()[int(before):int(after)]
    vals = {}
    for line in new:
        r = json.loads(line)
        # The A/A half is the same arm under a second label; it measures rig
        # noise, not this cell, so it is excluded from the cell's own figure.
        if r.get("kind") != "measurement" or r.get("variant", {}).get("aa_label"):
            continue
        # ch_* say WHY a cell moved when the batch shrinks: whether ClickHouse
        # costs more per row, or task threads are just blocking on more inserts.
        for k in ("rows_per_s", "rows_per_s_per_core", "gc_pause_total_us",
                  "ch_cpu_us_per_row", "ch_rows_per_insert", "cores_used"):
            if k in r.get("metrics", {}):
                vals.setdefault(k, []).append(r["metrics"][k]["value"])
    def med(k, nd=0):
        if not vals.get(k):
            return None
        m = statistics.median(vals[k])
        return round(m, nd) if nd else round(m)
    out.update({
        "rows_per_s": med("rows_per_s"),
        "rows_per_s_per_core": med("rows_per_s_per_core"),
        "gc_pause_total_us": med("gc_pause_total_us"),
        "cores_used": med("cores_used", 2),
        "ch_cpu_us_per_row": med("ch_cpu_us_per_row", 3),
        "ch_rows_per_insert": med("ch_rows_per_insert"),
    })
except Exception as e:
    out["error"] = str(e)
print(json.dumps(out), flush=True)
PY
  aws s3 cp "$PROGRESS" "$S3_RUN/logs/progress.jsonl" || note "progress upload refused"
  tail -1 "$PROGRESS"
}

best_rows() { python3 -c "
import json,sys
v=[json.loads(l).get('rows_per_s') or 0 for l in open('$PROGRESS')]
print(int(max(v or [0])))"; }

# The bring-up above left one message on the topic, and prefill refuses a
# partially-filled corpus rather than topping it up. Drop and wait: the delete
# is asynchronous, and prefilling before it lands spreads the corpus over a
# topic that is still going away.
note "dropping $TOPIC before the screen's prefill"
docker exec spate-bench-redpanda rpk topic delete "$TOPIC" >/dev/null 2>&1 || true
for _ in $(seq 1 60); do
  depth=$(docker exec spate-bench-redpanda rpk topic describe -p "$TOPIC" 2>/dev/null \
    | awk 'NR>1{s+=$6} END{print s+0}')
  [ "${depth:-0}" = 0 ] && break
  sleep 2
done

note "prefilling $SCREEN_BATCHES batches for the screen"
"$BENCH" prefill --env "$ENV_ID" --batches "$SCREEN_BATCHES"
note "building $ARM"
"$BENCH" build "$ARM"

# The bundle that won the poll_records=500 sweep: 4,696,111 rows/s, 172,037 per
# core, GC at 8.0% over 540 collections with zero mixed and zero full.
# IHOP is not here — pinned at 25% it measured identical GC behaviour (540
# collections, 0 mixed, 0 full, 3 concurrent cycles) and changed nothing.
TUNED="-XX:+UnlockExperimentalVMOptions -XX:ObjectAlignmentInBytes=16 \
-XX:G1NewSizePercent=60 -XX:ConcGCThreads=12 -XX:+AlwaysPreTouch \
-XX:+ExitOnOutOfMemoryError"

cellk() { # id poll
  cell "$1" 1 "$SCREEN_BATCHES" --knob heap_mib=63488 \
    --knob "poll_records=$2" --knob "buffer_count=$2" --knob "jvm_opts=$TUNED"
}

# Collection COUNT is allocation over eden and does not move with poll_records;
# survivors per collection do. So GC time should keep falling as the batch
# shrinks, until the insert count starts costing more than the GC it saves.
# 7,350 rows per INSERT at poll=100 is below where ClickHouse's per-row cost is
# expected to start climbing, which is what ch_cpu_us_per_row is on the progress
# line to catch.
#
# 500 first as the anchor: it reproduces 4,696,111 on this box, so the curve is
# internally comparable rather than joined to another run.
cellk p500 500

if [ "$(best_rows)" -lt "$GATE_ROWS_PER_S" ]; then
  note "GATE: the anchor measured $(best_rows) rows/s against 4696111 on the"
  note "previous box. The baseline has moved, so a curve built on it would not"
  note "be comparable to anything. Stopping."
  cat "$PROGRESS"
  exit 0
fi

cellk p400 400
cellk p300 300
cellk p200 200
cellk p100 100

note "screen complete"
cat "$PROGRESS"
