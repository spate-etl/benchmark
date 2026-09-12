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
# The vector source/chunk screen.
#
# Scripted rather than typed into a held session: the SSM console freezes, and a
# session nobody can see is a box burning $5/hour blind. Every cell uploads its
# records and one progress line BEFORE the next starts, so
# `aws s3 cp $S3_RUN/logs/progress.jsonl -` is the whole monitoring story and it
# survives the agent dying.
#
# Screening only. What this settles is declared in the descriptor and
# re-measured as an ordinary published run — see methodology/comparability.md.
# ---------------------------------------------------------------------------
ARM=vector:json-each-row
# 12M batches is ~882M landed rows: ~7 minutes a drain at the 2.10M rows/s this
# arm last published, and a cell is two drains because the driver always
# measures its first arm again as an A/A control. It stays above MIN_WINDOW_S
# until 7.35M rows/s, well past anything expected here.
SCREEN_BATCHES=12000000
PROGRESS=/tmp/progress.jsonl
# The anchor reproduces 2,095,054 rows/s. Materially below means the baseline
# has moved and a curve built on it compares to nothing. It carries the 0.58
# upgrade and the batch-seal fix (max_bytes raised so max_events binds), so it
# is a sanity gate, not a regression gate.
GATE_ROWS_PER_S=1600000
# Remap in-flight residency scales with the source count, so a source cell is
# projected from a measured cell before it is spent rather than discovered as an
# OOM kill. 80 GiB of the 96 GiB envelope.
MEM_CEILING=85899345920

records() { ls "tuning/$ENV_ID/vector/"*.jsonl 2>/dev/null | head -1; }
record_lines() { local f; f=$(records); [ -n "$f" ] && wc -l < "$f" || echo 0; }

# A cell is two drains and uploads only once it ends, which is too coarse to
# abort on. The heartbeat ships the running cell's log and an elapsed line every
# half minute, so `aws s3 cp $S3_RUN/logs/live-cell.log -` shows a cell going
# wrong while it is still going wrong.
CURRENT=/tmp/current-cell
heartbeat() {
  local id start
  while sleep 30; do
    [ -s "$CURRENT" ] || continue
    id=$(cut -d' ' -f1 "$CURRENT"); start=$(cut -d' ' -f2 "$CURRENT")
    [ -n "$id" ] || continue
    jq -nc --arg cell "$id" --argjson elapsed "$(( SECONDS - start ))" \
       --arg tail "$(tail -c 2000 "/tmp/cell-$id.log" 2>/dev/null)" \
       '{cell:$cell,elapsed_s:$elapsed,tail:$tail}' \
      > /tmp/live.json 2>/dev/null || continue
    aws s3 cp /tmp/live.json "$S3_RUN/logs/live.json" >/dev/null 2>&1 || true
    aws s3 cp "/tmp/cell-$id.log" "$S3_RUN/logs/live-cell.log" >/dev/null 2>&1 || true
  done
}
heartbeat &
HEARTBEAT=$!
trap 'kill "$HEARTBEAT" 2>/dev/null || true' EXIT

cell() { # id knob...
  local id=$1; shift
  local log=/tmp/cell-$id.log t0=$SECONDS rc=0 before after
  before=$(record_lines)
  : > /tmp/.cellmark
  : > "$log"
  echo "$id $SECONDS" > "$CURRENT"

  note "cell $id: $*"
  "$BENCH" run "$ARM" --reps 1 --batches "$SCREEN_BATCHES" --env "$ENV_ID" \
    --trigger tuning "$@" > "$log" 2>&1 || rc=$?
  : > "$CURRENT"
  after=$(record_lines)

  aws s3 cp "$log" "$S3_RUN/logs/cell-$id.log" || note "cell log upload refused"
  # `cp` and not `sync`: the instance role has no ListBucket, by design, so
  # nothing here may compare against the bucket.
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
        for k in ("rows_per_s", "rows_per_s_per_core", "cores_used",
                  "peak_anon_bytes", "cpu_us_per_row", "ch_cpu_us_per_row",
                  "ch_rows_per_insert"):
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
        "cores_used": med("cores_used", 2),
        "peak_anon_bytes": med("peak_anon_bytes"),
        "cpu_us_per_row": med("cpu_us_per_row", 2),
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

# One field of one cell, or empty if the cell did not report it.
figure() { # cell field
  python3 -c "
import json,sys
for l in open('$PROGRESS'):
    r = json.loads(l)
    if r.get('cell') == '$1' and r.get('$2') is not None:
        print(int(r['$2'])); break
"
}

# Does cell \$1 beat cell \$2 on the lead metric by at least 5%?
beats() { # cell reference
  local a b
  a=$(figure "$1" rows_per_s_per_core); b=$(figure "$2" rows_per_s_per_core)
  [ -n "$a" ] && [ -n "$b" ] && [ "$a" -gt "$(( b * 105 / 100 ))" ]
}

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
note "building vector"
"$BENCH" build vector

# v0 — the committed configuration, with the transform in the spelling that
# preceded the rewrite. Every later cell is read against this or against the
# cell it varies from.
cell v0 --knob transform=twopass

anchor=$(figure v0 rows_per_s)
if [ -z "$anchor" ] || [ "$anchor" -lt "$GATE_ROWS_PER_S" ]; then
  note "GATE: the anchor measured ${anchor:-no} rows/s against 2095054 on the"
  note "published 0.57 reps. The baseline has moved, so nothing built on it"
  note "would be comparable. Stopping."
  cat "$PROGRESS"
  exit 0
fi

# The three decisive cells, each against the reference it varies from: v1 vs v0
# isolates the VRL rewrite, v2 vs v1 the runner's in-flight budget, v3 vs v2 the
# source count. v3 carries v2's chunk size because 16 remaps at the shipped 1000
# projects past the envelope — the point of v2 running first.
cell v1 --knob transform=fused
cell v2 --knob transform=fused --knob chunk_size_events=64
cell v3 --knob transform=fused --knob chunk_size_events=64 --knob sources=16

if ! beats v1 v0 && ! beats v2 v1 && ! beats v3 v2; then
  note "GATE: none of the VRL rewrite, the chunk size or the source count"
  note "cleared 5% against the cell it varies from. No lever here is reachable"
  note "from configuration. Stopping rather than pricing the refinements."
  cat "$PROGRESS"
  exit 0
fi

# Source cells are projected from v2's measured residency before being spent:
# the fixed terms (librdkafka prefetch, sink encoders and buffer) do not scale
# with the source count, the remap term does.
mem_v2=$(figure v2 peak_anon_bytes)
project() { # sources -> projected bytes at v2's chunk size
  python3 -c "
fixed = 21.6e9
per_remap = max(($mem_v2 - fixed) / 8.0, 0)
print(int(fixed + per_remap * $1))"
}

for n in 32 4; do
  if [ -z "$mem_v2" ]; then
    note "v2 reported no residency, so sources=$n cannot be projected; running"
    note "only the source count that is smaller than the measured one."
    [ "$n" -gt 8 ] && continue
    projected=0
  else
    projected=$(project "$n")
  fi
  if [ "$projected" -gt "$MEM_CEILING" ]; then
    note "skipping sources=$n: projects $(( projected / 1000000000 ))GB from v2's"
    note "measured $(( mem_v2 / 1000000000 ))GB, past the envelope. Not spending a cell on an OOM."
    continue
  fi
  cell "v_s$n" --knob transform=fused --knob chunk_size_events=64 --knob "sources=$n"
done

cell v6 --knob transform=fused --knob chunk_size_events=256 --knob sources=16

note "screen complete"
cat "$PROGRESS"
