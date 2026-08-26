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

cat <<'EOF'

=== the box is now held open for a tuning session ===

The ladder has run; its rungs are in /tmp/rungs.jsonl and in $S3_RUN/logs/.
The session's remaining work: set the profile from the rungs, build the
entrants, and measure arms with --trigger tuning for the 50% headroom check.

  sudo -i; cd /opt/bench
  bench=target/release/bench

To move a cap, edit environments/<env>.toml, `docker rm -f` the container it
re-caps, and re-run `$bench ceiling --measure --write --env <env>`. Changing
the partition count needs the topic dropped — the delete is asynchronous, so
poll depth to zero before prefilling again.

EOF

# Hold, then end the payload cleanly a quarter-hour inside its own timeout.
#
# Not `sleep infinity`: run-bench.sh runs the payload under `timeout`, and being
# killed by it is a non-zero exit, which makes the user-data trap write
# _FAILED.json and the collector report a box failure. A tuning box reaching the
# end of its budget is the expected outcome, not a fault, so it exits zero and
# the run is claimed as the tuning run it is.
#
# The session does not extend the box's life. Save anything worth keeping to
# $S3_RUN before the deadline below.
budget=$(( (TTL_HOURS - 2) * 3600 - 900 ))
deadline=$(( SECONDS + budget ))
note "holding for $(( budget / 60 ))m; the box terminates itself after that"
while [ "$SECONDS" -lt "$deadline" ]; do
  note "held, $(( (deadline - SECONDS) / 60 ))m of session budget left"
  sleep 600
done
note "session budget spent"
