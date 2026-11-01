#!/usr/bin/env bash
# bootstrap/ch268-pool only, not for merge: a scripted tuning session for spate
# 0.4.0 on ClickHouse 26.8 at 48 cores. For background_pool_size 16, 24 and 32
# it measures the Spate native arm (two reps and the A/A control, with the
# arm's metrics tapped) and the Native and RowBinary ingest ceilings. It uploads
# everything and returns, so the box terminates as soon as the work is done.
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

# Every upload is a single PutObject: `aws s3 cp` goes multipart above 8 MB, and
# on this bucket's KMS key that needs kms:Decrypt, which the instance role lacks.
put() { # local-file key-under-the-run-prefix
  local b=${S3_RUN#s3://}
  aws s3api put-object --bucket "${b%%/*}" --key "${b#*/}/$2" --body "$1" >/dev/null \
    || note "upload of $2 refused"
}

DIAG=/tmp/diag
diag_start() { # label
  mkdir -p "$DIAG/$1"
  python3 "$REPO/.github/aws/diag.py" "$DIAG/$1" </dev/null >>"$DIAG/$1/collector.err" 2>&1 &
  echo $! > "$DIAG/collector.pid"
}
diag_stop() {
  [ -f "$DIAG/collector.pid" ] || return 0
  local pid
  pid=$(cat "$DIAG/collector.pid")
  rm -f "$DIAG/collector.pid"
  kill -TERM "$pid" 2>/dev/null || true
  for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
  kill -KILL "$pid" 2>/dev/null || true
}

# Ships whatever exists, on success or failure: the box's filesystem dies with it.
ship() {
  diag_stop
  [ -d "$REPO/tuning" ] && cp -r "$REPO/tuning" "$DIAG/tuning"
  [ -f "$RUNGS" ] && cp "$RUNGS" "$DIAG/rungs.jsonl"
  [ -f "$LOG" ] && cp "$LOG" "$DIAG/ladder.log"
  cp "$PROFILE" "$DIAG/$ENV_ID.toml"
  cp /tmp/ceil-*.json "$DIAG/" 2>/dev/null || true
  if [ -d "$REPO/tuning" ]; then
    (cd "$REPO" && find tuning -type f -name '*.jsonl') | while read -r f; do
      put "$REPO/$f" "logs/$f"
    done
  fi
  [ -f "$RUNGS" ] && put "$RUNGS" logs/rungs.jsonl
  tar -C /tmp -czf /tmp/diag.tar.gz diag
  put /tmp/diag.tar.gz logs/diag.tar.gz
}
trap ship EXIT


chq() { docker exec spate-bench-clickhouse clickhouse-client --password bench -q "$1"; }

# The pool grows at runtime and cannot shrink without a restart, so the values
# ascend. The file lives in the running container, so every step re-checks it.
set_pool() { # threads
  docker exec spate-bench-clickhouse bash -c \
    "printf '<clickhouse><background_pool_size>%s</background_pool_size></clickhouse>\n' $1 \
     > /etc/clickhouse-server/config.d/zz-bench-pool.xml"
  chq "SYSTEM RELOAD CONFIG"
  sleep 5
  check_pool "$1"
}
check_pool() { # expected threads
  local got size
  got=$(chq "SELECT value FROM system.server_settings WHERE name = 'background_pool_size'")
  size=$(chq "SELECT value FROM system.metrics WHERE metric = 'BackgroundMergesAndMutationsPoolSize'")
  note "background_pool_size expected $1, setting $got, pool size metric $size"
  [ "$got" = "$1" ]
}

note "host: $(nproc) cpus, $(free -g | awk '/^Mem:/{print $2}') GiB"
note "profile: $(grep -E '^(image|cpus|cluster)' "$PROFILE" | tr '\n' ' ')"

note "building the spate arm"
"$BENCH" build spate >>"$LOG" 2>&1

note "prefilling the run-mode corpus"
"$BENCH" prefill --env "$ENV_ID" >>"$LOG" 2>&1

for pool in 16 24 32; do
  set_pool "$pool" || note "pool $pool was not applied"

  note "arm cells, pool $pool"
  diag_start "pool$pool"
  "$BENCH" run spate:native --reps 2 \
    --env "$ENV_ID" --trigger tuning >>"$LOG" 2>&1 || note "pool $pool arm run failed"
  diag_stop
  check_pool "$pool" || note "pool $pool lost during the arm run"

  rm -f "$CEIL"
  measure "pool$pool" "$(grep -E '^partitions' "$PROFILE" | tr -dc 0-9)" 3 48 \
    --only rowbinary --only native
  [ -f "$CEIL" ] && cp "$CEIL" "/tmp/ceil-pool$pool.json"
  check_pool "$pool" || note "pool $pool lost during the ceiling pass"
done

note "rungs measured:"
cat "$RUNGS" || true
note "session done; the payload returns and the box terminates"
