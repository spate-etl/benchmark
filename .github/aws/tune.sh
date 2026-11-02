#!/usr/bin/env bash
# bootstrap/spate-1651-consumers only, not for merge: a scripted tuning session
# for spate#1651. With ClickHouse's insert delay lifted, it measures the Spate
# native arm at 1, 2, 4 and 8 Kafka consumers, in one interleaved sweep
# of two reps with the A/A control, the arm's metrics tapped and the host
# collector running. It uploads everything and returns, so the box terminates
# as soon as the work is done.
#
# Nothing this box does may be published: every record is --trigger tuning,
# and the arm is pinned to an unreleased spate commit.
set -euo pipefail

: "${REPO:?}" "${ENV_ID:?}" "${S3_RUN:?}" "${TTL_HOURS:?}"

cd "$REPO"
BENCH=./target/release/bench
PROFILE=environments/$ENV_ID.toml
LOG=/tmp/ladder.log

note() { echo "=== tune: $* ==="; }

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
  mkdir -p "$DIAG"
  [ -d "$REPO/tuning" ] && cp -r "$REPO/tuning" "$DIAG/tuning"
  [ -f "$LOG" ] && cp "$LOG" "$DIAG/ladder.log"
  cp "$PROFILE" "$DIAG/$ENV_ID.toml"
  if [ -d "$REPO/tuning" ]; then
    (cd "$REPO" && find tuning -type f -name '*.jsonl') | while read -r f; do
      put "$REPO/$f" "logs/$f"
    done
  fi
  [ -f "$LOG" ] && put "$LOG" logs/ladder.log
  tar -C /tmp -czf /tmp/diag.tar.gz diag
  put /tmp/diag.tar.gz logs/diag.tar.gz
}
trap ship EXIT

note "host: $(nproc) cpus, $(free -g | awk '/^Mem:/{print $2}') GiB"
note "profile: $(grep -E '^(image|cpus|cluster)' "$PROFILE" | tr '\n' ' ')"

note "building the spate arm"
"$BENCH" build spate >>"$LOG" 2>&1

note "prefilling the run-mode corpus"
"$BENCH" prefill --env "$ENV_ID" >>"$LOG" 2>&1

# Lift the sink's bound: at two consumers ClickHouse delayed 54-67% of inserts
# on the active-part count while using 34 of its 48 cores. A larger merge pool
# and higher part limits let the arm run past it, so the sweep measures the
# source. The file lives in the running container, so a restart keeps it.
chq() { docker exec spate-bench-clickhouse clickhouse-client --password bench -q "$1"; }
docker exec spate-bench-clickhouse bash -c "cat > /etc/clickhouse-server/config.d/zz-bench-1651.xml" <<'XML'
<clickhouse>
  <background_pool_size>32</background_pool_size>
  <merge_tree>
    <parts_to_delay_insert>3000</parts_to_delay_insert>
    <parts_to_throw_insert>6000</parts_to_throw_insert>
  </merge_tree>
</clickhouse>
XML
docker restart spate-bench-clickhouse >/dev/null
for _ in $(seq 1 90); do chq "SELECT 1" >/dev/null 2>&1 && break; sleep 2; done
note "clickhouse pool $(chq "SELECT value FROM system.server_settings WHERE name = 'background_pool_size'") \
delay $(chq "SELECT value FROM system.merge_tree_settings WHERE name = 'parts_to_delay_insert'") \
throw $(chq "SELECT value FROM system.merge_tree_settings WHERE name = 'parts_to_throw_insert'")"

note "arm cells: native (1 consumer), native-c2, native-c4, native-c8"
diag_start consumers
"$BENCH" run spate:native spate:native-c2 spate:native-c4 spate:native-c8 --reps 2 \
  --env "$ENV_ID" --trigger tuning >>"$LOG" 2>&1 || note "arm run failed; see ladder.log"
diag_stop

note "session done; the payload returns and the box terminates"
