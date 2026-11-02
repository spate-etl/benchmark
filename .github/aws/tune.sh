#!/usr/bin/env bash
# bootstrap/spate-1651-consumers only, not for merge: a scripted tuning session
# for spate#1659. With the target table swapped for a Null engine copy
# (BENCH_NULL_SINK=1), it tests whether librdkafka's fetch.queue.backoff.ms
# (default 1000 ms) explains the 4-consumer shortfall: the Spate native arm at
# 1 and 4 consumers on the default, and at 1, 2, 4 and 8 consumers with the
# backoff at 10 ms. Two reps with the A/A control, per-partition source metrics
# on. It uploads everything and returns, so the box terminates as soon as the
# work is done.
#
# Nothing this box does may be published: every record is --trigger tuning,
# the arm is pinned to an unreleased spate commit, and no record carries a
# correctness gate.
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

chq() { docker exec spate-bench-clickhouse clickhouse-client --password bench -q "$1"; }

note "arm cells: native, native-c4, native-qb10, native-c2-qb10, native-c4-qb10, native-c8-qb10"
diag_start consumers
BENCH_NULL_SINK=1 "$BENCH" run spate:native spate:native-c4 spate:native-qb10 spate:native-c2-qb10 \
  spate:native-c4-qb10 spate:native-c8-qb10 --reps 2 \
  --env "$ENV_ID" --trigger tuning >>"$LOG" 2>&1 || note "arm run failed; see ladder.log"
diag_stop

# The sweep is only what it claims if the driver took the null path and the
# table stayed Null throughout.
grep -q "NULL SINK: sensor_events is a Null engine table" "$LOG" \
  && note "driver took the null-sink path" || note "driver did NOT take the null-sink path"
note "sensor_events engine at the end: $(chq "SELECT engine FROM system.tables WHERE name = 'sensor_events'" || echo unreadable)"
note "rows counted by the view in the last rep: $(chq "SELECT sum(n) FROM sensor_events_rows" || echo unreadable)"

note "session done; the payload returns and the box terminates"
