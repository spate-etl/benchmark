#!/usr/bin/env bash
# bootstrap/spate-1651-consumers only, not for merge: a scripted tuning session
# for spate#1659. With the target table swapped for a Null engine copy
# (BENCH_NULL_SINK=1), it measures the Spate native arm at 1, 2 and 4 Kafka
# consumers, two reps with the A/A control, per-partition source metrics on and
# the arm built with frame pointers. Rep 2 of the 2- and 4-consumer drains is
# profiled mid-drain: off-CPU stacks of every thread (offcputime) for 15 s,
# then on-CPU samples (perf) for 12 s. Rep 1 runs unprofiled. It uploads
# everything and returns, so the box terminates as soon as the work is done.
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

note "installing profilers"
PROF=$DIAG/prof
mkdir -p "$PROF"
if DEBIAN_FRONTEND=noninteractive apt-get install -y -q bpfcc-tools \
     "linux-tools-$(uname -r)" "linux-headers-$(uname -r)" >>"$LOG" 2>&1; then
  note "profilers: $(command -v offcputime-bpfcc) $(command -v perf)"
else
  note "profiler install failed; the sweep runs unprofiled"
fi

# Profiles the second drain of each of `CONSUMERS` 2 and 4, 22 s after its
# container starts, while records flow. Stops when the sweep writes its marker.
profile_drain() { # container-id consumers
  local pid
  pid=$(docker inspect -f '{{.State.Pid}}' "$1")
  sleep 22
  timeout 60 offcputime-bpfcc -p "$pid" -f 15 >"$PROF/offcpu-c$2.folded" 2>"$PROF/offcpu-c$2.err" || true
  timeout 60 perf record -F 199 --call-graph fp -p "$pid" -o "/tmp/perf-c$2.data" -- sleep 12 \
    >"$PROF/perf-c$2.err" 2>&1 || true
  perf script -i "/tmp/perf-c$2.data" 2>>"$PROF/perf-c$2.err" | gzip >"$PROF/perf-c$2.txt.gz" || true
  note "profiled the c$2 drain (pid $pid)"
}
profile_watch() {
  local last="" cid c n2=0 n4=0
  while [ ! -f /tmp/sweep.done ]; do
    cid=$(docker ps -q --filter name=spate-bench-sut-sut | head -1 || true)
    if [ -n "$cid" ] && [ "$cid" != "$last" ]; then
      last=$cid
      c=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$cid" 2>/dev/null \
        | sed -n 's/^CONSUMERS=//p' || true)
      case "$c" in
        2) n2=$((n2 + 1)); [ "$n2" = 2 ] && profile_drain "$cid" 2 & ;;
        4) n4=$((n4 + 1)); [ "$n4" = 2 ] && profile_drain "$cid" 4 & ;;
      esac
    fi
    sleep 1
  done
  wait
}

note "building the spate arm"
"$BENCH" build spate >>"$LOG" 2>&1

note "prefilling the run-mode corpus"
"$BENCH" prefill --env "$ENV_ID" >>"$LOG" 2>&1

chq() { docker exec spate-bench-clickhouse clickhouse-client --password bench -q "$1"; }

note "arm cells: native (1 consumer), native-c2, native-c4"
diag_start consumers
rm -f /tmp/sweep.done
profile_watch &
watch_pid=$!
BENCH_NULL_SINK=1 "$BENCH" run spate:native spate:native-c2 spate:native-c4 --reps 2 \
  --env "$ENV_ID" --trigger tuning >>"$LOG" 2>&1 || note "arm run failed; see ladder.log"
touch /tmp/sweep.done
wait "$watch_pid" || true
diag_stop

# The sweep is only what it claims if the driver took the null path and the
# table stayed Null throughout.
grep -q "NULL SINK: sensor_events is a Null engine table" "$LOG" \
  && note "driver took the null-sink path" || note "driver did NOT take the null-sink path"
note "sensor_events engine at the end: $(chq "SELECT engine FROM system.tables WHERE name = 'sensor_events'" || echo unreadable)"
note "rows counted by the view in the last rep: $(chq "SELECT sum(n) FROM sensor_events_rows" || echo unreadable)"

note "session done; the payload returns and the box terminates"
