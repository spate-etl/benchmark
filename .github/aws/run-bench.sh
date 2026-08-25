#!/usr/bin/env bash
# The benchmark box's payload. Executed by the user-data stub from a checkout
# of this repository at the approved SHA, as root, on a fresh Ubuntu 24.04
# arm64 c8gd.metal-24xl. Everything it needs arrives in the environment
# (RUN_ID/SHA/ENV_ID/SELECTOR/REPS/TRIGGER/MODE/BUCKET/TTL_HOURS); everything
# it produces leaves via s3://$BUCKET/incoming/$RUN_ID/.
#
# Three exits are possible and all of them terminate the machine:
#   - success: results (or ceilings) uploaded, _COMPLETE.json written last;
#   - failure: the user-data trap writes _FAILED.json and ships the logs;
#   - overrun: the `timeout` below kills the payload two hours before the
#     reaper's TTL would, so the box still ships its logs and self-terminates
#     rather than being reaped.
#
# MODE=tuning is the infrastructure search (.github/aws/tune.sh). It edits the
# environment profile on the box and writes nothing to results/: what it
# produces is a ladder of measurements a maintainer reads, not a published
# number.
set -euo pipefail

: "${RUN_ID:?}" "${SHA:?}" "${ENV_ID:?}" "${SELECTOR:?}" "${REPS:?}"
: "${TRIGGER:?}" "${MODE:?}" "${BUCKET:?}" "${TTL_HOURS:?}"
# The user-data stub exports HOME=/root (cloud-init leaves it unset); fail
# here in a second, not a minute into the payload where rustup's env needs it.
: "${HOME:?}"

export PATH="$PATH:/snap/bin"

# Exported because the payload runs in a child shell under `timeout`.
export REPO=/opt/bench
export S3_RUN="s3://$BUCKET/incoming/$RUN_ID"
LOG=/var/log/bench-run.log

run_step() { # name cmd...
  local name=$1 t0
  shift
  t0=$SECONDS
  echo "=== step: $name ==="
  "$@"
  echo "=== step: $name ok (${SECONDS}s total, $((SECONDS - t0))s step) ==="
}

payload() {
  # Docker CE from Docker's own apt repo — the Ubuntu archive's docker.io can
  # lag on cgroup and buildx behaviour the harness depends on.
  run_step install-docker bash -c '
    set -euo pipefail
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] \
      https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
      > /etc/apt/sources.list.d/docker.list
    apt-get update
    apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin \
      build-essential cmake pkg-config libssl-dev zlib1g-dev curl jq python3
  '

  # rustup with no default toolchain: `rustup show` inside the checkout is what
  # materialises rust-toolchain.toml's pin — the same mechanism CI uses, so the
  # compiler that builds the harness here is the one the file names.
  run_step install-rust bash -c '
    set -euo pipefail
    curl --proto "=https" --tlsv1.2 -fsSL https://sh.rustup.rs \
      | sh -s -- -y --profile minimal --default-toolchain none
  '
  # shellcheck disable=SC1091
  . "$HOME/.cargo/env"

  cd "$REPO"
  rustup show

  # Built in the clone it will operate on: `repo_root()` is a compile-time
  # constant, so a bench binary built anywhere else would resolve the wrong
  # repository root.
  run_step build-harness cargo build --release --locked \
    -p spate-benchmark-harness --bin bench
  local bench="$REPO/target/release/bench"

  # The infrastructure search. No arm images: it measures what the broker and
  # ClickHouse absorb at a ladder of caps, and none of that runs an entrant.
  # Building six of them would be most of the box time for nothing.
  if [ "$MODE" = tuning ]; then
    run_step tune bash "$REPO/.github/aws/tune.sh"
    return 0
  fi

  # The selector may be several selectors ('spate flink'); split on purpose.
  local -a sel
  read -ra sel <<< "$SELECTOR"

  run_step build-entrants "$bench" build "${sel[@]}"
  # The run-mode corpus depth (the 1.5M default) is part of what an arm
  # measures — arms replay the topic to exhaustion — so it is not touched
  # here. The ceiling pass is different: its corpus is fuel for a measurement
  # window, and the consume pass REFUSES (DRAINED) when the backlog cannot
  # outlast the window it sized (seen on the first c8g bootstrap: 1.5M
  # messages was ~6s of backlog at the calibrated rate). Prefill deep enough
  # to feed an 8s window at rates well above the calibrated one.
  if [ "$MODE" = ceiling-bootstrap ]; then
    run_step prefill "$bench" prefill --env "$ENV_ID" --batches 30000000
  else
    run_step prefill "$bench" prefill --env "$ENV_ID"
  fi

  if [ "$MODE" = ceiling-bootstrap ]; then
    run_step ceiling-measure "$bench" ceiling --measure --write --env "$ENV_ID"
    aws s3 cp "environments/ceilings/$ENV_ID.json" "$S3_RUN/ceilings/$ENV_ID.json"
  else
    # The gate check first, separately: exit 3 is REFUSED, and a refusal
    # message in the log beats discovering it per-arm mid-sweep.
    run_step ceiling-gate "$bench" ceiling --env "$ENV_ID"
    run_step run "$bench" run "${sel[@]}" --reps "$REPS" \
      --env "$ENV_ID" --trigger "$TRIGGER"

    # Upload only the APPENDED lines of each results file. `bench run` only
    # ever appends — no code path truncates a results file — so everything past
    # the committed line count is exactly this run's records. The collector
    # appends them verbatim; `merge=union` and run_id uniqueness make that safe.
    #
    # The committed line count is taken with `git cat-file -e` + `awk 'END'`
    # rather than `git show | wc -l || echo 0`: under the `set -o pipefail` this
    # function now runs with, a missing blob makes `git show` fail the pipe and
    # the `|| echo 0` would emit a SECOND zero, corrupting the count. `awk` also
    # counts a final line that lacks a trailing newline, so the last committed
    # record is not re-uploaded as a duplicate.
    # -uall, or an environment's FIRST results land as one untracked-directory
    # entry (`?? results/<env>/`) whose `tail` fails and kills the upload under
    # `set -e` — the first c8g sweep measured 24 records and lost every one of
    # them to exactly that. -uall lists untracked files individually, and a
    # file absent from HEAD uploads whole via the old=0 branch below.
    git status --porcelain -uall -- results/ | while read -r _ f; do
      if git cat-file -e "HEAD:$f" 2>/dev/null; then
        old=$(git show "HEAD:$f" | awk 'END { print NR + 0 }')
      else
        old=0
      fi
      tail -n +"$((old + 1))" "$f" > /tmp/appended.jsonl
      if [ -s /tmp/appended.jsonl ]; then
        aws s3 cp /tmp/appended.jsonl "$S3_RUN/results/$f"
      fi
    done
  fi
}

overall_rc=0
export -f payload run_step
# `bash -c` starts a fresh shell that does NOT inherit this script's `set -euo
# pipefail`, and shell options are not carried across the process boundary by
# `export -f`. Re-arm them inside the child before calling payload, so a failed
# step (docker install, harness build, a refused ceiling gate — exit 3) aborts
# with a non-zero status instead of marching on and reporting the run complete.
# The outer shell's pipefail then propagates that status through the `tee` pipe
# into overall_rc.
timeout --signal=TERM --kill-after=5m "$(( (TTL_HOURS - 2) * 3600 ))s" \
  bash -c 'set -euo pipefail; payload' 2>&1 | tee -a "$LOG" || overall_rc=$?

aws s3 cp "$LOG" "$S3_RUN/logs/bench-run.log" || true

if [ "$overall_rc" -eq 0 ]; then
  # The completion marker is what the collector polls for; written last, after
  # every artefact it describes is already uploaded.
  python3 - "$RUN_ID" "$MODE" "$SELECTOR" "$ENV_ID" "$TRIGGER" "$REPS" "$SHA" <<'PY' > /tmp/_COMPLETE.json
import json, sys, urllib.request

def imds(path):
    try:
        tok = urllib.request.urlopen(urllib.request.Request(
            "http://169.254.169.254/latest/api/token", method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}), timeout=2).read().decode()
        return urllib.request.urlopen(urllib.request.Request(
            f"http://169.254.169.254/latest/{path}",
            headers={"X-aws-ec2-metadata-token": tok}), timeout=2).read().decode()
    except Exception:
        return "unknown"

print(json.dumps({
    "run_id": sys.argv[1], "status": "complete", "mode": sys.argv[2],
    "selector": sys.argv[3], "env_id": sys.argv[4], "trigger": sys.argv[5],
    "reps": sys.argv[6], "sha": sys.argv[7],
    "instance_type": imds("meta-data/instance-type"),
    "instance_id": imds("meta-data/instance-id"),
    "ami_id": imds("meta-data/ami-id"),
}, indent=2))
PY
  aws s3 cp /tmp/_COMPLETE.json "$S3_RUN/_COMPLETE.json"
  touch /run/bench-complete
fi

exit "$overall_rc"
