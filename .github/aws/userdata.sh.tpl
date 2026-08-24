#!/bin/bash
# User-data for the benchmark box. Rendered by the launcher (in the private
# benchmark-ops repo) with envsubst,
# restricted to the ${...} names listed there — every other dollar sign in this
# file is ordinary shell and survives rendering untouched.
#
# This template is deliberately a stub: it installs just enough to report home,
# clones the repository at the APPROVED SHA, and hands over to
# .github/aws/run-bench.sh from that checkout — so the logic that matters is
# versioned and reviewed like any other change, and what runs is what the
# approver saw. The box holds no GitHub credential (the repository is public)
# and its only AWS power is PutObject into incoming/ on one bucket.
#
# The trap is the box's promise: whatever happens — clone failure, build
# failure, timeout, refused run — the logs land in S3 and the machine shuts
# down. Shutdown terminates (the launcher sets
# instance-initiated-shutdown-behavior=terminate) and termination deletes the
# volume (DeleteOnTermination), so the steady state is always "nothing running,
# nothing billed".
set -euo pipefail
# Line-buffer the tee: without stdbuf it full-buffers to the log file, so the
# EXIT trap can upload the log while the very lines that explain a failure are
# still sitting in the pipe's buffer, unwritten. Line buffering flushes each
# line as it is produced, so the uploaded log is complete up to the last line.
exec > >(stdbuf -oL tee -a /var/log/bench-userdata.log) 2>&1

export RUN_ID='${RUN_ID}'
export SHA='${SHA}'
export ENV_ID='${ENV_ID}'
export SELECTOR='${SELECTOR}'
export REPS='${REPS}'
export TRIGGER='${TRIGGER}'
export MODE='${MODE}'
export BUCKET='${BUCKET}'
export TTL_HOURS='${TTL_HOURS}'
export AWS_DEFAULT_REGION='${AWS_REGION}'
# cloud-init runs user-data with HOME unset. rustup's env file, cargo and the
# docker CLI all dereference it, and run-bench.sh runs under `set -u` — an
# unset HOME aborts the payload one step after install-rust.
export HOME=/root

finish() {
  status=$?
  set +e
  if [ ! -f /run/bench-complete ]; then
    printf '{"run_id":"%s","status":"failed","exit_code":%d}\n' "$RUN_ID" "$status" \
      > /tmp/_FAILED.json
    aws s3 cp /tmp/_FAILED.json "s3://$BUCKET/incoming/$RUN_ID/_FAILED.json"
  fi
  aws s3 cp /var/log/bench-userdata.log "s3://$BUCKET/incoming/$RUN_ID/logs/userdata.log"
  if [ -f /var/log/cloud-init-output.log ]; then
    aws s3 cp /var/log/cloud-init-output.log "s3://$BUCKET/incoming/$RUN_ID/logs/cloud-init-output.log"
  fi
  shutdown -h now
}
trap finish EXIT

apt-get update
apt-get install -y git xfsprogs
snap install aws-cli --classic
export PATH="$PATH:/snap/bin"

echo "boot: user-data reached $(date -u +%Y-%m-%dT%H:%M:%SZ), $(awk '{print $1}' /proc/uptime)s after power-on"

# Instance-store NVMe, if this instance type has any. Each measured path gets a
# device so the write path and the read path under test do not share a queue;
# Docker takes the third, because every arm container's writable layer is on it.
#
# Mounted before Docker is installed. The environment profile declares which
# layout it expects and the harness refuses the run if the host does not have
# it, so a type without instance store fails there rather than here.
mapfile -t stores < <(lsblk -dn -o NAME,MODEL | awk '/Instance Storage/ {print "/dev/"$1}')
echo "instance-store devices: ${stores[*]:-none}"

mount_store() {
  local device=$1 path=$2 owner=${3:-}
  [ -n "$device" ] || return 0
  mkfs.xfs -f -q "$device"
  mkdir -p "$path"
  mount -o noatime "$device" "$path"
  if [ -n "$owner" ]; then chown "$owner" "$path"; fi
  echo "mounted $device at $path"
}

# 101:101 is the uid:gid both ClickHouse and Redpanda run as in their images.
mount_store "${stores[0]:-}" /mnt/bench-clickhouse 101:101
mount_store "${stores[1]:-}" /mnt/bench-broker 101:101
mount_store "${stores[2]:-}" /var/lib/docker

git clone https://github.com/spate-etl/benchmark /opt/bench
cd /opt/bench
git checkout --detach "$SHA"

bash .github/aws/run-bench.sh
