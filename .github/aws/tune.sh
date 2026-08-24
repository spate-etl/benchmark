#!/usr/bin/env bash
# The tuning box's payload: bring the infrastructure up, prefill it, prove the
# storage layout, and hold the box open for the session that drives the search.
#
# The search itself is not here yet, and that is deliberate rather than
# unfinished. It has unknowns a script cannot answer in advance — how long a
# 12M-batch prefill takes on this host, whether an NVMe-backed broker keeps the
# corpus across a cap change, how Redpanda handles a shard-count change against
# an existing data directory — and a blind ladder that guesses wrong spends ten
# hours doing it. The ladder is walked interactively once, and the script that
# reproduces it lands here afterwards.
#
# Nothing this box does may be published. Driving the search edits
# environments/$ENV_ID.toml, which moves env_digest and infra_digest, so every
# ceiling measured here describes a profile that exists only on this box.
set -euo pipefail

: "${REPO:?}" "${ENV_ID:?}" "${S3_RUN:?}" "${TTL_HOURS:?}"

BENCH="$REPO/target/release/bench"
PREFILL_BATCHES=12000000

note() { echo "=== tune: $* ==="; }

note "host: $(nproc) cpus, $(free -g | awk '/^Mem:/{print $2}') GiB, up $(awk '{print $1}' /proc/uptime)s"
lsblk -o NAME,SIZE,MODEL,MOUNTPOINT | sed 's/^/    /'
for target in / /mnt/bench-clickhouse /mnt/bench-broker /var/lib/docker; do
  printf '    %-24s %s\n' "$target" "$(findmnt -no SOURCE --target "$target" 2>/dev/null || echo '(no mount)')"
done

# Brings the infrastructure up and refuses if the declared storage layout is not
# the one this host has, before any of the box time below is spent.
note "bringing infrastructure up"
"$BENCH" prefill --env "$ENV_ID" --batches "$PREFILL_BATCHES"
note "prefill of $PREFILL_BATCHES batches complete at $(awk '{print $1}' /proc/uptime)s uptime"

"$BENCH" ceiling --env "$ENV_ID" || true

cat <<'EOF'

=== the box is now held open for a tuning session ===

  aws ssm start-session --target <instance-id> --region eu-west-2

  sudo -i; cd /opt/bench
  bench=target/release/bench

The corpus is prefilled and the infrastructure is up. To move a cap, edit
environments/<env>.toml and re-run `$bench ceiling --measure --write --env <env>`;
`infra::bring_up` recreates only the container whose cap moved.

Changing the partition count needs the topic dropped and the corpus refilled:
  docker exec spate-bench-redpanda rpk topic delete comparison-sensor-batches

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
