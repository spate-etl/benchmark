#!/bin/bash
# Config glue only — this script reads back what the config claims and exits
# non-zero on any mismatch, which kills the official entrypoint (it runs
# initdb under `set -eo pipefail`) and therefore REFUSES the container start.
# It is not pipeline code: everything it checks was set declaratively in
# config.d/ and users.d/, and the pipeline itself is the three objects
# initdb/10_ddl.sql creates.
#
# Why it exists: every failure below is otherwise SILENT. An env var that
# from_env never carried leaves the inline default in force; a renamed
# setting leaves the shipped default in force; a mistyped cluster host
# forwards into a connection-refused loop — and each produces a plausible,
# wrong number instead of an error. The convention across this benchmark is
# that settings are asserted to have taken effect, not assumed to.
#
# Runs against the entrypoint's init server (localhost-only, same config the
# real server then starts with), as user `default` — the same user the
# pipeline's objects belong to, so the session settings read back here are
# the pipeline's.
set -eu

ch() { clickhouse-client --host 127.0.0.1 --query "$1"; }

fail=0
expect() { # expect <description> <query> <want>
    # The assignment sits in an `if !` condition so that a FAILING query — a
    # denied system table, a malformed cluster — reports through the same
    # accumulate-and-REFUSE path as a wrong value, instead of set -e killing
    # the script before it can say which check died.
    if ! got="$(ch "$2")"; then
        echo >&2 "ASSERT FAILED: $1"
        echo >&2 "  query: $2"
        echo >&2 "  (the query itself failed)"
        fail=1
        return
    fi
    if [ "$got" != "$3" ]; then
        echo >&2 "ASSERT FAILED: $1"
        echo >&2 "  query: $2"
        echo >&2 "  want:  $3"
        echo >&2 "  got:   $got"
        fail=1
    fi
}

# The guarantee-bearing session settings (see users.d/10-profile.xml).
expect "distributed_foreground_insert is on — offsets must not commit before the shared server acks" \
    "SELECT value FROM system.settings WHERE name = 'distributed_foreground_insert'" \
    "1"
expect "async_insert is off — a forwarded insert must not take the async path on the shared server" \
    "SELECT value FROM system.settings WHERE name = 'async_insert'" \
    "0"
expect "materialized_views_ignore_errors is off — the loss gate must see stall-and-replay, never a skip" \
    "SELECT value FROM system.settings WHERE name = 'materialized_views_ignore_errors'" \
    "0"

# The forward target, exactly as harness/src/infra.rs runs it. A wrong host or
# port here is a container that consumes and forwards into nowhere.
expect "bench_target is one replica: the shared infra ClickHouse, native port" \
    "SELECT host_name, toString(port) FROM system.clusters WHERE cluster = 'bench_target'" \
    "spate-bench-clickhouse	9000"

# from_env carried the registry into the AvroConfluent format setting.
expect "format_avro_schema_registry_url carries \$REGISTRY_URL" \
    "SELECT value FROM system.settings WHERE name = 'format_avro_schema_registry_url'" \
    "$REGISTRY_URL"

# from_env carried the driver's per-run identity and the knobs into the named
# collection the Kafka table reads. Existence is checked via
# system.named_collections; the VALUES are not — on 26.8 that table masks
# every value as [HIDDEN] unless the user holds SHOW NAMED COLLECTIONS
# SECRETS (verified on 26.8.25.14), and granting a secrets privilege to read
# back a topic name is the wrong trade. The values are asserted against the
# server's own preprocessed config instead: it is the artifact from_env
# substitution writes and the one the server loaded the collection from.
expect "kafka_src exists in system.named_collections" \
    "SELECT count() FROM system.named_collections WHERE name = 'kafka_src'" \
    "1"
PREPROCESSED=/var/lib/clickhouse/preprocessed_configs/config.xml
nc() { sed -n "s|.*<$1>\\([^<]*\\)</$1>.*|\\1|p" "$PREPROCESSED" | head -n 1; }
# Every [env] variable the driver sets, read back — the whole point of this
# script is that no published knob can silently run at a default.
[ "$(nc kafka_broker_list)" = "$BOOTSTRAP" ] || { echo >&2 "ASSERT FAILED: kafka_broker_list != \$BOOTSTRAP ($(nc kafka_broker_list) != $BOOTSTRAP)"; fail=1; }
[ "$(nc kafka_topic_list)" = "$TOPIC" ] || { echo >&2 "ASSERT FAILED: kafka_topic_list != \$TOPIC ($(nc kafka_topic_list) != $TOPIC)"; fail=1; }
[ "$(nc kafka_group_name)" = "$GROUP_ID" ] || { echo >&2 "ASSERT FAILED: kafka_group_name != \$GROUP_ID ($(nc kafka_group_name) != $GROUP_ID)"; fail=1; }
[ "$(nc auto_offset_reset)" = "$OFFSET_RESET" ] || { echo >&2 "ASSERT FAILED: auto_offset_reset != \$OFFSET_RESET ($(nc auto_offset_reset) != $OFFSET_RESET)"; fail=1; }
[ "$(nc kafka_num_consumers)" = "$KAFKA_NUM_CONSUMERS" ] || { echo >&2 "ASSERT FAILED: kafka_num_consumers != \$KAFKA_NUM_CONSUMERS ($(nc kafka_num_consumers) != $KAFKA_NUM_CONSUMERS)"; fail=1; }
[ "$(nc kafka_max_block_size)" = "$KAFKA_MAX_BLOCK_MSGS" ] || { echo >&2 "ASSERT FAILED: kafka_max_block_size != \$KAFKA_MAX_BLOCK_MSGS ($(nc kafka_max_block_size) != $KAFKA_MAX_BLOCK_MSGS)"; fail=1; }
[ "$(nc kafka_flush_interval_ms)" = "$KAFKA_FLUSH_MS" ] || { echo >&2 "ASSERT FAILED: kafka_flush_interval_ms != \$KAFKA_FLUSH_MS — the [guarantees] interval would not be the one in force ($(nc kafka_flush_interval_ms) != $KAFKA_FLUSH_MS)"; fail=1; }
[ "$(nc kafka_poll_timeout_ms)" = "$KAFKA_POLL_TIMEOUT_MS" ] || { echo >&2 "ASSERT FAILED: kafka_poll_timeout_ms != \$KAFKA_POLL_TIMEOUT_MS ($(nc kafka_poll_timeout_ms) != $KAFKA_POLL_TIMEOUT_MS)"; fail=1; }

# The two fixed values no correct configuration may move (issue #35153; the
# loss gate). Asserted so an edit to the XML cannot pass unnoticed.
[ "$(nc kafka_thread_per_consumer)" = "1" ] || { echo >&2 "ASSERT FAILED: kafka_thread_per_consumer must be 1 (#35153: 0 squashes all consumers into one flush thread)"; fail=1; }
[ "$(nc kafka_skip_broken_messages)" = "0" ] || { echo >&2 "ASSERT FAILED: kafka_skip_broken_messages must be 0 (a skipped message silently drops 100 rows)"; fail=1; }

# The three objects, so a partially-applied DDL cannot start a container that
# consumes without transforming or transforms without forwarding.
expect "the pipeline's three objects exist" \
    "SELECT name FROM system.tables WHERE database = 'default' AND name IN ('sensor_batches_queue', 'sensor_events_dist', 'sensor_events_mv') ORDER BY name" \
    "sensor_batches_queue
sensor_events_dist
sensor_events_mv"

if [ "$fail" -ne 0 ]; then
    echo >&2 "REFUSED: configuration did not take effect; not starting a server that would measure something else."
    exit 1
fi
echo "20_assert.sh: configuration read back and verified."
