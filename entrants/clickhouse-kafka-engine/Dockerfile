# The ClickHouse Kafka table engine arm of the streaming ETL benchmark.
#
# Build with the REPOSITORY ROOT as context, uniformly with every other
# entrant:
#
#   bench build clickhouse-kafka-engine
#
# or by hand:
#
#   docker build -f entrants/clickhouse-kafka-engine/Dockerfile -t spate-bench-ch-kafka .
#
# The image is the official server plus three kinds of declarative files: a
# config.d/ overlay (remote cluster, the kafka_src named collection, sizing
# for a node that stores nothing), a users.d/ profile (the settings the
# pipeline's inserts run under), and initdb (static DDL plus an assert script
# that refuses the container if any of it failed to take effect). There is no
# custom code anywhere in this arm.
#
# The base is the SAME major as the shared storage server
# (environments/*.toml runs clickhouse/clickhouse-server:26.3): one ClickHouse
# version in the provenance, and the consumer is not tuned on a newer codebase
# than its own storage tier. Digest-pinned so a re-pushed tag cannot silently
# move what a published number ran on; [version].pinned in entrant.toml
# additionally asserts the version string the image reports.
FROM clickhouse/clickhouse-server:26.3@sha256:85c434814ac8905e5648027ce926f74ab067edd6aadbccb6c0c165cd3571ea49

# config.d and users.d are merged over the shipped config by ClickHouse's own
# preprocessor; initdb runs once against a fresh data directory, which every
# harness-started container has. --chmod on initdb: the entrypoint EXECUTES
# *.sh only if the executable bit survived the checkout, and a sourced script
# is a subtly different contract than an executed one.
COPY entrants/clickhouse-kafka-engine/config.d/ /etc/clickhouse-server/config.d/
COPY entrants/clickhouse-kafka-engine/users.d/ /etc/clickhouse-server/users.d/
COPY --chmod=0755 entrants/clickhouse-kafka-engine/initdb/ /docker-entrypoint-initdb.d/

# Defaults equal the published knob values and target the bench network, so a
# hand-run container matches a DRAIN-mode run's configuration (sustained runs
# consume a different topic from `latest`, both set by the driver); the driver
# overrides what it varies. Each variable lands via from_env in config.d/ and
# users.d/, and initdb/20_assert.sh proves it landed.
ENV BOOTSTRAP=spate-bench-redpanda:29092 \
    REGISTRY_URL=http://spate-bench-redpanda:8081 \
    TOPIC=comparison-sensor-batches \
    GROUP_ID=ch-kafka-engine \
    OFFSET_RESET=earliest \
    KAFKA_NUM_CONSUMERS=32 \
    KAFKA_MAX_BLOCK_MSGS=16384 \
    KAFKA_FLUSH_MS=5000 \
    KAFKA_POLL_TIMEOUT_MS=500

# Entrypoint and CMD are the base image's: the official entrypoint is what
# runs initdb and then execs the server.
