# The Apache Flink arm of the cross-framework comparison.
#
# Build with the REPOSITORY ROOT as context, which every entrant does uniformly —
# the context needs `entrants/flink/` and `workload/schema/`. `.dockerignore`
# keeps `results/` and `website/` out of the upload.
#
#   bench build flink
#
# or by hand:
#
#   docker build -f entrants/flink/Dockerfile -t spate-bench-flink .
#
# The resulting image is startable as either half of a standalone cluster:
#
#   docker run ... spate-bench-flink jobmanager        # session cluster
#   docker run ... spate-bench-flink standalone-job    # application mode
#   docker run ... spate-bench-flink taskmanager
#
# README.md carries the full run recipe including the cgroup caps.

# ---------------------------------------------------------------------------
# Stage 1 — build the job jar.
#
# In a maven container on purpose: there is no Maven on the benchmark host and
# installing one would put the build's provenance outside the image. Temurin 17
# because Flink 2.x supports 11/17/21, 17 is what the runtime image ships, and 21
# is still flagged experimental for Flink — compiling on a JDK the runtime does
# not use is how you get a fresh class-file-version failure at submission time.
# ---------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build
COPY entrants/flink/pom.xml ./pom.xml
COPY entrants/flink/src ./src

# THE one schema. methodology/ forbids re-declaring it inline, so the
# committed .avsc is copied into the jar's resources and SensorBatchSchema reads
# it from the classpath. `assertFieldOrder` then proves the positional field
# constants still match it, at job submission and again at operator open.
COPY workload/schema/sensor_batch.avsc ./src/main/resources/sensor_batch.avsc

# The cache mount makes an incremental rebuild seconds rather than minutes.
# Because a cache mount is not part of the resulting layer, anything that must
# survive has to be written under /build in the same RUN.
#
# dependency:tree is recorded into the image: Maven has no lockfile, so the exact
# resolved graph is the only provenance a later re-run can be compared against.
# The plugin is fully qualified and version-pinned because a `dependency:tree`
# prefix lookup resolves the plugin's own version from the network, which is one
# more thing that can move between two builds of the same source.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp -DskipTests package \
    && mvn -B -ntp org.apache.maven.plugins:maven-dependency-plugin:3.8.1:tree \
       -DoutputFile=/build/dependencies.txt \
    && test -s target/comparison-flink.jar

# ---------------------------------------------------------------------------
# Stage 2 — the runtime image.
#
# Flink 2.2.1, not 2.3: no flink-connector-kafka release lists Flink 2.3 as
# compatible (the published matrix is connector 5.0.0 -> Flink 2.1.x/2.2.x), so a
# 2.3 base would mean running the Kafka connector outside its supported range in
# a benchmark whose whole point is that the configuration is defensible.
# ---------------------------------------------------------------------------
FROM flink:2.2.1-java17

USER root

# usrlib is the user-code classpath both `standalone-job` and a submitted job
# pick up. checkpoints/ is the local mount point for
# execution.checkpointing.dir; mount the same volume here in the JobManager and
# the TaskManager so recovery is real rather than nominal.
RUN mkdir -p /opt/flink/usrlib /opt/flink/checkpoints \
    && chown -R flink:flink /opt/flink/usrlib /opt/flink/checkpoints

COPY --from=build --chown=flink:flink /build/target/comparison-flink.jar \
     /opt/flink/usrlib/comparison-flink.jar
COPY --from=build --chown=flink:flink /build/dependencies.txt \
     /opt/flink/usrlib/dependencies.txt

# Keep the image's own config for the drift check below, then replace it. The
# entrypoint patches config.yaml in place at start-up (jobmanager.rpc.address,
# blob/query ports), so it must remain writable by the flink user.
RUN cp /opt/flink/conf/config.yaml /opt/flink/conf/config.yaml.base
COPY --chown=flink:flink entrants/flink/config.yaml /opt/flink/conf/config.yaml
COPY --chown=flink:flink entrants/flink/log4j-console.properties /opt/flink/conf/log4j-console.properties

# Our config.yaml REPLACES the image's, which means it also has to carry forward
# the entries the image needs. env.java.opts.all is the dangerous one: Flink 2.x
# on Java 17 needs those --add-opens/--add-exports, and losing them fails at
# start-up with an InaccessibleObjectException that reads like our bug. Fail the
# build on drift rather than discovering it in a benchmark run.
RUN base="$(grep -m1 -E '^[[:space:]]+all: --add-exports' /opt/flink/conf/config.yaml.base | sed 's/^[[:space:]]*all: //')" \
    && ours="$(grep -m1 -E '^[[:space:]]+all: --add-exports' /opt/flink/conf/config.yaml | sed 's/^[[:space:]]*all: //')" \
    && test -n "$base" \
    && if [ "$base" != "$ours" ]; then \
         echo "FATAL: env.java.opts.all in entrants/flink/config.yaml has drifted from flink:2.2.1-java17." >&2; \
         echo "  image: $base" >&2; \
         echo "  ours : $ours" >&2; \
         exit 1; \
       fi \
    && echo "env.java.opts.all matches the base image"

USER flink

# Defaults target the bench network; the driver overrides what it varies.
#
# "What it varies" is now every sink knob and the parallelism, which is what
# METHODOLOGY rule 2 requires and what this list previously prevented:
# SINK_MAX_BUFFERED_ROWS was declared here and nowhere in `entrant.toml`'s
# `[env]`, so the driver could not reach it, and ComparisonJob's
# batch-must-be-smaller-than-buffer rule then capped the insert batch at under
# 50,000 rows. A value that only exists here is an image default for a hand-run
# container; it is not a knob.
#
# The sink defaults are kept EQUAL to the descriptor's published knobs. Rule 7
# sends this image's configuration to upstream maintainers, and a reviewer who
# starts the container by hand should get the configuration the published numbers
# describe rather than a stale one.
#
# SINK_MAX_BATCH_BYTES is the one sink setting that is still only an image
# default. It binds above roughly 150,000 rows per batch, so it would cap
# `max_rows` before `max_rows` does. It is well clear at the published 50,000,
# and is recorded here rather than made a knob.
#
# EXPECT_PARALLELISM must equal `parallelism.default` in config.yaml, and
# `entrants_are_valid` asserts that it does. It is the number ComparisonJob
# checks the cluster's resolved parallelism against, so a hand-run container has
# to assert the truth about itself rather than being exempt from the check.
#
# No metrics reporter is configured on purpose. Flink's own metrics and the
# ClickHouse sink's counters stay as the frameworks ship them, because they are
# production-idiomatic, but methodology/ takes every published figure
# from outside: ClickHouse for throughput and latency, cgroup v2 for CPU and
# memory, /opt/flink/log/gc.log for GC.
ENV BOOTSTRAP=spate-bench-redpanda:29092 \
    EXPECT_PARALLELISM=32 \
    TOPIC=comparison-sensor-batches \
    GROUP_ID=comparison-flink \
    STARTING_OFFSETS=earliest \
    REGISTRY_URL=http://spate-bench-redpanda:8081 \
    CLICKHOUSE_URL=http://spate-bench-clickhouse:8123 \
    CLICKHOUSE_USER=default \
    CLICKHOUSE_PASSWORD=bench \
    CLICKHOUSE_DATABASE=default \
    SINK_MAX_BATCH_ROWS=50000 \
    SINK_MAX_BUFFERED_ROWS=100000 \
    SINK_MAX_BATCH_BYTES=16777216 \
    SINK_MAX_ROW_BYTES=1048576 \
    SINK_LINGER_MS=1000 \
    SINK_MAX_IN_FLIGHT=2

# Inherited from the base image; repeated so that the two entrypoint arguments
# this arm is started with are visible without reading the base Dockerfile.
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]
