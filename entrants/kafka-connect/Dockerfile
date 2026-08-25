# The Kafka Connect arm of the cross-framework comparison.
#
# Build with the REPOSITORY ROOT as context, uniformly with every entrant:
#
#   bench build kafka-connect
#
# or by hand:
#
#   docker build -f entrants/kafka-connect/Dockerfile -t spate-bench-kafka-connect .
#
# One image, one container: Connect STANDALONE runs worker, connector and tasks
# in a single JVM (see entrant.toml for why distributed-with-one-worker was
# rejected). README.md carries the run recipe including the cgroup caps.

# ---------------------------------------------------------------------------
# Stage 1 — resolve the Avro converter's dependency closure.
#
# Apache Kafka's Connect runtime ships no Avro converter, so this arm bundles
# io.confluent:kafka-connect-avro-converter (Apache-2.0, POM-verified; see
# converter-pom.xml for the licence analysis and the Apicurio rejection).
# In a maven container on purpose: no Maven on the benchmark host, and the
# build's provenance stays inside the image. Temurin 21 to match the runtime
# image's JRE — nothing is compiled here, but resolving on the JDK the runtime
# uses keeps the toolchain story one sentence.
# ---------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS deps

WORKDIR /build
COPY entrants/kafka-connect/converter-pom.xml ./pom.xml

# includeScope=runtime: provided-scope artifacts (connect-api) stay out,
# because the Connect runtime supplies them. kafka-clients does NOT stay out —
# it enters the converter's closure at compile scope (as 8.3.0-ccs, Confluent's
# Apache-2.0 build of the Apache client) and lands in the plugin directory.
# That is harmless by design: Connect's plugin isolation loads
# org.apache.kafka.* parent-first, so the runtime's own client classes win —
# and it is recorded in dependencies.txt like the rest of the closure.
#
# dependency:tree is recorded into the image: Maven has no lockfile, so the
# exact resolved graph is the only provenance a later re-run can be compared
# against. Both plugin invocations are fully qualified and version-pinned,
# because a prefix lookup resolves the plugin's own version from the network —
# one more thing that can move between two builds of the same source.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -ntp org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies \
      -DincludeScope=runtime -DoutputDirectory=/build/avro-converter \
    && mvn -B -ntp org.apache.maven.plugins:maven-dependency-plugin:3.8.1:tree \
      -DoutputFile=/build/dependencies.txt \
    && test -n "$(ls /build/avro-converter/kafka-connect-avro-converter-*.jar)"

# ---------------------------------------------------------------------------
# Stage 2 — fetch the connector, checksum-pinned.
#
# The GitHub release zip is the artifact ClickHouse publishes; there is no
# Maven coordinate for the -confluent packaging. A tag can be re-pushed and a
# release asset replaced, so the sha256 is pinned here and the build fails on
# any drift rather than shipping bytes nobody verified.
# ---------------------------------------------------------------------------
FROM alpine:3 AS connector

ARG CKC_VERSION=v1.4.0
# sha256 of clickhouse-kafka-connect-v1.4.0.zip, computed from the downloaded
# release asset at pin time.
ARG CKC_SHA256=e146cf1205c3a15630fcdf667556da931cb3269e63b1f3f3bedf7dfd1ffc2cb0

RUN wget -q -O /release.zip \
      "https://github.com/ClickHouse/clickhouse-kafka-connect/releases/download/${CKC_VERSION}/clickhouse-kafka-connect-${CKC_VERSION}.zip" \
    && echo "${CKC_SHA256}  /release.zip" | sha256sum -c - \
    && unzip -q /release.zip -d /unpacked \
    && mkdir -p /plugin \
    && cp /unpacked/clickhouse-kafka-connect-${CKC_VERSION}/lib/*.jar /plugin/

# ---------------------------------------------------------------------------
# Stage 3 — the runtime image.
#
# apache/kafka:4.3.1 is the ASF's own image (Temurin 21 JRE base, /opt/kafka,
# runs as appuser) — every artefact in the final image is Apache-2.0, which is
# what closed the former [planned].licence_gate. NOT a Confluent Platform
# image: those are under the Confluent Community Licence.
# ---------------------------------------------------------------------------
FROM apache/kafka:4.3.1

USER root

# plugins/: one subdirectory per plugin so each gets its own classloader.
# connect-data/: the rendered properties files and the offset storage file
# StandaloneConfig requires (unused for a sink, whose offsets go to
# __consumer_offsets). logs/: kafka-run-class.sh's LOG_DIR and the GC log's
# home — it does not exist in the base image.
RUN mkdir -p /opt/kafka/plugins/clickhouse-kafka-connect \
             /opt/kafka/plugins/avro-converter \
             /opt/kafka/connect-data \
             /opt/kafka/logs \
             /opt/connect \
    && chown -R appuser:appuser /opt/kafka/plugins /opt/kafka/connect-data \
             /opt/kafka/logs /opt/connect

COPY --from=connector --chown=appuser:appuser /plugin/*.jar \
     /opt/kafka/plugins/clickhouse-kafka-connect/
COPY --from=deps --chown=appuser:appuser /build/avro-converter/*.jar \
     /opt/kafka/plugins/avro-converter/
COPY --from=deps --chown=appuser:appuser /build/dependencies.txt \
     /opt/connect/dependencies.txt

COPY --chown=appuser:appuser entrants/kafka-connect/worker.properties.tmpl \
     entrants/kafka-connect/clickhouse-sink.properties.tmpl \
     entrants/kafka-connect/log4j2.yaml \
     entrants/kafka-connect/entrypoint.sh \
     /opt/connect/
RUN chmod +x /opt/connect/entrypoint.sh

# Drift guard for the jars [version].command parses: if a base-image bump or a
# release re-package changes either filename pattern, the build fails here
# rather than the version command silently printing an empty half and the
# driver refusing every run. EXACTLY one match each, not merely one: a second
# matching jar would embed a newline mid-token, and some shapes of that output
# round-trip parse_version as a plausible-but-wrong version instead of failing.
RUN test "$(ls /opt/kafka/libs | sed -n 's/^connect-runtime-\(.*\)\.jar$/\1/p' | wc -l)" -eq 1 \
    && test "$(ls /opt/kafka/plugins/clickhouse-kafka-connect | sed -n 's/^clickhouse-kafka-connect-\(v[0-9.]*\)-confluent\.jar$/\1/p' | wc -l)" -eq 1 \
    && test -n "$(ls /opt/kafka/plugins/avro-converter/kafka-connect-avro-converter-*.jar)"

USER appuser

# Defaults target the bench network and are kept EQUAL to the descriptor's
# published knobs: rule 7 sends this configuration upstream, and a reviewer who
# starts the container by hand should get the configuration the published
# numbers describe. The driver overrides what it varies.
#
# GROUP_ID is the CONNECTOR NAME (Connect derives the sink's consumer group as
# connect-<name>); the driver sends a fresh id per repetition so a drain
# replays from offset zero. Rerunning BY HAND needs the same freshness — see
# the README's run recipe.
ENV BOOTSTRAP=spate-bench-redpanda:29092 \
    REGISTRY_URL=http://spate-bench-redpanda:8081 \
    CLICKHOUSE_URL=http://spate-bench-clickhouse:8123 \
    CLICKHOUSE_PASSWORD=bench \
    TOPIC=comparison-sensor-batches \
    GROUP_ID=comparison-kafka-connect \
    OFFSET_RESET=earliest \
    TASKS_MAX=32 \
    BUFFER_COUNT=2000 \
    BUFFER_FLUSH_MS=1000

# JVM sizing for the 96 GiB container: 82688m heap + 3072m direct (the 32
# consumers' fetch buffers and the HTTP client's socket I/O) + 256m metaspace = 86016m,
# the same figure and the same limit/8 slack rule as the Flink TaskManager —
# `entrants_are_valid` holds this line to the declared container. Xms = Xmx so
# the heap never grows mid-measurement. The launcher's shipped G1
# (KAFKA_JVM_PERFORMANCE_OPTS in kafka-run-class.sh) is kept: rule 1, the
# system's own default collector.
ENV KAFKA_HEAP_OPTS="-Xms82688m -Xmx82688m -XX:MaxDirectMemorySize=3072m -XX:MaxMetaspaceSize=256m"

# The GC log the descriptor's gc_log names. KAFKA_OPTS, not KAFKA_GC_LOG_OPTS:
# kafka-run-class.sh assembles its own GC logging only behind the -loggc flag,
# which connect-standalone.sh never passes in any mode — KAFKA_OPTS is on the
# exec line unconditionally. Decorations (uptime,level,tags) are what
# harness/src/jvm.rs parses; filecount=0 keeps one unrotated file for the
# driver to copy out.
ENV KAFKA_OPTS="-Xlog:gc*:file=/opt/kafka/logs/gc.log:uptime,level,tags:filecount=0"

# Kafka 4.x configures log4j2 in YAML; ours is the shipped connect-log4j2.yaml
# shape with root WARN and console only (see log4j2.yaml for why).
ENV KAFKA_LOG4J_OPTS="-Dlog4j2.configurationFile=/opt/connect/log4j2.yaml"

ENTRYPOINT ["/opt/connect/entrypoint.sh"]
