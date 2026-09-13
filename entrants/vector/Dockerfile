# The Vector arm of the cross-framework comparison.
#
# Build with the REPOSITORY ROOT as context, which every entrant does uniformly —
# the context needs `entrants/vector/` and `workload/schema/`.
#
#   bench build vector
#
# or by hand:
#
#   docker build -f entrants/vector/Dockerfile -t spate-bench-vector .
#
# README.md carries the full run recipe including the cgroup caps.

# ---------------------------------------------------------------------------
# Stage 1 — generate the configs.
#
# The inline Avro schema is manufactured here from the committed
# workload/schema/sensor_batch.avsc: Vector has no Schema Registry client, and
# methodology/ forbids re-declaring the schema by hand, so there is no second
# copy to drift. Each substitution asserts its placeholder exists first.
#
# One config per source count: the count cannot be a runtime placeholder because
# YAML has no loop.
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS gen

WORKDIR /gen
COPY workload/schema/sensor_batch.avsc entrants/vector/vector.yaml.tmpl ./

RUN python3 - <<'PY'
import json

SOURCE_COUNTS = (1, 2, 4, 8, 16, 32)

schema = json.dumps(json.load(open("sensor_batch.avsc")), separators=(",", ":"))
tmpl = open("vector.yaml.tmpl").read()
# The full `schema:` line, not the bare token, so the template's comments may
# name the placeholder without being substituted themselves.
needle = "schema: __AVRO_SCHEMA_JSON__"
assert needle in tmpl, "vector.yaml.tmpl lost its schema placeholder"
base = tmpl.replace(needle, f"schema: {json.dumps(schema)}")

def region(text, begin, end):
    assert begin in text and end in text, f"vector.yaml.tmpl lost its {begin!r} marker"
    start, stop = text.index(begin), text.index(end) + len(end)
    assert start < stop, f"{begin!r} markers are out of order"
    return start, stop, text[start + len(begin):text.index(end)]

def expand(text, n):
    """Repeat each per-source region n times, renumbering its component ids."""
    begin, end = "# >>> per-source\n", "# <<< per-source\n"
    while begin in text:
        start, stop, body = region(text, begin, end)
        repeated = "".join(
            body.replace("in-0", f"in-{i}").replace("shape-0", f"shape-{i}")
            for i in range(n)
        )
        text = text[:start] + repeated + text[stop:]
    assert end not in text, "unpaired per-source marker"
    return text

for n in SOURCE_COUNTS:
    text = expand(base, n)
    assert "per-source" not in text
    assert text.count("type: kafka") == n, n
    assert text.count("type: remap") == n, n
    assert text.count(f"in-{n - 1}:") == 1 and f"in-{n}:" not in text, n
    open(f"vector-s{n}.yaml", "w").write(text)
PY

# ---------------------------------------------------------------------------
# Stage 2 — the runtime image.
#
# Pinned by tag AND digest. The digest is the OCI INDEX, not the linux/arm64
# per-image digest Docker Hub's tag page shows — that one would pin the build to
# a single architecture. `[version].pinned` in entrant.toml resolves the version
# by running the image, so a repointed tag is caught independently.
#
# The -debian variant rather than -distroless-static: glibc, plus a shell for
# the descriptor's version command.
# ---------------------------------------------------------------------------
FROM timberio/vector:0.58.0-debian@sha256:1c1ea358c617ea0b23003d5af87f7a678b30f8f7096437e680380c47fc13d2d9

COPY --from=gen /gen/vector-*.yaml /etc/vector/
COPY entrants/vector/transform.vrl /etc/vector/transform.vrl
COPY entrants/vector/entrypoint.sh /usr/local/bin/entrypoint.sh

# Defaults target the bench network; the driver overrides what it varies. The
# knob defaults are kept EQUAL to the default variant's published knobs, so a
# reviewer who starts the container by hand gets the configuration the numbers
# describe — a harness test holds the three files to the same values.
#
# VECTOR_DANGEROUSLY_ALLOW_ENV_VAR_INTERPOLATION is what makes the knobs knobs:
# without it Vector leaves `${...}` as literal text, the `:-` defaults are not
# applied, and the numeric fields fail deserialization at the `vector validate`
# below. The name is upstream's; the values interpolated here are the driver's
# knobs, not secrets.
#
# VECTOR_LOG=info keeps a stalled arm diagnosable: librdkafka's assignment
# lifecycle and the sink's start-up both log at INFO while per-request detail is
# DEBUG/TRACE, so the hot path stays quiet. No metrics exporter is configured,
# because no published figure comes from an arm's self-report.
ENV VECTOR_DANGEROUSLY_ALLOW_ENV_VAR_INTERPOLATION=true \
    VECTOR_LOG=info \
    BOOTSTRAP=spate-bench-redpanda:29092 \
    CLICKHOUSE_URL=http://spate-bench-clickhouse:8123 \
    TOPIC=comparison-sensor-batches \
    GROUP_ID=comparison-vector \
    AUTO_OFFSET_RESET=earliest \
    FORMAT=json_each_row \
    SOURCES=8 \
    VECTOR_THREADS=32 \
    VECTOR_CHUNK_SIZE_EVENTS=1000 \
    SINK_BATCH_EVENTS=262144 \
    SINK_BATCH_TIMEOUT_SECS=1 \
    SINK_REQUEST_CONCURRENCY=32 \
    SINK_BUFFER_EVENTS=524288 \
    SINK_COMPRESSION=none

# Build-time drift guard: parses every generated config and compiles the VRL
# program against the exact binary that will run it. `--no-environment` skips
# the health checks, because there is no Kafka or ClickHouse at build time.
#
RUN chmod +x /usr/local/bin/entrypoint.sh \
    && for cfg in /etc/vector/vector-*.yaml; do \
         vector validate --no-environment "$cfg" || exit 1; \
       done \
    && grep -q "format: json_each_row" /etc/vector/vector-s8.yaml

# The variant's config is chosen by name rather than by a field, so `vector` is
# invoked with an explicit `--config`. `vector` is still resolved from PATH, so a
# base-image relocation of the binary cannot diverge from a path hardcoded here.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
