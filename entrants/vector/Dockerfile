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
# Stage 1 — bake THE schema into the config.
#
# Vector has no Schema Registry client: its avro decoder takes a static inline
# schema. methodology/ forbids re-declaring the schema by hand, so the inline
# copy is manufactured HERE from the committed workload/schema/sensor_batch.avsc
# — it cannot drift from the schema every other arm fetches from the registry,
# because there is no second copy to drift. The substitution asserts the
# placeholder exists first: a template that silently stopped containing it would
# otherwise ship a config whose schema is the literal string
# `__AVRO_SCHEMA_JSON__`, discovered only when the container starts.
#
# json.dumps of the minified schema text produces a double-quoted, escaped
# scalar, which is valid YAML — no YAML-aware templating needed.
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS gen

WORKDIR /gen
COPY workload/schema/sensor_batch.avsc entrants/vector/vector.yaml.tmpl ./

RUN python3 - <<'PY'
import json

schema = json.dumps(json.load(open("sensor_batch.avsc")), separators=(",", ":"))
tmpl = open("vector.yaml.tmpl").read()
# The full `schema:` line, not the bare token, so the template's comments may
# NAME the placeholder without being substituted themselves.
needle = "schema: __AVRO_SCHEMA_JSON__"
assert needle in tmpl, "vector.yaml.tmpl lost its schema placeholder"
base = tmpl.replace(needle, f"schema: {json.dumps(schema)}")

# One config per variant. The arrow-only region carries `batch_encoding`, which
# Vector refuses alongside any format but `arrow_stream` — and whose ABSENCE
# under `format: arrow_stream` is accepted and silently emits JSONEachRow. Both
# halves of that trap are closed by generating the pair here: neither config can
# name a format its encoder does not produce.
BEGIN, END = "# >>> arrow-only\n", "# <<< arrow-only\n"
assert BEGIN in base and END in base, "vector.yaml.tmpl lost its arrow-only markers"
start, stop = base.index(BEGIN), base.index(END) + len(END)
assert start < stop, "arrow-only markers are out of order"

arrow = base.replace(BEGIN, "").replace(END, "").replace("__FORMAT__", "arrow_stream")
json_each_row = (base[:start] + base[stop:]).replace("__FORMAT__", "json_each_row")
for name, text in (("vector-arrow.yaml", arrow), ("vector-json.yaml", json_each_row)):
    assert "__FORMAT__" not in text and "arrow-only" not in text, name
    open(name, "w").write(text)
assert "batch_encoding" in arrow, "the arrow config lost its encoder"
assert "batch_encoding" not in json_each_row, "the json config kept the arrow encoder"
PY

# ---------------------------------------------------------------------------
# Stage 2 — the runtime image.
#
# 0.57.0 pinned: the current release at activation, and the exact binary every
# published number describes — `[version].pinned` in entrant.toml refuses the
# run if the resolved version ever disagrees. (The clickhouse sink's `format`
# option — the ArrowStream / JSONEachRow axis this arm publishes — shipped
# earlier, in 0.53 via vector#24373; what 0.57 specifically brings is the
# env-var interpolation gate handled below.) The -debian variant rather than
# -distroless-static: glibc, plus a shell for the descriptor's version command.
# ---------------------------------------------------------------------------
FROM timberio/vector:0.57.0-debian

COPY --from=gen /gen/vector-arrow.yaml /etc/vector/vector-arrow.yaml
COPY --from=gen /gen/vector-json.yaml /etc/vector/vector-json.yaml
COPY entrants/vector/transform.vrl /etc/vector/transform.vrl
COPY entrants/vector/entrypoint.sh /usr/local/bin/entrypoint.sh

# Defaults target the bench network; the driver overrides what it varies. The
# knob defaults are kept EQUAL to the descriptor's published knobs, so a
# reviewer who starts the container by hand gets the configuration the numbers
# describe (rule 7 sends this image's configuration upstream).
#
# VECTOR_DANGEROUSLY_ALLOW_ENV_VAR_INTERPOLATION gates a 0.57 breaking change:
# without it, Vector leaves `${...}` in config files as literal text — the `:-`
# machinery is part of the interpolator, so the defaults are NOT applied — and
# the numeric fields then fail config deserialization, which the `vector
# validate` below turns into a build failure rather than a runtime surprise.
# The flag is what makes the knobs knobs. The name is upstream's, not a comment
# on this use: the values interpolated here are the driver's own knobs, not
# secrets.
#
# VECTOR_LOG=info is what makes a stalled arm diagnosable: the two things worth
# reading when nothing moves — librdkafka's assignment lifecycle and the sink's
# start-up, where arrow_stream fetches the table schema from system.columns —
# both log at INFO, while per-request detail is DEBUG/TRACE, so the hot path
# stays quiet. Narrow with a directive (`warn,vector::sinks=info`) if INFO ever
# costs a number; a level that silences a stall is not the remedy. No metrics
# exporter is configured, because no published figure comes from an arm's
# self-report.
ENV VECTOR_DANGEROUSLY_ALLOW_ENV_VAR_INTERPOLATION=true \
    VECTOR_LOG=info \
    BOOTSTRAP=spate-bench-redpanda:29092 \
    CLICKHOUSE_URL=http://spate-bench-clickhouse:8123 \
    TOPIC=comparison-sensor-batches \
    GROUP_ID=comparison-vector \
    AUTO_OFFSET_RESET=earliest \
    FORMAT=arrow_stream \
    VECTOR_THREADS=32 \
    SINK_BATCH_EVENTS=262144 \
    SINK_BATCH_TIMEOUT_SECS=1 \
    SINK_REQUEST_CONCURRENCY=32 \
    SINK_BUFFER_EVENTS=524288 \
    SINK_COMPRESSION=none

# Build-time drift guard: parses each baked YAML (including the alias-expanded
# eight-source topology), compiles transform.vrl, and rejects unknown keys —
# against the exact binary that will run it. `--no-environment` skips the
# health checks, because there is no Kafka or ClickHouse at build time.
#
# It does NOT prove the format/encoder pairing: `vector validate` accepts both a
# format with no encoder and an encoder with the wrong format, and only the
# topology builder refuses the second. Generating the two configs from one
# marked region is what makes the pairing hold; this checks everything else.
RUN chmod +x /usr/local/bin/entrypoint.sh \
    && vector validate --no-environment /etc/vector/vector-arrow.yaml \
    && vector validate --no-environment /etc/vector/vector-json.yaml \
    && grep -q 'FORMAT ArrowStream\|arrow_stream' /etc/vector/vector-arrow.yaml \
    && grep -q batch_encoding /etc/vector/vector-arrow.yaml \
    && ! grep -q batch_encoding /etc/vector/vector-json.yaml

# The variant's config is chosen by name rather than by a field, so `vector` is
# invoked with an explicit `--config` and the base image's default path is no
# longer what runs. `vector` is still resolved from PATH, so a base-image
# relocation of the binary cannot diverge from a path hardcoded here.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
