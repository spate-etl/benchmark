# The Apache Flink arm

Kafka → Confluent-framed Avro → `flatMap` → ClickHouse, held to
[the fairness contract](../../methodology/), which is normative. Read that first;
this file records only what is specific to Flink.

Delivery is **at-least-once**, with 5 s `AT_LEAST_ONCE` checkpoints to a shared
filesystem path. The insert format is **`RowBinaryWithNamesAndTypes`**,
uncompressed, over HTTP — forced by the ClickHouse connector's typed mode, which
ignores `setClickHouseFormat`. It is row-oriented, so ClickHouse pivots every row
into columns server-side; the Spate arm publishes a `rowbinary` control number for
exactly this reason, and that control is the like-for-like comparison here.

## Configuration

Everything tunable lives in [`config.yaml`](config.yaml) or in the sink's
env-driven batch settings, and nothing tunable lives in the Java: a reviewer
should be able to read the whole tuning surface without decompiling a jar.

### Knobs the driver sets per run

These are the descriptor's knobs. The record carries what was in force, and that
record is authoritative — `config.yaml`'s copies are the image's defaults, kept
equal to the published values so a hand-run container matches the numbers.

| Knob | Value | Reaches Flink as | What it controls |
|---|---|---|---|
| `parallelism` | **32** | `FLINK_PROPERTIES`, `TASK_MANAGER_NUMBER_OF_TASK_SLOTS` | Subtasks, one per **partition** (32), matched to the 32-CPU data plane. The drain is paced by the busiest source subtask: any subtask owning two partitions paces the drain at half rate. `KafkaSource` gives one partition to one subtask, so a 33rd would never receive a split. |
| `max_rows` | **50,000** | `SINK_MAX_BATCH_ROWS` | Rows per INSERT, inside ClickHouse's recommended 10k–100k. The connector's own default is **500**, two orders of magnitude off. |
| `buffered_rows` | **100,000** | `SINK_MAX_BUFFERED_ROWS` | Rows a subtask may hold. Must be *strictly* greater than `max_rows` or `AsyncSinkWriter` refuses to construct; `[[constraints]]` in `entrant.toml` catches that before a container starts. |
| `inflight` | **2** | `SINK_MAX_IN_FLIGHT` | Concurrent INSERTs per subtask. The connector's default is **50**, which at parallelism 8 would permit 400 concurrent INSERTs and a corresponding flood of parts. |
| `linger_ms` | **1000** | `SINK_LINGER_MS` | Bounds latency in sustained mode. In drain mode batches fill on size first. |

These are **per subtask**, so the cross-arm quantities are the products: rows in
flight are `parallelism × inflight × max_rows` = **800,000**, and total buffered
rows are `parallelism × buffered_rows` = **800,000**. The arm holds `parallelism`
copies of every buffer, and `buffered_rows` also bounds checkpoint state, because
`AsyncSinkWriter.snapshotState` returns the buffered request entries.

`EXPECT_PARALLELISM` is set alongside and is an **assertion, not a setting**:
`ComparisonJob.assertParallelism` compares it against the parallelism the cluster
resolved and refuses the job if they differ, so a run at the wrong width fails
instead of producing a plausible number.

Two image defaults are not knobs and are worth knowing: `SINK_MAX_BATCH_BYTES` is
16 MiB and binds above roughly 150,000 rows per batch, so it would cap `max_rows`
before `max_rows` does; `SINK_MAX_ROW_BYTES` is 1 MiB and must be no larger.

### Values fixed in `config.yaml`

| Key | Value | Why |
|---|---|---|
| `taskmanager.memory.process.size` | `86016m` | Total process memory in the 96 GiB container, 12 GiB slack (limit/8). `entrants_are_valid` refuses anything below `86016m`, so this is partly a floor the harness imposes. |
| `taskmanager.memory.managed.fraction` | **`0.0`** | The default `0.4` *reserves* 5.7 GiB whether or not anything uses it, and a stateless job on the `hashmap` backend uses none. |
| `taskmanager.memory.task.off-heap.size` | `128m` | Headroom for the sink's socket I/O, so a burst is backpressure rather than `OutOfMemoryError: Direct buffer memory`. |
| `jobmanager.memory.process.size` | `1920m` | 2 GiB container, 128 MiB slack. Deliberately generous: the control plane must never be what limits the arm, and its *measured* cost is what gets published. |
| `execution.checkpointing.interval` | `5s` | Matched to every other arm's durability cadence. Checkpointing is off entirely unless an interval is set. |
| `execution.checkpointing.mode` | `AT_LEAST_ONCE` | The default is `EXACTLY_ONCE`, which buys aligned barriers and buffer blocking for a guarantee no other arm provides. |
| `execution.checkpointing.storage` | `filesystem` | The default `jobmanager` storage caps state at 5 MiB and would begin failing checkpoints under load. |
| `execution.buffer-timeout.enabled` | `false` | Flush a network buffer only when it is full. In 2.x the old duration key split in two, so setting it is accepted-but-deprecated rather than effective. |
| `pipeline.object-reuse` | `true` | Chained operators hand the same reference downstream instead of copying. |
| `pipeline.operator-chaining.enabled` | `true` | Load-bearing: source, flatMap and sink writer are one chain, so no `GenericRecord` and no output row is ever serialized. |
| `state.backend.type` | `hashmap` | The only state is Kafka offsets plus the sink's buffered entries — which is also why managed memory can be zero. |

**Flink 2.x reads `config.yaml` only.** `flink-conf.yaml` was removed in 2.0 and a
file by that name is silently ignored. Our copy *replaces* the image's, so it must
carry forward `env.java.opts.all`; the Dockerfile diffs it against the base image
and fails the build on drift.

[`log4j-console.properties`](log4j-console.properties) keeps the root logger at
`INFO` and turns the ClickHouse, Kafka and Avro loggers down to `WARN` — the Kafka
consumer logs its full resolved configuration per subtask, and the ClickHouse
writer logs three lines per submitted batch. `CH_LOG_LEVEL=INFO` puts the
ClickHouse lines back for debugging.

GC is G1, the Java 17 / Flink 2.x default. `-Xlog:gc*` writes
`/opt/flink/log/gc.log` and `gc-jm.log` for the driver to read.

## Build

```sh
bench build flink
```

By hand — the build context is the **repository root**, uniformly for every
entrant, because the build needs `entrants/flink/` and `workload/schema/` and
nothing else:

```sh
docker build -f entrants/flink/Dockerfile -t spate-bench-flink .
```

## Run

```sh
bench run flink --reps 3
```

By hand, which is what a reviewer runs to look inside the container. Flink splits
across two containers: the TaskManager gets the full 32 CPU / 96 GiB data-plane
envelope, and the JobManager's 1 CPU / 2 GiB is allocated **on top** as control
plane, with its measured consumption published beside the arm's total rather than
pre-charged against it.

```sh
# Shared checkpoint storage. Both halves mount it, so recovery is real rather
# than nominal. The name is the one declared in `[volumes].named`.
docker volume create spate-bench-flink-cp

# JobManager. `standalone-job` is Application Mode — it runs the job's main() and
# submits it, so there is no separate `flink run` step.
docker run -d --name spate-bench-flink-jm --network spate-bench-net \
  --cpus 1 --memory 2g --memory-swap 2g \
  -e JOB_MANAGER_RPC_ADDRESS=spate-bench-flink-jm \
  -v spate-bench-flink-cp:/opt/flink/checkpoints \
  -p 18085:8081 \
  spate-bench-flink standalone-job

# TaskManager: the full data-plane envelope.
docker run -d --name spate-bench-flink-tm --network spate-bench-net \
  --cpus 32 --memory 96g --memory-swap 96g \
  -e JOB_MANAGER_RPC_ADDRESS=spate-bench-flink-jm \
  -v spate-bench-flink-cp:/opt/flink/checkpoints \
  spate-bench-flink taskmanager
```

`--memory-swap` equals `--memory` on both, so memory pressure surfaces instead of
hiding in a swapfile. The job never terminates itself: the source is unbounded and
the driver removes the containers.

**That recipe runs the image's defaults.** The driver additionally sets the seven
variables the knob table names above, and those are what a published record's
knobs mean.

A session cluster works too (`jobmanager` instead of `standalone-job`, then
`flink run /opt/flink/usrlib/comparison-flink.jar`).

## Versions

| Component | Coordinate / image | Version |
|---|---|---|
| Flink runtime | `flink:2.2.1-java17` (digest `sha256:3d050f35…8f1c`) | 2.2.1 |
| JVM | Temurin (image default) | 17.0.19+10 |
| Build JDK | `maven:3.9-eclipse-temurin-17` | 17 |
| Kafka connector | `org.apache.flink:flink-connector-kafka` | `5.0.0-2.2` |
| Kafka client | `org.apache.kafka:kafka-clients` (transitive) | 4.2.0 |
| Avro format | `org.apache.flink:flink-avro` | 2.2.1 |
| Confluent registry format | `org.apache.flink:flink-avro-confluent-registry` | 2.2.1 |
| Avro | `org.apache.avro:avro` (transitive) | 1.11.4 |
| Schema Registry client | `io.confluent:kafka-schema-registry-client` (transitive) | 7.5.3 |
| ClickHouse sink | `com.clickhouse.flink:flink-connector-clickhouse-2.0.0`, classifier `all` | 0.2.0 |

The full resolved graph is baked into the image at
`/opt/flink/usrlib/dependencies.txt`, because Maven has no lockfile and the
resolved graph is the only thing a later re-run can be compared against. The job
jar is ~38 MB.

Three coordinate traps worth knowing. `2.0.0` is part of the ClickHouse
connector's **artifactId** — it names the Flink minor the artifact targets — and
the connector's own version is `0.2.0`. That artifact is `pom`-packaged with a
single `all`-classifier jar, so the dependency needs
`<classifier>all</classifier>` or Maven resolves only the pom and the build fails
with `NoClassDefFoundError` at submission rather than at resolution. And
`flink-avro-confluent-registry` pulls
`io.confluent:kafka-schema-registry-client`, which is **not on Maven Central** —
`pom.xml` declares `https://packages.confluent.io/maven/`, exactly as Flink's own
pom does.

## Differences worth knowing

- **The JobManager is allocated on top of the data-plane envelope.** Charging a
  whole coordinator against a single TaskManager is an artefact of running one
  TaskManager; in production one JobManager serves a cluster. Its measured cost is
  0.07–0.11 cores and about 780 MB of anonymous memory, against a 1344m heap whose
  peak live set is roughly 25 MiB. Both figures are published, so a reader who
  prefers the stricter rule can apply it.
- **The sink batch knobs are per subtask**, so comparing this arm's `max_rows`
  against a single-process arm's is not comparing the same quantity. The products
  are above.
- **This arm cannot choose its wire format.** `RowBinaryWithNamesAndTypes` is
  forced by the connector's typed mode because the Java client has no Native
  writer ([clickhouse-java#2509](https://github.com/ClickHouse/clickhouse-java/issues/2509),
  open). That is a gap in the Java client rather than a Flink deficiency, and it is
  not a win we claim: read this arm against Spate's `rowbinary` control.
- **It does not send `insert_deduplication_token`.** The shared DDL sets
  `non_replicated_deduplication_window = 1000`, so ClickHouse hashes this arm's
  blocks and skips hashing those of an arm that sends a token. Its duplicate count
  is reported rather than suppressed.
- **`Rows.asciiUpper` is hand-written**, because `toUpperCase(Locale.ROOT)` is
  still Unicode-aware — it maps `ß` to `SS` — and would not match the other arms'
  `to_ascii_uppercase`. Only `a-z` is folded, which is what the contract specifies.
- **`GenericRecord` string fields are converted to `java.lang.String` in the
  flatMap.** Avro yields `org.apache.avro.util.Utf8`; the connector's `DataWriter`
  would stringify it anyway, and its checkpointed payload map accepts only a fixed
  set of value types.
- **A JVM on Docker Desktop for macOS is not a JVM on Linux.** G1's heuristics
  react to a vCPU count the hypervisor maps non-deterministically across
  performance and efficiency cores, so this is the arm most likely to improve on
  bare metal. JIT warm-up is inside the measured window and a full drain takes
  about 49 seconds, so this arm's published rate is a **floor**.

## Two verified traps

`DataWriter.writeDateTime64` accepts only `LocalDateTime` or `ZonedDateTime` and
serializes with a hardcoded `ZoneId.of("UTC")`; a bare `Long` in the payload map
throws, and a `LocalDateTime` built in any other zone lands offset.
`SensorBatchSchema.fromEpochMillis` / `fromEpochMicros` build UTC
`LocalDateTime`s, and a standalone probe against a live ClickHouse confirmed exact
round-trip of `DateTime64(3)` and `DateTime64(6)`, `Nullable(Float64)` nulls,
`LowCardinality(String)` and `Array(LowCardinality(String))`.

The connector's own `numRecordsSend` and `numBytesSend` **over-report by 2×**: the
counter is incremented inside the client's request-body callback, which runs twice
per HTTP request. No row is inserted twice — `numRequestSubmitted` and
`system.query_log`'s `written_rows` both agree with one insert per batch. This
comparison reads no framework metric for any published figure, but anyone else
reading those counters would be misled.
