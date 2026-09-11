# The Apache Flink arm

Kafka → Confluent-framed Avro → flatten/filter → ClickHouse, under the
[normative fairness contract](../../methodology/README.md).

The current default is the published baseline, not a claimed tuning optimum.
The [fairness review](REVIEW.md) records evidence, experiments and remaining
questions. Documentation corrections are tracked in
[issue #74](https://github.com/spate-etl/benchmark/issues/74).

## Configuration and accounting

The driver reads `entrant.toml`; each result records the effective knobs.
`config.yaml` supplies image defaults, and the official image entrypoint applies
`FLINK_PROPERTIES` and `TASK_MANAGER_NUMBER_OF_TASK_SLOTS` before starting Java.
`EXPECT_PARALLELISM` asserts the resolved job width at submission.

| Setting | Baseline | Meaning |
|---|---|---|
| Job parallelism / slots per TaskManager | 32 / 32 | Independent settings; one TaskManager initially |
| Data-plane envelope | 32 CPUs / 96 GiB | One TaskManager under the current contract |
| JobManager envelope | 1 CPU / 2 GiB | Additional control plane; measured cost is included in arm totals |
| TaskManager process size | 21,504 MiB | Flink derives heap, direct memory, metaspace and overhead; not the cgroup limit |
| Managed-memory fraction | 0 | This pipeline uses no managed-memory operators |
| Task off-heap memory | 512 MiB | Configured separately from managed and network memory |
| Object reuse / operator chaining | enabled / enabled | Avoid copies on eligible chained edges |
| Checkpoints | 5 s, AT_LEAST_ONCE, filesystem | Shared named volume mounted on JobManager and TaskManagers |
| `max_rows` / `buffered_rows` | 50,000 / 100,000 | Per sink subtask; buffered rows must exceed batch rows |
| `inflight` / `linger_ms` | 2 / 1,000 ms | Requests per sink subtask; timer can flush below the row cap |
| `max_batch_bytes` | 16 MiB | Independent batch limit; exposed for large-batch experiments |
| `avro_mode` | generic | Optional generated specific records use the same canonical schema and shipped deserializer |

At width 32 the configured capacities are **3.2 million buffered rows plus up to
3.2 million rows in flight**. They are limits, not measured occupancy. The sink
retains a map and cached wire bytes for each row, so multiplying wire size alone
underestimates heap demand. Checkpoints serialize buffered maps; in-flight
requests must complete before the checkpoint can proceed.

The descriptor also exposes Kafka poll/fetch limits, ClickHouse connection and
network-buffer settings, and extra TaskManager JVM options. Their defaults match
the pinned libraries. JVM process sizing and collector choices are tuning
variables, but the current normative sizing guard still applies to committed
variants. Larger heaps and multiple TaskManagers are exploratory contract questions.

## Typing and serialization

The source uses Flink's `ConfluentRegistryAvroDeserializationSchema`. Generic mode
produces `GenericRecordAvroTypeInfo`, whose stream serializer is Avro, not Kryo.
The shipped decoder calls `datumReader.read(null, ...)`: pipeline object reuse
does not make that decoder reuse the previous record.

`SensorRow` is a Flink POJO. With Flink 2.2.1's automatic type extraction its two
`LocalDateTime` fields are generic types and its tags are `NullableList<String>`.
A POJO can contain a generic field. The connector's "typed/POJO mode" is a
separate concept: it consumes the supplied `DataMapper`.

Equal parallelism permits forward edges, but chaining also depends on operator
strategies, slot sharing and exchange mode. Tests verify one chain at widths
8, 16 and 32; diagnostic runs also capture the actual deployed job plan. On a
chained edge, object reuse passes references without serializer copies. Avro
input decoding, ClickHouse output encoding and checkpoint serialization still
happen. A mismatched width does not automatically select Kryo.

The transform reuses its outer row. The connector synchronously copies fields
into a new map and caches encoded bytes. Strings and timestamps are immutable;
tags are copied into a fresh list. Tests check aliasing and a connector
checkpoint round trip, including nulls and timestamp precision.

## ClickHouse batches

Typed connector mode forces uncompressed `RowBinaryWithNamesAndTypes`, with
`async_insert=0`. Spate's RowBinary control uses the closely related `RowBinary`
format, with 262,144-row batches, 32 shards, four requests per shard and 500 ms
linger. Configured limits are not actual batch sizes: compare ClickHouse's
`ch_rows_per_insert`, query logs, and merge costs.

The shipped adaptive rate limiter initially permits only one full batch's worth
of in-flight rows, even with four request slots. Its capacity increases by ten
rows per successful request. The [review](REVIEW.md) explains this constraint and
why the connector's batch-size histograms are not reliable measurements of
submitted batches in this release.

The review tests 131,072 and 262,144 rows, the Spate settings explicitly, and
larger batches when justified. It raises the byte cap and tests linger as well
as row limits. Large buffers may increase GC enough to offset fewer INSERTs.

## Build, tests and versions

```sh
bench build flink
bench run flink --reps 3
python3 .github/aws/flink-review.py --dry-run
```

The Docker build runs Java topology/transform/checkpoint tests and records the
resolved Maven dependency graph in `/opt/flink/usrlib/dependencies.txt`.

| Component | Pin |
|---|---|
| Flink | 2.2.1; exact runtime image digest recorded per run |
| Baseline Java | Temurin 17, runtime version recorded in GC logs |
| Kafka connector | 5.0.0-2.2 |
| Avro runtime/compiler | 1.11.4 |
| ClickHouse Flink connector | `flink-connector-clickhouse-2.0.0`, version 0.2.0, classifier `all` |

The ClickHouse artifact's `2.0.0` is a Flink compatibility coordinate, not its
release version. Inspect the bundled Java-client version separately.

`mode=tuning`, `selector=flink` in the ops launcher runs the bounded review
session. Every trial uses `trigger=tuning`; artifacts go to the run's S3 logs
prefix, and records remain outside `results/`. Final performance confirmation
runs without profilers. An independently declared, clean measurement is required
before publishing a chosen configuration.

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

**That recipe runs the image's defaults.** The driver additionally sets the configuration
variables in `entrant.toml`, and those are what a published record's
knobs mean.

A session cluster works too (`jobmanager` instead of `standalone-job`, then
`flink run /opt/flink/usrlib/comparison-flink.jar`).


## Versions

| Component | Coordinate / image | Version |
|---|---|---|
| Flink runtime | `flink:2.2.1-java17`; historical base digest `sha256:3d050f35…8f1c` | 2.2.1 |
| JVM | Temurin 17 baseline | archived 17.0.19+10; review 17.0.20+8 |
| Experimental runtime | `flink:2.2.1-java21` | Java 21, Generational ZGC candidate |
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
resolved graph is the only thing a later re-run can be compared against. The result
records the actual JVM version as `sut.toolchain`, the arm image digest, and
the `runtime_image` knob. The image embeds its base-image name and refuses a
mismatched knob. Historical abbreviated digests above are observations, not
immutable build pins.

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


## Additional correctness and API disclosures

- Typed mode cannot select Native output. It forces `RowBinaryWithNamesAndTypes`; compare it with Spate's RowBinary control when isolating wire-format effects (rule 5).
- This arm does not supply `insert_deduplication_token`. With the shared `non_replicated_deduplication_window = 1000`, ClickHouse hashes its blocks; Spate supplies a token. Duplicate counts and server cost remain visible.
- `Rows.asciiUpper` implements the workload's ASCII-only rule. Java's `toUpperCase(Locale.ROOT)` also folds non-ASCII text such as `ß`, so it cannot express that transform faithfully. This is application semantics, not a replacement framework decoder.
- Generic Avro strings arrive as `Utf8`; the flatten converts them to `String`. Tests now feed both generic and generated specific records through the actual flatten, converter and connector checkpoint round trip.

## Two verified traps

`DataWriter.writeDateTime64` accepts only `LocalDateTime` or `ZonedDateTime` and
serializes with a hardcoded `ZoneId.of("UTC")`; a bare `Long` in the payload map
throws, and a `LocalDateTime` built in any other zone lands offset.
`SensorBatchSchema.fromEpochMillis` / `fromEpochMicros` build UTC
`LocalDateTime`s, and a standalone probe against a live ClickHouse confirmed exact
round-trip of `DateTime64(3)` and `DateTime64(6)`, `Nullable(Float64)` nulls,
`LowCardinality(String)` and `Array(LowCardinality(String))`.

The archived connector/client probe found that `numRecordsSend` and
`numBytesSend` **over-report by 2×**: the
counter is incremented inside the client's request-body callback, which ran twice
per HTTP request in that probe. No row is inserted twice — `numRequestSubmitted` and
`system.query_log`'s `written_rows` both agree with one insert per batch. This
comparison reads no framework metric for any published figure, but anyone else
reading those counters would be misled.
