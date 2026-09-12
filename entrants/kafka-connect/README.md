# The Kafka Connect arm

Kafka → Confluent-framed Avro → `clickhouse-kafka-connect` → ClickHouse
materialized view → `sensor_events`, held to
[the fairness contract](../../methodology/), which is normative. Read that
first; this file records only what is specific to Kafka Connect.

Delivery is **at-least-once** (`exactlyOnce=false`, the connector's
`AtLeastOnceBufferStrategy`), with worker offset flushes matched to the 5 s
durability cadence every arm runs. The insert format is **RowBinary**,
uncompressed, over HTTP — verified in the v1.5.0 source
(`ClickHouseWriter.java:1066`; `RowBinaryWithDefaults` is chosen only when the
target has `DEFAULT` columns, and the landing table has none).

**The structural deviation, first.** Connect has no fan-out operator: one Kafka
record cannot become ~100 rows inside the runtime. So the connector lands the
*nested* batch into a `Null`-engine landing table and a ClickHouse materialized
view performs the flatten, both filters and both derived columns
([`clickhouse/arm.sql`](clickhouse/arm.sql), applied per repetition by the
harness DDL hook). That is a legitimate, widely-deployed real-world pattern —
and it moves the transform's CPU into the shared ClickHouse, where the cgroup
sampler cannot see it. This arm's efficiency comparison therefore leans on
ClickHouse's own `ProfileEvents` via `system.query_log` (the MV's cost rides on
the parent insert; background merges are excluded — and the landing table,
being `Null`, causes none). Declared in `[[deviations]]` in
[`entrant.toml`](entrant.toml); the site renders it beside the numbers.

## Configuration

Everything tunable lives in the two properties templates, rendered by
[`entrypoint.sh`](entrypoint.sh) from the container environment at start-up.
There is no Java in this arm at all: the pipeline is configuration plus the
materialized view's SQL, which is exactly what makes it worth measuring.

### Knobs the driver sets per run

| Knob | Value | Reaches Connect as | What it controls |
|---|---|---|---|
| `tasks` | **32** | `tasks.max` | One task per **partition** (32). A 33rd task would own no partitions; fewer leaves a task owning two partitions and pacing the drain, the same arithmetic as Flink's parallelism. |
| `buffer_count` | **2000** | `bufferCount` | Records (messages) per buffered insert. At 100 events/message that is ~200,000 landed events and ~149,000 surviving rows per insert after the MV's filters — the same order as the other arms' batch sizes, inside ClickHouse's recommended 10k–100k+ band. The connector's own default is `bufferCount=0` — buffering disabled entirely, an insert per poll — so setting it at all is the difference between batched and per-poll inserts. |
| `poll_records` | **2000** | `consumer.max.poll.records` | How much one poll delivers, and so how much one insert carries: the buffer flushes as soon as it reaches `bufferCount`. Separate from `buffer_count` because the two set the peak differently — see the sizing note below. |
| `buffer_flush_ms` | **1000** | `bufferFlushTime` | Bounds sustained-mode latency, matching Flink's 1000 ms linger. **Must be > 0 whenever `bufferCount` > 0**: the buffer flushes on size *or* time, so a zero flush time strands a sub-`bufferCount` tail and a drain never completes. Enforced by the descriptor's `[[constraints]]` floor (`at_least = 1`) — the driver refuses such a cell before a container starts; only the conditional only-when-buffering form is inexpressible, and every committed variant pins `buffer_count` > 0. |
| `client_version` | **V2** | `client_version` | V2 takes the `DataStreamWriter` path added in v1.5.0, which serializes RowBinary straight to the network stream; V1 stages the batch through a piped stream. |
| `heap_mib` | **63488** | `-Xms`/`-Xmx` | See *JVM sizing* below. |
| `jvm_opts` | *(see below)* | appended to `KAFKA_OPTS` | Extra JVM flags. `entrypoint.sh` refuses any that would set the heap or redirect the GC log. |

### JVM sizing

Each task holds `(bufferCount - 1 + poll_records)` messages live until its insert
returns — the buffer only clears afterwards — as both a Connect `Struct` and the
connector's `jsonMap` derived from it. Measured against this schema at 77,397 B
per message, that is **~9.9 GiB across 32 tasks**, or ~11.7 GiB once 16-byte
object alignment pads it.

Eden has to exceed that, or every insert's working set is promoted to the old
generation and collected there. Hence `-XX:G1NewSizePercent=40` (~25 GiB of eden)
rather than the shipped 5, and a heap large enough to carry it.

`63488m` is not a round 64 GiB because the compressed-oops boundaries are not
round. Measured on this image's own Temurin 21 (linux/arm64), reading
`[gc,init] Compressed Oops` out of `-Xlog:gc*`:

| object alignment | zero-based ≤ | compressed oops ≤ |
|---|---:|---:|
| 8 (default) | 30,720m | 32,736m |
| 16 | **63,488m** | 65,504m |

Both zero-based edges are `addressable - HeapBaseMinAddress`. Above them the JVM
pays a base add on every reference; above the right-hand column it drops
compressed oops and every reference doubles. `-XX:ObjectAlignmentInBytes=16`
costs +18.4% on retained size for this workload's small, numerous objects, which
the extra 32 GiB of heap more than absorbs.

Both `entrypoint.sh` and `entrants_are_valid` bound `heap_mib` at the zero-based
edge for whichever alignment `jvm_opts` sets.

### Values fixed in the templates

| Key | Value | Why |
|---|---|---|
| `exactlyOnce` | `false` | At-least-once, matched guarantee-for-guarantee. `true` adds state-store round-trips no other arm pays for a guarantee no other arm offers. |
| `errors.tolerance` | `none` (default kept) | A poison record fails the task loudly. `all` drops silently, and a silent drop voids the loss gate — faster for the wrong reason. |
| `offset.flush.interval.ms` | `5000` | The matched durability cadence (shipped: 60000). |
| `consumer.max.partition.fetch.bytes` | `8388608` | Shipped 1 MiB is ~258 messages/partition/fetch at this corpus's ~4.0 KiB (4065 B) mean framed message, starving a 2000-record poll. `fetch.max.bytes` stays at the shipped 50 MiB. |
| `ignorePartitionsWhenBatching` | `false` (default kept) | Per-partition batching is what keeps the connector's derived dedup token coherent. |
| `clickhouseSettings` | unset | The connector sets `async_insert=0, wait_end_of_query=1` as non-overriding defaults on every insert (`ClickHouseSinkConfig.java:233-234`; a user-supplied value would win, and this arm supplies none) — exactly what server-side attribution requires. |
| Direct memory, metaspace | `768m`, `256m` | Neither scales with the batch, so they stay fixed while `heap_mib` is a knob. |
| Collector | G1 | Matched to the Flink arm, and measured slightly ahead of generational ZGC on this rig. ZGC also has no compressed oops, which costs +29.2% on retained size here. |

`GROUP_ID` is the **connector name**, not a consumer `group.id`: Connect derives
a sink's consumer group as `connect-<name>`, so the fresh consumer group each
repetition needs (a drain replays from offset zero) arrives by naming the
connector with the driver's fresh id. (`consumer.override.group.id` would also
work — standalone applies connector-level overrides under the default policy —
but the name keeps the connector and its group one identity, with nothing to
keep in step.)

[`log4j2.yaml`](log4j2.yaml) is root `WARN`, console only — Kafka 4.x
configures log4j2 in YAML, and the shipped Connect config is root `INFO` with a
rolling *file* appender: disk writes on the hot path, inside the measured
cgroup, for output nothing reads.

`-Xlog:gc*,safepoint` writes `/opt/kafka/logs/gc.log` for the driver to read —
set via `KAFKA_OPTS`,
not `KAFKA_GC_LOG_OPTS`, because `kafka-run-class.sh` assembles its own GC
logging only behind the `-loggc` flag, which `connect-standalone.sh` never
passes in any mode; `KAFKA_OPTS` is on the exec line unconditionally.

## Build

```sh
bench build kafka-connect
```

By hand — the build context is the **repository root**, uniformly for every
entrant:

```sh
docker build -f entrants/kafka-connect/Dockerfile -t spate-bench-kafka-connect .
```

The build fetches the connector's GitHub release zip and fails unless its
sha256 matches the pinned `CKC_SHA256` — a release asset can be replaced under
the same tag, and unverified bytes do not ship. The Avro converter's resolved
dependency graph is recorded at `/opt/connect/dependencies.txt` (Maven has no
lockfile; the graph is the only provenance a re-run can be compared against).

## Run

```sh
bench run kafka-connect --reps 3
```

By hand, which is what a reviewer runs to look inside the container. One
container, the full 32 CPU / 96 GiB data-plane envelope; standalone Connect has
no control plane by design:

```sh
docker run -d --name spate-bench-kafka-connect --network spate-bench-net \
  --cpus 32 --memory 96g --memory-swap 96g \
  spate-bench-kafka-connect
```

`--memory-swap` equals `--memory` so memory pressure surfaces instead of hiding
in a swapfile. The worker never terminates itself; the driver removes the
container. **That recipe runs the image's defaults**, which are kept equal to
the published knobs; the driver additionally sets the variables `[env]` names.

**Rerunning by hand needs a fresh `GROUP_ID`** (e.g. add
`-e GROUP_ID=comparison-kafka-connect-$(date +%s)`): the image's default is
stable, so a second run under it resumes the consumer group `connect-…` at the
tail of the topic and consumes nothing — the same trap the driver avoids by
sending a fresh id every repetition.

The rendered configuration in force is readable out of the running container:

```sh
docker exec spate-bench-kafka-connect cat /opt/kafka/connect-data/worker.properties \
  /opt/kafka/connect-data/clickhouse-sink.properties
```

Smoke-checking a drain by hand: watch `SELECT count() FROM sensor_events` reach
the expected row count. If it stalls a few thousand rows short with the
consumer group at zero lag, the first suspect is `bufferFlushTime=0` stranding
the tail — see the knob table.

## Versions

| Component | Coordinate / image | Version |
|---|---|---|
| Connect runtime | `apache/kafka:4.3.1` (ASF's own image) | 4.3.1 |
| JVM | Temurin JRE (image default) | 21 |
| ClickHouse sink | `clickhouse-kafka-connect-v1.5.0.zip` (GitHub release, sha256-pinned) | v1.5.0 |
| Avro converter | `io.confluent:kafka-connect-avro-converter` | 8.3.0 |
| Registry client | `io.confluent:kafka-schema-registry-client` (transitive) | 8.3.0 |
| Avro | `org.apache.avro:avro` (transitive) | per `dependencies.txt` |

`[version]` in the descriptor resolves `4.3.1-v1.5.0` from the jar filenames
that actually run (the Connect runtime jar and the plugin jar); the Dockerfile
fails the build if either filename pattern drifts. Joined with `-` rather than
`+` because the driver's version parser accepts only `[0-9A-Za-z.-]` tokens.

## Licence analysis

Everything in the container is Apache-2.0, which is what closed the
`[planned].licence_gate` this arm used to carry:

- `apache/kafka:4.3.1` is the ASF's own image — **not** a Confluent Platform
  image, which is what the Confluent Community Licence covers.
- `clickhouse-kafka-connect` is Apache-2.0 (LICENSE in the release zip).
- `kafka-connect-avro-converter` 8.3.0 and its runtime closure are Apache-2.0
  **at the artifact level**, verified against the POMs on
  `packages.confluent.io/maven/`. The closure includes
  `kafka-clients-8.3.0-ccs` — Confluent's Apache-2.0 build of the Apache Kafka
  client, which arrives at compile scope and lands in the plugin directory
  (harmless: Connect loads `org.apache.kafka.*` parent-first, and it is
  recorded in `dependencies.txt`). The CCL covers the Schema Registry
  *server*, which is not present (the registry here is Redpanda's
  Confluent-API implementation). [Aiven's licence analysis](https://aiven.io/blog/aiven-statement-on-kafka-license)
  reaches the same reading.
- Apicurio's Apache-2.0 converter was evaluated and rejected on function, not
  licence: its documented Confluent compatibility is server-side —
  [adr/0001](https://github.com/Apicurio/apicurio-registry/blob/main/adr/0001-confluent-schema-registry-compatibility.md)
  scopes it to Apicurio *serving* the Confluent API to Confluent clients — and
  its own serdes are documented only against Apicurio's registry API, leaving
  no supported client-side path to a third-party Confluent-API registry. No
  other Apache-2.0 converter on Maven Central speaks that API at all.

The converter's non-Central origin is declared in `[[deviations]]`.

## Differences worth knowing

- **The transform runs in ClickHouse, not in Connect.** The arm's client-side
  CPU (decode + RowBinary re-encode) and the other arms' client-side CPU
  (decode + transform + encode) are not the same work. Read this arm's numbers
  with the server-side CPU column, which is where its flatten/filter/derive
  cost lands, and which excludes background merges — though the `Null`-engine
  landing table produces no parts and therefore no merges of its own.
- **This arm is not gated against the ClickHouse ingest ceiling.** The
  `rowbinary` ceiling was measured as direct inserts into the bare target; this
  arm's every insert also runs the MV, so the harness refuses that gate rather
  than proving headroom against work the ceiling never measured. Its records
  carry headroom-unproven on the ClickHouse axis (the broker-consume gate still
  applies), and the server-side CPU column is the honest measure of its target
  load.
- **The upstream integration test for array-of-Struct → `Array(Tuple)` is
  `@Disabled`** (for an unrelated flag) at v1.5.0, so the nested write path
  this arm depends on is not exercised by the connector's own CI. First local
  smoke of a live drain is the verification, not the upstream suite — and that
  applies doubly since `client_version=V2` takes a different RowBinary encoder
  from V1's.
- **The MV-attribution belief in `harness/src/serverside.rs` is what this arm
  verifies.** An insert into the landing table names the view's target in
  `tables` as well, so attribution by `hasAny` catches it; the landing table is
  also declared in `[clickhouse].attribution_tables` as the module recommends.
- **The connector derives `insert_deduplication_token`
  (`topic-partition-minOffset-maxOffset`) on every schema-path insert**, even
  with `exactlyOnce=false`. Inert here — the landing table is `ENGINE = Null`,
  no parts, no dedup window — but it is the same mechanism `ddl.sql` discloses
  for the Spate arm, so it is declared rather than discovered.
- **Connect ≥ 2.7 compatibility statement**: the connector declares Kafka
  Connect 2.7+ compatibility; Kafka 4.3's Connect API is well inside that
  range, but a base-image major bump should re-check it.

## Gregg's question (candidate)

Why X and not 2X: the collector, downstream of how much the decode path
allocates. The last published sweep spent **1,440–1,848 s of stop-the-world
pause in a ~1,987 s window** in `Full (G1 Compaction Pause)`, so most of the
26.7 cores it used were G1's parallel workers rather than work. ClickHouse was
not the constraint — `ch_cpu_us_per_row` was 0.601, third lowest of any arm, and
inserts already carried ~155,000 rows.

Measured locally against this arm's own jars, per 3.8 KiB message of 100 events:
`AvroConverter.toConnectData` allocates 163 KiB, and `StructToJsonMap.toJsonMap`
a further 435 KiB while retaining only 44 KiB of it. At the rate that sweep
sustained, the worker allocates **~10.8 GB/s**, or ~8 KiB per output row — the
connector encodes every event, and the MV drops 26.5% of them afterwards, so
messages are `rows / (100 x 0.735)` and not `rows / 100`. Against
default G1 — `G1NewSizePercent=5`, so ~1 GiB of eden — every insert's working set
outlives eden and is promoted.

That is what the heap and GC configuration above are sized against. Unconfirmed
until a sweep measures it: these figures come from a local probe and the
published record, not from a cell that ran the current configuration.
