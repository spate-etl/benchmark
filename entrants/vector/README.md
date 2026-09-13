# The Vector arm

Kafka → Confluent-framed Avro → remap fan-out → ClickHouse, held to
[the fairness contract](../../methodology/), which is normative. Read that first;
this file records only what is specific to Vector.

Delivery is **at-least-once**: end-to-end acknowledgements are on, so a source
offset commits (every 5 s) only after ClickHouse has acked every row derived
from the message.
[Vector's acknowledgement doc](https://vector.dev/docs/about/under-the-hood/architecture/end-to-end-acknowledgements/)
states this for *copies* of an event — status is shared across all copies, the
source is notified once all are processed, worst status wins — and the fan-out
case rides the same mechanism: each child of a remap's array-assign carries a
clone of the parent's metadata, whose finalizers are `Arc`-shared
([finalization.rs](https://github.com/vectordotdev/vector/blob/master/lib/vector-common/src/finalization.rs)),
so the offset resolves only when the last child does. Two wire formats are
published — `json_each_row` (default) and `arrow_stream` — because they are not
the same amount of server-side work; the `format` option shipped in 0.53 via
[vector#24373](https://github.com/vectordotdev/vector/pull/24373), whose
motivating issue
[vector#24074](https://github.com/vectordotdev/vector/issues/24074) quotes
JSONEachRow as "~4-5x less efficient" server-side. That saving is real and lands
on ClickHouse, which this envelope does not charge the arm for; client-side
ArrowStream is the more expensive of the two, because its encoder converts each
event to `serde_json::Value` and feeds those trees to an arrow-json tape reader.
JSONEachRow is therefore the default, and ArrowStream — still labelled **beta**
in the 0.58 docs, so also a declared deviation — is published beside it.

Vector is Rust with the same no-GC story as Spate and may beat it. That is the
reason to run it.

## Topology: N sources → N remaps → 1 sink

A Vector [kafka source](https://vector.dev/docs/reference/configuration/sources/kafka/)
splits a queue per assigned partition and decodes each on its own task
(`split_partition_queue` in `src/sources/kafka.rs`, which is why a message
reaching the main consumer queue is `unreachable!()`), so **one** source already
spreads decode across all 32 partitions. Decode parallelism is not what the
source count buys.

What it buys is output paths. Each remap runs its VRL 32-wide, but every
completed chunk is returned through `send_outputs` **on the remap's own runner
task**, and that path costs per row: it re-clones the event metadata that the
fan-out's children share, records a latency histogram, and walks each new row
for a size estimate. `sources` is therefore a knob — it sets how many such
serial paths run in parallel — and N is chosen by measurement rather than from
the partition count. N sources in one group is the in-process analogue of the
instance-per-partition deployment the maintainers recommend
([discussion #15884](https://github.com/vectordotdev/vector/discussions/15884)),
and the group protocol treats them as it would N processes.

Each source feeds its own
[remap](https://vector.dev/docs/reference/configuration/transforms/remap/),
all loading the same committed [`transform.vrl`](transform.vrl). The blocks are
generated at image build from one marked region in
[`vector.yaml.tmpl`](vector.yaml.tmpl), so "identical" is a property of the
generator rather than a promise, and the build asserts the count it produced.

A reviewer can check the per-partition claim at runtime: with `VECTOR_LOG=debug`
a single source logs its partition consumers once each.

## Configuration

The wire format is chosen by *file*, not by a field. `arrow_stream` needs a
`batch_encoding` block that `json_each_row` must not carry, so
[`vector.yaml.tmpl`](vector.yaml.tmpl) marks that block off and the image build
emits one config per variant; `entrypoint.sh` selects between them on `FORMAT`
and rejects any other value. Neither config can therefore name a format its
encoder does not produce — a combination Vector accepts and ClickHouse then
refuses on every insert.

Every other knob reaches the container as an environment variable. Five live in
[`vector.yaml.tmpl`](vector.yaml.tmpl) as `${...:-default}` placeholders; the
sixth, `threads`, is `VECTOR_THREADS` — the binary's own variable, with no
config-file line to hold it — so its default lives only in the Dockerfile `ENV`
block, beside the other five's. All committed defaults equal the published
knobs (a harness test holds the Dockerfile, the template and the descriptor to
the same values), so a hand-run container matches the numbers.

### Knobs the driver sets per run

| Knob | Shipped default | Ours | Why |
|---|---|---|---|
| `threads` | detected parallelism | **32** | `VECTOR_THREADS`, the tokio worker count. Vector's default uses `available_parallelism()`, which honors the cgroup quota, so inside the container it would land on 32 anyway — the knob makes the width a declared statement rather than a detection. The partition-count rule in [envelope.md](../../methodology/envelope.md) guards partition-*owning* units; here those are the source's per-partition decode tasks, of which there are 32 at any source count (see Topology). |
| `sources` | n/a | **8** | Source/remap pairs, generated at image build. Not a decode-parallelism knob — it sets how many serial `send_outputs` paths run in parallel (see Topology). Eight is where the arm fills its envelope; 4, 16 and 32 were measured and all spend fewer cores (see below). |
| `chunk_size_events` | 1000 | **1000** | `VECTOR_CHUNK_SIZE_EVENTS`. Scales `ready_array_capacity` (×4) and the source-sender buffer together, so it sets the runner's in-flight budget: 32 in-flight chunks × 4×this × the 100:1 fan-out, per remap. Left at the shipped value: 64 more than halves resident memory and costs 1.1% throughput. |
| `batch_events` | ~40k rows effective (the 10 MiB byte bound seals first) | **262144** | Rows per INSERT, equal to Spate's cap so the cross-arm batch quantity is comparable, under a byte cap raised until **events** bind. |
| `batch_timeout_secs` | 1 | **1** | Kept: it is the sustained-mode p99 floor; in drain, batches fill on size first. Sweepable. |
| `request_concurrency` | `adaptive` | **32** | Fixed width over the adaptive (ARC) controller: a drain window is tens of seconds and ARC spends exactly that long probing its way up, so the measurement would be of the controller's warm-up. Also the sink's encode width — it spawns this many concurrent batch encoders. Not a cross-arm constant; the other arms' insert-concurrency knobs count different things (Spate: inflight per shard; Flink: inflight per subtask). Sweepable. |
| `buffer_events` | 500 | **524288** | The shipped 500-event sink buffer cannot feed even one 262144-row batch — the batcher would seal on starvation every time. 2× the batch so the next batch fills while the last drains. |
| `compression` | `gzip` | **`none`** | The default spends the envelope's scarce CPU compressing inserts to save same-host bandwidth the environment has in abundance. |

`buffer_events` must exceed `batch_events` — declared in `[[constraints]]` so a
sweep is refused before a container starts, not discovered as a starved batcher
minutes into a cell.

### What the sweep measured

Screening run at 12M batches, one repetition per cell, each cell carrying its
own A/A control; figures are pair midpoints on this rig.

| transform | chunk | sources | rows/s | rows/s/core | cores | A/A |
|---|---:|---:|---:|---:|---:|---:|
| two-pass | 1000 | 8 | 2,183,373 | 68,974 | 31.66 | 0.65% |
| **fused** | **1000** | **8** | **2,840,905** | **90,002** | **31.56** | **0.04%** |
| fused | 64 | 8 | 2,831,135 | 89,625 | 31.59 | 1.42% |
| fused | 64 | 4 | 2,721,795 | 93,273 | 29.18 | 0.55% |
| fused | 64 | 16 | 2,755,545 | 91,351 | 30.16 | 0.84% |
| fused | 64 | 32 | 2,445,238 | 91,399 | 26.75 | 0.29% |

The transform rewrite is the whole of it: **+30.5%** on the lead metric against
an A/A spread of 0.04%, with `duplicate_rows` at zero and the correctness gate
passing on every drain.

Neither of the other two knobs earns a change. Raising the source count does not
widen the arm — per-core efficiency stays flat while envelope occupancy falls
(31.6 cores at 8, 30.2 at 16, 26.8 at 32), so eight is where it spends the most
CPU productively. Shrinking `chunk_size_events` to 64 more than halves resident
memory, 52 GB to 24 GB, and costs 1.1%: residency was never the constraint, and
the knob's only other use — making a 32-source topology fit in the envelope —
buys a topology that is slower anyway.

**Disclosed, because rule 2 requires it:** `sources = 4` posts the best per-core
figure in the sweep, 93,273, which is 3.6% above the committed configuration. It
gets there by spending 2.4 fewer cores — the 4→8 step buys 109k rows/s for 2.41
cores, ~45k per marginal core against a ~90k average. It is not taken because
3.6% does not clear the 5% margin this search declared in advance, and it costs
4.2% of raw throughput. `sources = 4` at the shipped chunk size was not
measured.

### Values fixed in the config

| Key | Value | Why |
|---|---|---|
| `commit_interval_ms` | 5000 | The durability cadence every arm pays (Spate's offset commits, Flink's `AT_LEAST_ONCE` checkpoints). Vector's shipped default, set explicitly so it is a statement rather than an inheritance. |
| `acknowledgements.enabled` | `true` | The load-bearing line: connects a ClickHouse ack back to the source's offset commit. Without it the 5 s interval commits offsets for rows the sink may still lose. |
| `query_settings.async_insert_settings.enabled` | `false` | ClickHouse 26.3 defaults `async_insert` on, under which the server acks before writing — an ack the acknowledgement chain would then trust. Matched to every other arm. |
| `buffer.when_full` | `block` | Backpressure, not loss: `drop_newest` breaks at-least-once while flattering throughput. |
| `skip_unknown_fields` | `false` | An unknown field means the transform emitted a column the table lacks — a bug to fail on, not absorb. |
| `drop_on_error` | `true` | A decided stance on the poison path. Vector's default forwards the *original, un-flattened* event on a VRL runtime error, which `skip_unknown_fields = false` turns into a rejected INSERT carrying 262144 good rows (or a wedge behind `when_full: block`). Dropping costs exactly the errored message's rows, which the loss gate counts and publishes. Unreachable on the deterministic corpus. |
| `date_time_best_effort` | `true` | Lands the epoch-derived `DateTime64(3)`/`(6)` values at full precision. |
| `batch.max_bytes` | 512 MiB | `max_bytes` is measured on in-memory `ByteSizeOf`, not encoded bytes, and a row of this shape is ~1285 B resident — so 262144 of them is ~337 MB. Set above that so `batch_events` is what seals a batch. At the 256 MiB this arm previously carried, bytes sealed first and every INSERT was 208,851 rows rather than the declared 262,144. |
| `partition.assignment.strategy` | `cooperative-sticky` | Incremental rebalance, so a late joiner does not stall the others. |
| `fetch.message.max.bytes` | 8 MiB | Above the corpus's largest framed message; the 1 MiB default costs extra round-trips. |
| `queued.max.messages.kbytes` | 262144 (= 256 MiB) | Prefetch bound, applied **per partition** — so 256 MiB against each of the topic's 32 partitions, ~8 GiB in total, whatever the source count. In drain the consumer must never be the starved side. |
| `VECTOR_LOG` | `info` | Keeps a stalled arm diagnosable: consumer assignment and sink start-up log at INFO, per-request detail at DEBUG/TRACE, so the hot path stays quiet. |
| `api.enabled` | `false` | No published figure comes from an arm's self-report; an idle API server is still a listener on the measured process. |

## Build

```sh
bench build vector
```

By hand — the build context is the **repository root**, uniformly for every
entrant, because the build needs `entrants/vector/` and `workload/schema/`:

```sh
docker build -f entrants/vector/Dockerfile -t spate-bench-vector .
```

Stage 1 bakes the committed `sensor_batch.avsc` into the config (Vector's avro
decoder takes a static inline schema — see Differences) and generates one config
per wire format and source count, asserting the topology it produced. Stage 2
runs `vector validate --no-environment` over every one of them, against both VRL
programs and the exact binary that will run them, so a config the loader rejects
or a program that does not compile fails the build rather than the benchmark
run.

## Run

```sh
bench run vector --reps 3
```

By hand, which is what a reviewer runs to look inside the container. One
container, the full 32 CPU / 96 GiB data-plane envelope, no control plane:

```sh
docker run -d --name spate-bench-sut-sut --network spate-bench-net \
  --cpus 32 --memory 96g --memory-swap 96g \
  spate-bench-vector
```

(`spate-bench-sut-sut` is the name the harness gives this container —
`spate-bench-sut-` plus the descriptor's container name — so sampler output
correlates; any name works for a look inside.)

`--memory-swap` equals `--memory`, so memory pressure surfaces instead of hiding
in a swapfile. `FORMAT` and `SOURCES` together select the config file; the
rest of the knobs arrive as `VECTOR_*` and `SINK_*`. **That recipe runs the image's defaults**, which are kept equal to the
default variant's published knobs.

The config itself can be re-checked at any time against the shipped binary:

```sh
docker run --rm --entrypoint vector spate-bench-vector \
    validate --no-environment /etc/vector/vector-json-s8.yaml
docker run --rm --entrypoint vector spate-bench-vector \
    validate --no-environment /etc/vector/vector-arrow-s8.yaml
```

`--entrypoint vector` because the image's entrypoint selects a config from
`FORMAT` and execs Vector with it.

## Versions

| Component | Coordinate / image | Version |
|---|---|---|
| Vector | `timberio/vector:0.58.0-debian` | 0.58.0 (2026-08-26) |
| Kafka client | librdkafka (statically linked by upstream) | as shipped in 0.58.0 |
| Avro decoder | `apache-avro` (Vector's `avro` codec) | as shipped in 0.58.0 |

Two independent pins. The Dockerfile's `FROM` carries the image digest
`sha256:1c1ea358…`, which is the **OCI index** digest rather than the
`linux/arm64` per-image one Docker Hub's tag page shows — pinning that would tie
the build to a single architecture. `[version]` in the descriptor resolves the
version by *running* the image (`vector --version`) and `pinned = "0.58.0"`
refuses the run on mismatch, so a repointed tag is caught even though the
Dockerfile is read, not executed, to check it. The `-debian` variant rather than
`-distroless-static`: glibc, plus a shell for the version command.

## Differences worth knowing

- **The Schema Registry is never contacted.** Vector has no registry
  integration; the decoder takes a static inline schema and
  `strip_schema_id_prefix` discards the 5-byte Confluent frame *without
  validating the schema id*. The arm pays no registry lookup, ever (the other
  arms pay one and then cache), and cannot detect a writer-schema change
  mid-run. Declared in `[[deviations]]`; the baked schema is manufactured at
  image build from the committed `.avsc` so it cannot drift.
- **`arrow_stream` is beta in 0.58** and fetches the target table's schema from
  `system.columns` once at sink start-up — a single query, outside the hot
  path, but a start-up dependency on the target the `json_each_row` variant
  does not have. Also declared in `[[deviations]]`.
- **It does not send `insert_deduplication_token`** — like every non-Spate arm.
  The shared DDL sets `non_replicated_deduplication_window = 1000`, so
  ClickHouse hashes this arm's blocks and skips hashing Spate's. Its duplicate
  count is reported rather than suppressed.
- **`upcase` is Unicode uppercase, not ASCII.** The contract specifies
  ASCII-only. On this corpus the two agree — metric names are drawn from a
  fixed set of lowercase ASCII identifiers — and the correctness gate's
  checksum over `name_upper` would fail the arm if that ever stopped being
  true. Noted because on another corpus (`ß` → `SS`) this would be a real
  difference, exactly as the Flink arm's hand-rolled `asciiUpper` documents
  from the other direction.
- **`value_scaled` goes through f64.** VRL's `/` is float division; the
  numerator is below 2^41 and the divisor at most 100, both exact in f64 and
  far below 2^53, so the quotient's integer part is exact and `to_int`
  truncates toward zero — the specified semantics. The `?? 0.0` in
  [`transform.vrl`](transform.vrl) only arms the type checker's divide-by-zero
  case, unreachable since `seq >= 0`.

## Traps a reviewer should check us on

- **Multiple kafka sources in one consumer group.**
  [Issue #21329](https://github.com/vectordotdev/vector/issues/21329) reports
  sources in the same group interfering — in the *different-topics* case. This
  arm's sources consume the **same** topic, the in-process analogue of the
  instance-per-partition deployment
  [#15884](https://github.com/vectordotdev/vector/discussions/15884)
  recommends, but no upstream doc blesses this shape by name. Adjacent:
  [#22006](https://github.com/vectordotdev/vector/issues/22006) (consumers stop
  after a rebalance), fixed in rust-rdkafka 0.37 and this image vendors 0.39.
  If upstream review (rule 7) says this topology mis-serves Vector, that is
  exactly the PR this repository most wants.
- **`async_insert` must actually be off on the wire.** The config sends
  `query_settings.async_insert_settings.enabled: false` and `vector validate`
  proves the key parses, but nothing here proves the setting reaches every
  `arrow_stream` request. On the first live run, check
  `system.query_log.Settings` for the arm's INSERTs: an async ack would make
  the e2e-acknowledgement chain trust a write that is not yet durable.
- **The per-row cost of the fan-out.** One event in, an array of up to 100
  objects assigned to `.`. Two costs sit behind that and neither is reachable
  from configuration: each child ends up owning its own copy of the event
  metadata, because the first thing to touch it calls `Arc::make_mut` while all
  ~73 siblings still share the Arc; and each row is an `ObjectMap` of twelve
  heap-allocated keys, ~1285 bytes resident for a row that is 115 bytes on the
  wire. Splitting into 2–4 clickhouse sinks would *not* widen encoding, which
  the sink already runs `request_concurrency`-wide.
- **`arrow_stream` is beta.** If it misbehaves, `json-each-row` is the same arm
  with one env var changed, and both are published regardless.

## Gregg's question (rule 6)

**Throughput is 2.84M rows/s and not 5.7M because a row costs 11.1 µs of CPU to
produce and only 0.3 µs of that is decoding it.** The arm spends its whole
32-CPU envelope while ClickHouse sits at 0.8 of its own 32; nothing downstream
is limiting it.

A `perf` profile of the running container, 95,170 samples, attributes the cost
to Vector's event model rather than to any one stage. Before the transform
rewrite, **52.8% of cycles went on allocation, atomics, `BTreeMap` and `Value`
clone/drop** — a row is an `ObjectMap` of twelve heap-allocated keys, ~1285
bytes resident to carry 115 bytes of ClickHouse row, and the fan-out's children
each end up owning a copy of the event metadata. Avro decode was 4.0% and JSON
encoding 2.5%.

The rewrite removed copies rather than work: absolute VRL clone/drop fell 61%,
and with it jemalloc 46%, `BTreeMap` 45% and atomics 35%. What remains is
structural and not reachable from configuration. A notable share of it is not
even Vector's pipeline: **9.9% of cycles are the `GroupedTraceableAllocator`
shim, which wraps every allocation in the shipped binary for an allocation
tracing feature that is switched off.**
