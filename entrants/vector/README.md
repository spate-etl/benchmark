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
published — `arrow_stream` (default) and `json_each_row` — because they are not
the same amount of server-side work; the `format` option shipped in 0.53 via
[vector#24373](https://github.com/vectordotdev/vector/pull/24373), whose
motivating issue
[vector#24074](https://github.com/vectordotdev/vector/issues/24074) quotes
JSONEachRow as "~4-5x less efficient" server-side. ArrowStream is still
labelled **beta** in the 0.57 docs, so it is also a declared deviation, and the
GA `json-each-row` variant is the control that quantifies what the beta format
buys.

Vector is Rust with the same no-GC story as Spate and may beat it. That is the
reason to run it.

## Topology: 8 sources → 8 remaps → 1 sink

A Vector [kafka source](https://vector.dev/docs/reference/configuration/sources/kafka/)
is one librdkafka consumer whose decoding runs in that source's own task, so a
single source cannot spread eight partitions' decode across cores. The
maintainers' scaling guidance is one Vector **instance** per partition with
`cooperative-sticky` assignment
([discussion #15884](https://github.com/vectordotdev/vector/discussions/15884));
the envelope allows one container, so this arm runs the in-process analogue:
eight identical sources in one consumer group, which the group protocol treats
exactly as it would eight processes, settling at one partition per consumer.
Each source feeds its own
[remap](https://vector.dev/docs/reference/configuration/transforms/remap/) —
structure rather than necessity:
[Vector's concurrency model](https://vector.dev/docs/about/under-the-hood/architecture/concurrency-model/)
says a remap is a stateless function transform it may parallelize on its own,
but one remap per source keeps each partition's pipeline a straight line with no
fan-in point whose scheduling would be trusted implicitly. All eight remaps load
the same committed [`transform.vrl`](transform.vrl). In
[`vector.yaml.tmpl`](vector.yaml.tmpl) the sources `in-1`..`in-7` are YAML
aliases of `in-0`, so "identical" is parser-enforced rather than promised.

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
| `threads` | detected parallelism | **32** | `VECTOR_THREADS`, the tokio worker count. Vector's default uses `available_parallelism()`, which honors the cgroup quota, so inside the container it would likely land on 32 anyway — the knob makes the width a declared statement rather than a detection. The partition-count rule in [envelope.md](../../methodology/envelope.md) guards partition-*owning* units, and here those are the eight sources, which multiplex freely over the workers — the measured arm spends the full 32-CPU envelope through them. |
| `batch_events` | ~40k rows effective (the 10 MiB byte bound seals first) | **262144** | Rows per INSERT, equal to Spate's cap so the cross-arm batch quantity is comparable, and inside a fixed 256 MiB byte cap raised so that **events** bind and the declared batch size is the one in force. |
| `batch_timeout_secs` | 1 | **1** | Kept: it is the sustained-mode p99 floor; in drain, batches fill on size first. Sweepable. |
| `request_concurrency` | `adaptive` | **8** | Fixed width over the adaptive (ARC) controller: a drain window is tens of seconds and ARC spends exactly that long probing its way up, so the measurement would be of the controller's warm-up. Eight is a starting width matched to the partition count, not a cross-arm constant — the other arms' insert-concurrency knobs count different things (Spate: inflight per shard; Flink: inflight per subtask), so no single number "matches" them. Sweepable. |
| `buffer_events` | 500 | **524288** | The shipped 500-event sink buffer cannot feed even one 262144-row batch — the batcher would seal on starvation every time. 2× the batch so the next batch fills while the last drains. |
| `compression` | `gzip` | **`none`** | The default spends the envelope's scarce CPU compressing inserts to save same-host bandwidth the environment has in abundance. |

`buffer_events` must exceed `batch_events` — declared in `[[constraints]]` so a
sweep is refused before a container starts, not discovered as a starved batcher
minutes into a cell.

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
| `batch.max_bytes` | 256 MiB | Lifted from the shipped 10 MiB so `batch_events` is what binds (see knob table). |
| 8 sources / 8 remaps | structural | One consumer per partition; one remap per source for structure, not parallelism (see Topology). |
| `partition.assignment.strategy` | `cooperative-sticky` | Settles eight consumers on one partition each; incremental rebalance, so a late joiner does not stall the other seven. |
| `fetch.message.max.bytes` | 8 MiB | Above the corpus's largest framed message; the 1 MiB default costs extra round-trips. |
| `queued.max.messages.kbytes` | 262144 (= 256 MiB) | Per-consumer prefetch bound; in drain the consumer must never be the starved side. 8 × 256 MiB is small against 96 GiB. |
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
decoder takes a static inline schema — see Differences), and stage 2 runs
`vector validate --no-environment` against the exact binary that will run it, so
a config the loader rejects or a VRL program that does not compile fails the
build rather than the benchmark run.

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
in a swapfile. `FORMAT` selects `arrow_stream` or `json_each_row`; the six knobs
above arrive as `VECTOR_THREADS` and `SINK_*`. **That recipe runs the image's
defaults**, which are kept equal to the published knobs.

The config itself can be re-checked at any time against the shipped binary:

```sh
docker run --rm --entrypoint vector spate-bench-vector \
    validate --no-environment /etc/vector/vector-arrow.yaml
docker run --rm --entrypoint vector spate-bench-vector \
    validate --no-environment /etc/vector/vector-json.yaml
```

`--entrypoint vector` because the image's entrypoint selects a config from
`FORMAT` and execs Vector with it.

## Versions

| Component | Coordinate / image | Version |
|---|---|---|
| Vector | `timberio/vector:0.57.0-debian` | 0.57.0 (2026-07-14) |
| Kafka client | librdkafka (statically linked by upstream) | as shipped in 0.57.0 |
| Avro decoder | `apache-avro` (Vector's `avro` codec) | as shipped in 0.57.0 |

`[version]` in the descriptor resolves the version by running the image
(`vector --version`) and `pinned = "0.57.0"` refuses the run on mismatch, so a
base-image bump cannot publish a mislabelled number. The `-debian` variant
rather than `-distroless-static`: glibc, plus a shell for the version command.

## Differences worth knowing

- **The Schema Registry is never contacted.** Vector has no registry
  integration; the decoder takes a static inline schema and
  `strip_schema_id_prefix` discards the 5-byte Confluent frame *without
  validating the schema id*. The arm pays no registry lookup, ever (the other
  arms pay one and then cache), and cannot detect a writer-schema change
  mid-run. Declared in `[[deviations]]`; the baked schema is manufactured at
  image build from the committed `.avsc` so it cannot drift.
- **`arrow_stream` is beta in 0.57** and fetches the target table's schema from
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
  arm's eight sources consume the **same** topic, the in-process analogue of
  the instance-per-partition deployment
  [#15884](https://github.com/vectordotdev/vector/discussions/15884)
  recommends — but no upstream doc blesses this shape by name, so the first
  live run must confirm the eight consumers settle 1:1 and stay settled.
  Adjacent: [#22006](https://github.com/vectordotdev/vector/issues/22006)
  (consumers stop after a rebalance; fixed in rust-rdkafka 0.37 — check which
  rust-rdkafka 0.57.0 vendors). If upstream review (rule 7) says this topology
  mis-serves Vector, that is exactly the PR this repository most wants.
- **`async_insert` must actually be off on the wire.** The config sends
  `query_settings.async_insert_settings.enabled: false` and `vector validate`
  proves the key parses, but nothing here proves the setting reaches every
  `arrow_stream` request. On the first live run, check
  `system.query_log.Settings` for the arm's INSERTs: an async ack would make
  the e2e-acknowledgement chain trust a write that is not yet durable.
- **The remap fan-out is the throughput risk.** One event in, an array of up to
  100 objects assigned to `.` — per-element object construction in VRL is the
  hottest code in the arm. If it binds, the rule-1-compliant fallbacks are a
  restructured VRL program (build the child rows with fewer intermediate
  allocations) or splitting the eight shapes across 2–4 clickhouse sinks
  (partitioned by input) to widen the sink side; both are configuration, not
  code Vector does not ship.
- **`arrow_stream` is beta.** If it misbehaves, `json-each-row` is the same arm
  with one env var changed, and both are published regardless.

## Gregg's question (rule 6)

To be answered from the measured run before publication. The candidate: the
per-event object construction in the eight remap tasks — the fan-out allocates
~100 child objects per message in VRL, where Spate's `flat_map` reuses decoded
buffers — with `vector top`/cgroup CPU attribution as the evidence either way.
If the sink side binds instead, the fixed `request.concurrency = 8` against the
measured ingest ceiling is the number to cite.
