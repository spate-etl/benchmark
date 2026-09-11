# The Spate arm

Kafka → Confluent-framed Avro → `flat_map` → ClickHouse, held to
[the fairness contract](../../methodology/), which is normative. Read that first;
this file records only what is specific to Spate.

Delivery is **at-least-once**, with offsets committed every 5 s and never past
unacknowledged data. Two wire formats are published — `native` and `rowbinary` —
because they are not the same amount of server-side work.

**This is the vendor's own entrant.** Everything below is written to be attacked.
If a competitor's arm is tuned worse than this one, that is a bug in the benchmark
— open an issue.

## Configuration

Every knob is set by the driver from the descriptor, and the record carries what
was in force.

| Knob | Value | What it controls |
|---|---|---|
| `threads` | **32** | Consumer/transform threads; scaling must be measured rather than inferred from partition ownership. |
| `shards` | **32** | Independent sink workers with their own queues, encoders and in-flight permits. |
| `inflight` | **4** | Concurrent INSERTs per shard, up to 128 across the arm. |
| `linger_ms` | **500 RowBinary / 2000 Native** | Timer limit; actual batch fill depends on arrival rate and backpressure. |
| `max_rows` | **262144 RowBinary / 1048576 Native** | Configured cap per INSERT, not a promise of actual rows per INSERT. |

Use `ch_rows_per_insert` and query logs to check actual batch size. Historical
claims of 99.7% fill and a 364 ms fill time do not describe every current run.

Fixed by the contract rather than chosen: `commit_interval` is **5 s**, matched to
Flink's `AT_LEAST_ONCE` checkpoint interval so both arms pay for the same
guarantee at the same cadence. `async_insert` is **0**, matching every other arm —
async inserts would move batching into the server and make the comparison one of
ClickHouse settings rather than of frameworks. `metrics.exporter` is **none**;
nothing this arm reports about itself is used for any published number.

Three values are the framework's and are **not reachable from any descriptor**, so
no record reports them:

| Setting | Value | Why it matters |
|---|---|---|
| `io_threads` | 2 | That runtime owns every sink writer and the LZ4 compression of every insert body. It is material and no descriptor can set it. |
| `chunk.target_bytes` | 64 KiB (framework default) | Under `native` this is the ClickHouse **block** size, so at ~57 bytes per row a block is about 1,150 rows and one 261,323-row INSERT is roughly 230 concatenated blocks. |
| `compression` | `lz4` (framework default) | This arm compresses each block inside ClickHouse's checksummed frame. |
| `max_inflight_bytes` | derived | Scales with `shards × inflight × max_rows`, so widening egress cannot backpressure on a budget *we* chose. Sound here only because rows bind long before bytes do. |

The last two are why the committed **Native ingest ceilings are an upper bound**:
the ceiling rig POSTs one large uncompressed block per request where this arm
seals many smaller compressed ones, and the ceilings file records that on every
Native figure.

## Build

```sh
bench build spate
```

The build context is the repository root: the arm is a cargo workspace member and
needs the workspace manifest and lockfile. While the framework is a private git
dependency the build also needs a credential, passed as a BuildKit secret so no
token is ever baked into an image layer.

## Run

```sh
bench run spate --reps 3
```

One container, the full 32 CPU / 96 GiB data-plane envelope, no control plane.
`FORMAT` selects `native` or `rowbinary`; the five knobs above arrive as
`THREADS`, `SHARDS`, `INFLIGHT`, `LINGER_MS` and `MAX_ROWS`.

## Versions

Resolved by running the built image — `spate-arm --version` — so the recorded
version is the one that was linked rather than one a human typed. The Rust
toolchain is pinned in `rust-toolchain.toml` and a test asserts the image's base
matches it, because codegen moves throughput and a silent divergence would make
the recorded toolchain wrong.

## Differences worth knowing

- **This arm sends `insert_deduplication_token` and no other arm does.**
  `etl-clickhouse`'s writer sets it, so the behaviour arrives with the framework
  under test rather than from anything in this directory — which is why it is easy
  to miss and why it is written down here. The target sets
  `non_replicated_deduplication_window = 1000` identically for every arm, so
  ClickHouse skips hashing this arm's blocks and hashes everybody else's. That is
  real server-side work avoided by the arm the benchmark's author wrote. Any
  framework could send tokens and none of the others currently does; the cost it
  avoids belongs in the published server-side figure rather than in a footnote.
- **The workload suits us.** Kafka → Avro → ClickHouse is the pipeline this
  framework was built for, and the benchmark was written by its author. A workload
  chosen by someone else would be a stronger test, which is why the corpus, the
  DDL and the rules are all committed rather than described.
- **`native` is not like-for-like with Flink.** It is what a real deployment runs,
  so it is published — but the arm to read against Flink is `rowbinary`,
  because ClickHouse's official Flink connector can only write
  `RowBinaryWithNamesAndTypes`. The gap between the two is server-side rather than
  in our encoder: client CPU per row is near enough equal, while ClickHouse's own
  cost differs by more than 3×. Presenting Native against Flink would be claiming
  credit for a gap in the Java client.
- **`rowbinary` sits closest to the headroom limit.** Above 70% of the
  measured ingest ceiling a number describes ClickHouse rather than the framework,
  and the harness records such a run as `infra_bound` rather than publishing it.
  Pinning that variant to a slower configuration to duck the gate would be tuning
  around the gate, so it is left where the search put it and the gate is allowed
  to fire.

## What this arm shares with the harness, and what it must not

The wire contract is shared, and only the wire contract: every arm decodes the
registry-served `sensor_batch.avsc`, the one schema committed in
`workload/schema/`, and the Flink arm parses that same `.avsc`. A schema is not
the transform, and sharing it shares nothing an arm is supposed to write.

**The transform is not shared.** The filter, the ASCII uppercase, the
scaled-value arithmetic and both constants are restated here rather than imported
from `spate_benchmark_harness::corpus` — the module that computes the closed-form
expectations the correctness gate holds every arm to. Were they imported, this arm
and the marking scheme could not disagree by construction, while the Flink arm,
which reimplements all of it in Java, could.
`harness/tests/each_arm_restates_the_transform.rs` fails if such an import
appears or if either arm's constants drift from `workload/workload.toml`.

The earlier tuning evidence also favored width over request depth: sixteen
concurrent INSERTs configured as 8 shards × 2 requests delivered **52% more**
throughput than 2 shards × 8 requests in that experiment. This is historical
tuning evidence, not a prediction that the current 32-shard configuration gains
the same amount. It explains why shard count and in-flight depth are separate
knobs rather than an interchangeable product.
