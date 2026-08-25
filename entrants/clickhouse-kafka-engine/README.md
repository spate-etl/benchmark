# The ClickHouse Kafka table engine arm

Kafka → Confluent-framed Avro → materialized view → the shared ClickHouse,
held to [the fairness contract](../../methodology/), which is normative. Read
that first; this file records only what is specific to this arm.

The arm is a dedicated **ingest tier**: one ClickHouse container gets the
whole 32 CPU / 96 GiB data-plane envelope and runs a Kafka engine table
(consume + AvroConfluent decode), a materialized view (flatten + filter +
derive), and a Distributed table that forwards the finished rows —
**synchronously** — to the shared infra ClickHouse that owns `sensor_events`
for every arm. It is *not* a zero-hop baseline: the container does no
MergeTree storage of its own, pays one network hop to storage like every arm,
and is published as "ClickHouse as its own ETL tier". The alternative — local
storage inside the 32-CPU envelope, paying merges the 32-CPU shared server
gives every other arm for free — would have been an unfair handicap dressed
as a stronger claim.

Delivery is **at-least-once**: the engine commits offsets once per flushed
block, *after* the block has been written through the materialized view, and
`distributed_foreground_insert = 1` makes that write block until the shared
server has acked — so an offset commit implies the rows exist remotely. A
crash between the remote ack and the commit replays the block, which surfaces
as the duplicate metric, never as loss. The insert format is **Native**
(lz4-compressed columnar blocks over the interserver TCP link) — with the
declared deviation that this is the Distributed engine's internal transfer,
not a client insert API, so read it against Spate's `native` rather than
Flink's `rowbinary_nt`.

There is **no custom code in this arm at all**: three static SQL objects
([`initdb/10_ddl.sql`](initdb/10_ddl.sql)), declarative XML
([`config.d/`](config.d/), [`users.d/`](users.d/)), and an assert script
([`initdb/20_assert.sh`](initdb/20_assert.sh)) that reads the configuration
back and **refuses the container start** on any mismatch.

## Configuration

Every tunable reaches the server as an environment variable, carried by
`from_env` into the `kafka_src` named collection or the default profile. The
DDL is static — `ENGINE = Kafka(kafka_src)` — so what a reviewer reads in the
SQL is exactly what runs, and what the driver set is exactly what
`20_assert.sh` verified.

### Knobs the driver sets per run

| Knob | Value | Engine default | What it controls |
|---|---|---|---|
| `num_consumers` | **32** | 1 | `kafka_num_consumers`: one consumer per **partition** (32). Fewer leaves the slowest consumer owning two partitions and pacing the drain. Matched to the 32-CPU envelope exactly as Spate `threads = 32` and Flink `parallelism = 32` are. The CREATE-time cap on this value is `max(detected cores, 16)` and a violation throws at CREATE (`StorageKafkaUtils.cpp`, v26.3.17.4), so 32 clears it exactly on this 32-core cgroup; the post-start check below guards a future cap change. |
| `block_msgs` | **16384** | 131,072 | `kafka_max_block_size`, in **messages**, not rows: 16384 messages ≈ 1.2M surviving rows per forwarded INSERT. Unset, the engine derives `max_insert_block_size / num_consumers` = 131,072 messages per consumer (`StorageKafka.cpp`, v26.3.17.4) — never reached at this corpus's rates, so under the default the flush timer always binds and block size stops being a knob. Sweep candidates: 8192 / 16384 / 32768. |
| `flush_ms` | **5000** | 7500 | `kafka_flush_interval_ms` — **the commit cadence**: offsets commit once per flushed block. The engine's own default (7500, from `stream_flush_interval_ms`) would be a *laxer* durability interval than every other arm's 5 s, so 5000 is matched, not tuned. |
| `poll_timeout_ms` | **500** | 500 | `kafka_poll_timeout_ms`, the stream thread's poll bound. Declared as a knob (at its default) so the `flush_ms > poll_timeout_ms` constraint in `entrant.toml` is checkable against stated values rather than an image default no record reports. |

### Values fixed in `config.d/` and `users.d/`

| Setting | Value | Why |
|---|---|---|
| `kafka_thread_per_consumer` | **1** | THE trap ([#35153](https://github.com/ClickHouse/ClickHouse/issues/35153)): the shipped default 0 squashes all consumers into ONE flush thread — 8 consumers measure like 1. Not a knob: no correct configuration has another value. |
| `kafka_skip_broken_messages` | **0** | One skipped message silently drops 100 rows; the loss gate then voids the arm. |
| `distributed_foreground_insert` | **1** | The guarantee-bearing setting. Default 0 spools the MV's insert to local disk and acks early, so offsets would commit before the shared server had the rows. The former name `insert_distributed_sync` remains an alias. |
| `async_insert` | **0** | Session settings travel with the Distributed forward, and 26.3 defaults this ON — unpinned, the shared server executes this arm's forwarded inserts down the async path (verified live), the harness refuses to attribute them (`AsyncInsertQuery` fingerprint), and the record would ship with no server-side metrics. 0 is what Spate's and Flink's clients set per insert. |
| `kafka_commit_every_batch` | 0 (shipped) | One offset commit per flushed block, not per librdkafka batch — this is what makes `flush_ms` the durability cadence. |
| `materialized_views_ignore_errors` | 0 (pinned) | 1 would turn a refused remote insert into a dropped block plus a log line; the loss gate must see stall-and-replay, never skip. |
| `background_message_broker_schedule_pool_size` | 16 (shipped, recorded) | The pool the streaming jobs run in; with `thread_per_consumer = 1` the arm needs ≥ 8. Recorded so a default change cannot move it silently. |
| `queued_max_messages_kbytes` | 262144 | librdkafka prefetch byte cap, 64 MiB → 256 MiB per consumer queue: ClickHouse itself targets `queued.min.messages = max(block_msgs, 100000)` messages of prefetch, and the shipped 64 MiB cap cuts that off below one 16384-msg × ~5 KiB block. |

Server sizing for a node that stores nothing (shrunk caches, 2-thread merge
pool with the matching `merge_tree` free-entries floors, every shipped system
log removed **except `query_log`** — the reviewer's window, ~3 rows/s: a
start and a finish row per forwarded insert — **and `crash_log`**, written
only on a fatal signal) is justified setting-by-setting in
[`config.d/30-server-sizing.xml`](config.d/30-server-sizing.xml).

## Build

```sh
bench build clickhouse-kafka-engine
```

By hand — the build context is the **repository root**, uniformly for every
entrant:

```sh
docker build -f entrants/clickhouse-kafka-engine/Dockerfile -t spate-bench-ch-kafka .
```

## Run

```sh
bench run clickhouse-kafka-engine --reps 3
```

By hand, which is what a reviewer runs to look inside the container:

```sh
docker run -d --name spate-bench-ch-kafka --network spate-bench-net \
  --cpus 32 --memory 96g --memory-swap 96g \
  spate-bench-ch-kafka
```

`--memory-swap` equals `--memory` so memory pressure surfaces instead of
hiding in a swapfile. **That recipe runs the image's defaults**, which are
kept equal to the published knob values; the driver additionally sets the
nine variables in `entrant.toml [env]`, and those are what a published
record's knobs mean. The image's official entrypoint runs the initdb DDL and
the assert script against a localhost-only init server, then starts the real
one — a container that comes up at all has already proven its configuration.

**Post-start operator check** — the one thing initdb cannot see, because
consumers materialise when streaming starts:

```sh
docker exec spate-bench-ch-kafka clickhouse-client --query \
  "SELECT count() FROM system.kafka_consumers WHERE table = 'sensor_batches_queue'"
```

Must print **8**. At v26.3.17.4 the CREATE-time cap (`max(detected cores,
16)`) cannot clamp 8 silently — a violation throws during initdb and the
container never starts — so this check exists to catch a *future* version
moving that behaviour, and a count under 8 still invalidates the run.

The SQL endpoint refuses remote clients: with no `CLICKHOUSE_PASSWORD`
set, the official entrypoint restricts the `default` user to localhost, so a
reviewer goes through `docker exec` as above.

## Versions

| Component | Coordinate / image | Version |
|---|---|---|
| ClickHouse server | `clickhouse/clickhouse-server:26.3` (digest `sha256:85c43481…ea49`) | 26.3.17.4 |
| librdkafka | bundled in the server build (listed in `system.licenses`) | pinned by the server tag's contrib submodule; not surfaced at runtime |

Same major as the shared storage server (`environments/*.toml`): one
ClickHouse version in the provenance, and the consumer is not tuned on a
newer codebase than its own storage tier. `[version].pinned` asserts the
version string the image reports, so a base bump refuses the run.

## Gregg's-question candidate

Each consumer's loop is strictly serial: poll → decode → ARRAY JOIN →
**synchronous** remote insert → commit, with no in-flight pipelining — the
remote-ack stall per block is dead time, and the arm is 8 such serial loops
on 32 CPUs. If the number is X and not 2X, this is the first place to look,
and it is the price of the setting that makes the guarantee real.

## Traps, verified

- **[#35153](https://github.com/ClickHouse/ClickHouse/issues/35153)** —
  `kafka_thread_per_consumer = 0` (the default) is one flush thread for all
  consumers. Fixed at 1 above.
- **Consumer-count cap under cgroups** ([#35926](https://github.com/ClickHouse/ClickHouse/pull/35926),
  [#40670](https://github.com/ClickHouse/ClickHouse/pull/40670)): the
  CREATE-time cap is `max(detected cores, 16)` at v26.3.17.4 and a violation
  **throws at CREATE** — a loud initdb failure, not a silent clamp — so this
  arm's 8 consumers fit on 6 detected cores with no override
  (`kafka_disable_num_consumers_limit` exists as the escape hatch and is
  deliberately NOT set: nothing here needs it). Verified against the built
  image: `system.kafka_consumers` reports 8.
- **AvroConfluent `array<record>`** decodes as `Array(Tuple(...))` and the
  nested fields are addressed `e.seq`, `e.name`, … after `ARRAY JOIN` — the
  decode path is not a flat-struct fast case, by workload design.
- **MATERIALIZED columns through Distributed**
  ([#4015](https://github.com/ClickHouse/ClickHouse/issues/4015),
  [#9439](https://github.com/ClickHouse/ClickHouse/issues/9439)): the
  Distributed shim declares the 12 physical columns and **no `ingest_ts`**;
  the shared server stamps it when the forwarded insert lands, so latency
  honestly includes the forward hop.
- **Foreground-insert error propagation**: kill the shared ClickHouse and the
  MV insert fails, the block is not committed, and the engine stalls and
  replays — a duplicate-metric event, never a skip. That is the
  `materialized_views_ignore_errors = 0` + `kafka_skip_broken_messages = 0`
  pairing doing its job.
- **A broker that is down does not stop the server** (verified standalone):
  the Kafka table's consumers retry resolution/connection in the background
  and the server serves queries throughout — so a mis-ordered bring-up
  degrades to lag, not to a crash loop.

## Differences worth knowing

- **The data plane is a database server.** It carries a server's overhead
  (scheduler, system tables, an idle SQL endpoint) inside the envelope, and
  does its storage on the shared server outside it — both directions are
  declared in `[[deviations]]` and rendered by the site.
- **The latency floor is the flush cadence.** Rows wait up to `flush_ms` in
  the engine's block buffer before the forwarded insert exists to be
  stamped. Same trade as Spate's `linger_ms = 500`, at **10×** the floor —
  and here the same knob is also the offset-commit cadence.
- **Durability is "at most 5 s"**: blocks sealing on `block_msgs` before the
  timer commit sooner — stricter than the convention, never looser.
- **No `insert_deduplication_token` is sent** (like every arm except Spate);
  the shared table's `non_replicated_deduplication_window = 1000`
  content-hashes this arm's forwarded blocks. A byte-identical replayed
  block after a crash is *absorbed* by that window — which is what it is for
  — and a replay that re-frames into a different block lands and is reported
  as the duplicate metric, never hidden.
