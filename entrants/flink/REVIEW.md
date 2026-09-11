# Flink fairness review

Review started 2026-09-10 against benchmark `9b4cab3`, Flink source tag
`release-2.2.1`, and official ClickHouse connector tag `v0.2.0`.
Tracking issue: [#74](https://github.com/spate-etl/benchmark/issues/74).

## Established evidence

The three current Flink measurements have median throughput approximately
2.98M rows/s, CPU efficiency 115,654 rows/s/core and CPU cost 8.646 µs/row.
In the same invocation Spate Native achieved approximately 1.60M rows/s/core
and ClickHouse Kafka approximately 1.13M. Spate RowBinary was infrastructure
bound; its throughput must retain that qualification.

One Flink repetition reports 8,495 G1 pauses totaling 771.4 seconds over a
987.9-second window. The maximum post-GC heap occupancy was approximately
17.5 GiB, close to the configured heap. Post-young-GC occupancy is not an
exact live-object measurement. The harness maps JVM uptime to the sampler
window approximately; raw logs and timestamps are needed before calling this
an exact stopped-time fraction. These observations prioritize allocation and
retention, rather than proving a particular allocation site is responsible.

The retired `c8g-8xl-ec2-docker` archive (before commit `c8f58cd`) used eight
partitions/subtasks within a six-CPU/24-GiB data plane. It delivered 2.0–2.2M
rows/s using approximately 5.2 cores and 2.4–2.65 µs/row. Its corpus was 1.5M
messages rather than 40M and it used harness v1, so the comparison is a
scaling lead, not an isolated treatment effect.

## Execution and allocation path

1. Kafka fetches framed byte values through the shipped Kafka connector.
2. `RegistryAvroDeserializationSchema.deserialize` reads the registry schema
   and calls `datumReader.read(null, decoder)`. Object reuse in the execution
   configuration does not reuse this decoder's preceding record.
3. The application filters events before most conversions, hoists batch
   fields/timestamps and reuses one outer `SensorRow`. Each retained event
   still needs string/array conversions and output values.
4. `ClickHouseConvertor.applyPOJOImpl` creates a `ClickHousePayload`, fills its
   `LinkedHashMap`, encodes the row through `DataWriter` and retains a copied
   byte array beside the map. This duplicates representations, not inserted rows.
5. `ClickHouseAsyncWriter` extends the connector's **own**
   `ExtendedAsyncSinkWriter`, rather than Flink 2.2.1's `AsyncSinkWriter`.
   Runtime source inspection alone therefore does not cover the entire sink.
6. The writer sends cached bytes with a names/types header over HTTP. Typed
   mode forces `RowBinaryWithNamesAndTypes`. Checkpointing waits for in-flight
   requests and serializes buffered maps through `ClickHouseAsyncSinkSerializer`.

Pinned source references:
[registry decoder](https://github.com/apache/flink/blob/release-2.2.1/flink-formats/flink-avro/src/main/java/org/apache/flink/formats/avro/RegistryAvroDeserializationSchema.java),
[converter](https://github.com/ClickHouse/flink-connector-clickhouse/blob/v0.2.0/flink-connector-clickhouse-2.0.0/src/main/java/org/apache/flink/connector/clickhouse/convertor/ClickHouseConvertor.java),
[writer](https://github.com/ClickHouse/flink-connector-clickhouse/blob/v0.2.0/flink-connector-clickhouse-2.0.0/src/main/java/org/apache/flink/connector/clickhouse/sink/ClickHouseAsyncWriter.java),
and [buffering implementation](https://github.com/ClickHouse/flink-connector-clickhouse/blob/v0.2.0/flink-connector-clickhouse-2.0.0/src/main/java/org/apache/flink/connector/clickhouse/sink/writer/ExtendedAsyncSinkWriter.java).

The connector's `actualRecordsPerBatch` and `actualBytesPerBatch` histograms
are misleading in this release: `flush()` updates them with the **remaining
buffer** after `createNextAvailableBatch()` removes the submitted rows. They
are retained as raw diagnostics, but batch-size conclusions must use
ClickHouse query-log measurements (`ch_rows_per_insert`). Flush-reason counters
can also increment in both `nonBlockingFlush()` and `flush()`; they are not an
exclusive accounting of submitted batches.

The configured 32 × 100,000 buffered rows and 32 × 2 × 50,000 in-flight rows
are each 3.2M. They are capacities rather than observed live counts. Increasing
parallelism multiplied these capacities while preserving roughly the old heap.

There is also an adaptive limit below the configured request count. The
connector constructs Flink's default `AsyncSinkWriterConfiguration`, whose
congestion-control strategy starts with `maxBatchSize` in-flight **rows** and
increases that capacity by ten after each successful request. At a 262,144-row
batch cap, permitting two full batches would require 26,215 successful requests
without intervening failures. Smaller timer-flushed batches can overlap sooner.
Thus matching Spate's four request slots does not match its effective concurrency.
The connector builder does not expose a replacement rate-limiting strategy;
changing its internals would exceed this review's configuration-only tuning.
This behavior is covered by a test against the pinned Flink library. See
[configuration defaults](https://github.com/apache/flink/blob/release-2.2.1/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/sink/writer/config/AsyncSinkWriterConfiguration.java)
and [additive increase](https://github.com/apache/flink/blob/release-2.2.1/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/sink/writer/strategy/AIMDScalingStrategy.java).

## POJO, chaining and fairness

Java tests against the pinned libraries resolve `SensorRow` as `PojoTypeInfo`
with 12 fields. Both timestamps resolve as `GenericType<LocalDateTime>`;
tags resolve as `NullableList<String>`. Thus the outer POJO does not establish
that every nested field avoids Kryo. Generic Avro input has its own Avro type
information and serializer.

The topology test uses the production pipeline assembly and a discard sink:
widths 8, 16 and 32 each yield one job vertex. The AWS runtime plans additionally
confirm one vertex containing the real Kafka source, flatten and ClickHouse
writer at all three widths, including the four-TaskManager deployment.
`StreamingJobGraphGenerator` also checks slot sharing, operator
strategies, partitioner/exchange mode and maximum parallelism. `OperatorChain`
selects reference-passing `ChainingOutput` when object reuse is enabled.

Equal width, object reuse and memory/JVM tuning are permitted by rules 1–3.
Multiple TaskManagers additionally require the contract change proposed in
PR #76. No Kafka decode, ClickHouse
encoding or recovery guarantee is removed. Generated Avro records use the
canonical schema, Avro's compiler and Flink's public registry deserializer.

Tests also preserve nulls, ASCII-only case folding, signed integer division,
array contents, millisecond/microsecond timestamps, and buffered rows after
another input batch and the official connector's checkpoint round trip.

## Measurement changes and experiments

The harness previously selected one data-plane container's CPU/memory and
overwrote its GC summary when multiple TaskManagers were declared. Data-plane
CPU now sums all such containers; anonymous memory uses simultaneous samples.
GC and heap figures remain per JVM for multiple-TaskManager configurations;
there is no misleading sum of overlapping stop-the-world pauses. Existing
single-TaskManager metric names remain. This corrects an unexercised topology;
the published single-TaskManager measurement protocol remains v2.

The envelope document and validator previously required exactly one data-plane
container. [PR #76](https://github.com/spate-etl/benchmark/pull/76) proposes
permitting one or more for every entrant with unchanged aggregate limits.
Multi-TaskManager results depend on that separate contract proposal, as
`CONTRIBUTING.md` requires;
the existing contract does not permit this configuration. The implementation
PR now retains exactly-one-container validation and has no dependency on PR #76.

The runner `.github/aws/flink-review.py` prints its initial matrix with
`--dry-run`. It preserves the environment, corpus, five-second at-least-once
checkpoints and synchronous INSERTs. Large batches are a priority: 131,072 and
262,144 rows, a 262,144/4/500-ms Spate RowBinary settings control, and larger
batches when earlier trials justify them. Byte caps, actual batch fill,
retention, GC, checkpoints and server/merge costs are recorded together.

Screening and profiling use the tuning quarantine. Confirmation is unprofiled
and requires three repetitions per configuration and an acceptable A/A control.
Recovery is a separate diagnostic: the harness is paused while a TaskManager
is restarted, so its timing is not a performance result. Checkpoint restore
evidence and the ordinary correctness gate must both be checked.

## First AWS session: screening evidence

Session `20260910T214620Z-flink-review` ran commit `a53ced2` on the fixed
`c8gd-metal-24xl-ec2-docker` environment and self-terminated after approximately
seven hours. The corpus remained 40M messages / 2.94B retained rows, with five
second at-least-once checkpoints. All records remain tuning evidence. The
[complete screening and incomplete confirmation records](review/2026-09-10-screening.jsonl)
include failed attempts and A/A controls; nothing was added to `results/`.

The following are single screening measurements, excluding A/A twins. GC time
is the harness's reported pause total, not CPU time or an exact stopped-time
fraction. Per-JVM figures are preserved for the multi-TaskManager case.

| Case | Rows/s | Rows/s/core | CPU µs/row | Rows/INSERT | CH CPU µs/row | GC pause s | Throttled s |
|---|---:|---:|---:|---:|---:|---:|---:|
| `baseline` | 4.78M | 181,656 | 5.505 | 42,733 | 1.608 | 414.3 | 206.1 |
| `batch50k-one` | 3.29M | 129,650 | 7.713 | 39,450 | 1.605 | 667.6 | 241.5 |
| `batch128k` | 3.90M | 158,863 | 6.295 | 90,264 | 1.582 | 426.4 | 348.5 |
| `batch256k` | 3.74M | 144,647 | 6.913 | 144,190 | 1.586 | 537.9 | 654.1 |
| `batch256k-fill` | 3.10M | 121,331 | 8.242 | 208,599 | 1.583 | 664.6 | 755.9 |
| `small-buffer` | 5.73M | 456,046 | 2.193 | 12,355 | 1.718 | 30.6 | 21.7 |
| `heap34g` | 1.06M | 44,089 | 22.681 | 25,237 | 1.653 | 2545.0 | 1886.9 |
| `batch256k-heap64g` | 3.79M | 147,611 | 6.775 | 221,202 | 1.585 | 502.6 | 1059.3 |
| `spate-rowbinary-settings` | 2.03M | 84,658 | 11.812 | 44,518 | 1.629 | 1073.3 | 2428.6 |
| `inflight-two` | 5.60M | 353,660 | 2.828 | 12,292 | 1.741 | 78.0 | 141.7 |
| `fixed-budget-eight` | 3.95M | 472,255 | 2.118 | 47,918 | 1.578 | 26.6 | 7.8 |
| `fixed-budget-sixteen` | 6.28M | 425,951 | 2.348 | 24,365 | 1.630 | 42.7 | 47.6 |
| `parallel-eight` | 3.61M | 494,004 | 2.024 | 12,353 | 1.704 | 7.6 | 8.0 |
| `parallel-sixteen` | 5.34M | 489,548 | 2.043 | 12,363 | 1.713 | 15.0 | 8.0 |
| `four-tms` | 5.34M | 509,859 | 1.961 | 12,370 | 1.719 | per JVM | 35.6 |

Smaller buffers are the strongest measured improvement. The first baseline's
raw GC log contains 183 full compaction pauses and 328 "To-space exhausted"
messages. The 12,500-row / 25,000-buffered / one-in-flight case has neither,
and its reported pause total falls from 414.3 to 30.6 seconds. The GC log's
user-plus-system CPU totals fall from approximately 9,324 to 692 CPU-seconds.
These raw-log intervals are slightly wider than the measurement window, so
they support GC as the dominant avoidable cost without forming an exact
decomposition of the headline CPU measurement.

Increasing heap alone did not solve the problem. The 34-GiB process trial
produced a 28.86-GiB heap with compressed references still **enabled**, yet ran
much worse. The 64-GiB process trials had compressed references disabled and
also failed to beat smaller buffers. Neither a universal heap-size rule nor
"compressed references explain every slowdown" is supported by these data.

Large batches were exercised: the 262,144-row cases averaged approximately
144K–221K rows per INSERT. Matching Spate's 500-ms linger instead averaged only
44.5K rows and was slower per core. Matching configured batch settings therefore
did not match observed batches or effective request concurrency.

These were not all uninterrupted executions. Captured checkpoint counters show
7 restores in `batch256k`, 55 in `heap34g`, and 5 in `batch256k-heap64g`.
Exception histories include checkpoint-coordinator RPC timeouts in the first
two and failed INSERTs with broken-pipe errors in the last. Their measured cost
includes recovery and some extra writes; it cannot be attributed to batch size
or GC alone. Baseline, small-buffer, and captured 8/16-subtask jobs had no
recorded job exceptions. `duplicate_rows` examines the correctness-gate window,
so zero there does not establish zero replays across the complete drain.

The 8- and 16-subtask trials all retained the same 32-CPU container limit.
Their much lower CPU cost supports the scaling concern: multiplying per-subtask
buffer capacity while retaining the original heap made the 32-subtask baseline
GC-heavy. Equal operator widths and object reuse were already active.

Confirmation did **not** complete. Three ordinary baseline repetitions finished,
but only two ordinary candidate repetitions finished; their A/A twins are not
additional independent confirmation repetitions. Candidate A/A spread was
approximately 1.1–1.2%, while the baseline A/A spread was 17–18%. The session
reached its deadline before recovery and fresh Spate/ClickHouse Kafka controls.
The recorded acceptance verdict is false. The promising four-TaskManager
configuration is not promoted to a default or a published result.

The first AWS diagnostic attempt at `a53ced2` failed before processing records:
the Flink image's `FLINK_PROPERTIES` parser removes whitespace and concatenated
the extra JVM arguments. Both attempts are retained as failed tuning records.
The descriptor now uses Flink's `FLINK_ENV_JAVA_OPTS_TM` launcher override for
the complete argument list. A local runtime check confirmed that separate
`ParallelGCThreads=8` and `ConcGCThreads=2` arguments reach Java 17.0.20.
Unprofiled configurations with an empty extra-argument string were unaffected.

The same argument-delivery bug prevented the Parallel GC, G1 worker-count and
specific-Avro trials from starting. The fetch trial timed out; the poll-size,
Java 21, larger-than-262,144-row and recovery trials did not complete or start.
None is represented as a measured optimization.

## Targeted follow-up

The follow-up at commit `093a97b` uses `.github/aws/flink-confirm.py`, a separate
four-hour instance limit, and a 3.5-hour experiment deadline. Combined instance
time remains below the original twelve-hour budget. It prioritizes a single
TaskManager with width 16, 12,500-row batches, 25,000 buffered rows and one request
in flight, which uses one TaskManager but conflicts with the current partition-width wording; see issue #78.

It repairs profiling, tests the previously unexecuted JVM/Avro settings, then
compares three unprofiled repetitions against the baseline. The tuned candidate
is the A/A control; baseline repetition spread still contributes to the required
improvement threshold. Recovery is separate, and the Spate RowBinary control
runs only within the remaining time. Defaults remain unchanged until the
performance and recovery checks both pass. Publication requires the separate
post-merge measurement process described in `CONTRIBUTING.md`.

At the start of confirmation, the follow-up's unprofiled reference completed
at 5.38M rows/s, 512,528 rows/s/core and 1.951 µs/row. Parallel GC was close
at 508,987 rows/s/core; a 4-MiB per-partition fetch limit was worse at 423,977.
These remain single screening observations. The G1 worker-count trial reached
the screening deadline before completing. Confirmation selected the reference:
one TaskManager, 16 subtasks, generic Avro, unchanged JVM collector settings,
12,500-row batches, 25,000 buffered rows and one in-flight request.

Two review-tool defects remain material limitations of this pinned run.
The specific-Avro trial failed during graph construction because Flink's
reflective type validation rejected a concrete generated record type against
the transform's `GenericRecord` interface. The fix supplies the same output
POJO type through Flink's public `flatMap` overload; topology tests now cover
both input modes at widths 8/16/32. This fix is not a successful AWS specific-Avro
measurement.

JFR arguments now reached the JVM, but all six downloaded recording files were
empty. The observer had copied JFR's empty destination before recording finished
and skipped subsequent copies because the local path existed. Capture now
refreshes through a temporary file and retains nonempty copies. No JFR CPU or
allocation conclusions can be drawn from this run's uploaded files. Both fixes
are later than the running checkout, which remains pinned to `093a97b`.

## Adversarial review corrections

The current default is not promoted from these incomplete sessions. The next
candidate is one TaskManager at **width 32**, small buffers, and Java 21 with
`-XX:+UseZGC -XX:+ZGenerational`, as requested in the review. The official image
builds and starts that collector locally; this is not an AWS performance result.
Flink's [Java compatibility documentation](https://github.com/apache/flink/blob/release-2.2.1/docs/content/docs/deployment/java_compatibility.md)
labels Java 21 experimental. Its trials are therefore marked `tuned`, and the
runtime image and actual Java version are recorded and checked.

Widths 8/16 require resolution of [issue #78](https://github.com/spate-etl/benchmark/issues/78):
the normative text sizes consumers to the partition count. Even partition
ownership at width 16 does not itself amend that text. Four TaskManagers require
PR #76. They also provide four separate 21,504-MiB process budgets (84 GiB total)
and approximately four times the configured heap: roughly 68.6 GiB versus
17.2 GiB for one TaskManager. Both fit the same 96-GiB container envelope, but
process layout and usable heap change together; this was not a topology-only test.

The identical first-session baseline spans **94,259–181,656 rows/s/core** across
the session, approximately a 1.93× range. The first measurement is about 1.61×
the median of the subsequent six. The narrower 17–18% A/A spreads do not describe
that full drift. Infrastructure was reused throughout the ordered ladder; the
64-GiB trials preceded later slower baselines. Timing establishes no cause.
`batch128k`, `batch256k` and `batch256k-heap64g` are **not separable from this
control variation**. Their single-trial ordering is not a batch-size ranking.
The smaller-buffer mechanism is supported by the repeated large efficiency
difference and raw GC evidence, but its effect size still needs confirmation.

The table now includes throttling and ClickHouse CPU per row. `throttled_us`
is cgroup throttled duration, not CPU-seconds consumed or necessarily wall time
lost by the application. `nr_throttled` was sampled but not emitted in the
historical records; the harness now emits it. GC and throttling rise together,
but correlation does not establish that throttling explains all of the loss.
In particular, the proposed sum of 23 G1 parallel GC workers and 32 application
threads during a young collection is incorrect: evacuation is
[stop-the-world](https://docs.oracle.com/en/java/javase/17/gctuning/garbage-first-g1-garbage-collector1.html).
Concurrent GC work and other runnable threads can compete for quota, and bursts
can exhaust a period's quota despite lower average CPU use. See the
[kernel's bandwidth-control description](https://www.kernel.org/doc/html/latest/scheduler/sched-bwc.html).
Capping GC workers is a high-priority hypothesis, not an established win.

For the first-session baseline and width-32 small-buffer trial, ClickHouse CPU
rises from approximately 1.608 to 1.718 µs/row (+6.8%), about 323 extra CPU-seconds
over 2.94B rows. Arm CPU falls by about 3.31 µs/row, roughly 9,738 CPU-seconds;
the extra target cost is about 3.3% of that saving. This comparison shares the
ordered-screening limitation. More INSERTs move a small but real cost outside
the arm envelope. The table exposes it rather than treating arm efficiency as
the entire system's cost.

The observed wire density of about 64.5 bytes/row makes 262,144 rows roughly
16.9 MB, below the large-trial 64-MiB byte cap. At 3.74M rows/s divided by 32
subtasks, that row cap takes about 2.24 seconds of average output to fill,
longer than the 1-second timer. The trial averaged 144K rows/INSERT; extending
linger to five seconds reached 209K. This arithmetic supports timer pressure,
but checkpoint flushes, backpressure and recovery also affect batch fill.
At the width-16 small-buffer rate, 262,144 rows would take about 0.8 seconds
per subtask **if that rate persisted with the larger buffers**. That is an
untested cell, not evidence that large buffers preserve the GC win. Width-16
25,000-row batches also deserve comparison: 6.28M rows/s exceeded the selected
12,500-row case by about 18%, while CPU efficiency was lower.

Screening used REST polling and file copying; unprofiled confirmation did not.
The screening table carries that diagnostic overhead. The revised runner does
not select the maximum of a single-trial ladder: the Java 21/ZGC candidate is
predeclared, and a 0.7% reference/Parallel-GC difference is treated as unresolved.
Recovery runs first. Each configuration receives its own A/A twin in each of
three repetitions, with pair order reversed between repetitions. Acceptance
requires both A/A families to pass, improvement beyond measured noise, no
greater than 2% raw-throughput regression, and no increased duplicate count in
the correctness window. Throttling and downstream CPU remain explicit in the
verdict. A six-hour remaining-budget check precedes all work; an undersized
session is refused. Additional controls and mechanism probes require their own
budget, rather than being promised time that confirmation will consume.

The proposed additional probes are GC worker limits at width 32, a 256-MiB
network-memory cap, and specific Avro with custom coders. The source default
network reservation is approximately 10% of Flink memory, about 2 GiB here;
a single chained vertex suggests it can be reduced, but the resulting heap
and runtime behavior must be measured. `avro_mode=specific` now enables custom
coders automatically. JVM option typos fail startup, GC logging cannot be
overridden through the tuning knob, and Java/Python/launcher tests run in CI.
