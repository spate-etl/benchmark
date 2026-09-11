# Flink fairness review

Three AWS sessions against benchmark `9b4cab3`, Flink source tag `release-2.2.1`
and official ClickHouse connector tag `v0.2.0`, on the fixed
`c8gd-metal-24xl-ec2-docker` environment. Tracking issue:
[#74](https://github.com/spate-etl/benchmark/issues/74).

Every record cited here carries `trigger: tuning` and lives in
[`review/`](review/), never in `results/`. The configuration the search settled
on is declared in the descriptor and is measured again cleanly as a published
run; no number below is a published number.

## What changed, and what it did

The default was 50,000-row batches, 100,000 buffered rows, two requests in
flight and generic Avro. It is now **12,500 / 25,000 / one request in flight,
with generated (specific) Avro records**.

Confirmed over three interleaved repetitions, each arm carrying its own A/A
control, pair order reversed on alternate repetitions
([`review/2026-09-11-session.jsonl`](review/2026-09-11-session.jsonl)):

| | baseline | candidate |
|---|---:|---:|
| rows/s/core (median of 3) | 121,496 | **523,082** |
| rows/s (median of 3) | 3.079M | **5.705M** |
| cores used | 25.4 | 10.9 |
| repetition spread | 14.90% | **2.12%** |
| A/A controls | 10.17%, 7.43%, 49.57% | 2.68%, 0.79%, **0.65%** |
| throttled | 310.6 s | **13.2 s** |
| duplicate rows | 0 | 0 |

**+330.5% per core and +85.3% raw throughput**, so the per-core figure is not
bought by throughput. Checkpoint recovery was exercised on the candidate before
confirmation: the TaskManager was restarted mid-drain and all 32 subtasks
restored, with no duplicates and no job exceptions.

### The recorded verdict is `accepted: false`, and the reason is the baseline

`confirmation_summary` requires every A/A control to sit within the declared 2%
noise floor. All three candidate controls did. The baseline's did not — 10.17%,
7.43% and **49.57%**, the last being the same configuration measured twice in
one sweep at 129,103 and 77,819 rows/s/core.

Six observations of the baseline across this session span **1.73×**, against a
candidate holding 2.12%; the two were interleaved in the same sweeps, on the
same box, minutes apart. The instability is a property of the configuration
rather than of the rig, and the first session saw the same thing across a wider
range (94K–182K on fixed knobs).

Promotion here is a deliberate decision to publish over a gate that failed on
the arm being retired. It is recorded rather than reconciled: the candidate
confirmed, and the configuration it replaces could not hold still.

## Why throughput was X and not 2X

The arm is **blocked, not saturated**. Flink reports every subtask 100% busy
with `idle = 0` for the whole drain, while the cgroup consumes 10.9 of its 32
cores — so a task thread is computing about a third of its wall time and
blocked the rest inside the sink.

Per subtask: 179,438 rows/s at 12,377 rows per INSERT is a **69 ms** cycle, of
which roughly 23 ms is CPU and **46 ms is waiting for ClickHouse to
acknowledge**, with one request in flight. Nothing is at a cap — the broker
serves 9% of its ceiling, ClickHouse absorbs 52% of its ingest ceiling, GC
accounts for 28 s of a 515 s window and throttling for 13 s.

More concurrency does not help: at the same batch size, two requests in flight
measured *lower* throughput (5.596M against 5.726M) and 26% more CPU. The lever
is batch size, and it is bounded by retention — see below.

## Where the CPU goes

A 60-second `settings=profile` flight recording of the candidate, 21,081
execution samples, attributed by caller:

| Share | Frame | Owner |
|---:|---|---|
| 14.0% | `BinaryStreamUtils.writeUnsignedInt16` ← `DataWriter.writeUInt16` | connector |
| 8.3% | `Arrays.copyOf` ← `ByteArrayOutputStream` grow and `toByteArray` | connector |
| 8.9% | `HashMap.putVal` ← `SensorRowMapper.toMap` | connector's payload contract |
| 6.3% | `Utf8.toString` ← `BinaryDecoder.readString` | Avro |
| 5.7% | `DataWriter.writeArray` | connector |
| 5.5% | `LinkedHashMap.newNode` | connector |
| 5.0% | `Rows.asciiUpper` ← `FlattenEvents.flatMap` | this arm |
| 4.5% | `BinaryStreamUtils.writeVarInt` | connector |
| 3.8% | `ZoneId.ofOffset` ← `DataWriter.writeDateTime64` | connector |

**Roughly 60% of the arm's CPU is inside the ClickHouse connector's RowBinary
encoder and Avro's decoder.** After the change above, this arm is bounded by the
connector rather than by Flink.

`Utf8.toString` is not a sign that specific Avro failed: the generated classes
carry `"avro.java.string":"String"` for every string field, and Avro's own
`BinaryDecoder.readString` implements String-reading as Utf8-then-convert.

### Three connector findings for upstream

Per rule 7, and worth more than anything this arm can do on its own side:

1. `DataWriter.writeDateTime64` resolves `ZoneId.of("UTC")` per value — 3.8% of
   arm CPU across two `DateTime64` columns per row.
2. `ClickHouseConvertor.applyPOJOImpl` builds a per-row `ByteArrayOutputStream`
   that grows and is then copied by `toByteArray()` — 8.3%.
3. The payload is a per-row `LinkedHashMap` of boxed values, retained alongside
   the encoded bytes until the batch flushes — 14.4% between `putVal` and
   `newNode`, and the dominant contributor to the retention below.

That retention is also the mechanism behind the headline result. At the old
defaults, 32 subtasks × (100,000 buffered + 2 × 50,000 in flight) is 6.4M live
payloads; session one's GC logs show G1's Eden collapsing to a single 16 MiB
region at the 25th percentile, the heap pinned at 98% occupancy doing
`G1 Preventive Collection`s, and **18,325 CPU-seconds of GC in one run — about
70% of the arm's entire CPU**. Smaller buffers remove the live set that causes
it.

## What was measured and rejected

| Cell | rows/s/core | vs reference | Verdict |
|---|---:|---:|---|
| `tuned-ref` (12,500/25,000/1, generic Avro) | 440,651 | — | the screen's reference |
| **`specific-avro`** | **522,678** | **+18.6%** | promoted |
| `network256m` (`taskmanager.memory.network.max: 256m`) | 402,252 | −8.7% | rejected |
| `java21-g1` | 393,745 | −10.6% | **not interpretable** |
| `java21-zgc` | 306,850 | −30.4% | rejected |

The Java 17 screen's A/A control was 1.77%, inside the noise floor. The Java 21
screen's was **15.64%** — `java21-g1` measured 393,745 against its own twin's
460,530 — so neither Java 21 cell separates and both need re-measuring before
anything is claimed about the runtime.

Generational ZGC did what it promises and lost on the metric that is published:
GC pause fell to **0.1 s** from 23–37 s, while CPU rose to 17.26 cores and
throttling to **423.9 s**. Load barriers and concurrent collection are CPU, and
this comparison is scored on CPU. It also maps its heap from a `memfd`, which
the kernel charges to `shmem`: it reported 17.15 GiB there, and under the
pre-`shmem` footprint definition would have published 4.69 GiB against a true
21.84 GiB.

`network256m` returns 1.72 GiB from unused network buffers to the task heap —
verified against Flink's own memory calculator — and still lost, with throttling
rising from 15.3 s to 36.2 s.

## Not measured, and why

**The G1 young-generation floor was never tested.** Both cells declared
`-XX:G1NewSizePercent=40` without `-XX:+UnlockExperimentalVMOptions`; the
entrypoint disables `IgnoreUnrecognizedVMOptions`, so the JVM refused to start
and each cell recorded a TaskManager that exited during the drain. The flag is
fixed and the launcher check now asserts that every JVM option the runner
declares actually starts a JVM.

**A larger batch at width 32 was never tried with one request in flight.** Given
the sink-latency finding it is the strongest remaining lever: 25,000 rows would
halve the round trips per row delivered. Session one's only 25,000-row cell ran
at width 16 with two in flight and produced that session's highest raw
throughput, 6.28M rows/s.

**Reduced widths remain deferred** under
[issue #78](https://github.com/spate-etl/benchmark/issues/78): the contract sizes
consumer width to the 32-partition topic, and even partition ownership at width
16 does not amend that text. Four TaskManagers have no open proposal —
[PR #76](https://github.com/spate-etl/benchmark/pull/76) was closed — so the
four-TaskManager records of session one remain historical evidence that nothing
here is selected from.

## Comparability of the published re-run

The re-measurement this search feeds runs **Flink alone**, so its record lands
beside numbers the other arms were measured with earlier. Every hard key
matches: `env_id`, `env_digest` `2aba9d60a529`, `harness_version` 2,
`dataset_version` `d2-60d7e5bb2a82` and `infra_digest` `6c8fc2dcbfeb`.

ClickHouse has moved from `26.3.22.7` and `26.3.23.7` to `26.3.33.24`.
`infra_digest` excludes versions by design — a ClickHouse patch release is soft
provenance, recorded per record and rendered as a footnote — and the archive
already spans two of them. The jump is wider than the last one, and ClickHouse
is the shared sink, so if its ingest behaviour moved then this arm's number
carries that and the others' do not. Stated here rather than left implicit.

## Cost moved to the server

Smaller batches mean more INSERTs: 12,374 rows per INSERT against roughly
39,250, so ClickHouse CPU per row rises from **1.624 µs to 1.720 µs, +5.9%**.
Against an arm-side saving of about 4.4 µs per row that is roughly 2% of the win
moved rather than removed, and it is disclosed here rather than left to be
found.

## Instrument limitations in these sessions

- Per-vertex REST metrics 404'd on every poll of session three: the metric ids
  were appended to the query string unencoded, and this job's chained vertex name
  carries spaces and `>`. Busy and idle above were recovered from
  `accumulated-busy-time` on the job resource instead, which carries no sink
  latency, so the 46 ms is arithmetic rather than a reading. Fixed.
- A recovery run's CPU figures are invalid by construction: restarting the
  TaskManager destroys its cgroup, the sampler exits when the cgroup vanishes,
  and the window is left with a tenth of its samples. Recovery is a correctness
  and restore check only.
- Screening cells carry the diagnostic observer's REST polling; confirmation
  runs without it.
- Session one's control drifted from 181,656 to about 113,000 rows/s/core across
  seven hours on fixed knobs with the infrastructure reused throughout. Its
  single-trial ordering is unresolved within that range, and its batch-size
  ladder is not separable from it.
