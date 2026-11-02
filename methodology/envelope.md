# The resource envelope

Part of [the fairness contract](README.md). What each system is given, what
the infrastructure around it is given, and the headroom rule that decides
whether a number describes the system or the rig.

**32 CPUs and 96 GiB of _data plane_ per system.** A system's control plane — a
Flink JobManager, a Connect worker's coordinator — is allocated **on top** of that
budget, and its **measured** consumption is published alongside the arm's total
rather than pre-charged against it.

The envelope is a **fairness constraint between arms, not a share of the host**,
and it is sized from the host and the partition count — never from any arm's
scaling curve, which would size the contract to one entrant. It is 32 because
the host affords one envelope CPU per partition for every arm identically:
32 partitions bound how much consumer parallelism an arm can spend, and a wider
envelope than the topic cannot be spent. On the retired 32-core host the
envelope was 6 — what remained beside the infrastructure — and at 6 CPUs
against 8 partitions no arm exercised vertical scaling at all, which penalised
the frameworks built for parallelism hardest. The 96-core host inverts that
arithmetic, and the arms that gain are exactly the ones the old cap bound:
vector spends 31.8 of the 32 CPUs the moment they exist.

This is a deviation from the more obvious rule ("32 CPU / 96 GiB total, control
plane included"), it is deliberate, and it is disclosed here and on the site
because it favours the multi-process arms:

> Charging a whole JobManager against a single TaskManager is an artefact of
> running one TaskManager. In production one JobManager serves an entire cluster,
> so a per-TM share of it is a rounding error. Measurement bears this out — the
> JobManager consumed **0.030 cores** at `parallelism = 32` on the current rig
> (0.066–0.088 at `parallelism = 8` on the retired one), so charging it a full
> core would tax Flink over 30× its real cost, and the resulting "win" would
> be an artefact of our own accounting.

Every arm therefore publishes two figures: the arm total, and
`data_plane_cores_used` / `data_plane_peak_anon_bytes` for the data plane alone. A
reader who disagrees with this rule can apply the stricter one from the published
numbers; a reader given only a blended total could not.

Each entrant declares its containers with roles in `[[envelope.container]]`, and
validation asserts that exactly one is `data-plane` and that the data-plane
containers sum to the declared `[envelope]` totals. The driver applies exactly
what is declared, then **reads the caps back out of the running containers' cgroups
and asserts they match**. A mismatch fails the run; it does not warn.

Swap is disabled (`--memory-swap` equals `--memory`) so memory pressure surfaces
instead of hiding in a swapfile.

**The published footprint is `anon` + `shmem`**, read from the container's own
`memory.stat`, and excludes the rest of the page cache. The quantity being
compared is memory the system caused to exist and is holding, rather than cache
the kernel populated on its behalf for reading its own input — and `anon` alone
is a proxy for that which one runtime breaks. A JVM using ZGC maps its heap from
a `memfd`, which the kernel charges to `shmem` rather than to `anon`: on one JVM
holding the same 600 MiB live set, G1 reports `anon=691M shmem=0` and
generational ZGC reports `anon=57M shmem=805M`. Counting `anon` alone would
publish that arm at a twelfth of its real footprint while it in fact held more,
which is a fabricated memory win in the direction this benchmark can least
afford. Because swap is disabled, swap-backed memory is exactly as irreducible
as anonymous memory, so adding it generalises the existing definition rather
than replacing it; every arm measured before the change reported `shmem` of
zero. `peak_shmem_bytes` is published beside the total so the split is visible
per record and the older definition can be recomputed from it.

## Why memory is generous, and what that does to the memory number

CPU is the scarce resource here and memory is not: one arm runs at a time, so
the host's memory only ever holds one envelope beside the infrastructure. Every
arm gets 96 GiB — 3 GiB per envelope CPU — against a largest measured peak of
56 GiB (vector, whose prefetch and in-flight batches scale with its request
concurrency). A JVM arm is sized to its own measured live set rather than to the
container, and `entrants_are_valid` bounds each one between the 24 GiB-era
sizing as a floor and the compressed-oops boundary as a ceiling.

Where that live set is small, the era sizing stands and the envelope's extra
memory serves the page cache and native buffers: Flink's sink retains ~350 MiB,
and every larger heap tried for it measured slower, because GC churn grows with
the heap while the live set does not. Where it is not small, the heap follows it
— Kafka Connect holds a whole poll of decoded records per task until its insert
returns, ~10 GiB across 32 tasks, and a heap sized for Flink's live set collects
that continuously.

Two things decide the ceiling, and both are measured per arm rather than assumed.
The compressed-oops boundary depends on object alignment — a heap above
`32768 - HeapBaseMinAddress` loses zero-based addressing at the default 8-byte
alignment, and above `65536 - HeapBaseMinAddress` at 16 — so an arm that wants
more than ~30 GiB pays either a base add on every reference or, past the
right-hand edge, 8-byte references throughout. And a heap large enough that the
collector never runs inside the drain window publishes deferred work as
throughput, which is the mirror of the rule below. Read both off the arm's own
`gc.log`; neither is safe to infer.

That is a fairness decision rather than a convenience. A garbage-collected
runtime held to a tight heap collects more often, and the resulting pauses would
be an artefact of *our* allocation choice rather than a property of the system.
Sizing a JVM down until it strains and then publishing its pause distribution is
a way to win an argument on purpose. The same allowance goes to every arm
including the Rust one, which will leave most of it untouched.

**The honest cost is that the memory figure stops being a requirement and becomes
a revealed preference.** Under a tight cap, peak anonymous memory approximates
what a system *needs*. Under a generous one it approximates what a system
*chooses to use when nothing forces it to economise* — a JVM will grow its heap
toward its maximum under load without ever being close to needing it. Both are
real quantities, but they are different ones, and this suite measures the second.

So the memory panel is labelled as what it is and is **not** presented as a
minimum footprint. "How small can this run?" is a different question, and
answering it properly means a separate sweep that tightens each arm until it
degrades. That would be worth publishing; it is not what these numbers are.

Every arm publishes `peak_anon` (with its `peak_shmem` component) and
`memory.peak`. JVM arms publish configured,
committed and live heap beside them (`jvm_heap_*`), so the gap between
allocation and use is visible rather than implied.

Infrastructure sits **outside** that budget and is identical for every arm, and is
declared per environment rather than passed on the command line: Redpanda
(3 CPUs, 8 GiB) and ClickHouse (48 CPUs, 64 GiB) in the committed environment
profile.

Those numbers are the output of a measured ladder rather than a guess. The
consume ceiling is flat from 8 to 32 partitions and from a 3-core broker cap to
an 8-core one (the broker never uses 2 full cores while serving it), so the
broker keeps the smallest cap that does not constrain it. ClickHouse gets 48
cores because 32 is the binding cap at the arms' rates on the synchronous insert
path: the Spate native arm at about 35M rows/s holds a 32-core server at about 31
cores, throttled 3.8–4.6 s per second, and leaves a 48-core server unthrottled at
about 32 cores. The arm's rate is the same under both caps, so the extra cores
remove CPU contention from the server during the window and add no throughput.
The host bounds the total: 3 + 48 + 32 leaves 13
of 96 cores for the driver, the sampler and the operating system.

One consequence of a 32-partition topic worth stating normatively: a consumer
thread count below the partition count leaves the slowest consumer owning two
partitions, and the drain runs at that consumer's pace. Per-entrant parallelism
knobs are therefore sized to the **partition count**, not to the CPU count — on
this rig the two rules land on the same number.

The Schema Registry is **Redpanda's built-in, Confluent-compatible one** on port
8081 rather than a separate Confluent container. That removes a second JVM from
the measurement environment entirely and returns a CPU and a GiB to the infra
budget, which is headroom the ceiling pass needs. It speaks the same REST API that
Kafka Connect's `AvroConverter` and ClickHouse's `AvroConfluent` expect. Host-side
it is published on `localhost:18081`; containers reach it at
`http://spate-bench-redpanda:8081`.

Before any arm is published, a ceiling pass measures what ClickHouse and the
broker can actually absorb at those caps. Two rules then decide whether a number
describes the system or the rig, and a run that breaks either is infra-bound and
cannot be published as a system comparison.

**An arm exceeding 70% of the broker's consume ceiling is infra-bound.** At that
point we are measuring the broker, not the system.

**An arm is infra-bound when ClickHouse was saturated over its measurement
window.** The harness samples ClickHouse's cgroup over the arm's own window and
reads the arm's inserts back out of `system.query_log`, and any one of three
tests marks the run server-bound:

- ClickHouse was throttled for at least 1% of its cap's time over the window;
- it used at least 90% of its CPU cap over the window;
- it held the arm's inserts back (`DelayedInsertsMilliseconds`) for at least 5%
  of their insert time (`query_duration_ms`).

The lines come from the reference host with ClickHouse 26.8, under the Spate
native arm at about 35M rows/s. At a 32-core cap ClickHouse used about 31 cores
and was throttled for 3.8–4.6 s of every second, about 13% of its cap's time. At
48 cores it used about 32 and was throttled for at most 7.5 ms a second, about
0.02%. Insert delays peaked at 1.7% of insert time with the stock merge pool and
were zero with a larger one, at the same arm rate. So 5% separates a server that
slows the arm from one that only delays a few of its inserts, and 1% of throttled
time sits far from both readings of the cgroup.

When the window's ClickHouse counters are missing, a test that did fire still
decides, and otherwise the arm is held to 70% of the ClickHouse ingest ceiling
for its wire format. Every record says which rule decided
(`run.infra.clickhouse_gate`: `measured_saturation` or `ceiling_share`), carries
a flag for each test that fired (`clickhouse_throttled`,
`clickhouse_cpu_saturated`, `clickhouse_inserts_delayed`), and publishes the
figures the tests read: `ch_cores_used`, `ch_throttled_share` and
`ch_delayed_insert_share`. If arms hit either limit, an envelope moves until they
are engine-bound.

**Which envelope moves is a diagnosis, not a preference.** Shrinking the arms is
right when the arms are too big for the rig around them. When the infrastructure
is the thing at its cap, shrinking every arm makes the comparison smaller for all
of them and leaves the fault in place. The record carries ClickHouse's counters
beside the arm's own, so read both, and move whichever is at its cap. Such a run
is recorded with `status: infra_bound` rather than discarded, so "we ran it and
it blew the limit" is distinguishable from "we never ran it".
