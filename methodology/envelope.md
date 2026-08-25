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

## Why memory is generous, and what that does to the memory number

CPU is the scarce resource here and memory is not: one arm runs at a time, so
the host's memory only ever holds one envelope beside the infrastructure. Every
arm gets 96 GiB — 3 GiB per envelope CPU — against a largest measured peak of
56 GiB (vector, whose prefetch and in-flight batches scale with its request
concurrency). JVM process totals are sized to the container by one rule,
container limit minus limit/8 slack, and their direct-memory bounds scale with
the task count; `entrants_are_valid` enforces both.

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

Every arm publishes `peak_anon` and `memory.peak`. JVM arms publish configured,
committed and live heap beside them (`jvm_heap_*`), so the gap between
allocation and use is visible rather than implied.

Infrastructure sits **outside** that budget and is identical for every arm, and is
declared per environment rather than passed on the command line: Redpanda
(3 CPUs, 8 GiB) and ClickHouse (32 CPUs, 32 GiB) in the committed environment
profile.

Those numbers are the output of a measured ladder rather than a guess. The
consume ceiling is flat from 8 to 32 partitions and from a 3-core broker cap to
an 8-core one (the broker never uses 2 full cores while serving it), so the
broker keeps the smallest cap that does not constrain it. ClickHouse ingest
stops scaling at about 28 cores — RowBinary reaches 6.0M rows/s at a throttled
16-core cap, 11.0M at 32, and the same 11.0M at 48 and 64 with the extra cores
idle — so it gets 32, the smallest cap at that plateau. Every core past the
plateau buys background merges rather than ceiling, which is contention inside
the measurement, not headroom. The host bounds the total: 3 + 32 + 32 leaves 29
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
broker can actually absorb at those caps. **An arm exceeding 70% of either ceiling
is infra-bound and cannot be published as a system comparison** — at that point we
are measuring ClickHouse, not the system. If arms hit the ceiling, an envelope
moves until they are engine-bound.

**Which envelope moves is a diagnosis, not a preference.** Shrinking the arms is
right when the arms are too big for the rig around them. When the infrastructure
is the thing at its cap, shrinking every arm makes the comparison smaller for all
of them and leaves the fault in place. Read the cgroup counters on both sides
first, and move whichever is at its cap. Such a run is recorded with
`status: infra_bound` rather than discarded, so "we ran it and it blew the limit"
is distinguishable from "we never ran it".
