---
id: limitations
title: Limitations
description: What this benchmark does not measure, where it is weakest, and how it could mislead you.
---

Every benchmark is an argument, and every argument has a shape it cannot make.
This page is the list of things a sceptical reader would raise, written before
they raise them.

## It is one workload, chosen by the author of one of the entrants

Kafka → Avro → ClickHouse is the pipeline Spate was built for. A workload chosen
by someone else would be a stronger test. The corpus, the DDL, the schema and the
rules are all committed precisely so that someone else *can* argue with the
choice — but the choice was still ours.

## It measures throughput per unit of CPU, and almost nothing else

Not covered, and not claimed:

- **Windowing, keyed state, watermarking, event time.** Flink has all of it and
  Spate has none of it. If your problem is "aggregate over time windows keyed by
  user", this comparison is not about your problem and Flink is the answer.
- **Exactly-once.** Every arm here runs at-least-once. A system that can only
  offer exactly-once would pay for it, and that would be a real cost this
  benchmark is not measuring.
- **Failure and recovery.** Nothing here kills a broker mid-run, withdraws
  ClickHouse, or measures what each system does about it. That is a planned
  section and it is likely the most interesting one, because it is where systems
  genuinely differ.
- **Multi-node scaling.** One process, one container, one node. Flink's job graph
  and Connect's worker model exist to scale across machines; measuring them on
  one machine measures the part of them that matters least.
- **Schema evolution, backpressure from a degraded sink, mixed workloads,
  operational cost, ecosystem, hiring pool.** All real, none measured.

## The memory number is not a minimum footprint

Every arm is given far more memory than it needs, deliberately, so that no
garbage-collected runtime is penalised for an allocation *we* chose. The
consequence is that the memory figure measures what a system chooses to use when
nothing forces it to economise, not what it requires. See
[the resource envelope](./contract/envelope.md) for the full argument. If you want "how small
can this run?", that is a different sweep and it has not been done.

## The host is rented

Measurements run on a fresh EC2 c8gd.metal-24xl per run — bare-metal Linux,
homogeneous physical cores, no hypervisor, ClickHouse and the broker each on
their own local NVMe device. What that leaves: AWS is a shared platform whose
behaviour we do not control, only pin and disclose, and one instance of one
type in one region is not every machine a system will meet.

## The transform is measured, but it is a very small one

The workload's transform — decode, flatten, filter, derive — is not a hard test
of transformation. It is two predicates, a coalesce, an integer division and an
ASCII uppercase, all per-row and entirely stateless, so it separates systems on
per-row compute cost and on nothing else. The work that actually distinguishes
a stream processor from a pipe — windows, joins, keyed aggregation — is not in
the workload, for the reason given above.

## Most systems are not here yet

Three of the six declared entrants are unimplemented. Their blockers are written
down in [the roadmap](./roadmap.md) rather than left implicit, because "we only
measured the ones we beat" is exactly the accusation a partial comparison invites
and the only defence is to name what is missing and why.

## All benchmarks are liars

Including this one. The numbers are produced honestly and the method is published
in full, and neither of those makes a result transferable to your workload, your
data shape, your hardware or your operational constraints. Use it to form a
hypothesis, then measure your own pipeline.
