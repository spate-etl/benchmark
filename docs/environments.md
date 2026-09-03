---
id: environments
title: Environments
description: The hardware every number is tied to, and why results are never compared across machines.
---

An environment is the unit of comparability for hardware. Every record carries an
environment id, and this site never draws two environments on one axis.

That is why an environment is a committed profile with a stable id rather than a
hostname. A hostname is not a hardware disclosure — it cannot be compared across
machines and tells a reader nothing they can reproduce against. Each record also
carries a digest of the profile, so editing it later cannot retroactively
re-describe runs that already happened.

## `c8gd-metal-24xl-ec2-docker` — the environment

**Class: authoritative.** A fresh machine per run, launched by
[the pipeline in this repository](reproduce.md#reproducing-the-cloud-environment)
and terminated when the run ends, so no state survives between measurements.

| | |
|---|---|
| Host | AWS EC2 c8gd.metal-24xl, on-demand, bare metal |
| CPU | Graviton4 — 96 physical cores, homogeneous, no SMT |
| Memory | 192 GiB |
| Storage | 3 × 1,900 GB local NVMe SSD; EBS gp3 200 GiB root |
| OS | Ubuntu 24.04, Docker CE, arm64 |

### Why it earns the class

Every vCPU is a dedicated physical core: Graviton has no simultaneous
multithreading, and on a metal instance there is no hypervisor at all, so a
cgroup CPU cap means what it says. Docker here is Docker CE on Linux —
containers are plain cgroups, the same mechanism the envelope enforcement and
the sampler read, with no VM between the harness and the kernel. A JVM on this
box **is** a JVM on Linux.

ClickHouse and the broker each write to their own local NVMe device, and
Docker's data root — where every arm container's writable layer lives — has a
third. The write path and the read path under test therefore do not share a
queue. Nothing about the disk is provisioned, so nothing about it is a number
this profile has to choose or defend.

The instance is rentable by anyone, which is the point: the environment is
reproducible with an AWS account and this repository, not with access to our
hardware.

## The envelope

**Per system: 32 CPUs and 96 GiB of data plane.** A control plane — a Flink
JobManager, a Connect coordinator — is allocated on top, with its *measured*
consumption published alongside the arm's total rather than pre-charged against
it. Swap is disabled so memory pressure surfaces rather than hiding.

Memory is deliberately far more than any arm needs, so that no garbage-collected
runtime is penalised for an allocation we chose.
[The resource envelope](./contract/envelope.md) states what that does to the memory
number,
which is the honest cost of the choice.

**Infrastructure sits outside every arm's budget** and is identical for all of
them: Redpanda, ClickHouse and the topic's partition count are declared in the
environment profile rather than passed on the command line — which is the fix for a real failure, where a runner script, the
driver's defaults and the written methodology stated three different envelopes
while no record said which had been in force.

The driver applies what is declared, then **reads the caps back out of the
running containers' cgroups and asserts they match.** A mismatch fails the run
rather than warning.

## The headroom rule

Before any arm is published, a ceiling pass measures what the shared consume path
can absorb. **An arm exceeding 70% of that ceiling is infra-bound and cannot be
published as a system comparison** — above it we are measuring the broker and
ClickHouse, not the system.

Such a run is recorded with a failed status rather than discarded, so "we ran it
and it blew the limit" stays distinguishable from "we never ran it".

## Adding one

Add a profile to `environments/`, run against it, and the site will keep its
results in their own comparability group automatically. Results from hardware we
do not control are welcome and are flagged as such — no rendering logic is
needed, because a different environment id already separates them.
