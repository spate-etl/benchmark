---
id: reproduce
title: Reproducing this
description: How to build the arms and run the benchmark yourself.
---

Everything here runs from a clone of
[the repository](https://github.com/spate-etl/benchmark). No number on
this site comes from anywhere else.

## What you need

- Docker, with enough headroom for the infrastructure and one arm at a time.
  The environment profile in `environments/` declares the exact allocations.
- A Rust toolchain. The version is pinned in `rust-toolchain.toml` and must match
  the arm image's base — a test asserts it, because codegen moves throughput and
  a silent divergence would make the recorded toolchain wrong.

## The commands

```sh
bench list                      # systems, variants, and when each was last measured
bench validate                  # what CI checks, runnable locally
bench build '*'                 # build every entrant image
bench prefill                   # populate the topic once per corpus
bench ceiling                   # report the ceilings, and refuse if none is gateable
bench ceiling --measure --write # re-measure them against this corpus and record it
bench run '*' --reps 3          # every arm, interleaved
bench run spate --reps 3        # one system; nothing else is touched
bench run '*' --dry-run         # print the plan without running it
bench run spate --mode sustained --rate 40000    # latency; has to be asked for
```

`--dry-run` is worth using before any full sweep. It prints the exact execution
list — one line per arm, with its entrant, variant, wire format and knobs
— which is how you check that "only Spate" really means only Spate before
spending hours finding out. (Image digests are resolved and recorded per arm
when a run actually executes, not in the dry-run.)

## Two properties worth knowing

**Runs are interleaved, not batched.** `bench run` alternates between arms rather
than completing all of one and then all of the next. This is not fastidiousness:
running arms in sequence has already manufactured a fake 30% difference in a
related project, because the machine is not in the same state at the end of a
long run as at the start.

**Nothing appends over anything.** `bench run` only ever appends, and there is no
code path in it that truncates a results file. Re-running one system produces a
diff confined to that system's file, and a number later found to be wrong is
corrected in a commit of its own.

## If you get a different number

That is useful and I would like to know. The most likely causes, in order:

1. **A different environment.** Add your own environment profile rather than
   comparing across; the site will refuse to draw them on one axis, which is
   the intended behaviour rather than an obstacle.
2. **A busy machine.** Background load moves throughput materially, which is
   why the site shows run-to-run spread on every chart.
3. **A real defect in the harness.** Open an issue. A benchmark that cannot be
   reproduced is a claim, not evidence.

## No credentials required

Every arm, including Spate's, builds from a clean clone of this repository. The
framework is consumed from crates.io and pinned in `Cargo.lock`, so the arm that
is ours is exactly as reproducible as every arm that is not.

## Reproducing the cloud environment

Published runs execute on a disposable EC2 box — one on-demand
`c8gd.metal-24xl` (Graviton4: 96 vCPUs that are 96 physical cores, no SMT, no
hypervisor), Ubuntu 24.04 arm64, with ClickHouse, the broker and Docker each on
one of the instance's three local NVMe devices, so storage is not a figure
anyone has to provision. What the box actually executes is public
and versioned at the SHA it ran: `.github/aws/userdata.sh.tpl` boots it and
`.github/aws/run-bench.sh` builds the harness from the clone at that SHA and
runs the suite. The instance type, volume, AMI and those scripts are the whole
of what makes a number reproducible, and they are all here.

The cloud plumbing that spends the money — the launcher, the collector, and the
AWS Terraform — runs from a **private operations repository**, because the
benchmark shares an AWS account and its account shape is not something to
publish. It changes nothing you can reproduce: a run executes this repo at an
approved commit and its results come back as an ordinary, validated pull
request. A full sweep costs roughly the instance-hours it takes at ~$5.6/hour,
bounded by a 36-hour TTL.
