# Spate Benchmark

A published, reproducible comparison of streaming ETL systems on one fixed
pipeline: **Kafka → Avro → ClickHouse**.

Results: **https://spate.kainth.dev/benchmarks/**

## Who runs this, and why that matters

This benchmark is built and run by the author of [Spate][spate], which is one of
the systems it measures. That is a conflict of interest, and the only useful
response to one is to make it impossible to hide:

- Every Spate row on the site carries a "run by the vendor" marker, driven by
  `vendor = "self"` in its entrant descriptor — not by anything hardcoded in the
  site.
- **No published number is reported by the system that produced it.** Throughput
  is `SELECT count()` against ClickHouse; CPU and memory are cgroup v2 counters
  read by a sidecar container; correctness is a query against the rows that
  actually landed. A framework's own metrics are available for debugging and are
  never read as results.
- Every competitor configuration is in this repository, in full, and we intend to
  send them upstream and ask whether we handicapped anyone. Whatever comes back
  gets linked — including "they told us to change X and we did".
- Where we lose, that is published with the same prominence as where we win. A
  comparison page containing only wins is read as marketing and convinces nobody.

Every arm — including Spate's — builds from a clean clone of this repository
with no credentials: the framework is consumed from [crates.io], pinned in
`Cargo.lock`. [Reproducing this](docs/reproduce.md) says exactly what a
reproduction needs.

If you think an arm is configured badly, that is a bug and we want the pull
request. See [CONTRIBUTING.md](CONTRIBUTING.md).

[spate]: https://github.com/spate-etl/spate
[crates.io]: https://crates.io/crates/spate-core

## How to read a number here

**Every result carries the version of the system that produced it, the exact
image digest, the environment it ran on, and the date.** None of that is optional
and none of it is typed in by hand — a run whose image digest cannot be read is
recorded as failed rather than published.

Three things invalidate comparison outright, and the site refuses to draw records
across them rather than quietly averaging:

| If this differs | Then |
|---|---|
| `harness_version` | The measurement protocol changed. Not comparable. |
| `dataset_version` | The corpus or schema changed. Not comparable. |
| `env_id` | Different hardware. Not comparable. |

Softer differences — a ClickHouse patch release, a compiler version — are
recorded and shown as a footnote rather than treated as disqualifying.

**`bench run` only appends.** There is no code path in it that truncates a results
file. A number later found to be wrong is corrected by editing the archive in a
commit of its own, so the change is in the repository's history. Re-running one
system does not re-run or overwrite any other.

## Current state

Measurements are produced on `c8gd-metal-24xl-ec2-docker`: a fresh EC2
c8gd.metal-24xl per run — bare-metal Linux, 96 homogeneous physical cores, no
SMT, no hypervisor, ClickHouse and the broker each on their own local NVMe
device — launched by an approval-gated pipeline and terminated when the run
ends. Its profile declares `class = "authoritative"`, and that label is
rendered from the environment's declared class rather than hardcoded anywhere
in the site.

## Repository layout

```
methodology/   the fairness contract. Normative, in four parts.
harness/       the driver and the `bench` CLI. Has no dependency on any entrant.
entrants/      one directory per system. Adding a system touches nothing else.
workload/      the one canonical workload: Avro schema, ClickHouse DDL, generator.
environments/  hardware profiles, referenced by id from every record.
results/       append-only JSONL, partitioned by environment and system.
```

The results site is part of the [Spate site](https://spate.kainth.dev/benchmarks/),
which includes this repository as a git submodule and renders `results/`,
`entrants/`, `environments/`, `methodology/` and `docs/` from it. A new
measurement reaches the site when that pin moves.

**[methodology/](methodology/) is normative.** Every implementation here,
including Spate's own, conforms to it. It is the complete specification for an
arm — [the rules](methodology/README.md), [the resource
envelope](methodology/envelope.md), [how you are
measured](methodology/measurement.md), and [what makes two numbers
comparable](methodology/comparability.md). If it is ambiguous, that is a bug in
the document — say so rather than guessing.

## Running it

```sh
bench list                      # systems, variants, and when each was last measured
bench validate                  # what CI checks, runnable locally
bench stale                     # arms whose measurement has fallen behind
bench build '*'                 # build the selected entrants' images
bench prefill                   # populate the topic once per corpus
bench ceiling                   # report the ceilings, and refuse if none is gateable
bench ceiling --measure --write # re-measure them against this corpus and record it
bench run '*' --reps 3          # every arm, interleaved
bench run spate --reps 3        # just one system; nothing else is touched
bench run '*' --dry-run         # print the plan without running it
bench run spate --mode sustained --rate 40000    # latency; has to be asked for
```

`bench run` only ever appends. There is no code path in it that truncates a
results file.

## Licence

Code is [Apache-2.0](LICENSE). Published results in `results/` are CC-BY-4.0 —
use them, cite them, and please link back so a reader can check the provenance.
