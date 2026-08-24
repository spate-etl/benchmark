# What makes two numbers comparable

Part of [the fairness contract](README.md). How the corpus is generated, what
invalidates a comparison outright, and the separation between tuning and
measurement.

## Deterministic data generation

The producer is shared, so every system receives byte-identical input. The
generator is a pure function of `batch_id`, which is what lets the driver compute
the expected checksum in closed form and prove that two systems performed the same
arithmetic rather than merely moving the same row count.

For `batch_id` in `0..N`, with `EVENTS_PER_BATCH = 100`:

```
sensor       = "sensor-{batch_id % 1024}"
region       = null if batch_id % 10 == 0 else "region-{batch_id % 7}"
batch_ts_ms  = BASE_TS_MS + batch_id
send_ts_us   = intended schedule time (NOT the actual send time)

for seq in 0..100:
    name    = "metric_{(batch_id * 31 + seq) % 32}"
    unit    = UNITS[(batch_id * 7 + seq) % 8]     // UNITS[3] == "drop"
    value   = (batch_id * 1_000_003 + seq * 97) % 2_147_483_647
    quality = null if (batch_id + seq) % 5 == 0
              else ((batch_id * 13 + seq * 7) % 100) / 100.0
    tags    = ["tag-{(batch_id + seq + j) % 16}" for j in 0..((batch_id + seq) % 4)]
```

`(batch_id, seq)` is the row identity. `uniqExact((batch_id, event_seq))` is
therefore the loss gate and `count() - uniqExact(...)` the duplicate count — both
exact, and both taken over a **bounded window** rather than the whole corpus. The
window is the top 100,000 batches of the landed range, which against the published
1,500,000-batch corpus is 6.7% of it, about ten million rows before the
workload's filters.

The bound is not a convenience. Exact-distinct needs a hash set proportional to
cardinality, and running it across the full 150M-row corpus asked ClickHouse for
10.45 GiB against a 10.8 GiB limit and was killed — taking a completed, valid
measurement down with it, which is the worst possible way to lose an hour. The
slice is taken from the *top* of the range because that is the part produced during
and after the measurement window, and a system that drops, duplicates or
mis-transforms rows does so systematically rather than once. The window size is
written into every record's note, so the gate is visibly a sample rather than
silently one.

The consequence a reader has to know is that the published `duplicate_rows` is the
duplicate count **within that window**, not over the corpus. It is a rate
indicator, not a total.

Within that window the gate compares closed-form expectations for `sensor`,
`region`, `name_upper`, `unit`, `tags` (count and content), `batch_ts` and the
null-`quality` count, as well as the row identity set and the two value sums. So
"the same arithmetic" is now close to literal: an arm that emits empty `tags`,
skips the null-`region` coalesce, uppercases with a locale-aware routine or loses
the `DateTime64` scaling fails a named expectation rather than passing quietly on
a matching row count.

Two columns are deliberately not compared exactly, and the reason is the same in
both cases — an exact comparison would fail honest arms. `quality`'s values are
`f64`, and a sum of floats depends on the order the server added them in, so only
its null pattern is pinned. `send_ts` in sustained mode is the producer's
intended schedule time rather than a function of `batch_id`, so it has no closed
form in the mode that matters; it is bounded instead, which is enough to catch a
timestamp regression landing every row in 1970.

The generator's tunables live in [`workload/workload.toml`](workload/workload.toml)
so that `dataset_version` can be derived from their content rather than
hand-maintained.

## What invalidates a comparison

Three properties are **hard**: records that differ in any of them are never drawn
on the same axis, and the site renders an explicit "not comparable" note instead of
quietly averaging them.

| Field | Bumped when | Effect |
|---|---|---|
| `harness_version` | The measurement protocol changes in a way that moves numbers — the definition of the measurement window, the drain protocol, sampler semantics, the gate set, envelope enforcement. Not when a log message changes. | Whole result set split |
| `dataset_version` | The corpus changes. Derived — from the parsed values of `workload.toml`, the bytes of the Avro schema, the normalised DDL, and the marked generator region of `harness/src/corpus.rs` — so it moves when the generator's *arithmetic* changes and not when a comment does. It cannot be forgotten. | Whole result set split |
| `env_id` | Different hardware or a different infrastructure envelope. | Comparable only within an environment |

One further axis is never drawn on one scale, and it is a property of the
experiment rather than of the protocol, so it does not version the archive — the
site simply refuses to combine it.

**Mode.** `rows_per_s` means "how fast can this go" in drain and "the rate we
asked for" in sustained, so two arms of entirely different capacity report the
same number; and a sustained arm's efficiency figures were taken with the broker
serving writes and reads at once and a generator competing for cores, which is
the whole argument drain exists for. Latency is single-mode by construction.

Softer provenance — a ClickHouse patch release, a compiler version, a broker
version — is recorded on every record and rendered as a footnote. Refusing to
compare across a ClickHouse patch would make the suite unusable; refusing across a
protocol change is the entire point.

### Harness versions

`harness_version` is hand-maintained rather than derived, deliberately: "did this
change move numbers?" is a judgement, and a content hash would answer yes to every
typo fix and shatter every comparability group. This table is the record, and CI
asserts it stays in step with the constant in `harness/src/report.rs`.

| Version | Date | Change |
|---|---|---|
| 1 | 2026-07-29 | Initial protocol. One measurement window — throughput, mean cores and CPU-per-row divide by the sampler's own window. Headroom gated against both ceilings, with a ceiling measured at the wrong message size or under a different infrastructure envelope refused rather than extrapolated. Gate set covers row count and every derived column. Sustained mode and latency. Peak memory is what an arm held at one instant across its containers. Server-side cost, GC pauses and JVM heap measured. |

What each change was and why is in the commit that made it; this table exists to
say which records may be drawn on one axis.

## Results are never overwritten

`bench run` appends. There is no code path in it that truncates a results file, and
that is enforced by the absence of the capability rather than by discipline.

### Tuning is not measurement, and cannot become it

Rule 1 makes configuration tuning unlimited and expected, so a search over an
arm's knobs is a normal part of preparing it. Those runs are real measurements on
real hardware, and none of them is a result: the danger is obvious and is the one
thing that would discredit the exercise — run until the number is liked, then
record it.

So the two activities are separated by machinery rather than by intention. A
tuning run carries `trigger: tuning`, is written to `tuning/` rather than
`results/`, and **`bench validate` refuses any record under `results/` whose
trigger bars publication** — so committing one fails rather than publishes.
`--knob`, which lets a run use values its descriptor does not declare, cannot be
used without such a trigger; the flag that makes a record misdescribe its own
variant is unreachable without the marking that keeps it out of the archive.

The configuration a search settles on is declared in the entrant's descriptor,
where the driver reads it, and is then measured again cleanly as a published run
— never lifted from the search that found it.

A run later found to be wrong is corrected by editing the archive in a commit of
its own, so what changed and why is in the repository's history rather than in a
marker every reader has to step over. Retiring an environment is the same
discipline: its records and its profile are removed in a commit of its own, and
the repository's history is the archive.

Re-running one system does not touch any other system's results. Records are
partitioned by `results/<env_id>/<entrant>/<YYYY-MM>.jsonl`, so a partial re-run
produces a diff confined to one file.

## Host caveat

Published measurements run on `c8gd-metal-24xl-ec2-docker`: a fresh EC2
c8gd.metal-24xl per run — bare-metal Linux, 96 homogeneous physical cores with
no SMT and no hypervisor. Its environment profile declares
`class = "authoritative"`, and the exact machine, storage layout and launch
pipeline are committed in this repository, so the environment is reproducible
with an AWS account rather than with access to anyone's hardware.

The site shows run-to-run spread on every chart and carries full environment
provenance in every record. Environments are never drawn on one axis: results
from any other hardware live under their own environment id, in their own
comparability group, with that environment's declared class rendered beside
them.
