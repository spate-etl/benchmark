# How you are measured

Part of [the fairness contract](README.md). Why an arm must not instrument
itself, what the instrument can resolve, and which mode measures what.

**Do not add metrics, timers, or counters for the benchmark's benefit.** Nothing a
system reports about itself is used for any published number. If your
implementation exposes metrics that it would expose in production, leave them;
they are useful for debugging. They will not be read as results.

Everything published comes from outside the system under test:

| Quantity | Source |
|---|---|
| Throughput | `SELECT count()` against ClickHouse, polled by the driver, over the sampler's own window |
| CPU | cgroup v2 `cpu.stat` (`usage_usec`, `user_usec`, `system_usec`), sampled at 10 Hz by a sidecar container |
| Memory | cgroup v2 peak **anonymous** memory, plus `memory.peak` and (for JVM arms) configured vs used heap |
| Latency | `ingest_ts - send_ts` computed in ClickHouse, where `ingest_ts` is a `MATERIALIZED now64(6)` column. Sustained mode only |
| Server-side cost | ClickHouse's own `ProfileEvents` CPU-per-row, via `system.query_log` |
| GC (JVM arms) | `-Xlog:gc*`, read with `docker cp` from the path the container's descriptor declares (`gc_log`) |

The published lead figure, `rows_per_s_per_core`, is the ratio of the first two
rows — a `SELECT count()` taken outside the system over cgroup counters read by
a sidecar — so no single source can move it on its own.

Every row above is collected. Three caveats travel with them rather than with
the table. **Latency exists only in sustained mode** — in drain the topic is
prefilled, so `send_ts` is a prefill timestamp and the difference measures how
old the backlog was; a drain record carries no latency metric at all, and the
harness makes that structural rather than conventional. **Server-side cost
excludes background merges**, which live in `system.part_log` and are
arm-dependent: 25,000-row batches make far more parts than 262,144-row ones, and
that cost lands nowhere in this figure. The exclusion is from that figure only.
Merges run on the same cores and the same disk as the measurement, so they reach
throughput while being charged to nobody. Two things bound them. Every
repetition waits for the target to report no active parts and no running merges
before its window opens, so a repetition pays for its own merges rather than for
its predecessor's; and `ch_rows_merged`, `ch_merge_duration_us` and
`ch_settle_us` are on every record, so the merge work a window ran against is
visible rather than inferred. **A GC number exists only for JVM arms**
— and only for a container whose descriptor declares `gc_log`; a JVM container
that declares none records no GC figures, an absence, not a zero. The absence is
never a zero in the other direction either: a Rust arm has no collector, so a
chart drawing a missing pause total as a zero-length bar would be asserting a
measurement nobody made.

One property of the latency figure is worth stating outright, because it is the
usual reason a latency number is worthless. `send_ts` is the message's *intended
schedule time*, never the moment it was actually sent, so the figure is
coordinated-omission-corrected: it charges the pipeline for time a message spent
waiting because the producer itself fell behind, instead of restarting the clock
when the producer finally managed to send. A suite that stamps at actual send
time reports its most flattering latency exactly when the system is failing.

The broker's message count is read once, before the sweep, and is what defines
how many rows a complete drain must produce; the drain ends when ClickHouse's
count reaches it. That is a **completeness** check — it is what makes "the arm
stopped early" impossible to mistake for "the arm was fast" — and it is not a
second, independent estimate of throughput. An earlier version of this table
described it as a cross-check, which overstated it: there is one throughput
measurement here, and it is the row count over the window.

The sampler is a sidecar container that mounts the cgroup filesystem with
`--cgroupns=host`, so that it sees the VM's cgroup tree rather than its own
namespaced view of it. The mount is **read-write**, which is more privilege than a
thing that only reads counters ought to ask for, and it is deliberate: resetting
`memory.peak` is a write, and without that reset at the start of the window the
memory figure would be a lifetime peak covering whatever the container touched
before the measurement began rather than a peak over the measurement itself. It
deliberately does not `docker exec` into the arm, because an arm's image may have
no shell — Spate's is distroless — and the same measurement must work for every arm
regardless of base image.

## What the instrument can resolve

The drain's window comes from the sampler's own timestamps, and the sampler runs
at 10 Hz. So **a throughput difference smaller than one sampler tick is not a
difference this instrument can see**.

How large that is depends on the arm, not on the rig. A drain's window is the
corpus divided by throughput, so it shrinks as arms get faster and has no lower
bound of its own — a faster arm is measured more coarsely, and the error is
systematic rather than random, because a fixed row count divided by a smaller
window reads high. Two things bound it. The corpus is sized so that the fastest
arm's window clears a declared floor of **120 seconds**, at which one tick is
0.08%; and every record carries `window_resolution`, the tick as a fraction of
the window it was actually read over, so a reading taken at 0.08% is
distinguishable from one taken at 7%. A window below the floor carries
`short_window` beside `reused_infra` and `cpu_cap_throttled`.

The per-core figure divides CPU by rows rather than by time, so the window
cancels and the tick does not quantise it. It is not independent of the sampler:
both ends of the CPU delta are sampler readings while a drain's row count is the
whole corpus, so a window clipped at either end understates the CPU and the
reading comes out flatteringly low. Raising the rate bounds that clipping; it
does not remove it, and the residual is why the floor exists as well.

## What the rig does when nothing changes

Every sweep measures its own first arm a second time, under a second label, in
the same interleave as any other pair. The two halves differ by everything two
arms differ by — container recreation, the truncate, the settle, position in the
rotation — and by nothing else, so the difference between them is the spread this
rig produces when the system under test does not change. Repetitions of one arm
cannot answer that: they never cross the path along which two arms differ.

The control is the sweep's first arm rather than a named one. Naming an entrant
would make one system the rig's reference, which is not something a benchmark run
by one of its entrants should hand itself.

The pair's difference is published as a `verdict` record for the sweep, joined to
its measurements by `invocation_id`. It is not a row in the comparison and the
control is not an entrant: the number exists to be differenced, not to be ranked.
The environment profile declares the spread above which a sweep's differences are
not to be believed; until it declares one, the figure is recorded as an
observation and nothing calls it acceptable. **An A/A control that reports a
difference is a bug in the harness or the environment rather than a finding.**

It is a separate limit from run-to-run spread, and the two are often confused.
Spread says how much a repeated measurement wanders; quantisation says how finely
any single one can be read. Both must clear before a difference means anything.

The practical consequence for a reader: **two arms within a few percent of each
other are not ranked by these numbers**, whatever order the page happens to draw
them in. Every published figure carries the spread across its repetitions for
exactly this reason.

## Which mode measures what, and why

**Throughput and efficiency come from DRAIN mode. Latency comes from SUSTAINED
mode.** That split is forced by host arithmetic, and it was measured rather than
assumed.

An arm's envelope, plus the broker's, plus ClickHouse's, plus a load generator
wide enough to offer millions of rows/s, plus the driver, **can exceed the cores
a host has**. The consequence is not subtle: under sustained load,
widening the Spate arm's egress concurrency changed its throughput not at all,
which reads exactly like "egress concurrency does not matter" — while the same
widening, measured in drain mode with the generator's CPU outside the window,
moved it substantially. The sustained result was host contention, not a property
of the system, and a suite that had only ever run in sustained mode would have
published that contention as a finding about Spate.

That observation comes from protocol-design work on the harness this one was
extracted from, and it is **not reproducible from this repository**: no committed
descriptor varies egress concurrency, every published Spate record fixes
`inflight = 4`, and no sweep exists that would produce the pair of numbers. It is
recorded here as the reason for a decision, not as a result, and it is deliberately
quoted without figures rather than with figures a reader cannot check.

**Produce first, then consume.** Drain mode populates the topic completely before
any arm starts, and runs no producer during the measurement. Two things follow, and
both matter: the broker does only its *read* path rather than serving writes and
reads at once, and the generator's CPU is entirely outside the window. We are not
benchmarking the broker, so it must never be doing concurrent work it would not be
doing in the measurement we claim to be making. `drain` is therefore the default
mode; `sustained` has to be asked for.

Two disclosures follow from prefilling. The corpus is largely served from the
broker's page cache, which is favourable — equally, for every arm — and is stated
rather than hidden. And the broker's *fetch* path is still inside the measurement,
because the pipeline under test is Kafka → ClickHouse; the requirement is not that
the broker is absent but that it is not the **bottleneck**, which is what the
ceiling pass exists to prove.

Latency is only meaningful in **sustained** mode: in drain mode `send_ts` is a
prefill timestamp, so the difference measures backlog age rather than pipeline
latency. Drain mode therefore reports throughput only.

A sustained arm that cannot keep up with the offered rate is recorded as
`SATURATED` with a `kept_up_share` metric. Such a point is a genuine ceiling
measurement, but its latency figures describe backlog age and must not be read as
latency at the offered rate.
