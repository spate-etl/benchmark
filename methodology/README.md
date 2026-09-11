# The fairness contract

**This file is normative.** Every entrant here — including Spate's own — conforms
to it. If you are implementing an arm, this is your complete specification; read
it before writing code, and if it is ambiguous, treat that as a bug in this file
and say so rather than guessing. An arm that quietly deviates is worse than no arm
at all, because it produces a number we would then publish.

The contract is in four parts:

| | |
|---|---|
| **This file** | the goal, the pipeline, the delivery guarantee, and the seven rules. |
| [The resource envelope](envelope.md) | what each system is given, and the headroom rule. |
| [How you are measured](measurement.md) | why an arm must not instrument itself, and what each mode measures. |
| [What makes two numbers comparable](comparability.md) | the corpus, what invalidates a comparison, and why tuning is not measurement. |

## The one-sentence goal

Find out, honestly, how much throughput each system delivers per unit of CPU and
memory on the same pipeline, the same bytes, and the same hardware — and publish
it whether or not Spate wins.

**The figure that answers it is `rows_per_s_per_core`**, and that is the column
the results table leads with. `cpu_us_per_row` is the same measurement inverted
rather than a second one: the sampler's window cancels out of
`rows_per_s / cores_used`, leaving rows per CPU-second, so the two are exact
reciprocals and the site shows one by default and the other on request.

Raw throughput is published beside that figure rather than in front of it,
because the two degrade differently. `rows_per_s` is the corpus over the
sampler's window, so it absorbs every source of wall-clock contention the shared
infrastructure produces — broker scheduling, ingest queueing, background merges.
An arm that no longer saturates its own envelope is one whose throughput
increasingly describes the rig rather than the system. The per-core figure is
work charged to the arm's own cgroup per row it produced, and is largely
indifferent to how long the wall clock took to deliver that work.

We expect to lose some arms. The ClickHouse Kafka engine does its decode and
transform inside the database's own C++, with no framework between consumer and
insert. Vector is Rust with the same no-GC story. Those results get published
with equal prominence, because a comparison page containing only wins is read
as marketing and convinces nobody.

## The pipeline

Consume one topic of Confluent-framed Avro `SensorBatch` messages from the
broker the environment declares, decode them, flatten each message's `events`
array into one row per event, and insert those rows into ClickHouse.

- Schema: [`workload/schema/sensor_batch.avsc`](workload/schema/sensor_batch.avsc)
  — read this file, do not re-declare the schema inline. The `events` array
  carries **100** events per message; the array is unbounded in the schema, so
  this is a generator constant and not a schema change. It was raised from 20 to
  buy consume-path headroom.
- Target: [`workload/clickhouse/ddl.sql`](workload/clickhouse/ddl.sql) —
  the `sensor_events` table. Column order is the wire contract.
- Wire format: Confluent framing (`0x00` + big-endian u32 schema id + datum),
  subject `comparison-sensor-batches-value` — the topic-name-strategy name for
  the topic — against a live Schema Registry. The framing belongs to the
  environment rather than to the pipeline: a broker that ships no registry
  carries the bare datum instead. That is the same decode over the same bytes,
  and the arm declares it under rule 4.
- Source: the broker is whichever one the environment profile declares. It is
  not a free choice per arm, and two broker families are never compared —
  see [what makes two numbers comparable](comparability.md).

**The transform**, applied to every decoded row, in this order — drop rows where
`unit = 'drop'` (the sentinel); drop rows where `quality` is non-null and
`< 0.2`; coalesce null `region` to `''` (forced by the target column type);
compute `value_scaled = value * 1000 / (event_seq + 1)` as integer division
truncating toward zero; compute `name_upper` as the **ASCII-only** uppercase of
`name`. ASCII-only is specified because Java's `String.toUpperCase()` is
locale-dependent, so "uppercase" alone would not be the same operation in every
language. About 73.5% of decoded rows survive the two filters and land in
`sensor_events`.

## Delivery semantics: at-least-once, matched

Every arm runs **at-least-once**, and every arm's durability mechanism is
configured to a comparable interval (Spate commits offsets every 5s; Flink
checkpoints every 5s in `AT_LEAST_ONCE` mode; `clickhouse-kafka-connect` runs
`exactlyOnce=false`).

Turning a system's fault tolerance off to make it faster is not permitted, even
though it would flatter the numbers of whichever arm we did it to. We are
comparing guarantee-for-guarantee. If a system can only offer exactly-once, we run
it that way and label it, rather than pretending the guarantee is free.

Each entrant declares its semantics in `[guarantees]` in its descriptor, and the
site renders them beside the numbers.

## Rules

1. **Use the best API the system ships — do not hand-write its internals.** This
   is the rule that decides what is being compared. Decoding, encoding and
   transport must go through the system's own public, documented APIs, choosing
   the fastest one it offers. Configuration tuning is unlimited and expected
   (`pipeline.object-reuse`, buffer timeouts, memory sizing, parallelism), because
   configuration is not code we wrote.

   The reason is symmetry. On the Spate side we take the framework's shipped
   Avro deserializer exactly as any user would, and the same standard applies to
   every arm. If we hand-optimised a competitor's decoder we would no longer be
   measuring that system, we would be measuring our own Java or Go — and the
   mirror-image accusation, that we tuned a competitor until it lost, would be
   just as fair.

   Pipeline *logic* is different and is yours to write well: the flatten, the
   filters and the derived columns are user code in every system, and every
   arm writes them.

   An optimisation the system itself provides is exactly what we are here to
   measure. Where a system evaluates the specified transform more cheaply than
   a row at a time — ClickHouse applying a function over a `LowCardinality`
   dictionary rather than per row — that is the engine doing its job, and an
   arm should let it.

2. **Optimise hard within rule 1.** Tune as an expert who wants to win would:
   correct parallelism, correct memory sizing, no needless copying, no debug
   logging on the hot path. A slow competitor arm is a bug in our benchmark, not a
   result. Deliberately leaving a *configuration* win on the table is the same
   failure as fabricating a number.

   Where a system's shipped default is measurably suboptimal, that is a publishable
   finding about the system — but it must be **stated as avoidable**, with the cost
   quantified if a secondary arm can do so. Publishing a number that a competitor's
   expert could beat, without saying so, is the failure mode that destroys a
   comparison page's credibility.

3. **Publish every system's best, and say what it cost to get there.** Use what
   a competent user would actually deploy — no pre-computing work outside the
   measured window, no dropping durability — and where harder tuning goes
   faster, publish that too, beside it and labelled.

   Each variant declares `approach`, and the site shows both publishable classes
   by default:

   | `approach` | Meaning |
   |---|---|
   | `realistic` | Rules 1 and 3 satisfied: what a competent user would deploy. Headline-eligible. |
   | `tuned` | Rule-1 compliant, but a configuration a typical user would not deploy — an experimental runtime, a setting found by a search rather than by a manual. Headline-eligible, and labelled everywhere it appears. |
   | `stripped` | Uses code the project does not ship, or drops a guarantee. Shown and filterable, never ranked; exists to quantify a specific effect. |

   `tuned` was once barred from the headline. That was the wrong instinct for a
   benchmark run by one of its entrants. Rule 2 already obliges us to tune every
   competitor as hard as an expert who wanted it to win, and hiding the result
   of having done so — in a view nobody switches on — made that obligation
   invisible exactly where a sceptical reader looks. A page that shows a
   competitor at its measured best, and still reports our own number beside it,
   is a stronger claim than one that shows competitors only at their defaults;
   it is also the one a maintainer of that system can check under rule 7.

   Two things hold this honest, and both are load-bearing:

   - **Every arm keeps a `realistic` row.** Exactly one variant is `default` and
     it must be `realistic`, so no arm can present a tuned best and quietly drop
     the figure a user would actually get. "How does this behave out of the box"
     and "how fast can this be made to go" are different questions, and the page
     has to answer both or it answers neither.
   - **`tuned` is a disclosure, not a demotion.** It reaches the reader as a
     label on the row, not as a filter they must find. An arm that carries it is
     saying "this number is real and this configuration is not our
     recommendation", which is a claim about deployment rather than about
     measurement.

   `stripped` stays out of the ranking, and the valve is not decorative: a
   hand-written replacement for a decoder a system ships is code *we* wrote, so
   rule 1 bars it from the ranking even when it makes that system look better —
   including when the system is ours.

4. **Record every deviation.** If the system cannot express part of the spec, put
   it in `[[deviations]]` in the descriptor — machine-readable, so the site renders
   it from the same source the driver reads, and prose cannot drift from behaviour.
   Kafka Connect, for instance, has no fan-out operator, so its arm must land the
   nested array and flatten with a ClickHouse materialized view — a legitimate
   real-world pattern and an interesting result, but it moves CPU to the server and
   must be disclosed, not smoothed over.

5. **Report the insert format.** Native, RowBinary, JSONEachRow and a Go SQL driver
   are not the same amount of server-side work. Every arm's format appears in the
   results table, from `reports.wire_format` in its descriptor.

6. **Answer Gregg's question.** For your final configuration, write one sentence
   saying why throughput was X and not 2X — what the binding constraint was, and
   the evidence. This goes in the published results table. A table where every row
   explains its own bottleneck is what makes the page hard to attack.

7. **Expect your config to be reviewed upstream.** We intend to send competitor
   configurations to the relevant maintainers and ask whether we handicapped them,
   then link the answers. `[maintainer].reviewed_upstream` records whether that has
   happened, and the site shows it. Write configs you would be comfortable
   defending.
