# Contributing

The most valuable pull request this repository can receive is one that makes a
competitor faster. This benchmark is run by the author of one of the systems in
it, so "we tuned them until they lost" is the obvious accusation, and the only
answer to it is a public record of outsiders changing our competitors' configs
and our publishing the improved numbers. If you maintain, or merely know, one of
these systems and think its arm is configured badly, that is a bug — open the PR.

## Changing an existing arm

Everything an arm does is in `entrants/<id>/`: the descriptor, the Dockerfile,
the pipeline code, and a README explaining the choices. Change what you need to
and say in the PR what you expect the change to do to the number and why.

Two rules from [methodology/](methodology/) decide most of these:

- **Rule 1** — use the best API the system ships; do not hand-write its
  internals. Configuration tuning is unlimited and expected. Replacing a
  system's own deserializer with one we wrote is not, even when it wins, because
  at that point we are measuring our Java rather than the system's.
- **Rule 2** — optimise hard within rule 1. A slow competitor arm is a bug in
  this benchmark, not a result, and leaving a configuration win on the table is
  the same failure as fabricating a number.

If the system cannot express part of the spec, that is a `[[deviations]]` entry
in the descriptor rather than a quiet departure. The site renders deviations from
the same source the driver reads, so prose cannot drift from behaviour.

## Adding a system

Adding entrant N+1 touches exactly one new directory. There is no central
registry to update, deliberately: the driver and the site both enumerate
`entrants/*/entrant.toml` and derive every filter facet from what they find, so
two concurrent entrant PRs conflict in nothing.

You need `entrants/<id>/entrant.toml`, a `Dockerfile`, a `README.md`, and the
pipeline code. The descriptor is validated by `harness/src/entrant.rs`, and it is
strict on purpose — unknown keys are an error, not something ignored:

- `[entrant]` — `id` must equal the directory name and is the join key used by
  every result record; plus `licence`, `vendor`, `kind`, `language`, `runtime`
  and `status`. `status = "planned"` relaxes everything below, so a roadmap entry
  does not have to invent an envelope in order to be listed.
- `[maintainer]`, `[display]` — `display.hue` is an angle, not a hex colour, and
  must sit at least 20° from every other entrant's. `display.order` must be
  unique.
- `[version]` — how to learn what actually ran. Prefer `strategy = "command"`
  against the built image; a version a human typed in is not provenance.
- `[envelope]` and `[[envelope.container]]` — one or more `data-plane`
  container, and the data-plane containers must sum to the declared
  `[envelope]` totals. A `control-plane` container is allowed and is budgeted on
  top, but it requires a `[[deviations]]` entry with `"envelope"` in `affects`,
  and `bench validate` refuses the descriptor without one — the disclosure is
  enforced, not requested. A JVM container also declares `gc_log`, the
  in-container path its own configuration sends `-Xlog:gc*` to, so the harness
  can `docker cp` the log out. That path is descriptor knowledge, not harness
  knowledge — only the entrant's configuration can state it truthfully, and a
  drift test holds the declaration to those configuration files; the descriptor
  alone does not count as corroboration. A JVM container that declares none
  records no `gc_*` metrics at all — an absence, never a zero — so omitting the
  key silently forfeits the GC row.
- `[[deviations]]` — rule 4, as data. Each entry is a `what` (required and
  non-empty: the fact that differs), a `why` (the part a reader weighs), and an
  `affects` list naming the published quantities the difference touches;
  `"envelope"` in `affects` is the exact value the control-plane requirement
  above keys on. It is a table rather than README prose so the site can render
  it from the same source the driver reads.
- `[clickhouse]` — only for arms whose pipeline puts SQL objects on the shared
  server or changes how their inserts appear in `system.query_log`; most arms
  omit it.
  - `arm_sql` / `arm_teardown_sql` — paths relative to the entrant directory
    (absolute and `..` paths are refused) to SQL applied around each
    repetition: teardown, then the workload target's TRUNCATE, then create at
    repetition start, and teardown again when the repetition ends on every
    path — so an arm's objects exist only inside its own repetitions and are
    never live through another arm's measured window. Write the teardown
    idempotently (`DROP … IF EXISTS`): the first repetition runs it against a
    server holding nothing. Statements are split with the same
    comment-stripping, string-literal-aware splitter as the workload DDL, and a
    failing statement records that arm's repetition as failed rather than
    killing the sweep. The workload target's own DDL is off-limits here — it is
    hashed into `dataset_version`, so arm objects in it would re-key every
    published record.
  - `attribution_tables` — extra table names whose inserts count as the arm's
    server-side work: the landing table of an MV-flatten arm (rule 4's Kafka
    Connect example) goes here, because its parent insert is the row that
    carries the view's cost. Names are unqualified, with no whitespace padding
    — they are qualified verbatim into the query-log attribution.
  - `forwarded_inserts = true` — for an arm whose inserts reach the shared
    server as forwarded rather than initial queries. The attribution predicate
    is inverted (`NOT is_initial_query`), not dropped, so initial and forwarded
    rows can never both be counted; without the flag, a Distributed-forwarding
    arm's strict predicate matches nothing and reads as an arm that never
    inserted.
- `[guarantees]` — at-least-once, matched to a comparable durability interval.
  Turning fault tolerance off to go faster is not permitted.
- `[[variants]]` — one per published arm. Each needs an `approach`
  (`realistic` / `tuned` / `stripped`) and `reports.wire_format`, because the
  wire formats are not the same server-side work. The formats the rig can prove
  ceilings for have canonical, exact spellings — `native`, `rowbinary`,
  `rowbinary_nt`, `json_each_row`, `arrow_stream` — and a near miss such as
  `"JSONEachRow"` is refused at validation with the canonical suggestion, so
  two arms on the same format cannot fail to group over a spelling. A format
  outside that set is allowed — rule 5 still applies — but it publishes with
  its headroom unproven, and flagged as such. Exactly one variant is `default`
  and it must be `realistic`.

Run `bench validate` before opening the PR. It is what CI runs, it reports every
problem rather than the first, and it also checks that every environment profile
and every committed result still parses.

## `methodology/` is normative

It is the complete specification for an arm, including ours. If something in it
is ambiguous, that is a bug in the document and we want the issue — an arm that
guesses and quietly deviates is worse than no arm at all, because it produces a
number we would then publish. Changes to the rules are their own PR, argued on
their own, and never bundled with an implementation that depends on them.

## Results

`results/` is append-only and is not something a pull request edits. Nothing in
`bench run` can truncate a results file; the capability does not exist, so
retention is not a matter of anyone remembering. A number later found to be wrong
is corrected by the maintainer in a commit of its own, so what changed and why is
in the repository's history rather than in a marker every reader has to step over.
Retiring an environment is the same discipline: its records and its profile are
removed in a commit of its own, and the repository's history is the archive.

Measurements are produced on the authoritative environment, because a number is only
comparable to the others if it came off the same hardware under the same
protocol. So a PR changes configuration, code and documentation; the
re-measurement happens after it merges, and the resulting records arrive as a
separate, validated pull request. If your change ought to move a number, say
what you expect and it will be checked against what comes back — including when
it does not.

When your merged change can move a published number, the pipeline proposes the
re-run automatically and a maintainer approves it — approval is also what issues
the pipeline's AWS credentials, so you cannot run our cloud environment
yourself, and nothing spends money without a human clicking. The launcher and
collector run from a private operations repo (the benchmark shares an AWS
account, so its account shape is not published), but what the box executes and
every published number remain fully reproducible from this repo — see
[reproducing this](docs/reproduce.md).
