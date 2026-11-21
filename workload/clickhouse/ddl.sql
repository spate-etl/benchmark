-- Shared ClickHouse target schema for the cross-framework comparison.
--
-- Every framework writes THIS table, with THIS column order. The order is the
-- wire contract for both RowBinary and Native, so a reordering here is a
-- breaking change to every implementation in entrants/.
--
-- The driver TRUNCATEs between arms rather than recreating, so all arms share one
-- table definition and therefore one set of server-side costs. Nothing about the
-- target may differ per framework — if it ever has to, that arm is not
-- comparable and must not be published.

-- ---------------------------------------------------------------------------
-- The workload: decode, flatten, filter, derive, insert.
-- ---------------------------------------------------------------------------
-- Specified work, in this order:
--   1. drop rows where unit = 'drop'                       (~12.5% of rows)
--   2. drop rows where quality IS NOT NULL AND quality < 0.2
--   3. coalesce a null region to ''
--   4. value_scaled = value * 1000 / (event_seq + 1), integer division,
--      truncating toward zero
--   5. name_upper  = ASCII-only uppercase of name
--
-- Step 5 is ASCII-only ON PURPOSE. Java's String.toUpperCase() is
-- locale-dependent, so an unqualified "uppercase" would not be the same
-- operation in every implementation. Metric names are drawn from a fixed set of
-- lowercase ASCII identifiers, which makes ASCII uppercase unambiguous.
CREATE TABLE IF NOT EXISTS sensor_events
(
    batch_id     UInt64,
    event_seq    UInt16,
    sensor       LowCardinality(String),
    -- NOT LowCardinality(Nullable(String)): the Native encoder rejects a
    -- non-String inner. The Avro null is coalesced to '' by the pipeline —
    -- step 3 above — which is why the coalesce is specified work rather than
    -- an incidental detail.
    region       LowCardinality(String),
    name_upper   LowCardinality(String),
    unit         LowCardinality(String),
    value        Int64,
    value_scaled Int64,
    quality      Nullable(Float64),
    tags         Array(LowCardinality(String)),
    batch_ts     DateTime64(3),
    -- Producer's INTENDED send time. The Native leaf writer does no DateTime64
    -- rescaling, so the Spate side must use the DateTime64Micros wrapper
    -- newtype here or every value silently lands in 1970.
    send_ts      DateTime64(6),
    -- Computed server-side at insert, identically for every framework and every
    -- wire format. This column is the entire latency measurement: no framework
    -- reports its own latency, and there is only one definition of "arrived".
    ingest_ts    DateTime64(6) MATERIALIZED now64(6)
)
ENGINE = MergeTree
ORDER BY (sensor, batch_ts, batch_id, event_seq)
-- On plain MergeTree this window defaults to 0, which makes
-- `insert_deduplication_token` silently do nothing. It is set here so the
-- setting is identical for every arm, so the choice shows up as a difference
-- between arms rather than as a difference between targets.
--
-- Today exactly one arm takes it up. `etl-clickhouse`'s writer sets
-- `insert_deduplication_token` on every sealed batch, so the Spate arm hands
-- ClickHouse a cheap token and the server skips hashing the block; every other
-- arm's inserts are deduplicated by block content hash, which is real per-insert
-- server-side work across a hundred and fifty million rows. Any framework could
-- send them and none of the others does, which makes this a difference in what
-- the arms ask of the target rather than a difference in the target — but it is
-- a real asymmetry, it favours the arm run by the vendor, and it is declared as
-- a deviation on that entrant rather than left in a SQL comment.
--
-- It is not visible in `entrants/spate/`: the setting is applied by the
-- framework under test, not by the arm's own source. A review that grepped the
-- entrant directory concluded no arm sent tokens and was wrong.
--
-- The three cleanup settings make a merged-away part leave the disk within
-- seconds rather than after `old_parts_lifetime`'s shipped 480 s, with the
-- cleanup thread polling every second (shipped: 30 s, backing off to 300 s).
-- An arm's own merged-away parts are then deleted inside its own repetition,
-- and the harness's settle wait covers them, so no arm's window runs while a
-- predecessor's parts are being deleted. An arm writing small inserts leaves
-- hundreds of thousands of them, and their deletion otherwise overlaps the next
-- arm's window, where the server held that arm's inserts back.
SETTINGS non_replicated_deduplication_window = 1000,
         old_parts_lifetime = 10,
         cleanup_delay_period = 1,
         max_cleanup_delay_period = 5;

-- ---------------------------------------------------------------------------
-- The gates the driver runs after every arm, before it emits anything.
-- ---------------------------------------------------------------------------
-- Loss (must equal the expected row count exactly — a framework that drops
-- rows is faster for the wrong reason, and its arm is void):
--
--   SELECT uniqExact((batch_id, event_seq)) FROM sensor_events;
--
-- Duplicates (reported as a metric, never suppressed: these are all
-- at-least-once systems and some duplication is legitimate):
--
--   SELECT count() - uniqExact((batch_id, event_seq)) FROM sensor_events;
--
-- Same-work checksum (proves two frameworks did the same arithmetic, not just
-- that they moved the same number of rows; compared against the driver's
-- closed-form expectation).
--
-- Taken over DEDUPLICATED rows, which matters: every arm here is at-least-once,
-- so a legitimate duplicate would otherwise inflate the sum and fail a correct
-- arm. `toInt128` because `sum` over `Int64` returns `Int64` and a large corpus
-- would overflow it silently.
--
--   SELECT sum(toInt128(value)) FROM
--     (SELECT DISTINCT batch_id, event_seq, value FROM sensor_events);
--
-- One sum per column, not one for `value`. Two integer sums proved only that the
-- integer arithmetic matched: an arm emitting `tags = []` skipped the
-- Array(LowCardinality(String)) encode on every row and passed, and so did one
-- that dropped the null-region coalesce, and so did one that lost the DateTime64
-- scaling below. The string columns are reduced to an integer BEFORE they are
-- aggregated, never collected — an exact-distinct over string values is the
-- shape of query that ran the server out of memory:
--
--   SELECT sum(toInt128(reinterpretAsUInt64(CAST(name_upper AS String)))
--            + toInt128(reinterpretAsUInt64(reverse(CAST(name_upper AS String)))))
--     FROM (SELECT DISTINCT batch_id, event_seq, name_upper FROM sensor_events);
--
-- Two columns cannot be checksummed and are checked as far as they can be.
-- `quality` is Float64, and a sum of floats depends on the order the server added
-- them in, so only its null pattern is pinned. `send_ts` in sustained mode is the
-- producer's intended schedule time — a clock reading, not a function of
-- batch_id — so it is bounded rather than summed: min(send_ts) must not predate
-- the corpus base timestamp, which is what the 1970 regression violates.
--
-- The driver additionally excludes the lowest and highest batch_id: a sealed sink
-- chunk can split one message's rows across two batches, so the boundary batches
-- may be only partially landed at the instant of the snapshot. Every exact test
-- above runs over a BOUNDED window taken from the top of the landed range, for
-- the same memory reason.
--
-- End-to-end latency (one definition, server-side, no framework cooperation).
--
-- SUSTAINED MODE ONLY. In drain the topic is prefilled, so `send_ts` is a
-- prefill timestamp and this difference measures how old the backlog was, not
-- what the pipeline cost. A drain record therefore carries no latency metric at
-- all, and the driver makes that structural rather than conventional.
--
-- The window predicate is not optional: a sustained run measures over the
-- sampler's own interval, and rows that landed outside it belong to the warm-up
-- or to the backlog drain afterwards.
--
--   SELECT quantile(0.5)(lat), quantile(0.99)(lat), quantile(0.999)(lat),
--          max(lat), count()
--     FROM (SELECT toUnixTimestamp64Micro(ingest_ts)
--                - toUnixTimestamp64Micro(send_ts) AS lat
--             FROM sensor_events
--            WHERE toUnixTimestamp64Milli(ingest_ts) >= <window start>
--              AND toUnixTimestamp64Milli(ingest_ts) <  <window end>);
--
-- Microseconds, and approximate quantiles with an EXACT max. `quantileExact`
-- sorts the column, which is the same memory shape as the `uniqExact` that
-- already killed a gate once. The max is exact because a single multi-second
-- stall is invisible at p999 over ninety million rows and is the thing a reader
-- most wants to know about.
--
-- `send_ts` is the message's INTENDED schedule time, never the moment it was
-- actually sent. That makes this figure coordinated-omission-corrected: it
-- charges the pipeline for time a message spent waiting because the producer
-- itself fell behind, instead of restarting the clock when the producer finally
-- managed to send. A benchmark that stamps at actual send time reports its most
-- flattering latency exactly when the system is failing.
