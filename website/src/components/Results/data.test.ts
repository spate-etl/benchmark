// What this file is for.
//
// `armLabel` decides the text under a system band, and it is the one label on
// the results table the descriptor does not state outright: the site derives it
// by taking the system's own name off the front of the variant label. Get that
// wrong and an arm renders under a name no descriptor contains, which a reader
// has no way to check against `entrants/`.
//
// The stripping matches on `entrant.name` and `display.short`, so a descriptor
// can move this label without touching the site. That also means a change to
// either field is a change to what the page says, which is what the first test
// pins for every arm the archive publishes.
//
// Run with `npm test`.

import assert from 'node:assert/strict';
import {test} from 'node:test';

// The `.ts` extension is required by `node --test`, which resolves this path
// itself rather than through the bundler.
import {armLabel, type Entrant, type Variant} from './data.ts';

/** An entrant carrying only the two fields the stripping matches against. */
const system = (name: string, short?: string): Entrant =>
  ({entrant: {name}, display: {short}}) as Entrant;

const arm = (id: string, label?: string): Variant => ({id, label});

test('every published arm renders without repeating its system name', () => {
  // The real descriptor strings, entrant by entrant. A change to a `name`, a
  // `display.short` or a variant `label` lands here first.
  assert.equal(armLabel(system('Spate', 'Spate'), arm('native', 'Spate · Native'), 'native'), 'Native');
  assert.equal(
    armLabel(system('Spate', 'Spate'), arm('rowbinary', 'Spate · RowBinary'), 'rowbinary'),
    'RowBinary',
  );

  assert.equal(
    armLabel(system('Vector', 'Vector'), arm('arrow', 'Vector · ArrowStream'), 'arrow'),
    'ArrowStream',
  );
  assert.equal(
    armLabel(system('Vector', 'Vector'), arm('json-each-row', 'Vector · JSONEachRow'), 'json-each-row'),
    'JSONEachRow',
  );

  // Flink's label carries the version, which `displayLabel` removes afterwards
  // against the version the row reports. So the residue here is not the final
  // rendering, and it is meant to still hold a separator.
  assert.equal(
    armLabel(system('Apache Flink', 'Flink'), arm('rowbinary-nt', 'Flink 2.2.1 · RowBinary'), 'rowbinary-nt'),
    '2.2.1 · RowBinary',
  );

  // The short, not the name, is what matches here: the name ends "+
  // clickhouse-kafka-connect" and the label ends "· clickhouse-kafka-connect".
  assert.equal(
    armLabel(
      system('Kafka Connect + clickhouse-kafka-connect', 'Kafka Connect'),
      arm('rowbinary-mv', 'Kafka Connect · clickhouse-kafka-connect · RowBinary + MV'),
      'rowbinary-mv',
    ),
    'clickhouse-kafka-connect · RowBinary + MV',
  );

  assert.equal(
    armLabel(
      system('ClickHouse Kafka table engine', 'ClickHouse Kafka'),
      arm('distributed', 'ClickHouse Kafka · Distributed forward'),
      'distributed',
    ),
    'Distributed forward',
  );
});

test('a variant that states no label falls back to its id', () => {
  assert.equal(armLabel(system('Vector', 'Vector'), arm('arrow'), 'arrow'), 'arrow');
  assert.equal(armLabel(undefined, undefined, 'arrow'), 'arrow');
});

test('a label that is only the system name keeps it rather than rendering empty', () => {
  assert.equal(armLabel(system('Vector', 'Vector'), arm('arrow', 'Vector'), 'arrow'), 'Vector');
});

test('the separator goes with the prefix, whichever one the descriptor used', () => {
  for (const sep of ['·', ':', '—', '-']) {
    const label = `Vector ${sep} ArrowStream`;
    assert.equal(armLabel(system('Vector', 'Vector'), arm('arrow', label), 'arrow'), 'ArrowStream', label);
  }
  // Padding around the separator is not required.
  assert.equal(armLabel(system('Vector', 'Vector'), arm('arrow', 'Vector:ArrowStream'), 'arrow'), 'ArrowStream');
});

// Fails against `armLabel` as written: the match tests only `startsWith`, so a
// prefix that ends mid-word is taken as the system name and the tail of that
// word is rendered as the arm. Today this renders "ised batch · RowBinary".
//
// The guard is to require the prefix to end on a word boundary before
// stripping — with `next = label.slice(prefix.length)`, proceed only when
// `next === '' || /^[\s·:—-]/.test(next)`. That leaves every case above
// unchanged, Flink's " 2.2.1 …" included, because each of those ends the
// prefix on whitespace or a separator.
//
// It does not cover a prefix that ends on a word boundary but is followed by
// more of the system's own name — "ClickHouse Kafka" against "ClickHouse Kafka
// engine · Distributed forward" stripped to "engine · Distributed forward".
// Nothing syntactic separates that from Flink's " 2.2.1 · RowBinary", which is
// stripped on purpose, so the descriptor states a label the short is an exact
// prefix of instead.
test('a label that merely opens with the same letters as the short is left alone', {todo: 'armLabel matches on startsWith, with no word boundary'}, () => {
  assert.equal(
    armLabel(system('Vector', 'Vector'), arm('batched', 'Vectorised batch · RowBinary'), 'batched'),
    'Vectorised batch · RowBinary',
  );
});
