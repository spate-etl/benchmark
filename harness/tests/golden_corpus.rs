//! The corpus is pinned by fingerprint, not merely by round-trip.
//!
//! Every other corpus test proves the generator agrees with *itself* — that
//! `encode_batch` and the typed decoder are mutual inverses, that the flatten
//! agrees with the closed-form expectations. None of them would notice if the
//! whole corpus shifted, because they would shift with it. A published
//! comparison cannot afford that: the corpus is the thing every arm is measured
//! against, and if it changes silently then two result sets are being compared
//! across different inputs while claiming to be like-for-like.
//!
//! So these values are absolute. The encoding fingerprints and the first three
//! closed-form expectations were taken from the pre-extraction harness
//! (`etl-rs/benchmarks/src/comparison_data.rs` at `f41280d51165`) and asserted
//! here unchanged, which is what proves the move between repositories did not
//! alter a single byte of what is being measured. The remaining closed-form
//! expectations pin columns the pre-extraction gate never checked, so they have
//! no earlier value to be carried forward from and are pinned from here on.
//!
//! **If one of these fails, do not update the constant to match.** Either the
//! generator changed — in which case `workload/workload.toml`, the schema, the
//! DDL or the marked region of `harness/src/corpus.rs` moved and
//! `dataset_version` must move with it, splitting the comparability group — or
//! something changed that should not have. Both need a human.

use spate_benchmark_harness::corpus::{encode_batch, expected, frame_confluent, str_fingerprint};

/// FNV-1a, inline. A dependency-free fingerprint: the point is stability across
/// versions of this repository, and a hash crate that changed its output would
/// be a false alarm rather than a finding.
fn fnv(seed: u64, bytes: &[u8]) -> u64 {
    let mut h = seed;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;

/// Batches fingerprinted. Enough to exercise every branch of the derivations —
/// the null-region every tenth batch, the null-quality every fifth event, all
/// eight units, all four tag-array lengths — without making the test slow.
const BATCHES: u64 = 1000;

#[test]
fn encoded_datums_are_byte_identical_to_the_original_harness() {
    let mut h = FNV_OFFSET;
    let mut total = 0usize;
    for id in 0..BATCHES {
        let b = encode_batch(
            id,
            1_772_000_000_000_000_i64 + i64::try_from(id).expect("fits i64"),
        );
        total += b.len();
        h = fnv(h, &b);
    }
    assert_eq!(
        format!("{h:016x}"),
        "5c66e4254fe9b472",
        "the Avro encoding of the corpus changed"
    );
    assert_eq!(total, 4_051_124, "the corpus changed size");
}

#[test]
fn confluent_framing_is_byte_identical_to_the_original_harness() {
    // Framing is checked separately from the datum because it is what every
    // competitor actually reads off the wire: a header regression would leave
    // the datums correct and every arm unable to decode them.
    let mut h = FNV_OFFSET;
    for id in 0..BATCHES {
        h = fnv(
            h,
            &frame_confluent(42, &encode_batch(id, i64::try_from(id).expect("fits i64"))),
        );
    }
    assert_eq!(
        format!("{h:016x}"),
        "5e3ced4c090971ea",
        "the Confluent framing of the corpus changed"
    );
}

#[test]
fn closed_form_expectations_are_unchanged() {
    // These are what the correctness gates compare against, so a drift here
    // would not fail a run — it would silently redefine what "correct" means.
    let e = expected(BATCHES);
    assert_eq!(e.rows, 73_500);
    assert_eq!(e.value_sum, 36_712_213_045_500);
    assert_eq!(e.value_scaled_sum, 1_903_970_758_089_327);
    assert_eq!(e.sensor_sum, 862_547_254_290_276_801_415_122);
    assert_eq!(e.region_sum, 789_224_923_606_139_885_762_498);
    assert_eq!(e.name_sum, 650_904_861_039_966_432_802_372);
    assert_eq!(e.unit_sum, 635_040_811_622_225_011_500);
    assert_eq!(e.tag_count_sum, 105_000);
    assert_eq!(e.tag_sum, 483_030_864_161_340_461_867_712);
    assert_eq!(e.batch_ts_sum, 130_242_000_036_711_750);
    assert_eq!(e.null_quality_rows, 17_500);
}

/// The fingerprint is half of the same-work checksum, and the other half lives
/// in a ClickHouse expression this test cannot execute.
///
/// So it is pinned against literals: if `str_fingerprint` is ever "simplified",
/// this fails immediately and says so, rather than the whole gate quietly
/// disagreeing with the server the next time an arm runs.
#[test]
fn the_string_fingerprint_is_unchanged() {
    // An absent value. A coalesced null region and an arm emitting `tags = []`
    // both land here, which is why the tag and region sums can catch them.
    assert_eq!(str_fingerprint(""), 0);
    // Shorter than the eight bytes `reinterpretAsUInt64` reads.
    assert_eq!(str_fingerprint("ms"), 57_568);
    // Exactly eight bytes, and the lower-case form the `name_upper` derivation
    // reads: the two must fingerprint differently or a skipped uppercase would
    // pass.
    assert_eq!(str_fingerprint("METRIC_7"), 9_557_931_003_773_166_724);
    assert_eq!(str_fingerprint("metric_7"), 11_872_851_856_941_630_628);
    // Longer than eight bytes, so the reversed half is doing the work.
    assert_eq!(str_fingerprint("sensor-1023"), 11_861_606_880_014_931_878);
    assert_eq!(str_fingerprint("region-3"), 11_930_833_490_185_392_805);
    // A three-element tag array, concatenated with no separator.
    assert_eq!(
        str_fingerprint("tag-15tag-0tag-1"),
        14_456_948_040_575_913_637
    );
}

#[test]
fn the_dataset_version_matches_the_committed_workload() {
    // Pinned so that editing the schema, the DDL's *structure*, the generator
    // constants or the generator's arithmetic without noticing is impossible:
    // this test fails and points at the change. Updating it is correct — but it
    // must happen in the same commit as the corpus change, which is the review
    // artefact worth having.
    //
    // The converse is pinned too, and it is why the `d1` scheme was replaced:
    // correcting a comment in any of those files must leave this alone. If a
    // pure prose change makes this test fail, the failure is in the derivation,
    // not in the prose.
    assert_eq!(
        spate_benchmark_harness::report::DATASET_VERSION,
        "d2-2ba9a27e5e89"
    );
}
