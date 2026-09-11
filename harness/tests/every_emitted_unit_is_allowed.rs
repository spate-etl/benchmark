//! `bench validate` checks the units of records that already exist. A metric
//! introduced with a unit the list does not carry therefore passes every gate
//! in this repository and fails at the collector, after a run has been paid
//! for — which is how `nr_throttled` reached the archive boundary carrying
//! `"periods"` and stranded six records there.
//!
//! This reads the driver's own source instead, so the unit is checked where it
//! is written rather than where it lands.

use spate_benchmark_harness::validate::ALLOWED_UNITS;

/// Every unit literal passed to a free-form `Metric` constructor, in source
/// order. `Metric::bytes` and friends carry their unit internally and are
/// covered by the constructor assertions in `validate.rs`.
fn emitted_units(src: &str) -> Vec<String> {
    let mut out = Vec::new();
    for (start, _) in src.match_indices("Metric::") {
        let tail = &src[start..];
        if !tail.starts_with("Metric::minimize(") && !tail.starts_with("Metric::maximize(") {
            continue;
        }
        // Walk to the matching parenthesis so a call spanning several lines is
        // one call, not a truncated prefix.
        let open = tail
            .find('(')
            .expect("a constructor call has an argument list");
        let (mut depth, mut end) = (0usize, None);
        for (i, c) in tail[open..].char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = Some(open + i);
                        break;
                    }
                }
                _ => {}
            }
        }
        let call = &tail[open..end.expect("an unclosed constructor call would not compile")];
        if let Some(close) = call.rfind('"')
            && let Some(quote) = call[..close].rfind('"')
        {
            out.push(call[quote + 1..close].to_owned());
        }
    }
    out
}

#[test]
fn every_unit_the_driver_emits_is_on_the_allow_list() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/driver.rs");
    let src = std::fs::read_to_string(&path).expect("read driver.rs");
    // Published metrics only: the test module below deliberately builds records
    // carrying units the list rejects, to prove that it rejects them.
    let published = src
        .split("\nmod tests {")
        .next()
        .expect("driver.rs has a body");

    let units = emitted_units(published);
    assert!(
        units.len() >= 8,
        "found only {} unit literals in {}; the scan has stopped matching the \
         source it is meant to guard",
        units.len(),
        path.display()
    );
    for unit in &units {
        assert!(
            ALLOWED_UNITS.contains(&unit.as_str()),
            "the driver emits unit {unit:?}, which `bench validate` refuses. Add it \
             to ALLOWED_UNITS in harness/src/validate.rs, having checked what every \
             consumer does with it — a record carrying it is rejected at the \
             collector, after the run is paid for"
        );
    }
}

#[test]
fn the_scan_reads_a_call_that_spans_several_lines() {
    // `rows_per_s_per_core` is written across seven lines in the driver, and a
    // line-wise scan would take its unit from the wrong place or miss it.
    let src = r#"
        .metric(
            LEAD_METRIC,
            Metric::maximize(
                if d.cost.cores_used > 0.0 { d.rows_per_s / d.cost.cores_used } else { 0.0 },
                "records/s",
            ),
        )
        .metric("nr_throttled", Metric::minimize(d.cost.nr_throttled, "periods"))
    "#;
    assert_eq!(emitted_units(src), vec!["records/s", "periods"]);
}
