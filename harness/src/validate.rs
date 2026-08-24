//! The gate three other files already cite as their authority.
//!
//! `report.rs` documents a metric's unit as "constrained to a known set by
//! `results_are_valid`", and documents `sut.commit` as required when
//! `sut.version` is absent, "asserted by `results_are_valid`". `.gitattributes`
//! turns `merge=union` on every results file on the strength of
//! `results_are_valid` asserting `run_id` uniqueness across the tree. Until this
//! module existed, all three cited a function nobody had written: three doc
//! comments describing rules, and nothing anywhere enforcing one of them.
//!
//! The union-merge backstop is the sharpest of the three. `merge=union` is
//! correct for an append-only record log and is the reason concurrent runs do not
//! escalate to a manual merge, but its failure mode is silent — a line applied
//! twice is still valid JSONL, still parses, and is then medianed into a
//! published number by a site that has no way to know it read one measurement
//! twice. A duplicate `run_id` is the only observable trace it leaves, so
//! something has to look.
//!
//! # Why a library function rather than a test
//!
//! `README.md` advertises `bench validate` as "what CI checks, runnable locally",
//! and `CONTRIBUTING.md` tells a contributor to run it before opening a PR. A
//! rule set implemented twice — once in a test, once in the command — drifts, and
//! the drift surfaces either as a CI failure nobody can reproduce locally or, far
//! worse, as a clean local run and a green build over an archive with a
//! duplicated record in it. One function, called from both, cannot disagree with
//! itself.
//!
//! # Why every problem, never the first
//!
//! An archive is repaired by a person reading a list. A validator that returns
//! the first bad record turns that into one commit per defect, each one
//! discovering the next, which is how a broken archive stays broken.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::report::{Flag, Report, SCHEMA_VERSION};

/// Every unit a record is permitted to carry.
///
/// Derived from what the harness actually emits: the `Metric::` constructions in
/// `driver.rs` produce `records/s`, `us`, `cores` and `rows` directly, and
/// `bytes` through `Metric::bytes`. `MB/s` is here because `Metric::bytes_per_s`
/// hard-codes it — the set is drawn from the constructors a measurement can
/// reach, not from the call sites that happen to exist today, so adding a byte
/// rate to the driver does not fail validation on the archive it produces.
///
/// **Adding a unit is a deliberate act**, and that is the entire point of the
/// list. A unit is the only thing that tells a consumer what a number means, and
/// consumers act on it: the site's formatter scales `bytes` and does not scale
/// `MB/s`. That exact seam has already produced a published defect once — a value
/// in megabytes tagged `bytes` rendered 1010 MB as "1.0 KB", which is what
/// `Metric::bytes` exists to prevent. A new unit string arriving unannounced gets
/// whichever branch of a consumer's formatter it falls through. So a metric in a
/// new unit is added here in the same commit that emits it, by someone who has
/// thought about what every consumer will do with it.
pub const ALLOWED_UNITS: [&str; 7] = ["MB/s", "bytes", "cores", "ratio", "records/s", "rows", "us"];

/// What a clean tree contained.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Summary {
    /// Result files walked.
    pub files: usize,
    /// Records read from them.
    pub records: usize,
}

/// Checks every committed record against the rules the rest of this repository
/// already tells its readers are enforced.
///
/// Walks `root` itself rather than going through [`crate::results::load_all`],
/// for one reason: that loader returns records, and a problem a maintainer can
/// act on has to name the file and line it is in. "Some record has a bad unit" is
/// not a repairable report of a tree with ten thousand records in it.
///
/// # Errors
///
/// Every problem found, in tree order, one string each. A file that cannot be
/// read and a line that cannot be parsed are problems like any other, so one
/// corrupt line does not hide the eleven defects after it.
pub fn results_are_valid(root: &Path) -> Result<Summary, Vec<String>> {
    let mut problems = Vec::new();
    let mut summary = Summary::default();

    let mut files = Vec::new();
    if let Err(e) = collect_jsonl(root, &mut files) {
        return Err(vec![format!("walk {}: {e}", root.display())]);
    }
    // Sorted so two runs of the validator over one tree report the same problems
    // in the same order. Directory iteration order is not stable across
    // filesystems, and a diffable failure list is worth more than the sort costs.
    files.sort();

    // `run_id` to where it was first seen. The whole tree, not one file: a union
    // merge double-applies a line within a file, but a botched hand-edit of an
    // archive moves records between them, and both must be caught.
    let mut seen: BTreeMap<String, String> = BTreeMap::new();

    for path in &files {
        summary.files += 1;
        let src = match std::fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) => {
                problems.push(format!("read {}: {e}", path.display()));
                continue;
            }
        };
        for (i, line) in src.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let site = format!("{}:{}", path.display(), i + 1);
            let record: Report = match serde_json::from_str(line) {
                Ok(r) => r,
                Err(e) => {
                    problems.push(format!("{site}: {e}"));
                    continue;
                }
            };
            summary.records += 1;

            let at = format!("{site} ({})", record.run_id);
            if let Some(first) = seen.insert(record.run_id.clone(), site) {
                problems.push(format!(
                    "{at}: this run_id already appears at {first}. A UUIDv7 is \
                     generated once per execution and cannot legitimately occur \
                     twice, so this is a record applied twice — most likely by the \
                     `merge=union` that .gitattributes sets on results files. Left \
                     alone it is medianed into a published number as though two \
                     measurements agreed. Delete the duplicate line, keeping one."
                ));
            }
            check_record(&record, &at, &mut problems);
        }
    }

    if problems.is_empty() {
        Ok(summary)
    } else {
        Err(problems)
    }
}

/// The per-record rules. Every one of them appends and none of them returns, so a
/// record with four defects reports four.
fn check_record(r: &Report, at: &str, problems: &mut Vec<String>) {
    // A record from the FUTURE is the failure; an older one is not.
    //
    // The tempting rule is `schema != SCHEMA_VERSION`, and it is wrong here in a
    // way worth writing down, because it looks stricter and therefore safer. An
    // append-only archive necessarily accumulates records at older schema
    // versions — that is what "never overwritten" means — so a validator that
    // refuses them turns every schema bump into a demand to rewrite records that
    // `results.rs` deliberately has no capability to rewrite. The forcing
    // function would force a violation of the invariant it is meant to protect.
    //
    // A record numbered ABOVE this binary's schema is a real problem: it was
    // written by something newer, and serde will have quietly accepted whatever
    // subset of it happens to fit these structs, so "it parsed" is not evidence
    // it was understood. An older record that parsed was understood — the fields
    // this binary reads meant the same thing when they were written.
    if r.schema > SCHEMA_VERSION {
        problems.push(format!(
            "{at}: schema {}, but this binary understands at most schema \
             {SCHEMA_VERSION}. It was written by a newer harness, so any field \
             whose meaning changed has been read under the old meaning and \
             silently. Update this binary rather than trusting what it just \
             parsed.",
            r.schema
        ));
    }

    // An invocation id, once the protocol carries one.
    //
    // Conditioned on the protocol version rather than required outright: every
    // record already in the archive predates the field, and a validator that
    // rejected them would be demanding a rewrite of an append-only store. From
    // harness 2 on it is what tells a consumer which repetitions were one
    // sitting — the site previously approximated that by UTC calendar day, so a
    // sweep crossing midnight split into two published rows and two sweeps on
    // one day merged into one.
    if r.run.harness_version >= 2 && r.run.invocation_id.trim().is_empty() {
        problems.push(format!(
            "{at}: harness {} records carry an invocation_id and this one is \
             empty. Without it, repetitions of one sweep cannot be told from two \
             sweeps that happened to land on the same day.",
            r.run.harness_version
        ));
    }

    // A run taken for a reason that bars publication must never reach the
    // archive.
    //
    // This is the check the vocabulary was waiting for. `Trigger::Pr` has said
    // "Never published" since schema 2 was written and was set by nothing and
    // refused by nothing; a `Flag::PrRun` said the same words and was likewise
    // inert. A tuning sweep makes that gap expensive rather than merely untidy:
    // it produces dozens of *real* measurements of a competitor arm, every one
    // of them taken at a configuration nobody has decided to publish, and the
    // failure mode it invites is the one that would discredit the whole
    // exercise — run until the number is liked, then commit that run.
    //
    // Refusing here is what makes the marking structural rather than advisory.
    // `bench validate` is what CI runs and what CONTRIBUTING tells a contributor
    // to run, so a tuning record that reaches `results/` fails the build instead
    // of being published; a flag a consumer may choose to honour would not have
    // stopped it, because the site's loader filters on nothing.
    //
    // The remedy is never to edit the trigger. The one configuration a search
    // concludes on is re-measured as an ordinary run, under the descriptor that
    // now declares it.
    if let Some(bar) = r.run.trigger.publication_bar() {
        problems.push(format!(
            // The wire spelling, so the message names the string a reader will
            // find in the file rather than a Rust identifier they will not.
            "{at}: trigger is {} ({bar}). Such a record is a measurement taken \
             for a reason that bars publication, and committing it would put it in \
             front of readers as though it were the arm's published configuration. \
             Delete the file rather than the trigger: the conclusion of a search \
             belongs in a document beside its rejected points, and the \
             configuration it settles on is re-measured as an ordinary run once \
             the descriptor declares it.",
            format!("{:?}", r.run.trigger).to_lowercase()
        ));
    }

    // A fixture run must never reach the archive.
    //
    // `Class::Fixture` is documented "synthetic data for development, never
    // published", and the driver marks every record it produces. The marking is
    // only worth having if something refuses the record downstream — otherwise
    // it is a note on a number that is already in the published set. Committing
    // one is the mistake this catches, and it catches it in `bench validate`,
    // which is what a contributor runs before opening the pull request.
    if r.flags.contains(&Flag::UnpublishableEnvironment) {
        problems.push(format!(
            "{at}: carries the `unpublishable_environment` flag, so it was produced \
             on a fixture environment against synthetic data. Such a run is for \
             development and its records must not be committed — delete the file \
             rather than the flag."
        ));
    }

    // Provenance. Every published number has to be able to say what produced it,
    // and these are the fields that say it.
    if r.sut.image_digest.trim().is_empty() {
        problems.push(format!(
            "{at}: sut.image_digest is empty. The digest is what makes the number \
             attributable — a tag can be re-pushed under the same name, so it is \
             the only field here that cannot lie. A run that could not read it is \
             recorded as failed rather than published without it."
        ));
    } else if !r.sut.image_digest.starts_with("sha256:") {
        problems.push(format!(
            "{at}: sut.image_digest is {:?}, which is not a `sha256:…` digest. \
             `docker inspect` emits the algorithm prefix; a value without it is \
             something a human typed.",
            r.sut.image_digest
        ));
    }

    if !present(r.sut.version.as_deref()) && !present(r.sut.commit.as_deref()) {
        problems.push(format!(
            "{at}: neither sut.version nor sut.commit is set. `version` is allowed \
             to be absent only for a system with no release concept, and then \
             `commit` carries the identity instead. With both missing the record \
             cannot say what was measured, which is the one thing a published \
             number must do."
        ));
    }

    for (field, value) in [
        ("run.env_id", &r.run.env_id),
        ("run.env_digest", &r.run.env_digest),
        ("run.dataset_version", &r.run.dataset_version),
    ] {
        if value.trim().is_empty() {
            problems.push(format!(
                "{at}: {field} is empty. It is one of the fields that decides which \
                 other records this one may be drawn on an axis with, and an empty \
                 one silently compares across the difference it was meant to name."
            ));
        }
    }

    for (name, m) in &r.metrics {
        if !ALLOWED_UNITS.contains(&m.unit.as_str()) {
            problems.push(format!(
                "{at}: metric {name} carries unit {:?}, which is not one of {}. If \
                 the harness has genuinely started emitting a new unit, add it to \
                 ALLOWED_UNITS in harness/src/validate.rs in the same commit, \
                 having checked what every consumer does with it.",
                m.unit,
                ALLOWED_UNITS.join(", ")
            ));
        }
    }

    // `Status::carries_metrics` is the authority in both directions. The reverse
    // check is the one that matters: a status saying "we never got a number" on a
    // record that carries numbers leaves a consumer to decide which half to
    // believe, and different consumers decide differently.
    match (r.status.carries_metrics(), r.metrics.is_empty()) {
        (true, true) => problems.push(format!(
            "{at}: status {:?} says this record carries publishable numbers, but \
             `metrics` is empty. An arm that produced no measurement is Failed, \
             and one that cannot express the variant at all is Unsupported.",
            r.status
        )),
        (false, false) => problems.push(format!(
            "{at}: status {:?} carries no metrics, but {} are attached. A consumer \
             filtering on status would drop numbers that exist; one iterating \
             metrics would publish numbers the status says are not to be believed.",
            r.status,
            r.metrics.len()
        )),
        _ => {}
    }

    if r.rep == 0 || r.rep > r.reps {
        problems.push(format!(
            "{at}: rep {} of {}. Repetition indices are 1-based and bounded by the \
             count the invocation asked for; `reps` exists so a reader can see that \
             rep 2 of 3 is missing rather than having to infer it, and it cannot do \
             that job if a record sits outside the range it declares.",
            r.rep, r.reps
        ));
    }
}

/// Whether an optional provenance field actually carries something.
///
/// An empty or whitespace string counts as absent. `Some("")` is a field that was
/// serialised rather than one that was resolved, and treating it as present is
/// how a record with no identity passes a presence check.
fn present(field: Option<&str>) -> bool {
    field.is_some_and(|v| !v.trim().is_empty())
}

/// Every `.jsonl` under `dir`, recursively.
///
/// A near-copy of the walker in `results.rs`, which is private there and whose
/// caller discards the paths. Kept separate rather than widening that module's
/// API: this one exists to attribute a problem to a file, which is a validator's
/// need and not a loader's.
fn collect_jsonl(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_jsonl(&path, out)?;
        } else if path.extension().is_some_and(|e| e == "jsonl") {
            out.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::report::{Infra, Kind, Metric, RunMeta, Status, Sut, Trigger, now_ms};

    fn sut() -> Sut {
        Sut {
            entrant: "spate".to_owned(),
            variant_id: "native".to_owned(),
            version: Some("0.1.0-dev".to_owned()),
            commit: Some("6f28a8b8912e".to_owned()),
            image_digest: format!("sha256:{}", "a".repeat(64)),
            image: "spate-bench-spate".to_owned(),
            toolchain: Some("rustc 1.97.0".to_owned()),
        }
    }

    fn infra() -> Infra {
        Infra {
            digest: "e3b0c44298fc".to_owned(),
            broker: "redpanda".to_owned(),
            broker_version: "v26.1.13".to_owned(),
            broker_image_digest: format!("sha256:{}", "b".repeat(64)),
            broker_cpus: "800000 100000".to_owned(),
            broker_memory: "8589934592".to_owned(),
            clickhouse_version: "26.3.1.1".to_owned(),
            clickhouse_image_digest: format!("sha256:{}", "c".repeat(64)),
            clickhouse_cpus: "500000 100000".to_owned(),
            clickhouse_memory: "12884901888".to_owned(),
            partitions: 8,
            storage: "local-nvme".to_owned(),
            registry: "redpanda-builtin".to_owned(),
            ceiling_msgs_per_s: 305_554,
            ceiling_bytes_per_s: 256_700_000,
            ceiling_rows_per_s: 3_100_000,
        }
    }

    /// A record that passes every rule, for a test to break one field of.
    fn report() -> Report {
        Report::new(
            "kafka_avro_clickhouse",
            Kind::Measurement,
            Status::Ok,
            sut(),
            RunMeta::new("test-env", "deadbeef", Trigger::Manual, infra()),
        )
        .metric("rows_per_s", Metric::maximize(4_383_663.0, "records/s"))
    }

    /// A private directory under the system temp dir, named so two runs of the
    /// suite cannot collide. Hand-rolled rather than a `tempfile` dev-dependency,
    /// matching `results.rs`.
    fn temp_root(tag: &str) -> PathBuf {
        let name = format!("spate-bench-{tag}-{}-{}", std::process::id(), now_ms());
        let dir = std::env::temp_dir().join(name);
        std::fs::create_dir_all(&dir).expect("create temp root");
        dir
    }

    /// Writes one file of records and validates the tree, cleaning up first so a
    /// failing assertion cannot leave a directory behind for someone to find
    /// weeks later.
    fn validate_lines(tag: &str, lines: &[String]) -> Result<Summary, Vec<String>> {
        let root = temp_root(tag);
        let dir = root.join("test-env").join("spate");
        std::fs::create_dir_all(&dir).expect("create results tree");
        let body: String = lines.iter().map(|l| format!("{l}\n")).collect();
        std::fs::write(dir.join("2026-07.jsonl"), body).expect("seed the archive");
        let out = results_are_valid(&root);
        let _ = std::fs::remove_dir_all(&root);
        out
    }

    fn line(r: &Report) -> String {
        r.to_line().expect("serialize")
    }

    #[test]
    fn a_record_the_harness_wrote_is_accepted() {
        let s = validate_lines("valid", &[line(&report()), line(&report())])
            .expect("records the harness itself produced must validate");
        assert_eq!(s.records, 2);
        assert_eq!(s.files, 1);
    }

    #[test]
    fn an_invocation_id_is_required_only_once_the_protocol_carries_one() {
        // Both halves matter. Requiring it outright would reject every record
        // already in the archive and demand a rewrite of an append-only store;
        // not requiring it at all leaves the site guessing which repetitions
        // were one sitting, which it previously did by calendar day.
        let mut old = report();
        old.run.harness_version = 1;
        old.run.invocation_id = String::new();
        validate_lines("no-invocation-v1", &[line(&old)])
            .expect("a record predating the field is not a defect");

        let mut current = report();
        current.run.harness_version = 2;
        current.run.invocation_id = String::new();
        let problems = validate_lines("no-invocation-v2", &[line(&current)])
            .expect_err("a harness 2 record without one must be refused");
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("invocation_id"), "{}", problems[0]);
    }

    #[test]
    fn an_older_schema_that_still_parses_is_accepted_and_a_newer_one_is_not() {
        // The asymmetry is the point, and the tempting "symmetric" rule —
        // `schema != SCHEMA_VERSION` — would break the archive's central
        // invariant. An append-only store accumulates older records by
        // definition; refusing them would demand a rewrite that `results.rs`
        // deliberately cannot perform. A record from a NEWER harness is the real
        // hazard: serde accepted whatever subset fitted these structs, so
        // "it parsed" is not evidence it was understood.
        let mut old = report();
        old.schema = SCHEMA_VERSION - 1;
        validate_lines("older-schema", &[line(&old)])
            .expect("a record this binary can still read is not a defect");

        let mut future = report();
        future.schema = SCHEMA_VERSION + 1;
        let problems = validate_lines("newer-schema", &[line(&future)])
            .expect_err("a record from a newer harness must be refused");
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("newer harness"), "{}", problems[0]);
    }

    #[test]
    fn a_line_applied_twice_by_a_union_merge_is_reported() {
        // The defect .gitattributes names: `merge=union` cannot detect that it
        // double-applied a record, the result is still valid JSONL, and nothing
        // downstream can tell one measurement read twice from two that agreed.
        let r = report();
        let problems = validate_lines("dup", &[line(&r), line(&r)])
            .expect_err("a duplicated run_id must be a problem");
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains(&r.run_id), "{}", problems[0]);
        assert!(problems[0].contains("2026-07.jsonl:1"), "{}", problems[0]);
    }

    #[test]
    fn every_problem_in_a_tree_is_reported_not_just_the_first() {
        // The property that makes an archive repairable in one pass.
        let mut a = report();
        a.sut.image_digest = String::new();
        let mut b = report();
        b.rep = 4;
        b.reps = 3;
        let mut c = report();
        c.run.env_id = String::new();

        let problems = validate_lines("many", &[line(&a), line(&b), line(&c)])
            .expect_err("three broken records must produce three problems");
        assert_eq!(problems.len(), 3, "{problems:?}");
        for (i, id) in [a.run_id, b.run_id, c.run_id].iter().enumerate() {
            assert!(problems[i].contains(id), "{}", problems[i]);
        }
    }

    /// The property the whole tuning-marking design rests on: a sweep's records
    /// cannot become published results, because committing one fails the build.
    ///
    /// Both barring triggers are checked, because the point of adding
    /// `Trigger::Tuning` rather than reusing `Trigger::Pr` was that the two are
    /// different causes — and a distinction is only worth drawing if both sides
    /// of it are enforced. `Trigger::Pr` had carried the words "Never published"
    /// since schema 2 and had never once been refused.
    #[test]
    fn a_record_whose_trigger_bars_publication_is_refused_by_name() {
        for (trigger, needle) in [
            (Trigger::Tuning, "tuning"),
            (Trigger::Pr, "pull-request run"),
        ] {
            let mut r = report();
            r.run.trigger = trigger;
            let problems = validate_lines("barred", &[line(&r)])
                .expect_err("a record that must never be published must be refused");
            assert_eq!(problems.len(), 1, "{problems:?}");
            assert!(problems[0].contains(needle), "{}", problems[0]);
            // The remedy must not read as "edit the trigger", which would leave
            // the number published and the marking gone.
            assert!(
                problems[0].contains("Delete the file rather than the trigger"),
                "{}",
                problems[0]
            );
        }
    }

    /// The converse, so the rule cannot quietly widen into "every record CI did
    /// not take is suspect". A hand-run measurement on the reference rig is how
    /// every published number in this archive was produced.
    #[test]
    fn an_ordinary_trigger_is_not_treated_as_a_publication_bar() {
        for trigger in [Trigger::Manual, Trigger::Nightly, Trigger::Release] {
            let mut r = report();
            r.run.trigger = trigger;
            validate_lines("ordinary", &[line(&r)])
                .unwrap_or_else(|p| panic!("{trigger:?} must validate: {p:?}"));
        }
    }

    #[test]
    fn a_record_with_neither_a_version_nor_a_commit_is_rejected() {
        let mut r = report();
        r.sut.version = None;
        r.sut.commit = None;
        let problems = validate_lines("prov", &[line(&r)]).expect_err("no provenance");
        assert!(problems[0].contains("sut.version"), "{}", problems[0]);

        // An empty string is absence wearing a value's clothes.
        let mut blank = report();
        blank.sut.version = Some("   ".to_owned());
        blank.sut.commit = Some(String::new());
        let problems = validate_lines("prov-blank", &[line(&blank)])
            .expect_err("a blank version and a blank commit are still no provenance");
        assert!(problems[0].contains("sut.commit"), "{}", problems[0]);
    }

    #[test]
    fn a_unit_outside_the_known_set_is_rejected_by_name() {
        let r = report().metric("footprint", Metric::minimize(1010.0, "MB"));
        let problems = validate_lines("unit", &[line(&r)]).expect_err("unknown unit");
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("footprint"), "{}", problems[0]);
        assert!(problems[0].contains("\"MB\""), "{}", problems[0]);
    }

    #[test]
    fn every_unit_the_harness_emits_is_in_the_known_set() {
        // Pins the set to the constructors rather than to a list someone once
        // typed: `Metric::bytes` and `Metric::bytes_per_s` choose their own unit
        // strings, so a change to either would otherwise make the validator
        // reject records the harness had just written.
        assert!(ALLOWED_UNITS.contains(&Metric::bytes(1.0).unit.as_str()));
        assert!(ALLOWED_UNITS.contains(&Metric::bytes_per_s(1.0).unit.as_str()));
        assert!(ALLOWED_UNITS.contains(&Metric::share(1.0).unit.as_str()));
    }

    #[test]
    fn status_and_metrics_must_agree_in_both_directions() {
        let mut empty = report();
        empty.metrics.clear();
        let problems = validate_lines("ok-empty", &[line(&empty)]).expect_err("ok with no metrics");
        assert!(problems[0].contains("metrics"), "{}", problems[0]);

        let mut failed = report();
        failed.status = Status::Failed;
        let problems =
            validate_lines("failed-full", &[line(&failed)]).expect_err("failed with metrics");
        assert!(problems[0].contains("Failed"), "{}", problems[0]);
    }

    #[test]
    fn a_tree_with_no_results_yet_is_not_a_failure() {
        let root = temp_root("empty");
        let s = results_are_valid(&root).expect("an empty tree has no problems");
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(s, Summary::default());
    }
}
