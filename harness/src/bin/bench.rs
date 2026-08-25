//! `bench` — the benchmark driver.
//!
//! Subcommands and flags rather than environment variables, deliberately. The
//! previous harness took its configuration from the environment, and that is
//! precisely how a runner script, the driver's own defaults and the written
//! methodology came to state three different infrastructure envelopes while
//! every recorded number stayed silent about which had been in force.
//!
//! Two properties of this tool are load-bearing rather than convenient:
//!
//! - **`--dry-run` prints the exact execution list.** A full sweep costs hours,
//!   so "which arms will this actually run?" has to be answerable before
//!   spending them rather than inferred afterwards from what appeared.
//! - **Nothing here can truncate a results file.** See `results.rs`: the
//!   capability does not exist, so retention is not a matter of remembering.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use spate_benchmark_harness::ceiling::{self, PassOptions};
use spate_benchmark_harness::corpus;
use spate_benchmark_harness::docker;
use spate_benchmark_harness::driver::{self, Mode, RunOptions, SUSTAINED_WINDOW_S};
use spate_benchmark_harness::entrant::{self, Entrant, Status};
use spate_benchmark_harness::environment::Environment;
use spate_benchmark_harness::infra;
use spate_benchmark_harness::report::{DATASET_VERSION, HARNESS_VERSION, Trigger, now_ms};
use spate_benchmark_harness::results;
use spate_benchmark_harness::sampler::ArmLock;
use spate_benchmark_harness::select::{self, Selector};
use spate_benchmark_harness::validate;

/// Exit code for a refusal: the run was attempted and declined to produce a
/// number. Distinct from 1 (a usage error) so a sweep script can tell "this arm
/// is invalid" from "you called me wrong".
const EXIT_REFUSED: u8 = 3;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = args.first().map(String::as_str) else {
        usage();
        return ExitCode::from(1);
    };
    let rest = &args[1..];

    let root = repo_root();

    let result = match cmd {
        "list" => cmd_list(&root, rest),
        "validate" => cmd_validate(&root),
        "build" => cmd_build(&root, rest),
        "stale" => cmd_stale(&root),
        "prefill" => cmd_prefill(&root, rest),
        "ceiling" => cmd_ceiling(&root, rest),
        "run" => cmd_run(&root, rest),
        "-h" | "--help" | "help" => {
            usage();
            return ExitCode::SUCCESS;
        }
        other => Err(format!("unknown subcommand {other:?}. Try `bench help`.")),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("bench: {msg}");
            ExitCode::from(if msg.starts_with("REFUSED") {
                EXIT_REFUSED
            } else {
                1
            })
        }
    }
}

fn usage() {
    println!(
        "\
bench — the Spate Benchmark driver

  bench list [--json]            systems, variants, and when each was last measured
  bench validate                 what CI checks, runnable locally
  bench build <selector>...      build the selected entrants' images
  bench stale                    arms whose measurement has fallen behind
  bench prefill                  populate the topic once per corpus (do this first)
  bench ceiling [--measure [--write]] [--seconds N] [--threads N]
                [--ingest-max N] [--only <format>]...
                                 prove the infrastructure is not the bottleneck.
                                 Without --measure it reports the committed
                                 ceilings and refuses if they cannot be gated
                                 against this corpus. With --measure it runs the
                                 rig against live infrastructure, which takes
                                 minutes and needs the corpus prefilled; --write
                                 then merges the result into the environment's
                                 ceilings file.
                                 --threads is the CONSUME pass's consumer count
                                 and may not exceed the topic's partitions. The
                                 ingest pass does not take a concurrency: it
                                 sweeps upwards until the target stops absorbing
                                 more, and --ingest-max only bounds how far it
                                 may look before giving up and refusing.
                                 --seconds is the ingest pass's window per rung
                                 and an UPPER BOUND on the consume pass's: the
                                 consume window is sized against the backlog in
                                 front of it, because from inside the network the
                                 whole corpus is under a second of reading and a
                                 longer window would measure an idle broker.
                                 Both passes run from containers on the bench
                                 network, as every arm does, and the ingest pass
                                 writes into ceiling tables of its own rather
                                 than the tables the arms are gated on.
                                 --only narrows the ingest pass to one insert
                                 format, so that a
                                 SEARCH over infrastructure allocations can
                                 measure the format that actually binds
                                 rather than all three. It changes which ceilings
                                 are measured and never how. Writing a narrowed
                                 pass is refused when it would leave the rest of
                                 the file describing a different envelope.
  bench run <selector>... [--reps N] [--dry-run] [--env <id>]
                          [--mode drain|sustained] [--rate N] [--window S]
                          [--trigger nightly|manual|pr|tuning|release]
                          [--knob <name>=<value>]...

tuning:
  --knob replaces one of the selected variant's declared knob values for this
  invocation, which is how a configuration search walks a product of knob
  values without editing a committed descriptor per cell. It requires
  `--trigger tuning`, because the record would otherwise name a variant whose
  published knobs it did not run.

  A tuning run's records are written to tuning/ rather than results/, so a
  search cannot land in the same file as the arm's published numbers, and
  `bench validate` refuses any record under results/ whose trigger bars
  publication. A sweep's measurements therefore cannot become published
  results: declare the configuration the search settles on in the descriptor,
  then re-measure it as an ordinary run.

  A knob the variant does not declare, or a combination the entrant's
  [[constraints]] rule out, is refused before any container starts, so
  `--dry-run` is enough to test a cell.

modes:
  drain      the default. Replays a prefilled topic to exhaustion and times the
             whole drain. Throughput and efficiency come from here.
  sustained  offers --rate messages/s for --window seconds. Latency comes from
             here and from nowhere else, and it has to be asked for: this host
             cannot hold the generator, the broker, ClickHouse and an arm at
             once, so a sustained run is usually a saturation measurement.

selectors:
  <entrant>[:<variant>]          '*' means any, '@<tag>' overrides the image
                                 there is no version position: a version is
                                 resolved by running the image, so name the
                                 image instead

  spate                          every variant of one system
  spate:rowbinary                one arm
  '*'                            everything runnable
  flink@spate-bench-flink:2.3.0  a specific image — how a new version is measured

`bench run` only ever appends. There is no code path in it that truncates a
results file."
    );
}

/// Flag parsing. Deliberately hand-rolled and strict: an unrecognised flag is an
/// error rather than something ignored, because a typo'd `--reps` that silently
/// ran once instead of three times would produce a result whose repetition count
/// nobody questioned.
fn opts_from(args: &[String], root: &Path) -> Result<RunOptions, String> {
    // Resolved AFTER the flags: `default_env` refuses to guess when several
    // environments exist, and that refusal must not fire on an invocation that
    // named one with `--env`.
    let mut env_id: Option<String> = None;
    let mut o = RunOptions {
        reps: 3,
        mode: Mode::Drain,
        env_id: String::new(),
        trigger: Trigger::Manual,
        dry_run: false,
        fresh_infra: false,
        fail_fast: false,
        topic: spate_benchmark_harness::corpus::TOPIC.to_owned(),
        // The corpus has to be long enough that the fastest arm's window
        // clears `driver::MIN_WINDOW_S`; below it a record carries
        // `short_window`. The fastest measured arm drains 30M in 131s, so 40M
        // holds the floor with a third in hand for the next release's speedup.
        // `--batches` moves no digest, so raising it later is free.
        batches: 40_000_000,
        knobs: BTreeMap::new(),
    };
    // Collected as locals and folded into `Mode` after the loop: a sustained run
    // carries its rate and window inside the variant, so no single flag can
    // express it and no ordering of flags may change the result.
    let mut mode = "drain".to_owned();
    let mut rate: Option<u64> = None;
    let mut window_s = SUSTAINED_WINDOW_S;
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if !a.starts_with('-') {
            i += 1;
            continue;
        }
        let mut value = || -> Result<String, String> {
            i += 1;
            args.get(i)
                .cloned()
                .ok_or_else(|| format!("{} needs a value", args[i - 1]))
        };
        match a.as_str() {
            "--reps" => o.reps = value()?.parse().map_err(|e| format!("--reps: {e}"))?,
            "--env" => env_id = Some(value()?),
            "--topic" => o.topic = value()?,
            "--batches" => o.batches = value()?.parse().map_err(|e| format!("--batches: {e}"))?,
            "--trigger" => {
                o.trigger = match value()?.as_str() {
                    "nightly" => Trigger::Nightly,
                    "manual" => Trigger::Manual,
                    "pr" => Trigger::Pr,
                    "tuning" => Trigger::Tuning,
                    "release" => Trigger::Release,
                    other => return Err(format!("unknown --trigger {other:?}")),
                }
            }
            "--knob" => {
                let raw = value()?;
                let (k, v) = raw.split_once('=').ok_or_else(|| {
                    format!("--knob {raw:?}: expected <name>=<value>, e.g. --knob parallelism=4")
                })?;
                if k.is_empty() {
                    return Err(format!("--knob {raw:?}: the name is empty"));
                }
                // An integer where the text is one, a string otherwise, matching
                // what a descriptor may declare. Guessing is safe because the
                // two render identically into an environment variable; the type
                // matters only to the record's variant map and to the
                // entrant's own `[[constraints]]`, both of which want a number
                // to be a number.
                let value = v
                    .parse::<i64>()
                    .map_or_else(|_| toml::Value::String(v.to_owned()), toml::Value::Integer);
                o.knobs.insert(k.to_owned(), value);
            }
            "--mode" => mode = value()?,
            "--rate" => rate = Some(value()?.parse().map_err(|e| format!("--rate: {e}"))?),
            "--window" => {
                window_s = value()?.parse().map_err(|e| format!("--window: {e}"))?;
            }
            "--dry-run" => o.dry_run = true,
            "--fresh-infra" => o.fresh_infra = true,
            "--fail-fast" => o.fail_fast = true,
            other => return Err(format!("unknown flag {other:?}. Try `bench help`.")),
        }
        i += 1;
    }

    // Drain is the default and sustained has to be asked for, which is
    // METHODOLOGY's rule rather than a convenience: sustained on this host
    // oversubscribes it, and a mode that could be reached by accident would put
    // contention into the published numbers.
    o.mode = match mode.as_str() {
        "drain" if rate.is_some() => {
            return Err("--rate only means something with --mode sustained".to_owned());
        }
        "drain" => Mode::Drain,
        "sustained" => {
            let offered_msgs_per_s = rate.ok_or(
                "--mode sustained needs --rate <messages/s>; there is no default, \
                 because the rate is the experiment",
            )?;
            if offered_msgs_per_s == 0 || window_s == 0 {
                return Err("--rate and --window must be positive".to_owned());
            }
            Mode::Sustained {
                offered_msgs_per_s,
                window_s,
            }
        }
        other => {
            return Err(format!(
                "unknown --mode {other:?}; expected drain or sustained"
            ));
        }
    };

    // A knob override and a publishable trigger cannot be asked for together,
    // and the coupling is the whole point of the flag rather than a restriction
    // on it.
    //
    // A record names a `variant_id`; that variant's knobs are committed in
    // `entrants/<id>/entrant.toml` for any reader to check. A run with
    // `--knob` did not use them, so the record describes a configuration that
    // exists nowhere in the repository — which is exactly what a configuration
    // search is for, and exactly what must never be published. Making the
    // override reachable only through a trigger the validator refuses means the
    // marking cannot be forgotten: there is no way to produce such a record
    // unmarked.
    //
    // Refused rather than silently upgraded to `tuning`. `--trigger manual
    // --knob max_rows=100000` is a contradiction, and quietly resolving it in
    // the operator's favour would rewrite what they explicitly asked for.
    if !o.knobs.is_empty() && !o.trigger.bars_publication() {
        return Err(format!(
            "REFUSED: --knob overrides the knobs a committed variant declares, so the \
             record would name a configuration that exists nowhere in this repository \
             — and it would sit in results/ beside numbers that do. Add `--trigger \
             tuning`. A tuning run is refused by `bench validate` if it ever reaches \
             results/, which is what makes a search safe to run: declare the \
             configuration it settles on in the descriptor, then re-measure it as an \
             ordinary run.\n\nAsked for: {} with --trigger {}.",
            o.knobs
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", "),
            format!("{:?}", o.trigger).to_lowercase(),
        ));
    }

    o.env_id = match env_id {
        Some(id) => id,
        None => default_env(root)?,
    };
    Ok(o)
}

/// The environment to use when none is named.
///
/// Refuses when there is more than one rather than picking: an ambient default
/// that silently selects hardware is exactly the class of thing this harness
/// exists to remove.
fn default_env(root: &Path) -> Result<String, String> {
    let dir = root.join("environments");
    let mut ids: Vec<String> = std::fs::read_dir(&dir)
        .map_err(|e| format!("read environments: {e}"))?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "toml"))
        .filter_map(|p| p.file_stem().and_then(|s| s.to_str()).map(str::to_owned))
        .collect();
    ids.sort();
    match ids.len() {
        1 => Ok(ids.remove(0)),
        0 => Err("no environment profiles in environments/".to_owned()),
        _ => Err(format!(
            "several environments exist ({}); name one with --env. Guessing which \
             hardware a number describes is not something this tool does.",
            ids.join(", ")
        )),
    }
}

fn cmd_prefill(root: &Path, args: &[String]) -> Result<(), String> {
    driver::prefill(root, &opts_from(args, root)?)
}

/// The flags `bench ceiling` owns, and whatever is left for [`opts_from`].
///
/// Parsed separately rather than added to `RunOptions` because these describe a
/// measurement rig and not a sweep, and because `opts_from` rejects a flag it
/// does not recognise — a strictness worth keeping, so the ceiling's own flags
/// are removed before it ever sees them.
#[derive(Debug)]
struct CeilingArgs {
    measure: bool,
    write: bool,
    seconds: u64,
    /// Consumers for the CONSUME pass, and for nothing else.
    ///
    /// It drove the ingest pass too until that pass was found to have been
    /// measured at eight-way concurrency because the topic had eight
    /// partitions — a bound that belongs to the broker's fetch path and to
    /// nothing on the ClickHouse side. The ingest pass now sweeps its own
    /// concurrency and this flag cannot reach it.
    threads: u64,
    /// How far the ingest sweep may climb before it gives up and refuses.
    ingest_max: u64,
    /// Which insert formats the ingest pass measures, as
    /// the operator typed them. Empty means all of them.
    ///
    /// Kept as raw strings here and resolved by [`ceiling::select_combinations`],
    /// so that the names this rig accepts are defined once, beside the encoders
    /// that produce them.
    only: Vec<String>,
    rest: Vec<String>,
}

impl CeilingArgs {
    fn parse(args: &[String]) -> Result<Self, String> {
        // Eight seconds and four threads are the shape the rig this was ported
        // from documented (`DURATION_S=8 MODE=split INSTANCES=1 THREADS=4`), and
        // the duration is short for a reason the rig also documented: the
        // backlog has to outlast the pass or the figure is the rate of a broker
        // that ran out of work.
        //
        // The ingest bound is the harness's own, not the operator's, and it is
        // read from the module that implements the sweep rather than spelled
        // again here.
        let mut out = Self {
            measure: false,
            write: false,
            seconds: 8,
            threads: 4,
            ingest_max: ceiling::INGEST_CONCURRENCY_MAX,
            only: Vec::new(),
            rest: Vec::new(),
        };
        let mut i = 0;
        while i < args.len() {
            let a = args[i].clone();
            match a.as_str() {
                "--measure" => out.measure = true,
                "--write" => out.write = true,
                "--only" => {
                    i += 1;
                    out.only
                        .push(args.get(i).ok_or("--only needs a value")?.clone());
                }
                "--seconds" | "--threads" | "--ingest-max" => {
                    i += 1;
                    let raw = args.get(i).ok_or_else(|| format!("{a} needs a value"))?;
                    let n: u64 = raw.parse().map_err(|e| format!("{a}: {e}"))?;
                    match a.as_str() {
                        "--seconds" => out.seconds = n,
                        "--threads" => out.threads = n,
                        _ => out.ingest_max = n,
                    }
                }
                // Everything else is `opts_from`'s, including its own strictness
                // about flags neither of us recognises.
                _ => out.rest.push(a),
            }
            i += 1;
        }
        if out.write && !out.measure {
            return Err(
                "--write has nothing to write without --measure. It commits a freshly \
                 measured ceiling; it does not bless a stale one."
                    .to_owned(),
            );
        }
        Ok(out)
    }
}

fn cmd_ceiling(root: &Path, args: &[String]) -> Result<(), String> {
    let ceiling_args = CeilingArgs::parse(args)?;
    let opts = opts_from(&ceiling_args.rest, root)?;
    let env = Environment::load(&root.join("environments"), &opts.env_id)?;

    // Resolved before anything is brought up, so a typo in `--only` costs a line
    // of output rather than a container start and a fifteen-minute pass.
    let combinations = ceiling::select_combinations(&ceiling_args.only)?;

    if !ceiling_args.measure {
        if !ceiling_args.only.is_empty() {
            return Err(
                "--only restricts what a MEASUREMENT pass measures; without --measure this \
                 command reports the committed ceilings, and it reports all of them."
                    .to_owned(),
            );
        }
        return report_ceiling(&env);
    }

    // The corpus lives inside the broker. Recreating the infrastructure destroys
    // it, and a ceiling pass is precisely the command most likely to be run
    // reflexively with a "start clean" flag.
    if opts.fresh_infra {
        return Err(
            "REFUSED: --fresh-infra recreates the broker, and the prefilled corpus lives \
             inside it. The consume ceiling is measured against the corpus's own messages, \
             so destroying it would leave nothing to measure — and would cost every arm \
             afterwards its input as well."
                .to_owned(),
        );
    }

    // The same lock a sweep takes. A ceiling pass truncates the arms' target
    // tables and reads the broker flat out; taken against a running arm it would
    // destroy that arm's measurement rather than merely slow it.
    let _lock = ArmLock::acquire("bench ceiling --measure").map_err(|e| {
        format!(
            "a run is in progress, and a ceiling pass truncates the target tables it is \
             writing into. Wait for it to finish. {e}"
        )
    })?;

    let (ep, _infra, _flags) = infra::bring_up(&env, true)?;
    for stmt in corpus::ddl_statements() {
        docker::clickhouse_sql(&ep.ch_host, ep.ch_port, &ep.ch_user, &ep.ch_password, &stmt)
            .map_err(|e| format!("DDL failed: {e}"))?;
    }

    // Read before the list is handed to the pass, because the refusal at the end
    // of this function needs to say how much of the file this pass covered and by
    // then the list belongs to `PassOptions`.
    let measured = if ceiling_args.only.is_empty() {
        "all".to_owned()
    } else {
        combinations.len().to_string()
    };

    let pass = ceiling::measure(
        &env,
        &ep,
        &PassOptions {
            topic: opts.topic.clone(),
            seconds: ceiling_args.seconds,
            consume_threads: ceiling_args.threads,
            ingest_max_concurrency: ceiling_args.ingest_max,
            ingest: combinations,
            date: iso_day(now_ms()),
        },
    )?;

    // A first bootstrap has no committed ceilings file to merge into —
    // creating it is the point of the pass. Only the missing-file case starts
    // empty: a file that exists but does not parse is still an error, because
    // masking corruption here would let the merge silently discard measured
    // ceilings.
    let mut ceilings = if env.ceilings_path().exists() {
        env.ceilings()?
    } else {
        ceiling::Ceilings::default()
    };
    ceilings.merge(pass);
    let gate = ceilings.gate(ceiling::corpus_message_bytes(), &env.infra_digest());
    println!(
        "\n{}: measured\n{}",
        env.spec.id,
        ceiling::describe(&ceilings, &gate)
    );

    if !ceiling_args.write {
        println!(
            "Nothing was written. Pass --write to merge this into {}.\n\
             \n\
             The overwrite is opt-in rather than automatic because this file is \
             provenance for every published record — each one carries the ceiling it \
             was gated against — and because a pass taken while the host was busy \
             produces a low figure that would silently tighten the gate on every arm \
             measured afterwards. Look at the numbers before you commit them.",
            env.ceilings_path().display()
        );
        return Ok(());
    }

    // A committed ceilings file is provenance for every published record, and one
    // whose entries were measured under two different infrastructure envelopes
    // cannot describe the environment it names. That state became reachable when
    // `--only` did: a restricted pass under new caps re-measures what it was asked
    // for and leaves everything else describing the caps that were in force
    // before. The gate would drop those entries later, loudly and per arm — but
    // later is at the end of a sweep, and here the remedy is one flag away.
    let stale = ceilings.measured_under_other_envelopes(&env.infra_digest());
    if !stale.is_empty() {
        return Err(format!(
            "REFUSED: this pass measured {} of the {} ceilings this rig can measure, so \
             writing it would leave {} still describing another infrastructure envelope: \
             {}. This environment is {}. A ceilings file is provenance for every published \
             record, and one that half-describes two envelopes cannot describe the \
             environment it names. Re-run without --only to measure the whole file.",
            measured,
            ceiling::all_combinations().len(),
            stale.len(),
            stale.join("; "),
            env.infra_digest(),
        ));
    }

    ceilings.save(&env.ceilings_path())?;
    println!(
        "wrote {}. Commit it in the same change as anything it re-gates.",
        env.ceilings_path().display()
    );
    Ok(())
}

/// Reports the committed ceilings, and refuses when none of them may be gated
/// against this corpus.
///
/// A refusal rather than a warning, and the exit code says so. This subcommand's
/// job is to *prove* the infrastructure is not the bottleneck; when the stored
/// figures cannot support that claim, succeeding quietly would be the same
/// failure the previous version of this command had — it printed a stored
/// constant and a paragraph admitting it had measured nothing, and exited zero.
fn report_ceiling(env: &Environment) -> Result<(), String> {
    let ceilings = env.ceilings()?;
    let gate = env.ceiling()?;
    println!("{}\n{}", env.spec.id, ceiling::describe(&ceilings, &gate));
    if gate.refusals().is_empty() {
        return Ok(());
    }
    Err(format!(
        "REFUSED: {} has no ceiling that may be gated against, so no arm measured here \
         can be shown to be engine-bound rather than infra-bound. Bring the \
         infrastructure up, run `bench prefill`, then `bench ceiling --measure`.",
        env.spec.id
    ))
}

fn cmd_run(root: &Path, args: &[String]) -> Result<(), String> {
    let entrants = load_entrants(root)?;
    let selectors = parse_selectors(args)?;
    let arms = select::expand(&entrants, &selectors)?;
    let opts = opts_from(args, root)?;
    driver::run(root, &arms, &opts)
}

/// The repository root, from this binary's compile-time location.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("harness/ has a parent")
        .to_path_buf()
}

fn load_entrants(root: &Path) -> Result<Vec<Entrant>, String> {
    entrant::load_all(&root.join("entrants")).map_err(|errs| {
        format!(
            "{} descriptor problem(s):\n  - {}",
            errs.len(),
            errs.join("\n  - ")
        )
    })
}

fn cmd_validate(root: &Path) -> Result<(), String> {
    let entrants = load_entrants(root)?;
    println!("entrants: {} descriptor(s) valid", entrants.len());

    // Every environment must load, since a record naming one that does not parse
    // could never be rendered. So must its ceilings file: a ceiling that cannot
    // be read is a gate that cannot run.
    let env_dir = root.join("environments");
    let mut envs = 0usize;
    let mut gateable = 0usize;
    let mut stale: Vec<(String, Vec<String>)> = Vec::new();
    for e in std::fs::read_dir(&env_dir).map_err(|e| format!("read environments: {e}"))? {
        let path = e.map_err(|e| e.to_string())?.path();
        if path.extension().is_some_and(|x| x == "toml") {
            let id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or("bad environment filename")?;
            let env = Environment::load(&env_dir, id)?;
            let gate = env.ceiling()?;
            if gate.refusals().is_empty() {
                gateable += 1;
            } else {
                stale.push((id.to_owned(), gate.refusals().to_vec()));
            }
            envs += 1;
        }
    }
    println!(
        "environments: {envs} profile(s) valid, {gateable} with a ceiling that may be \
         gated against"
    );

    // Reported, not failed, and the distinction is deliberate. A malformed
    // ceilings file is a defect in a committed file and CI should stop for it —
    // it does, above, because the load returns Err. A ceiling that is well
    // formed but no longer describes this corpus is not a file to fix; it is a
    // measurement to re-take on hardware CI does not have. Failing here would
    // make the cheapest way to green the build an edit to the number, which is
    // exactly how the stale figure survived as long as it did. It is enforced
    // where it bites instead: `bench ceiling` refuses, and `bench run` gates
    // nothing and flags every record it produces.
    for (id, why) in &stale {
        println!("  {id}: NOT GATEABLE — records produced here carry no proven headroom");
        for w in why {
            println!("    - {w}");
        }
    }

    // `results_are_valid` rather than a parse and a count, because three other
    // files in this repository already tell their readers this command enforces
    // its rules: `report.rs` on metric units and on provenance, `.gitattributes`
    // on `run_id` uniqueness under `merge=union`. It reports every problem it
    // finds, so an archive is repaired by reading the list once.
    let summary = validate::results_are_valid(&root.join("results")).map_err(|problems| {
        format!(
            "{} result problem(s):\n  - {}",
            problems.len(),
            problems.join("\n  - ")
        )
    })?;
    println!(
        "results: {} record(s) in {} file(s) valid",
        summary.records, summary.files
    );
    println!("harness v{HARNESS_VERSION}, dataset {DATASET_VERSION}");
    Ok(())
}

fn cmd_list(root: &Path, args: &[String]) -> Result<(), String> {
    let entrants = load_entrants(root)?;
    let (records, _) = results::load_all(&root.join("results")).unwrap_or_default();

    if args.iter().any(|a| a == "--json") {
        let rows: Vec<serde_json::Value> = entrants
            .iter()
            .map(|e| {
                serde_json::json!({
                    "id": e.id(),
                    "name": e.spec.entrant.name,
                    "status": format!("{:?}", e.spec.entrant.status).to_lowercase(),
                    "runtime": e.spec.entrant.runtime,
                    "licence": e.spec.entrant.licence,
                    "vendor": e.spec.entrant.vendor,
                    "variants": e.spec.variants.iter().map(|v| &v.id).collect::<Vec<_>>(),
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&rows).map_err(|e| e.to_string())?
        );
        return Ok(());
    }

    for e in &entrants {
        let status = format!("{:?}", e.spec.entrant.status).to_lowercase();
        let ours = if e.spec.entrant.vendor == "self" {
            "  [vendor-run]"
        } else {
            ""
        };
        println!(
            "{:<24} {:<11} {:<7} {}{ours}",
            e.id(),
            status,
            e.spec.entrant.runtime,
            e.spec.entrant.name
        );

        if e.spec.entrant.status != Status::Planned {
            for v in &e.spec.variants {
                let last = records
                    .iter()
                    .filter(|r| r.sut.entrant == *e.id() && r.sut.variant_id == v.id)
                    .map(|r| r.run.ts_ms)
                    .max();
                let when = last.map_or_else(
                    || "never measured".to_owned(),
                    |ts| format!("last {}", iso_day(ts)),
                );
                let approach = format!("{:?}", v.approach).to_lowercase();
                let default = if v.default { " (default)" } else { "" };
                println!("    {:<28} {approach:<10} {when}{default}", v.id);
            }
        } else if let Some(p) = &e.spec.planned {
            let first = p.blockers.trim().lines().next().unwrap_or("");
            println!("    blocked: {first}");
        }
    }
    Ok(())
}

fn cmd_build(root: &Path, args: &[String]) -> Result<(), String> {
    let entrants = load_entrants(root)?;
    let selectors = parse_selectors(args)?;
    let arms = select::expand(&entrants, &selectors)?;

    // One image per entrant, not per arm: variants differ by environment, not by
    // build. Building the same image once per variant would multiply a slow
    // docker build by the variant count for no difference in the result.
    let mut seen = std::collections::BTreeSet::new();
    for arm in &arms {
        if !seen.insert(arm.entrant.id()) {
            continue;
        }
        let e = arm.entrant;
        let build = e
            .spec
            .build
            .as_ref()
            .ok_or_else(|| format!("{}: no [build] section", e.id()))?;

        // Canonicalised before comparing: `[build].context` is relative to the
        // entrant directory and is typically `../..`, which does not strip as a
        // textual prefix even though it is genuinely an ancestor.
        let context = e
            .dir
            .join(&build.context)
            .canonicalize()
            .map_err(|err| format!("{}: build context: {err}", e.id()))?;
        let dockerfile = e
            .dir
            .join(&build.dockerfile)
            .canonicalize()
            .map_err(|err| format!("{}: dockerfile: {err}", e.id()))?;
        let dockerfile_rel = dockerfile
            .strip_prefix(&context)
            .map_err(|_| format!("{}: dockerfile is outside the build context", e.id()))?;

        let mut argv: Vec<String> = vec![
            "build".into(),
            "-f".into(),
            dockerfile_rel.display().to_string(),
            "-t".into(),
            build.image.clone(),
        ];
        // No build secrets are defined: every entrant, including Spate, builds
        // from public sources. The schema keeps the field so a descriptor that
        // declares one fails here with a real explanation rather than a parse
        // error.
        if let Some(s) = build.secrets.first() {
            return Err(format!(
                "{}: declares build secret {s:?}, but no build secrets are \
                 defined — every entrant builds from public sources",
                e.id()
            ));
        }
        argv.push(".".into());

        println!("building {} -> {}", e.id(), build.image);
        let mut cmd = std::process::Command::new("docker");
        cmd.args(&argv).current_dir(&context);
        let status = cmd.status().map_err(|err| format!("docker: {err}"))?;
        if !status.success() {
            return Err(format!("{}: docker build failed", e.id()));
        }
    }
    Ok(())
}

fn cmd_stale(root: &Path) -> Result<(), String> {
    let entrants = load_entrants(root)?;
    let (records, _) = results::load_all(&root.join("results")).unwrap_or_default();

    let mut any = false;
    for e in entrants
        .iter()
        .filter(|e| e.spec.entrant.status.is_runnable())
    {
        for v in &e.spec.variants {
            let mine: Vec<_> = records
                .iter()
                .filter(|r| r.sut.entrant == *e.id() && r.sut.variant_id == v.id)
                .collect();
            let latest = mine.iter().copied().max_by_key(|r| r.run.ts_ms);
            match latest {
                None => {
                    any = true;
                    println!("{}:{} — never measured", e.id(), v.id);
                }
                // A record produced under a superseded protocol is stale in the
                // way that matters most: it cannot be drawn on the same axis as
                // anything current, so it is invisible rather than merely old.
                Some(r) if r.run.harness_version != HARNESS_VERSION => {
                    any = true;
                    println!(
                        "{}:{} — harness v{} (current v{HARNESS_VERSION}); not comparable",
                        e.id(),
                        v.id,
                        r.run.harness_version
                    );
                }
                Some(r) if r.run.dataset_version != DATASET_VERSION => {
                    any = true;
                    println!(
                        "{}:{} — dataset {} (current {DATASET_VERSION}); not comparable",
                        e.id(),
                        v.id,
                        r.run.dataset_version
                    );
                }
                Some(_) => {}
            }
        }
    }
    if !any {
        println!("every runnable arm has a current measurement");
    }
    Ok(())
}

/// Flags that consume the following argument.
///
/// Needed because a selector is a positional argument, so the parser has to know
/// which non-flag words are already spoken for. Without this, `--reps 1` offers
/// `1` as a selector and the run fails with `no entrant "1"` — which is at least
/// loud, but the same slip on `--topic x` would silently add an entrant-shaped
/// word to the plan.
const VALUED_FLAGS: [&str; 9] = [
    "--reps",
    "--env",
    "--topic",
    "--batches",
    "--trigger",
    "--knob",
    "--mode",
    "--rate",
    "--window",
];

fn parse_selectors(args: &[String]) -> Result<Vec<Selector>, String> {
    let mut raw: Vec<&String> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if VALUED_FLAGS.contains(&a.as_str()) {
            i += 2;
            continue;
        }
        if !a.starts_with('-') {
            raw.push(a);
        }
        i += 1;
    }
    if raw.is_empty() {
        return Err("no selector given. Use '*' for everything.".to_owned());
    }
    raw.iter().map(|s| Selector::parse(s)).collect()
}

/// `YYYY-MM-DD` from epoch milliseconds.
fn iso_day(ts_ms: u64) -> String {
    let days = (ts_ms / 86_400_000) as i64;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}
