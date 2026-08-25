//! Every committed descriptor parses and satisfies the contract.
//!
//! This is the gate that keeps the entrant contract from becoming decoration.
//! Without it a descriptor could declare a 4 CPU envelope while the driver
//! started containers totalling five, or claim a wire format no arm writes, and
//! nothing would notice until a reader did.
//!
//! The last two tests check things that live *outside* the descriptor but which
//! it asserts: the Flink JVM's own sizing against its declared container, and
//! the Rust toolchain pin against the arm's image. Both are cases where two
//! files have to agree and neither is obviously the source of truth — exactly
//! the shape that drifts silently.

use std::path::{Path, PathBuf};

use spate_benchmark_harness::entrant::{self, Approach, Role, Status};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("harness/ has a parent")
        .to_path_buf()
}

fn entrants_dir() -> PathBuf {
    repo_root().join("entrants")
}

#[test]
fn every_descriptor_parses_and_validates() {
    match entrant::load_all(&entrants_dir()) {
        Ok(entrants) => {
            assert!(!entrants.is_empty(), "no entrants found");
        }
        Err(errors) => panic!(
            "{} descriptor problem(s):\n  - {}",
            errors.len(),
            errors.join("\n  - ")
        ),
    }
}

#[test]
fn the_vendor_entry_is_unique_and_present() {
    // Exactly one entrant may claim `vendor = "self"`. The site renders its
    // conflict-of-interest disclosure from that field, so two would be
    // meaningless and zero would silently drop the disclosure entirely.
    let entrants = entrant::load_all(&entrants_dir()).expect("descriptors valid");
    let ours: Vec<&str> = entrants
        .iter()
        .filter(|e| e.spec.entrant.vendor == "self")
        .map(entrant::Entrant::id)
        .collect();
    assert_eq!(ours, ["spate"], "expected exactly one vendor-run entrant");
}

#[test]
fn every_active_entrant_has_a_realistic_default() {
    let entrants = entrant::load_all(&entrants_dir()).expect("descriptors valid");
    for e in entrants
        .iter()
        .filter(|e| e.spec.entrant.status.is_runnable())
    {
        let d = e.default_variant().expect("a default variant");
        assert_eq!(
            d.approach,
            Approach::Realistic,
            "{}: default variant {} must be realistic",
            e.id(),
            d.id
        );
    }
}

#[test]
fn planned_entrants_explain_themselves() {
    // A roadmap that says only "later" is a promise, not a plan. Anything not yet
    // measured has to say what is blocking it, so the gap is legible to a reader
    // deciding whether the omission is convenient for us.
    let entrants = entrant::load_all(&entrants_dir()).expect("descriptors valid");
    let planned: Vec<_> = entrants
        .iter()
        .filter(|e| e.spec.entrant.status == Status::Planned)
        .collect();
    assert!(!planned.is_empty(), "expected the roadmap to be non-empty");
    for e in planned {
        let p = e.spec.planned.as_ref().expect("[planned] present");
        assert!(
            p.blockers.trim().len() > 40,
            "{}: [planned].blockers is too thin to be informative",
            e.id()
        );
    }
}

#[test]
fn flink_jvm_sizing_fits_its_declared_container() {
    // Defect this exists to prevent, found in the extracted harness: config.yaml
    // sized the TaskManager JVM for a 3 GiB container while the driver started it
    // with 4 GiB, leaving ~1.1 GiB of Flink's own allowance unused and
    // undisclosed. That handicapped a competitor in a comparison we publish,
    // which is the direction of error this benchmark can least afford.
    let entrants = entrant::load_all(&entrants_dir()).expect("descriptors valid");
    let flink = entrants
        .iter()
        .find(|e| e.id() == "flink")
        .expect("flink entrant");

    let config = std::fs::read_to_string(flink.dir.join("config.yaml")).expect("read config.yaml");
    let sizes = process_sizes(&config);
    assert_eq!(
        sizes.len(),
        2,
        "expected a JobManager and a TaskManager size"
    );

    let envelope = flink.spec.envelope.as_ref().expect("envelope");
    for container in &envelope.containers {
        let limit = mib(&container.memory).expect("container memory parses");
        // Which JVM belongs to which container is decided by ordering in the
        // file: jobmanager first, then taskmanager, matching Flink's own layout.
        let jvm = match container.role {
            Role::ControlPlane => sizes[0],
            Role::DataPlane => sizes[1],
        };
        // The JVM's floor is the smaller of the container less limit/8 slack
        // (the JVM's accounting does not cover everything in the container)
        // and the 24 GiB-era process size, which measured faster on the 96 GiB
        // envelope than every larger heap tried — GC churn grows with the
        // heap while the live set does not. The ceiling is the process size
        // whose derived heap sits at the compressed-oops boundary: past ~32g
        // every reference doubles. Below the floor the arm is denied memory
        // it was allocated; above the ceiling it is denied a configuration
        // anyone would deploy.
        const ERA_PROCESS_MIB: u64 = 21_504;
        const OOPS_BOUNDARY_PROCESS_MIB: u64 = 34_816;
        let floor = (limit - limit / 8).min(ERA_PROCESS_MIB);
        assert!(
            jvm >= floor,
            "flink {}: JVM process.size {jvm}m is under the {floor}m its \
             {limit}m container affords; Flink is being handicapped",
            container.name
        );
        assert!(
            jvm <= limit.min(OOPS_BOUNDARY_PROCESS_MIB),
            "flink {}: JVM process.size {jvm}m exceeds its container's {limit}m \
             or the compressed-oops boundary",
            container.name
        );
    }
}

#[test]
fn kafka_connect_jvm_sizing_fits_its_declared_container() {
    // The same drift `flink_jvm_sizing_fits_its_declared_container` exists to
    // prevent, for the arm whose JVM is sized in its Dockerfile rather than in
    // a config file: KAFKA_HEAP_OPTS is what actually sizes the Connect
    // worker's JVM, and the descriptor's container memory is what the driver
    // caps the cgroup at. Neither is obviously the source of truth. A heap
    // sized for a smaller container silently handicaps a competitor; one sized
    // for a larger container is an OOM-kill mid-drain.
    //
    // The JVM's committed footprint is bounded by heap + direct memory +
    // metaspace (thread stacks and JIT code cache live in the slack, which is
    // also why slack must exist at all).
    let entrants = entrant::load_all(&entrants_dir()).expect("descriptors valid");
    let kc = entrants
        .iter()
        .find(|e| e.id() == "kafka-connect")
        .expect("kafka-connect entrant");

    let dockerfile = std::fs::read_to_string(kc.dir.join("Dockerfile")).expect("read Dockerfile");
    let heap_opts = dockerfile
        .lines()
        .find_map(|l| l.trim().split_once("KAFKA_HEAP_OPTS=\"").map(|(_, v)| v))
        .and_then(|v| v.split('"').next())
        .expect("the Dockerfile sets KAFKA_HEAP_OPTS on one line");

    let flag = |prefix: &str| -> u64 {
        heap_opts
            .split_whitespace()
            .find_map(|t| t.strip_prefix(prefix))
            .and_then(mib)
            .unwrap_or_else(|| panic!("KAFKA_HEAP_OPTS carries no parseable {prefix}"))
    };
    let jvm = flag("-Xmx") + flag("-XX:MaxDirectMemorySize=") + flag("-XX:MaxMetaspaceSize=");

    let worker = kc.data_plane().expect("a data-plane container");
    let limit = mib(&worker.memory).expect("container memory parses");
    // The same budgets as the Flink check: the 24 GiB-era total as the floor,
    // and 35072m — a 31744m heap at the compressed-oops boundary plus direct
    // memory and metaspace — as the ceiling.
    const ERA_TOTAL_MIB: u64 = 21_504;
    const OOPS_BOUNDARY_TOTAL_MIB: u64 = 35_072;
    let floor = (limit - limit / 8).min(ERA_TOTAL_MIB);
    assert!(
        jvm >= floor,
        "kafka-connect: heap+direct+metaspace {jvm}m is under the {floor}m its \
         {limit}m container affords; Connect is being handicapped"
    );
    assert!(
        jvm <= limit.min(OOPS_BOUNDARY_TOTAL_MIB),
        "kafka-connect: heap+direct+metaspace {jvm}m exceeds its container's \
         {limit}m or the compressed-oops boundary"
    );
    let heap = flag("-Xmx");
    assert!(
        heap <= 31_744,
        "kafka-connect: -Xmx{heap}m is past the compressed-oops boundary; every \
         reference doubles there and the configuration is slower than a smaller \
         heap"
    );
}

#[test]
fn a_jvm_containers_declared_gc_log_is_where_its_configuration_sends_it() {
    // The descriptor's `gc_log` is what the harness copies out after a run; the
    // arm's own configuration is what decides where the JVM writes. Neither
    // file is obviously the source of truth, which is the shape that drifts
    // silently — and a drift here does not fail anything, it just records no GC
    // figures (or another JVM's) for an arm that produced them.
    let entrants = entrant::load_all(&entrants_dir()).expect("descriptors valid");
    let mut seen_a_declaration = false;
    for e in entrants.iter().filter(|e| {
        e.spec.entrant.runtime == "jvm" && e.spec.entrant.status == entrant::Status::Active
    }) {
        let envelope = e
            .spec
            .envelope
            .as_ref()
            .expect("active JVM arm has envelope");
        // Every JVM container declares one, and no two containers of one arm
        // share a path — a shared path would read one JVM's pauses as the
        // other's.
        let mut paths = std::collections::BTreeSet::new();
        for c in &envelope.containers {
            let gc_log = c.gc_log.as_deref().unwrap_or_else(|| {
                panic!(
                    "{}: container {:?} declares no gc_log; a JVM arm without one \
                     publishes no GC figures at all",
                    e.id(),
                    c.name
                )
            });
            assert!(
                paths.insert(gc_log),
                "{}: two containers declare gc_log {gc_log:?}; the JVMs write \
                 separate logs or one arm's pauses are the other's",
                e.id()
            );
            seen_a_declaration = true;
            // The path must appear in the entrant's own configuration — the
            // file that actually aims the JVM's -Xlog — somewhere under its
            // directory. Searched rather than parsed, because each runtime
            // spells its options differently (Flink's config.yaml, Connect's
            // Dockerfile KAFKA_OPTS) and a parser per runtime would be the
            // per-entrant harness knowledge this field exists to remove.
            // `configuration_files` deliberately skips `entrant.toml`: the
            // descriptor contains the declared string by definition, so
            // including it would satisfy this check with the declaration
            // itself and no drift could ever fail here.
            let mentioned = configuration_files(&e.dir)
                .iter()
                .any(|text| text.contains(gc_log));
            assert!(
                mentioned,
                "{}: gc_log {gc_log:?} appears in no configuration file under {}; \
                 the descriptor names a path nothing writes to",
                e.id(),
                e.dir.display()
            );
        }
    }
    assert!(
        seen_a_declaration,
        "no active JVM arm declared a gc_log; this test is checking nothing"
    );
}

/// Every plausibly-configuration file directly under an entrant's directory,
/// read as text. Flat rather than recursive: the files that aim a JVM's flags
/// live at the top of the entrant, and a recursive walk would read source trees.
///
/// `entrant.toml` is excluded, and the exclusion is load-bearing: the declared
/// `gc_log` path is a string in the descriptor, so a scan that includes the
/// descriptor finds the declaration in itself and the containment check above
/// passes vacuously — rename the path in `config.yaml` without touching the
/// descriptor and nothing fails, which is precisely the drift the check exists
/// to catch. The declaration cannot be its own corroboration; what the test
/// needs is a file the *container actually reads* sending GC logging to the
/// declared path.
fn configuration_files(dir: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for entry in rd.filter_map(Result::ok) {
            let path = entry.path();
            if path.file_name().is_some_and(|n| n == "entrant.toml") {
                continue;
            }
            if path.is_file()
                && let Ok(text) = std::fs::read_to_string(&path)
            {
                out.push(text);
            }
        }
    }
    out
}

#[test]
fn the_flink_images_parallelism_matches_the_number_it_asserts_about_itself() {
    // Two files inside one image have to agree, and neither is obviously the
    // source of truth — the shape that drifts silently.
    //
    // `ComparisonJob` refuses to submit unless the parallelism the cluster
    // resolved equals `EXPECT_PARALLELISM`. That check is what stops a
    // parallelism sweep recording values it never ran at, and it only works if
    // the image's own default asserts the truth about itself: a container run by
    // hand gets `config.yaml`'s `parallelism.default` and the Dockerfile's
    // `EXPECT_PARALLELISM`, with no driver to set either. If those two disagree,
    // the image refuses to start every job, and the first person to meet it will
    // be told that FLINK_PROPERTIES is broken when it is not.
    //
    // The descriptor's `parallelism` knob is deliberately NOT tied to these. It
    // is what the driver applies, and requiring it to match the image would mean
    // an image rebuild every time the published configuration changed — which is
    // exactly the coupling making the knob reachable was for.
    let entrants = entrant::load_all(&entrants_dir()).expect("descriptors valid");
    let flink = entrants
        .iter()
        .find(|e| e.id() == "flink")
        .expect("flink entrant");

    let config = std::fs::read_to_string(flink.dir.join("config.yaml")).expect("read config.yaml");
    let mut lines = config.lines().skip_while(|l| l.trim() != "parallelism:");
    lines.next().expect("config.yaml declares parallelism");
    let default: u32 = lines
        .find_map(|l| l.trim().strip_prefix("default:"))
        .and_then(|v| v.trim().parse().ok())
        .expect("config.yaml declares parallelism.default as an integer");

    let dockerfile =
        std::fs::read_to_string(flink.dir.join("Dockerfile")).expect("read Dockerfile");
    let expected: u32 = dockerfile
        .lines()
        .find_map(|l| l.trim().strip_prefix("EXPECT_PARALLELISM="))
        .and_then(|v| v.trim_end_matches(" \\").trim().parse().ok())
        .expect("the Dockerfile sets EXPECT_PARALLELISM to an integer");

    assert_eq!(
        default, expected,
        "entrants/flink/config.yaml sets parallelism.default={default} but the \
         Dockerfile sets EXPECT_PARALLELISM={expected}. ComparisonJob compares the \
         two at job submission, so a hand-run container would refuse every job and \
         report it as a configuration-override failure that has not happened."
    );
}

#[test]
fn the_rust_toolchain_pin_matches_the_arm_image() {
    // Two files have to agree and neither is obviously authoritative: the
    // toolchain that runs the host gates, and the one inside the image that
    // actually builds the measured binary. Codegen moves throughput, so a silent
    // divergence would make the recorded toolchain wrong.
    let root = repo_root();
    let pin =
        std::fs::read_to_string(root.join("rust-toolchain.toml")).expect("rust-toolchain.toml");
    let channel = pin
        .lines()
        .find_map(|l| l.trim().strip_prefix("channel = "))
        .map(|v| v.trim().trim_matches('"').to_owned())
        .expect("channel in rust-toolchain.toml");

    let dockerfile =
        std::fs::read_to_string(root.join("entrants/spate/Dockerfile")).expect("arm Dockerfile");
    let from = dockerfile
        .lines()
        .find_map(|l| l.trim().strip_prefix("FROM rust:"))
        .map(|v| v.split('-').next().unwrap_or_default().to_owned())
        .expect("FROM rust:<version> in the arm Dockerfile");

    assert_eq!(
        channel, from,
        "rust-toolchain.toml pins {channel} but entrants/spate/Dockerfile builds on {from}"
    );
}

#[test]
fn the_kafka_engine_arms_forward_target_matches_the_infra_it_names() {
    // Two files have to agree and neither can import the other: the shared
    // ClickHouse's coordinates are constants in `harness/src/infra.rs`, and the
    // Kafka engine arm repeats them as literals in its cluster XML — an arm
    // image cannot read harness code, and the harness does not template entrant
    // config files. Without this edge a renamed infra container or a moved
    // credential leaves the arm's own assert green (20_assert.sh checks the XML
    // against itself) while every forwarded insert resolves to nowhere and each
    // rep burns the full drain limit. Same shape as the toolchain pin above:
    // neither file is obviously authoritative, so the agreement is the check.
    let root = repo_root();
    let xml = std::fs::read_to_string(
        root.join("entrants/clickhouse-kafka-engine/config.d/10-remote-cluster.xml"),
    )
    .expect("10-remote-cluster.xml");
    let tag = |name: &str| -> &str {
        xml.split(&format!("<{name}>"))
            .nth(1)
            .and_then(|rest| rest.split(&format!("</{name}>")).next())
            .unwrap_or_else(|| panic!("<{name}> missing from 10-remote-cluster.xml"))
    };
    let infra = std::fs::read_to_string(root.join("harness/src/infra.rs")).expect("infra.rs");

    let host = tag("host");
    assert!(
        infra.contains(&format!("const CLICKHOUSE: &str = \"{host}\"")),
        "the arm forwards to <host>{host}</host> but harness/src/infra.rs does not name \
         that container; the Distributed table would forward into a resolution error"
    );
    let password = tag("password");
    assert!(
        infra.contains(&format!("CLICKHOUSE_PASSWORD={password}")),
        "the arm authenticates with <password>{password}</password> but \
         harness/src/infra.rs does not start the shared server with it"
    );
    assert!(
        infra.contains(&format!("ch_user: \"{}\"", tag("user"))),
        "the arm connects as <user>{}</user> but harness/src/infra.rs uses a \
         different user for the shared server",
        tag("user")
    );
    // The arm speaks to the container-internal native port; infra publishes it
    // as 19000 on the host. The mapping string is the one place infra spells
    // the internal port, so it is what pins the XML's <port>.
    assert_eq!(
        tag("port"),
        "9000",
        "the cluster XML must target the container-internal native port"
    );
    assert!(
        infra.contains("\"19000:9000\""),
        "harness/src/infra.rs no longer publishes the shared server's native port as \
         19000:9000 — if the internal port moved, the arm's cluster XML must move with it"
    );
}

/// The `size:` values under each `process:` key, in file order, as MiB.
fn process_sizes(config: &str) -> Vec<u64> {
    let mut out = Vec::new();
    let mut lines = config.lines().peekable();
    while let Some(line) = lines.next() {
        if line.trim() != "process:" {
            continue;
        }
        for next in lines.by_ref() {
            let t = next.trim();
            if t.is_empty() || t.starts_with('#') {
                continue;
            }
            if let Some(v) = t.strip_prefix("size:")
                && let Some(m) = mib(v.trim())
            {
                out.push(m);
            }
            break;
        }
    }
    out
}

/// `3900m`, `4g`, `960m` as MiB.
///
/// Delegates rather than parsing, because this used to be the fourth copy of the
/// suffix parser and the one that had diverged furthest: it returned MiB where
/// the others returned bytes, and rejected suffixes the descriptors are allowed
/// to use. A test whose own arithmetic disagrees with the code under test is not
/// a check.
fn mib(s: &str) -> Option<u64> {
    entrant::parse_memory(s).map(|bytes| bytes / (1024 * 1024))
}
