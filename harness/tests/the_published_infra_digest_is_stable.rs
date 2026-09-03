//! `infra_digest` is provenance on every published record and the key every
//! committed ceiling is gated by, so it must not move except on purpose.
//!
//! `methodology/comparability.md` now names it a hard comparability field and
//! says a different broker family splits a group. That claim is only worth
//! making if the digest for the environment we actually publish from is pinned:
//! a refactor that changed its inputs — the order of the format string, the
//! spelling of a broker family — would silently orphan every record in
//! `results/` and drop every ceiling in `environments/ceilings/`.

use std::path::{Path, PathBuf};

use spate_benchmark_harness::environment::Environment;

fn environments_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("harness/ has a parent")
        .join("environments")
}

/// The digest every record under `results/c8gd-metal-24xl-ec2-docker/` carries
/// and every ceiling in that environment's file was measured under.
const PUBLISHED_DIGEST: &str = "6c8fc2dcbfeb";
const PUBLISHED_ENV: &str = "c8gd-metal-24xl-ec2-docker";

#[test]
fn the_reference_environment_keeps_the_digest_its_records_were_written_under() {
    let env = Environment::load(&environments_dir(), PUBLISHED_ENV)
        .unwrap_or_else(|e| panic!("load {PUBLISHED_ENV}: {e}"));

    assert_eq!(
        env.infra_digest(),
        PUBLISHED_DIGEST,
        "infra_digest moved. Every record already committed under \
         results/{PUBLISHED_ENV}/ carries {PUBLISHED_DIGEST}, and every ceiling in \
         environments/ceilings/{PUBLISHED_ENV}.json was measured under it, so a \
         change here splits the published archive from everything measured after \
         it and drops every ceiling. If the infrastructure genuinely changed, that \
         split is correct and this constant moves with it — in a commit that says \
         so. If it did not, this is a refactor that altered the digest's inputs."
    );
}
