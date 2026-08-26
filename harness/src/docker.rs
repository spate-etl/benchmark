//! Primitives over the `docker` CLI: running it, the shared network, and
//! querying ClickHouse.
//!
//! Deliberately does **not** know how to start the infrastructure. That lives in
//! [`crate::infra`], which takes its caps from the environment profile, applies
//! them, and reads them back out of the running containers' cgroups.
//!
//! The framework repository's equivalent of this module *did* own container
//! start-up, driven by environment variables (`CLICKHOUSE_CPUS`, `KAFKA_CPUS`,
//! `FRESH`) and reusing a healthy container so repeated runs stayed cheap. Both
//! choices are right for a development rig and wrong for a published comparison:
//! ambient variables are how three different envelopes came to be stated in
//! three places while no record said which was in force, and silently reusing a
//! warm server of the wrong version is a published-number defect rather than an
//! inconvenience. Two ways to start infrastructure is the bug, so there is one.

use std::process::Command;

/// The docker network every bench container joins, so a containerised client
/// can reach the broker and ClickHouse by container name.
///
/// The cross-framework comparison runs the framework under test in a container
/// (an in-process host run would get all the host's cores and invalidate the
/// resource envelope), and that container cannot use the host-facing
/// `localhost` addresses. See [`ensure_network`].
pub const NETWORK: &str = "spate-bench-net";

/// Run the `docker` CLI, returning trimmed stdout. Panics with the argv and
/// stderr on a non-zero exit: a failed `docker run` (e.g. a rejected `--cpus`)
/// must fail loudly here, not surface later as a misleading 90s ping timeout.
pub fn docker(args: &[&str]) -> String {
    docker_try(args).unwrap_or_else(|stderr| panic!("docker {args:?} failed: {stderr}"))
}

/// Like [`docker`] but returns the trimmed stderr as `Err` on a non-zero exit
/// instead of panicking, for callers that tolerate failure (a `rm -f` of a
/// container that isn't there).
pub fn docker_try(args: &[&str]) -> Result<String, String> {
    let out = Command::new("docker")
        .args(args)
        .output()
        .expect("docker CLI");
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_owned())
    }
}

/// The last `tail` lines a container wrote, on **both** streams.
///
/// `docker logs` replicates each of the container's streams to the matching one
/// of its own, so [`docker_try`], which keeps stdout alone, drops the whole log
/// of a SUT that writes to stderr — Vector does, log4j does not.
///
/// Never returns an empty string: silence and an unreadable log are different
/// facts, and a reader who cannot tell them apart debugs the wrong one.
#[must_use]
pub fn container_logs(name: &str, tail: u32) -> String {
    let out = Command::new("docker")
        .args(["logs", "--tail", &tail.to_string(), name])
        .output()
        .expect("docker CLI");
    merge_log_streams(
        &String::from_utf8_lossy(&out.stdout),
        &String::from_utf8_lossy(&out.stderr),
        out.status.success(),
    )
}

/// Splices what `docker logs` put on each stream into one block.
///
/// Separate from [`container_logs`] so the three outcomes — output, no output,
/// unreadable — are testable without a daemon.
fn merge_log_streams(stdout: &str, stderr: &str, ok: bool) -> String {
    if !ok {
        return format!("<log unreadable: {}>", stderr.trim());
    }
    let joined = format!("{}{}", stdout, stderr);
    let joined = joined.trim();
    if joined.is_empty() {
        "<container wrote nothing on either stream>".to_owned()
    } else {
        joined.to_owned()
    }
}

/// Force-remove any container of this name (running or exited), ignoring a
/// "no such container" miss.
///
/// Called before every `docker run`. A stopped/exited container of the same
/// name — the normal state after an interrupted or crashed run — would make
/// `docker run --name` fail with a name conflict. Starting fresh here (rather
/// than `docker start`-ing a stopped one) gives the new container cold OS and
/// query caches, which the server-CPU measurements in `ch_native_format` rely
/// on. This only applies on the fresh-start path, though: a server still
/// RUNNING from a previous run is reused as-is (warm caches) unless `FRESH=1`
/// forces a remove+recreate.
pub fn ensure_network() {
    if docker_try(&["network", "inspect", NETWORK]).is_err() {
        // A concurrent creator is not an error; re-inspect decides.
        let _ = docker_try(&["network", "create", NETWORK]);
        assert!(
            docker_try(&["network", "inspect", NETWORK]).is_ok(),
            "could not create docker network {NETWORK}"
        );
    }
}

/// Attach a running container to the shared bench network, ignoring the
/// "already exists in network" case.
///
/// Called on containers that may predate the network — the broker and
/// ClickHouse are reused across runs when already healthy, so they cannot be
/// assumed to have been started with `--network`.
pub fn attach_to_network(container: &str) {
    ensure_network();
    if let Err(stderr) = docker_try(&["network", "connect", NETWORK, container]) {
        assert!(
            stderr.contains("already exists") || stderr.contains("already in network"),
            "could not attach {container} to {NETWORK}: {stderr}"
        );
    }
}

/// Runs one statement against ClickHouse over HTTP, returning its body.
///
/// The fallible counterpart to [`clickhouse_sql`], which asserts on a
/// `DB::Exception`. Callers that run a query whose failure is *survivable* — a
/// disabled system table, a server-side figure that is nice to have — must use
/// this one: a panic here takes a thirty-hour sweep down over a measurement the
/// run could have proceeded without.
///
/// # Errors
///
/// If the request cannot be made, or the server answers with an exception.
pub fn try_clickhouse_sql(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    sql: &str,
) -> std::io::Result<String> {
    let body = crate::http::post(
        host,
        port,
        &format!("/?user={user}&password={password}"),
        sql,
    )?;
    if std::env::var("BENCH_SQL_DEBUG").is_ok() {
        eprintln!("SQL {sql:?} @ {host}:{port} -> {body:?}");
    }
    Ok(body)
}

/// Like [`clickhouse_sql`] but with an explicit read timeout, for statements
/// whose result is legitimately slow to produce.
///
/// # Errors
///
/// If the request cannot be made, or the server answers with an exception.
pub fn clickhouse_sql_slow(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    sql: &str,
    read_timeout: std::time::Duration,
) -> std::io::Result<String> {
    let body = crate::http::post_slow(
        host,
        port,
        &format!("/?user={user}&password={password}"),
        sql,
        read_timeout,
    )?;
    assert!(
        !body.contains("DB::Exception"),
        "clickhouse error for {sql:?}: {body}"
    );
    Ok(body)
}

/// Run one SQL statement, panicking on a server exception so a misconfigured
/// bench fails loudly instead of producing a zero-row "result".
pub fn clickhouse_sql(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    sql: &str,
) -> std::io::Result<String> {
    let body = try_clickhouse_sql(host, port, user, password, sql)?;
    assert!(
        !body.contains("DB::Exception"),
        "clickhouse error for {sql:?}: {body}"
    );
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A container whose whole log is on stderr, which a stdout-only capture
    /// renders as no failure information at all.
    #[test]
    fn stderr_only_output_is_kept() {
        let merged = merge_log_streams("", "ERROR sink failed\n", true);
        assert!(
            merged.contains("ERROR sink failed"),
            "stderr-only logs must survive, got {merged:?}"
        );
    }

    #[test]
    fn both_streams_are_kept() {
        let merged = merge_log_streams("out line\n", "err line\n", true);
        assert!(merged.contains("out line") && merged.contains("err line"));
    }

    /// Silence and unreadability are different facts and must not share a
    /// rendering — telling them apart is the whole point of this function.
    #[test]
    fn silence_and_unreadable_are_distinguishable() {
        let quiet = merge_log_streams("", "", true);
        let broken = merge_log_streams("", "No such container: x", false);
        assert_ne!(quiet, broken);
        assert!(!quiet.is_empty() && !broken.is_empty());
        assert!(broken.contains("No such container"));
    }
}
