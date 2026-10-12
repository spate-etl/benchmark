//! The measurement harness for the Spate streaming ETL benchmark.
//!
//! Everything a published number depends on lives here, and nothing here depends
//! on any system under test. That direction of dependency is the point: a
//! competitor must be able to audit how they were measured without trusting the
//! code of the system they are being compared against.
//!
//! The modules split along the same seam as the repository:
//!
//! - [`report`] — the versioned record. What gets published.
//! - [`validate`] — the rules a committed record has to satisfy, as one function
//!   so that `bench validate` and anything gating a pull request check the same
//!   thing rather than two implementations of it.
//! - [`corpus`] — the deterministic data, as a pure function of `batch_id`.
//! - [`ceiling`] — what the broker and ClickHouse can absorb, measured, and the
//!   refusal to gate against a measurement that does not describe this corpus.
//! - [`inserter`] — the ceiling pass's ClickHouse inserter, run from inside the
//!   bench network so that it crosses the same boundary an arm's inserts do.
//! - [`fetcher`] — the ceiling pass's broker consumer, inside the network for
//!   the same reason and to the same effect: twenty-four times the message rate
//!   the published port served.
//! - [`sampler`] — cgroup v2 measurement from outside the container under test.
//! - [`docker`], [`http`] — plumbing.
//!
//! `methodology/` is normative. Where this code and that document disagree,
//! the document is what the competitor implementations were written against, so
//! the code is wrong.

// The driver narrates progress on stderr by design: a 30-hour sweep that says
// nothing until it finishes is unusable.
#![allow(clippy::print_stderr, clippy::print_stdout)]

/// `key` from the environment, else `default`.
///
/// Deliberately rare in this crate: the measurement envelope comes from an
/// environment profile, not from ambient variables. This exists for the few
/// values that genuinely are ambient — local credentials, an image override.
pub fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// `key` from the environment parsed as `u64`, else `default`.
///
/// # Panics
///
/// If the variable is set but does not parse. A typo in a knob must stop the
/// run, not silently fall back to a default and record a value that was never
/// applied.
pub fn env_u64(key: &str, default: u64) -> u64 {
    match std::env::var(key) {
        Ok(v) => v
            .parse()
            .unwrap_or_else(|e| panic!("{key}={v:?} is not a u64: {e}")),
        Err(_) => default,
    }
}

pub mod ceiling;
pub mod corpus;
pub mod docker;
pub mod driver;
pub mod entrant;
pub mod environment;
pub mod fetcher;
pub mod frames;
pub mod http;
pub mod infra;
pub mod inserter;
pub mod jvm;
pub mod kafka;
pub mod report;
pub mod results;
pub mod sampler;
pub mod select;
pub mod serverside;
pub mod validate;
