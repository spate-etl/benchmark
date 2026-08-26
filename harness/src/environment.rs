//! Environment profiles: the unit of comparability for hardware.
//!
//! Records carry an `env_id` and the site never draws two environments on one
//! axis. That is why a profile is a committed file with a stable id rather than
//! a hostname — `Marcuss-MBP.kainth.co.uk` is not a hardware disclosure, cannot
//! be compared across machines, and tells a reader nothing they can reproduce
//! against.
//!
//! The profile also owns the **infrastructure envelope**, and that placement is
//! the fix for a specific failure. Previously the broker and ClickHouse CPU
//! caps came from environment variables: a runner script set one pair, the
//! driver's defaults declared another, and the written methodology stated a
//! third, while every recorded number stayed silent about which had been in
//! force. One source, applied and then read back from the running containers.

use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

/// A loaded environment profile and its content digest.
#[derive(Debug, Clone)]
pub struct Environment {
    /// The profile.
    pub spec: Profile,
    /// Hash of the file's bytes, recorded on every result so a later edit
    /// cannot retroactively re-describe runs that already happened.
    pub digest: String,
    /// Directory the profile was loaded from, for resolving relative paths.
    pub dir: PathBuf,
}

/// The profile's contents.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// Stable identifier, equal to the file stem.
    pub id: String,
    /// Whether numbers from here are authoritative.
    pub class: Class,
    /// Hardware description, published on the site.
    pub host: Host,
    /// Shared infrastructure, identical for every arm.
    pub infra: Infra,
    /// Where the measured ceiling lives.
    pub ceiling: CeilingRef,
    /// What this rig does when nothing changes.
    #[serde(default)]
    pub noise: Noise,
}

/// The rig's own spread, measured by an A/A control.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Noise {
    /// Relative difference between the two halves of an A/A pair, above which a
    /// sweep's timing verdicts are not to be believed.
    ///
    /// `None` until an A/A run has measured one here. A sweep still runs its
    /// control and still records the delta; what it cannot do is call the delta
    /// acceptable, because nothing has said what acceptable is.
    #[serde(default)]
    pub aa_spread: Option<f64>,
}

/// How much weight a reader should give numbers from this environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Class {
    /// Dedicated hardware; numbers stand on their own.
    Authoritative,
    /// A shared or virtualised host. The site renders its caveat banner from
    /// this value, not from a string match on the OS — so the banner disappears
    /// on its own when an authoritative environment is added, rather than
    /// having to be remembered.
    Indicative,
    /// Synthetic data for development. Never published.
    Fixture,
}

impl Class {
    /// Why records from this class may not be published, if they may not.
    ///
    /// Written as an exhaustive match rather than a negated comparison so that
    /// adding a class forces somebody to answer the question for it, and written
    /// as a *reason* rather than a boolean because the reason is the part that
    /// was got wrong. The driver answered `false` here by attaching
    /// `Flag::ThirdPartyHardware` to the record — a flag that means "produced on
    /// hardware we do not control", which is a different claim, and a false one
    /// about a fixture run on the very machine the reference profile describes.
    /// A consumer filtering on that flag would have read a synthetic development
    /// record as a real measurement taken somewhere else.
    ///
    /// The string is the marker a record carries, so it has to say what is wrong
    /// with the number rather than what is wrong with the hardware.
    #[must_use]
    pub fn publication_bar(self) -> Option<&'static str> {
        match self {
            // Deliberately still runnable. A fixture environment is where
            // exploratory work goes — a configuration sweep whose records must
            // exist somewhere and must never be published — so the answer is to
            // mark its records unmistakably, not to refuse the run and push the
            // work somewhere with no record at all.
            Self::Fixture => {
                Some("fixture environment: synthetic development data, never published")
            }
            Self::Authoritative | Self::Indicative => None,
        }
    }
}

/// Hardware description.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Host {
    pub description: String,
    pub cpu: String,
    pub cores: u32,
    #[serde(default)]
    pub core_layout: String,
    pub memory: String,
    pub os: String,
    pub arch: String,
    #[serde(default)]
    pub vm_cpus: u32,
    #[serde(default)]
    pub vm_memory: String,
    #[serde(default)]
    pub caveats: String,
}

/// Shared infrastructure, outside every arm's envelope.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Infra {
    /// Topic partition count. Bounds consume parallelism for every arm equally.
    pub partitions: i32,
    pub broker: Broker,
    pub clickhouse: ClickHouse,
    /// What the measured data paths sit on.
    #[serde(default)]
    pub storage: Storage,
}

/// Which device each measured data path sits on.
///
/// [`Kind`] is part of [`Environment::infra_digest`], so a ceiling measured
/// under one layout does not gate a run under another.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Storage {
    #[serde(default)]
    pub kind: Kind,
    /// Host path bind-mounted at ClickHouse's data directory.
    #[serde(default)]
    pub clickhouse_data: String,
    /// Host path bind-mounted at the broker's data directory.
    #[serde(default)]
    pub broker_data: String,
}

/// How the infrastructure's data paths are laid out on the host.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Kind {
    /// Every container writes to its own layer on the host's root filesystem.
    /// One device, shared by ClickHouse, the broker and Docker. The default,
    /// and what a single-disk host has.
    #[default]
    SharedRoot,
    /// ClickHouse and the broker each bind-mount a host path on a device of its
    /// own.
    LocalNvme,
}

/// The broker and its built-in registry.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Broker {
    pub kind: String,
    pub image: String,
    pub cpus: String,
    pub memory: String,
    pub registry: String,
}

/// The ingestion target.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouse {
    pub image: String,
    pub cpus: String,
    pub memory: String,
}

/// Where the measured ceiling for this environment lives.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CeilingRef {
    /// Path relative to the environments directory.
    pub file: String,
}

/// The ceilings an arm may be gated against, resolved for the current corpus.
///
/// Re-exported rather than defined here because a ceiling is a property of the
/// measurement rather than of the hardware profile, and because the resolution —
/// which of the committed figures still describe this corpus and this envelope —
/// is [`crate::ceiling`]'s whole subject. The name stays `Ceiling` so that every
/// call site that already says `environment::Ceiling` keeps resolving to the one
/// type the harness gates on.
pub use crate::ceiling::Ceiling;
/// The fraction of a measured ceiling above which an arm is infra-bound.
///
/// Re-exported for the same reason: the limit belongs beside the ceilings, and
/// `environment::HEADROOM_LIMIT` is the name the driver and the methodology both
/// already use.
pub use crate::ceiling::HEADROOM_LIMIT;

impl Environment {
    /// Loads the profile named `id` from `dir`.
    ///
    /// # Errors
    ///
    /// If the file is missing, does not parse, or its `id` disagrees with the
    /// filename — a mismatch would let two profiles claim the same identity and
    /// silently merge two hardware configurations into one comparison group.
    pub fn load(dir: &Path, id: &str) -> Result<Self, String> {
        let path = dir.join(format!("{id}.toml"));
        let src =
            std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
        let spec: Profile = toml::from_str(&src).map_err(|e| format!("{}: {e}", path.display()))?;
        if spec.id != id {
            return Err(format!(
                "{}: declares id {:?} but is named {id:?}",
                path.display(),
                spec.id
            ));
        }
        spec.infra
            .storage
            .check()
            .map_err(|e| format!("{}: {e}", path.display()))?;
        let digest = short_digest(src.as_bytes());
        Ok(Self {
            spec,
            digest,
            dir: dir.to_path_buf(),
        })
    }

    /// Where this environment's ceilings file lives.
    #[must_use]
    pub fn ceilings_path(&self) -> PathBuf {
        self.dir.join(&self.spec.ceiling.file)
    }

    /// Every ceiling measured for this environment, exactly as committed.
    ///
    /// The file, not the gate. `bench ceiling` needs this to show a maintainer
    /// what was measured *and* why it is or is not usable; everything that acts
    /// on a ceiling wants [`Environment::ceiling`] instead.
    ///
    /// An absent file yields an empty set, which [`crate::ceiling::Ceilings::gate`]
    /// then refuses with a reason naming the measurement to take. The same
    /// distinction [`Environment::ceiling`] draws: `Err` is a file somebody has
    /// to fix, a refusal is a measurement somebody has to run, and an
    /// environment committed ahead of its bootstrap owes a measurement.
    ///
    /// # Errors
    ///
    /// If the referenced file exists and does not parse.
    pub fn ceilings(&self) -> Result<crate::ceiling::Ceilings, String> {
        let path = self.ceilings_path();
        if !path.exists() {
            return Ok(crate::ceiling::Ceilings::default());
        }
        crate::ceiling::Ceilings::load(&path)
    }

    /// The ceilings this environment may actually be **gated against**.
    ///
    /// Resolved against the corpus the harness currently generates and against
    /// this profile's own infrastructure envelope, so a figure measured at
    /// another message size or under other caps is dropped rather than scaled.
    /// A dropped ceiling leaves `consume_msgs_per_s` at zero — which is what
    /// raises `Flag::HeadroomUnproven` — and its reason in
    /// [`Ceiling::refusals`], so "we did not gate this arm" never reads as "this
    /// arm cleared the gate".
    ///
    /// Returning `Ok` with nothing gateable rather than `Err` is deliberate.
    /// `Err` here means the file is broken and somebody has to fix a file; a
    /// refusal means the file is fine and somebody has to run a measurement.
    /// Collapsing the two would make a stale ceiling look like a parse error.
    ///
    /// # Errors
    ///
    /// If the referenced file is missing or does not parse.
    pub fn ceiling(&self) -> Result<Ceiling, String> {
        Ok(self
            .ceilings()?
            .gate(crate::ceiling::corpus_message_bytes(), &self.infra_digest()))
    }

    /// Digest over the **envelope-defining** subset of the infrastructure.
    ///
    /// Deliberately excludes versions. A ClickHouse patch release is soft
    /// provenance — recorded, rendered as a footnote — because refusing to
    /// compare across one would make the suite unusable. What splits a
    /// comparability group is a change in the *shape* of the infrastructure:
    /// CPU, memory, partitions, broker family, and the storage [`Kind`].
    ///
    /// The storage paths are excluded. A mount point is not a device, so moving
    /// one changes nothing the rig contends for; a path that is not its own
    /// mount is caught by `crate::infra`, which reads the mount back.
    #[must_use]
    pub fn infra_digest(&self) -> String {
        let i = &self.spec.infra;
        short_digest(
            format!(
                "{}|{}|{}|{}|{}|{}|{}",
                i.broker.kind,
                i.broker.cpus,
                i.broker.memory,
                i.clickhouse.cpus,
                i.clickhouse.memory,
                i.partitions,
                i.storage.kind.as_str()
            )
            .as_bytes(),
        )
    }

    /// Whether results from here may be published.
    #[must_use]
    pub fn is_publishable(&self) -> bool {
        self.spec.class.publication_bar().is_none()
    }

    /// The marker every record produced here must carry, if any.
    ///
    /// `None` for a publishable environment. The driver prefixes the record's
    /// `note` with this, so a fixture record says so in the archive rather than
    /// only on the terminal of whoever ran it.
    #[must_use]
    pub fn publication_bar(&self) -> Option<&'static str> {
        self.spec.class.publication_bar()
    }
}

impl Kind {
    /// The digest token, and the word an operator reads.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SharedRoot => "shared-root",
            Self::LocalNvme => "local-nvme",
        }
    }
}

impl Storage {
    /// Refuses `local-nvme` that does not name both paths.
    ///
    /// Without them the containers write to their own layers on the root
    /// filesystem while `infra_digest` records a layout they are not running
    /// under.
    fn check(&self) -> Result<(), String> {
        if self.kind == Kind::LocalNvme
            && (self.clickhouse_data.trim().is_empty() || self.broker_data.trim().is_empty())
        {
            return Err("[infra.storage] kind = \"local-nvme\" must also give \
                        clickhouse_data and broker_data: without them the containers \
                        write to their own layers on the root filesystem, which is what \
                        \"shared-root\" means, while infra_digest goes on saying they \
                        did not"
                .to_owned());
        }
        Ok(())
    }
}

/// Twelve hex characters of SHA-256 — enough to identify, short enough to read.
fn short_digest(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize()
        .iter()
        .take(6)
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_digest_is_stable_and_short() {
        let a = short_digest(b"hello");
        assert_eq!(a.len(), 12);
        assert_eq!(a, short_digest(b"hello"));
        assert_ne!(a, short_digest(b"hello "));
    }

    #[test]
    fn only_the_fixture_class_bars_publication() {
        // The defect: the driver read "not publishable" and wrote
        // `Flag::ThirdPartyHardware` onto the record, which means "produced on
        // hardware we do not control". A fixture run happens on our own machine,
        // so the record made a false claim about where it came from while
        // saying nothing true about why it must not be believed — and the run
        // still appended to `results/`.
        assert!(Class::Fixture.publication_bar().is_some());
        assert!(Class::Indicative.publication_bar().is_none());
        assert!(Class::Authoritative.publication_bar().is_none());
    }

    #[test]
    fn the_publication_bar_describes_the_data_not_the_hardware() {
        let bar = Class::Fixture.publication_bar().expect("fixture is barred");
        assert!(bar.contains("fixture"), "{bar}");
        assert!(
            !bar.contains("hardware"),
            "the marker must not restate the third-party-hardware claim: {bar}"
        );
    }

    /// The limit itself is pinned in `crate::ceiling`, where it lives. This
    /// asserts the re-export, because the driver imports it from here and a
    /// profile module that stopped offering the name would fail the gate rather
    /// than fail to compile if the import were ever made conditional.
    #[test]
    fn the_headroom_limit_is_reachable_under_the_name_the_driver_imports() {
        assert!((HEADROOM_LIMIT - crate::ceiling::HEADROOM_LIMIT).abs() < f64::EPSILON);
    }

    fn infra_with(storage: Storage) -> Infra {
        Infra {
            partitions: 8,
            broker: Broker {
                kind: "redpanda".to_owned(),
                image: "redpandadata/redpanda:v26.1.13".to_owned(),
                cpus: "3".to_owned(),
                memory: "8g".to_owned(),
                registry: "redpanda-builtin".to_owned(),
            },
            clickhouse: ClickHouse {
                image: "clickhouse/clickhouse-server:26.3".to_owned(),
                cpus: "16".to_owned(),
                memory: "16g".to_owned(),
            },
            storage,
        }
    }

    fn env_with(infra: Infra) -> Environment {
        Environment {
            spec: Profile {
                id: "t".to_owned(),
                class: Class::Fixture,
                host: Host {
                    description: String::new(),
                    cpu: String::new(),
                    cores: 1,
                    core_layout: String::new(),
                    memory: String::new(),
                    os: String::new(),
                    arch: String::new(),
                    vm_cpus: 0,
                    vm_memory: String::new(),
                    caveats: String::new(),
                },
                infra,
                ceiling: CeilingRef {
                    file: "ceilings/t.json".to_owned(),
                },
                noise: Noise::default(),
            },
            digest: "0".repeat(12),
            dir: PathBuf::from("."),
        }
    }

    /// A profile committed ahead of its ceiling bootstrap owes a measurement,
    /// not a file fix, and the two exits are different.
    #[test]
    fn an_unmeasured_ceiling_is_a_refusal_rather_than_an_error() {
        let gate = crate::ceiling::Ceilings::default().gate(4056, "abcdef123456");
        assert_eq!(gate.consume_msgs_per_s, 0);
        assert!(
            gate.refusals()
                .iter()
                .any(|r| r.contains("bench ceiling --measure")),
            "the refusal must name the measurement to take: {:?}",
            gate.refusals()
        );
    }

    #[test]
    fn the_storage_layout_splits_a_comparability_group() {
        let shared = env_with(infra_with(Storage::default()));
        let nvme = env_with(infra_with(Storage {
            kind: Kind::LocalNvme,
            clickhouse_data: "/mnt/ch".to_owned(),
            broker_data: "/mnt/br".to_owned(),
        }));
        assert_ne!(shared.infra_digest(), nvme.infra_digest());

        // Paths are excluded.
        let moved = env_with(infra_with(Storage {
            kind: Kind::LocalNvme,
            clickhouse_data: "/data/ch".to_owned(),
            broker_data: "/data/br".to_owned(),
        }));
        assert_eq!(nvme.infra_digest(), moved.infra_digest());
    }

    #[test]
    fn a_profile_that_says_nothing_about_storage_is_saying_shared_root() {
        assert_eq!(Storage::default().kind, Kind::SharedRoot);
        assert_eq!(Kind::SharedRoot.as_str(), "shared-root");
        assert_eq!(Kind::LocalNvme.as_str(), "local-nvme");
    }

    #[test]
    fn declaring_local_nvme_without_paths_is_refused_at_load() {
        let missing = Storage {
            kind: Kind::LocalNvme,
            clickhouse_data: String::new(),
            broker_data: "/mnt/br".to_owned(),
        };
        let e = missing.check().expect_err("must refuse");
        assert!(e.contains("clickhouse_data"), "{e}");

        assert!(Storage::default().check().is_ok());
        assert!(
            Storage {
                kind: Kind::LocalNvme,
                clickhouse_data: "/mnt/ch".to_owned(),
                broker_data: "/mnt/br".to_owned(),
            }
            .check()
            .is_ok()
        );
    }
}
