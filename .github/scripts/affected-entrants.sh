#!/usr/bin/env bash
# Map a push's diff to the benchmark work it invalidates.
#
#   .github/scripts/affected-entrants.sh <before-sha> <after-sha>
#
# Prints two lines on stdout:
#
#   selector=<bench selectors, space-separated, or '*' or empty>
#   trigger=<release|nightly>
#
# An empty selector means "this push moves no published number" and the launch
# workflow proposes nothing. The rules err toward over-proposing: the approval
# gate in front of every launch is the "is this re-run actually warranted"
# decision, and the plan job prints the exact arm list the money would buy —
# so a false positive costs a click, while a false negative silently leaves a
# stale number published.
set -euo pipefail

before=${1:?usage: affected-entrants.sh <before> <after>}
after=${2:?usage: affected-entrants.sh <before> <after>}

here=$(dirname "$0")

all=false
release=false
lockfile_touched=false
declare -A touched=()

# A spate version bump is invisible to path rules (it lives inside
# Cargo.lock), and it is the one change that gets `--trigger release`.
if ! diff -q \
  <("$here/spate-versions.sh" "$before") \
  <("$here/spate-versions.sh" "$after") >/dev/null; then
  touched[spate]=1
  release=true
fi

while IFS= read -r f; do
  case "$f" in
    # The comparability keys: harness code moves harness_version, the workload
    # moves dataset_version, the toolchain is provenance, and the instance
    # scripts define the box every arm runs on. Any of these invalidates
    # every published number at once.
    harness/*|workload/*|rust-toolchain.toml|.github/aws/*)
      all=true ;;
    # A profile or a ceilings file forces a re-measurement on its environment.
    # Matched by directory rather than by an id prefix: a prefix stops matching
    # the moment an environment is renamed, and the failure is silent — the
    # pipeline proposes nothing for the file that changed.
    environments/*|environments/ceilings/*)
      all=true ;;
    # The lockfile and root manifest pin every harness dependency, and each arm
    # is built `--locked` against them: a dependency bump moves codegen and thus
    # every number. Whether that re-runs everything is decided after the loop —
    # a spate-only bump is already caught above as a targeted release, so only a
    # lockfile move NOT explained by the spate versions re-runs the whole set.
    Cargo.lock|Cargo.toml)
      lockfile_touched=true ;;
    entrants/*/*)
      e=${f#entrants/}
      touched["${e%%/*}"]=1 ;;
  esac
done < <(git diff --name-only "$before" "$after")

# A lockfile/manifest change beyond a spate version bump moved some other
# harness dependency — rebuild-everything territory, at nightly cadence (a
# spate release stays the targeted `release` run handled above).
#
# Known gap: a single push that bumps a spate crate AND an unrelated dependency
# in the same Cargo.lock proposes only `spate` (release wins, so this guard is
# skipped), leaving the other arms on stale numbers. It is deliberately not
# closed here: telling "spate pulled its own transitive churn" (which should
# stay a targeted release) apart from "an unrelated dep also moved" (which
# should re-run everything) needs real dependency-graph analysis, and the cheap
# proxy — "any non-spate lock line changed" — would turn every ordinary spate
# release into a multi-hour full sweep. A `cargo update`-style combined bump is
# rare (Dependabot groups the spate crates separately) and visible in the PR;
# dispatch a `*` run by hand when one lands.
if $lockfile_touched && ! $release; then
  all=true
fi

if $all; then
  echo "selector=*"
else
  selectors=""
  for e in "${!touched[@]}"; do
    # A deleted or renamed entrant leaves a diff path whose entrant.toml no
    # longer exists; running sed on it would abort the whole script under
    # `set -e` and take the plan job red on every such push. A gone entrant
    # measures nothing — skip it.
    toml="entrants/$e/entrant.toml"
    [ -f "$toml" ] || continue
    # Only runnable entrants: a planned entrant's descriptor edits move
    # nothing measurable yet.
    status=$(sed -n 's/^status *= *"\(.*\)"/\1/p' "$toml" | head -1)
    if [ "$status" = active ]; then
      selectors="$selectors $e"
    fi
  done
  echo "selector=${selectors# }"
fi

if $release; then
  echo "trigger=release"
else
  echo "trigger=nightly"
fi
