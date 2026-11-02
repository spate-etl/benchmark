# The Spate arm of the streaming ETL benchmark.
#
# Build with the REPOSITORY ROOT as context, uniformly with every other entrant.
# The arm is a cargo workspace member, so the build needs the workspace manifest
# and lockfile as well as this crate:
#
#   bench build spate
#
# or by hand:
#
#   docker build -f entrants/spate/Dockerfile -t spate-bench-spate .
#
# Why this is containerised at all: every arm runs under the same cgroup caps,
# and a host process would get all of the host's cores. Measuring our own
# framework outside the envelope we hold competitors to would invalidate the
# entire comparison.
#
# The Rust toolchain is pinned and must equal `rust-toolchain.toml`, which
# `entrants_are_valid` asserts. The pin is not boilerplate: codegen changes
# between compiler releases move throughput numbers, so the toolchain is part of
# a published benchmark's provenance. `build.rs` bakes the compiler string into
# the binary, so `--version` reports what actually built it rather than whatever
# `rustc` the host driver happens to have.
FROM rust:1.97-bookworm AS builder
WORKDIR /build

# An explicit COPY list rather than `COPY . .`. `.dockerignore` already excludes
# results/ and website/, but naming what the build needs keeps an unrelated file
# from silently invalidating the layer cache — and makes it obvious that no
# result data enters the image.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY harness harness
COPY entrants/spate entrants/spate
COPY workload workload

# `--locked` for the same reason CI passes it: without it cargo may resolve a
# different dependency graph than the one recorded, which for a benchmark means
# silently measuring different code.
#
# The framework is consumed from crates.io, so the build needs no credentials of
# any kind: a clean clone of this public repository builds the arm as-is.
#
# Cache mounts make an incremental rebuild seconds rather than minutes (rdkafka
# compiles librdkafka from vendored C sources). A cache mount is not part of the
# resulting layer, so the binary must be copied out inside the same RUN.
# Tuning branch only: frame pointers in Rust and in vendored librdkafka, so a
# profiler on the host can walk the pipeline threads' stacks.
ENV RUSTFLAGS="-C force-frame-pointers=yes" CFLAGS="-O2 -fno-omit-frame-pointer"
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked -p spate-arm --bin spate-arm \
    && cp target/release/spate-arm /build/arm

# Stage the one shared library the runtime base lacks.
#
# `distroless/cc` ships glibc, libgcc, libm and the loader, but not zlib — which
# vendored librdkafka links. Without this the container exits 127 with
# `libz.so.1: cannot open shared object file` before processing a single record.
#
# The multiarch directory is resolved from `ldd` rather than hardcoded: these
# numbers are produced on arm64 today, but the methodology anticipates a later
# bare-metal x86_64 environment, and a hardcoded triple would fail that build for
# a reason with nothing to do with the benchmark. Staged under the real
# `/usr/lib` path, not the `/lib` that `ldd` prints: both Debian and the
# distroless base are usrmerged, so `/lib` is a *symlink* and a COPY targeting it
# fails with "cannot copy to non-directory". `cp -L` dereferences the versioned
# file and writes it under the SONAME the loader actually searches for.
RUN libz="$(ldd /build/arm | awk '/libz\.so\.1/ { print $3 }')" \
    && test -n "$libz" \
    && dir="/usr/lib/$(basename "$(dirname "$libz")")" \
    && mkdir -p "/staging$dir" \
    && cp -L "$libz" "/staging$dir/libz.so.1"

# Distroless: glibc and libgcc only, no shell, runs as nonroot (65532).
#
# Note for anyone extending the harness: the cgroup sampler deliberately does not
# `docker exec` into an arm container, precisely because an image like this has
# no shell. It reads the cgroup filesystem from a sidecar instead, which also
# means the same measurement works for every arm regardless of base image.
FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=builder /staging/ /
COPY --from=builder /build/arm /usr/local/bin/spate-arm

# Defaults target the bench network; the driver overrides what it varies.
ENV FORMAT=native \
    BOOTSTRAP=spate-bench-redpanda:29092 \
    REGISTRY_URL=http://spate-bench-redpanda:8081 \
    CLICKHOUSE_URL=http://spate-bench-clickhouse:8123 \
    TOPIC=comparison-sensor-batches \
    GROUP_ID=comparison-spate

# No metrics port is exposed, on purpose: `metrics.exporter` is `none` and no
# published figure comes from the framework's own instrumentation.
ENTRYPOINT ["/usr/local/bin/spate-arm"]
