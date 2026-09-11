#!/usr/bin/env python3
"""Sample one container's cgroup v2 counters and emit CSV on stdout.

Runs INSIDE a container (`--cgroupns=host -v /sys/fs/cgroup:/cg:rw`), because on
Docker Desktop for macOS the cgroup filesystem lives in the Linux VM and is not
reachable from the host at all. Embedded into the driver with `include_str!` and
passed via `python3 -c`, so there is no bind mount of a measured path.

Why this exists rather than `docker stats`: `docker stats` reports a
pre-computed CPU *percentage* over an interval it chooses, with no cumulative
microsecond counter, and its memory figure folds in page cache. Neither can
support a defensible CPU-per-record number. Reading the cgroup directly gives
monotonic `usage_usec` and a page-cache-free footprint, identically for every
framework, with no cooperation from the thing being measured.

Two deliberate design points:

* **`memory.peak` is reset once, at startup, through a held file descriptor.**
  cgroup v2 scopes that reset to the fd: a write resets the value only for
  subsequent reads through the *same* fd, while a fresh open still returns the
  cgroup's lifetime peak. Holding the fd therefore gives an exact peak over the
  sampling window with no sampling gap. The driver starts this sampler when the
  arm's container starts and stops it the moment the drain completes, and that
  interval IS the measurement window — every published rate divides by it — so no
  signalling between driver and sampler is needed.
* **The headline memory figure is `anon` + `shmem`, not `memory.current`.**
  `memory.current` includes page cache, which on a Kafka-consuming container is
  mostly the kernel's doing rather than the framework's, and would let a
  framework look expensive for reading its own input. `anon` alone is not the
  whole of what a framework holds: a JVM running ZGC maps its heap from a
  `memfd`, and the kernel charges that to `shmem` (a subset of `file`) rather
  than to `anon`. Measured on the same JVM and the same 600 MiB live set, G1
  reports `anon=691M shmem=0` and generational ZGC reports `anon=57M
  shmem=805M` — so counting `anon` alone would publish a ZGC arm at a twelfth
  of its real footprint. Both keys are emitted separately as well, so the split
  is visible per record and the old definition can be recomputed from it.

  `shmem` rather than `file` is the right addition: swap is disabled for every
  arm, so swap-backed memory (tmpfs, shm, shared anonymous maps) is exactly as
  irreducible as `anon`, while the rest of `file` is reclaimable page cache the
  kernel populated on the framework's behalf.

`nr_throttled`/`throttled_usec` are emitted because they answer "why was it X and
not 2X?" directly: a throttled arm is CPU-cap-bound, and that is evidence rather
than inference.
"""

import os
import sys
import time

CID = sys.argv[1]
INTERVAL = float(sys.argv[2]) if len(sys.argv) > 2 else 0.1

# The container's cgroup directory depends on the daemon's cgroup driver, so it
# is resolved HERE, where the tree is visible, rather than assumed by the
# driver on the host (which, on Docker Desktop, cannot see the tree at all).
# The `cgroupfs` driver (Docker Desktop's VM) puts containers at docker/<id>;
# the `systemd` driver (the default beside systemd, e.g. Ubuntu's Docker CE)
# puts them at system.slice/docker-<id>.scope. Probe both, in that order.
_CANDIDATES = (
    f"/cg/docker/{CID}",
    f"/cg/system.slice/docker-{CID}.scope",
)
CG = next((p for p in _CANDIDATES if os.path.isdir(p)), None)

CPU_KEYS = ("usage_usec", "user_usec", "system_usec", "nr_throttled", "throttled_usec")
# `shmem` sits next to `file` because it is a subset of it; see the module
# docstring for why it is added to `anon` rather than left inside the cache.
MEM_KEYS = ("anon", "file", "shmem", "slab", "kernel_stack", "sock")


def keyed(path, wanted):
    """Parse a `key value` file into a dict, keeping only `wanted`."""
    out = {}
    try:
        with open(path) as fh:
            for line in fh:
                parts = line.split()
                if len(parts) >= 2 and parts[0] in wanted:
                    out[parts[0]] = int(parts[1])
    except OSError:
        pass
    return out


def scalar(path):
    try:
        with open(path) as fh:
            return int(fh.read().strip())
    except (OSError, ValueError):
        return -1


def main():
    if CG is None:
        print(f"sampler: no cgroup for container {CID}; probed "
              + ", ".join(_CANDIDATES), file=sys.stderr)
        return 2

    # Held open for the process lifetime; see the module docstring.
    peak_fd = None
    try:
        peak_fd = open(os.path.join(CG, "memory.peak"), "r+")
        peak_fd.write("0")
        peak_fd.flush()
    except OSError as exc:
        # Not fatal: the driver can still take a windowed peak from the
        # `mem_current`/`anon` series. Say so loudly rather than silently
        # reporting a lifetime peak as if it were a windowed one.
        print(f"sampler: memory.peak not resettable ({exc}); "
              f"mem_peak column is LIFETIME, not windowed", file=sys.stderr)

    # Record the cap alongside the usage: reading it back from cpu.max proves the
    # envelope was actually applied, rather than trusting that `--cpus` was
    # accepted.
    cpu_max = "unknown"
    try:
        with open(os.path.join(CG, "cpu.max")) as fh:
            cpu_max = fh.read().strip().replace(" ", "/")
    except OSError:
        pass
    print(f"# cgroup={CG} cpu.max={cpu_max} memory.max={scalar(CG + '/memory.max')} "
          f"interval={INTERVAL}", flush=True)
    print("t_ms," + ",".join(CPU_KEYS) + ",mem_current,mem_peak,"
          + ",".join(MEM_KEYS), flush=True)

    while True:
        cpu = keyed(os.path.join(CG, "cpu.stat"), CPU_KEYS)
        mem = keyed(os.path.join(CG, "memory.stat"), MEM_KEYS)
        current = scalar(os.path.join(CG, "memory.current"))

        peak = -1
        if peak_fd is not None:
            try:
                peak_fd.seek(0)
                peak = int(peak_fd.read().strip())
            except (OSError, ValueError):
                peak = -1

        # A vanished cgroup means the container exited; stop cleanly so the
        # driver sees EOF rather than a stream of -1 rows.
        if current < 0 and not os.path.isdir(CG):
            return 0

        row = [str(int(time.time() * 1000))]
        row += [str(cpu.get(k, -1)) for k in CPU_KEYS]
        row += [str(current), str(peak)]
        row += [str(mem.get(k, -1)) for k in MEM_KEYS]
        print(",".join(row), flush=True)
        time.sleep(INTERVAL)


if __name__ == "__main__":
    sys.exit(main())
