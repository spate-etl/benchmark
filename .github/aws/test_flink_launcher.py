#!/usr/bin/env python3
"""Docker smoke checks, called explicitly by CI after the Java build."""
import argparse
import importlib.util
from pathlib import Path
import subprocess
import time
import tomllib


def check_image(image, runtime_image="flink:2.2.1-java17"):
    def launch(options="", mode="generic", expected=None):
        expected = expected or runtime_image
        command = ('source /opt/flink/bin/config.sh; '
                   'java ${FLINK_ENV_JAVA_OPTS} ${FLINK_ENV_JAVA_OPTS_TM} '
                   '-XshowSettings:properties -version; status=$?; '
                   'cat /opt/flink/log/gc.log; exit "$status"')
        return subprocess.run(["docker", "run", "--rm", "--cpus=2",
                               "-e", f"BENCH_JVM_OPTS={options}", "-e", f"AVRO_MODE={mode}",
                               "-e", f"EXPECT_RUNTIME_IMAGE={expected}", image,
                               "bash", "-c", command, "/opt/flink/bin/taskmanager.sh"],
                              text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)

    result = launch("-XX:ParallelGCThreads=8 -XX:ConcGCThreads=2", mode="specific")
    assert result.returncode == 0, result.stdout
    for evidence in ("Parallel Workers: 8", "Concurrent Workers: 2",
                     "org.apache.avro.specific.use_custom_coders = true"):
        assert evidence in result.stdout, (evidence, result.stdout)
    for options in ("-XX:MisspelledGCWorkerCount=8", "-Xlog:gc*:file=/tmp/elsewhere.log",
                    "-XX:+IgnoreUnrecognizedVMOptions", "-Xmx1g"):
        rejected = launch(options)
        assert rejected.returncode != 0, (options, rejected.stdout)
    mismatch = launch(expected="deliberately-wrong-image")
    assert mismatch.returncode != 0, mismatch.stdout

    # The sizing guard only runs on the `taskmanager` argument, so the checks
    # above never reach it: they hand the entrypoint `bash`. Drive it directly.
    # Every case here is refused before the entrypoint execs Flink, so no
    # cluster starts and nothing needs a JobManager to talk to.
    def size(process_mib, memory="96g"):
        return subprocess.run(["docker", "run", "--rm", "--cpus=2", f"--memory={memory}",
                               f"--memory-swap={memory}", "-e", f"BENCH_PROCESS_MIB={process_mib}",
                               image, "taskmanager"],
                              text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=60)

    for process_mib, memory, why in (
            ("21503", "96g", "one MiB below the era-sizing floor the envelope declares"),
            ("34817", "96g", "one MiB above the compressed-oops boundary"),
            ("not-a-number", "96g", "not an integer, so the arithmetic must not decide it"),
            ("21504", "22g", "in range, but leaves under an eighth of the container "
                             "outside Flink's own budget"),
    ):
        refused = size(process_mib, memory)
        assert refused.returncode != 0, (process_mib, memory, why, refused.stdout)
        assert "sizing guard" in refused.stdout or "outside Flink" in refused.stdout, \
            (process_mib, why, refused.stdout)

    # Every JVM option the review runner declares must actually start a JVM.
    # `G1NewSizePercent` is experimental and needs `-XX:+UnlockExperimentalVMOptions`
    # ahead of it; declared without the unlock it cost a screening cell, because
    # the entrypoint disables `IgnoreUnrecognizedVMOptions` and the TaskManager
    # exits at startup. That reads as a container that died during the drain, so
    # nothing points at the flag. The cells are the source of truth here rather
    # than a second list that can drift from them.
    for name, options in declared_jvm_opts(runtime_image):
        started = launch(options)
        assert started.returncode == 0, (name, options, started.stdout)

    # Positive control: without it every assertion above is satisfied by a
    # guard that refuses everything, which would stop the arm from running at
    # all. Started detached and removed by name — a `timeout` on `docker run`
    # detaches the client and leaves the container alive. What happens after
    # the guard does not matter here; a TaskManager with no JobManager to reach
    # is free to fail, so long as it failed for its own reasons.
    subprocess.run(["docker", "rm", "-f", "flink-launcher-sizing"], capture_output=True, timeout=30)
    subprocess.run(["docker", "run", "-d", "--name", "flink-launcher-sizing", "--cpus=2",
                    "--memory=96g", "--memory-swap=96g", "-e", "BENCH_PROCESS_MIB=21504",
                    image, "taskmanager"], capture_output=True, timeout=60, check=True)
    try:
        time.sleep(10)
        accepted = subprocess.run(["docker", "logs", "flink-launcher-sizing"], text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30).stdout
    finally:
        subprocess.run(["docker", "rm", "-f", "flink-launcher-sizing"], capture_output=True, timeout=30)
    for refusal in ("sizing guard", "outside Flink"):
        assert refusal not in accepted, (refusal, accepted[-2000:])

    if "java21" in runtime_image:
        zgc = launch("-XX:+UseZGC -XX:+ZGenerational")
        assert zgc.returncode == 0, zgc.stdout
        assert "Using The Z Garbage Collector" in zgc.stdout, zgc.stdout
        assert "java.version = 21" in zgc.stdout, zgc.stdout
    print("Flink launcher: worker counts, custom coders, GC path, typo rejection, "
          "runtime provenance and TaskManager sizing bounds passed")


def declared_jvm_opts(runtime_image):
    """Each screening cell's JVM options, for the runtime it names."""
    here = Path(__file__).parent
    spec = importlib.util.spec_from_file_location("confirm", here / "flink-confirm.py")
    confirm = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(confirm)
    descriptor = here.parents[1] / "entrants/flink/entrant.toml"
    defaults = tomllib.loads(descriptor.read_text())["variants"][0]["knobs"]
    return [(name, knobs["jvm_opts"]) for name, knobs in confirm.screen_cases(defaults)
            if knobs["jvm_opts"] and knobs["runtime_image"] == runtime_image]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--runtime-image", default="flink:2.2.1-java17")
    args = parser.parse_args()
    check_image(args.image, args.runtime_image)
