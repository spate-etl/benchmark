#!/usr/bin/env python3
"""Docker smoke checks, called explicitly by CI after the Java build."""
import argparse
import subprocess


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
    if "java21" in runtime_image:
        zgc = launch("-XX:+UseZGC -XX:+ZGenerational")
        assert zgc.returncode == 0, zgc.stdout
        assert "Using The Z Garbage Collector" in zgc.stdout, zgc.stdout
        assert "java.version = 21" in zgc.stdout, zgc.stdout
    print("Flink launcher: worker counts, custom coders, GC path, typo rejection and runtime provenance passed")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--runtime-image", default="flink:2.2.1-java17")
    args = parser.parse_args()
    check_image(args.image, args.runtime_image)
