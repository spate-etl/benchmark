#!/usr/bin/env bash
set -euo pipefail

# Preserve spaces without the official FLINK_PROPERTIES parser touching JVM
# arguments. Delegate all cluster startup to the shipped entrypoint below.
if [[ -n "${EXPECT_RUNTIME_IMAGE:-}" && "${EXPECT_RUNTIME_IMAGE}" != "${BENCH_FLINK_RUNTIME_IMAGE}" ]]; then
    echo "Runtime image differs from the recorded runtime_image knob" >&2
    exit 1
fi

if [[ "${1:-}" == taskmanager ]]; then
    process_mib=${BENCH_PROCESS_MIB:-21504}
    if [[ ! "$process_mib" =~ ^[0-9]+$ ]] || (( process_mib < 21504 || process_mib > 34816 )); then
        echo "TaskManager process budget violates the current 21504..34816 MiB sizing guard" >&2
        exit 1
    fi
    memory_limit=$(cat /sys/fs/cgroup/memory.max)
    if [[ "$memory_limit" != max ]] && (( process_mib * 1048576 > memory_limit - memory_limit / 8 )); then
        echo "TaskManager process budget must leave one eighth of container memory outside Flink's budget" >&2
        exit 1
    fi
fi

# These options would defeat the recorded memory budget, logging or flag checks.
# Other JVM tuning remains available. Shell expansions are never evaluated.
for option in ${BENCH_JVM_OPTS:-}; do
    case "$option" in
        -Xlog*|-Xmx*|-Xms*|-XX:*IgnoreUnrecognizedVMOptions*|-XX:*MaxHeapSize*|-XX:*InitialHeapSize*|-Dorg.apache.avro.specific.use_custom_coders=*)
            echo "Use the dedicated sizing/Avro knobs; GC logging and flag validation are mandatory: $option" >&2
            exit 1 ;;
    esac
done

avro_opts=""
if [[ "${AVRO_MODE:-generic}" == specific ]]; then
    avro_opts="-Dorg.apache.avro.specific.use_custom_coders=true"
fi
export FLINK_ENV_JAVA_OPTS_TM="${BENCH_JVM_OPTS:-} ${avro_opts} -XX:-IgnoreUnrecognizedVMOptions -Xlog:gc*,safepoint:file=/opt/flink/log/gc.log:uptime,level,tags:filecount=0"
exec /docker-entrypoint.sh "$@"
