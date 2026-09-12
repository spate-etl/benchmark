#!/usr/bin/env python3
"""One poll of Flink's own busy/backpressure/idle metrics, per job vertex.

Diagnostic only — this never reaches a published or tuning record (no
validate.rs/ALLOWED_UNITS concern), it just answers, for one point in time,
which vertex in the job graph is backpressured. tune-entrant.sh calls this
in a loop, in the background, alongside a cell's `bench run`.

The JobManager container isn't given a host port mapping by the automated
harness (unlike the manual run recipe in entrants/flink/README.md), so this
resolves its address on the bench docker network directly rather than going
through localhost.
"""
import json
import subprocess
import sys
import urllib.request

JM_CONTAINER = "spate-bench-sut-jm"
NETWORK = "spate-bench-net"
METRICS = "busyTimeMsPerSecond,backPressuredTimeMsPerSecond,idleTimeMsPerSecond"


def jm_address():
    out = subprocess.run(
        [
            "docker", "inspect", JM_CONTAINER,
            "--format", "{{.NetworkSettings.Networks." + NETWORK + ".IPAddress}}",
        ],
        capture_output=True, text=True, timeout=5,
    )
    ip = out.stdout.strip()
    return ip or None


def get_json(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        return json.load(r)


def poll(label):
    ip = jm_address()
    if not ip:
        return {"label": label, "error": "jobmanager container not found or not on the network"}

    base = f"http://{ip}:8081"
    try:
        jobs = get_json(f"{base}/jobs")["jobs"]
    except Exception as e:
        return {"label": label, "error": f"GET /jobs: {e}"}

    running = [j["id"] for j in jobs if j.get("status") == "RUNNING"]
    if not running:
        return {"label": label, "note": "no RUNNING job (between reps, or not yet submitted)"}
    job_id = running[0]

    try:
        vertices = get_json(f"{base}/jobs/{job_id}")["vertices"]
    except Exception as e:
        return {"label": label, "job_id": job_id, "error": f"GET /jobs/{job_id}: {e}"}

    per_vertex = {}
    for v in vertices:
        vid, name = v["id"], v["name"]
        try:
            metrics = get_json(
                f"{base}/jobs/{job_id}/vertices/{vid}/subtasks/metrics"
                f"?get={METRICS}&agg=avg,max,min"
            )
            per_vertex[name] = {m["id"]: {k: v for k, v in m.items() if k != "id"} for m in metrics}
        except Exception as e:
            per_vertex[name] = {"error": str(e)}

    return {"label": label, "job_id": job_id, "vertices": per_vertex}


if __name__ == "__main__":
    print(json.dumps(poll(sys.argv[1] if len(sys.argv) > 1 else "")))
