#!/usr/bin/env bash
# Top-level driver.
#
# Affinity sidecars: with `affinity.auto_spawn_sidecar = 1` (default in
# config/compute_node_config.json), the leader compute_server fork+execs
# `mpirun` itself on startup — no manual launch needed. Set AFFINITY=1
# to use the legacy out-of-process launcher (affinity_sidecar_launch.sh)
# instead, e.g. when auto_spawn_sidecar is disabled in JSON.

pkill -f test_multi_node.py

if [[ "${AFFINITY:-0}" == "1" ]]; then
    bash ./affinity_sidecar_launch.sh stop || true
    bash ./affinity_sidecar_launch.sh start
    # Tear sidecars down when the orchestrator exits — best-effort, runs in a
    # detached subshell so it survives the run.log redirect.
    trap 'bash ./affinity_sidecar_launch.sh stop || true' EXIT
fi

nohup bash -c 'python3 -u test_multi_node.py 2>&1 | awk '\''{ print strftime("[%Y-%m-%d %H:%M:%S]"), $0; fflush(); }'\''' > run.log &
