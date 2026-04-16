#!/usr/bin/env bash
# Phase 6 — Launch / shutdown helper for the parmetis_sidecar cluster.
#
# Each compute node needs one sidecar process; all sidecars must share a single
# MPI_COMM_WORLD. The helper takes the compute-node IP list from
# config/compute_node_config.json -> remote_compute_nodes.remote_compute_node_ips
# and runs `mpirun -np N --hostfile ...` to launch the cluster.
#
# Usage:
#   ./affinity_sidecar_launch.sh start
#   ./affinity_sidecar_launch.sh stop
#   ./affinity_sidecar_launch.sh status
#
# Required environment / assumptions:
#   - parmetis_sidecar binary built at $REMOTE_BUILD/parmetis_sidecar/parmetis_sidecar
#   - mpirun in PATH on the launching host AND target hosts
#   - SSH keyless login between host running this script and every compute node
#   - The UDS path matches affinity.sidecar_uds_path in compute_node_config.json
#     (default /tmp/wookong_parmetis.sock).
#
# Run BEFORE compute_server (so the compute_server's PartitionerLoop can
# connect via UDS), and SIGTERM the mpirun pgroup AFTER compute_server exits.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_JSON="${SCRIPT_DIR}/config/compute_node_config.json"
REMOTE_BUILD="${REMOTE_BUILD:-/usr/local/exper/Hybrid_Cloud_MP/build}"
SIDECAR_BIN="${SIDECAR_BIN:-${REMOTE_BUILD}/parmetis_sidecar/parmetis_sidecar}"
HOSTFILE="${HOSTFILE:-${SCRIPT_DIR}/.affinity_hostfile}"
PID_FILE="${PID_FILE:-${SCRIPT_DIR}/.affinity_sidecar_mpirun.pid}"
LOG_FILE="${LOG_FILE:-${SCRIPT_DIR}/affinity_sidecar.log}"

extract_uds_path() {
    python3 - "$CONFIG_JSON" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    cfg = json.load(f)
print(cfg.get('affinity', {}).get('sidecar_uds_path', '/tmp/wookong_parmetis.sock'))
PY
}

extract_hostnames() {
    python3 - "$CONFIG_JSON" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    cfg = json.load(f)
hosts = cfg.get('remote_compute_nodes', {}).get('remote_compute_node_ips', [])
for h in hosts:
    # mpirun hostfile: "<host> slots=1"
    print(f"{h} slots=1")
PY
}

build_hostfile() {
    extract_hostnames > "$HOSTFILE"
    local n
    n=$(wc -l < "$HOSTFILE")
    if [[ "$n" -lt 1 ]]; then
        echo "ERROR: no compute nodes parsed from $CONFIG_JSON" >&2
        exit 2
    fi
    echo "$n"
}

cmd_start() {
    local n uds
    n=$(build_hostfile)
    uds=$(extract_uds_path)
    echo "[affinity-sidecar] launching ${n} ranks; UDS=${uds}; bin=${SIDECAR_BIN}"
    echo "[affinity-sidecar] hostfile:"
    cat "$HOSTFILE"

    # mpirun keeps the cluster bound; we background it and remember the pgid so
    # `stop` can SIGTERM the entire group. Each rank cleans up its UDS socket
    # on exit, but we also pre-clean to recover from a crashed previous run.
    nohup bash -c "
        for h in \$(awk '{print \$1}' '$HOSTFILE'); do
            ssh -o StrictHostKeyChecking=no \"\$h\" \"rm -f '$uds'\" || true
        done
        exec mpirun -np ${n} --hostfile '$HOSTFILE' \
            --mca btl_tcp_if_include eth0 \
            '$SIDECAR_BIN' '$uds'
    " >"$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    echo "[affinity-sidecar] mpirun pid=$(cat "$PID_FILE"); log=${LOG_FILE}"

    # Wait briefly for sidecars to bind UDS — best-effort: 5s.
    sleep 5
    echo "[affinity-sidecar] startup wait done; check ${LOG_FILE} for collective handshake."
}

cmd_stop() {
    if [[ -f "$PID_FILE" ]]; then
        local pid
        pid=$(cat "$PID_FILE")
        echo "[affinity-sidecar] SIGTERM pgid of pid=${pid}"
        kill -TERM "-$(ps -o pgid= "$pid" | tr -d ' ')" 2>/dev/null || true
        rm -f "$PID_FILE"
    fi
    # Belt-and-suspenders: ssh-kill leftovers per host.
    if [[ -f "$HOSTFILE" ]]; then
        for h in $(awk '{print $1}' "$HOSTFILE"); do
            ssh -o StrictHostKeyChecking=no "$h" "pkill -TERM -f parmetis_sidecar || true" || true
        done
    fi
    echo "[affinity-sidecar] stopped."
}

cmd_status() {
    if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
        echo "[affinity-sidecar] mpirun pid=$(cat "$PID_FILE") RUNNING"
    else
        echo "[affinity-sidecar] mpirun NOT RUNNING"
    fi
    if [[ -f "$HOSTFILE" ]]; then
        echo "[affinity-sidecar] per-host check:"
        for h in $(awk '{print $1}' "$HOSTFILE"); do
            echo -n "  $h: "
            ssh -o StrictHostKeyChecking=no "$h" "pgrep -fa parmetis_sidecar || echo '(none)'" 2>/dev/null \
                || echo '(ssh fail)'
        done
    fi
}

case "${1:-}" in
    start)  cmd_start ;;
    stop)   cmd_stop ;;
    status) cmd_status ;;
    *) echo "usage: $0 {start|stop|status}" >&2; exit 1 ;;
esac
