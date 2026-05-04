#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/usr/local/workspace/Hybrid_Cloud_MP"
PYDEPS_DIR="${ROOT_DIR}/.pydeps"
MPL_CFG_DIR="/tmp/matplotlib_cfg"

mkdir -p "${PYDEPS_DIR}"
mkdir -p "${MPL_CFG_DIR}"

check_matplotlib() {
  PYTHONPATH="${PYDEPS_DIR}${PYTHONPATH:+:${PYTHONPATH}}" \
  python3 - <<'PY'
import importlib.util
import sys
sys.exit(0 if importlib.util.find_spec("matplotlib") else 1)
PY
}

echo "[run_report] checking matplotlib..."
if ! check_matplotlib; then
  echo "[run_report] matplotlib not found, installing to ${PYDEPS_DIR} ..."
  if ! python3 -m pip install --target "${PYDEPS_DIR}" --upgrade matplotlib; then
    echo "[run_report] warning: matplotlib install failed, report will still run but charts may be skipped." >&2
  fi
fi

echo "[run_report] generating report..."
PYTHONPATH="${PYDEPS_DIR}${PYTHONPATH:+:${PYTHONPATH}}" \
MPLCONFIGDIR="${MPL_CFG_DIR}" \
python3 "${ROOT_DIR}/generate_report.py"

echo "[run_report] generating fixed-parameter and lazy-only charts..."
PYTHONPATH="${PYDEPS_DIR}${PYTHONPATH:+:${PYTHONPATH}}" \
MPLCONFIGDIR="${MPL_CFG_DIR}" \
python3 "${ROOT_DIR}/generate_time_breakdown.py"
