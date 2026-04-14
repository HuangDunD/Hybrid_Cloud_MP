#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="/usr/local/workspace/Hybrid_Cloud_MP"
PYDEPS_DIR="${ROOT_DIR}/.pydeps"
MPL_CFG_DIR="/tmp/matplotlib_cfg"

mkdir -p "${MPL_CFG_DIR}"

PYTHONPATH="${PYDEPS_DIR}${PYTHONPATH:+:${PYTHONPATH}}" \
MPLCONFIGDIR="${MPL_CFG_DIR}" \
python3 "${ROOT_DIR}/generate_report.py"
