#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export RECOVERY_SESSION_NAME="${RECOVERY_SESSION_NAME:-recovery_test_early}"
export KILL_DELAY="${KILL_DELAY:-20}"

exec "${SCRIPT_DIR}/test_recovery.sh" "$@"
