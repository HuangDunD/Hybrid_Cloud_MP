#!/bin/bash
set -euo pipefail
exec python3 -B "$(dirname "$(readlink -f "$0")")/cluster.py" "$@"
