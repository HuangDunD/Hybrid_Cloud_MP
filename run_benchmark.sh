#!/usr/bin/env bash
set -euo pipefail
repo=$(cd -- "$(dirname -- "$0")" && pwd -P)
cd -- "$repo"
result_root=${RESULT_ROOT:-result_1124}
mkdir -p -- "$result_root"
result_root=$(cd -- "$result_root" && pwd -P)
exec 9>"$repo/.git/aliyun-benchmark-launch.lock"
flock -x 9
state="$repo/.git/aliyun-benchmark-driver"
if [[ -f "$state" ]]; then
    read -r active_pid active_log < "$state"
    if [[ -r "/proc/$active_pid/cmdline" ]] && tr '\0' '\n' < "/proc/$active_pid/cmdline" | grep -Fxq "$repo/test_multi_node.py"; then
        echo "Already running (PID $active_pid): $active_log"
        exit 0
    fi
fi
stamp=$(date +%Y%m%d_%H%M%S)
log_dir=$(mktemp -d -- "$result_root/driver_${stamp}.XXXXXX")
result="$result_root/benchmark_results_${stamp}_${log_dir##*.}"
nohup setsid python3 -u "$repo/test_multi_node.py" --result-dir "$result" 9>&- </dev/null >"$log_dir/driver.log" 2>&1 &
pid=$!
printf '%s %s\n' "$pid" "$log_dir/driver.log" > "$state"
echo "Started PID $pid. Log: $log_dir/driver.log"
echo "Results: $result"
