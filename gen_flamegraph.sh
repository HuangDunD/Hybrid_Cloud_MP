#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${ROOT_DIR}"

if ! command -v pidof >/dev/null 2>&1; then
  echo "pidof 不可用，请先安装 procps。" >&2
  exit 1
fi

if ! command -v perf >/dev/null 2>&1; then
  echo "perf 不可用，请先安装 linux-tools。" >&2
  exit 1
fi

if [ ! -f "${ROOT_DIR}/FlameGraph/stackcollapse-perf.pl" ] || [ ! -f "${ROOT_DIR}/FlameGraph/flamegraph.pl" ]; then
  echo "未找到 FlameGraph 脚本，请确认目录存在: ${ROOT_DIR}/FlameGraph" >&2
  exit 1
fi

if [ "$#" -lt 1 ]; then
  echo "请传入进程名，例如: ./gen_flamegraph.sh compute_server" >&2
  exit 1
fi

PROC_NAME="$1"
SAMPLE_SECONDS="${2:-15}"
SAMPLE_FREQ="${3:-49}"
CALLGRAPH_MODE="${4:-fp}"
MAX_STACK="${5:-127}"
PERF_DATA="${ROOT_DIR}/perf.data"
TMP_PERF_TXT="${ROOT_DIR}/out.perf"
TMP_FOLDED="${ROOT_DIR}/out.folded"
TMP_SVG="${ROOT_DIR}/flame.svg.tmp"
FINAL_SVG="${ROOT_DIR}/flame.svg"
if [ "${CALLGRAPH_MODE}" = "fp" ]; then
  CALLGRAPH_ARG="fp"
elif [ "${CALLGRAPH_MODE}" = "dwarf" ]; then
  CALLGRAPH_ARG="dwarf,${MAX_STACK}"
else
  echo "call-graph 模式只支持 fp 或 dwarf，当前: ${CALLGRAPH_MODE}" >&2
  exit 1
fi
PID="$(pidof "${PROC_NAME}" 2>/dev/null | awk '{print $1}' || true)"
if [ -z "${PID}" ]; then
  echo "未找到进程: ${PROC_NAME}，请先启动该进程。" >&2
  exit 1
fi

echo "目标进程: ${PROC_NAME}, PID: ${PID}"
echo "开始采样: sudo perf record -F ${SAMPLE_FREQ} -p ${PID} -g --call-graph ${CALLGRAPH_ARG} -- sleep ${SAMPLE_SECONDS}"
sudo perf record -o "${PERF_DATA}" -F "${SAMPLE_FREQ}" -p "${PID}" -g --call-graph "${CALLGRAPH_ARG}" -- sleep "${SAMPLE_SECONDS}"

echo "导出 perf 文本: sudo perf script --no-inline -i ${PERF_DATA}"
sudo perf script --no-inline -i "${PERF_DATA}" > "${TMP_PERF_TXT}"
if [ ! -s "${TMP_PERF_TXT}" ]; then
  echo "perf script 结果为空，无法生成火焰图。" >&2
  exit 1
fi

echo "折叠调用栈"
"${ROOT_DIR}/FlameGraph/stackcollapse-perf.pl" "${TMP_PERF_TXT}" > "${TMP_FOLDED}"
if [ ! -s "${TMP_FOLDED}" ]; then
  echo "折叠后的调用栈为空，无法生成火焰图。" >&2
  exit 1
fi

echo "生成火焰图"
"${ROOT_DIR}/FlameGraph/flamegraph.pl" "${TMP_FOLDED}" > "${TMP_SVG}"
if [ ! -s "${TMP_SVG}" ]; then
  echo "flame.svg 生成失败或为空。" >&2
  exit 1
fi
mv "${TMP_SVG}" "${FINAL_SVG}"
rm -f "${PERF_DATA}" "${TMP_PERF_TXT}" "${TMP_FOLDED}" "${TMP_SVG}"

echo "完成: ${FINAL_SVG}"
