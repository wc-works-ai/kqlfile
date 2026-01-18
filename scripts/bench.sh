#!/usr/bin/env bash
set -euo pipefail

SIZE_GB=1
ROWS_PER_CHUNK=100000
OUTPUT="testdata/bench.csv"
QUERY='T | where age > 30 | project id, age, city | take 100000'

while [[ $# -gt 0 ]]; do
  case "$1" in
    --size-gb) SIZE_GB="$2"; shift 2;;
    --rows-per-chunk) ROWS_PER_CHUNK="$2"; shift 2;;
    --output) OUTPUT="$2"; shift 2;;
    --query) QUERY="$2"; shift 2;;
    *) echo "unknown flag: $1"; exit 1;;
  esac
done

OS="$(uname -s)"
CPU_COUNT="$(getconf _NPROCESSORS_ONLN || sysctl -n hw.ncpu)"
RAM_MB=""
if [[ "$OS" == "Darwin" ]]; then
  RAM_MB=$(($(sysctl -n hw.memsize) / 1024 / 1024))
elif [[ "$OS" == "Linux" ]]; then
  RAM_MB=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
fi

printf "OS: %s\n" "$OS"
printf "CPU: %s\n" "$CPU_COUNT"
if [[ -n "$RAM_MB" ]]; then
  printf "RAM: %s MB\n" "$RAM_MB"
fi

python3 scripts/gen_csv.py --size-gb "$SIZE_GB" --rows-per-chunk "$ROWS_PER_CHUNK" --output "$OUTPUT"

python3 - <<PY
import os, time, subprocess, sys
output = "$OUTPUT"
query = "$QUERY"
cmd = ["./kqlfile", "--input", output, "--query", query, "--type", "csv"]
start = time.time()
with open("/tmp/kqlfile-bench.out", "wb") as f:
    subprocess.check_call(cmd, stdout=f)
elapsed = time.time() - start
size_bytes = os.path.getsize(output)
size_mb = int(size_bytes / 1024 / 1024)
throughput = int(size_mb / elapsed) if elapsed > 0 else 0
print(f"Elapsed: {elapsed:.2f}s")
print(f"Size: {size_mb} MB")
print(f"Approx throughput: {throughput} MB/s")
PY
