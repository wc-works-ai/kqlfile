# Benchmarking kqlfile

This project includes a repeatable benchmark workflow to measure throughput and memory usage on large files. The default dataset size is 1GB and is configurable.

## Overview
- Generates a synthetic CSV dataset of a target size.
- Runs a fixed query and reports elapsed time and basic throughput.
- Logs system info (OS, CPU count, RAM) when available.

## Requirements
- Go toolchain
- Python 3 (for dataset generation)

## Usage (macOS/Linux)
```
./scripts/bench.sh --size-gb 1 --rows-per-chunk 100000 --output testdata/bench.csv
```

## Usage (Windows PowerShell)
```
powershell -ExecutionPolicy Bypass -File scripts/bench.ps1 -SizeGB 1 -RowsPerChunk 100000 -Output testdata\bench.csv
```

## Parameters
- `--size-gb` / `-SizeGB`: target size in GB (default: 1)
- `--rows-per-chunk` / `-RowsPerChunk`: rows per write chunk (default: 100000)
- `--output` / `-Output`: output CSV path (default: testdata/bench.csv)
- `--query` / `-Query`: query to run (default provided in scripts)

## Metrics Collected
- Wall-clock time
- Rows processed (estimated from file size)
- Throughput (approx MB/s)
- System info (OS, CPU count, RAM)

## Notes
- `order by` and `summarize` materialize in memory and may skew results.
- Use simple filter/project queries for baseline throughput.
