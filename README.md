# kqlfile

A fast, streaming Go CLI that runs a small KQL subset over large flat files. It is designed to expand to other file types; currently only comma-based CSV is supported.

## Why
- Quick ad-hoc analysis of large CSVs without a database
- Lightweight ETL filtering before importing into BI tools
- Log or export investigations with a familiar query syntax
- CI and data validation checks with deterministic results

## Features
- Streaming execution for filters and projections
- KQL subset: where, project, extend, summarize (count), take, order by, join (inner)
- Output formats: csv, json, table
- Designed for multi-format inputs (currently CSV only)

## Install
```
make build
```

## Usage
```
./kqlfile --input testdata/people.csv --query "T | where age > 30 | project name, age" --type csv
```

Example result:
```
name,age
bob,41
dan,37
```

## Join Example
```
./kqlfile --input testdata/join_left.csv --query "T | join kind=inner (testdata/join_right.csv) on dept_id == dept_id | project name, dept_name" --type csv
```

Example result:
```
name,dept_name
alice,engineering
bob,finance
```

## Multiple Inputs
Use named inputs and reference the table name in the query:
```
./kqlfile --input A=testdata/people_big.csv --input B=testdata/orders_big.csv --query "A | join kind=inner (B) on id == user_id | project name, amount | take 5" --type csv
```

## Developer Commands
Makefile (Linux/macOS/WSL):
```
make build
make test
make run-sample
```

PowerShell (Windows):
```
powershell -ExecutionPolicy Bypass -File scripts/dev.ps1 -Task build
powershell -ExecutionPolicy Bypass -File scripts/dev.ps1 -Task test
```

## Limitations
- `order by` and `summarize` materialize in memory.
- `join` builds a hash table for the right input.
- Expressions are limited to simple comparisons with `and`/`or`.
- No parentheses or nested expression parsing yet.

## License
MIT

## Development Note
This project was built with the assistance of OpenAI Codex.

## Releases
Releases are automated via semantic-release on pushes to `master` using Conventional Commits.

## Roadmap
See `docs/roadmap.md` for planned JSON input support.
