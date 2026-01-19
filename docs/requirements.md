# Product Requirements

## Summary
Build an open-source Go CLI that executes a subset of Kusto Query Language (KQL) over large CSV files, prioritizing streaming execution, deterministic output, and clear operator semantics.

## Goals
- Support common KQL operators for exploration and reporting.
- Handle multi-GB CSV files without loading them into memory.
- Provide a simple CLI UX with predictable output formats.

## Non-goals (v1)
- Full KQL compatibility
- Distributed execution or clustering
- GUI or web service

## Functional Requirements
- Read one or more input files from disk (CSV and JSON Lines).
- Infer schema or accept explicit schema definitions.
- Support typed columns: string, int, float, bool, datetime.
- Parse a KQL subset (see operator list below).
- Build a logical plan and execute a physical plan with pushdown.
- Output results to stdout (csv/json/table).

## Supported Operators (v1)
- where
- project
- extend
- summarize (count only)
- take
- order by
- join (inner, hash build on right side)

## Non-functional Requirements
- Streaming execution for filters and projections.
- Deterministic results for identical inputs.
- Clear error messages and exit codes.
- High test coverage for parser, planner, and executor.

## CLI UX
Example:
```
./kqlfile --input data.csv --query "T | where age > 30 | project name, age"
```

Flags:
- --input: input CSV file
- --query: KQL query string
- --schema: optional schema override (col:type,...)
- --format: csv|json|table
