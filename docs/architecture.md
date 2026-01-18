# Architecture Design (High Level)

## Overview
The system is a streaming query pipeline:

CSV Reader -> Schema -> Parser -> Logical Plan -> Physical Plan -> Executor -> Output

Each component is isolated and testable, enabling contributors to evolve the system without tightly coupled changes.

## Components
- cmd/kqlfile: CLI entrypoint and flag parsing
- pkg/csvio: CSV reader and schema inference
- pkg/parser: KQL tokenizer/parser (subset)
- pkg/plan: logical and physical plan structures
- pkg/exec: streaming execution engine and operators
- pkg/output: output formatters (csv/json/table)
- pkg/stats: optional profiling and metrics

## Data Flow
1) CSV rows are streamed and typed by the schema layer.
2) The parser converts KQL into an AST.
3) The planner constructs a logical plan and then a physical pipeline.
4) The executor streams rows through operators.
5) Output formatting writes results to stdout.

## Performance Considerations
- Streaming operators for filters and projections.
- Early column pruning to reduce work.
- Materialization only when required (order by, summarize, join hash build).
