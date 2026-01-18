# Architecture Design (Detailed)

This document explains why each component exists, how it behaves, and how it fits into the overall pipeline. It is intended for new contributors and reviewers.

## 1) CLI Layer (cmd/kqlfile)
Purpose: Provide a stable interface for users and translate flags into configuration.
- Parses input paths, query string, output format, and schema overrides.
- Validates inputs and emits actionable error messages.
Why it matters: Keeps UX concerns isolated so core execution logic stays clean.

## 2) CSV Reader (pkg/csvio)
Purpose: Stream CSV rows from disk without loading the full file into memory.
- Uses a reusable CSV reader for low allocation overhead.
- Supports schema inference from a sample window.
Why it matters: Large CSVs can exceed RAM; streaming keeps memory stable.

## 3) Schema and Types (pkg/model)
Purpose: Provide typed values to enable correct comparisons and aggregations.
- Supports string, int, float, bool, datetime.
- Converts raw CSV strings into typed values.
Why it matters: Correct type handling prevents string-based comparison errors.

## 4) Parser (pkg/parser)
Purpose: Convert KQL-like text into a structured representation.
- Tokenizes operators and expressions.
- Produces a plan-friendly AST.
Why it matters: Separates language syntax from execution, enabling incremental expansion.

## 5) Planner (pkg/plan)
Purpose: Represent query logic independently from execution details.
- Logical plan models the query intent.
- Physical plan maps the intent to executable operators.
Why it matters: Makes optimization and testing simpler and localized.

## 6) Execution Engine (pkg/exec)
Purpose: Execute the physical plan as a streaming pipeline.
- Filters, projections, and simple expressions stream row-by-row.
- Joins build a right-side hash map to match incoming rows.
- Order by and summarize currently materialize in memory.
Why it matters: The engine is the core of performance and correctness.

## 7) Output Formatting (pkg/output)
Purpose: Present results in common formats.
- CSV for interoperability.
- JSON for programmatic processing.
- Table for human readability.
Why it matters: Keeps formatting concerns out of query execution.

## Data Flow Example
Query:
```
T | where age > 30 and active == true | project name, age
```
Flow:
1) CLI reads query and input path.
2) CSV reader streams rows and applies inferred schema.
3) Parser builds an expression tree.
4) Planner constructs a pipeline of operators.
5) Executor filters and projects rows.
6) Output writer prints results.

## Extension Guidelines
Adding a new operator typically requires:
- Parser support for syntax.
- A plan node definition.
- An execution operator implementation.
- Tests for both parsing and execution.

Each component is intentionally small to keep changes local and reviewable.
