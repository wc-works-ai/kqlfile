# Roadmap

## JSON Input Support (Planned)

Goal: Add support for JSON inputs while preserving the streaming execution model.

### Scope
- JSON Lines (NDJSON) as primary input format.
- Optional support for JSON arrays (non-streaming) for small files.
- Schema inference for common primitives (string, int, float, bool, datetime).

### Proposed Changes
1) Input layer
- Add `pkg/jsonio` with streaming decoder for NDJSON.
- Extend CLI `--type` to accept `json`.
- Use existing `model.Schema` and typed `Value` model.

2) Schema inference
- Infer types from a sample window similar to CSV.
- Support nested fields as dotted paths (optional v2).

3) Tests
- Add `testdata/sample.jsonl` and `testdata/big.jsonl`.
- Unit tests for JSON reader and schema inference.
- End-to-end query tests using the same query set as CSV.

4) Documentation
- Update README with JSON examples.
- Note limitations for nested objects and arrays.

### Milestones
- M1: JSON Lines reader + schema inference
- M2: CLI type switch + E2E tests
- M3: Optional JSON array support
