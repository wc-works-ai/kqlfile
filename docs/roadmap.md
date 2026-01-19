# Roadmap

## JSON Input Support (In Progress)

Goal: Add support for JSON inputs while preserving the streaming execution model.

### Scope
- JSON Lines (NDJSON) as primary input format. (done)
- Optional support for JSON arrays (non-streaming) for small files.
- Schema inference for common primitives (string, int, float, bool, datetime). (done)

### Proposed Changes
1) Input layer
- Add `pkg/jsonio` with streaming decoder for NDJSON. (done)
- Extend CLI `--type` to accept `json`. (done)
- Use existing `model.Schema` and typed `Value` model. (done)

2) Schema inference
- Infer types from a sample window similar to CSV. (done)
- Support nested fields as dotted paths (optional v2).

3) Tests
- Add `testdata/sample.jsonl` and `testdata/big.jsonl`. (partial: sample done)
- Unit tests for JSON reader and schema inference. (done)
- End-to-end query tests using the same query set as CSV. (pending)

4) Documentation
- Update README with JSON examples. (done)
- Note limitations for nested objects and arrays. (pending)

### Milestones
- M1: JSON Lines reader + schema inference
- M2: CLI type switch + E2E tests
- M3: Optional JSON array support
