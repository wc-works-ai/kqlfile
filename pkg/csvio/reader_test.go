package csvio

import "testing"

func TestSchemaInference(t *testing.T) {
	reader, err := NewReader("../../testdata/sample.csv", nil)
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer reader.Close()
	sch := reader.Schema()
	if len(sch.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(sch.Columns))
	}
	if sch.Columns[1].Type != "int" {
		t.Fatalf("expected age to be int, got %s", sch.Columns[1].Type)
	}
}
