package exec

import (
	"io"
	"testing"

	"kqlfile/pkg/csvio"
	"kqlfile/pkg/parser"
)

func TestEndToEndWhereProject(t *testing.T) {
	reader, err := csvio.NewReader("../../testdata/sample.csv", nil)
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer reader.Close()
	if _, ok := reader.Schema().Index["name"]; !ok {
		t.Fatalf("schema missing name column")
	}

	ops, err := parser.Parse("T | where age > 30 | project name, age")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	pipe, err := BuildPipeline(reader, ops)
	if err != nil {
		t.Fatalf("pipeline error: %v", err)
	}

	row, err := pipe.Next()
	if err != nil {
		t.Fatalf("next error: %v", err)
	}
	if row.Values[0].String() != "bob" {
		t.Fatalf("expected bob, got %s", row.Values[0].String())
	}
	if _, err = pipe.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestEndToEndWhereAnd(t *testing.T) {
	reader, err := csvio.NewReader("../../testdata/sample.csv", nil)
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer reader.Close()

	ops, err := parser.Parse("T | where age > 30 and active == false | project name")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	pipe, err := BuildPipeline(reader, ops)
	if err != nil {
		t.Fatalf("pipeline error: %v", err)
	}

	row, err := pipe.Next()
	if err != nil {
		t.Fatalf("next error: %v", err)
	}
	if row.Values[0].String() != "bob" {
		t.Fatalf("expected bob, got %s", row.Values[0].String())
	}
	if _, err = pipe.Next(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestEndToEndJoinInner(t *testing.T) {
	reader, err := csvio.NewReader("../../testdata/join_left.csv", nil)
	if err != nil {
		t.Fatalf("reader error: %v", err)
	}
	defer reader.Close()

	ops, err := parser.Parse("T | join kind=inner (../../testdata/join_right.csv) on dept_id == dept_id | project name, dept_name")
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	pipe, err := BuildPipeline(reader, ops)
	if err != nil {
		t.Fatalf("pipeline error: %v", err)
	}

	var names []string
	for {
		row, err := pipe.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("exec error: %v", err)
		}
		names = append(names, row.Values[0].String())
	}
	if len(names) != 2 {
		t.Fatalf("expected 2 joined rows, got %d", len(names))
	}
}
