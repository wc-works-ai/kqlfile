package parser

import "testing"

func TestParseWhereProject(t *testing.T) {
	ops, err := Parse("T | where age > 30 | project name, age")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if len(ops) != 2 {
		t.Fatalf("expected 2 ops, got %d", len(ops))
	}
}

func TestParseWhereAndOr(t *testing.T) {
	_, err := Parse("T | where age > 30 and active == true or name == \"bob\"")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
}

func TestParseJoin(t *testing.T) {
	_, err := Parse("T | join kind=inner (testdata/join_right.csv) on dept_id == dept_id")
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
}
