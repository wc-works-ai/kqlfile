package parser

import "testing"

func TestParseErrors(t *testing.T) {
	if _, err := Parse(""); err == nil {
		t.Fatalf("expected empty error")
	}
	if _, err := Parse("where"); err == nil {
		t.Fatalf("expected invalid where")
	}
	if _, err := Parse("T | project"); err == nil {
		t.Fatalf("expected project error")
	}
	if _, err := Parse("T | extend a"); err == nil {
		t.Fatalf("expected extend error")
	}
	if _, err := Parse("T | extend x = "); err == nil {
		t.Fatalf("expected extend value error")
	}
	if _, err := Parse("T | summarize sum(x)"); err == nil {
		t.Fatalf("expected summarize error")
	}
	if _, err := Parse("T | take x"); err == nil {
		t.Fatalf("expected take error")
	}
	if _, err := Parse("T | order by"); err == nil {
		t.Fatalf("expected order by error")
	}
	if _, err := Parse("T | order by col side"); err == nil {
		t.Fatalf("expected order by direction error")
	}
	if _, err := Parse("T | join (x) on a != b"); err == nil {
		t.Fatalf("expected join op error")
	}
	if _, err := Parse("T | join kind=left (x) on a == b"); err == nil {
		t.Fatalf("expected join kind error")
	}
	if _, err := Parse("T | join ( ) on a == b"); err == nil {
		t.Fatalf("expected join path error")
	}
	if _, err := Parse("T | join (x) on"); err == nil {
		t.Fatalf("expected join on error")
	}
	if _, err := parseOperator("unknown"); err == nil {
		t.Fatalf("expected unknown operator")
	}
}

func TestParseLogicalErrors(t *testing.T) {
	if _, err := Parse("T | where age > 1 and"); err == nil {
		t.Fatalf("expected logical error")
	}
	if _, err := Parse("T | where age > 1 xor active == true"); err == nil {
		t.Fatalf("expected logical op error")
	}
}

func TestParseLiterals(t *testing.T) {
	if _, err := parseLiteralOrColumn("\"x\""); err != nil {
		t.Fatalf("string literal: %v", err)
	}
	if _, err := parseLiteralOrColumn("'x'"); err != nil {
		t.Fatalf("string literal: %v", err)
	}
	if _, err := parseLiteralOrColumn("10"); err != nil {
		t.Fatalf("int literal: %v", err)
	}
	if _, err := parseLiteralOrColumn("1.25"); err != nil {
		t.Fatalf("float literal: %v", err)
	}
	if _, err := parseLiteralOrColumn("true"); err != nil {
		t.Fatalf("bool literal: %v", err)
	}
	if _, err := parseLiteralOrColumn("col"); err != nil {
		t.Fatalf("column literal: %v", err)
	}
	if _, err := parseLiteralOrColumn(""); err == nil {
		t.Fatalf("expected empty literal error")
	}
}

func TestParseSuccessCases(t *testing.T) {
	if _, err := Parse("T | extend x = 1"); err != nil {
		t.Fatalf("extend parse: %v", err)
	}
	if _, err := Parse("T | summarize count()"); err != nil {
		t.Fatalf("summarize parse: %v", err)
	}
	if _, err := Parse("T | summarize count() by a, b"); err != nil {
		t.Fatalf("summarize by parse: %v", err)
	}
	if _, err := Parse("T | take 5"); err != nil {
		t.Fatalf("take parse: %v", err)
	}
	if _, err := Parse("T | order by a asc"); err != nil {
		t.Fatalf("order asc parse: %v", err)
	}
	if _, err := Parse("T | order by a desc"); err != nil {
		t.Fatalf("order desc parse: %v", err)
	}
	if _, err := Parse("T | join kind=inner (file.csv) on a == b"); err != nil {
		t.Fatalf("join parse: %v", err)
	}
	if _, err := Parse("T | join (file.csv) on a == b"); err != nil {
		t.Fatalf("join default parse: %v", err)
	}
	if _, err := parseCompare([]string{"a", "==", "b"}); err != nil {
		t.Fatalf("compare parse: %v", err)
	}
	if _, err := parseCompare([]string{"a", "==", ""}); err == nil {
		t.Fatalf("expected compare error")
	}
	if _, err := parseCompare([]string{"a", "~~", "b"}); err == nil {
		t.Fatalf("expected compare operator error")
	}
}

func TestParseInternalErrors(t *testing.T) {
	if _, err := parseLogicalExpr("age"); err == nil {
		t.Fatalf("expected logical len error")
	}
	if _, err := parseLogicalExpr("age == "); err == nil {
		t.Fatalf("expected logical compare error")
	}
	if _, err := parseLogicalExpr("age ~~ 1"); err == nil {
		t.Fatalf("expected logical invalid operator")
	}
	if _, err := Parse("T | where age > 1 and x =="); err == nil {
		t.Fatalf("expected logical right compare error")
	}
	if _, err := Parse("T | where age > 1 and x ~~ 2"); err == nil {
		t.Fatalf("expected logical invalid operator error")
	}
	if _, err := parseCompare([]string{"a"}); err == nil {
		t.Fatalf("expected compare error")
	}
	if _, err := parseOrderBy("order x"); err == nil {
		t.Fatalf("expected order by prefix error")
	}
	if _, err := parseJoin("join x on a == b"); err == nil {
		t.Fatalf("expected join paren error")
	}
	if _, err := parseJoin("join )("); err == nil {
		t.Fatalf("expected join paren order error")
	}
	if _, err := parseJoin("x"); err == nil {
		t.Fatalf("expected invalid join error")
	}
	if _, err := parseJoin("join (x) where a == b"); err == nil {
		t.Fatalf("expected join on clause error")
	}
	if _, err := parseJoin("join (x) on a"); err == nil {
		t.Fatalf("expected join on arity error")
	}
	if _, err := parseOperator("take"); err == nil {
		t.Fatalf("expected take arity error")
	}
	if _, err := parseOperator("order by a"); err != nil {
		t.Fatalf("order operator: %v", err)
	}
	if _, err := parseOperator("summarize count()"); err != nil {
		t.Fatalf("summarize operator: %v", err)
	}
}

func TestParseOperatorAll(t *testing.T) {
	cases := []string{
		"where a > 1",
		"project a",
		"extend x = 1",
		"summarize count()",
		"take 1",
		"order by a",
		"join (file.csv) on a == b",
		"join (file.csv) on a = b",
	}
	for _, c := range cases {
		if _, err := parseOperator(c); err != nil {
			t.Fatalf("parseOperator failed for %s: %v", c, err)
		}
	}
	if _, err := parseOperator(""); err == nil {
		t.Fatalf("expected empty segment error")
	}
}

func TestParseJoinSuccessCases(t *testing.T) {
	if _, err := parseJoin("join (file.csv) on a == b"); err != nil {
		t.Fatalf("join == parse: %v", err)
	}
	if _, err := parseJoin("join (file.csv) on a = b"); err != nil {
		t.Fatalf("join = parse: %v", err)
	}
	if _, err := parseJoin("join kind=inner (file.csv) on a == b"); err != nil {
		t.Fatalf("join kind parse: %v", err)
	}
}
