package plan

import "testing"

func TestPlanTypes(t *testing.T) {
	if (ColumnRef{Name: "x"}).ExprType() != "column" {
		t.Fatalf("column expr")
	}
	if (Literal{}).ExprType() != "literal" {
		t.Fatalf("literal expr")
	}
	if (CompareExpr{}).ExprType() != "compare" {
		t.Fatalf("compare expr")
	}
	if (LogicalExpr{}).ExprType() != "logical" {
		t.Fatalf("logical expr")
	}

	if (WhereOp{}).Type() != "where" {
		t.Fatalf("where type")
	}
	if (ProjectOp{}).Type() != "project" {
		t.Fatalf("project type")
	}
	if (ExtendOp{}).Type() != "extend" {
		t.Fatalf("extend type")
	}
	if (TakeOp{}).Type() != "take" {
		t.Fatalf("take type")
	}
	if (OrderByOp{}).Type() != "orderby" {
		t.Fatalf("orderby type")
	}
	if (SummarizeOp{}).Type() != "summarize" {
		t.Fatalf("summarize type")
	}
	if (JoinOp{}).Type() != "join" {
		t.Fatalf("join type")
	}
}
