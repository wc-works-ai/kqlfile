package plan

import "kqlfile/pkg/model"

type Operator interface {
	Type() string
}

type Expr interface {
	ExprType() string
}

type ColumnRef struct {
	Name string
}

func (c ColumnRef) ExprType() string { return "column" }

type Literal struct {
	Value model.Value
}

func (l Literal) ExprType() string { return "literal" }

type CompareExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

func (c CompareExpr) ExprType() string { return "compare" }

type LogicalExpr struct {
	Left  Expr
	Op    string
	Right Expr
}

func (l LogicalExpr) ExprType() string { return "logical" }

type WhereOp struct {
	Predicate Expr
}

func (o WhereOp) Type() string { return "where" }

type ProjectOp struct {
	Columns []string
}

func (o ProjectOp) Type() string { return "project" }

type ExtendOp struct {
	Name  string
	Value Expr
}

func (o ExtendOp) Type() string { return "extend" }

type TakeOp struct {
	Count int
}

func (o TakeOp) Type() string { return "take" }

type OrderByOp struct {
	Column string
	Desc   bool
}

func (o OrderByOp) Type() string { return "orderby" }

type SummarizeOp struct {
	ByColumns []string
}

func (o SummarizeOp) Type() string { return "summarize" }

type JoinOp struct {
	Kind     string
	Right    string
	LeftKey  string
	RightKey string
}

func (o JoinOp) Type() string { return "join" }
