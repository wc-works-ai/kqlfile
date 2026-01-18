package exec

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kqlfile/pkg/csvio"
	"kqlfile/pkg/model"
	"kqlfile/pkg/plan"
)

type sliceOp struct {
	rows []*csvio.Row
	idx  int
}

func (s *sliceOp) Next() (*csvio.Row, error) {
	if s.idx >= len(s.rows) {
		return nil, io.EOF
	}
	row := s.rows[s.idx]
	s.idx++
	return row, nil
}

type errOp struct {
	err error
}

func (e *errOp) Next() (*csvio.Row, error) {
	return nil, e.err
}

type badExpr struct{}

func (badExpr) ExprType() string { return "bad" }

type badOp struct{}

func (badOp) Type() string { return "bad" }

func TestCompareValues(t *testing.T) {
	if compareValues(model.Value{Type: model.TypeInt, V: int64(1)}, model.Value{Type: model.TypeInt, V: int64(2)}) >= 0 {
		t.Fatalf("int compare")
	}
	if compareValues(model.Value{Type: model.TypeInt, V: int64(3)}, model.Value{Type: model.TypeInt, V: int64(2)}) <= 0 {
		t.Fatalf("int compare greater")
	}
	if compareValues(model.Value{Type: model.TypeFloat, V: 2.0}, model.Value{Type: model.TypeFloat, V: 1.0}) <= 0 {
		t.Fatalf("float compare")
	}
	if compareValues(model.Value{Type: model.TypeFloat, V: 1.0}, model.Value{Type: model.TypeFloat, V: 2.0}) >= 0 {
		t.Fatalf("float compare less")
	}
	if compareValues(model.Value{Type: model.TypeBool, V: false}, model.Value{Type: model.TypeBool, V: true}) >= 0 {
		t.Fatalf("bool compare")
	}
	if compareValues(model.Value{Type: model.TypeBool, V: true}, model.Value{Type: model.TypeBool, V: false}) <= 0 {
		t.Fatalf("bool compare greater")
	}
	if compareValues(model.Value{Type: model.TypeBool, V: true}, model.Value{Type: model.TypeBool, V: true}) != 0 {
		t.Fatalf("bool equal")
	}
	tm := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if compareValues(model.Value{Type: model.TypeDateTime, V: tm}, model.Value{Type: model.TypeDateTime, V: tm.Add(time.Hour)}) >= 0 {
		t.Fatalf("time compare")
	}
	if compareValues(model.Value{Type: model.TypeDateTime, V: tm.Add(time.Hour)}, model.Value{Type: model.TypeDateTime, V: tm}) <= 0 {
		t.Fatalf("time compare greater")
	}
	if compareValues(model.Value{Type: model.TypeDateTime, V: tm}, model.Value{Type: model.TypeDateTime, V: tm}) != 0 {
		t.Fatalf("time equal")
	}
	if compareValues(model.Value{Type: model.TypeString, V: "a"}, model.Value{Type: model.TypeString, V: "b"}) >= 0 {
		t.Fatalf("string compare")
	}
	if compareValues(model.Value{Type: model.TypeString, V: "b"}, model.Value{Type: model.TypeString, V: "a"}) <= 0 {
		t.Fatalf("string compare greater")
	}
	if compareValues(model.Value{Type: model.TypeString, V: "a"}, model.Value{Type: model.TypeString, V: "a"}) != 0 {
		t.Fatalf("string equal")
	}
	if compareValues(model.Value{Type: model.TypeInt, V: int64(1)}, model.Value{Type: model.TypeFloat, V: float64(2)}) >= 0 {
		t.Fatalf("mixed compare")
	}
	if compareValues(model.Value{Type: model.TypeFloat, V: float64(2)}, model.Value{Type: model.TypeInt, V: int64(1)}) <= 0 {
		t.Fatalf("mixed compare float")
	}
	if compareValues(model.Value{Type: model.TypeInt, V: int64(2)}, model.Value{Type: model.TypeInt, V: int64(2)}) != 0 {
		t.Fatalf("int equal")
	}
	if compareValues(model.Value{Type: model.TypeFloat, V: float64(2)}, model.Value{Type: model.TypeFloat, V: float64(2)}) != 0 {
		t.Fatalf("float equal")
	}
	if compareValues(model.Value{Type: model.Type("other"), V: 1}, model.Value{Type: model.Type("other"), V: 2}) >= 0 {
		t.Fatalf("default compare")
	}
}

func TestEvalCompareErrors(t *testing.T) {
	row := sampleRow()
	_, err := evalCompare(row, plan.CompareExpr{Left: plan.ColumnRef{Name: "age"}, Op: "?", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(1)}}})
	if err == nil {
		t.Fatalf("expected operator error")
	}
	_, err = evalCompare(row, plan.CompareExpr{Left: badExpr{}, Op: "==", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(1)}}})
	if err == nil {
		t.Fatalf("expected eval error")
	}
	_, err = evalCompare(row, plan.CompareExpr{Left: plan.ColumnRef{Name: "age"}, Op: "==", Right: badExpr{}})
	if err == nil {
		t.Fatalf("expected eval error right")
	}
	_, err = evalExpr(row, badExpr{})
	if err == nil {
		t.Fatalf("expected expr error")
	}
	_, err = evalLogical(row, badExpr{})
	if err == nil {
		t.Fatalf("expected logical error")
	}
}

func TestEvalLogicalAndOr(t *testing.T) {
	row := sampleRow()
	left := plan.CompareExpr{Left: plan.ColumnRef{Name: "age"}, Op: ">", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(1)}}}
	right := plan.CompareExpr{Left: plan.ColumnRef{Name: "age"}, Op: "<", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(10)}}}
	expr := plan.LogicalExpr{Left: left, Op: "and", Right: right}
	ok, err := evalLogical(row, expr)
	if err != nil || !ok {
		t.Fatalf("expected true and")
	}
	rightFalse := plan.CompareExpr{Left: plan.ColumnRef{Name: "age"}, Op: ">", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(100)}}}
	exprFalse := plan.LogicalExpr{Left: left, Op: "and", Right: rightFalse}
	ok, err = evalLogical(row, exprFalse)
	if err != nil || ok {
		t.Fatalf("expected false and")
	}
	exprOr := plan.LogicalExpr{Left: left, Op: "or", Right: right}
	ok, err = evalLogical(row, exprOr)
	if err != nil || !ok {
		t.Fatalf("expected true or")
	}
	_, err = evalLogical(row, plan.LogicalExpr{Left: left, Op: "xor", Right: right})
	if err == nil {
		t.Fatalf("expected logical op error")
	}
}

func TestEvalLogicalShortCircuit(t *testing.T) {
	row := sampleRow()
	leftFalse := plan.CompareExpr{Left: plan.ColumnRef{Name: "age"}, Op: ">", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(100)}}}
	exprAnd := plan.LogicalExpr{Left: leftFalse, Op: "and", Right: badExpr{}}
	ok, err := evalLogical(row, exprAnd)
	if err != nil || ok {
		t.Fatalf("expected short-circuit false")
	}
	leftTrue := plan.CompareExpr{Left: plan.ColumnRef{Name: "age"}, Op: ">", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(1)}}}
	exprOr := plan.LogicalExpr{Left: leftTrue, Op: "or", Right: badExpr{}}
	ok, err = evalLogical(row, exprOr)
	if err != nil || !ok {
		t.Fatalf("expected short-circuit true")
	}
	exprOrEval := plan.LogicalExpr{Left: leftFalse, Op: "or", Right: leftTrue}
	ok, err = evalLogical(row, exprOrEval)
	if err != nil || !ok {
		t.Fatalf("expected or evaluation")
	}
	exprRightErr := plan.LogicalExpr{Left: leftTrue, Op: "and", Right: badExpr{}}
	if _, err := evalLogical(row, exprRightErr); err == nil {
		t.Fatalf("expected right error")
	}
	exprLeftErr := plan.LogicalExpr{Left: badExpr{}, Op: "and", Right: leftTrue}
	if _, err := evalLogical(row, exprLeftErr); err == nil {
		t.Fatalf("expected left error")
	}
}

func TestEvalLogicalCompareExpr(t *testing.T) {
	row := sampleRow()
	ok, err := evalLogical(row, plan.CompareExpr{Left: plan.ColumnRef{Name: "age"}, Op: "==", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(3)}}})
	if err != nil || !ok {
		t.Fatalf("expected compare expr true")
	}
}

func TestHelpers(t *testing.T) {
	if toInt64(model.Value{Type: model.TypeBool, V: true}) != 1 {
		t.Fatalf("toInt64 bool")
	}
	if toInt64(model.Value{Type: model.TypeBool, V: false}) != 0 {
		t.Fatalf("toInt64 bool false")
	}
	if toInt64(model.Value{Type: model.TypeFloat, V: 1.5}) != 1 {
		t.Fatalf("toInt64 float")
	}
	if toInt64(model.Value{Type: model.TypeInt, V: int64(2)}) != 2 {
		t.Fatalf("toInt64 int")
	}
	if toInt64(model.Value{Type: model.TypeString, V: "x"}) != 0 {
		t.Fatalf("toInt64 default")
	}
	if toFloat64(model.Value{Type: model.TypeBool, V: false}) != 0 {
		t.Fatalf("toFloat64 bool")
	}
	if toFloat64(model.Value{Type: model.TypeBool, V: true}) != 1 {
		t.Fatalf("toFloat64 bool true")
	}
	if toFloat64(model.Value{Type: model.TypeInt, V: int64(2)}) != 2 {
		t.Fatalf("toFloat64 int")
	}
	if toFloat64(model.Value{Type: model.TypeFloat, V: 1.25}) != 1.25 {
		t.Fatalf("toFloat64 float")
	}
	if toFloat64(model.Value{Type: model.TypeString, V: "x"}) != 0 {
		t.Fatalf("toFloat64 default")
	}
	if toBool(model.Value{Type: model.TypeString, V: "true"}) != true {
		t.Fatalf("toBool string")
	}
	if toBool(model.Value{Type: model.TypeInt, V: int64(0)}) != false {
		t.Fatalf("toBool int")
	}
	if toBool(model.Value{Type: model.TypeFloat, V: float64(0)}) != false {
		t.Fatalf("toBool float")
	}
	if !toTime(model.Value{Type: model.TypeDateTime, V: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}).Equal(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("toTime datetime")
	}
	if !toTime(model.Value{Type: model.TypeString, V: "x"}).IsZero() {
		t.Fatalf("toTime default")
	}
	if stringsJoin([]string{}, "|") != "" {
		t.Fatalf("stringsJoin empty")
	}
	if stringsJoin([]string{"a", "b"}, "|") != "a|b" {
		t.Fatalf("stringsJoin value")
	}
}

func TestEvalCompareOps(t *testing.T) {
	row := sampleRow()
	cmp := func(op string, right int64) bool {
		ok, err := evalCompare(row, plan.CompareExpr{Left: plan.ColumnRef{Name: "age"}, Op: op, Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: right}}})
		if err != nil {
			t.Fatalf("compare %s: %v", op, err)
		}
		return ok
	}
	if !cmp("==", 3) || cmp("!=", 3) {
		t.Fatalf("eq/ne failed")
	}
	if !cmp("!=", 4) {
		t.Fatalf("ne true failed")
	}
	if !cmp(">", 1) || !cmp(">=", 3) {
		t.Fatalf("gt/ge failed")
	}
	if !cmp("<", 5) || !cmp("<=", 3) {
		t.Fatalf("lt/le failed")
	}
	if ok, err := evalCompare(row, plan.CompareExpr{Left: plan.ColumnRef{Name: "age"}, Op: "=", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(3)}}}); err != nil || !ok {
		t.Fatalf("expected = compare")
	}
}

func TestOrderBy(t *testing.T) {
	op := &sliceOp{rows: []*csvio.Row{rowWithInt(2), rowWithInt(1)}}
	ord, err := NewOrderByOp(op, "n", false)
	if err != nil {
		t.Fatalf("orderby: %v", err)
	}
	row, _ := ord.Next()
	if row.Values[0].V.(int64) != 1 {
		t.Fatalf("expected asc")
	}
}

func TestOrderByDesc(t *testing.T) {
	op := &sliceOp{rows: []*csvio.Row{rowWithInt(1), rowWithInt(2)}}
	ord, err := NewOrderByOp(op, "n", true)
	if err != nil {
		t.Fatalf("orderby: %v", err)
	}
	row, _ := ord.Next()
	if row.Values[0].V.(int64) != 2 {
		t.Fatalf("expected desc")
	}
}

func TestOrderByEmpty(t *testing.T) {
	op := &sliceOp{rows: []*csvio.Row{}}
	ord, err := NewOrderByOp(op, "n", false)
	if err != nil {
		t.Fatalf("orderby: %v", err)
	}
	if _, err := ord.Next(); err != io.EOF {
		t.Fatalf("expected EOF")
	}
}

func TestOrderByInputError(t *testing.T) {
	_, err := NewOrderByOp(&errOp{err: errors.New("boom")}, "n", false)
	if err == nil {
		t.Fatalf("expected order by error")
	}
}

func TestSummarize(t *testing.T) {
	op := &sliceOp{rows: []*csvio.Row{rowWithInt(1), rowWithInt(1)}}
	sum, err := NewSummarizeOp(op, []string{"n"})
	if err != nil {
		t.Fatalf("summarize: %v", err)
	}
	row, err := sum.Next()
	if err != nil {
		t.Fatalf("sum next: %v", err)
	}
	if row.Values[1].V.(int64) != 2 {
		t.Fatalf("expected count 2")
	}
	if _, err := sum.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF")
	}
}

func TestSummarizeNoBy(t *testing.T) {
	op := &sliceOp{rows: []*csvio.Row{rowWithInt(1)}}
	sum, err := NewSummarizeOp(op, []string{})
	if err != nil {
		t.Fatalf("summarize: %v", err)
	}
	row, err := sum.Next()
	if err != nil {
		t.Fatalf("sum next: %v", err)
	}
	if row.Values[0].V.(int64) != 1 {
		t.Fatalf("expected count 1")
	}
}

func TestSummarizeInputError(t *testing.T) {
	_, err := NewSummarizeOp(&errOp{err: errors.New("boom")}, []string{"n"})
	if err == nil {
		t.Fatalf("expected summarize error")
	}
}

func TestFilterNoMatch(t *testing.T) {
	op := &sliceOp{rows: []*csvio.Row{rowWithInt(1)}}
	filter := FilterOp{In: op, Expr: plan.CompareExpr{Left: plan.ColumnRef{Name: "n"}, Op: ">", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(10)}}}}
	if _, err := filter.Next(); err != io.EOF {
		t.Fatalf("expected EOF")
	}
}

func TestFilterInputError(t *testing.T) {
	filter := FilterOp{In: &errOp{err: errors.New("boom")}, Expr: plan.CompareExpr{Left: plan.ColumnRef{Name: "n"}, Op: ">", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(10)}}}}
	if _, err := filter.Next(); err == nil {
		t.Fatalf("expected filter error")
	}
}

func TestFilterExprError(t *testing.T) {
	filter := FilterOp{In: &sliceOp{rows: []*csvio.Row{rowWithInt(1)}}, Expr: badExpr{}}
	if _, err := filter.Next(); err == nil {
		t.Fatalf("expected expr error")
	}
}

func TestProjectMissingColumn(t *testing.T) {
	op := &sliceOp{rows: []*csvio.Row{rowWithInt(1)}}
	proj := ProjectOp{In: op, Columns: []string{"missing"}}
	row, err := proj.Next()
	if err != nil {
		t.Fatalf("project: %v", err)
	}
	if len(row.Values) != 0 {
		t.Fatalf("expected empty projection")
	}
}

func TestExtendOp(t *testing.T) {
	op := &sliceOp{rows: []*csvio.Row{rowWithInt(1)}}
	ext := ExtendOp{In: op, Name: "x", Value: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(5)}}}
	row, err := ext.Next()
	if err != nil {
		t.Fatalf("extend: %v", err)
	}
	if row.Values[len(row.Values)-1].V.(int64) != 5 {
		t.Fatalf("extend value")
	}
}

func TestExtendOpError(t *testing.T) {
	op := &sliceOp{rows: []*csvio.Row{rowWithInt(1)}}
	ext := ExtendOp{In: op, Name: "x", Value: badExpr{}}
	if _, err := ext.Next(); err == nil {
		t.Fatalf("expected extend error")
	}
}

func TestExtendOpColumnRef(t *testing.T) {
	op := &sliceOp{rows: []*csvio.Row{rowWithInt(7)}}
	ext := ExtendOp{In: op, Name: "copy", Value: plan.ColumnRef{Name: "n"}}
	row, err := ext.Next()
	if err != nil {
		t.Fatalf("extend: %v", err)
	}
	if row.Values[len(row.Values)-1].V.(int64) != 7 {
		t.Fatalf("extend column ref")
	}
}

func TestExtendOpInputError(t *testing.T) {
	ext := ExtendOp{In: &errOp{err: errors.New("boom")}, Name: "x", Value: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(1)}}}
	if _, err := ext.Next(); err == nil {
		t.Fatalf("expected extend input error")
	}
}

func TestTake(t *testing.T) {
	op := &sliceOp{rows: []*csvio.Row{rowWithInt(1), rowWithInt(2)}}
	take := &TakeOp{In: op, Total: 1}
	if _, err := take.Next(); err != nil {
		t.Fatalf("take: %v", err)
	}
	if _, err := take.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF")
	}
}

func TestTakeInputError(t *testing.T) {
	take := &TakeOp{In: &errOp{err: errors.New("boom")}, Total: 1}
	if _, err := take.Next(); err == nil {
		t.Fatalf("expected take error")
	}
}

func TestBuildPipelineUnsupported(t *testing.T) {
	reader, err := csvio.NewReader("../../testdata/sample.csv", nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	_, err = BuildPipeline(reader, []plan.Operator{badOp{}})
	if err == nil {
		t.Fatalf("expected unsupported error")
	}
}

func TestBuildPipelineAllOps(t *testing.T) {
	reader, err := csvio.NewReader("../../testdata/sample.csv", nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	ops := []plan.Operator{
		plan.WhereOp{Predicate: plan.CompareExpr{Left: plan.ColumnRef{Name: "age"}, Op: ">", Right: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(0)}}}},
		plan.ExtendOp{Name: "x", Value: plan.Literal{Value: model.Value{Type: model.TypeInt, V: int64(1)}}},
		plan.ProjectOp{Columns: []string{"name", "age"}},
		plan.TakeOp{Count: 1},
	}
	pipe, err := BuildPipeline(reader, ops)
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if _, err := pipe.Next(); err != nil {
		t.Fatalf("next: %v", err)
	}
}

func TestBuildPipelineOrderBySummarize(t *testing.T) {
	reader, err := csvio.NewReader("../../testdata/sample.csv", nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	ops := []plan.Operator{
		plan.OrderByOp{Column: "age", Desc: true},
		plan.SummarizeOp{ByColumns: []string{"active"}},
	}
	pipe, err := BuildPipeline(reader, ops)
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if _, err := pipe.Next(); err != nil {
		t.Fatalf("next: %v", err)
	}
}

func TestBuildPipelineOrderByError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.csv")
	if err := os.WriteFile(path, []byte("n\nx\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	sch := model.NewSchema([]model.Column{{Name: "n", Type: model.TypeInt}})
	reader, err := csvio.NewReader(path, &sch)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	ops := []plan.Operator{plan.OrderByOp{Column: "n"}}
	if _, err := BuildPipeline(reader, ops); err == nil {
		t.Fatalf("expected orderby error")
	}
}

func TestBuildPipelineSummarizeError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.csv")
	if err := os.WriteFile(path, []byte("n\nx\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	sch := model.NewSchema([]model.Column{{Name: "n", Type: model.TypeInt}})
	reader, err := csvio.NewReader(path, &sch)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	ops := []plan.Operator{plan.SummarizeOp{ByColumns: []string{"n"}}}
	if _, err := BuildPipeline(reader, ops); err == nil {
		t.Fatalf("expected summarize error")
	}
}

func TestBuildPipelineJoin(t *testing.T) {
	reader, err := csvio.NewReader("../../testdata/join_left.csv", nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	ops := []plan.Operator{
		plan.JoinOp{Kind: "inner", Right: "../../testdata/join_right.csv", LeftKey: "dept_id", RightKey: "dept_id"},
	}
	pipe, err := BuildPipeline(reader, ops)
	if err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if _, err := pipe.Next(); err != nil {
		t.Fatalf("next: %v", err)
	}
}

func TestBuildPipelineJoinError(t *testing.T) {
	reader, err := csvio.NewReader("../../testdata/join_left.csv", nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	ops := []plan.Operator{
		plan.JoinOp{Kind: "inner", Right: "missing.csv", LeftKey: "dept_id", RightKey: "dept_id"},
	}
	if _, err := BuildPipeline(reader, ops); err == nil {
		t.Fatalf("expected join error")
	}
}

func TestNewJoinOpParseError(t *testing.T) {
	rightPath := filepath.Join(t.TempDir(), "right.csv")
	if err := os.WriteFile(rightPath, []byte("id\n\"unterminated\n"), 0644); err != nil {
		t.Fatalf("write right: %v", err)
	}
	if _, err := NewJoinOp(&sliceOp{}, rightPath, "id", "id"); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestNewJoinOpEmptyRight(t *testing.T) {
	rightPath := filepath.Join(t.TempDir(), "right.csv")
	if err := os.WriteFile(rightPath, []byte("id\n"), 0644); err != nil {
		t.Fatalf("write right: %v", err)
	}
	join, err := NewJoinOp(&sliceOp{}, rightPath, "id", "id")
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if len(join.Right) != 0 {
		t.Fatalf("expected empty right map")
	}
}

func TestNewJoinOpLoopError(t *testing.T) {
	rightPath := filepath.Join(t.TempDir(), "right.csv")
	var b strings.Builder
	b.WriteString("id\n")
	for i := 0; i < 100; i++ {
		b.WriteString("1\n")
	}
	b.WriteString("\"unterminated\n")
	if err := os.WriteFile(rightPath, []byte(b.String()), 0644); err != nil {
		t.Fatalf("write right: %v", err)
	}
	if _, err := NewJoinOp(&sliceOp{}, rightPath, "id", "id"); err == nil {
		t.Fatalf("expected loop parse error")
	}
}

func TestNewJoinOpError(t *testing.T) {
	if _, err := NewJoinOp(&sliceOp{}, "missing.csv", "a", "b"); err == nil {
		t.Fatalf("expected join error")
	}
}

func TestJoinColumnPrefix(t *testing.T) {
	leftPath := filepath.Join(t.TempDir(), "left.csv")
	rightPath := filepath.Join(t.TempDir(), "right.csv")
	if err := os.WriteFile(leftPath, []byte("id,name\n1,a\n"), 0644); err != nil {
		t.Fatalf("write left: %v", err)
	}
	if err := os.WriteFile(rightPath, []byte("id,dept\n1,x\n"), 0644); err != nil {
		t.Fatalf("write right: %v", err)
	}

	reader, err := csvio.NewReader(leftPath, nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	join, err := NewJoinOp(SourceOp{Reader: reader}, rightPath, "id", "id")
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	row, err := join.Next()
	if err != nil {
		t.Fatalf("join next: %v", err)
	}
	if _, ok := row.Schema.Index["right.id"]; !ok {
		t.Fatalf("expected right.id column")
	}
}

func TestJoinNoMatch(t *testing.T) {
	leftPath := filepath.Join(t.TempDir(), "left.csv")
	rightPath := filepath.Join(t.TempDir(), "right.csv")
	if err := os.WriteFile(leftPath, []byte("id\n1\n"), 0644); err != nil {
		t.Fatalf("write left: %v", err)
	}
	if err := os.WriteFile(rightPath, []byte("id\n2\n"), 0644); err != nil {
		t.Fatalf("write right: %v", err)
	}

	reader, err := csvio.NewReader(leftPath, nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	join, err := NewJoinOp(SourceOp{Reader: reader}, rightPath, "id", "id")
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if _, err := join.Next(); err != io.EOF {
		t.Fatalf("expected EOF")
	}
}

func rowWithInt(n int64) *csvio.Row {
	schema := model.NewSchema([]model.Column{{Name: "n", Type: model.TypeInt}})
	return &csvio.Row{Schema: schema, Values: []model.Value{{Type: model.TypeInt, V: n}}}
}

func sampleRow() *csvio.Row {
	schema := model.NewSchema([]model.Column{{Name: "age", Type: model.TypeInt}})
	return &csvio.Row{Schema: schema, Values: []model.Value{{Type: model.TypeInt, V: int64(3)}}}
}
