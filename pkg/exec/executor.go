package exec

import (
	"errors"
	"io"
	"sort"
	"time"

	"kqlfile/pkg/csvio"
	"kqlfile/pkg/model"
	"kqlfile/pkg/plan"
)

type Operator interface {
	Next() (*csvio.Row, error)
}

type RowReader interface {
	Next() (*csvio.Row, error)
}

type SourceOp struct {
	Reader RowReader
}

func (s SourceOp) Next() (*csvio.Row, error) {
	return s.Reader.Next()
}

type FilterOp struct {
	In   Operator
	Expr plan.Expr
}

func (f FilterOp) Next() (*csvio.Row, error) {
	for {
		row, err := f.In.Next()
		if err != nil {
			return nil, err
		}
		ok, err := evalLogical(row, f.Expr)
		if err != nil {
			return nil, err
		}
		if ok {
			return row, nil
		}
	}
}

type ProjectOp struct {
	In      Operator
	Columns []string
}

func (p ProjectOp) Next() (*csvio.Row, error) {
	row, err := p.In.Next()
	if err != nil {
		return nil, err
	}
	cols := make([]model.Column, 0, len(p.Columns))
	vals := make([]model.Value, 0, len(p.Columns))
	for _, name := range p.Columns {
		v, ok := row.Get(name)
		if !ok {
			continue
		}
		cols = append(cols, model.Column{Name: name, Type: v.Type})
		vals = append(vals, v)
	}
	return &csvio.Row{Schema: model.NewSchema(cols), Values: vals}, nil
}

type ExtendOp struct {
	In    Operator
	Name  string
	Value plan.Expr
}

func (e ExtendOp) Next() (*csvio.Row, error) {
	row, err := e.In.Next()
	if err != nil {
		return nil, err
	}
	val, err := evalExpr(row, e.Value)
	if err != nil {
		return nil, err
	}
	cols := append([]model.Column(nil), row.Schema.Columns...)
	cols = append(cols, model.Column{Name: e.Name, Type: val.Type})
	vals := append([]model.Value(nil), row.Values...)
	vals = append(vals, val)
	return &csvio.Row{Schema: model.NewSchema(cols), Values: vals}, nil
}

type TakeOp struct {
	In    Operator
	Left  int
	Total int
}

func (t *TakeOp) Next() (*csvio.Row, error) {
	if t.Left >= t.Total {
		return nil, io.EOF
	}
	row, err := t.In.Next()
	if err != nil {
		return nil, err
	}
	t.Left++
	return row, nil
}

type OrderByOp struct {
	rows []*csvio.Row
	idx  int
}

func NewOrderByOp(in Operator, column string, desc bool) (OrderByOp, error) {
	rows := make([]*csvio.Row, 0)
	for {
		row, err := in.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return OrderByOp{}, err
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return OrderByOp{rows: rows}, nil
	}
	sort.Slice(rows, func(i, j int) bool {
		vi, _ := rows[i].Get(column)
		vj, _ := rows[j].Get(column)
		less := compareValues(vi, vj) < 0
		if desc {
			return !less
		}
		return less
	})
	return OrderByOp{rows: rows}, nil
}

func (o *OrderByOp) Next() (*csvio.Row, error) {
	if o.idx >= len(o.rows) {
		return nil, io.EOF
	}
	row := o.rows[o.idx]
	o.idx++
	return row, nil
}

type SummarizeOp struct {
	rows []*csvio.Row
	idx  int
}

func NewSummarizeOp(in Operator, by []string) (SummarizeOp, error) {
	counts := make(map[string]int)
	keys := make(map[string][]model.Value)
	for {
		row, err := in.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return SummarizeOp{}, err
		}
		vals := make([]model.Value, 0, len(by))
		keyParts := make([]string, 0, len(by))
		for _, name := range by {
			v, _ := row.Get(name)
			vals = append(vals, v)
			keyParts = append(keyParts, v.String())
		}
		k := stringsJoin(keyParts, "|")
		counts[k]++
		if _, ok := keys[k]; !ok {
			keys[k] = vals
		}
	}
	rows := make([]*csvio.Row, 0, len(counts))
	for k, cnt := range counts {
		_ = k
		vals := keys[k]
		cols := make([]model.Column, 0, len(by)+1)
		for i, name := range by {
			cols = append(cols, model.Column{Name: name, Type: vals[i].Type})
		}
		cols = append(cols, model.Column{Name: "count", Type: model.TypeInt})
		vals = append(vals, model.Value{Type: model.TypeInt, V: int64(cnt)})
		rows = append(rows, &csvio.Row{Schema: model.NewSchema(cols), Values: vals})
	}
	return SummarizeOp{rows: rows}, nil
}

func (s *SummarizeOp) Next() (*csvio.Row, error) {
	if s.idx >= len(s.rows) {
		return nil, io.EOF
	}
	row := s.rows[s.idx]
	s.idx++
	return row, nil
}

func BuildPipeline(reader RowReader, ops []plan.Operator) (Operator, error) {
	var current Operator = SourceOp{Reader: reader}
	for _, op := range ops {
		switch o := op.(type) {
		case plan.WhereOp:
			current = FilterOp{In: current, Expr: o.Predicate}
		case plan.ProjectOp:
			current = ProjectOp{In: current, Columns: o.Columns}
		case plan.ExtendOp:
			current = ExtendOp{In: current, Name: o.Name, Value: o.Value}
		case plan.TakeOp:
			current = &TakeOp{In: current, Total: o.Count}
		case plan.OrderByOp:
			ord, err := NewOrderByOp(current, o.Column, o.Desc)
			if err != nil {
				return nil, err
			}
			current = &ord
		case plan.SummarizeOp:
			sum, err := NewSummarizeOp(current, o.ByColumns)
			if err != nil {
				return nil, err
			}
			current = &sum
		case plan.JoinOp:
			join, err := NewJoinOp(current, o.Right, o.LeftKey, o.RightKey)
			if err != nil {
				return nil, err
			}
			current = join
		default:
			return nil, errors.New("unsupported operator")
		}
	}
	return current, nil
}

func evalExpr(row *csvio.Row, expr plan.Expr) (model.Value, error) {
	switch e := expr.(type) {
	case plan.ColumnRef:
		v, _ := row.Get(e.Name)
		return v, nil
	case plan.Literal:
		return e.Value, nil
	default:
		return model.Value{}, errors.New("unsupported expression")
	}
}

func evalCompare(row *csvio.Row, cmp plan.CompareExpr) (bool, error) {
	l, err := evalExpr(row, cmp.Left)
	if err != nil {
		return false, err
	}
	r, err := evalExpr(row, cmp.Right)
	if err != nil {
		return false, err
	}
	c := compareValues(l, r)
	switch cmp.Op {
	case "==", "=":
		return c == 0, nil
	case "!=":
		return c != 0, nil
	case ">":
		return c > 0, nil
	case ">=":
		return c >= 0, nil
	case "<":
		return c < 0, nil
	case "<=":
		return c <= 0, nil
	default:
		return false, errors.New("unsupported operator")
	}
}

func evalLogical(row *csvio.Row, expr plan.Expr) (bool, error) {
	switch e := expr.(type) {
	case plan.CompareExpr:
		return evalCompare(row, e)
	case plan.LogicalExpr:
		left, err := evalLogical(row, e.Left)
		if err != nil {
			return false, err
		}
		if e.Op == "and" && !left {
			return false, nil
		}
		if e.Op == "or" && left {
			return true, nil
		}
		right, err := evalLogical(row, e.Right)
		if err != nil {
			return false, err
		}
		if e.Op == "and" {
			return left && right, nil
		}
		if e.Op == "or" {
			return left || right, nil
		}
		return false, errors.New("unsupported logical operator")
	default:
		return false, errors.New("unsupported expression")
	}
}

func compareValues(a, b model.Value) int {
	switch a.Type {
	case model.TypeInt:
		ai := a.V.(int64)
		bi := toInt64(b)
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
		return 0
	case model.TypeFloat:
		af := a.V.(float64)
		bf := toFloat64(b)
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	case model.TypeBool:
		ab := a.V.(bool)
		bb := toBool(b)
		if !ab && bb {
			return -1
		}
		if ab && !bb {
			return 1
		}
		return 0
	case model.TypeDateTime:
		at := a.V.(time.Time)
		bt := toTime(b)
		if at.Before(bt) {
			return -1
		}
		if at.After(bt) {
			return 1
		}
		return 0
	default:
		as := a.String()
		bs := b.String()
		if as < bs {
			return -1
		}
		if as > bs {
			return 1
		}
		return 0
	}
}

func toInt64(v model.Value) int64 {
	switch v.Type {
	case model.TypeInt:
		return v.V.(int64)
	case model.TypeFloat:
		return int64(v.V.(float64))
	case model.TypeBool:
		if v.V.(bool) {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func toFloat64(v model.Value) float64 {
	switch v.Type {
	case model.TypeFloat:
		return v.V.(float64)
	case model.TypeInt:
		return float64(v.V.(int64))
	case model.TypeBool:
		if v.V.(bool) {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func toBool(v model.Value) bool {
	switch v.Type {
	case model.TypeBool:
		return v.V.(bool)
	case model.TypeInt:
		return v.V.(int64) != 0
	case model.TypeFloat:
		return v.V.(float64) != 0
	default:
		return v.String() == "true"
	}
}

func toTime(v model.Value) time.Time {
	if v.Type == model.TypeDateTime {
		return v.V.(time.Time)
	}
	return time.Time{}
}

func stringsJoin(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for i := 1; i < len(parts); i++ {
		out += sep + parts[i]
	}
	return out
}

type JoinOp struct {
	In          Operator
	Right       map[string][]*csvio.Row
	RightSchema model.Schema
	LeftKey     string
	RightKey    string
	pending     []*csvio.Row
	pendingIdx  int
}

func NewJoinOp(in Operator, rightPath, leftKey, rightKey string) (*JoinOp, error) {
	reader, err := csvio.NewReader(rightPath, nil)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	rightSchema := reader.Schema()
	rightRows := make(map[string][]*csvio.Row)
	for {
		row, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		v, _ := row.Get(rightKey)
		key := v.String()
		rightRows[key] = append(rightRows[key], row)
	}

	return &JoinOp{
		In:          in,
		Right:       rightRows,
		RightSchema: rightSchema,
		LeftKey:     leftKey,
		RightKey:    rightKey,
	}, nil
}

func (j *JoinOp) Next() (*csvio.Row, error) {
	for {
		if j.pendingIdx < len(j.pending) {
			row := j.pending[j.pendingIdx]
			j.pendingIdx++
			return row, nil
		}
		j.pending = nil
		j.pendingIdx = 0

		leftRow, err := j.In.Next()
		if err != nil {
			return nil, err
		}
		lv, _ := leftRow.Get(j.LeftKey)
		matches := j.Right[lv.String()]
		if len(matches) == 0 {
			continue
		}
		j.pending = buildJoinedRows(leftRow, matches, j.RightSchema)
	}
}

func buildJoinedRows(left *csvio.Row, rights []*csvio.Row, rightSchema model.Schema) []*csvio.Row {
	cols := make([]model.Column, 0, len(left.Schema.Columns)+len(rightSchema.Columns))
	cols = append(cols, left.Schema.Columns...)
	for _, c := range rightSchema.Columns {
		name := c.Name
		if _, ok := left.Schema.Index[name]; ok {
			name = "right." + name
		}
		cols = append(cols, model.Column{Name: name, Type: c.Type})
	}
	joinedSchema := model.NewSchema(cols)

	rows := make([]*csvio.Row, 0, len(rights))
	for _, r := range rights {
		vals := make([]model.Value, 0, len(cols))
		vals = append(vals, left.Values...)
		for i := range rightSchema.Columns {
			vals = append(vals, r.Values[i])
		}
		rows = append(rows, &csvio.Row{Schema: joinedSchema, Values: vals})
	}
	return rows
}
