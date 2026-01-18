package parser

import (
	"fmt"
	"strconv"
	"strings"

	"kqlfile/pkg/model"
	"kqlfile/pkg/plan"
)

func Parse(query string) ([]plan.Operator, error) {
	parts := strings.Split(query, "|")
	ops := make([]plan.Operator, 0, len(parts))
	for i, p := range parts {
		seg := strings.TrimSpace(p)
		if seg == "" {
			continue
		}
		if i == 0 {
			fields := strings.Fields(seg)
			if len(fields) == 1 && !isOperator(fields[0]) {
				continue
			}
		}
		op, err := parseOperator(seg)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	if len(ops) == 0 {
		return nil, fmt.Errorf("empty query")
	}
	return ops, nil
}

func isOperator(tok string) bool {
	switch strings.ToLower(tok) {
	case "where", "project", "extend", "summarize", "take", "order", "join":
		return true
	default:
		return false
	}
}

func parseOperator(seg string) (plan.Operator, error) {
	fields := strings.Fields(seg)
	if len(fields) == 0 {
		return nil, fmt.Errorf("empty segment")
	}
	switch strings.ToLower(fields[0]) {
	case "where":
		return parseWhere(seg)
	case "project":
		return parseProject(seg)
	case "extend":
		return parseExtend(seg)
	case "summarize":
		return parseSummarize(seg)
	case "take":
		return parseTake(seg)
	case "order":
		return parseOrderBy(seg)
	case "join":
		return parseJoin(seg)
	default:
		return nil, fmt.Errorf("unknown operator: %s", fields[0])
	}
}

func parseWhere(seg string) (plan.Operator, error) {
	body := strings.TrimSpace(strings.TrimPrefix(seg, "where"))
	if body == "" {
		return nil, fmt.Errorf("where requires an expression")
	}
	expr, err := parseLogicalExpr(body)
	if err != nil {
		return nil, err
	}
	return plan.WhereOp{Predicate: expr}, nil
}

func parseProject(seg string) (plan.Operator, error) {
	body := strings.TrimSpace(strings.TrimPrefix(seg, "project"))
	cols := splitCSVList(body)
	if len(cols) == 0 {
		return nil, fmt.Errorf("project requires columns")
	}
	return plan.ProjectOp{Columns: cols}, nil
}

func parseExtend(seg string) (plan.Operator, error) {
	body := strings.TrimSpace(strings.TrimPrefix(seg, "extend"))
	parts := strings.SplitN(body, "=", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("extend requires name = value")
	}
	name := strings.TrimSpace(parts[0])
	valueExpr, err := parseLiteralOrColumn(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, err
	}
	return plan.ExtendOp{Name: name, Value: valueExpr}, nil
}

func parseSummarize(seg string) (plan.Operator, error) {
	body := strings.TrimSpace(strings.TrimPrefix(seg, "summarize"))
	if !strings.HasPrefix(strings.ToLower(body), "count()") {
		return nil, fmt.Errorf("only count() supported in summarize")
	}
	body = strings.TrimSpace(body[len("count()"):])
	if strings.HasPrefix(strings.ToLower(body), "by") {
		body = strings.TrimSpace(body[2:])
	}
	cols := splitCSVList(body)
	return plan.SummarizeOp{ByColumns: cols}, nil
}

func parseTake(seg string) (plan.Operator, error) {
	parts := strings.Fields(seg)
	if len(parts) != 2 {
		return nil, fmt.Errorf("take requires a count")
	}
	n, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid take count: %w", err)
	}
	return plan.TakeOp{Count: n}, nil
}

func parseOrderBy(seg string) (plan.Operator, error) {
	lower := strings.ToLower(seg)
	if !strings.HasPrefix(lower, "order by") {
		return nil, fmt.Errorf("invalid order by")
	}
	body := strings.TrimSpace(seg[len("order by"):])
	parts := strings.Fields(body)
	if len(parts) == 0 {
		return nil, fmt.Errorf("order by requires a column")
	}
	col := parts[0]
	desc := false
	if len(parts) > 1 {
		switch strings.ToLower(parts[1]) {
		case "desc":
			desc = true
		case "asc":
			desc = false
		default:
			return nil, fmt.Errorf("order by direction must be asc or desc")
		}
	}
	return plan.OrderByOp{Column: col, Desc: desc}, nil
}

func parseJoin(seg string) (plan.Operator, error) {
	lower := strings.ToLower(seg)
	if !strings.HasPrefix(lower, "join") {
		return nil, fmt.Errorf("invalid join")
	}
	openIdx := strings.Index(seg, "(")
	closeIdx := strings.LastIndex(seg, ")")
	if openIdx == -1 || closeIdx == -1 || closeIdx <= openIdx {
		return nil, fmt.Errorf("join requires right input in parentheses")
	}
	rightPath := strings.TrimSpace(seg[openIdx+1 : closeIdx])
	if rightPath == "" {
		return nil, fmt.Errorf("join requires right input path")
	}
	kind := "inner"
	joinHead := strings.TrimSpace(seg[:openIdx])
	if strings.Contains(strings.ToLower(joinHead), "kind=") {
		parts := strings.Fields(joinHead)
		for _, p := range parts {
			if strings.HasPrefix(strings.ToLower(p), "kind=") {
				kind = strings.ToLower(strings.TrimPrefix(p, "kind="))
			}
		}
	}
	rest := strings.TrimSpace(seg[closeIdx+1:])
	restLower := strings.ToLower(rest)
	if !strings.HasPrefix(restLower, "on ") {
		return nil, fmt.Errorf("join requires on clause")
	}
	tokens := strings.Fields(rest[3:])
	if len(tokens) < 3 {
		return nil, fmt.Errorf("join on requires left op right")
	}
	leftKey := tokens[0]
	op := tokens[1]
	rightKey := tokens[2]
	if op != "==" && op != "=" {
		return nil, fmt.Errorf("join on only supports = or ==")
	}
	if kind != "inner" {
		return nil, fmt.Errorf("only inner join supported")
	}
	return plan.JoinOp{Kind: kind, Right: rightPath, LeftKey: leftKey, RightKey: rightKey}, nil
}

func parseLiteralOrColumn(raw string) (plan.Expr, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty literal or column")
	}
	if strings.HasPrefix(raw, "\"") && strings.HasSuffix(raw, "\"") {
		return plan.Literal{Value: model.Value{Type: model.TypeString, V: strings.Trim(raw, "\"")}}, nil
	}
	if strings.HasPrefix(raw, "'") && strings.HasSuffix(raw, "'") {
		return plan.Literal{Value: model.Value{Type: model.TypeString, V: strings.Trim(raw, "'")}}, nil
	}
	if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return plan.Literal{Value: model.Value{Type: model.TypeInt, V: v}}, nil
	}
	if v, err := strconv.ParseFloat(raw, 64); err == nil {
		return plan.Literal{Value: model.Value{Type: model.TypeFloat, V: v}}, nil
	}
	if v, err := strconv.ParseBool(raw); err == nil {
		return plan.Literal{Value: model.Value{Type: model.TypeBool, V: v}}, nil
	}
	return plan.ColumnRef{Name: raw}, nil
}

func splitCSVList(body string) []string {
	body = strings.TrimSpace(body)
	if body == "" {
		return nil
	}
	parts := strings.Split(body, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		name := strings.TrimSpace(p)
		if name != "" {
			out = append(out, name)
		}
	}
	return out
}

func parseLogicalExpr(body string) (plan.Expr, error) {
	tokens := strings.Fields(body)
	if len(tokens) < 3 {
		return nil, fmt.Errorf("invalid expression: %s", body)
	}
	left, err := parseCompare(tokens[:3])
	if err != nil {
		return nil, err
	}
	expr := left
	rest := tokens[3:]
	for len(rest) > 0 {
		if len(rest) < 4 {
			return nil, fmt.Errorf("invalid logical expression: %s", body)
		}
		op := strings.ToLower(rest[0])
		if op != "and" && op != "or" {
			return nil, fmt.Errorf("expected logical operator, got %s", rest[0])
		}
		right, err := parseCompare(rest[1:4])
		if err != nil {
			return nil, err
		}
		expr = plan.LogicalExpr{Left: expr, Op: op, Right: right}
		rest = rest[4:]
	}
	return expr, nil
}

func parseCompare(tokens []string) (plan.Expr, error) {
	if len(tokens) < 3 {
		return nil, fmt.Errorf("invalid comparison")
	}
	left := plan.ColumnRef{Name: tokens[0]}
	op := tokens[1]
	switch op {
	case "==", "=", "!=", ">", ">=", "<", "<=":
	default:
		return nil, fmt.Errorf("invalid comparison operator")
	}
	right, err := parseLiteralOrColumn(strings.Join(tokens[2:], " "))
	if err != nil {
		return nil, err
	}
	return plan.CompareExpr{Left: left, Op: op, Right: right}, nil
}
