package model

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Type string

const (
	TypeString   Type = "string"
	TypeInt      Type = "int"
	TypeFloat    Type = "float"
	TypeBool     Type = "bool"
	TypeDateTime Type = "datetime"
)

type Column struct {
	Name string
	Type Type
}

type Schema struct {
	Columns []Column
	Index   map[string]int
}

func NewSchema(cols []Column) Schema {
	idx := make(map[string]int, len(cols))
	for i, c := range cols {
		idx[c.Name] = i
	}
	return Schema{Columns: cols, Index: idx}
}

type Value struct {
	Type Type
	V    any
}

func (v Value) String() string {
	switch v.Type {
	case TypeString:
		return v.V.(string)
	case TypeInt:
		return strconv.FormatInt(v.V.(int64), 10)
	case TypeFloat:
		return strconv.FormatFloat(v.V.(float64), 'f', -1, 64)
	case TypeBool:
		if v.V.(bool) {
			return "true"
		}
		return "false"
	case TypeDateTime:
		return v.V.(time.Time).Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", v.V)
	}
}

func ParseValue(t Type, raw string) (Value, error) {
	s := strings.TrimSpace(raw)
	switch t {
	case TypeInt:
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeInt, V: v}, nil
	case TypeFloat:
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeFloat, V: v}, nil
	case TypeBool:
		v, err := strconv.ParseBool(s)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeBool, V: v}, nil
	case TypeDateTime:
		v, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return Value{}, err
		}
		return Value{Type: TypeDateTime, V: v}, nil
	default:
		return Value{Type: TypeString, V: s}, nil
	}
}

func InferType(values []string) Type {
	if len(values) == 0 {
		return TypeString
	}

	isInt := true
	isFloat := true
	isBool := true
	isTime := true

	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if isInt {
			if _, err := strconv.ParseInt(v, 10, 64); err != nil {
				isInt = false
			}
		}
		if isFloat {
			if _, err := strconv.ParseFloat(v, 64); err != nil {
				isFloat = false
			}
		}
		if isBool {
			if _, err := strconv.ParseBool(v); err != nil {
				isBool = false
			}
		}
		if isTime {
			if _, err := time.Parse(time.RFC3339, v); err != nil {
				isTime = false
			}
		}
	}

	if isInt {
		return TypeInt
	}
	if isFloat {
		return TypeFloat
	}
	if isBool {
		return TypeBool
	}
	if isTime {
		return TypeDateTime
	}
	return TypeString
}
