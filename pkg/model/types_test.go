package model

import (
	"testing"
	"time"
)

func TestParseValue(t *testing.T) {
	if v, err := ParseValue(TypeInt, "10"); err != nil || v.V.(int64) != 10 {
		t.Fatalf("int parse failed: %v", err)
	}
	if v, err := ParseValue(TypeFloat, "1.5"); err != nil || v.V.(float64) != 1.5 {
		t.Fatalf("float parse failed: %v", err)
	}
	if v, err := ParseValue(TypeBool, "true"); err != nil || v.V.(bool) != true {
		t.Fatalf("bool parse failed: %v", err)
	}
	tm := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if v, err := ParseValue(TypeDateTime, tm.Format(time.RFC3339)); err != nil || v.V.(time.Time) != tm {
		t.Fatalf("time parse failed: %v", err)
	}
	if v, err := ParseValue(TypeString, " x "); err != nil || v.V.(string) != "x" {
		t.Fatalf("string parse failed: %v", err)
	}
}

func TestParseValueErrors(t *testing.T) {
	if _, err := ParseValue(TypeInt, "x"); err == nil {
		t.Fatalf("expected int error")
	}
	if _, err := ParseValue(TypeFloat, "x"); err == nil {
		t.Fatalf("expected float error")
	}
	if _, err := ParseValue(TypeBool, "x"); err == nil {
		t.Fatalf("expected bool error")
	}
	if _, err := ParseValue(TypeDateTime, "x"); err == nil {
		t.Fatalf("expected time error")
	}
}

func TestInferType(t *testing.T) {
	if InferType([]string{}) != TypeString {
		t.Fatalf("empty should be string")
	}
	if InferType([]string{"1", "2"}) != TypeInt {
		t.Fatalf("expected int")
	}
	if InferType([]string{"1.1", "2.2"}) != TypeFloat {
		t.Fatalf("expected float")
	}
	if InferType([]string{"true", "false"}) != TypeBool {
		t.Fatalf("expected bool")
	}
	if InferType([]string{"2024-01-01T00:00:00Z"}) != TypeDateTime {
		t.Fatalf("expected datetime")
	}
	if InferType([]string{"x", "2"}) != TypeString {
		t.Fatalf("expected string")
	}
	if InferType([]string{""}) != TypeInt {
		t.Fatalf("expected int from empty values")
	}
}

func TestValueString(t *testing.T) {
	if (Value{Type: TypeString, V: "x"}).String() != "x" {
		t.Fatalf("string failed")
	}
	if (Value{Type: TypeInt, V: int64(2)}).String() != "2" {
		t.Fatalf("int failed")
	}
	if (Value{Type: TypeFloat, V: float64(1.5)}).String() == "" {
		t.Fatalf("float failed")
	}
	if (Value{Type: TypeBool, V: true}).String() != "true" {
		t.Fatalf("bool failed")
	}
	if (Value{Type: TypeBool, V: false}).String() != "false" {
		t.Fatalf("bool false failed")
	}
	tm := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	if (Value{Type: TypeDateTime, V: tm}).String() == "" {
		t.Fatalf("time failed")
	}
	if (Value{Type: Type("other"), V: 1}).String() == "" {
		t.Fatalf("default failed")
	}
}

func TestNewSchema(t *testing.T) {
	sch := NewSchema([]Column{{Name: "a", Type: TypeString}})
	if sch.Index["a"] != 0 {
		t.Fatalf("schema index failed")
	}
}
