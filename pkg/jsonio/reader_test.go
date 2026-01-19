package jsonio

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kqlfile/pkg/model"
)

func TestJSONReaderInfer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	data := "{\"name\":\"alice\",\"age\":30}\n{\"name\":\"bob\",\"age\":41}\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	reader, err := NewReader(path, nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()
	ageIdx, ok := reader.Schema().Index["age"]
	if !ok {
		t.Fatalf("missing age column")
	}
	if reader.Schema().Columns[ageIdx].Type != model.TypeInt {
		t.Fatalf("expected int age")
	}
}

func TestJSONReaderInferBlankLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	data := "\n{\"name\":\"alice\",\"age\":30}\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	reader, err := NewReader(path, nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()
	if _, ok := reader.Schema().Index["name"]; !ok {
		t.Fatalf("missing name column")
	}
}

func TestJSONReaderSchemaOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	data := "{\"name\":\"alice\",\"age\":30}\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	sch := model.NewSchema([]model.Column{{Name: "age", Type: model.TypeInt}})
	reader, err := NewReader(path, &sch)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()
	row, err := reader.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if row.Values[0].String() != "30" {
		t.Fatalf("expected 30")
	}
}

func TestJSONReaderErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	data := "notjson\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	reader, err := NewReader(path, nil)
	if err == nil {
		reader.Close()
		t.Fatalf("expected parse error")
	}
}

func TestJSONReaderParseError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	data := "{\"age\":\"x\"}\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	sch := model.NewSchema([]model.Column{{Name: "age", Type: model.TypeInt}})
	reader, err := NewReader(path, &sch)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()
	if _, err := reader.Next(); err == nil {
		t.Fatalf("expected value parse error")
	}
}

func TestJSONReaderBlankLinesAndEOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	data := "\n{\"a\":1}\n\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	sch := model.NewSchema([]model.Column{{Name: "a", Type: model.TypeInt}})
	reader, err := NewReader(path, &sch)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()
	if _, err := reader.Next(); err != nil {
		t.Fatalf("next: %v", err)
	}
	if _, err := reader.Next(); err == nil {
		t.Fatalf("expected EOF")
	}
}

func TestJSONReaderBufferUse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	data := "{\"a\":1}\n{\"a\":2}\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	reader, err := NewReader(path, nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()
	if _, err := reader.Next(); err != nil {
		t.Fatalf("next: %v", err)
	}
	if _, err := reader.Next(); err != nil {
		t.Fatalf("next: %v", err)
	}
}

func TestParseJSONLineNotObject(t *testing.T) {
	if _, err := parseJSONLine("[]"); err == nil {
		t.Fatalf("expected object error")
	}
}

func TestParseJSONLineNull(t *testing.T) {
	if _, err := parseJSONLine("null"); err == nil {
		t.Fatalf("expected null object error")
	}
}

func TestJSONReaderNextParseError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	var b strings.Builder
	for i := 0; i < 100; i++ {
		b.WriteString("{\"a\":1}\n")
	}
	b.WriteString("notjson\n")
	data := b.String()
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	reader, err := NewReader(path, nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()
	for i := 0; i < 100; i++ {
		if _, err := reader.Next(); err != nil {
			t.Fatalf("next: %v", err)
		}
	}
	if _, err := reader.Next(); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestJSONReaderScannerError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	line := "{" + strings.Repeat("a", 11*1024*1024) + "}\n"
	if err := os.WriteFile(path, []byte(line), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	sch := model.NewSchema([]model.Column{{Name: "a", Type: model.TypeString}})
	reader, err := NewReader(path, &sch)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()
	if _, err := reader.Next(); err == nil {
		t.Fatalf("expected scanner error")
	}
}

func TestJSONReaderMissingField(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	data := "{\"a\":1}\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	sch := model.NewSchema([]model.Column{{Name: "a", Type: model.TypeInt}, {Name: "b", Type: model.TypeString}})
	reader, err := NewReader(path, &sch)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()
	row, err := reader.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if row.Values[1].String() != "" {
		t.Fatalf("expected empty missing field")
	}
}

func TestJSONReaderOpenError(t *testing.T) {
	if _, err := NewReader("missing.jsonl", nil); err == nil {
		t.Fatalf("expected open error")
	}
}
