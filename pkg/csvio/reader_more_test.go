package csvio

import (
	"os"
	"path/filepath"
	"testing"

	"kqlfile/pkg/model"
)

func TestReaderEmptyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.csv")
	if err := os.WriteFile(path, []byte(""), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := NewReader(path, nil); err == nil {
		t.Fatalf("expected header error")
	}
}

func TestReaderOpenError(t *testing.T) {
	if _, err := NewReader("missing.csv", nil); err == nil {
		t.Fatalf("expected open error")
	}
}

func TestReaderParseError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.csv")
	data := "age\nnotint\n"
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
		t.Fatalf("expected parse error")
	}
}

func TestRowGetMissing(t *testing.T) {
	row := &Row{Schema: model.NewSchema([]model.Column{{Name: "a", Type: model.TypeString}})}
	if _, ok := row.Get("missing"); ok {
		t.Fatalf("expected missing")
	}
}

func TestRowGetIndexTooLarge(t *testing.T) {
	row := &Row{
		Schema: model.NewSchema([]model.Column{{Name: "a", Type: model.TypeString}}),
		Values: []model.Value{},
	}
	if _, ok := row.Get("a"); ok {
		t.Fatalf("expected missing due to length")
	}
}

func TestRowGetSuccess(t *testing.T) {
	row := &Row{
		Schema: model.NewSchema([]model.Column{{Name: "a", Type: model.TypeString}}),
		Values: []model.Value{{Type: model.TypeString, V: "x"}},
	}
	if v, ok := row.Get("a"); !ok || v.String() != "x" {
		t.Fatalf("expected value")
	}
}

func TestReaderBuffer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "short.csv")
	data := "a,b\n1,2\n3,4\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	reader, err := NewReader(path, nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()

	row, err := reader.Next()
	if err != nil {
		t.Fatalf("next: %v", err)
	}
	if row.Values[0].String() == "" {
		t.Fatalf("expected value")
	}
}

func TestParseRecordMissingColumn(t *testing.T) {
	sch := model.NewSchema([]model.Column{
		{Name: "a", Type: model.TypeInt},
		{Name: "b", Type: model.TypeString},
	})
	r := &Reader{schema: sch}
	row, err := r.parseRecord([]string{"1"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if row.Values[1].String() != "" {
		t.Fatalf("expected empty missing column")
	}
}

func TestReaderInferNoRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "header.csv")
	data := "a,b\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	reader, err := NewReader(path, nil)
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	defer reader.Close()
}

func TestReaderSampleReadError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.csv")
	data := "a,b\n\"unterminated\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := NewReader(path, nil); err == nil {
		t.Fatalf("expected sample read error")
	}
}

func TestReaderEOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	data := "a\n1\n"
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
	if _, err := reader.Next(); err == nil {
		t.Fatalf("expected EOF")
	}
}
