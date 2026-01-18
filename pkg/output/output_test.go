package output

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"kqlfile/pkg/csvio"
	"kqlfile/pkg/model"
)

func TestWriteCSV(t *testing.T) {
	rows := make(chan *csvio.Row, 1)
	rows <- sampleRow()
	close(rows)

	var buf bytes.Buffer
	if err := WriteTo(&buf, FormatCSV, rows); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	out := strings.TrimSpace(buf.String())
	if out == "" {
		t.Fatalf("expected output")
	}
	if !strings.HasPrefix(out, "name,age") {
		t.Fatalf("missing header: %s", out)
	}
}

func TestWriteJSON(t *testing.T) {
	rows := make(chan *csvio.Row, 1)
	rows <- sampleRow()
	close(rows)

	var buf bytes.Buffer
	if err := WriteTo(&buf, FormatJSON, rows); err != nil {
		t.Fatalf("write json: %v", err)
	}
	var obj map[string]any
	if err := json.Unmarshal(buf.Bytes(), &obj); err != nil {
		t.Fatalf("decode json: %v", err)
	}
	if obj["name"] != "alice" {
		t.Fatalf("unexpected json value: %v", obj["name"])
	}
}

func TestWriteTable(t *testing.T) {
	rows := make(chan *csvio.Row, 1)
	rows <- sampleRow()
	close(rows)

	var buf bytes.Buffer
	if err := WriteTo(&buf, FormatTable, rows); err != nil {
		t.Fatalf("write table: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "name") {
		t.Fatalf("missing table header")
	}
}

func TestWriteDefaultFormat(t *testing.T) {
	rows := make(chan *csvio.Row, 1)
	rows <- sampleRow()
	close(rows)

	var buf bytes.Buffer
	if err := WriteTo(&buf, Format("unknown"), rows); err != nil {
		t.Fatalf("write default: %v", err)
	}
	if !strings.Contains(buf.String(), "alice") {
		t.Fatalf("expected csv output")
	}
}

func TestWriteCSVEmpty(t *testing.T) {
	rows := make(chan *csvio.Row)
	close(rows)

	var buf bytes.Buffer
	if err := WriteTo(&buf, FormatCSV, rows); err != nil {
		t.Fatalf("write empty csv: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected empty output")
	}
}

func TestWriteCSVMultiRow(t *testing.T) {
	rows := make(chan *csvio.Row, 2)
	rows <- sampleRow()
	rows <- sampleRow()
	close(rows)

	var buf bytes.Buffer
	if err := WriteTo(&buf, FormatCSV, rows); err != nil {
		t.Fatalf("write multi csv: %v", err)
	}
	if strings.Count(buf.String(), "alice") != 2 {
		t.Fatalf("expected two rows")
	}
}

func TestWriteStdout(t *testing.T) {
	rows := make(chan *csvio.Row, 1)
	rows <- sampleRow()
	close(rows)

	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	defer func() {
		os.Stdout = old
	}()

	if err := Write(FormatCSV, rows); err != nil {
		t.Fatalf("write stdout: %v", err)
	}
	w.Close()
	out, _ := io.ReadAll(r)
	if !strings.Contains(string(out), "alice") {
		t.Fatalf("expected stdout output")
	}
}

func TestWriteErrors(t *testing.T) {
	rows := make(chan *csvio.Row, 1)
	rows <- sampleRow()
	close(rows)

	if err := WriteTo(errWriter{}, FormatCSV, rows); err == nil {
		t.Fatalf("expected csv error")
	}

	rows = make(chan *csvio.Row, 1)
	rows <- sampleRow()
	close(rows)
	if err := WriteTo(errWriter{}, FormatJSON, rows); err == nil {
		t.Fatalf("expected json error")
	}
}

func TestWriteCSVRecordError(t *testing.T) {
	rows := make(chan *csvio.Row, 1)
	large := strings.Repeat("x", 5000)
	schema := model.NewSchema([]model.Column{{Name: "col", Type: model.TypeString}})
	rows <- &csvio.Row{Schema: schema, Values: []model.Value{{Type: model.TypeString, V: large}}}
	close(rows)

	if err := WriteTo(errWriter{}, FormatCSV, rows); err == nil {
		t.Fatalf("expected csv record error")
	}
}

func TestWriteCSVHeaderError(t *testing.T) {
	rows := make(chan *csvio.Row, 1)
	large := strings.Repeat("h", 5000)
	schema := model.NewSchema([]model.Column{{Name: large, Type: model.TypeString}})
	rows <- &csvio.Row{Schema: schema, Values: []model.Value{{Type: model.TypeString, V: "x"}}}
	close(rows)

	if err := WriteTo(errWriter{}, FormatCSV, rows); err == nil {
		t.Fatalf("expected csv header error")
	}
}

func TestWriteTableEmpty(t *testing.T) {
	rows := make(chan *csvio.Row)
	close(rows)

	var buf bytes.Buffer
	if err := WriteTo(&buf, FormatTable, rows); err != nil {
		t.Fatalf("write empty table: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected empty output")
	}
}

type errWriter struct{}

func (errWriter) Write(p []byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func sampleRow() *csvio.Row {
	schema := model.NewSchema([]model.Column{
		{Name: "name", Type: model.TypeString},
		{Name: "age", Type: model.TypeInt},
	})
	return &csvio.Row{
		Schema: schema,
		Values: []model.Value{
			{Type: model.TypeString, V: "alice"},
			{Type: model.TypeInt, V: int64(30)},
		},
	}
}
