package jsonio

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"kqlfile/pkg/csvio"
	"kqlfile/pkg/model"
)

const inferSampleSize = 100

type Reader struct {
	file     *os.File
	scanner  *bufio.Scanner
	schema   model.Schema
	buffer   []map[string]any
	bufferIx int
	columns  []string
}

func NewReader(path string, schema *model.Schema) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)

	var sch model.Schema
	columns := []string{}
	buffer := make([]map[string]any, 0, inferSampleSize)
	if schema != nil {
		sch = *schema
		for _, c := range sch.Columns {
			columns = append(columns, c.Name)
		}
	} else {
		colSamples := map[string][]string{}
		for i := 0; i < inferSampleSize && sc.Scan(); i++ {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			obj, err := parseJSONLine(line)
			if err != nil {
				f.Close()
				return nil, err
			}
			buffer = append(buffer, obj)
			for k, v := range obj {
				colSamples[k] = append(colSamples[k], fmt.Sprintf("%v", v))
			}
		}
		cols := make([]model.Column, 0, len(colSamples))
		for name, values := range colSamples {
			cols = append(cols, model.Column{Name: name, Type: model.InferType(values)})
			columns = append(columns, name)
		}
		sch = model.NewSchema(cols)
	}

	return &Reader{file: f, scanner: sc, schema: sch, buffer: buffer, columns: columns}, nil
}

func (r *Reader) Schema() model.Schema {
	return r.schema
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) Next() (*csvio.Row, error) {
	if r.bufferIx < len(r.buffer) {
		obj := r.buffer[r.bufferIx]
		r.bufferIx++
		return r.parseObject(obj)
	}
	for r.scanner.Scan() {
		line := strings.TrimSpace(r.scanner.Text())
		if line == "" {
			continue
		}
		obj, err := parseJSONLine(line)
		if err != nil {
			return nil, err
		}
		return r.parseObject(obj)
	}
	if err := r.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func parseJSONLine(line string) (map[string]any, error) {
	var obj map[string]any
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, fmt.Errorf("expected object")
	}
	return obj, nil
}

func (r *Reader) parseObject(obj map[string]any) (*csvio.Row, error) {
	vals := make([]model.Value, len(r.schema.Columns))
	for i, col := range r.schema.Columns {
		raw, ok := obj[col.Name]
		if !ok {
			vals[i] = model.Value{Type: col.Type, V: ""}
			continue
		}
		valStr := fmt.Sprintf("%v", raw)
		v, err := model.ParseValue(col.Type, valStr)
		if err != nil {
			return nil, fmt.Errorf("parse field %s: %w", col.Name, err)
		}
		vals[i] = v
	}
	return &csvio.Row{Schema: r.schema, Values: vals}, nil
}
