package csvio

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"kqlfile/pkg/model"
)

const inferSampleSize = 100

type Row struct {
	Schema model.Schema
	Values []model.Value
}

func (r Row) Get(col string) (model.Value, bool) {
	idx, ok := r.Schema.Index[col]
	if !ok || idx >= len(r.Values) {
		return model.Value{}, false
	}
	return r.Values[idx], true
}

type Reader struct {
	file     *os.File
	csvr     *csv.Reader
	schema   model.Schema
	buffer   [][]string
	bufferIx int
}

func NewReader(path string, schema *model.Schema) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	csvr := csv.NewReader(f)
	csvr.ReuseRecord = true

	head, err := csvr.Read()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}
	head = append([]string(nil), head...)

	var sch model.Schema
	buffer := make([][]string, 0, inferSampleSize)
	if schema != nil {
		sch = *schema
	} else {
		cols := make([][]string, len(head))
		for i := 0; i < inferSampleSize; i++ {
			rec, err := csvr.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("sample read: %w", err)
			}
			cp := append([]string(nil), rec...)
			buffer = append(buffer, cp)
			for c := range head {
				if c < len(rec) {
					cols[c] = append(cols[c], rec[c])
				}
			}
		}
		inferred := make([]model.Column, len(head))
		for i, name := range head {
			inferred[i] = model.Column{Name: name, Type: model.InferType(cols[i])}
		}
		sch = model.NewSchema(inferred)
	}

	return &Reader{file: f, csvr: csvr, schema: sch, buffer: buffer}, nil
}

func (r *Reader) Schema() model.Schema {
	return r.schema
}

func (r *Reader) Close() error {
	return r.file.Close()
}

func (r *Reader) Next() (*Row, error) {
	if r.bufferIx < len(r.buffer) {
		rec := r.buffer[r.bufferIx]
		r.bufferIx++
		return r.parseRecord(rec)
	}
	rec, err := r.csvr.Read()
	if err != nil {
		return nil, err
	}
	return r.parseRecord(rec)
}

func (r *Reader) parseRecord(rec []string) (*Row, error) {
	vals := make([]model.Value, len(r.schema.Columns))
	for i, col := range r.schema.Columns {
		if i >= len(rec) {
			vals[i] = model.Value{Type: col.Type, V: ""}
			continue
		}
		v, err := model.ParseValue(col.Type, rec[i])
		if err != nil {
			return nil, fmt.Errorf("parse column %s: %w", col.Name, err)
		}
		vals[i] = v
	}
	return &Row{Schema: r.schema, Values: vals}, nil
}
