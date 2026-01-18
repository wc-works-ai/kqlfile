package output

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"kqlfile/pkg/csvio"
)

type Format string

const (
	FormatCSV   Format = "csv"
	FormatJSON  Format = "json"
	FormatTable Format = "table"
)

func Write(format Format, rows <-chan *csvio.Row) error {
	return WriteTo(os.Stdout, format, rows)
}

func WriteTo(w io.Writer, format Format, rows <-chan *csvio.Row) error {
	switch format {
	case FormatJSON:
		return writeJSON(w, rows)
	case FormatTable:
		return writeTable(w, rows)
	default:
		return writeCSV(w, rows)
	}
}

func writeCSV(w io.Writer, rows <-chan *csvio.Row) error {
	cw := csv.NewWriter(w)
	var headerWritten bool
	for row := range rows {
		if !headerWritten {
			headers := make([]string, len(row.Schema.Columns))
			for i, c := range row.Schema.Columns {
				headers[i] = c.Name
			}
			if err := cw.Write(headers); err != nil {
				return err
			}
			headerWritten = true
		}
		rec := make([]string, len(row.Values))
		for i, v := range row.Values {
			rec[i] = v.String()
		}
		if err := cw.Write(rec); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func writeJSON(w io.Writer, rows <-chan *csvio.Row) error {
	enc := json.NewEncoder(w)
	for row := range rows {
		obj := make(map[string]any, len(row.Schema.Columns))
		for i, c := range row.Schema.Columns {
			obj[c.Name] = row.Values[i].V
		}
		if err := enc.Encode(obj); err != nil {
			return err
		}
	}
	return nil
}

func writeTable(w io.Writer, rows <-chan *csvio.Row) error {
	var data [][]string
	var headers []string
	for row := range rows {
		if headers == nil {
			headers = make([]string, len(row.Schema.Columns))
			for i, c := range row.Schema.Columns {
				headers[i] = c.Name
			}
		}
		rec := make([]string, len(row.Values))
		for i, v := range row.Values {
			rec[i] = v.String()
		}
		data = append(data, rec)
	}
	if headers == nil {
		return nil
	}
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, rec := range data {
		for i, v := range rec {
			if len(v) > widths[i] {
				widths[i] = len(v)
			}
		}
	}
	fmt.Fprintln(w, formatRow(headers, widths))
	fmt.Fprintln(w, formatSep(widths))
	for _, rec := range data {
		fmt.Fprintln(w, formatRow(rec, widths))
	}
	return nil
}

func formatRow(vals []string, widths []int) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		pad := widths[i] - len(v)
		parts[i] = v + strings.Repeat(" ", pad)
	}
	return strings.Join(parts, "  ")
}

func formatSep(widths []int) string {
	parts := make([]string, len(widths))
	for i, w := range widths {
		parts[i] = strings.Repeat("-", w)
	}
	return strings.Join(parts, "  ")
}
