package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"kqlfile/pkg/csvio"
	"kqlfile/pkg/exec"
	"kqlfile/pkg/jsonio"
	"kqlfile/pkg/model"
	"kqlfile/pkg/output"
	"kqlfile/pkg/parser"
	"kqlfile/pkg/plan"
)

var exitFunc = os.Exit

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		exitFunc(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("kqlfile", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var inputs inputList
	var query string
	var format string
	var schemaStr string
	var fileType string

	fs.Var(&inputs, "input", "CSV input file path or name=path (repeatable)")
	fs.StringVar(&query, "query", "", "KQL query string")
	fs.StringVar(&format, "format", "csv", "Output format: csv|json|table")
	fs.StringVar(&schemaStr, "schema", "", "Schema override: col:type,col:type")
	fs.StringVar(&fileType, "type", "csv", "Input file type (csv)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if len(inputs) == 0 || query == "" {
		fmt.Fprintln(stderr, "input and query are required")
		fs.Usage()
		return errors.New("missing required flags")
	}
	fileType = strings.ToLower(fileType)
	if fileType != "csv" && fileType != "json" {
		return fmt.Errorf("unsupported input type: %s", fileType)
	}

	inputMap, err := inputs.ToMap()
	if err != nil {
		fmt.Fprintln(stderr, "input error:", err)
		return err
	}
	tableName := parseTableName(query)
	if tableName == "" {
		if len(inputMap) > 1 {
			return errors.New("query must specify a table name when multiple inputs are provided")
		}
		tableName = "T"
	}
	inputPath, ok := inputMap[tableName]
	if !ok {
		return fmt.Errorf("unknown table name: %s", tableName)
	}

	var schema *model.Schema
	if schemaStr != "" {
		parsed, err := parseSchema(schemaStr)
		if err != nil {
			fmt.Fprintln(stderr, "schema error:", err)
			return err
		}
		schema = &parsed
	}

	reader, err := openReader(fileType, inputPath, schema)
	if err != nil {
		fmt.Fprintln(stderr, "reader error:", err)
		return err
	}
	defer reader.Close()

	ops, err := parser.Parse(query)
	if err != nil {
		fmt.Fprintln(stderr, "parse error:", err)
		return err
	}
	ops = resolveJoinInputs(ops, inputMap)

	pipe, err := exec.BuildPipeline(reader, ops)
	if err != nil {
		fmt.Fprintln(stderr, "plan error:", err)
		return err
	}

	rows := make(chan *csvio.Row)
	go func() {
		defer close(rows)
		for {
			row, err := pipe.Next()
			if err == io.EOF {
				return
			}
			if err != nil {
				fmt.Fprintln(stderr, "exec error:", err)
				return
			}
			rows <- row
		}
	}()

	fmtType := output.Format(strings.ToLower(format))
	if err := output.WriteTo(stdout, fmtType, rows); err != nil {
		fmt.Fprintln(stderr, "output error:", err)
		return err
	}
	return nil
}

func parseSchema(raw string) (model.Schema, error) {
	parts := strings.Split(raw, ",")
	cols := make([]model.Column, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, ":", 2)
		if len(kv) != 2 {
			return model.Schema{}, errors.New("schema must be col:type")
		}
		name := strings.TrimSpace(kv[0])
		typeStr := strings.ToLower(strings.TrimSpace(kv[1]))
		cols = append(cols, model.Column{Name: name, Type: model.Type(typeStr)})
	}
	if len(cols) == 0 {
		return model.Schema{}, errors.New("empty schema")
	}
	return model.NewSchema(cols), nil
}

type rowReader interface {
	Schema() model.Schema
	Next() (*csvio.Row, error)
	Close() error
}

func openReader(fileType, path string, schema *model.Schema) (rowReader, error) {
	switch fileType {
	case "csv":
		return csvio.NewReader(path, schema)
	case "json":
		return jsonio.NewReader(path, schema)
	default:
		return nil, fmt.Errorf("unsupported input type: %s", fileType)
	}
}

type inputList []string

func (i *inputList) String() string {
	return strings.Join(*i, ",")
}

func (i *inputList) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func (i inputList) ToMap() (map[string]string, error) {
	out := make(map[string]string)
	unnamed := 0
	for _, v := range i {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if strings.Contains(v, "=") {
			parts := strings.SplitN(v, "=", 2)
			name := strings.TrimSpace(parts[0])
			path := strings.TrimSpace(parts[1])
			if name == "" || path == "" {
				return nil, errors.New("input must be name=path")
			}
			out[name] = path
		} else {
			unnamed++
			if unnamed > 1 {
				return nil, errors.New("only one unnamed input is allowed")
			}
			out["T"] = v
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no valid inputs provided")
	}
	return out, nil
}

func parseTableName(query string) string {
	parts := strings.Split(query, "|")
	first := strings.TrimSpace(parts[0])
	if first == "" {
		return ""
	}
	fields := strings.Fields(first)
	if isOperatorToken(fields[0]) {
		return ""
	}
	return fields[0]
}

func isOperatorToken(tok string) bool {
	switch strings.ToLower(tok) {
	case "where", "project", "extend", "summarize", "take", "order", "join":
		return true
	default:
		return false
	}
}

func resolveJoinInputs(ops []plan.Operator, inputs map[string]string) []plan.Operator {
	out := make([]plan.Operator, 0, len(ops))
	for _, op := range ops {
		j, ok := op.(plan.JoinOp)
		if !ok {
			out = append(out, op)
			continue
		}
		if path, ok := inputs[j.Right]; ok {
			j.Right = path
		}
		out = append(out, j)
	}
	return out
}
