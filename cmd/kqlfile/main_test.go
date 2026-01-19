package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"kqlfile/pkg/plan"
)

type errWriter struct{}

func (errWriter) Write(p []byte) (int, error) {
	return 0, errors.New("write error")
}

func TestRunSuccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	data := "name,age\nalice,30\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer

	err := run([]string{"--input", path, "--query", "T | project name"}, &out, &errBuf)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out.String(), "alice") {
		t.Fatalf("expected output")
	}
}

func TestRunMissingFlags(t *testing.T) {
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{}, &out, &errBuf); err == nil {
		t.Fatalf("expected error")
	}
}

func TestRunSchemaError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	data := "name,age\nalice,30\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", path, "--query", "T | project name", "--schema", "bad"}, &out, &errBuf); err == nil {
		t.Fatalf("expected schema error")
	}
}

func TestRunParseError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	data := "name,age\nalice,30\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", path, "--query", "T | badop"}, &out, &errBuf); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestRunUnknownFlag(t *testing.T) {
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--unknown"}, &out, &errBuf); err == nil {
		t.Fatalf("expected flag error")
	}
}

func TestRunReaderError(t *testing.T) {
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", "missing.csv", "--query", "T | project name"}, &out, &errBuf); err == nil {
		t.Fatalf("expected reader error")
	}
}

func TestRunMultipleInputsRequireTable(t *testing.T) {
	pathA := filepath.Join(t.TempDir(), "a.csv")
	pathB := filepath.Join(t.TempDir(), "b.csv")
	if err := os.WriteFile(pathA, []byte("name\nalice\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(pathB, []byte("name\nbob\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", "A=" + pathA, "--input", "B=" + pathB, "--query", "where name == \"alice\""}, &out, &errBuf); err == nil {
		t.Fatalf("expected table error")
	}
}

func TestRunMultipleInputsTableName(t *testing.T) {
	pathA := filepath.Join(t.TempDir(), "a.csv")
	pathB := filepath.Join(t.TempDir(), "b.csv")
	if err := os.WriteFile(pathA, []byte("name\nalice\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(pathB, []byte("name\nbob\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", "A=" + pathA, "--input", "B=" + pathB, "--query", "A | project name"}, &out, &errBuf); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out.String(), "alice") {
		t.Fatalf("expected alice")
	}
}

func TestRunUnknownTableName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.csv")
	if err := os.WriteFile(path, []byte("name\nalice\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", "A=" + path, "--query", "B | project name"}, &out, &errBuf); err == nil {
		t.Fatalf("expected unknown table error")
	}
}

func TestRunMultipleUnnamedInputs(t *testing.T) {
	pathA := filepath.Join(t.TempDir(), "a.csv")
	pathB := filepath.Join(t.TempDir(), "b.csv")
	if err := os.WriteFile(pathA, []byte("name\nalice\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.WriteFile(pathB, []byte("name\nbob\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", pathA, "--input", pathB, "--query", "T | project name"}, &out, &errBuf); err == nil {
		t.Fatalf("expected unnamed input error")
	}
}

func TestRunUnsupportedType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte("name\nalice\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", path, "--query", "T | project name", "--type", "parquet"}, &out, &errBuf); err == nil {
		t.Fatalf("expected type error")
	}
}

func TestOpenReaderUnsupported(t *testing.T) {
	if _, err := openReader("parquet", "file", nil); err == nil {
		t.Fatalf("expected openReader error")
	}
}

func TestRunJSONType(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.jsonl")
	data := "{\"name\":\"alice\",\"age\":30}\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", path, "--query", "T | project name", "--type", "json"}, &out, &errBuf); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out.String(), "alice") {
		t.Fatalf("expected json output")
	}
}

func TestRunPlanError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	data := "id\n1\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", path, "--query", "T | join kind=inner (missing.csv) on id == id"}, &out, &errBuf); err == nil {
		t.Fatalf("expected plan error")
	}
}

func TestRunOutputError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	data := "name,age\nalice,30\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var errBuf bytes.Buffer
	if err := run([]string{"--input", path, "--query", "T | project name"}, errWriter{}, &errBuf); err == nil {
		t.Fatalf("expected output error")
	}
}

func TestRunExecError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	data := "age\nnotint\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", path, "--query", "T | project age", "--schema", "age:int"}, &out, &errBuf); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(errBuf.String(), "exec error") {
		t.Fatalf("expected exec error")
	}
}

func TestMain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	data := "name,age\nalice,30\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	oldArgs := os.Args
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	defer func() {
		os.Args = oldArgs
		os.Stdout = oldStdout
		os.Stderr = oldStderr
	}()

	r, w, _ := os.Pipe()
	os.Stdout = w
	os.Stderr = w
	os.Args = []string{"kqlfile", "--input", path, "--query", "T | project name"}

	main()
	w.Close()
	out, _ := io.ReadAll(r)
	if !strings.Contains(string(out), "alice") {
		t.Fatalf("expected main output")
	}
}

func TestMainExitOnError(t *testing.T) {
	oldArgs := os.Args
	oldExit := exitFunc
	defer func() {
		os.Args = oldArgs
		exitFunc = oldExit
	}()

	called := 0
	exitFunc = func(code int) {
		called = code
	}
	os.Args = []string{"kqlfile"}

	main()
	if called != 1 {
		t.Fatalf("expected exit code 1")
	}
}

func TestParseSchema(t *testing.T) {
	sch, err := parseSchema("a:string,b:int")
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	if sch.Columns[1].Type != "int" {
		t.Fatalf("expected int")
	}
	if _, err := parseSchema(""); err == nil {
		t.Fatalf("expected empty schema error")
	}
}

func TestInputListToMap(t *testing.T) {
	var inputs inputList
	inputs = append(inputs, "A=path.csv", "T=other.csv")
	m, err := inputs.ToMap()
	if err != nil {
		t.Fatalf("to map: %v", err)
	}
	if m["A"] != "path.csv" {
		t.Fatalf("expected A mapping")
	}
}

func TestInputListUnnamed(t *testing.T) {
	var inputs inputList
	inputs = append(inputs, "path.csv")
	m, err := inputs.ToMap()
	if err != nil {
		t.Fatalf("to map: %v", err)
	}
	if m["T"] != "path.csv" {
		t.Fatalf("expected default T mapping")
	}
}

func TestInputListErrors(t *testing.T) {
	var inputs inputList
	inputs = append(inputs, "=", "A=")
	if _, err := inputs.ToMap(); err == nil {
		t.Fatalf("expected name=path error")
	}
	inputs = inputList{}
	inputs = append(inputs, "a.csv", "b.csv")
	if _, err := inputs.ToMap(); err == nil {
		t.Fatalf("expected unnamed error")
	}
	inputs = inputList{}
	inputs = append(inputs, "   ")
	if _, err := inputs.ToMap(); err == nil {
		t.Fatalf("expected no valid inputs error")
	}
}

func TestParseTableName(t *testing.T) {
	if name := parseTableName("T | where age > 1"); name != "T" {
		t.Fatalf("expected T")
	}
	if name := parseTableName("where age > 1"); name != "" {
		t.Fatalf("expected empty")
	}
	if name := parseTableName(" | where age > 1"); name != "" {
		t.Fatalf("expected empty with leading pipe")
	}
	if name := parseTableName("  "); name != "" {
		t.Fatalf("expected empty")
	}
}

func TestIsOperatorToken(t *testing.T) {
	if !isOperatorToken("where") {
		t.Fatalf("expected operator")
	}
	if isOperatorToken("table") {
		t.Fatalf("expected non-operator")
	}
}

func TestResolveJoinInputs(t *testing.T) {
	ops := []plan.Operator{
		plan.JoinOp{Kind: "inner", Right: "B", LeftKey: "id", RightKey: "id"},
	}
	out := resolveJoinInputs(ops, map[string]string{"B": "path.csv"})
	join := out[0].(plan.JoinOp)
	if join.Right != "path.csv" {
		t.Fatalf("expected path replacement")
	}
}

func TestRunSingleInputNoTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte("name\nalice\n"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", path, "--query", "where name == \"alice\""}, &out, &errBuf); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out.String(), "alice") {
		t.Fatalf("expected output")
	}
}

func TestRunInputMapError(t *testing.T) {
	var out bytes.Buffer
	var errBuf bytes.Buffer
	if err := run([]string{"--input", "=", "--query", "T | project name"}, &out, &errBuf); err == nil {
		t.Fatalf("expected input map error")
	}
}
