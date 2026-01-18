param(
  [ValidateSet("build","test","fmt","vet","lint","clean","run-sample","bench")]
  [string]$Task = "build"
)

$App = "kqlfile"

switch ($Task) {
  "build" { go build "./cmd/$App" }
  "test" { go test ./... }
  "fmt" { go fmt ./... }
  "vet" { go vet ./... }
  "lint" { go vet ./... }
  "clean" {
    if (Test-Path "$App.exe") { Remove-Item "$App.exe" }
    if (Test-Path $App) { Remove-Item $App }
  }
  "run-sample" {
    go run "./cmd/$App" --input testdata/people.csv --query "T | where age > 30 | project name, age"
  }
  "bench" {
    powershell -ExecutionPolicy Bypass -File scripts/bench.ps1
  }
}
