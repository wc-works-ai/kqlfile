param(
  [double]$SizeGB = 1.0,
  [int]$RowsPerChunk = 100000,
  [string]$Output = "testdata\bench.csv",
  [string]$Query = "T | where age > 30 | project id, age, city | take 100000"
)

$os = $PSVersionTable.OS
$cpu = (Get-CimInstance Win32_Processor | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum
$ram = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
$ramMB = [int]($ram / 1MB)

Write-Host "OS: $os"
Write-Host "CPU: $cpu"
Write-Host "RAM: $ramMB MB"

python scripts/gen_csv.py --size-gb $SizeGB --rows-per-chunk $RowsPerChunk --output $Output

$sw = [System.Diagnostics.Stopwatch]::StartNew()
./kqlfile --input $Output --query $Query --type csv | Out-File -FilePath $env:TEMP\kqlfile-bench.out -Encoding utf8
$sw.Stop()

$sizeBytes = (Get-Item $Output).Length
$sizeMB = [int]($sizeBytes / 1MB)
$elapsed = [int]$sw.Elapsed.TotalSeconds
if ($elapsed -gt 0) { $throughput = [int]($sizeMB / $elapsed) } else { $throughput = 0 }

Write-Host "Elapsed: $elapsed s"
Write-Host "Size: $sizeMB MB"
Write-Host "Approx throughput: $throughput MB/s"
