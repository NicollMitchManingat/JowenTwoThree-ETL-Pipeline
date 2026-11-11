$ErrorActionPreference = 'Stop'
$etlRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $etlRoot
$etlPython = Join-Path $etlRoot '.venv\Scripts\python.exe'
& $etlPython -B tools/local_warehouse.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& $etlPython -B -m pipelines.run
exit $LASTEXITCODE
