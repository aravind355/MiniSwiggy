# Compiles every Java file in src\ and tests\ into out\ (needs JDK 17 or newer).
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
if (Test-Path out) { Remove-Item -Recurse -Force out }
$sources = Get-ChildItem -Recurse -Filter *.java -Path src, tests | ForEach-Object { $_.FullName }
javac --release 17 -d out $sources
if ($LASTEXITCODE -ne 0) { Write-Host "Build FAILED"; exit $LASTEXITCODE }
Write-Host "Build OK: $($sources.Count) source files compiled into out\"
