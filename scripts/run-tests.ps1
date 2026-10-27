# Builds the project and runs the automated tests. Add -Verbose to see component logs.
param([switch]$Verbose)
Set-Location (Split-Path -Parent $PSScriptRoot)
& "$PSScriptRoot\build.ps1"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if ($Verbose) { java -cp out tests.TestRunner --verbose } else { java -cp out tests.TestRunner }
exit $LASTEXITCODE
