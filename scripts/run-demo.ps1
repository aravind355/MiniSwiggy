# Builds the project and runs the scripted demo (about 1 minute).
# The full output is also saved to logs\demo-transcript.txt
Set-Location (Split-Path -Parent $PSScriptRoot)
& "$PSScriptRoot\build.ps1"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
java -cp out demo.DemoRunner --transcript logs/demo-transcript.txt
exit $LASTEXITCODE
