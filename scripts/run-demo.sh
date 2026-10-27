#!/bin/sh
# Builds the project and runs the scripted demo (about 1 minute).
# The full output is also saved to logs/demo-transcript.txt
set -e
cd "$(dirname "$0")/.."
sh scripts/build.sh
java -cp out demo.DemoRunner --transcript logs/demo-transcript.txt
