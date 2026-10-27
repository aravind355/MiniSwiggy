#!/bin/sh
# Builds the project and runs the automated tests. Pass --verbose to see component logs.
set -e
cd "$(dirname "$0")/.."
sh scripts/build.sh
java -cp out tests.TestRunner "$@"
