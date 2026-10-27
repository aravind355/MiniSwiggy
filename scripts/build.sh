#!/bin/sh
# Compiles every Java file in src/ and tests/ into out/ (needs JDK 17 or newer).
set -e
cd "$(dirname "$0")/.."
rm -rf out
find src tests -name '*.java' > out-sources.txt
javac --release 17 -d out @out-sources.txt
rm -f out-sources.txt
echo "Build OK: classes are in out/"
