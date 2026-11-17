#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p benchmark-output
run_root=$(mktemp -d "$PWD/benchmark-output/run-XXXXXX")
./scripts/run.sh generate --documents "${1:-1000}" --shards "${2:-8}" --seed 42 --output "$run_root/raw"
./scripts/run.sh prepare --input "$run_root/raw" --output "$run_root/corpus" --reducers "${3:-2}"
./scripts/run.sh inspect "$run_root/corpus"
echo "Measured job counters and timings: $run_root/corpus/_MANIFEST.json"
