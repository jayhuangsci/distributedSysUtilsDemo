#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
movies=${1:-1000}
users=${2:-100}
density=${3:-0.1}
repetitions=${4:-3}
partitions=${5:-4}
mkdir -p benchmark-output
run_root=$(mktemp -d "$PWD/benchmark-output/run-XXXXXX")
./scripts/run.sh generate --movies "$movies" --users "$users" --density "$density" --seed 42 --output "$run_root/input.csv"
./scripts/run.sh demo "$run_root/input.csv" "$run_root/results" "$partitions" "$repetitions"
echo "Experiment report: $run_root/results/report.csv"
