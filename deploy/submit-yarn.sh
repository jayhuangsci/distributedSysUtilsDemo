#!/usr/bin/env bash
set -euo pipefail
repo_root=$(cd "$(dirname "$0")/.." && pwd)
if [[ $# -lt 2 ]]; then
  echo 'Usage: deploy/submit-yarn.sh <hdfs-input> <new-hdfs-output> [pipeline options...]' >&2
  exit 2
fi
input=$1
output=$2
shift 2
application_jar=${CORPUS_JAR:-$repo_root/target/hadoop-corpus-pipeline-2.0.0.jar}
if [[ ! -f "$application_jar" ]]; then echo "Build the application JAR first: $application_jar" >&2; exit 2; fi
exec hadoop jar "$application_jar" -Dmapreduce.framework.name=yarn -Dmapreduce.map.output.compress=true \
  --input "$input" --output "$output" "$@"
