#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# -lt 1 ]]; then
  echo 'Usage: scripts/run.sh <prepare|generate|inspect|demo> [arguments...]' >&2
  exit 2
fi
command=$1
shift
case "$command" in
  prepare) main=io.github.jayhuangsci.corpus.hadoop.CorpusPipeline ;;
  generate) main=io.github.jayhuangsci.corpus.tools.GenerateCorpus ;;
  inspect) main=io.github.jayhuangsci.corpus.tools.CorpusInspector ;;
  demo) main=io.github.jayhuangsci.corpus.tools.Demo ;;
  *) echo "Unknown command: $command" >&2; exit 2 ;;
esac
if [[ ! -f target/classpath.txt || ! -f target/classes/io/github/jayhuangsci/corpus/hadoop/CorpusPipeline.class ]]; then
  ./mvnw -q -DskipTests package
fi
java_bin=java
if [[ -n "${JAVA_HOME:-}" ]]; then java_bin="$JAVA_HOME/bin/java"; fi
exec "$java_bin" -Xmx1536m -Dorg.slf4j.simpleLogger.defaultLogLevel=warn \
  -cp "target/classes:$(cat target/classpath.txt)" "$main" "$@"
