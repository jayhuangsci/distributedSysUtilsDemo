#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# -lt 1 ]]; then
  echo 'Usage: scripts/run.sh <hadoop|spark|generate|compare|demo> [arguments...]' >&2
  exit 2
fi
command=$1
shift
case "$command" in
  hadoop) main=io.github.jayhuangsci.ratings.hadoop.HadoopAnalytics ;;
  spark) main=io.github.jayhuangsci.ratings.spark.SparkAnalytics ;;
  generate) main=io.github.jayhuangsci.ratings.tools.GenerateRatings ;;
  compare) main=io.github.jayhuangsci.ratings.tools.CompareOutputs ;;
  demo) main=io.github.jayhuangsci.ratings.tools.Demo ;;
  *) echo "Unknown command: $command" >&2; exit 2 ;;
esac
if [[ ! -f target/classpath.txt || ! -f target/classes/io/github/jayhuangsci/ratings/tools/Demo.class ]]; then
  ./mvnw -q -DskipTests package
fi
java_bin=java
if [[ -n "${JAVA_HOME:-}" ]]; then java_bin="$JAVA_HOME/bin/java"; fi
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
exec "$java_bin" -Xmx2g --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -cp "target/classes:$(cat target/classpath.txt)" "$main" "$@"
