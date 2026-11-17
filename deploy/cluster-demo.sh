#!/usr/bin/env bash
set -euo pipefail
evidence=${1:-/app/evidence}
run_id="run-$(date -u +%Y%m%dT%H%M%SZ)-$$"
raw="/tmp/corpus-raw-$run_id"
input="/corpus/input/$run_id"
output="/corpus/output/$run_id"
mkdir -p "$evidence"

for attempt in $(seq 1 60); do
  if hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes (2)' \
      && yarn node -list 2>/dev/null | grep -q 'Total Nodes:2'; then
    break
  fi
  if [[ $attempt == 60 ]]; then echo 'Two DataNodes and two YARN workers did not become ready' >&2; exit 1; fi
  sleep 2
done
hdfs dfsadmin -safemode wait
hdfs dfsadmin -report > "$evidence/hdfs-report.txt"
yarn node -list -all > "$evidence/yarn-nodes.txt"
java -cp /app/corpus.jar io.github.jayhuangsci.corpus.tools.GenerateCorpus \
  --documents 80 --shards 16 --duplicate-every 10 --seed 42 --output "$raw"
hdfs dfs -mkdir -p "$input"
hdfs dfs -put "$raw"/raw-*.txt "$input/"
hadoop jar /app/corpus.jar -Dmapreduce.framework.name=yarn \
  --input "$input" --output "$output" --reducers 3 \
  --chunk-tokens 32 --chunk-overlap 8 --validation-percent 20
hdfs dfs -get "$output" "$evidence/corpus"
hdfs fsck "$input" -files -blocks -locations > "$evidence/hdfs-blocks.txt"
java -cp /app/corpus.jar io.github.jayhuangsci.corpus.tools.CorpusInspector "$evidence/corpus" > "$evidence/inspection.json"
java -cp /app/corpus.jar io.github.jayhuangsci.corpus.tools.ClusterAudit \
  "$evidence/corpus/_MANIFEST.json" http://resourcemanager:8088 http://historyserver:19888 \
  "$evidence/deployment-audit.json"
printf 'Distributed run completed. HDFS output: %s\nEvidence: %s\n' "$output" "$evidence"
