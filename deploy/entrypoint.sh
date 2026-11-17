#!/usr/bin/env bash
set -euo pipefail
case "${1:-}" in
  namenode)
    if [[ ! -f /data/name/current/VERSION ]]; then
      hdfs namenode -format -nonInteractive corpus-demo
    fi
    exec hdfs namenode
    ;;
  resourcemanager) exec yarn resourcemanager ;;
  historyserver)
    hdfs dfs -mkdir -p /mr-history/tmp /mr-history/done /app-logs
    exec mapred historyserver
    ;;
  worker)
    hdfs datanode &
    datanode_pid=$!
    yarn nodemanager &
    nodemanager_pid=$!
    cleanup() {
      kill "$datanode_pid" "$nodemanager_pid" 2>/dev/null || true
      wait "$datanode_pid" "$nodemanager_pid" 2>/dev/null || true
    }
    trap cleanup EXIT
    trap 'exit 143' TERM INT
    wait -n "$datanode_pid" "$nodemanager_pid"
    ;;
  *) exec "$@" ;;
esac
