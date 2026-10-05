#!/usr/bin/env bash
set -euo pipefail
mkdir -p /state/nn /state/dn /state/logs /tmp/hadoop-pids
mkdir -p "$HADOOP_CONF_DIR"
cp -a "$HADOOP_HOME/etc/hadoop/." "$HADOOP_CONF_DIR/"
cp /config-overrides/*.xml "$HADOOP_CONF_DIR/"
if [ ! -f /state/nn/current/VERSION ]; then
  # Never format nonempty storage: missing VERSION can also mean damaged metadata.
  if [ -n "$(find /state/nn -mindepth 1 -print -quit)" ]; then
    echo 'NameNode storage is nonempty without VERSION; refusing to format' >&2
    exit 2
  fi
  hdfs namenode -format -nonInteractive -clusterId ml-governance-dev
fi
stop_services() {
  mapred --daemon stop historyserver || true
  yarn --daemon stop nodemanager || true
  yarn --daemon stop resourcemanager || true
  hdfs --daemon stop datanode || true
  hdfs --daemon stop namenode || true
}
trap stop_services EXIT
trap 'exit 0' TERM INT
hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
for iteration in $(seq 1 60); do
  if hdfs dfs -mkdir -p /ml/raw /ml/staging /ml/published /ml/jobhistory/tmp /ml/jobhistory/done /ml/yarn-logs; then
    break
  fi
  sleep 2
done
mapred --daemon start historyserver
echo 'Single-node HDFS/YARN ready; replication=1, not physical-node high availability'
while true; do
  sleep 15 & wait $!
  for service in namenode datanode resourcemanager nodemanager historyserver; do
    # Drain the entire pipeline: grep -q can close early, SIGPIPE its producer,
    # and make pipefail report a healthy daemon as stopped under larger loads.
    if ! ps -eo args | grep -v grep | grep -i "${service}" >/dev/null; then
      echo "Hadoop service stopped: ${service}" >&2
      exit 3
    fi
  done
done
