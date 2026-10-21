#!/bin/bash
# Builds results/<out>.log for a finished run from the best source it has.
#
# A run writes its measurements twice: to HDFS as it goes, and to the driver log the follower keeps.
# Either can be the more complete one — HDFS only shows what the writer flushed before the driver
# exited, and the follower only shows what it captured before its connection dropped — so the run is
# read from whichever holds more measurements.
#
#   ./collect-results.sh <out>            # results/<out>.log, from the more complete source
set -euo pipefail

OUT=${1:?out name}
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.3.1
export HADOOP_CONF_DIR=/opt/hadoop-3.3.1/etc/hadoop
NN=hdfs://hdfs-namenode-0001.host.platform.basemind.com:8020
HERE=$(cd "$(dirname "$0")" && pwd)
RESULTS=$HERE/results
RUNS=$HERE/runs
mkdir -p "$RESULTS"

FROM_HDFS=$RESULTS/$OUT.from-hdfs.log
"$HADOOP_HOME/bin/hdfs" dfs -cat "$NN/user/wanganyang/sparkann/bench/$OUT.jsonl" \
  > "$FROM_HDFS" 2>/dev/null || true
grep -E "^BENCH" "$RUNS/$OUT.driver.log" 2>/dev/null > "$RESULTS/$OUT.from-driver.log" || true

hdfs_count=$(grep -c "^BENCHRESULT" "$FROM_HDFS" 2>/dev/null || true)
driver_count=$(grep -c "^BENCHRESULT" "$RESULTS/$OUT.from-driver.log" 2>/dev/null || true)

if [ "${hdfs_count:-0}" -ge "${driver_count:-0}" ]; then
  cp "$FROM_HDFS" "$RESULTS/$OUT.log"
  echo ">>> $OUT: $hdfs_count measurements from HDFS"
else
  cp "$RESULTS/$OUT.from-driver.log" "$RESULTS/$OUT.log"
  echo ">>> $OUT: $driver_count measurements from the driver log"
fi

grep -c "^BENCHSCAN" "$RESULTS/$OUT.log" || true
grep -q "^BENCHSUMMARY {\"kind\":\"done\"}" "$RESULTS/$OUT.log" \
  && echo ">>> run reached its closing summary" \
  || echo ">>> run did not reach its closing summary: the numbers may be partial"
