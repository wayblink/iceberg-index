#!/bin/bash
# Runs the vector benchmark on the A cluster (sh-a-spark) and keeps everything it printed.
#
#   ./run-benchmark.sh <out-name> <base-table> <query-table> \
#       [topK] [queries] [warmup] [repeats] [over-samples] [indexes] [mode]
#
# [mode] is `rule` (default: ORDER BY, which is what a user writes and where over-sample matters) or
# `procedure` (CALL vector_search: one index, no over-sample, for the structures the rule declines).
#
# <indexes> is a comma separated list of index names to sweep, or "-" for the column's default. A
# column that holds several indexes is measured one arm per name, in one run and on the same rows,
# which is what makes two parameter sets comparable.
#
# The measurements are written to <base>/bench/results/<out>.log from the run's own output file on
# HDFS, not from the driver log: a log followed over a network connection dies with the connection,
# and a run of tens of minutes is not worth losing to that.
#
# The benchmark class is compiled against the jars that are baked into the image, then handed to the
# cluster as the application jar. Everything it prints is captured into results/<out-name>.log, which
# is what build-report.py turns into a report: a run is never measured twice for a report.
#
# The jars must be the ones the image was built from (see image-jars-new/ and Dockerfile): a session
# extension is not loaded through spark.jars, so the image is the only way to change the code.
set -euo pipefail

OUT=${1:?out name, used for results/<out>.log}
BASE=${2:?base table, for example hms.sparkann.sift10m_base}
QUERY_TABLE=${3:?query table, for example hms.sparkann.sift10m_query}
TOPK=${4:-10}
QUERIES=${5:-15}
WARMUP=${6:-3}
REPEATS=${7:-2}
OVERS=${8:-1,3,10,30,100}
INDEXES=${9:--}
MODE=${10:-rule}

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.3.1
export HADOOP_CONF_DIR=/opt/hadoop-3.3.1/etc/hadoop
export SPARK_HOME=/opt/spark
export KUBECONFIG=/home/wanganyang/workspace/k8s-config/a-spark-ops-amdin-kubeconfig
API=https://10.130.17.1:6443
IMG=${IMG:-hub.i.basemind.com/spark/sparkann-e2e:20261021b}

# How much each executor may hold. A run has to fit the largest index it is asked to measure, and
# the largest one is the graph: at 100M it is 23.67 GB over six executors, which is 3.95 GB each.
# That is more than a heap-relative budget can derive, so the budget is given explicitly -- a run
# sized for the smaller arms would report the graph as an uncached scan of its own files.
EXECUTORS=${EXECUTORS:-6}
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-12g}
EXECUTOR_OVERHEAD=${EXECUTOR_OVERHEAD:-4g}
CACHE_BYTES=${CACHE_BYTES:-6g}
NN=hdfs://hdfs-namenode-0001.host.platform.basemind.com:8020
HERE=$(cd "$(dirname "$0")" && pwd)
RESULTS=$HERE/results
RUNS=$HERE/runs
mkdir -p "$RESULTS" "$RUNS"

echo ">>> compiling the benchmark against the image's jars"
rm -rf "$HERE/build"
mkdir -p "$HERE/build"
"$JAVA_HOME/bin/javac" -nowarn \
  -cp "$(ls "$HERE"/image-jars-new/*.jar | tr '\n' ':')/opt/spark/jars/*" \
  -d "$HERE/build" "$HERE/VectorBenchmark.java"
"$JAVA_HOME/bin/jar" cf "$HERE/vector-benchmark.jar" -C "$HERE/build" .

NAME=sparkann-bench
SUBMIT_LOG=$(mktemp)
echo ">>> submitting $OUT: $BASE over $OVERS (x$REPEATS), $QUERIES queries, indexes $INDEXES, mode $MODE"
nohup "$SPARK_HOME/bin/spark-submit" \
  --master "k8s://$API" \
  --deploy-mode cluster \
  --name "$NAME" \
  --class org.apache.iceberg.spark.index.VectorBenchmark \
  --conf spark.kubernetes.container.image="$IMG" \
  --conf spark.kubernetes.namespace=spark \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-ops-admin-sa \
  --conf spark.kubernetes.file.upload.path="$NN/user/wanganyang/sparkann/staging" \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  --conf spark.kubernetes.driver.label.app=sparkann-e2e \
  --conf spark.kubernetes.executor.label.app=sparkann-e2e \
  --conf spark.executor.instances="$EXECUTORS" \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory="$EXECUTOR_MEMORY" \
  --conf spark.executor.memoryOverhead="$EXECUTOR_OVERHEAD" \
  --conf spark.iceberg.index.cache.bytes="$CACHE_BYTES" \
  --conf spark.driver.memory=6g \
  --conf spark.driver.cores=2 \
  --conf spark.hadoop.fs.defaultFS="$NN" \
  --conf spark.driver.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED \
  --conf spark.executor.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED \
  "$HERE/vector-benchmark.jar" \
  "$NN/spark-warehouse/" thrift://10.130.17.34:9083 \
  "$BASE" "$QUERY_TABLE" vector "$TOPK" "$QUERIES" "$WARMUP" "$REPEATS" \
  "$NN/user/wanganyang/sparkann/bench/$OUT.jsonl" "$OVERS" "$INDEXES" "$MODE" \
  > "$SUBMIT_LOG" 2>&1 &
SUBMIT_PID=$!

# the submission id names the driver pod, so the follower cannot pick up a pod left over from an
# earlier run, which is what a name prefix alone does
SUBMISSION=""
for _ in $(seq 1 60); do
  SUBMISSION=$(grep -ho "spark:$NAME-[0-9a-f]*" "$SUBMIT_LOG" | head -1 | sed "s|spark:$NAME-||" || true)
  if [ -n "$SUBMISSION" ]; then
    break
  fi
  sleep 2
done

if [ -z "$SUBMISSION" ]; then
  echo "!!! no submission id in the submit log:" >&2
  tail -20 "$SUBMIT_LOG" >&2
  exit 1
fi
echo ">>> driver pod carries $SUBMISSION"

DRIVER=""
for _ in $(seq 1 60); do
  DRIVER=$(kubectl get pods -n spark --no-headers 2>/dev/null \
    | grep -- "-driver" | grep "$SUBMISSION" | awk '{print $1}' | head -1 || true)
  if [ -n "$DRIVER" ]; then
    break
  fi
  sleep 2
done

if [ -z "$DRIVER" ]; then
  echo "!!! no driver pod for $SUBMISSION" >&2
  exit 1
fi

# `kubectl logs` fails while the container is still being created, so the follow waits for the
# driver to be ready. Missing it means losing the run: a finished driver pod is deleted with it.
for _ in $(seq 1 120); do
  READY=$(kubectl get pod -n spark "$DRIVER" --no-headers 2>/dev/null | awk '{print $2}' || true)
  if [ "$READY" = "1/1" ]; then
    break
  fi
  sleep 3
done

echo ">>> following $DRIVER (Spark's own log, for progress only: $RUNS/$OUT.driver.log)"
kubectl logs -n spark -f "$DRIVER" > "$RUNS/$OUT.driver.log" 2>&1 || true
wait "$SUBMIT_PID" || true

echo ">>> collecting the measurements from HDFS"
"$HADOOP_HOME/bin/hdfs" dfs -cat "$NN/user/wanganyang/sparkann/bench/$OUT.jsonl" \
  > "$RESULTS/$OUT.log" 2>/dev/null || true

if ! grep -q "^BENCHMETA" "$RESULTS/$OUT.log" 2>/dev/null; then
  echo "!!! $RESULTS/$OUT.log has no BENCHMETA line: the run wrote nothing" >&2
  exit 1
fi

echo ">>> wrote $RESULTS/$OUT.log"
grep -E "^BENCHSUMMARY" "$RESULTS/$OUT.log" | tail -3 || true
echo ">>> done"
