# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#!/bin/bash
# Builds the SIFT10M vector index on the A cluster (sh-a-spark). One task per executor, so a
# task gets the whole executor memory: hnswlib builds the graph in native memory, which counts
# against the container limit but not against the Java heap.
#
# The jars are baked into the image rather than passed through spark.jars: the company image ships
# Iceberg 1.9.1 and parquet 1.13.1 on the JVM's own classpath, which shadows the trial's jars, and
# a session extension is not loaded through the child-first loader that userClassPathFirst would fix.
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.3.1
export HADOOP_CONF_DIR=/opt/hadoop-3.3.1/etc/hadoop
export SPARK_HOME=/opt/spark
# required: the kubeconfig of the cluster the job runs on
export KUBECONFIG=${KUBECONFIG:?set KUBECONFIG to the target cluster}
API=https://10.130.17.1:6443
IMG=${IMG:?set IMG to the image built from tools/cluster/Dockerfile}
NN=hdfs://hdfs-namenode-0001.host.platform.basemind.com:8020
TABLE=${1:?table, for example db.t}
MAXENT=${2:-262144}
EXEC_MEM=${3:-5g}
EXEC_OVER=${4:-2g}
EXEC_CORES=${5:-1}
EXEC_N=${6:-6}
TASK_CPUS=${7:-1}
exec $SPARK_HOME/bin/spark-submit \
  --master k8s://$API \
  --deploy-mode cluster \
  --name sparkann-index-build \
  --class org.apache.iceberg.spark.index.ClusterIndexBuild \
  --conf spark.kubernetes.container.image=$IMG \
  --conf spark.kubernetes.namespace=spark \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark-ops-admin-sa \
  --conf spark.kubernetes.file.upload.path=${UPLOAD_PATH:-$NN/tmp/spark-upload} \
  --conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
  --conf spark.kubernetes.driver.label.app=sparkann-e2e \
  --conf spark.kubernetes.executor.label.app=sparkann-e2e \
  --conf spark.executor.instances=$EXEC_N \
  --conf spark.executor.cores=$EXEC_CORES \
  --conf spark.executor.memory=$EXEC_MEM \
  --conf spark.executor.memoryOverhead=$EXEC_OVER \
  --conf spark.driver.memory=4g \
  --conf spark.driver.cores=2 \
  --conf spark.task.cpus=$TASK_CPUS --conf spark.driver.extraJavaOptions=--add-opens=java.base/java.nio=ALL-UNNAMED --conf spark.executor.extraJavaOptions="--add-opens=java.base/java.nio=ALL-UNNAMED -Xss8m" \
  --conf spark.hadoop.fs.defaultFS=$NN \
  "${CLUSTER_BUILD_JAR:-cluster-build.jar}" \
  $NN/spark-warehouse/ thrift://10.130.17.34:9083 $TABLE $MAXENT ${STRUCT:-HNSW} ${QUANT:-NONE} "${OPTS:-}" "${NAME:-}"
