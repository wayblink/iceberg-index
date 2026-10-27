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
# Test image for the Iceberg index end-to-end trial.
#
# The company Spark image is the base, unchanged except for its jars: the Iceberg runtime jar and the
# parquet and avro jars it ships are removed, and the jars the trial needs are put in their place.
#
# Why: the image ships Iceberg 1.9.1 and parquet 1.13.1 in /opt/spark/jars, which is on the JVM's own
# classpath. Two things go wrong with that. The Iceberg jar shadows the trial's jars, so the session
# extension resolved to the image's 1.9.1 copy, which has no vec_distance and every query planned as a
# plain scan; and parquet 1.13.1 cannot serve parquet-variant, which the trial's Iceberg needs, so
# writing failed with a missing logical type. Handing the jars over through spark.jars instead does
# not work either: with user class path first they are only reachable through the child-first loader,
# which is not what loads a session extension, and without it the image's jars win.
#
# So the jars the trial needs are baked in, and the ones they replace are taken out. Java stays the
# image's 17.0.18 and the Spark distribution stays the image's.
FROM hub.i.basemind.com/spark/spark:3.5.7-STEP-rc2-tpcds-dsdgen-lance-20260416a
LABEL purpose="sparkann e2e: company spark image with iceberg/parquet/avro jars replaced by the trial's"
RUN rm -f /opt/spark/jars/iceberg-spark-runtime-*.jar \
          /opt/spark/jars/parquet-*.jar \
          /opt/spark/jars/avro-1.11.4.jar \
          /opt/spark/jars/avro-ipc-1.11.4.jar \
          /opt/spark/jars/avro-mapred-1.11.4.jar \
    && if ls /opt/spark/jars/ | grep -qiE "iceberg|^parquet-|^avro-1\.11"; then echo "replaced jars still present"; exit 1; fi \
    && echo "replaced jars removed"
# image-jars/ is built next to this file and checked against image-jars.txt
COPY image-jars/ /opt/spark/jars/
RUN ls /opt/spark/jars/ | grep -cE "iceberg|parquet|avro" \
    && ls /opt/spark/jars/ | grep -E "iceberg-spark-extensions|parquet-avro|hnswlib" 
