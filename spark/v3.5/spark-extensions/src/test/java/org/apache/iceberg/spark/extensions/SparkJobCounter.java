/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.extensions;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;

/**
 * How many Spark jobs a query costs.
 *
 * <p>A vector query is planned once and answered by one pass over the index files, so the work a
 * query costs is the work one job does. Anything that runs a second job to collect what the first
 * one found is spending a scheduling round and a driver barrier on something a task could have done,
 * and that is what this counts.
 */
final class SparkJobCounter {

  private SparkJobCounter() {}

  /** The number of jobs the body started, listening for the whole of it. */
  static int jobsDuring(SparkSession spark, Runnable body) {
    AtomicInteger started = new AtomicInteger();
    SparkListener listener =
        new SparkListener() {
          @Override
          public void onJobStart(SparkListenerJobStart jobStart) {
            started.incrementAndGet();
          }
        };

    spark.sparkContext().addSparkListener(listener);
    try {
      body.run();
    } finally {
      spark.sparkContext().removeSparkListener(listener);
    }

    return started.get();
  }
}
