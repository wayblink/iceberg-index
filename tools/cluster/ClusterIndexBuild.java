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
/*
 * Builds a vector index on the cluster rather than on a laptop.
 *
 * spark-submit in cluster deploy mode has no interactive shell, so the procedure call that a
 * spark-sql session would make is made here instead. Everything else about the build is the
 * procedure's own: the driver plans, the executors build one payload per index file.
 */
package org.apache.iceberg.spark.index;

import org.apache.spark.sql.SparkSession;

public class ClusterIndexBuild {

  public static void main(String[] args) {
    String warehouse = args[0];
    String hms = args[1];
    String table = args[2];
    int maxEntries = Integer.parseInt(args[3]);
    String structure = args.length > 4 ? args[4] : "HNSW";
    String quantizer = args.length > 5 ? args[5] : "NONE";
    String options = args.length > 6 ? args[6] : "";
    // a column may hold several indexes, told apart by name, which is how two parameter sets are
    // compared on one table
    String name = args.length > 7 ? args[7] : "";

    SparkSession spark =
        SparkSession.builder()
            .appName("sparkann-index-build")
            .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.hms", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.hms.type", "hive")
            .config("spark.sql.catalog.hms.uri", hms)
            .config("spark.sql.catalog.hms.warehouse", warehouse)
            .getOrCreate();

    String buildSql =
        "CALL hms.system.build_vector_index("
            + "table => '"
            + table
            + "', column => 'vector', dimension => 128, metric => 'euclidean'"
            + ", structure => '"
            + structure
            + "', quantizer => '"
            + quantizer
            + "'"
            + (options.isEmpty() ? "" : ", options => map(" + options + ")")
            + (name.isEmpty() ? "" : ", name => '" + name + "'")
            + ", max_entries_per_index_file => "
            + maxEntries
            + ")";

    System.out.println("BUILD name=" + name);
    System.out.println(
        "BUILD structure=" + structure + " quantizer=" + quantizer + " options=[" + options + "]");
    System.out.println("BUILD sql=" + buildSql);

    long start = System.currentTimeMillis();
    spark.sql(buildSql).show(false);
    System.out.println("BUILD elapsed_ms=" + (System.currentTimeMillis() - start));

    spark.sql("CALL hms.system.list_indexes(table => '" + table + "')").show(false);
    spark.stop();
  }
}
