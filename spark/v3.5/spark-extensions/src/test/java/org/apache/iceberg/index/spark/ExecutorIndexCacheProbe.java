package org.apache.iceberg.index.spark;

/** Reads the executor cache's counters from a test that runs the executor in the same JVM. */
public final class ExecutorIndexCacheProbe {
  private ExecutorIndexCacheProbe() {}

  public static ExecutorIndexCache.Stats stats() {
    return ExecutorIndexCache.get().stats();
  }

  public static String config(org.apache.spark.sql.SparkSession spark) {
    ExecutorIndexCache.Config config = ExecutorIndexCache.Config.from(spark);
    return "enabled=" + config.enabled() + " bytes=" + config.maxBytes() + " maxEntry=" + config.maxEntryBytes();
  }
}
