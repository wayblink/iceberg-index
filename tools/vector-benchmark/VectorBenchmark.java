/*
 * Measures a vector index against a full scan, over a grid of the settings a search reads.
 *
 * The index answer and the exact answer are both asked for as SQL, so what is measured is what a
 * user gets: `ORDER BY vec_distance(...) LIMIT k` goes through the rewrite rule, and the same
 * ordering wrapped in an addition the rule does not recognize is answered by a scan. The exact
 * answer is also the ground truth a recall figure is taken against, so it is asked for once per
 * query and reused by every setting.
 *
 * The settings are visited per query rather than setting by setting: the cluster is shared and the
 * resident index cache warms as the run goes, so a setting measured last would otherwise look
 * better or worse than the same setting measured first.
 *
 * Every measurement is printed as one line with a fixed prefix, so a run is a log file that a report
 * can be built from without re-running anything:
 *
 *   BENCHMETA <json>      what was measured: tables, index definition, top k, settings
 *   BENCHSCAN <json>      one exact query: its latency and the rows it returned
 *   BENCHRESULT <json>    one indexed query: index, setting, latency, hits, and its recall against
 *                         BENCHSCAN
 *   BENCHSUMMARY <json>   closing marker, so a truncated run is recognizable
 *
 * The lines are also written to the output path the caller gives, one flush each. A run takes tens of
 * minutes on a shared cluster, and a driver log that is followed over a network connection is lost
 * when that connection is, which turns a finished run into no data at all; a file that is flushed as
 * it goes keeps every measurement that was taken, even if the driver never reaches its summary.
 */
package org.apache.iceberg.spark.index;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class VectorBenchmark {

  private static final String META = "BENCHMETA ";
  private static final String SCAN = "BENCHSCAN ";
  private static final String RESULT = "BENCHRESULT ";
  private static final String SUMMARY = "BENCHSUMMARY ";

  private static Writer file;
  private static org.apache.hadoop.fs.FSDataOutputStream stream;

  /** Prints a line and appends it to the run's output file, so a lost log loses no measurement. */
  private static void emit(String line) {
    System.out.println(line);
    if (file != null) {
      try {
        file.write(line);
        file.write("\n");
        file.flush();
        // flush() only empties the Java buffers: HDFS does not show a reader what is still in the
        // DataNode pipeline until it is told to, which is the point of writing as the run goes
        stream.hflush();
      } catch (java.io.IOException e) {
        throw new java.io.UncheckedIOException("Failed to write the benchmark output", e);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String warehouse = args[0];
    String hms = args[1];
    String base = args[2];
    String queryTable = args[3];
    String column = args[4];
    int topK = Integer.parseInt(args[5]);
    int queries = Integer.parseInt(args[6]);
    int warmup = Integer.parseInt(args[7]);
    int repeats = Integer.parseInt(args[8]);
    String outPath = args[9];
    String[] overSamples = args[10].split(",");
    // several indexes may sit on one column, told apart by name; "-" means leave the column's
    // default alone, which is what a table with a single index needs. An arm may be written
    // `name#label` so a UUID, which is how an index built without a name is addressed, still prints
    // as something a report can show
    String[] indexNames = args.length > 11 ? args[11].split(",") : new String[] {"-"};

    SparkSession spark =
        SparkSession.builder()
            .appName("sparkann-vector-benchmark")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.hms", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.hms.type", "hive")
            .config("spark.sql.catalog.hms.uri", hms)
            .config("spark.sql.catalog.hms.warehouse", warehouse)
            .getOrCreate();

    org.apache.hadoop.fs.Path target = new org.apache.hadoop.fs.Path(outPath);
    org.apache.hadoop.fs.FileSystem filesystem =
        target.getFileSystem(spark.sparkContext().hadoopConfiguration());
    stream = filesystem.create(target, true);
    file = new BufferedWriter(new OutputStreamWriter(stream, StandardCharsets.UTF_8));

    // what is being measured, so a report can say which index and which table it describes
    emit(
        META
            + "{\"base\":\""
            + base
            + "\",\"queries\":\""
            + queryTable
            + "\",\"column\":\""
            + column
            + "\",\"topK\":"
            + topK
            + ",\"measuredQueries\":"
            + queries
            + ",\"warmupQueries\":"
            + warmup
            + ",\"repeats\":"
            + repeats
            + ",\"overSamples\":["
            + join(overSamples)
            + "],\"indexes\":["
            + quoted(indexNames)
            + "],\"sparkVersion\":\""
            + spark.version()
            + "\"}");
    List<Row> listed =
        spark.sql("CALL hms.system.list_indexes(table => '" + shortName(base) + "')")
            .collectAsList();
    for (Row index : listed) {
      emit(META + "{\"index\":\"" + index.toString().replace("\"", "'") + "\"}");
    }

    // which of those rows each arm is, so a report shows the arm's own definition rather than every
    // index the column holds
    for (String spec : indexNames) {
      int hash = spec.indexOf('#');
      String key = (hash < 0 ? spec : spec.substring(0, hash)).trim();
      String label = (hash < 0 ? spec : spec.substring(hash + 1)).trim();
      for (Row index : listed) {
        if (!"-".equals(key) && index.toString().contains(key)) {
          emit(META + "{\"arm\":\"" + label + "\",\"definition\":\""
              + index.toString().replace("\"", "'") + "\"}");
        }
      }
    }

    List<String> vectors = new ArrayList<>();
    List<Set<Long>> truth = new ArrayList<>();
    List<Long> scanMillis = new ArrayList<>();
    for (int q = 0; q < warmup + queries; q++) {
      List<Row> rows =
          spark.sql("SELECT vector FROM " + queryTable + " WHERE pk = " + q).collectAsList();
      if (rows.isEmpty()) {
        emit(META + "{\"query\":" + q + ",\"missing\":true}");
        break;
      }

      List<Float> values = rows.get(0).getList(0);
      StringBuilder literal = new StringBuilder("array(");
      for (int i = 0; i < values.size(); i++) {
        if (i > 0) {
          literal.append(',');
        }
        literal.append(String.format(Locale.ROOT, "%sd", values.get(i)));
      }
      String query = literal.append(')').toString();
      vectors.add(query);

      // the exact answer, and the scan the index is compared against
      long start = System.nanoTime();
      List<Row> scanned =
          spark
              .sql(
                  "SELECT pk FROM " + base + " ORDER BY vec_distance(" + column + ", " + query
                      + ") + 0.0d LIMIT " + topK)
              .collectAsList();
      long millis = (System.nanoTime() - start) / 1_000_000;
      Set<Long> pks = new HashSet<>();
      StringBuilder ids = new StringBuilder();
      for (Row row : scanned) {
        pks.add(row.getLong(0));
        ids.append(ids.length() == 0 ? "" : ",").append(row.getLong(0));
      }
      truth.add(pks);
      scanMillis.add(millis);
      emit(
          SCAN + "{\"query\":" + q + ",\"warmup\":" + (q < warmup) + ",\"ms\":" + millis
              + ",\"hits\":" + scanned.size() + ",\"pks\":[" + ids + "]}");

      if (q < warmup) {
        // the index path pays for its own warm-up: the first search reads the files and builds the
        // cache that the measured queries then reuse
        spark.sql(
            "SELECT pk FROM " + base + " ORDER BY vec_distance(" + column + ", " + query
                + ") LIMIT " + topK)
            .collectAsList();
      }
    }

    int measured = Math.min(queries, vectors.size() - warmup);
    for (int q = warmup; q < warmup + measured; q++) {
      for (String indexName : indexNames) {
        String spec = indexName.trim();
        int hash = spec.indexOf('#');
        String arm = hash < 0 ? spec : spec.substring(0, hash);
        String label = hash < 0 ? spec : spec.substring(hash + 1);
        if ("-".equals(arm)) {
          spark.conf().unset("spark.iceberg.index.vector.name");
        } else {
          spark.conf().set("spark.iceberg.index.vector.name", arm);
        }
        for (String overSample : overSamples) {
          spark.conf().set("spark.iceberg.index.vector.over-sample", overSample.trim());
          for (int repeat = 1; repeat <= repeats; repeat++) {
            long start = System.nanoTime();
            List<Row> hits =
                spark
                    .sql(
                        "SELECT pk FROM " + base + " ORDER BY vec_distance(" + column + ", "
                            + vectors.get(q) + ") LIMIT " + topK)
                    .collectAsList();
            long millis = (System.nanoTime() - start) / 1_000_000;

            int found = 0;
            for (Row hit : hits) {
              if (truth.get(q).contains(hit.getLong(0))) {
                found += 1;
              }
            }

            emit(
                RESULT
                    + "{\"index\":\"" + ("-".equals(label) ? "" : label) + "\""
                    + ",\"over\":" + Integer.parseInt(overSample.trim())
                    + ",\"repeat\":" + repeat
                    + ",\"query\":" + q
                    + ",\"ms\":" + millis
                    + ",\"hits\":" + hits.size()
                    + ",\"found\":" + found
                    + ",\"recall\":" + String.format(Locale.ROOT, "%.6f",
                        topK == 0 ? 0.0 : found / (double) topK)
                    + "}");
          }
        }
      }
    }

    spark.conf().unset("spark.iceberg.index.vector.over-sample");
    spark.conf().unset("spark.iceberg.index.vector.name");

    // the scan baseline as a summary too, so a report does not have to recompute it
    List<Long> measuredScans = new ArrayList<>();
    for (int q = warmup; q < warmup + measured; q++) {
      measuredScans.add(scanMillis.get(q));
    }
    emit(
        SUMMARY + "{\"kind\":\"scan\"," + statistics(measuredScans) + ",\"queries\":" + measured
            + "}");
    emit(SUMMARY + "{\"kind\":\"done\"}");
    file.close();
    spark.stop();
  }

  /** The usual quartiles of a set of latencies, in milliseconds. */
  private static String statistics(List<Long> values) {
    if (values.isEmpty()) {
      return "\"median\":0,\"min\":0,\"p25\":0,\"p75\":0,\"max\":0,\"mean\":0,\"count\":0";
    }

    List<Long> sorted = new ArrayList<>(values);
    Collections.sort(sorted);
    long sum = 0;
    for (long value : sorted) {
      sum += value;
    }

    return "\"median\":"
        + percentile(sorted, 0.5)
        + ",\"min\":"
        + sorted.get(0)
        + ",\"p25\":"
        + percentile(sorted, 0.25)
        + ",\"p75\":"
        + percentile(sorted, 0.75)
        + ",\"max\":"
        + sorted.get(sorted.size() - 1)
        + ",\"mean\":"
        + Math.round(sum / (double) sorted.size())
        + ",\"count\":"
        + sorted.size();
  }

  private static long percentile(List<Long> sorted, double fraction) {
    int index = (int) Math.round(fraction * (sorted.size() - 1));
    return sorted.get(Math.min(sorted.size() - 1, Math.max(0, index)));
  }

  private static String quoted(String[] values) {
    StringBuilder text = new StringBuilder();
    for (String value : values) {
      String arm = value.trim();
      text.append(text.length() == 0 ? "" : ",").append('"').append("-".equals(arm) ? "" : arm)
          .append('"');
    }
    return text.toString();
  }

  private static String join(String[] values) {
    StringBuilder text = new StringBuilder();
    for (String value : values) {
      text.append(text.length() == 0 ? "" : ",").append(Integer.parseInt(value.trim()));
    }
    return text.toString();
  }

  /** {@code hms.sparkann.sift10m_base} to {@code sparkann.sift10m_base}, which is what CALL takes. */
  private static String shortName(String table) {
    return table.substring(table.indexOf('.') + 1);
  }
}
