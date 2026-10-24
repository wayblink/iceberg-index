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
 *                         BENCHSCAN. A brute force index is measured through the search procedure
 *                         instead, because the rewrite rule refuses one: it scores every entry, so
 *                         running it is slower than scanning, and the rule stands aside on purpose.
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

  /**
   * Prints what the index cache holds, one line per executor plus the summary.
   *
   * <p>Coverage is the fraction of a query's index files that stayed resident. A derived budget
   * that cannot reach 1.0 is a budget below the working set, and the latency of every query after
   * it is the latency of an eviction rather than of a search.
   */
  private static void reportCoverage(SparkSession spark) {
    for (Row row : spark.sql("CALL hms.system.index_cache_status()").collectAsList()) {
      emit(
          "COVERAGE {\"executor\":\"" + row.getString(0) + "\",\"mode\":\"" + row.getString(3)
              + "\",\"entries\":" + row.getLong(4) + ",\"bytes\":" + row.getLong(5)
              + ",\"hits\":" + row.getLong(6) + ",\"misses\":" + row.getLong(7)
              + ",\"evictions\":" + row.getLong(9) + ",\"coverage\":" + row.getDouble(10)
              + ",\"workingSetBytes\":" + row.getLong(11) + ",\"stale\":" + row.getBoolean(14)
              + "}");
    }
  }

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
    // rule: through ORDER BY, which is what a user writes, and where over-sample matters.
    // procedure: straight at one index, for the structures the rule declines to use.
    String mode = args.length > 12 ? args[12] : "rule";

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
    // with the registry in a table property, a catalog that caches the table answers from the
    // metadata it first loaded; an index built minutes ago is invisible until the table is refreshed
    try {
      spark.catalog().refreshTable(base);
    } catch (RuntimeException e) {
      // a catalog that cannot refresh still lets the run continue against what it has
    }

    List<Row> listed =
        spark.sql("CALL hms.system.list_indexes(table => '" + shortName(base) + "')")
            .collectAsList();
    for (Row index : listed) {
      emit(META + "{\"index\":\"" + index.toString().replace("\"", "'") + "\"}");
    }

    // which of those rows each arm is, so a report shows the arm's own definition rather than every
    // index the column holds; the bytes it occupies is one of the things being compared, so it is
    // measured here rather than written down by hand
    org.apache.iceberg.Table icebergTable;
    try {
      icebergTable = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, base);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    for (String spec : indexNames) {
      int hash = spec.indexOf('#');
      String key = (hash < 0 ? spec : spec.substring(0, hash)).trim();
      String label = (hash < 0 ? spec : spec.substring(hash + 1)).trim();
      for (Row index : listed) {
        if (!"-".equals(key) && index.toString().contains(key)) {
          org.apache.iceberg.index.IndexMetadata metadata =
              org.apache.iceberg.index.spark.VectorIndexes.findIndex(
                  icebergTable, column, "vector", "-".equals(key) ? null : key);
          long bytes = 0L;
          int files = 0;
          for (org.apache.iceberg.index.IndexSnapshot snapshot : metadata.snapshots()) {
            org.apache.iceberg.index.IndexManifestList manifestList =
                org.apache.iceberg.index.IndexManifestList.read(
                    icebergTable.io().newInputFile(snapshot.indexManifestList()));
            for (org.apache.iceberg.index.IndexManifestList.ManifestRef ref
                : manifestList.manifests()) {
              for (org.apache.iceberg.index.IndexFile file
                  : org.apache.iceberg.index.IndexManifest.read(
                      icebergTable.io().newInputFile(ref.location())).indexFiles()) {
                bytes += icebergTable.io().newInputFile(file.location()).getLength();
                files += 1;
              }
            }
          }

          org.apache.iceberg.index.vector.VectorIndexDefinition definition =
              (org.apache.iceberg.index.vector.VectorIndexDefinition) metadata.definition();
          emit(META + "{\"arm\":\"" + label + "\""
              + ",\"definition\":\"" + index.toString().replace("\"", "'") + "\""
              + ",\"indexBytes\":" + bytes
              + ",\"indexFiles\":" + files
              + ",\"structure\":\"" + definition.structure() + "\""
              + ",\"quantizer\":\"" + definition.quantizer() + "\""
              + ",\"properties\":\"" + definition.properties().toString().replace("\"", "'")
                  + "\""
              + "}");
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
    if ("coverage".equals(mode)) {
      // a search first, because the status call reports what the executors last told the driver and
      // without a search they have told it nothing. The budget is decided before the search, so
      // what it leaves resident is what any later query would get.
      spark
          .sql(
              "SELECT pk FROM " + base + " ORDER BY vec_distance(" + column + ", "
                  + vectors.get(warmup) + ") LIMIT " + topK)
          .collectAsList();
      for (String indexName : indexNames) {
        String label = indexName.contains("#")
            ? indexName.substring(indexName.indexOf('#') + 1) : indexName.trim();
        if (!"-".equals(label)) {
          // a named arm is searched directly so that the arm asked about is the one that loaded
          spark
              .sql(
                  "CALL hms.system.vector_search('" + shortName(base) + "', '" + column + "', "
                      + vectors.get(warmup) + ", " + topK + ", '" + label + "')")
              .collectAsList();
        }
      }

      reportCoverage(spark);
    } else if ("procedure".equals(mode)) {
      for (int q = warmup; q < warmup + measured; q++) {
        for (String indexName : indexNames) {
          String label = indexName.contains("#")
              ? indexName.substring(indexName.indexOf('#') + 1) : indexName.trim();
          for (int repeat = 1; repeat <= repeats; repeat++) {
            long start = System.nanoTime();
            List<Row> hits =
                // positional, because the named form is resolved differently by some versions and
                // the only thing this call needs to be is unambiguous
                spark
                    .sql(
                        "CALL hms.system.vector_search('" + shortName(base) + "', '" + column
                            + "', " + vectors.get(q) + ", " + topK + ", '" + label + "')")
                    .collectAsList();
            long millis = (System.nanoTime() - start) / 1_000_000;

            int found = 0;
            for (Row hit : hits) {
              String row = hit.getString(3);
              if (row == null) {
                continue;
              }
              java.util.regex.Matcher matcher =
                  java.util.regex.Pattern.compile("\"pk\":(-?\\d+)").matcher(row);
              if (matcher.find() && truth.get(q).contains(Long.parseLong(matcher.group(1)))) {
                found += 1;
              }
            }

            emit(
                RESULT
                    + "{\"index\":\"" + label + "\""
                    + ",\"path\":\"procedure\""
                    + ",\"over\":0"
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

      List<Long> procedureScans = new ArrayList<>();
      for (int q = warmup; q < warmup + measured; q++) {
        procedureScans.add(scanMillis.get(q));
      }
      emit(SUMMARY + "{\"kind\":\"scan\"," + statistics(procedureScans)
          + ",\"queries\":" + measured + "}");
      emit(SUMMARY + "{\"kind\":\"done\"}");
      file.close();
      spark.stop();
      return;
    }

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
