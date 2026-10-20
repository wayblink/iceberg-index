# Vector index benchmark

Measures a vector index against a full scan on the cluster, over a grid of the settings a search
reads, and turns the run into a self-contained HTML report.

It exists because the same measurement kept being done by hand: a probe program per question, a
hand-written table per answer, and no way to tell a run that finished from a run whose log was cut
short. A run is now a log file, and a report is that log file rendered.

## What it measures

Both sides are asked for as SQL, so what is measured is what a user gets:

| side | SQL | what answers it |
|---|---|---|
| index | `ORDER BY vec_distance(col, q) LIMIT k` | the rewrite rule, i.e. the index |
| exact | `ORDER BY vec_distance(col, q) + 0.0d LIMIT k` | a full scan, since the rule does not recognize the shape |

The exact side is also the ground truth recall is taken against, and it is asked for once per query
and reused by every setting.

`over-sample` (`spark.iceberg.index.vector.over-sample`) is the setting that decides how many
candidates the index hands to the exact re-scoring; 1 means off. `nprobe` is a property of the index
definition, so comparing nprobe values needs one index build per value.

## Running it

On `shai`, in `/data/workspace/sparkann/bench` (the directory holds a symlink to `image-jars-new`,
the jars the image was built from):

```bash
./run-benchmark.sh <out-name> <base-table> <query-table> [topK] [queries] [warmup] [repeats] [over-samples]

./run-benchmark.sh sift10m-over hms.sparkann.sift10m_base hms.sparkann.sift10m_query \
    10 15 3 2 1,3,10,30,100
```

* the measurements are collected from the run's own output file on HDFS
  (`/user/wanganyang/sparkann/bench/<out>.jsonl`), not from the driver log: a `kubectl logs -f`
  dies with its network connection, and a run of tens of minutes is not worth losing to that. The
  output file is flushed (`hflush`) after every line, so even a killed driver leaves its
  measurements behind.
* the driver log is still followed, into `runs/<out>.driver.log`, for progress and for a stack trace
  if the job fails.
* settings are measured per query rather than setting by setting, so a cluster that gets faster or
  slower over the run does not systematically favour one of them.
* the image is the only way to change the code being measured (a session extension is not loaded
  through `spark.jars`), so rebuild it with the jars and pass it as `IMG=...`.

## Building the report

```bash
./build-report.py results/*.log -o ../../reports/vector-index-benchmark.html \
    --title "IVF × RaBitQ 向量索引 benchmark（SIFT 1M / 10M）"
```

The report is one HTML file with inline CSS and inline SVG: no network, no JavaScript, no build step.
It carries the charts (recall and latency against the setting, the recall/latency trade-off, the
per-query spread), the summary table, the method and its caveats, and every raw measurement.

## Reading the numbers

* the vertical lines on the recall chart are min–max over (query × round): recall varies far more
  between queries than between rounds of the same query.
* latency is dominated by the tail on a shared cluster: read the median and the quartiles, and
  compare settings measured in the same run rather than numbers from different runs.
* a run that has no `BENCHSUMMARY {"kind":"done"}` line was cut short; the report says so.
