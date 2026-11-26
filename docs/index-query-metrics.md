# 索引查询指标：一次查询的时间花在哪

## 一句话结论

`SELECT … FROM vector_search(…)` 这类融合查询，现在会按 task 上报 **四段墙钟**（读索引 / 探测打分 / 回表 / 合并）、一个用于对账的 `taskTimeNs`，以及索引读取的字节与请求数；全部默认开启，没有新增任何配置项。指标走 Spark 的 SQL metric 通道，所以 Spark UI 的 SQL 页能看，`spark.eventLog` 里也能按 task 读出来做归因。

## 指标表

| 指标名 | event log 里的 description（契约） | 口径 |
| --- | --- | --- |
| `indexReadTimeNs` | `time a task spent opening and reading index files (ns)` | 打开索引文件（整文件读或 range 打开，含缓存命中判断）加上 range 路径在搜索中读 list 块的时间 |
| `probeScoreTimeNs` | `time a task spent probing lists and scoring their records (ns)` | 内核探测与打分：选 list、解码、距离计算、top-k 维护；已扣掉其中的 list 块读时间，但块缓存的锁等待仍计在这一段 |
| `fetchBackTimeNs` | `time a task spent fetching the rows its hits name (ns)` | 回表：`candidatesOf`（prefilter 时读过滤列）与 `rowsOf`（按位置读数据文件、判删除、判过滤） |
| `mergeTimeNs` | `time a task spent merging and building its answer (ns)` | task 内排序 / 精确重排 / 回表后的过滤与去重 / 组装输出行 |
| `taskTimeNs` | `time a task spent on its index files (ns)` | 一个 task 处理其 index file 的总墙钟；用于对账，不是 Spark 的 task 时长 |
| `numIndexFileBytesRead` | `bytes a task read to load index files whole` | 缓存未命中时整文件读取的字节数（等于索引文件长度） |
| `numIndexRangeBytesRead` | `bytes a task read from index files in ranges` | range 路径读的字节：容器 footer + 描述区（centroid / list 目录）+ 选中的 list 块 |
| `numIndexRangeReadRequests` | `range requests a task made to index files` | 上面这些字节对应的读请求数 |
| `numIndexListBlocksRead` | `index list blocks a task read through ranges` | 搜索消费的 list 块数：块缓存命中或从存储读都算，每次 `IvfListRanges.read` 计 1（不是 range 读请求数，请求数看 `numIndexRangeReadRequests`） |
| `numIndexWholeListAreaReads` | `searches that read the whole list area instead of their ranges` | 规划判断逐段读不划算（选中的范围密集或散扩、待读字节已接近整个 list 区）时，改为一次读整个 list 区的次数 |
| `numIndexCacheHits` | `index files an executor already held` | 打开索引文件时内存里已有该文件的次数（缓存开启时才有意义） |
| `numIndexCacheMisses` | `index file opens that read from storage` | 打开索引文件时付了存储 IO 的次数；`cache.enabled=false` 时每次打开都记一次 |
| `numIndexBlockCacheHits` | `index list blocks an executor already held` | 本次搜索中块缓存命中的 list 块数（per-search；不是 executor 生命周期累计，一个 task 可把它打开的索引文件的计数相加） |
| `numIndexBlockCacheMisses` | `index list blocks a task read because the executor did not hold them` | 本次搜索中块未命中的次数（未命中的块从存储读；读成功才可能回填） |
| `numIndexBlockLoads` | `index list blocks a task added to the executor cache` | 本次搜索中**真正进入**块缓存的 list 块数（被预算拒绝的块不计） |
| `numIndexBlockEvictions` | `index list blocks the executor cache evicted` | 本次搜索的装入按预算逐出的块数（per-search；空闲 TTL 淘汰不计入这个计数） |

四段之和覆盖 `taskTimeNs` 的绝大部分（验收容差 <5%，实测通常 <1%），但**不要把它当严格等式**：回表之后的过滤/去重计入 merge，块缓存的锁等待计入 probe-score，所以这两段都比字面含义稍宽。它仍是"指标没漏算"的自检：判断一条查询慢在哪时，先看四段的占比，再看 `numIndexFileBytesRead` / `numIndexRangeBytesRead` 判断是读得太多还是算得太慢。

## 怎么读

1. **Spark UI**：查询的 SQL 页里，`VectorSearchSourceExec` 节点的 metrics 列表直接显示上面这些值。
2. **Spark event log**：每个 task 结束事件里，`Task Info.Accumulables` 带这些指标（按 description 匹配，`Update` 是该 task 的增量，`Value` 是累计值，求和只能用 `Update`）。
3. **基准工具**：外部工具仓 `iceberg-tools`（不在本仓库内，也不参与本仓的构建与 CI）的 `format_benchmark.eventlog` 已经把 iceberg 的 description 映射成上表的短名，输出在 `iceberg_metrics` 里，同时 `search_metrics` 合并了 Lance 与 iceberg 两类指标，便于跨格式比较；`--search-only` 现在把 `VectorSearchSource` / `VectorSearchHitsRequest` 也算作搜索执行。

## 与 Lance 侧的关系

Lance 的分布式 `VECTOR_SEARCH` 从 #3089 起上报 12 个读路径指标（`numBytesRead` / `numIops` / `datasetOpenTimeNs` / `scannerCreateTimeNs` / `batchLoadTimeNs` 等）。iceberg 侧这组指标沿用同样的风格（`num…` 计数、`…TimeNs` 计时），目的是让同一条基准里两个格式的"读索引 / 打分 / 回表"可以并排看，而不是只看一个总时长。

## 已知边界（别用这些指标回答的问题）

- **回表的字节数与请求数没有报**：`lance-iceberg` 格式的回表走 Lance SDK，不经过 `InputFile`，无法在现有读路径上计数；`parquet` 格式的 `readFileWithFormat` 同样没有暴露 IO 计数。回表先用 `fetchBackTimeNs` + 已有的 `numFetchedRows` / `numRowsAtFiles` 代理，字节级归因留给回表重写（#3122）。
- **只有融合路径有这些指标**：`CALL system.vector_search` / `vector_search_batch` 与地址类表函数 `vector_index_search` / `vector_index_search_batch` 走的是 driver 侧搜索（`VectorSearchBridge`，CALL 默认还回表），不经过 `searchWithRows`，因此不产出这组指标。
- **range 与整文件缓存不是同一种缓存**：`range.read.enabled=true` 时查询不读整文件，所以 `numIndexCacheHits` 通常为 0、`numIndexCacheMisses` 约等于索引文件数；range 路径复用内存靠的是 **list 块缓存**（`numIndexBlockCacheHits`/`numIndexBlockCacheMisses`），它与整文件缓存共享同一个开关与预算（`spark.iceberg.index.cache.enabled` / `spark.iceberg.index.cache.bytes`）：块从整文件占用后的剩余预算里装入，整文件装入需要空间时也会回收块；块与整文件共用同一份预算，但**块不计入 `index_cache_status` 的 `usedBytes`**（那是整文件的内存模型投影），所以看占用时 `usedBytes` 是下界；空闲 TTL 淘汰与 `offload_index` 释放索引都会清掉对应的块。命中块时 `numIndexRangeBytesRead` 只剩容器 footer 与描述区。
- **`taskTimeNs` 不是 Spark 的 task 时长**：它只覆盖 task 处理索引文件的那段墙钟，不含调度、反序列化广播、框架开销；和 event log 的 `tasks` 相位对比时，后者还会包含 task 之间的空隙与尾部。
- **指标只在有 task 的执行里出现**：没有 task 的计划/管理命令（如 `CALL index_cache_status`）不会带这些 accumulable。

## 名称是契约

event log 按 description 匹配指标，短名只在 UI 与工具里用。改 description 等于静默丢指标：产品侧 `DistributedIndexSearch.SearchStages.DESCRIPTIONS` 把 16 个 metrics 的 description 钉成了集合；外部工具仓 `iceberg-tools`（不在本仓库内，也不参与本仓的构建与 CI）的 `tests/test_eventlog.py` 按同一组 description 匹配。改产品侧的 description 时必须同步工具仓，否则是工具仓那边的测试才会红，本仓测试不会发现。
