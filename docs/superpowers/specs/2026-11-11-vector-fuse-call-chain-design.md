<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# 向量检索的调用链路：现状、job 归属与融合后的目标形态

日期：2026-11-11。关联：#3038（本设计的实施卡）、#3013（带过滤检索，已合入）、#3039（废弃 `auto`，已合入）。代码基线：`main @ 731ce9dccd`。

状态：第 2、3、4 步已实施（2026-11-11，分支 `card/3038`）；第 5、6 步未做。实施结果与本文的偏差见 §8。

## 1. 结论摘要

一条带过滤的向量查询，现在要起 **3 个 Spark job**（`pre` 链路 **5 个**）。其中**两个 job 是纯开销**：把第一批 job 找到的行收集回 driver，再把它们重新并行化发出去。融合的目标是把这两种形状都压到 **1 个 job**（存在"索引未覆盖文件"残差分支时为 2 个）。

关键事实：**执行侧的按位置读器已经存在**，且格式无关（Parquet 与 Lance 共用同一条路径），融合不需要新写读器、不需要新增依赖，只是把它从"RDD 里"挪到"检索任务里"。

## 2. 现状链路

### 2.1 计划期：规则与绑定

```
Spark SQL: SELECT ... FROM t WHERE <filter> ORDER BY vec_distance(col, q) LIMIT k
   │
   ├─ RewriteVectorSearch (Rule)                         spark/.../index/RewriteVectorSearch.scala
   │    ├─ eligibleChain(limit)          白名单：投影/别名/Sort/LocalLimit/SubqueryAlias/Filter/Scan
   │    ├─ globalSort + distanceOf       必须是"单键升序距离排序"
   │    ├─ filterOf(chain, column, table) 把非守卫过滤转成 Iceberg Expression（转不了则放弃改写）
   │    ├─ IndexRewriteSupport.icebergTable / hasOnlyIdentityPartitions / scanColumns / queryOf
   │    ├─ VectorSearchRequest.forTable(...)              请求：表、列、查询向量、k、快照、contract
   │    └─ BindVectorSearch.bind(spark, request)          冻结表快照 + 索引快照；规划 data 文件与 scanTasks
   │         ├─ VectorQueryParameters.decide(raw)         over-sample / filter-mode(pre|post)
   │         └─ VectorQueryContext{ request, inputs, parameters, budget, filter, filterColumns }
   │
   └─ 计划树替换：DataSourceV2Relation(scan)  →  VectorSearchSource(context)
```

`VectorSearchSource` 是叶子节点（`VectorSearchSource.scala:58`），`Sort` 与 `Limit` 留在它上面。因此**最终排序与精确重算由原计划的算子完成**，检索只负责产出候选行。

### 2.2 执行期：一个 job 变三个

```
VectorSearchStrategy → VectorSearchSourceExec.doExecute            VectorSearchStrategy.scala:47,131
   │
   └─ VectorSearchExec.rows(spark, source)                         VectorSearchExec.scala:64
        │
        ├─(A) candidatesFor(spark, context)                        VectorSearchExec.scala:93
        │      filter-mode=post → null
        │      否则 IndexEntries.matchingPositions(...) ─────────► job #1
        │           逐分区流式读过滤列，位置累积到 IndexCandidates（上限 200 万行）
        │
        ├─(B) candidateRows(spark, context, names, readSchema, candidates)   :251
        │      while (未凑够 k && 轮数 < MaxRounds) {
        │        DistributedIndexSearch.search(...)  ────────────► job #2
        │             一个索引文件一个分区：ExecutorIndexCache 取/载 payload
        │             → prepared.search(query, localTopK[, candidates])
        │             只把全局 top-k 的 (dataFile, rowPosition, score) 汇合回 driver
        │        IndexEntries.rowsAt(...)            ────────────► job #3
        │             tasksAt(命中文件) → liveRowsRdd → coalesce(readTaskCount) → collect()
        │             → Map[file, Map[pos, Row]]    ← 行回到 driver
        │        filter-mode=pre → 全部命中都是候选；post → IndexRowFilter 逐行判定，
        │             被拒的行计入 rejected，下一轮预算按 (k + rejected) 放大
        │      }
        │
        ├─(C) rowsToRdd(rowsToRdd:437)                            ────────────► job #4
        │     spark.createDataFrame(rows.asJava).queryExecution.toRdd
        │     → 把 driver 上的行重新并行化成 RDD
        │
        └─(D) residualRows(...)                                  ────────────► job #5
              "索引未覆盖的数据文件"整文件扫描（与检索无关的分支）
```

代码里几处自述可以印证：`VectorSearchExec.scala:55` 类注释写"candidate branch runs on the driver…not a promise about peak memory"，`:189` 的日志字段 `driverMaterialization=true` 是**硬编码字符串**；`IndexEntries.java:353-354` 的 `coalesce(readTaskCount(files)).collect()`；`:809-812` 的 `readTaskCount` 默认"每文件一个 task"。

### 2.3 实测 job 数（`local[2]`，200 行，IVF，`over-sample=10`）

| 形状 | job 数 | 说明 |
|---|---|---|
| 无过滤 | 3 | (B) 检索 + (B) 回表 + (C) 重新并行化 |
| `post` | 3 | 同上：行在 driver 上被判定，不额外起 job |
| `pre` | 5 | 上述 3 个 + (A) 的 `toLocalIterator` 相关 2 个 |

测量方式：`SparkJobCounter.jobsDuring(spark, body)` 监听 `SparkListener.onJobStart`。**不要**用日志字段当证据——`driverMaterialization=true` 改不改都不会变。

## 3. 目标链路

### 3.1 任务内融合

```
VectorSearchExec.rows (或新的 fused 入口)
   │
   ├─(A') 候选集：pre 链路下移为"每个检索任务只为自己覆盖的数据文件算"（第 4 步）
   │
   ├─(B') DistributedIndexSearch.searchWithRows(...)        ← 新重载，一个 job
   │      一个分区 = 一个索引文件（后续可细化到"索引文件 ↔ 其覆盖的数据文件"分组）
   │      任务内顺序：
   │        1) ExecutorIndexCache 取/载索引 payload（不变，仍受驻留管理）
   │        2) prepared.search(query, localTopK, candidates)  → 本分区命中的 (dataFile, pos, score)
   │        3) 对本分区覆盖的数据文件调用
   │             IndexEntries.readFileWithFormat(location, format, projection, schema, positions, conf)
   │           └─ FormatModelRegistry.readBuilder(format, Record.class, input)
   │                · Parquet → Iceberg parquet 格式模型
   │                · Lance    → Lance 集成注册的格式模型
   │              builder.positions(wanted)  → 只有实现了该能力的格式才据此跳过（实测：Lance 实现，Parquet 不实现）
   │        4) 应用 SnapshotDeletes（位置删除 / 等值删除），剔除已删除行
   │        5) 任务内维护本地 top-k；不足 k 时**在任务内**扩大预算重探（MaxRounds 下移）
   │        6) 输出 (Row, score)
   │
   ├─(C') 删除：不再需要 rowsToRdd —— 输出的已经是行
   │
   └─(D') residual 分支保持独立 job（索引未覆盖的整文件扫描）
```

driver 只做 `takeOrdered(localTopK)` 的最终归并，**不再持有命中行**。

### 3.2 为什么这条读器已经可用

`IndexEntries.readFileWithFormat`（`IndexEntries.java:833-880`）已经是执行侧、格式无关、按位置读：

```java
InputFile input = HadoopInputFile.fromLocation(location, taskConf(conf));
ReadBuilder<Record, Void> builder = FormatModelRegistry.readBuilder(format, Record.class, input);
if (positions != null && !positions.isEmpty()) { ...; builder.positions(wanted); }
try (CloseableIterable<Record> records = builder.project(projection).build()) { ... }
```

它返回 `Iterator<Tuple2<Row, Long>>`，**不依赖 `Dataset`**，所以能在 task 内调用。当前它被包在 `positionedRows` 的 `parallelize(singletonList(dataFile), 1).flatMapToPair(...)` 里，于是每个数据文件变成一个 RDD 分区、又是一个 job——这就是要拆掉的那层。

`IndexEntries.java:1296-1320` 的注释说明了格式无关的意图，并指出删除文件也按表格式读（Lance 表的删除文件是 Lance 文件）。

## 4. 关键约束（实施时必须守住）

1. **`rowsAt` 有四个调用者**：`VectorSearchExec`（本卡要改的）、`IndexSearch`（`vector_search` TVF / procedure）、`RewriteTextSearch`（全文检索）、`IndexRewriteSupport.rowsAt`。融合必须是**新增路径**，只接 `VectorSearchExec`，后三者语义不动、不改其实现。
2. **精确下发 `FileScanTask`**：任务只能拿到自己覆盖的数据文件对应的 scan task。现在 `inputs.scanTasks` 是全表列表随算子闭包下发，不收紧这点，省下的 task 启动会被闭包序列化吃回去。
3. **归并走的是行，不是位置**：`takeOrdered(localTopK)` 每个分区上送 k 条完整行。宽表或把向量列投影出来会放大 shuffle，需要一条"每分区上行字节"的上限观察项。
4. **任务内重试要有上限并如实报告不足**：对应现有 `MaxRounds` 与 `VectorSearchStats.indexUnderfilled`，语义不得退化成"悄悄少返回几行"。
5. **删除语义不变**：仍用 `SnapshotDeletes`；第一版不把 delete position 集合下沉广播。
6. **不新增配置项、不改 Puffin 布局、不加表属性、不动索引名字面**。

## 5. 分步实施

| 步 | 内容 | 完成标志 |
|---|---|---|
| 1 | 探针：`SparkJobCounter` + 三条断言（无过滤 / `post` / `pre` 各 1 个 job） | 已完成，**红**（`expected: 1 but was: 3`） |
| 2 | `DistributedIndexSearch` 新增带行输出的重载；任务内 `searchFile` → `readFileWithFormat` → 本地 top-k | 已完成：三种形状各 1 个 job |
| 3 | `VectorSearchExec` 在该路径上不再调用 `rowsAt` / `rowsToDd`；`MaxRounds` 下移 | 已完成：driver 持有命中行 = 0 |
| 4 | `pre` 的候选计算下移（每个任务只为自己的覆盖文件算） | 已完成：`MaxCandidateRows` 与"超限即失败"分支消失，`pre` 断言转绿 |
| 5 | 分组细化：按"索引文件 ↔ 覆盖的数据文件"分区并精确下发 scan task | 每分区 task 数与上送字节数可观测 |
| 6 | 验收：逐行一致（delete / residual / `pre`+`post`）、**补 Lance 格式端到端用例** | 与现实现结果集与顺序一致 |

`auto` 已在 #3039 废弃，链路只有 `pre` 与 `post` 两种取值、默认 `pre`。

## 6. 与 lance-spark 的对照（为什么它天然 1 个 job）

lance-spark 把融合做在**原生扫描器**里：`ScanOptions.Builder().nearest(q.build())`，并把 `filter(...)`、`prefilter(true)`、`columns(...)`、`indexSegments(...)` 一起交给扫描器（`LanceDistributedSearchColumnarPartitionReader.java:168-187`），原生 reader 一次完成"检索 + 读列"，直接吐 `ColumnarBatch`；Spark 侧只是把 `PartitionReader<ColumnarBatch>` 包成行迭代器。它的 `prefilter(true)` 即本设计的 `pre` 语义，且要求"fragment 受限的 nearest scan"必须显式开启 prefilter。

差别在于：Lance 的 reader 接受**查询向量**，Iceberg 的 Parquet 读取接受**谓词**，没有"按距离排序返回"的能力。所以要达到同等效果，只能由我们自己把"检索结果 → 位置 → 按位置读行"这三步放在同一个 task 里做；`FormatModelRegistry` 的 `positions(...)` 提供了按位置读的部分，缺的只是把它搬进 task。

## 7. 融合器的职责与非职责（评审已确认）

融合器是一个 **executor 侧的工具类**，由 `DistributedIndexSearch` 的任务体调用。它把今天散在 driver 侧的四段收进一个任务：`candidatesFor` 的候选、`candidateRows` 的检索、`rowsAt` 的回表、`rowsToDd` 的重新并行化。

### 7.1 它做的事（全部在任务内、全部是 local 操作）

1. 加载索引 payload，或从 `ExecutorIndexCache` 取驻留的那份（驻留与驱逐行为不变）；
2. 用查询向量算出本分区的候选 `(dataFile, rowPosition, score)`；
3. 按位置读行：`FormatModelRegistry.readBuilder(format, ...)` + `builder.positions(wanted)`，Parquet 与 Lance 共用同一条路径；
4. 应用 `SnapshotDeletes`（位置删除 + 等值删除）；
5. **前过滤**：task 先读自己覆盖的数据文件里过滤要的列，算出放行行位置，再让 kernel 只给这些行打分（见 §8.2）；
6. **后过滤**：取回的行就地判定，被拒的不占窗口，任务内扩大预算重探；
7. 维护本地 top-k，输出行迭代器；**窗口不足时在任务内扩大预算重探**（旧 `MaxRounds` 下移），本索引文件扩到底仍不足则如实标记（旧 `VectorSearchStats.indexUnderfilled`，现在是任务上报的计数）。

### 7.2 它不做的事（实施时不得越界）

1. **不是 Spark 的数据源 reader。** 不实现 `PartitionReader` / `ScanBuilder`，不改 `VectorSearchSource`、不动扫描计划。lance-spark 能做 `ScanOptions.nearest` 是因为它的 reader 属于数据源自身；向量检索在我们这里是一个算子的执行体，改造成 V2 数据源会牵动全表扫描语义，风险远大于收益。
2. **不接管全局排序。** 只产出本地 top-k，全局顺序由上层 `Sort` 用**精确重算**的距离决定。融合器内部的分数只用于挑本地候选。
3. **前过滤不做"先扫一遍过滤列"。** 如果每个任务都先扫自己那份的过滤列来造候选，等于把今天一次全局预扫变成 N 份分区扫描——总 IO 相同但每任务都要等扫完才开始检索，串行化更差。正确形态是：**候选由索引给，过滤在行上判定**，向量列与过滤列在同一次按位置读里取。`pre` 与 `post` 在任务内的差别因此只剩"判定在打分前还是取行后"，不再有额外的一趟扫描——`pre` 的 2 个额外 job 也随之消失。
4. **不改删除与快照语义。** 沿用 `SnapshotDeletes`；第一版不把 delete position 集合下沉广播。
5. **不碰其余三条入口。** `IndexSearch`（TVF/procedure）、`RewriteTextSearch`（全文）、`IndexRewriteSupport.rowsAt` 保持原实现。

### 7.3 与 lance-spark 的对齐边界

调度层面改完后与它平起平坐：1 个 job、无 driver 物化。**IO 层面不会一样**：它的索引内建于格式、扫描期就能决定打开哪些页；我们的是外部索引算候选、再按位置读，`positions` 能跳过行组，但一个页内只要有一行命中仍要整页解。要追平 IO 需要把候选下推成页级过滤（自定义 reader，需新增 `iceberg-data` 依赖），只有在 benchmark 证明 IO 是瓶颈后才做。

## 8. 实施记录（2026-11-11，`card/3038`）

### 8.1 落地的形状与本文的偏差

1. **`searchWithRows` 返回 RDD，不返回 driver 侧列表。** 初版把任务读到的行 `takeOrdered` 回 driver，那仍然要第二次 job 把行重新并行化（3 → 2 个 job）。现在每个任务直接输出自己那份 top-k 的行，`VectorSearchExec` 用 `spark.createDataFrame(JavaRDD, schema).queryExecution.toRdd` 转成内部行——这是 `mapPartitions` 级的类型转换，不起 job。实测三种形状（无过滤 / `pre` / `post`）从 3 / 5 / 3 个 job 降到 **1 / 1 / 1**。
2. **`pre` 是 task 内的前过滤，不是"过滤在行上判定"。** 本文 §7.2 第 3 条曾主张"两个取值在任务内只剩判定时机之差"，那是错的，实现时也一度照此写了：把 `candidates` 一律传 null、只在取回的行上判定，等于把 `pre` 悄悄变成 `post`。现在 `pre` 恢复成本地预过滤——task 用 `IndexEntries.positionsMatching` 读自己覆盖的数据文件（只读过滤要的列，删除照常应用）得到 `IndexCandidates`，交给 `prepared.search(query, k, candidates)`，kernel 只给放行的行打分（`IvfKernel` 的第三重载会把探针扩到填满窗口为止）。`post` 仍是对取回的行判定 + task 内扩样。
   两者的可观测差别有测试钉住（`TestVectorSearchFiltered.runsTheFilterWhereTheTaskSearches`）：200 行里只有 1 行通过一个选择性极高的过滤，`pre` **1 轮**就答出那一行，`post` 要一轮一轮拒绝窗口。
3. **`pre` 的候选集不再有全局上限。** 它只活在读到它的 task 里，规模是该 task 覆盖文件的放行行数，因此 `MaxCandidateRows`（200 万）与"超限即失败"分支一并删除：那是 driver 内存的约束，不是 task 内存的约束。
4. **分数方向是索引自己的决定。** 本地 top-k 必须按 `IndexAdapter.resultComparator()` 的方向取（HNSW 的分数是相似度，越大越好；IVF 的距离越小越好）。按"越小越好"写死会安静地返回最远的 k 行——实测就是这样错了三个用例。
5. **`readFileWithFormat` 的位置语义被修正。** 旧代码在流式读取时就把 `wanted.get(i)` 当作行位置，只有在"读完发现条数不等"时才回退成按序编号。Parquet 的格式模型**不实现** `positions(...)`（`ReadBuilder` 默认忽略，这里只有 Lance 实现），于是流式读到第 `wanted.size()+1` 行就越界。现在行一律按流过的序号编号，读完再判断"条数正好等于请求数"才重贴位置；调用方另外按请求集合过滤，两种情形都正确。
6. **过滤列在判定之前不能被裁掉。** 取回的行是"投影 + 过滤需要的列"，判定通过后才裁回投影宽度；先裁再判会让过滤看到不含自己列的行走，`post` 会因此拒绝一切。
7. **指标改成任务上报。** 行不再经过 driver，所以 `numCandidates` / `numCandidateRequests` / `numCandidateRounds` / `numLiveCandidates` / `numDeletedOrInvisibleHits` / `numHitsOutsideSnapshot` / `numRowsAtFiles` / `numIndexUnderfilled` 都是任务累加的计数（语义从"一条查询的数"变成"各索引文件之和"），driver 仍只设定 `numResidualFiles`。
8. **`ExecutorIndexCache` 的局部性表不再被融合查询刷新。** `mergeCacheReports` 原本在 `search()` 的 `takeOrdered` 之后执行；融合路径没有任何 driver 侧 action，因此本查询无法在规划期消费自己的上报。**已知退化，未修**：局部性提示由其它入口（TVF / 全文）维持。

### 8.2 验证

- `TestVectorSearchFiltered.answersAFilteredQueryInOneJob`：三种形状各 1 个 job（红 → 绿）。
- `TestVectorSearchFiltered.runsTheFilterWhereTheTaskSearches`：同一句过滤下 `pre` 1 轮答出唯一放行行、`post` 多轮拒绝窗口——两个取值真的是两条局部策略。
- `TestVectorSearchExecution.aWindowShortOfLiveRowsWidensInsideTheTask`：8 行表、删除最近 3 行、`over-sample=1`，任务内扩样一轮补齐窗口（`numCandidateRounds ≥ 2`、`numIndexUnderfilled = 0`）。
- 向量/全文相关 spark-extensions 用例 305 个 + `index-spark` 393 个全绿（含 `rowsAt` / 删除 / 等值删除路径）。
- 未做：第 5 步（按"索引文件 ↔ 覆盖数据文件"精确分组下发 scan task，现在仍是全表 `scanTasks` 随 broadcast 下发）、第 6 步的 Lance 端到端用例、以及性能测量（按 #5447 的裁决留到功能完成后单独跑）。

## 9. 待评审确认的点

1. ~~分步顺序是否认可~~：已确认。
2. ~~`rowsAt`/`rowsToDd` 的移除范围~~：已确认为只针对 `VectorSearchExec` 路径。
3. 待定：第 5 步"分组细化"本轮必须做，还是先按"一索引文件一分区"跑通再单独优化。
4. 待定：融合查询不再刷新 `ExecutorIndexCache` 的局部性表（§8.1-8），是否值得为它单独设计一条上报路径。
5. 待定：`pre` 的候选集现在是按文件的 `long[]`，规模是该 task 覆盖文件的放行行数；10M/100M 规模下的 task 内存还没有实测（在 §8.2 的"未做"里）。
