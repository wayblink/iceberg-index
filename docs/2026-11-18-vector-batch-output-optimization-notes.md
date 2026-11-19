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

# 向量检索的批量化与输出侧优化：经验记录（2026-11-18）

本文记录 `index` 分支在 2026-11-18 前后做的几项向量检索优化：做了什么、为什么这样做、
实测到什么程度、有哪些坑、以及在什么场景下应该怎么用。**它不是设计文档**（设计在被引用的
`docs/superpowers/specs/` 里），也不是参数参考（参数在 `docs/index-user-guide.md` 与
`docs/vector-index-deployment-guide.md`），而是把实现过程中值得重复利用的经验固定下来。

基线：`index` @ `7bed71824e`；期间并行合入的 `IndexAdapter` payload 去复制（#3123）不在本文范围。

## 1. 一页结论

1. **批量入口的输出形态是块大小和 driver 内存量的主因**。带行 JSON 时每行约 `20 B/dim`
   （2560 维约 51 KB），由此推算 10,000 条查询 × k=10 约 5 GB 量级；换成只回位置与分数的
   `positions` 后每行约 30 B，同规模约 3 MB 量级。**这两组总量是按行宽常量与代码路径推算的，
   不是实测**（§4.3）。
2. **块大小由三个预算里最紧的一个决定**（块内广播 8 MiB、driver 行+JSON 8 MiB、shuffle 128 MiB），
   上限 128。2560 维、k=10、带行时只有约 9–11 条/块；`positions` 下可到 128。
3. **`positions` 省掉回表与行 JSON 两段工作**（本地实测作业数因此 27→7）。本地受控实测
   （512 维、400 查询、k=10）：墙钟约 2.9× 快，driver 堆峰值低 3–4 倍；
   集群实测（2560 维、33 个索引文件、1000 查询、k=10）：作业 290→25、墙钟约 1.9× 快、
   堆峰值约 2.6× 低（§4.4）。两处探针都走 `CALL`，不是表函数路径（§4.3）。
4. **表函数路径按块转换输出**（#3124，本批加 `.coalesce(1)`）：每块恰好一个分区，但它**不是**
   把整批流式交给下游——driver 在 `union` 执行前仍持有每块转出的 `UnsafeRow`，峰值是
   O(整批) 而不是 O(一块)。真实收益是每块的 `Row`+JSON 在转换后即可释放，且分区数等于块数；
   `CALL` 过程是结果集 API，仍必须收全（用 `positions` 把每行降到 ~30 B）。
5. **查询期参数可以逐语句指定**（#3105；本批移除了调用级 `filter_mode`）：`nprobe`/`over_sample`/`index`，
   优先级 **语句 > session**（索引定义里目前不记录这三项的默认值），非法值报错不回退。`nprobe`
   以调用值为准；session 值只对 IVF 生效，非 IVF 索引上显式传 `nprobe` 报错。`filter_mode` 现在
   只有 session 配置 `spark.iceberg.index.vector.filter-mode`，且只在 `ORDER BY vec_distance`
   改写带过滤时生效。
6. **查询载体的装箱已经去掉**（#3115）：`Vector[Array[Float]]`，4 B/dim；10k×2560 约 102 MB，
   但**仍是全量常驻**，真正“按块承载查询”没有做。

## 2. 改了什么（按提交）

| 提交 | 卡片 | 内容 |
|---|---|---|
| `42ab1de179` | #3098 | 只做格式：修掉此前就红的 5 个文件（与行为分开） |
| `95fa7b851d` | #3098 | 缓存开关的租用判定两侧一致；新增 `numFetchedRows`；整列表区回退计数；`readTail` 兜底与 RangeReadable 覆盖；executor 墓碑按 incarnation；删掉旧 `searchWithRows` 兼容层；`jsonOfRows` 条数断言；路由原型移入测试源集；批量删除等价用例 |
| `ae8c63db7a` | #3105 | `CALL vector_search` / `vector_search_batch` 加 `nprobe` / `over_sample` / `filter_mode`（`filter_mode` 后在本批移出调用参数），`CALL_ARGUMENT` 覆盖并记录被覆盖层 |
| `232305fb13` | #3105 | `vector_search` 表函数加同一组参数（外加 `index`；调用级 `filter_mode` 后移除）；引入内部节点 `VectorSearchOptions` 让重写规则能看见调用参数 |
| `2c2c80f9c4` | #3105 | `vector_index_search(_batch)` 在位置参数后接受同一组命名选项（调用级 `filter_mode` 后移除） |
| `e3a3d3b691` | #3115 | 查询载体从 `Vector[Vector[Float]]` 改为 `Vector[Array[Float]]`，规划时拷贝；driver 常驻 20 B/dim → 4 B/dim |
| `ddb395c574` | #3116 | 新增 `coverage => 'positions'` 契约：不回表、不含行 JSON、分数是索引估计 |
| `c922f8a00c` | #3124 | 表函数批量输出逐块转分区（本批加 `.coalesce(1)`，一块一个分区），最后 `union`；driver 仍保留整批 UnsafeRow 直到 union 执行 |
| `103fd87350` | — | 使用文档补齐 coverage/批量/指标/缓存/优先级/内部开关/限制 |

本批 review 修复另外做了两处与本文口径相关的改动：移除所有调用级 `filter_mode`（只保留
session 配置），以及给表函数的每个块 RDD 加 `.coalesce(1)`。

## 3. 关键设计与踩过的坑

### 3.1 表函数的参数在优化器阶段看不见

向量重写 `RewriteVectorSearch` 是 **optimizer rule**，而表函数的参数在分析（analysis）结束时就
没了。所以只在表函数里解析参数会得到“写了但没生效”。

做法：表函数把它构造的计划包进一个内部节点 `VectorSearchOptions`（参数原样携带），重写规则在
lowering 时消费并丢弃；索引不可用时丢弃节点、保留扫描。

**同类坑**：Spark 3.5 的 `ResolveHints$RemoveAllHints` 在 analyzer 阶段会**静默丢掉**没有规则消费的
hint。因此“用 SQL hint 传参”必须自己在 resolution 阶段先消费，并让非法/未知值报错——这条仍在
someday（#3109），没有做。

### 3.2 自定义 `LeafExecNode` 的输出必须是 UnsafeRow

逐块产出时，第一版直接用 `sc.parallelize(GenericInternalRow)`，在 `collect` 时被
`SparkPlan.getByteArrayRdd` 强转 `UnsafeRow` 而失败。最终做法是每块
`spark.createDataFrame(rows, schema).queryExecution.toRdd.map(_.copy()).coalesce(1)`：得到
UnsafeRow，且一块恰好一个分区（`LocalTableScanExec` 否则会把一块切成多个分区）。

这**不是**“driver 只持一块”的流式输出：每块转出的 UnsafeRow 都留在 `union` 的各个
`LocalTableScanExec` RDD 里，直到整批执行时才被消费，所以 driver 峰值是 O(整批)。它换到的是：
每块的 `Row`+JSON 在转换完成后即可释放，内存里留下的是更紧凑的 `UnsafeRow`，并且分区数等于
块数。块越多，转换的固定开销越高——每块一次 `createDataFrame` + 一次本地转换，这是逐块转换
相对“整批在 driver 上建行”的代价。

容量规划时，把 Driver 已累积的批量输出与当前块的临时空间相加。对表函数，已累积部分
应按全部结果的序列化行字节估算，包含选取的 JSON；每块预算只约束正在搜索和转换的工作集。
Spark 分区数增加不会降低这部分保留量，目标环境需实测总峰值。

### 3.3 块大小是被 JSON 预算压下来的

`session.conf`/内部属性都改不了“块装多少”，它由 `derivedQueriesPerBlock` 取
`byPayload`、`byDriver`、`byShuffle` 的最小值再封顶 128。`byDriver` 里的
`k × (dim × 20 B + 32 B)` 项在 2560 维时就是 51 KB/行，直接把它压到 ~10 条/块。
`iceberg.index.batch.queries-per-block` 只是测量覆盖，仍被 128 封顶。

### 3.4 `positions` 的语义必须写清楚

它返回索引自己的命中：**不回表、不判快照/删除、分数是估计值**。所以建索引后被删除的行仍会出现，
也不能与 `ORDER BY vec_distance`（精确距离）混用。它换到的是：不读数据文件、不做行 JSON、
块可放大、块内少了两段作业（回表与行 JSON）。

### 3.5 缓存开关是“加载侧 + 查询侧”共读的

`cache.enabled=false` 不只是不加载：已有驻留副本也不会被查询租用（文本与向量一致）。修复前的
窗口是“先开过缓存、再关掉”，这时向量侧仍会命中旧副本而文本侧不会；现在两侧一致。切换只影响
下一次查询，已借出的副本随租约释放结束。

### 3.6 指标：`numLiveCandidates` 不是回表行数

`numLiveCandidates` 记的是任务**保留**的行（≤ k）。回表成本要看新增的 `numFetchedRows`
（按轮累计）。整列表区回退由 `numIndexWholeListAreaReads` 计（一次回退计 1），DEBUG 日志里另有
`whole-list-area reads`。

## 4. 实测（本地 + 集群）

### 4.1 方法

- 环境：单 JVM，Spark `local[2]`，hadoop catalog，Iceberg 扩展；dim=512、20,000 行、
  查询 400 条（`query_table`）、k=10、over=0；IVF+RABITQ 4bit、nlist=64、单个索引文件；
  无缓存、无删除、无未覆盖文件。
- 对照：同一张表、同一索引，只切 `coverage`（`table` / `positions`）。
- 指标：`SparkListener.onJobStart` 计作业数；每 20 ms 采样 `Runtime.totalMemory-freeMemory`
  （跑前 `System.gc()`）取峰值与 GC 后增长；`System.nanoTime` 计墙钟；`collectAsList` 后的行数
  与行 JSON 字节。
- 跑 3 次独立 JVM（第 2、3 次强制 rerun）。

### 4.2 数据

| 运行 | coverage | 行数 | 行 JSON | 作业数 | 墙钟 | 堆峰值 | GC 后增长 |
|---|---|---|---|---|---|---|---|
| 1 | `table` | 4000 | 7 MB | 27 | 2115 ms | 520 MB | 438 MB |
| 1 | `positions` | 4000 | 0 | 7 | 760 ms | 175 MB | 73 MB |
| 2 | `table` | 4000 | 7 MB | 27 | 1977 ms | 765 MB | 684 MB |
| 2 | `positions` | 4000 | 0 | 7 | 655 ms | 174 MB | 66 MB |
| 3 | `table` | 4000 | 7 MB | 27 | 2167 ms | 496 MB | 404 MB |
| 3 | `positions` | 4000 | 0 | 7 | 721 ms | 173 MB | 71 MB |

换算：作业数约 3.9× 少（扣除每次调用的固定作业后，差异来自 `positions` 省掉的回表与行 JSON
两类作业）；墙钟约 2.8–3.0× 快；堆峰值约 3–4× 低；GC 后增长约 6–9× 低；`positions`
行 JSON 恒为 0，证明确实没有回表。

### 4.3 边界（不要外推）

- 单机、512 维、400 查询、单索引文件、无缓存/删除/未覆盖文件；不代表集群；
- 堆采样有 ±25% 波动（`table` 峰值 496–765 MB），但倍数与方向稳定；
- 2560 维、10k 查询的“5 GB / 3 MB / 0.5 GB→102 MB”都是按常量与代码路径**推算**；
- 本次没有单独量化 #3124 的表函数逐块产出（探针走 `CALL`），它的正确性由
  `theOutputIsAssembledFromTheBlocks` 覆盖；因此下面的作业数与堆峰值是 `CALL` 路径的数据，
  不能直接当成表函数路径的 driver 峰值（后者在 `union` 执行前仍持有整批 `UnsafeRow`）。

### 4.4 集群（2560 维、33 个索引文件、1000 条查询）

环境：镜像 `oss-bench-20261118-e569321799`（digest `sha256:b530d6ca26d8001eaf20d00038bc57a9e2411aa5042f8923f13c1e32b711b43a`），
表 `smoke.mm_lance`（Lance 格式的 Iceberg 表），索引 `fmt-ivf-lb-4b`（IVF+RABITQ 4bit、
`ivf-list-blocks=true`、`max-entries-per-index-file=340000`，33 个索引文件、每个约 700 MB），
查询表 1000 行（2560 维确定性伪随机），k=10、over=0、nprobe=32；driver 4c/12g，
12 个 executor × 4c/30g；执行器索引缓存开（`cache.enabled=true`、`cache.bytes=22 GiB`、
`cache.mode=PIN_ALL`、`spark.plugins=IndexCachePlugin`、`schedule.locality=true`），
两种模式都在测量前用一条查询暖缓存。

| 模式 | 行数 | 行 JSON | 作业数 | 墙钟 | 堆峰值 | GC 后增长 |
|---|---|---|---|---|---|---|
| `table` | 10,000 | 403 MB | 290 | 1671 s（27.9 min） | 2865 MB | 2736 MB |
| `positions` | 10,000 | 0 | 25 | 858 s（14.3 min） | 1120 MB | 983 MB |

结论：作业数 290→25（约 **11.6×** 少，直接来自块大小 128 vs 约 10）；墙钟约 **1.9×** 快；
堆峰值约 **2.6×** 低、GC 后增长约 **2.8×** 低；行 JSON 403 MB→0。

边界：只测了 1000 条查询。10k 没有跑：`positions` 按同速率约需 2.4 小时，`table` 约需 4.6 小时，
且按 403 MB JSON → 2.7 GB 堆增长的比例外推，5 GB JSON 约需 34 GB 增长，12g driver 会 OOM——
这是**外推，不是实测**。查询向量是确定性伪随机而非 SIFT 真查询，绝对时间不可与论文或别家对比，
只用于两种模式的相对比较；两种模式共享同一缓存与暖机配置，块大小差异本身来自 JSON 预算，
属于被比较机制的一部分。

## 5. 怎么用（决策表）

| 场景 | 用什么 | 理由 |
|---|---|---|
| 结果要精确 top-k、要行内容 | `ORDER BY vec_distance(...)` 或 `FROM vector_search(...)` | 走精确重排/精确距离，代价是行与 JSON |
| 批量，结果要活行（JSON） | `CALL ... vector_search_batch(..., coverage => 'table')` | 会回表、会判删除，块约 9–11（2560 维） |
| 大批量，只要位置+分数 | `CALL ... vector_search_batch(..., coverage => 'positions')` | 不回表、无 JSON、块到 128 |
| 大批量，结果在 Spark 里消费 | 表函数 `vector_index_search_batch(...)` | 块逐块转换、一块一个分区；driver 仍持整批 UnsafeRow 到 union 执行（峰值 O(整批)）；`CALL` 必须收全 |
| 需要按查询调参 | 语句参数 `nprobe`/`over_sample`/`index` | 优先级 语句 > session；`nprobe` 调用值优先，session 值只对 IVF 生效，非 IVF 显式值报错 |
| 需要改过滤相对打分的位置 | session `spark.iceberg.index.vector.filter-mode` | 不是调用参数；只在 `ORDER BY vec_distance` 改写带过滤时生效 |
| 想改块大小做压测 | JVM 属性 `iceberg.index.batch.queries-per-block` | 内部测量属性，不是用户参数；仍封顶 128 |

## 6. 验证与复现

- 功能测试（跟着提交走）：`TestVectorQueryParameters`、`TestVectorSearchSharedRules`、
  `TestVectorSearchTableFunction`、`TestVectorIndexSearchBatch`、`TestVectorCoverageModes`、
  `TestVectorSearchExecution`、`TestVectorSearchPlanning`、`TestVectorCandidateBudget`、
  `TestBatchSearchOutcomes`、`TestVectorRefine`。
- 定位命令（`-DsparkVersions=3.5`，JDK17）：
  `./gradlew :iceberg-spark:iceberg-spark-extensions-3.5_2.12:test --tests '<类名>'`。
- 本次实测的探针没有入库：本地探针是临时 worktree 里的一个 JUnit 类（§4.1），集群探针是
  一个临时 `BatchProbe` 主类（建查询表、暖缓存、跑一种 `coverage`、`SparkListener` 计作业、
  `Runtime` 采样堆，提交到 k8s），两者都不提交产品代码。复现时建议固定数据、索引参数与 warmup，
  并对每种模式跑多次取中位数；集群数据来自一次 `table` 与一次 `positions` 的独立提交。

## 7. 未做与后续

- **集群 10k 查询的实测**：已测 2560 维、33 个索引文件、1000 查询（§4.4）；10k 未跑，
  结论按 1000 的外推给出（`table` 会先撞 OOM）。
- **真正“按块承载查询”**：请求仍持有全部 Q（4 B/dim）；要做到 driver 只持一块，需要改请求承载与
  执行时机，并与共享的已绑定 context 配合。表函数路径也同样：块转换后整批 UnsafeRow 仍留在
  `union` 里等待执行，不是流式输出。
- **`CALL` 的写表模式**：过程返回结果集是 API 限制；要流式只能改入口语义（结果写表），未做。
- **SQL 逐语句参数（hint）**：`RemoveAllHints` 会静默丢弃，必须在 resolution 阶段消费且非法值报错；
  设计卡在 someday（#3109）。
- **按列表评分、全局路由**：路线图里，尚未接生产。

## 8. 参考

- 使用说明：`docs/index-user-guide.md` §4.3/4.7/4.8/7/8.1/9.7/9.12/9.13
- 参数与坑：`docs/vector-index-deployment-guide.md` §5.6；`docs/docs/spark-procedures.md`
- 设计：`docs/superpowers/specs/2026-11-17-vector-batch-shared-execution-design.md`、
  `docs/superpowers/specs/2026-11-18-vector-refine-design.md`、
  `docs/superpowers/specs/2026-11-18-vector-cold-read-design.md`
- 诊断与路线图：`docs/2026-11-17-vector-query-performance-analysis.md`、
  `docs/2026-11-17-vector-query-performance-roadmap.md`
