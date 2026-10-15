# 调研 · Lance 作为回表数据格式的可行性（#707）

> **Status:** 调研完成（2026-10-15，含第二轮联网核查见 §11）。结论：点查性能成立（26x–1000x，取决于 row group 大小）；**加入 Iceberg 的路径有两条活路**——(1) 把 Lance 当作表的 data file 格式（走 Iceberg 1.11.0 的 File Format API，统一性完整，业界有 POC 先例）；(2) 派生 sidecar（仅当基础表必须保持 parquet 时）。优先级仍应低于 B1（覆盖索引）。
> **输入:** 卡片 #707；`~/.memories/step-ws/docs/sparkann-index-latency-analysis.md`（回表占 35%、3.2s/10 行的定位来源）
> **证据:** Iceberg/Lance 源码、厂内 Lance fork 与 tpcds worktree、Kyuubi 真集群运行日志、自建同规模基准（1M × 128 维）、**apache/iceberg 与 Iceberg PMC 的公开 PR/博客/issue（第二轮联网核查，见 §11）**
> **后续:** 真集群端到端验证另立 POC 卡片（#810，随本报告提交）

---

## 0. 结论先行

| 问题 | 结论 |
|---|---|
| 点查性能 | **Lance 对 parquet 是数量级胜利**：同规模 10 行点查，Lance ~1.4–2.1ms / ~32KB I/O；parquet（生产同形态 ~3MB row group）~53ms / ~31MB I/O；parquet（128MB row group）~1839ms / ~471MB I/O。差距是结构性的，不是调优问题 |
| Iceberg 关系 | **核心枚举不认 Lance，但 1.11.0 的 File Format API 就是为此开的口子**：格式可插拔（`FormatModel`/`FormatModelRegistry`），PMC 官方博客**点名 Lance 与 Vortex**。我们 fork 基线即 1.11.0，Spark 3.5 已有 `SparkFormatModels`（§11.1） |
| 架构统一性 | **两条不破坏立论的活路**：(1) **Lance 作为表的 data file 格式**（`write.format.default=lance`）——Iceberg 独占 catalog/snapshot/delete，无第二套系统、无桥接，统一性完整；(2) 派生 sidecar（仅当基础表必须留 parquet）。**用 Lance 自己的 catalog/表格式（落位 A）仍然否决**（§5、§11.8） |
| 删除/更新语义 | Lance 内部语义与 Iceberg **概念同构**（version↔snapshot、deletion vector↔position delete、row address=(fragment,offset)↔(file,pos)），可对齐。**首次写下时假定了 sidecar 落位，因此强调“桥接成本”；在 B2b 落位下这些桥接成本大部分不存在**（§11.8） |
| 厂内先例 | **有，且已趟平链路**：Lance fork（含公司自研 HDFS provider）+ tpcds worktree（100G/1T Lance 建表）+ Kyuubi 103 个 TPC-DS query 全跑通 |
| Spark 3.5 成熟度 | **可用但不成熟**：beta connector（0.4.0-beta-2），与生产 Gluten 有 Arrow classpath 冲突，必须专用 engine 且关闭 Gluten；HDFS 支持依赖公司 fork，升级需 rebase。**索引侧测试可先把 Gluten 排除，用 Gluten-free engine 跑** |
| 业界先例（§11） | **有，且很新**：Iceberg File Format API 定稿并点名 Lance；Lance 有两个 apache/iceberg POC（#15585 模块式、#15751 走 API，读写+Spark SQL 闭环，均因 stale 关闭而非否决）；Vortex 同路线 open 并推进（#15915），其公开实测证明**非字节可切格式必须补 row-splittability**（补后 TPC-H 100 整库快 ~30%）；第三方实测随机取 20–50 行快 ~40x、存储 +50% |

**建议（第二轮修订后）：B1（覆盖索引）→ B2b（Lance 作 Iceberg data file 格式）→ B2a（sidecar）；换成 Lance 自己的表格式/catalog（落位 A）否决。** B1 立论内、零第二系统、对“向量 + pk/少量列”类查询回表直接归零；B2b 把“回表本身也快”也拿到，且不引入第二套系统。

> **第二轮修订（必读）**：联网核查后发现 Iceberg 1.11.0 的 **File Format API** 让"把 Lance 当纯数据格式加进 Iceberg"成为**不破坏统一性的首选路线**（Iceberg 独占 catalog/snapshot/delete）。推荐顺序变为 **B1 → B2b（Lance 作 data file 格式）→ B2a（sidecar）**，落位 A（Lance 表格式 + 自己的 catalog）仍否决。完整证据与代价分析见 **§11**，POC 已按此修订为 #810。

---

## 1. 背景

一次 topK=10 查询的延迟拆解（SIFT1M 真集群，本地 4 核同查询 ~10s）：

| 阶段 | 耗时 | 占比 |
|---|---|---|
| 规划 | 367ms | 4% |
| HNSW 索引搜索 | ~4.4s | 48% |
| **回表读命中行** | **~3.2s** | **35%** |
| merge 排序 | 3ms | 0.3% |

回表慢的机制：索引条目只存 `(dataFile, rowPosition)`，必须去 parquet 捞行，而 parquet 读取的最小粒度是 row group——为一个 3MB 文件里的 10 行读一整块（生产数据文件就是 ~3MB 小文件，10 个命中行散布在约 10 个文件里）。parquet 是分析型列存，为顺序大读优化，点查是它的最坏场景。

本调研回答：换成 Lance 这种点查友好的格式，回表能快多少、以什么代价。

---

## 2. 调研方法与证据边界

四类证据：

1. **源码**：本仓 Iceberg fork（`api/.../FileFormat.java`）、厂内 Lance fork（Java API + format spec 文档）。
2. **厂内资产**：`/data/workspace/lance`、`/data/workspace/tpcds`（lance-tpcds-hdfs worktree）。
3. **真集群日志**：Kyuubi Lance smoke / 全量 TPC-DS 运行记录（2026-04）。
4. **自建基准**：1M 行 ×（id + vec[128] + payload 64B），同数据同规模，Lance vs parquet 三种 row group 布局，随机 10 行点查，warm/cold 各论（复现方法见 §10）。

基准边界（读数前必须知道）：

- 单机单进程、本地 xfs（rbd 网络块设备），**没有 Spark、没有 HDFS、没有并发**；测的是"格式+布局"的点查能力，不是端到端查询延迟。
- topK=10、行位置均匀随机——与索引返回的候选行分布一致。
- 生产 3.2s 里含 Spark 调度/小文件打开/HDFS RTT；格式替换拿掉的是 I/O 放大这一项，端到端收益会小于格式级倍数。

---

## 3. Q1 · 随机行读取延迟实测（同规模 1M × 128 维）

测试数据：1M 行，每行 id(int64) + vec(float32[128]) + payload(~64B)，单行 ~590B，每格式总量 ~520–640MB。每次操作：随机取 10 行的三个列。

### 3.1 结果总表（mean，200–300 次操作）

| 格式与布局 | warm mean | cold mean（方差大，见 3.3） | 每次操作读字节 | 相对 Lance |
|---|---|---|---|---|
| **Lance**，1 fragment × 1M 行（page=1024 行，默认布局） | **1.71ms** | 6–143ms（波动） | **34KB** | 1x |
| **Lance**，200 fragments × 5k 行（≈生产文件形态） | **2.05ms** | — | **32KB** | 1x |
| **Lance**，10 fragments × 100k 行 | 1.42ms | 1.5–47ms | 31KB | 1x |
| parquet，row group = 5k 行（~3.2MB，**生产同形态**） | 52.6ms | — | **31.3MB** | **~26x 慢 / ~980x I/O** |
| parquet，row group = 29k 行（~15MB） | 325.8ms | 334.6ms | 137.2MB | ~190x / ~4300x |
| parquet，row group = 230k 行（~104MB，Spark 默认量级） | 1838.6ms | 1897.1ms | **470.8MB** | ~1075x / ~13800x |

p99：Lance 各布局 2.3–4.1ms；parquet-5k 78ms；parquet-230k 2293ms。Lance 全部 300 次操作返回行数全部正确（bad=0）。

### 3.2 解读

- **差距的本质是 I/O 放大，不是格式玄学**。Lance 的 take 按 (fragment, offset) 定位，只读命中行所在的 page：10 行 = ~34KB。parquet 的读取最小粒度是 row group：10 个散布的命中行平均要读 4–5 个完整 row group，生产形态（3MB/RG）下就是 ~31MB/op——比 Lance 多读约 1000 倍。
- **延迟随 I/O 放大线性传导**：本地盘上 ~31MB→53ms、~137MB→326ms、~471MB→1839ms，基本是顺序读带宽的倒数。放到 HDFS + Spark 小文件打开，还要叠加每文件一次 RTT 和 task 开销——这正是生产 3.2s 的构成。
- **"把 parquet row group 调小"只能缓解不能拉平**：row group 从 230k 行缩到 5k 行（生产形态），点查从 1839ms 降到 53ms，但仍比 Lance 慢 26 倍、多读 980 倍；且小 row group 会伤害顺序扫描（元数据膨胀、I/O 放大给分析查询），是与分析负载抢参数的零和游戏。
- Lance 的 fragment 大小（5k–1M 行）对点查延迟几乎无影响（1.4–2.1ms），因为寻址本来就走 page 级——布局参数不需要为点查妥协。

### 3.3 冷缓存与噪声（诚实声明）

- parquet 冷热几乎无差（1839 vs 1897ms）：大读场景页缓存收益被设备带宽吃掉。
- Lance 冷缓存方差大：同一配置多次"清缓存后首跑"测到 6–143ms（共享主机上还有他人作业，rbd 争抢）。清缓存后第二跑普遍回落到个位数 ms（首次读的 readahead 已预热）。**结论只取 warm 数字 + rchar 字节数**，这两项跨运行稳定；冷缓存的绝对延迟在本机不可靠，但"Lance 冷读 ~34KB vs parquet 冷读 14–471MB"的结构不变。

---

## 4. Q2 · Iceberg 是否有 Lance 的 FileFormat 支持？

**没有，且结构上不会有。**

> **⚠ 本节在第二轮被部分修正：见 §11。**Iceberg 1.11.0 已交付 **File Format API**（格式可插拔，PMC 博客点名 Lance/Vortex），因此“给 Iceberg 加 Lance data file 格式”是官方预留的扩展点，不需要改 core SPI 设计。仍然成立的是：Lance 用自己的 catalog/表格式时，与 Iceberg 平级。

- Iceberg 的 `FileFormat` 仍是**封闭枚举**（`api/src/main/java/org/apache/iceberg/FileFormat.java`）：`PUFFIN / ORC / PARQUET / AVRO / METADATA`——新格式需要同时改枚举与注册（Vortex 的 PR 就是把 `VORTEX("vortex", true)` 加进去）；但枚举不是“结构上不可动”，而是一个需要上游/规范同步的扩展点。
- 更深层的不匹配：Iceberg 的 scan 规划模型建立在**可 split 的顺序字节范围**上；Lance 的随机访问模型是 **(fragment, offset) → page**，没有“split 成字节范围”的自然对应。**这是这条路真正的硬伤**（Vortex 已经踩过，见 §11.3），但可以解：补 row-split 或“多写小文件换并行度”。
- Lance 自己是**表格式**（dataset root 下 `data/ _versions/ _deletions/ _indices/ _refs/ tree/`，每 version 一个 manifest），与 Iceberg 是平级概念，不是“Iceberg 里的一种文件格式”。
- 厂内实际用法也证明了这一定位：`lance-namespace-hive2-0.1.3` 把 Lance 表注册进 HMS，Spark 侧是**独立 catalog**（`spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog`），与 `spark_catalog`（Iceberg）并列。

**结论（修订）：Lance 作为 data file 编码可以进 Iceberg（§11）；Lance 作为表格式/catalog 与 Iceberg 平级。**

---

## 5. Q3 · 若 Iceberg 不认 Lance，单独引入是否破坏"架构统一性"立论？

立论原文（latency-analysis）："不需要维护第二套系统：数据、快照、删除语义、权限都和表一致。"

四种落位，逐个过（第二列为第一轮判断，第四列为第二轮修订）：

| 落位 | 做法 | 对立论的影响 | 第二轮修订 |
|---|---|---|---|
| A. 换成 Lance 的表格式 + 自己的 catalog | 整表交给 Lance 管（Lance metadata/namespace 成为权威） | **直接破坏**。双表格式、双快照模型、双 catalog；Iceberg 降级为“另一个元数据源”。一票否决 | 仍然否决 |
| **B2b. Lance 作为 Iceberg 表的 data file 格式**（`write.format.default=lance`） | 表仍是 Iceberg 表：catalog、snapshot、delete/update、权限全在 Iceberg；只是 data file 用 Lance 编码，由 Iceberg 的 File Format API 读写 | **不破坏**：没有第二套系统、没有第二个权威、没有桥接 | **新增，且为首选**（依据 §11） |
| B2a. 派生 sidecar | Iceberg 权威不变；为每个 snapshot 物化一份 Lance 回表副本，索引绑定 | **部分受损**：权威仍在 Iceberg，但多一份全量副本 + snapshot/delete/rewrite 桥接 + 双向 GC | 降为备选（仅当基础表必须保持 parquet） |
| C. 只把索引/向量段存 Lance | 不动回表路径 | 不破坏立论（ecosystem 拓扑文档已建议），但**不解决 3.2s 回表** | 不变 |

判断（修订）：

- **B2 的首选形式是 B2b**：它把“回表快”与“统一性”同时拿到，代价从“桥接”转移到“规范扩展 + 跨引擎互操作 + 格式工程缺口”（§11.6、§11.8）。
- B2a 的桥接成本（§6）仍在，但只在“基础表必须保持 parquet（其它引擎不能读 Lance）”时才值得付。
- 对比 B1（索引内物化投影列）：B1 在同一立论内让“向量+少量列”的查询**完全不用回表**（3.2s → 0），不引入任何新格式。B1 覆盖不到的，是“查询要回表取很多列/大 payload”的场景。
- B1 与 B2b 不互斥：B1 管“常用投影”，B2b 管“回表本身也快”。
- 所以这不是"Lance 好不好"的问题，是**用什么代价换什么覆盖面的问题**：B1 用索引体积换窄覆盖面；B2(sidecar) 用第二副本+桥接换任意列点查。

---

## 6. Q4 · Lance 删除/更新语义能否与 Iceberg 快照模型对齐？

### 6.1 概念映射（Lance format spec v2 / Java API 对照 Iceberg）

| 概念 | Iceberg | Lance | 对齐度 |
|---|---|---|---|
| 版本 | snapshot（manifest list + manifests） | version（`_versions/*.manifest`，每 version 一个） | 同构 |
| 行删除 | position delete file（data file + pos） | deletion vector（`_deletions/*.arrow`/`*.bin`，per-fragment 行偏移 tombstone，`FLAG_DELETION_FILES`） | 同构 |
| 行身份 | (data file path, `_pos`) | row address = `(fragment_id << 32) \| local_row_offset` | **同构**——我们索引条目存的 (file, pos) 可直接换成 (fragment, offset) |
| 稳定行 id | v3 lineage / 序列号 | stable row id（`FLAG_STABLE_ROW_IDS`，`_rowid`，manifest 维护 `next_row_id`） | 都有，两侧都新 |
| 分支/标签 | branch / tag | `_refs/branches`、`_refs/tags` | 同构 |
| 并发写 | 乐观并发（metadata 替换） | transaction（readVersion + 冲突检测，`_transactions/*.txn`） | 同构 |
| 更新列 | copy-on-write / merge-on-read | `updateColumns` / `mergeColumns`（重写 fragment 列） | 同构 |

**Lance 自身的删除/更新语义与 Iceberg 是对齐的**，我们的"删除感知依赖 position delete"可以平移成"依赖 deletion vector + (fragment, offset)"，语义不损失。

### 6.2 真正的难点：桥接，不是 Lance

sidecar 路线下要额外维护的不变量：

1. **版本绑定**：每个 Lance 副本绑定一个 Iceberg snapshot-id；表发生 append/overwrite/delete 时，决定"增量更新 Lance"还是"重建"还是"索引宣告失效回退 fallback"。
2. **删除传播**：Iceberg position delete 到达时，Lance 侧要有对应的 deletion vector 写入；顺序颠倒会产生"索引命中但 Lance 已删/未删"的窗口。
3. **compaction 漂移**：Iceberg compaction 重写 data file 后 (file, pos) → (fragment, offset) 的映射整体失效，Lance 副本要么跟着重写、要么索引条目重定位——这是最重的运营成本。
4. **过期与孤儿**：snapshot 过期、Lance version 清理、索引 version 三者的 GC 要协同，否则双向孤儿文件。
5. **一致性读**：查询时校验"索引版本 ↔ Lance 版本 ↔ Iceberg snapshot"三者一致，任何不一致走 fallback 全表扫描（正确性兜底，性能兜底）。

结论：**语义可对齐，工程成本集中在桥接层**；这些成本应当进 POC 的验收清单逐条验证，而不是拍桌子认为"能对齐"。

---

## 7. Q5 · 厂内 Lance 先例

**有，而且是全链路先例。**

1. **Lance fork**：`/data/workspace/lance`，`v5.0.0-beta.6-6-gbcc1fce`，分支 `step`（origin 仍指向上游 lancedb/lance，6 个公司提交在上游 beta.6 发布提交之上）：
   - `de6d467 feat(io): add hdfs native object store provider`——公司自研 HDFS object store provider（`rust/lance-io/src/object_store/providers/hdfs.rs`，109 行）。**上游 object_store 没有 HDFS provider，Lance-on-HDFS 依赖这个 fork**。
   - `361bb46 feat(integration): add kyuubi lance hdfs smoke assets` + `bcc1fce fix(integration): tighten kyuubi lance smoke checks`——Kyuubi 侧 pod template / 部署 / 查询 runner 全套。
   - `a7123b1` JNI bundle 刷新与 async scanner 回调加固。工作树还有未提交的 kyuubi 模板调优。
2. **tpcds worktree**（`/data/workspace/tpcds` 的 `lance-tpcds-hdfs` worktree）：`GenTPCDSLanceData` / `CreateTPCDSLanceTables` / `RunTPCDSLanceBenchmark` 三个 Scala main + 100G 与 1T 的生成/建表/benchmark 脚本，数据落 HDFS、表经 Hive2Namespace 注册进生产 HMS。
3. **真集群运行证据**：Kyuubi（kyuubi-dev2）上对 `lance.tpcds100g_lance` 跑完 **103/103 个 TPC-DS query**（2026-04-21，总 5688s，run 日志在 `integration/tpcds/out/`）；smoke：24 张表、`count(*)=30`。
4. **镜像证据**：厂内 spark 镜像 `spark:3.5.7-STEP-rc2-tpcds-dsdgen-lance-20260416a`（dsdgen + Lance 一体化）。
5. `/data/workspace/spark4-iceberg-validation`：**与 Lance 无关**（Spark 4 + Iceberg read-option 验证项目），对本调研无输入，排除。

---

## 8. Q6 · Lance 的 Spark 3.5 读路径成熟度

组合：`lance-spark-bundle-3.5_2.12-0.4.0-beta-2` + `lance-namespace-hive2-0.1.3`（Spark 3.5 / Scala 2.12，均为 beta）。

**已被真集群证明可用的部分**： catalog 注册（Hive2Namespace → HMS）、SHOW TABLES / count、103 个 TPC-DS 查询全过、HDFS 读写（靠公司 fork）。

**四条硬约束**（来自 Kyuubi 集成 README 与 patch notes，是生产化前必须消化的）：

1. **不能走 `spark.jars`**：Lance 的 Arrow 类与 Gluten shaded Arrow C Data 类在 driver/executor 主类路径上冲突（classloader 冲突，Kyuubi engine pod 内复现）。必须用 driver/executor `extraClassPath` 主类路径注入。
2. **与生产 Gluten/Velox 互斥**：Lance 会话必须 `spark.gluten.enabled=false`、`spark.plugins=`、`spark.shuffle.manager=sort` + `LocalDiskShuffleDataIO`。生产 Spark 是 Gluten 加速的——Lance 查询要么独占一个专用 CONNECTION-scoped engine，要么说服 Gluten 侧解冲突。**这是运营层面最贵的一条。**
3. **HDFS 支持是 fork 私货**：升级 Lance 版本 = rebase 公司提交（或推动上游化），否则退回"只能本地盘/S3"。
4. **分区写语义未验证**：建表时 `PARTITION_TABLES=false`（connector 的 partitionBy + recover partitions 语义当时未确认）；我们的表按时间分区是常态，POC 必须补这一课。

结论：**Spark 3.5 读路径"能跑、跑过全量 TPC-DS"，但是 beta + 需要专用 engine + 依赖 fork**。作为回表格式引入，等于给索引层绑定一条需要持续维护的非标准 Spark 路径。

---

## 9. 建议

1. **回表优化先做 B1（索引内物化投影列）**：立论内、零第二系统、对"向量 + pk/少量列"类查询回表直接归零。多数在线检索查询落在这个类别。
2. **落位 A（换成 Lance 表格式 + 自己的 catalog）否决**；但 **B2b（Lance 作 data file 格式，走 Iceberg File Format API）改为可选首选**，依据 §11（官方 API + Lance/Vortex 先例 + 我们 fork 已具备）。
3. **若确有任意列点查需求，按 POC 卡片（#810）验证 B2b**：重点验证 (a) Lance data file 在 Spark 3.5 上的读写闭环与 Iceberg 删除/更新语义（position delete 仍由 Iceberg 管）；(b) 回表 3.2s → ？端到端；(c) splittability/列统计 缺失对 fallback 全扫的影响；(d) 跨引擎互操作成本。索引侧测试**先把 Gluten 排除**（专用 Gluten-free engine），避免 Arrow classpath 冲突把 POC 复杂化。
4. 无论走哪条，**parquet row group 调小**都是应立即做的无形态变更（配置级，生产 3MB 文件本身就是小 row group，此条对生产的实际收益有限，但对"大 row group 表"是免费午餐）——注意它与 B1/B2 不互斥。

---

## 10. 复现附录

基准资产在厂内 dev host：`/data/workspace/lance-bench/`

```
gen_data.py      # 生成 1M×128 (id+vec+payload) IPC + parquet 三种 row group 布局
LanceBench.java  # Lance write/bench（用厂内 bundle jar：lance-spark-bundle-3.5_2.12-0.4.0-beta.2.jar，
                 #   含 org.lance Java API + 内嵌 liblance_jni.so + arrow）
parquet_bench.py # parquet 点查 bench（pyarrow，按 row group 定位读取）
fadvise.py       # posix_fadvise DONTNEED 清页缓存（冷缓存对照）
run_matrix.sh    # 全矩阵：3 种 Lance 布局 × 3 种 parquet 布局 × warm/cold
```

关键命令：

```bash
# Lance 写（rowsPerFile / rowsPerGroup 两参控制布局）
java -Xmx4g -cp .:/tmp/lance-bundle.jar:<slf4j-api> LanceBench write data/data.ipc data/lance_5k 5000 1024
# Lance 点查 bench
java -Xmx4g -cp .:/tmp/lance-bundle.jar:<slf4j-api> LanceBench bench data/lance_5k 300 10
# parquet 点查 bench
python3 parquet_bench.py data/pq_5k.parquet 100 10
# 冷缓存
python3 fadvise.py data/lance_5k
```

原始结果：`/data/workspace/lance-bench/results.log`（数据文件已清理，`gen_data.py` + `gen_parquet.py` 可完整重建）。

---

## 11. 第二轮（联网核查）· Lance 作为 Iceberg **原生数据格式**的可行性与业界先例

第一轮的判断是"Lance 是 Iceberg 不认的平级表格式，只能做 sidecar"。第二轮核实后修正了一半：**Iceberg 已经把自己的文件格式层做成插件机制，Lance 正是被点名的候选之一**。若走这条路，"把 Lance 当纯数据格式、不使用它的 catalog 层"就不破坏架构统一性——Iceberg 仍然独占 catalog、snapshot、删除/更新语义，Lance 只是 data file 的编码。

### 11.1 Iceberg File Format API（已定稿，随 1.11.0 发布）

- 2026-02-20 Iceberg PMC 官方博客《Introducing the Apache Iceberg File Format API》：文件格式层改为**可插拔**（`FormatModel` / `FormatModelRegistry` / `ReadBuilder` / `WriteBuilder`），随 **1.11.0** 发布，引擎侧接入已合并。
- 博客原文明确把新格式的动机写成我们的场景："New formats now emphasize extremely fast random access... File structures that **do not match traditional row-group-based designs**"；并在 "What the New API Unlocks" 里点名：**"allows us to try out formats such as Vortex and Lance to integrate cleanly using predictable APIs"**。
- 注册方式：`FormatModelRegistry.register(FormatModel)`；core 里是一份类名列表（`CLASSES_TO_REGISTER`），新增一种格式 = 新增一个模块 + 在列表里加一个类名 + 给 `FileFormat` 枚举加一个值。
- **我们 fork 已具备**：基线为 `apache-iceberg-1.11.0-688-g5ee64caba`，`core/src/main/java/org/apache/iceberg/formats/` 下 `FormatModel` / `FormatModelRegistry` 齐备，`ReadBuilder` 已有 `split(start,length)`，且 `SparkFormatModels` 在 **spark/v3.5、v4.0、v4.1 三个版本都有源码**。即：**不需要等上游、也不需要 Spark 4，我们在 Spark 3.5.7 上就能自己接**。

### 11.2 先例一：Lance 自己（两个 POC，均因 stale 关闭，非技术否决）

| 出处 | 内容 | 状态 |
|---|---|---|
| apache/iceberg **#15585** | 新增 `iceberg-lance` 模块 + `FileFormat.LANCE("lance", true)`，约 3.2k 行，22 文件 | 2026-04-18 stale 关闭 |
| apache/iceberg **#15751** | **走 File Format API 的完整 POC**：`FormatModel`/`ReadBuilder`/`WriteBuilder` 读写闭环；Spark 4.1 SQL `CREATE TABLE ... TBLPROPERTIES('write.format.default'='lance')` / INSERT / SELECT / 投影 / WHERE；向量化（`ArrowColumnVector`）与行式两条读路径；**position delete 写路径（`DeleteSchemaUtil.pathPosSchema()` + `PositionDelete` 转换器）**；19 个单测；Iceberg schema 经 Arrow field id 保留 | 作者标注 draft/POC、"not requesting merge"，2026-05-01 stale bot 关闭 |

#15751 自述的**已知缺口**（`LANCE_SDK_GAPS.md`，5 项）：

1. 文件写入字节数需绕（JNI 的 `closeNative` 丢弃 `finish()` 返回值）；
2. **无列级统计 → 丢失文件级 pruning**（等 Lance PR #5639 落地 + JNI 暴露）；
3. **`splittable=false` → 一个文件一个 task，无文件内并行**；
4. filter 不下推（靠 Iceberg 残差过滤保正确性，但会全读）；
5. `NameMapping` 不生效（列改名后读不了老文件）。

其中第 3 项有已经被踩过的前车（§11.3）；第 2 项对**点查**不痛不痒，对**全表扫描**才痛。

### 11.3 先例二：Vortex（同一条路，走得更远，且有公开实测）

- **apache/iceberg #15915 "Core: Add Vortex format to Iceberg"**：由 Vortex 作者（Spiral）提交，**仍 open、持续更新**（66 commits / 211 files）。核心改动正是给 `FileFormat` 枚举加 `VORTEX("vortex", true)`，再加注册与 `ReadBuilder` 的**行切分**扩展——即"新增一种格式"的实际改动面很小，真正的工作在格式侧。
- **SpiralDB × Microsoft Gray Systems Lab《Vortex on Ice》**（2026 Iceberg Summit 的 tech demo）记录了完整代价与收益：
  - 做法与我们设想的完全一致：基于 File Format API 的 fork + **JNI 绑定 Rust 库**（Vortex 与 Lance 同为 Rust + Arrow，路径同构）。
  - **踩的坑**：Vortex 不是按字节范围可切的（columnar 布局），于是 Iceberg "一个文件一个 task"，单 task 几千万行导致 shuffle spill/merge 极慢，**大 join 查询一度比 Parquet 慢 2x**。原文归因："It all goes back to Iceberg's idea of file **splittability**."
  - **解法**：给 Iceberg 规划器加 **row-splittability**（元数据里报"行区间"而非字节区间）。补上后：**整套 TPC-H 100 快约 30%，最慢的几个查询快 2–4x**。
  - 对我们的含义：**这是同一条路的实测先例，也是必答题**。我们的 fork 目前只有字节 split，**没有 row-split**——要么补它，要么接受"一个文件一个 task"。注意我们生产表本来就是 ~3MB 小文件（5k 行级），"一文件一 task"的并行度损失在**小文件布局下并不致命**，这反而让我们的现状比 Vortex 的 512MB 大文件场景更宽容。

### 11.4 第三方独立数据点（littleDrew，apache/iceberg #13438）

1M 行自测（他自己的口径，注明"40x 是在 Lance 文件上测的、不是 Lance 表"）：

- 随机取 20–50 行：**快约 40x**；
- 全表扫描：列数 < 50 时快 1.5–3x，**列多则不如 parquet**；
- **磁盘占用比 parquet 多约 50%**。

与本轮自建基准（26x–1000x，取决于 row group 大小）方向一致，并补上两条我们没测的代价：**列多时扫描不占优、存储 +50%**。存储这条与我们实测吻合（同数据 Lance 514MB vs parquet 521MB 量级相当，但那是 vector 主导的小列场景；列多时 Lance 的编码开销会显现）。

### 11.5 社区治理状态（决定能不能进上游）

Issue #13438 中 Iceberg PMC 成员 pvary 给出的三步：

1. reader/writer API 层 —— **已完成**（#12298 → #12774 → 1.11.0）；
2. 合规测试框架（TCK）—— **进行中**（官方博客 "Current State" 里唯一未打勾项）；有了它，Lance 这类格式才有功能性验证的落脚点；
3. **社区决定"要不要、怎么支持新格式"** —— **未决**；当前精力在收尾 V3。

含义：**技术上可以自己做（fork 内），短期内不要指望上游合并**。两个 Lance POC 都是被 stale bot 关掉而非被否决，也谈不上"社区已认可"。

### 11.6 规范与跨引擎互操作

Iceberg v1–v4 规范中 `data_file.file_format` 的取值是 **avro / orc / parquet（+ puffin，用于删除/统计 blob）**。也就是说 **Lance 作为 data file 是规范扩展**：Java 实现（枚举 + FormatModel）能跑，但 Trino / Flink / pyiceberg / iceberg-rust / iceberg-go 要各自实现 Lance reader 才能读同一张表。

**"架构统一"在控制面成立（一套 catalog/snapshot/delete/权限），在访问面则取决于各引擎是否补 Lance reader。** 这是 B2b 相比 parquet 的真实代价，必须在 POC 里明确记账（我们自己的引擎栈：Spark 3.5 可以补；Kyuubi/Trino 若也要读同一张表，就得补或走 fallback）。

### 11.7 反向先例：Lance 自己是"伴生表"方案

Lance 官方 namespace 规范中，**Apache Iceberg REST Catalog** 的支持方式是：注册 Lance 表时**在同一路径创建一个 dummy schema 的伴生 Iceberg 表**，用 table properties 标记它是 Lance 表。即 Lance 生态目前交付的 Iceberg 互操作是 **catalog 层伴生**，不是 data-file 层集成。

反过来说明了 B2b 的现状：**POC 有（#15751），产品无**。

### 11.8 结论修订（对第一轮的修正）

| 第一轮结论 | 第二轮修订 |
|---|---|
| "Iceberg 不认 Lance，`FileFormat` 是封闭枚举，**结构上不会有**" | **部分错误**。枚举仍封闭，但 1.11.0 的 **File Format API** 就是为此开的口子，官方博客**点名 Lance**。我们 fork 已具备该 API + Spark 3.5 接入 |
| "唯一可行落位是派生 sidecar" | **新增更好的落位 B2b：Lance 作为 Iceberg 表的 data file 格式**。Iceberg 独占 catalog/snapshot/delete，**无第二套系统、无桥接**，统一性完整 |
| sidecar 的桥接成本（§6 五项） | 在 B2b 下**大部分消失**（不需 snapshot 对齐、不需跨系统 delete 传播、不需双向 GC）。代价转移到：跨引擎互操作（§11.6）、splittability（§11.3）、列统计缺失、Lance SDK 五缺口、JNI/HDFS 依赖与 fork 维护、+50% 存储（§11.4） |

**落位排序（修订）**

1. **B1 · 覆盖索引**：最便宜，立论内，回表归零（窄投影）。
2. **B2b · Lance 作 data file 格式**：统一性完整，回表直接读 Lance；需补 row-split（或依赖小文件布局）、接受规范扩展与跨引擎成本。
3. **B2a · sidecar**：仅当基础表必须保持 parquet 时。
4. **A · 换成 Lance 表格式 + 自己的 catalog**：否决。

**测试范围说明**：索引侧验证可先把 Gluten 排除（用 Gluten-free 的专用 engine 跑 Lance 读取），避免 Arrow classpath 冲突与 Gluten 关闭矩阵把 POC 复杂化——这一点已写入 #810。

### 参考资料（第二轮联网核查）

| 资料 | 链接 |
|---|---|
| Iceberg PMC：Introducing the Apache Iceberg File Format API（2026-02-20，点名 Lance/Vortex） | https://iceberg.apache.org/blog/apache-iceberg-file-format-api/ |
| apache/iceberg #12225 File Format API（issue） / #12774 API 实现（PR） / #12298 更早的接口层（PR） | https://github.com/apache/iceberg/issues/12225 · .../pull/12774 · .../pull/12298 |
| apache/iceberg #15751 Core, Data, Lance: Add Lance file format support via File Format API（POC，stale 关闭） | https://github.com/apache/iceberg/pull/15751 |
| apache/iceberg #15585 feat: Add iceberg-lance module（模块式方案，stale 关闭） | https://github.com/apache/iceberg/pull/15585 |
| apache/iceberg #15915 Core: Add Vortex format to Iceberg（open，Spiral 提交） | https://github.com/apache/iceberg/pull/15915 |
| apache/iceberg #13438 Lance 集成讨论（含 pvary 的三步路线、littleDrew 的实测数据） | https://github.com/apache/iceberg/issues/13438 |
| SpiralDB × Microsoft GS Lab：Vortex on Ice（row-splittability 的由来与 TPC-H 100 实测） | https://spiraldb.com/blog/vortex-on-ice |
| Iceberg 规范：manifest `data_file.file_format` 取值 | https://iceberg.apache.org/spec/ |
| Lance 规范：Apache Iceberg REST Catalog（伴生表方案） | https://lance.org/format/namespace/supported-catalogs/iceberg/ |
