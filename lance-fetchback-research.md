# 调研 · Lance 作为回表数据格式的可行性（#707）

> **Status:** 调研完成（2026-10-15）。结论：点查性能成立，架构路线只有"派生 sidecar"一种活法，且优先级应低于 B1（覆盖索引）。
> **输入:** 卡片 #707；`~/.memories/step-ws/docs/sparkann-index-latency-analysis.md`（回表占 35%、3.2s/10 行的定位来源）
> **证据:** Iceberg/Lance 源码、厂内 Lance fork 与 tpcds worktree、Kyuubi 真集群运行日志、自建同规模基准（1M × 128 维）
> **后续:** 真集群端到端验证另立 POC 卡片（#810，随本报告提交）

---

## 0. 结论先行

| 问题 | 结论 |
|---|---|
| 点查性能 | **Lance 对 parquet 是数量级胜利**：同规模 10 行点查，Lance ~1.4–2.1ms / ~32KB I/O；parquet（生产同形态 ~3MB row group）~53ms / ~31MB I/O；parquet（128MB row group）~1839ms / ~471MB I/O。差距是结构性的，不是调优问题 |
| Iceberg 关系 | **Iceberg 不认 Lance**。`FileFormat` 是封闭枚举（PUFFIN/ORC/PARQUET/AVRO/METADATA），Lance 是表格式，与 Iceberg 平级，只能作为 catalog 并列的外层 |
| 架构统一性 | **把基础表换成 Lance = 直接破坏立论**（"不需要维护第二套系统"）。唯一不破坏立论的落位是**派生 sidecar**：Iceberg 保持权威，Lance 作为绑定 snapshot 的回表物化层，与索引层同级 |
| 删除/更新语义 | Lance 内部语义与 Iceberg **概念同构**（version↔snapshot、deletion vector↔position delete、row address=(fragment,offset)↔(file,pos)），可对齐；真正成本在 **Lance version ↔ Iceberg snapshot 的桥接** |
| 厂内先例 | **有，且已趟平链路**：Lance fork（含公司自研 HDFS provider）+ tpcds worktree（100G/1T Lance 建表）+ Kyuubi 103 个 TPC-DS query 全跑通 |
| Spark 3.5 成熟度 | **可用但不成熟**：beta connector（0.4.0-beta-2），与生产 Gluten 有 Arrow classpath 冲突，必须专用 engine 且关闭 Gluten；HDFS 支持依赖公司 fork，升级需 rebase |

**建议：不做基础表替换。** 回表优化优先走 B1（索引内物化/覆盖索引，立论内、无第二系统、回表直接归零）；若后续出现"B1 投影列覆盖不了的任意列点查"需求，再按 POC 卡片验证 Lance sidecar 路线。

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

- Iceberg 的 `FileFormat` 是封闭枚举（`api/src/main/java/org/apache/iceberg/FileFormat.java`）：`PUFFIN / ORC / PARQUET / AVRO / METADATA`。manifest 里的 data file 只能声明这几种格式；加 Lance 需要改 Iceberg core 的枚举与 reader SPI。
- 更深层的不匹配：Iceberg 的 scan 规划模型建立在**可 split 的顺序字节范围**上（split 规划、column projection、delete file 合并）；Lance 的随机访问模型是**(fragment, offset) → page**，没有"split 成字节范围"的自然对应。即便硬加一个 FileFormat，也享受不到 Iceberg 的 split 并行与 delete 合并语义。
- Lance 自己是**表格式**（dataset root 下 `data/ _versions/ _deletions/ _indices/ _refs/ tree/`，每 version 一个 manifest），与 Iceberg 是平级概念，不是"Iceberg 里的一种文件格式"。
- 厂内实际用法也证明了这一定位：`lance-namespace-hive2-0.1.3` 把 Lance 表注册进 HMS，Spark 侧是**独立 catalog**（`spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog`），与 `spark_catalog`（Iceberg）并列，不是其下一层。

**结论：Lance 只能作为与 Iceberg 平级的表存储外层存在。**

---

## 5. Q3 · 若 Iceberg 不认 Lance，单独引入是否破坏"架构统一性"立论？

立论原文（latency-analysis）："不需要维护第二套系统：数据、快照、删除语义、权限都和表一致。"

三种落位，逐个过：

| 落位 | 做法 | 对立论的影响 |
|---|---|---|
| A. 基础表换 Lance | Iceberg 表的数据文件改成 .lance | **直接破坏**。双表格式、双快照模型、双 catalog；Iceberg 从"权威"降级为"另一个元数据源"，统一性名存实亡。一票否决 |
| B. 派生 sidecar | Iceberg 保持权威；为每个（需要的）snapshot 物化一份 Lance 回表副本，索引与它绑定 | **部分受损但可控**。权威仍在 Iceberg；Lance 副本是指数层同级的"物化访问结构"（与 index leaf files 同性质）。代价：全量数据第二副本 + snapshot/delete/rewrite 桥接 + 孤儿清理——本质上是"缩小版的第二套系统" |
| C. 只把索引/向量段存 Lance | 不动回表路径 | 不破坏立论（这正是 ecosystem 拓扑文档已建议的方向），但**不解决 3.2s 回表**，与 B2 目标无关 |

判断：

- **B2 若做，只有 B 成立。** 而 B 的桥接成本（§6）恰是立论里最疼的部分——它买来的，是"任意列点查都快"。
- 对比 B1（索引内物化投影列）：B1 在同一立论内让"向量+少量列"的查询**完全不用回表**（3.2s → 0），不引入任何第二存储。B1 覆盖不到的，是"查询要回表取很多列/大 payload"的场景。
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
2. **B2 不作为基础表替换**（落位 A 否决）。
3. **若确有任意列点查需求，按 POC 卡片（#810）验证 sidecar 路线**：重点验证 §6.2 的桥接成本与 §8 的 Gluten 共存问题，用真集群端到端数据决定是否值得。
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
