# POC 进度 · Lance 作为 Iceberg 原生 data file 格式（#810）— 检查点 1

> **Status:** 检查点 1 第 1–4 项完成（Spark 3.5.9 端到端跑通 + 与 parquet 的规划/存储对比），第 5 项（改动面清单）见本文；检查点 2 被两个已知缺口阻塞（见 §4）。
> **分支/环境:** fork 基线 `apache-iceberg-1.11.0-688`；JDK 17；`-DsparkVersions=3.5 -DscalaVersion=2.12`；Arrow **15.0.2**（与 Spark 3.5、厂内 Lance bundle 同代，非 Arrow 18）
> **起点代码:** apache/iceberg #15751（走 File Format API 的 Lance POC）移植到我们的 fork

---

## 1. 做了什么

在本仓新增 `lance` 模块（`iceberg-lance`），把 Lance 接成 Iceberg 的一种 data file 格式：

| 改动 | 文件 |
|---|---|
| `FileFormat` 增加 `LANCE("lance", false)` | `api/src/main/java/org/apache/iceberg/FileFormat.java` |
| 注册 Lance 到格式注册表 | `core/src/main/java/org/apache/iceberg/formats/FormatModelRegistry.java` |
| 新模块 `iceberg-lance`（FormatModel / ReadBuilder / WriteBuilder / Arrow 转换 / Spark 适配） | `lance/`（17 个源文件，移植自 #15751） |
| 模块接线（settings/build）、Spark 3.5 测试依赖 | `settings.gradle`、`build.gradle`、`spark/v3.5/build.gradle` |

**Iceberg 仍然是唯一权威**：catalog、snapshot、manifest、delete/update 语义全在 Iceberg；Lance 只承担 data file 编码。没有使用 Lance 的 catalog/namespace。

## 2. 验证结果（Spark 3.5.9，local[2]，Hadoop catalog）

`TestLanceFormatTable`（5/5 绿）：

| 用例 | 验证内容 | 结果 |
|---|---|---|
| `writesAndReadsALanceBackedIcebergTable` | `CREATE TABLE ... TBLPROPERTIES('write.format.default'='lance')`、INSERT、SELECT、列投影、WHERE 过滤；`.files` 里 data file 后缀是 `.lance` | ✅ |
| `deleteUpdateAndMergeKeepIcebergSemantics` | DELETE / UPDATE / MERGE（copy-on-write）后读结果正确；重写后的 data file 仍是 `.lance` | ✅ |
| `mergeOnReadWritesIcebergDeleteFilesNotLanceFiles` | `write.delete.mode=merge-on-read`：行结果正确；delete file 由 Iceberg 写、可被 Iceberg 读 | ✅ |
| `deleteFileFormatCanStayOnIcebergsDefaultWhileDataIsLance` | 显式 `write.delete.format.default=parquet` 时 delete file 是 `.parquet`（删除路径可以完全留在 parquet） | ✅ |
| `timeTravelStillSeesTheOldSnapshot` | `VERSION AS OF <snapshot>` 看到旧快照（2 行）而最新快照是 3 行 | ✅ |

`TestLanceReaderClose`（3/3 绿）：纯 JVM 复现 reader 生命周期（全量消费后显式 close、连续读写两个文件、只消费一部分就 close）。

`TestLanceRoundTrip` / `TestLanceFormatModel` / `TestLanceSchemaConversion`（19/19 绿）：格式级读写闭环、schema 转换、null、投影。

### 一个重要的默认行为（值得记住）

`write.format.default=lance` 时，**delete file 也默认是 Lance**：`SparkWriteConf.deleteFileFormat()` 在没设 `write.delete.format.default` 时回落到表的 data format。想保持"删除走 parquet"必须显式设置 `write.delete.format.default=parquet`（上面第 4 个用例验证了这条）。

## 3. 与 parquet 的规划/存储对比（200k 行，4 个 data file）

`TestLanceFormatComparison`（本机 local[4]，parquet row group 设成 256KB 以暴露 split 行为）：

| 格式 | data file | Iceberg 规划任务 | Spark 读分区 | data 字节 | count(*) | 全表扫描 |
|---|---|---|---|---|---|---|
| Lance | 4 | **4**（1 文件 1 任务） | 1 | 4.89 MB | 40 ms | 76 ms |
| parquet | 4 | **5** | **14** | 0.65 MB | 34 ms | 89 ms |

- **splittable=false 是真的**：Lance 一个文件只能出一个任务（`FileFormat.LANCE("lance", false)`），parquet 同一批数据被切成 14 个读分区。小数据量、本地盘上这个差异**没有造成可见的耗时惩罚**（76ms vs 89ms 在噪声内）；风险在"大文件 + 大表"——Vortex 的公开实测就是在大 join 上一度慢 2x，补了 row-splittability 才好（见调研 §11.3）。
- **存储**：本例 Lance 4.89MB vs parquet 0.65MB（7.5x）。这是**合成数据的最坏情况**（payload 高度重复，parquet 字典编码收益极大）；真实数据参考第三方实测是 **+50%**，我们在 1M×128 维基准里两者同量级。
- 缓解手段（两者不互斥）：Lance 侧多写小文件换并行度（我们生产表本来就是 ~3MB 小文件，天然适配）；或给 Iceberg 规划器补 row-split（Vortex 那条路）。

## 4. 发现的缺口（移植过程中撞出来的，全部有复现）

1. **Lance SDK：`ArrowArrayStream` 所有权 bug（阻塞级，已修，需上游或 fork）**
   `LanceFileReader.readAll` 用 try-with-resources 包住 `ArrowArrayStream`，在 `Data.importArrayStream` 之后仍然 close 了 wrapper → reader 关闭时 NPE `ArrowArrayStream is already closed`，且 native stream 从不 release（泄漏）。
   症状：任何"正确关闭 reader"的调用方（Iceberg、Spark）在任务收尾时失败。
   修：`lance/patches/0001-lance-file-reader-arrow-stream-ownership.patch`（4 行）。本机用打过补丁的 `org.lance:lance-core:5.0.0-beta.6-poc1` 验证通过。
   *副作用经验*：POC #15751 的单测之所以全绿，是因为它的用例没有显式 close iterator；这个 bug 在真实引擎里必现。
2. **打开路径必须容忍重复 close**：Spark 的 `BaseReader.close()` 会被调用多次，而 Lance reader 的资源是一次性的 → `close()` 必须幂等。
3. **元数据列不属于 Lance 文件**：`_file` / `_pos` 由 Iceberg 提供，不能向 Lance 索取（否则 `Invalid user input: Column _file does not exist`）。且 `_pos` 必须**按行**生成（batch 起始 offset + 行内序号），不能按 batch 注常量，否则 MOR 删除会删错行——这也是移植代码里没有的。
4. **不支持 LIST / FIXED_SIZE_LIST**（阻塞检查点 2）：Spark writer 直接抛 `Unsupported type for Spark writer: LIST`；generic 转换器也没有 list 分支。**向量列就是 array**，所以"用 Lance 存向量表"必须先补这个（读写两侧 + Spark 三条 reader 路径）。
5. Lance Java SDK 原有 5 项缺口依然成立（文件长度、列统计、split 规划、谓词下推、NameMapping），见 `lance/LANCE_SDK_GAPS.md`。

## 5. 改动面与接线成本（检查点 1 第 5 项）

| 项 | 成本 |
|---|---|
| Iceberg 侧改动 | `FileFormat` +1 行、注册表 +1 行、新模块 17 个文件（移植版 ~2.9k 行）+ Spark 模块测试依赖；**core 的 scan/delete 逻辑一行未改** |
| 依赖 | `org.lance:lance-core`（165MB，含各平台 native）、`arrow-c-data`、`netty-buffer`；Arrow 可留在 15.0.2，**不需要升到 18** |
| SDK 缺口 | 至少需要上面第 1、3、4 项；第 1 项是上游 bug（要么推动上游修，要么维护 fork，和 HDFS provider 一样） |
| 规范/互操作 | manifest 的 `file_format` 目前只列 avro/orc/parquet(+puffin)；Lance data file 属规范扩展，其它引擎（Trino/Flink/pyiceberg）要各自实现 reader |
| HDFS 读路径 | 用厂内 fork 的 HDFS object store provider（上游 object_store 没有 hdfs provider）；本次本机验证走本地盘，HDFS 留到检查点 2 在集群上验 |
| Gluten | 本轮**不涉及**：测试全程 Gluten-free（用户要求），Arrow classpath 冲突问题不在此 POC 范围内 |

## 6. 下一步（检查点 2）

1. **先补 LIST/FIXED_SIZE_LIST 支持**（`LanceSchemaUtil` + `LanceArrowConverter` + Spark 三条 reader 路径 + writer），否则向量列无法写入 Lance 表。
2. 在 Lance-backed 表上建向量索引（复用 #396/#438 流程），跑 SIFT1M 改写生效 + recall@10（复现 #398 测法）。
3. 回表段：确认索引条目 `(dataFile, _pos)` 语义不变，测回表 10 行延迟（对比 parquet 路径的 3.2s 基线）与 TIMING 四段归因。
4. 集群验证（厂内 Lance fork + HDFS + 专用 Gluten-free engine）。

## 7. 复现

```bash
cd ~/workspace/iceberg
export JAVA_HOME=/opt/homebrew/opt/openjdk@17

# 格式级
./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 :iceberg-lance:test

# Spark 3.5 端到端
./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 \
  :iceberg-spark:iceberg-spark-extensions-3.5_2.12:test \
  --tests "org.apache.iceberg.spark.extensions.TestLanceFormatTable"

# 与 parquet 的规划/存储对比（结果写到 spark/v3.5/spark-extensions/target/lance-poc-comparison.txt）
./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 \
  :iceberg-spark:iceberg-spark-extensions-3.5_2.12:test \
  --tests "org.apache.iceberg.spark.extensions.TestLanceFormatComparison"
```

本地依赖：`~/lance-m2/org/lance/lance-core/5.0.0-beta.6-poc1/`（打过补丁的 SDK，由 `lance/patches/0001-*.patch` 生成）+ `~/.gradle/init.d/00-cn-mirror.gradle`（镜像源 + 本地仓库）。

---

## 8. 检查点 2 进展（2026-10-15 续）

### 已清障：LIST / FIXED_SIZE_LIST 支持

向量列是 array，原来直接抛 `Unsupported type for Spark writer: LIST`。已补齐：

| 层 | 改动 |
|---|---|
| schema | 双向转换原本已支持 list（`ArrowType.List` ↔ `Types.ListType`） |
| generic 写 | `LanceArrowConverter.writeListValue`：Java List → `ListVector`（boolean/int/long/float/double/string 元素） |
| generic 读 | `readListValue`：`ListVector` → Java List |
| Spark 写 | `SparkLanceWriter.extractList`：`InternalRow.getArray` → Java List |
| Spark 行读 | `SparkLanceRowReader.readSparkList`：`ListVector` → `GenericArrayData`（覆盖 `array<float>` 向量列） |

验证：新增 `TestLanceFormatTable.writesAndReadsAListColumn`（3000 行 × `array<float>[4]`，校验 `sum(size(vec))`、`sum(vec[0])`、逐行取值、data file 仍为 `.lance`）→ **6/6 全绿**。

### （已解决，见 §9）新阻塞点：索引层把数据文件读取硬编码成 parquet

- `index-spark/.../IndexEntries.java:297`：`spark.read().parquet(dataFile)`（回表 `liveRowsOf`）
- 同一文件里建索引/读取向量列也走同一条 Spark parquet 读路径
- 现象：在 Lance-backed 表上 `CALL system.build_vector_index(...)` 直接失败
  `[CANNOT_READ_FILE_FOOTER] ... .lance is not a parquet file`

这不是 Lance 的问题，是索引层自己的假设。**修复方向（待做）**：

1. 把"读一个数据文件的行"抽成一个 format-aware 的读取：
   `FileFormat.fromFileName(path)` + `FormatModelRegistry.readBuilder(format, <targetType>, inputFile)`，
   Lance 侧我们已注册 `Record.class`（generic）与 Spark 的 `InternalRow` / `ColumnarBatch` 模型，parquet/orc/avro 也有对应模型 → 一套代码覆盖所有格式。
2. `liveRowsOf` 现在靠 `toJavaRDD().zipWithIndex()` 拿行位置，这是 parquet 单文件读的副作用；换成 format-aware 读取后位置应来自读取器（Lance 是 (fragment, offset)，parquet 是行组内偏移），比 `zipWithIndex` 更可靠。
3. delete 文件读取（`IndexEntries` 里 `spark.read().parquet(deleteFile)`）同样要 format-aware；本轮 POC 可先把 `write.delete.format.default` 固定在 parquet 以减少变量。
4. 回归门槛：`TestRewriteVectorSearch`（12 个用例）必须保持全绿——它是索引改写行为的护栏。

`TestLanceFetchBack`（Lance 表 vs parquet 表的回表 A/B）已写好并按上述原因 `@Disabled`，解除阻塞后直接启用即可给出检查点 2 的端到端数字（回表延迟 + 相同结果校验）。


---

## 9. 检查点 2 完成：索引层 format-aware 读取 + Lance 回表端到端

### 改动（index-spark，孩子卡 #983）

`IndexEntries.liveRowsOf` 原来用 `spark.read().parquet(dataFile)` 读数据文件（建索引与回表共用）。现在抽成 `positionedRows`：

| 格式 | 读取方式 |
|---|---|
| parquet | 保留原 Spark parquet 读（向量化、行为与耗时不变） |
| 其它格式 | `task.file().format()` + `FormatModelRegistry.readBuilder(format, Record.class, inputFile)`（executor 侧 `HadoopInputFile` + `SerializableConfiguration`），行位置由读取顺序给出，不再依赖 `zipWithIndex` |

配套：Iceberg→Spark 值的本地转换（primitive + list；数组按 Spark 外部 Row 的约定返回 Scala Seq，与 `columnValue` 的期望一致）；delete 文件读取仍是 parquet（POC 固定 `write.delete.format.default=parquet`，Lance delete 文件留作后续）。

**护栏**：`TestRewriteVectorSearch` 12/12 保持绿（parquet 路径无回归）。

### 端到端结果（`TestLanceFetchBack`，200k 行 × 32 维，两种格式同数据同索引）

```
rows=200000 dim=32 topK=10
query whole plan   lance=336ms parquet=290ms
fetchback 10 rows  lance=79ms  parquet=87ms
data bytes         lance=31.18MB parquet=6.93MB
full scan no index (lance)=198ms
topId=12345 sameResults=true
```

- **正确性成立**：Lance-backed 表能建向量索引、能跑改写后的 `ORDER BY vec_distance(...) LIMIT k`，结果与 parquet 表**逐行一致**（`sameResults=true`）。
- **回表段 79ms vs 87ms**（约 9%）：这个规模下**Spark 任务启动开销（几十 ms）盖过了格式差异**，与 #707 的格式级数据（每次回表 32KB vs 31MB I/O）不矛盾——差距要在大表上才显出来。
- **整条查询 lance 336ms vs parquet 290ms，且都慢于无索引全扫 198ms**：20 万行规模下索引+回表的路子本来就不划算（生产基线是 SIFT1M 上"全扫 17.7s vs 索引 7.65s"）。
- **存储**：本例 Lance 31.2MB vs parquet 6.9MB（4.5x）。payload 是 md5 十六进制字符串、向量是 sin 值，parquet 的 snappy+字典收益大；Lance 默认编码在这个数据上明显偏大 → **上生产前要单独调压缩/编码配置**（第三方实测是 +50%，本例更差，说明这取决于数据形态）。

**结论**：集成链路（建索引、回表、改写、结果一致性）在本地已经跑通并可回归；**回表加速的量化必须在集群上用 SIFT1M/10M 规模重跑**（即 #810 检查点 2 的剩余部分），本机 20 万行无法体现。

---

## 10. 集群验证（#1037）：SIFT1M 上 Lance vs parquet 的回表 A/B

环境：A 集群（sh-a-spark），k8s spark-submit，3 executor × 2 core × 10g，**Gluten-free**（镜像里本来就没有 Gluten），Lance 走镜像主类路径。

### 结果（同一批查询、同一套 HNSW 索引、100 万 × 128 维，warm run）

| | parquet（sift1m_base） | Lance（sift1m_lance_v2） |
|---|---|---|
| 行数 / 数据文件 | 1,000,000 / 28 | 1,000,000 / 9 |
| 数据字节 | 94.4 MB | **518.7 MB（5.5x）** |
| 索引 | HNSW，PUBLISHED | HNSW，PUBLISHED |
| TIMING plan data files | 11 ms | 5 ms |
| TIMING uncovered | 14 ms | 8 ms |
| **TIMING search+resolve（含回表）** | **3281 ms** | **1581 ms（2.08x 快）** |
| TIMING merge | 0 ms | 0 ms |
| **整条查询（best of 3）** | **3317 ms** | **1784 ms（2.07x 快）** |
| top-10 pk | 932085,934876,561813,708177,706771,695756,435345,701258,455537,872728 | **完全相同** |

冷启 run0：parquet 5957ms / Lance 2450ms。索引搜索在两边是同一份索引、同样工作量，所以 search+resolve 的 1.7s 差额来自**读命中行那一步（回表）**——即回表路径在 Lance 上约快 2x，整条查询 3.3s → 1.8s。
（生产基线参考：全扫 17.7s / 索引 7.65s，其中回表段 3.2s。本次 parquet 侧 3.3s 与基线同量级。）

### 集群侧踩到的坑（都已解决，属于本 POC 的接线成本）

1. **镜像里的 Lance bundle 没有 HDFS provider**：基础镜像自带的 `lance-spark-bundle` 里 `.so` 是 160MB、无 hdfs 符号；换成厂内 `lance-tpcds-hdfs` worktree 里**刷新过 JNI 的 bundle**（285MB `.so`，含 HDFS provider）后，HDFS 读写才通。
2. **Arrow 代际混用可用**：镜像里 Spark 自带 arrow 12.0.1，bundle 自带 arrow 15.0.2（c-data 只在 bundle 里）。我们按 15.0.2 编译的 `iceberg-lance.jar` 在这个组合下**实测可用**（冒烟 + 全流程）。
3. **索引构建 OOM**：4g executor 跑索引构建被 SIGKILL（exit 137）；提到 **10g + `spark.sql.shuffle.partitions=32`**（Lance 文件更小）后通过。根因是 format model 读取路径**逐行装箱**（`List<Float>`），内存约为原始 float 的 4 倍；后续可做向量化读取（列为优化项）。
4. **`DROP TABLE ... PURGE` 不清 `<table>/indexes/`**：重建同名表会捡到旧索引，`build_vector_index` 直接报 "A vector index already exists"（且没有 `drop_vector_index` 过程）。本次用新表名绕过。**这是索引层自身的卫生问题**（孤儿索引清理），与 Lance 无关，值得单独开卡。
5. **EXACT 索引会让改写让开**（卡片 #573 的设计）：第一次 A/B 用了 `mode => 'exact'`，结果三条 Lance 查询全部走全扫（日志：`Index ... is exact, which searches every entry like a scan does, so scanning`），2.7s 的数字其实是 Lance 全扫；换成 **HNSW** 后才真正走索引，才有上面的对比。

### 结论（检查点 2）

- **B2b 端到端成立**：Lance 作为 Iceberg data file 格式，在集群 100 万 × 128 维上完成「建表 → 建 HNSW 索引 → 改写命中 → 回表 → 结果与 parquet 逐行一致」，回表路径约 **2x**、整条查询 3.3s → 1.8s。
- **代价明确**：同一份 SIFT1M 数据 Lance 占 **5.5x** 空间（94MB → 519MB，SIFT 是 0–255 的整数但以 float32 存）。上生产前必须做编码/压缩选型（列式字典、量化为 uint8 等）。
- **剩余工作**：向量化读取降低内存（同时会让回表更快）；索引层面的孤儿清理/`drop_vector_index`；以及 #944 的 10M 规模复测。

### 10.1 为什么 Lance 文件大 5.5x：定量拆解

先看每值字节数（1,000,000 × 128 = 128M 个值）：

| | 字节 | B/value |
|---|---|---|
| 原始 float32 | 512.0 MB | 4.00 |
| 集群实测 parquet（sift1m_base） | 94.4 MB | **0.74** |
| 集群实测 Lance（sift1m_lance_v2） | 518.7 MB | **4.05** |

同形状合成数据（1M×128，取值 0..116，float32，pyarrow 写入）验证机制：

| 写法 | 字节 | B/value |
|---|---|---|
| parquet 默认（snappy + 字典） | 116.8 MB | 0.91 |
| parquet snappy，**关字典** | 261.5 MB | 2.04 |
| parquet **无压缩无字典** | 525.0 MB | 4.10 |
| parquet 存成 uint8（snappy + 字典） | 116.8 MB | 0.91 |

结论有三条：

1. **差距几乎全部来自 parquet 的字典编码**：SIFT 描述子本就是 0..255 的整数（实测值域 0..116），却以 float32 存 —— 只有 ~117 个不同值。parquet 把它编码成 ~7 bit 的字典索引 + RLE，于是 0.7–0.9 B/value；关掉字典立刻变 2.0，再关压缩就是 4.1。
2. **Lance 的 v2 默认编码对 float32 列基本不做处理**：518.7MB ≈ 512MB 原始大小（+1% 元数据）。它的字典/bitpacking/RLE 主要服务字符串与整数列，浮点列落回 plain。所以不是"Lance 天生费 5 倍"，而是"parquet 吃到了这份数据的低基数字典红利，Lance 没吃"。
3. **高熵数据下两者持平**：本地 1M×128 正态分布随机 float 基准（#707）Lance 514MB vs parquet 521MB。低熵数据（SIFT）才拉开 5.5x。

**Lance 侧目前没有编码/压缩开关**：`WriteParams` / `org.lance.file` 都没有 compression/encoding 选项，Java SDK 无法指定（要走就得改 Rust 默认或走 Dataset 侧的写参数）。可用的手段在数据表示层：

- **把向量列改存整数**（SIFT 的原生形态，`array<int>`/tinyint）：原始就是 128MB，Lance 的整数编码（bitpacking/RLE）会生效 → 预期大幅缩小；parquet 仍在 ~117MB。需要给 `LanceArrowConverter`/Spark writer 补 int8/int16 元素类型，索引适配器把 `Number` 转 float 即可。
- **float16 或标量量化/二值化**：2 B/value 或 1 B/value，同时加速 ANN。
- 注意**权衡**：任何压缩都会给刚变快 2x 的回表路径加解压 CPU，需要实测净收益（建议下一步就是"uint8 版 Lance 表：体积 + 查询耗时"一起测）。

索引文件本身已经是 zstd 压缩过的（#575），5.5x 只关于数据文件。

### 10.2 Lance 有没有 parquet 那样的编码能力？——有，但默认关着，且字典编码不覆盖 32 位值

**能力清单**（`rust/lance-encoding`，规范见 `docs/src/format/file/encoding.md`）：字典编码、Bitpacking、RLE、FSST（变长）、Byte Stream Split（BSS，浮点）、Constant（全 null）、以及 general 压缩（lz4 / zstd / snappy）。配置项写在 **Arrow field metadata** 里（也可用 `LANCE_ENCODING_*` 环境变量）：

| Key | 默认 | 说明 |
|---|---|---|
| `lance-encoding:compression` | **none** | lz4 / zstd / none，是否做通用压缩 |
| `lance-encoding:general` | **off** | 是否应用 general 压缩 |
| `lance-encoding:bss` | auto | 字节流切分，**只在 general 压缩开启时才生效** |
| `lance-encoding:rle-threshold` | 0.5 | runs/values 低于它才用 RLE |
| `lance-encoding:dict-divisor` / `dict-size-ratio` | 2 / 0.8 | 字典编码的启用门限 |
| `lance-encoding:dict-values-compression` | lz4 | 字典值本身的压缩 |

**为什么我们的列一点没吃到**：两件事叠加

1. **默认不做通用压缩**（`compression=none`、`general=off`，BSS 也因此不生效）——parquet 默认却是 snappy + 字典，两边默认策略相反（Lance 偏向随机读速度）。
2. **字典编码在代码里只覆盖 64/128 位定宽值**：`rust/lance-encoding/src/encodings/logical/primitive.rs:4884` 附近
   ```rust
   DataBlock::FixedWidth(fixed) => {
       if fixed.bits_per_value != 64 && fixed.bits_per_value != 128 { return None; }
   ```
   → `array<float>` 是 32 位，**永远不会被字典编码**（无论门限怎么调）。32 位浮点能用的只有 plain / BSS / lz4-zstd。
   而且 64 位值那条路要 **文件格式 ≥ 2.2**，而 2.2 在规范里标注为 **unstable**（见下），默认/稳定版是 2.1 → 生产上等于拿不到字典编码。

**实测（同 200k×128、取值 0..116，本地 lance 模块 `TestLanceEncodingSizes`）**：

| 列类型 | 字节 | B/value |
|---|---|---|
| `array<float>` | 103,674,594 | **4.050** |
| `array<int>` | 26,874,592 | **1.050（3.86x 小）** |

整数列触发了 bitpacking（mini-block 1024 值/块、块内 7 bit）。对照 parquet 的 0.74–0.91 B/value，**把向量列存成整数后 5.5x 缩到约 1.2–1.4x**。

**对我们的意义**：SIFT 描述子本来就是 0..255 的整数，存成 `array<int>`（或 tinyint）语义更正确、体积直接降 3.86x，而且**我们现有的写入路径已经支持**（`LanceArrowConverter` 的 LIST 分支已支持 INTEGER 元素）。进一步可用 `lance-encoding:compression=zstd` + BSS 缩小剩余差距——但需要把 Iceberg 列属性映射成 Arrow field metadata（`LanceSchemaUtil` 目前只写 `ICEBERG_FIELD_ID`，Java SDK 的 `WriteParams` 本身没有编码开关）。

**待验证**：① 索引/改写路径是否接受 `array<int>` 向量列（适配器把 `Number` 转 float，可能只差一个类型校验）；② 开压缩后回表耗时的净变化（压缩省空间但加解压 CPU）。


### 10.3 版本速查（2026-10-15 核对）

**文件（编码）格式版本**（`rust/lance-encoding/src/version.rs` + `docs/src/format/file/versioning.md`）：

| 版本 | 状态 |
|---|---|
| 0.1 | legacy，已不能写 |
| 2.0 | 稳定 |
| **2.1** | **默认 / 最新稳定**（`#[default] V2_1`，`stable` 别名解析到它） |
| 2.2 | **unstable**（文档标注；64 位值的字典编码、map 支持等从这代开始） |
| **2.3** | **最新，但不稳定**，只能经 `next` 别名使用（文档警告不可用于生产） |

**软件版本**：我们 fork 的 crate = `5.0.0-beta.6`；crates.io `lance` 最新 = **13.0.0**；Maven Central `org.lance:lance-core` 最新 = **14.0.0-beta.18**（2026-10-15 更新，Java 侧版本线独立）。

**表（dataset/table）格式**：无单一数字版本，用 feature flags（`FLAG_DELETION_FILES` / `FLAG_STABLE_ROW_IDS` / `FLAG_TABLE_CONFIG` / `FLAG_BASE_PATHS`；≥32 的未知位会被拒绝）。

**对我们的意义**：① 我们写出的文件是默认 **2.1**，bitpacking 在这代就生效（所以"整数列"方案可用）；② 字典编码要 2.2（unstable）或变长数据，生产不可依赖；③ Java SDK 5.0.0-beta.6 与上游 14.0.0-beta.18 差距很大，升级要连带 rebase 公司 HDFS 补丁——这是"跟随上游"的真实成本。

### 10.4 2.1（当前稳定版）下 SIFT 向量列的压缩配方（实测）

同一批数据（200k×128、值 0..116），全部用默认的 2.1 格式，本地 lance 模块 `TestLanceCompressionOptions` / `TestLanceEncodingSizes` 实测：

| 手段 | B/value | 相对 float32 原始 | 需要配置 |
|---|---|---|---|
| `array<float>` plain（我们现状） | 4.050 | 1.00x | — |
| **`array<int>` plain**（bitpacking） | **1.050** | **3.86x** | 无（换列类型即可） |
| **`array<float>` + `compression=zstd`**（level 9） | **1.326** | **3.05x** | 需写 field metadata |
| `array<float>` + `compression=lz4` | 1.776 | 2.28x | 同上 |
| `array<int>` + zstd（叠加） | 1.086 | 3.73x | 同上，**反而更大** |
| 参考：parquet（默认 snappy+字典） | 0.74–0.91 | 4.4–5.5x | — |

要点：

1. **通用压缩默认是关的**，打开就有 3x——这是"2.1 已经支持、我们没用"的那块能力。配置键写在 **Arrow field metadata**（编码器只从 field metadata 读，`compression` 没有环境变量回退）：`lance-encoding:compression=zstd`、`lance-encoding:compression-level=9`、`lance-encoding:general=on`。`bss=on` 在 zstd 之上没有额外收益（实测 1.326 → 1.326）。
2. **整数列（bitpacking）比 zstd 更好且零配置**（1.05 vs 1.33），但会改列类型（`array<float>` → `array<int>`），需要确认索引/改写路径接受整数向量列。
3. **两者不要叠加**：bitpacking 之后再 zstd 只会变大（1.050 → 1.086）。
4. **lz4 是速度折中**：1.776 B/value（仍比原始小 2.3x），解压比 zstd 快，适合"回表延迟敏感"的场景。
5. 想再往下只能靠**语义量化**（4-bit 码 → ~0.5 B/value、1-bit 码 → ~0.125 B/value + 码本），这是检索精度/体积的权衡，不属于格式能力。
6. **接入成本**：`LanceSchemaUtil.convertField` 目前只写 `ICEBERG_FIELD_ID`；要把 Iceberg 侧属性（例如表属性 `lance.encoding.compression=zstd`）映射到 list element 的 field metadata，改动很小（写入路径的 `ModelWriteBuilder.set()` 已经能把 writer 属性带进来）。

**下一步该测的**：把"体积"和"回表耗时"一起测（zstd/lz4 会在刚快 2x 的回表路径上加解压 CPU）——建议用 SIFT1M 在集群上跑 `float32+plain` / `float32+zstd` / `float32+lz4` / `int32` 四组。

### 10.5 为什么 Lance 的字典编码排除了 32 位？（官方理由，2025-10-20 issue #5004）

维护者原文（issue #5004，至今 open）：

> 目前 2.1 的字典编码只覆盖部分 128 位定宽类型。我认为这是几件事叠加：
> * 当时我们**还没有对字典索引做 bit-packing**，所以把字典编码用于更小的类型不太可能有收益；
> * 在**所有列上估计 cardinality 太贵**；
> * TPC-H 数据有大量 128 位 decimal 能从字典编码受益，因此专注 128 位有**被夸大且不真实**的 benchmark 收益。
> 不过我们至少应该考虑 64 位、甚至 32 位类型。

逐条对上代码：

1. **索引没 bit-pack 之前，窄类型做了不划算**：索引常量 `DICT_INDICES_BITS_PER_VALUE = 32`，若索引固定 4 字节，4 字节的值变成「4 字节索引 + 字典」= 不省甚至更差；只有 128 位大值划算。
2. **cardinality 估计太贵** → 代码在**估计阶段直接跳过**（不是评估后放弃）：
   ```rust
   let bytes_per_value = match fixed_block.bits_per_value { 64 => 8, 128 => 16, _ => return None };
   ```
   低基数 float32 列连被评估的机会都没有。
3. **benchmark 数据偏置**：TPC-H 的 128 位 decimal 让首版只做 128 位。

演进（都是"逐代加"，非"永久排除"）：

| 时间 | 事件 |
|---|---|
| 2025-09-25 | #4813 支持已是字典数组的数据（2.1） |
| 2025-10-20 | **#5004 open：允许字典编码支持更多定宽类型**（"至少 64 位，也许 32 位"） |
| 2026-01-01 | **#5594 加 64 位字典（int64/double）**，随**格式 2.2** 落地；PR 表格里直接拿 Parquet 做对照 |
| 2026-01-28 | #5011 索引统一 32 位；#5055 不划算就中止字典 |
| 2026-02-25 | #5891 用字典条目/编码尺寸替代 cardinality 决策 |
| 今天的主线 | `64|128` 之外仍 `return None` → **32 位仍未实现** |

**结论**：32 位（`array<float>`）不是设计性排除，而是历史范围遗留；已在 #5004 挂着，但 open 近一年且落地要新格式版本号 → 不要等。2.1 下仍是：整数 bitpacking 1.05 / float32+zstd 1.33 / float32+lz4 1.78（parquet 字典 0.73）。Lance 官方自己也拿 Parquet 当压缩对标（#5594），说明这是他们已知短板。
