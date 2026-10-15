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
