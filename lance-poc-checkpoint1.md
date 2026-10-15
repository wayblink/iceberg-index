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
