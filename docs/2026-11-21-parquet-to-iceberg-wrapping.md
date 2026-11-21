# 存量 Parquet 接入 Iceberg 与向量检索：方案评估（2026-11-21）

> 状态：方案评估，尚未执行任何导入或建索引操作。代码结论基于本分支 `index`（基线 `apache-iceberg-1.9.1`，Spark 3.5 运行时）；性能数字引用 2026-11-19 的实测。真实业务数据上的 PoC 未做。

## 结论

能用 `add_files` 把现成 parquet 目录"登记"成一张 Iceberg 表的数据文件：只读每个文件的 footer 生成清单和统计，不移动、不改写文件内容。登记后 `build_vector_index`、`ORDER BY vec_distance` 改写、`vector_search` 都可用，不需要为向量检索单独开发表格式支持。

三个前提缺一不可：

1. 向量列在 parquet 里是 `LIST<FLOAT>` 或 `LIST<DOUBLE>`（Spark 的 `array<float>`/`array<double>`）；
2. 数据文件"写一次不再改"，路径内容保持不变；
3. 目标表的列名、类型、identity 分区与物理数据一致。

不满足第 1 条（向量存成 `binary` 或字符串）时改走"索引专表"；不满足第 2 条时零拷贝不安全，要么改上游写入方式，要么走重写方案。

## 为什么能零拷贝：代码依据

- **路径源与分区推断**：`AddFilesProcedure.java:185` 的 `importFileTable` 接受 `` `parquet`.`<目录>` `` 形式，`Spark3Util.getInferredSpec`（同文件 :194）从目录名推断分区；`SparkTableUtil.findCompatibleSpec`（`SparkTableUtil.java:1179`）只接受 identity 分区，且分区列名要与目标表一致。代码里没有"文件必须在表 location 下"的校验，所以业务数据可以留在原目录，只把元数据放到新目录。
- **name mapping**：业务 parquet 没有 Iceberg field id。`AddFilesProcedure.ensureNameMappingPresent`（:176）会自动把目标表 schema 写成 `schema.name-mapping.default`；查询侧 `BaseReader` 和文本过滤读都靠它做名字解析。列名必须与 parquet 完全一致（区分大小写）。
- **统计**：导入时 `TableMigrationUtil.listPartition`（`data/src/main/java/org/apache/iceberg/data/TableMigrationUtil.java:228`）调用 `ParquetUtil.fileMetrics` 读 footer，得到 recordCount 和列统计。索引分组 `IndexGrouping.java:78` 直接用 `dataFile.recordCount()`，所以必须走正规导入，不能手工拼 metadata.json。
- **读路径**：建索引时 parquet 由 Spark 自己的 reader 按列名读取（`IndexEntries.java:1159`、`:1163` 的 `spark.read().schema(readSchema).parquet(dataFile)`）；查询回表走 Iceberg Spark scan，靠 name mapping。两条路都要求列名与表 schema 对齐。
- **既有能力边界**：向量列类型见 `docs/index-user-guide.md` §3.1；`add_files` 不校验 schema、不移动文件、被加入的文件归 Iceberg 所有，见 `docs/docs/spark-procedures.md` 的 `add_files` 一节。

## 接入步骤

### 第 1 步：单分区 PoC（只读，不改业务数据）

```sql
-- 元数据放可写的新目录，业务数据原地不动
CREATE TABLE cat.db.foo (id bigint, ..., embedding array<float>)
USING iceberg
PARTITIONED BY (dt)
LOCATION 's3://lake/iceberg-meta/biz_foo';

CALL cat.system.add_files(
  table => 'db.foo',
  source_table => '`parquet`.`s3://biz/foo`',
  partition_filter => map('dt', '2026-11-18'),
  parallelism => 8);
```

验收三件事：(1) `count(*)` 与源目录一致；(2) 抽 100 行与外部脚本手算的 L2 距离对得上；(3) `build_vector_index` 后，改写查询与全扫结果对比 recall。

### 第 2 步：全量导入与首次建索引

按分区或日期分批 `add_files`，每批形成一个快照，最后建一次全量索引。索引构建成本随 rows×dim 线性增长：参考 2026-11-19 的实测，10,784,907 行 × 2560 维、12 executor × 4c，native 构建 236,687 ms，33 个索引文件合计 17,890,137,090 B。先用这个口径估算全量时间与索引存储。

### 第 3 步：增量同步

新分区 `add_files` 后 `refresh_vector_index`，只重建新增或变更文件对应的索引文件。`check_duplicate_files=true` 会 join entries 元数据表，大表上按分区批导更稳。

## 风险与红线

**文件不可变是硬前提。** 索引条目是 `(data_file, 文件内行位置)`。上游如果按相同路径覆盖写、或清空后重写同名文件，索引不会报错，但会指向错误的行。Spark/Hive 正常写新文件名没问题；需要确认数据管道的提交方式。

**Iceberg 会把这些文件当成自己的。** `DROP TABLE ... PURGE`、把快照 expire 到最后、`DELETE` 或覆盖写之后再 expire，都可能物理删除业务文件。缓解：表 location 与数据目录分离（`remove_orphan_files` 只扫表 location）；把包装表设为只读，禁 purge、禁 compaction/rewrite，权限收紧。

**schema 不校验。** 列名或类型不一致会读出错列或运行期失败；上游 rename 列会破坏 name mapping。上游 schema 变更必须同步到包装表。另外确认会话没有开启 `spark.sql.parquet.fieldId.read.enabled`（默认 false），否则没有 field id 的文件读不了。

**分区只支持 identity。** 非 Hive 风格目录只能当无分区表，查询没有分区裁剪；分区列类型要与 Spark 的路径推断一致，否则 `findCompatibleSpec` 找不到匹配的 spec。

**小文件成本。** `add_files` 每文件读一次 footer，建索引按文件起任务；文件特别多时先在 PoC 上量单分区耗时，再乘总数。

**应用入口。** 检索入口是 Spark SQL（`ORDER BY vec_distance` 改写或表函数）；业务应用如果不是 Spark 客户端，还需要服务化封装（Thrift Server 或自建 API），这与表格式无关。

## 备选方案与取舍

| 方案 | 是否动数据 | 适用 | 代价 |
|---|---|---|---|
| `add_files` 包装 | 不动 | 文件不可变、schema 对齐 | Iceberg 接管文件所有权、需要持续同步 |
| `migrate`/`snapshot`/CTAS | 全量重写 | 后续新数据也要进 Iceberg | TB 级重写与双份存储 |
| 索引专表 `(id, embedding)` | 只拷向量列 | 向量类型不兼容、不想让 Iceberg 管业务文件 | 多存一份向量、按 id 回源 join |

`migrate`/`snapshot` 适合"收掉两套系统"的长期目标，不适合存量冷数据的一次性接入。索引专表是向量类型不兼容时的兜底：在源 parquet 上只抽 `(业务主键, array<float>)` 写成一张小 Iceberg 表，检索拿 id 回源取整行。手工生成 metadata.json/manifest 与 `add_files` 等价，除非遇到它表达不了的场景（非 identity 分区、需要断点续传的超大批量），否则不值得自建，还要自己维护统计和 name mapping。

## 待确认事实

决定走哪条路之前需要确认四项：向量列在 parquet 中的物理类型（`list<float>`/`list<double>`/`binary`/`string`）；目录是否 Hive 分区、分区列及其类型；单文件行数与大小、总文件数；上游是否可能就地覆盖写同名文件。四项确认后按第 1 步做只读 PoC，再决定全量方案。

## 附：相关代码与文档

- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/AddFilesProcedure.java`
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/SparkTableUtil.java`
- `data/src/main/java/org/apache/iceberg/data/TableMigrationUtil.java`
- `index-spark/src/main/java/org/apache/iceberg/index/spark/IndexGrouping.java`
- `index-spark/src/main/java/org/apache/iceberg/index/spark/IndexEntries.java`
- `docs/index-user-guide.md` §3、§4.3
- `docs/docs/spark-procedures.md`（`add_files`）
