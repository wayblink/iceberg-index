# Iceberg 索引用户手册

适用版本：本分支（`step-1.9-index`，基线 `apache-iceberg-1.9.1`）新增的索引能力——向量检索与 BM25 全文检索。

这本手册按"要做一件事该敲什么"组织。设计取舍不在这里，需要时看 `vector-index-guide.md`（向量深入）与 `docs/vector-index-deployment-guide.md`（上线与运维）。

文中 `local` 指会话里的一个 Iceberg catalog 名，换成 `spark_catalog`、`hive`、`rest` 都行。所有过程都写作 `CALL local.system.<名>(...)`。

---

## 0. 一页看清能力与边界

能做的：在 `array<float>` / `array<double>` 列上建向量索引，做"最像的 k 条"；在 `string` 列上建 BM25 全文索引，做相关度排序和关键词过滤。两种索引都不改表结构，都按表快照增量刷新，同一列可以建多个（带名字）以便对比参数。

用之前必须知道的三件事：

1. **索引不会随写入自动维护**，新数据要手动 `refresh_*_index`。
2. **全文索引目前只对拉丁语系有效**，中文因为不分词基本不可用（5.1）。
3. **"精确 top-k"只在你用 `ORDER BY vec_distance(...) LIMIT k` 这种写法时成立**；过程和"索引地址"类表函数返回的是索引自己的排序（4.3）。

---

## 1. 前置条件

Spark 3.5（Scala 2.12）并加载 Iceberg 扩展。没有扩展，`vec_distance`、`match`、`text_score`、全部表函数、以及本文所有 SQL 语法糖都不存在：

```bash
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/tmp/warehouse
```

索引类型注册由每次 `CALL` 内部完成；但**自动改写**必须靠上面那个 `spark.sql.extensions`。

---

## 2. 十分钟上手

### 2.1 向量

```sql
CREATE TABLE local.db.docs (id bigint, title string, embedding array<double>) USING iceberg;

INSERT INTO local.db.docs VALUES
  (1, 'iceberg tables', array(1.0, 0.0)),
  (2, 'vector search',  array(0.9, 0.1)),
  (3, 'parquet files',  array(0.0, 1.0));

CALL local.system.build_vector_index(
  table => 'db.docs', column => 'embedding', dimension => 2, metric => 'COSINE');

-- 推荐写法：普通 SQL，优化器会改写成索引查询
SELECT id, title FROM local.db.docs
ORDER BY vec_distance(embedding, array(0.95d, 0.05d))
LIMIT 3;

CALL local.system.index_build_status(table => 'db.docs');
```

用 `EXPLAIN` 看有没有改写：计划里没有 `TableScan` 就说明命中了。

### 2.2 全文

```sql
CREATE TABLE local.db.papers (id bigint, body string, year int) USING iceberg;

INSERT INTO local.db.papers VALUES
  (1, 'iceberg table format with hidden partitioning', 2020),
  (2, 'vector search inside a lakehouse table', 2023),
  (3, 'parquet file layout and column statistics', 2020);

CALL local.system.build_text_index(table => 'db.papers', column => 'body');

-- 相关度排序：text_score 越大越相关，所以是 DESC
SELECT id, body FROM local.db.papers
ORDER BY text_score(body, 'table format') DESC
LIMIT 5;
```

**过滤要单独一条语句**，不要和别的谓词并列——这一点非常容易踩：

```sql
-- ✅ 只在 match 上过滤
SELECT id FROM local.db.papers WHERE match(body, 'parquet statistics');

-- ❌ 不会走索引，运行期报 "text_score needs a text index ... none was found"
SELECT id FROM local.db.papers WHERE match(body, 'parquet statistics') AND year = 2020;
```

原因见 5.4。要在过滤结果上再筛，先取出再在应用层筛，或者用 `CALL text_search(...)`。

---

## 3. 建表与数据前提

### 3.1 向量列

用 `array<float>` 或 `array<double>`。索引内部一律按 `float` 处理，`double` 会损失精度——对 128 维以上的近似检索几乎没有影响。

建索引时声明的 `dimension` 必须等于数组实际长度。长度不符是最常见的失败原因：构建在写出索引文件时失败，`index_build_status.error` 会指出问题。

值为 `NULL` 的行不被索引、不计入条目数，查询里直接不是候选（见 9.1）。

### 3.2 文本列

必须是 `string`。`NULL` 文档被当作"没有词的空文档"计入语料统计，建索引与查询两侧一致。

### 3.3 分区

只有 **identity 分区**（值本身出现在数据文件路径里）的表能被自动改写识别。`bucket`、`truncate`、`years/months/days/hours` 会让改写在规划阶段让开，退化成全表扫描——**结果仍然正确，只是慢**。这条只影响自动改写。

### 3.4 索引不自动维护

索引绑定建索引那一刻的表快照，之后的写入都不更新索引，需要 `refresh_*_index`（4.5 / 5.5）。落后不刷新也不会查错：向量改写会答索引能答的部分、扫未覆盖的文件，合并后排序；文本是例外（5.4）。

---

## 4. 向量索引

### 4.1 结构与量化怎么选

| `structure` | 打分方式 | 什么时候用 | 代价 |
|---|---|---|---|
| `FLAT` | 精确，逐条比 | 小规模对拍、要求绝对精确 | 搜索量等同全扫，省不了时间 |
| `HNSW` | 近似，图 | 追求高召回、内存装得下 | 索引最大（10M/128 维约 2.2 GB） |
| `IVF` | 近似，倒排 + 质心 | 大表、要控制索引体积 | 召回要靠 `nprobe` 与查询期 over-sample 补 |

`quantizer => 'RABITQ'` 只对 `IVF` 有意义：每维压到 1–8 bit，体积降一个数量级，分数变成估计值，召回靠候选放宽补。

三种结构都参与自动改写。`FLAT` 也会走索引路径，只是候选就是全部条目、重排后仍是全量打分——把它当"可对照的精确实现"，不是加速手段。

### 4.2 建索引

```sql
CALL local.system.build_vector_index(
  table => 'db.docs',
  column => 'embedding',
  dimension => 768,
  metric => 'COSINE',           -- COSINE | DOT_PRODUCT | EUCLIDEAN，默认 COSINE
  structure => 'IVF',           -- FLAT | HNSW | IVF，默认 FLAT
  quantizer => 'RABITQ',        -- NONE | RABITQ，默认 NONE
  options => MAP('nlist', '64', 'nprobe', '32', 'rabitq-bits', '1'),
  name => 'ivf_1bit',
  max_entries_per_index_file => 1000000);
```

注意 `metric` 这里**只认枚举名**：`COSINE` / `DOT_PRODUCT` / `EUCLIDEAN`，写 `L2` 或 `cos` 会失败（与 `vec_distance` 的第三参数不同，见 4.3）。

DDL 写法，`WITH` 的键就是上面这些参数名：

```sql
ALTER TABLE local.db.docs CREATE INDEX emb_ivf
  USING vector (embedding)
  WITH (dimension = 768, metric = 'COSINE', structure = 'IVF',
        options = MAP('nlist', '64', 'nprobe', '32'));
```

DDL 必须给名字；`CALL` 在列上还没有索引时可以不命名。**`dimension` 是必填的**，`ALTER TABLE … CREATE INDEX i USING vector (embedding)` 不带 `WITH (dimension = …)` 会失败。

| 参数 | 默认 | 说明 |
|---|---|---|
| `metric` | `COSINE` | 只认枚举名 |
| `structure` | `FLAT` | 见 4.1 |
| `quantizer` | `NONE` | `RABITQ` 仅配合 `IVF` |
| `options` | 空 | 结构/量化器选项；最后应用，会覆盖同名具名参数 |
| `max_m` / `ef_construction` / `ef_search` | 16 / 200 / 50 | 仅 `HNSW` 时写入 |
| `options.nlist` | 0（按 `round(sqrt(entries))` 推导） | IVF 分区数 |
| `options.nprobe` | 8 | 每查询访问的分区数 |
| `options.rabitq-bits` | 1 | 1–8 |
| `max_entries_per_index_file` | 1,000,000 | 索引文件的条目预算，决定如何分组；不拆单个数据文件 |
| `name` | 无 | 同列多索引时必须给 |
| `mode` | 无 | 旧名，`EXACT`→`FLAT`、`HNSW`→`HNSW`；与 `structure` 同时给会报错 |

输出四列：`index_uuid`、`source_snapshot_id`、`index_files`、`entries`。

同列多索引时，查询用 `index => '…'` 指定，或在会话里设 `spark.iceberg.index.vector.name`。

### 4.3 查询：五种入口，先分清再用

这是最容易用错的地方。**同名的东西有两种**：`CALL system.vector_search(...)` 是过程，返回索引命中的地址；`FROM vector_search(...)` 是表函数，返回业务行。它们不是一回事。

| 入口 | 写法 | 返回什么 | 是否精确 top-k | 索引落后时 |
|---|---|---|---|---|
| **ORDER BY 改写** | `SELECT … ORDER BY vec_distance(col, q) LIMIT k` | 表的列 | **是**（候选放宽后按真实距离重排） | 残差扫描补全，结果完整 |
| 过程 | `CALL … vector_search(…)` | `data_file, pos, score, row` | 否，索引自己的分与序 | 只取索引快照，不补残差 |
| 表函数（业务行） | `SELECT * FROM vector_search(…)` | 表的列 + `_distance` | **是**（metric 与索引不一致时退扫描，仍是精确值） | 见下 |
| 表函数（地址） | `SELECT * FROM vector_index_search(…)` | `data_file, pos, score, row` | 否 | 用索引**最新**快照，不按源表对齐 |
| 表函数（批量地址） | `SELECT * FROM vector_index_search_batch(…)` | 同上 + `query_id` | 否 | 同上 |

**要结果正确就用改写写法或 `FROM vector_search(...)`**；要"拿到索引自己的排序"或"批量取行引用"才用过程和地址类表函数。

三个表函数的参数（全部必须是常量，不能是列或子查询）：

```sql
SELECT * FROM vector_index_search('local.db.docs', 'embedding', array(0.95d, 0.05d), 3);
SELECT * FROM vector_index_search_batch('local.db.docs', 'embedding', array('0.95,0.05', '0.1,0.9'), 2);
```

注意批查的 queries 是**字符串数组**，每个字符串是逗号分隔的数字，不是二维数组。

```sql
-- 命名参数形式；位置形式是 (table, query_vector, k, vector_column)
SELECT id, title, _distance FROM vector_search(
  table => 'local.db.docs',
  query_vector => array(0.95d, 0.05d),
  vector_column => 'embedding',
  k => 3);
```

`vector_search` 表函数的 `distance_type` 可以显式指定 `cosine` / `dot_product` / `euclidean`（也接受 `cos`、`l2` 等别名）；它与索引的 metric 不一致时，改写会退让并做全表扫描，但**答案仍是你要的那个 metric 的精确值**。

#### 改写发生的条件（全部满足）

1. 列上有向量索引；同列多个时必须用 `spark.iceberg.index.vector.name` 指定，否则不动手；
2. 表只有 identity 分区；
3. 形态是"单表 + 唯一一个升序距离排序 + 可折叠的正数 `LIMIT`"，中间只有投影、子查询别名、以及向量列上的 `IS NOT NULL`；
4. 显式 `metric`（如果写了第三参数）与索引的 metric 一致，否则只 WARN 并退扫描。

反过来，`DESC`、多个排序键、`OFFSET`、join、聚合、窗口、任何非 `IS NOT NULL` 的过滤（包括 `WHERE year = 2020`）、非 identity 分区、`LIMIT` 不是字面量——都会让规则退让，退回全表扫描。

一个容易忽略的细节：改写后排序键会被绑定成**索引的 metric**。所以不带第三参数的 `ORDER BY vec_distance(col, q)` 是按索引的 metric 计算的，与"没有索引时全扫描默认 COSINE"可能不是同一个量。

### 4.4 近似结构要调 over-sample

```sql
SET `spark.iceberg.index.vector.over-sample`=10;   -- 默认 1，即不放宽
```

配置名里有点和短横，`SET` 必须用反引号包起来，否则解析器报语法错误。

经验值：`IVF·3bit` 约 10 倍、`IVF·1bit` 约 50–100 倍才能把真实 top-10 包进候选集；`HNSW` 一般 1–3 倍。放宽不是免费的，倍数直接乘在回表行数上。

这个参数只存在于 conf 里，没有对应的 `CALL` 参数；表函数也只能通过它影响。

`EXPLAIN` 会标出来：计划里的 `VectorSearchSource` 带 `result=exact` 或 `result=approximate`。只有 `FLAT` 且无量化才是 `exact`；看到 `approximate` 又没设 `over-sample`，就是明确接受了"候选集里可能没有真正的最近邻"。

### 4.5 刷新与删除

```sql
CALL local.system.refresh_vector_index(table => 'db.docs', column => 'embedding', index => 'ivf_1bit');
```

只重建受影响的索引文件。输出与构建一致；如果当前快照已被覆盖（没有新文件可补），返回 `index_files = 0, entries = 0`。

```sql
CALL local.system.drop_vector_index(table => 'db.docs', column => 'embedding', index => 'ivf_1bit');
CALL local.system.drop_vector_index(table => 'db.docs', column => 'embedding', index => 'ivf_1bit', purge => false);
```

`purge` 默认 `true`：注销注册并**立即删除**索引文件，不可恢复。`purge => false` 只注销、留文件——但**本分支没有任何入口把索引重新注册回来**，所以留文件的实际用途只是"确认没问题后手工删"。

注意 `ALTER TABLE … DROP INDEX <name>` **没有 `purge` 参数**，它落到默认值 `true`，即直接删文件。

---

## 5. 全文索引

### 5.1 能力边界（先看这条再决定用不用）

索引按"连续字母或数字"切词，以空白和标点作边界。这意味着：

- **中文基本不可用**。汉字是 `Letter` 但没有空格，一句中文会成为一个词。查"检索"命不中；要匹配得输入整句。`min_token_length` 默认 2，单个汉字查询直接是空查询。
- **没有 Unicode 归一化**。`café` 的 NFC 与 NFD 互相搜不到；全角 `ＡＢＣ` 搜不到 `abc`；土耳其大写 `İ` 会切成 `stanbul`。
- **没有词干化**（`tables` 与 `table` 是两个词），**没有短语查询**（payload 不存位置）。

要处理中文，当前唯一可行的办法是在写入前自己分词、用空格连接（例如离线用 jieba 分好词写进列），索引就能按词切。这不是内置能力。

### 5.2 建索引

```sql
CALL local.system.build_text_index(
  table => 'db.papers',
  column => 'body',
  k1 => 1.2,                     -- 词频饱和，默认 1.2
  length_norm => 0.75,           -- 长度归一化 b，默认 0.75
  min_token_length => 2,         -- 默认 2
  lowercase => true,             -- 默认 true
  stopwords => array('the', 'a'),-- 可选
  max_entries_per_index_file => 1000000);
```

DDL 用 `USING fts`，**不能带索引名**：

```sql
ALTER TABLE local.db.papers CREATE INDEX USING fts (body);
```

写成 `CREATE INDEX body_fts USING fts (body)` 会报 "a text index cannot be named"。这条限制会牵连后续的按名寻址（5.5）。

停用词请用小写：分词后的 token 是小写的，停用词表不做归一化，大写写的停用词不会生效。

**同列已有任何类型的索引时会被拒**（不只是文本索引）。

### 5.3 查询

```sql
-- 相关度排序（改写形态）
SELECT id, body FROM local.db.papers
ORDER BY text_score(body, 'table format') DESC LIMIT 10;

-- 关键词过滤（改写形态，条件必须是 match 本身）
SELECT id FROM local.db.papers WHERE match(body, 'parquet statistics');

-- 过程：返回命中地址，不参与改写
CALL local.system.text_search(table => 'db.papers', column => 'body', query => 'table format', k => 10);

-- 表函数：返回业务行 + _score
SELECT id, body, _score FROM search(
  table => 'local.db.papers', query => 'table format', search_columns => array('body'), limit => 10);

-- 表函数：返回命中地址
SELECT * FROM text_index_search('local.db.papers', 'body', 'table format', 10);
```

函数签名：`text_score(列, 查询)`、`match(列, 查询)`，都**恰好两个参数**，不支持第三个参数（AND/OR 之类的选项还没有）。多词查询是 **OR**：任一命中即算命中。

`NULL` 输入返回 `NULL`（不是 `false`）。`text_score` **不能在没有文本索引时计算**——BM25 需要的语料统计在索引里，不在行里。

### 5.4 三个必须知道的坑

**5.4.1 零命中会报错，而不是返回空结果。** 改写内部拿到 0 个命中时会放弃绑定，计划里留下未绑定的 `text_score`，执行时报 `text_score needs a text index on the column; none was found, build one with build_text_index`。索引**落后**（建完又写入）也会走到同一条错误路径，错误信息完全误导——索引其实存在，只是没覆盖当前快照。遇到这条报错，先 `refresh_text_index` 再重试。

**5.4.2 `match(...)` 不能与其他谓词并列。** 规则只匹配"整个过滤条件就是 `match(...)`"的 `Filter`。`WHERE match(body, 'x') AND year = 2020` 的根条件是 `AND`，不会被改写，运行期报上面那条错。这与英文文档的描述不符，以代码为准。

**5.4.3 排序形态不检查中间算子。** `ORDER BY text_score(...) DESC LIMIT k` 的改写只找子树里第一个 Iceberg scan，不校验 join 或额外过滤。所以

```sql
SELECT d.id FROM docs d JOIN meta m ON d.kind = m.kind
ORDER BY text_score(d.body, 'fox') DESC LIMIT 10;
```

会先取全局 top-10 再 join，可能与全扫描的"先 join 再排 top-10"结果不同。**要 join 就别用排序改写**，先用 `text_search` 取出候选再 join。

### 5.5 刷新与删除（当前是缺口）

```sql
CALL local.system.refresh_text_index(table => 'db.papers', column => 'body');
```

按列刷新，没有 `index` 参数（因为文本索引没有名字）。

删除目前**没有任何 SQL 入口**：

- `CALL system.drop_vector_index(...)` 按类型解析，只认 `vector`；
- `ALTER TABLE … DROP INDEX` 与 `REFRESH INDEX` 同样按类型分派，文本索引报 `cannot be dropped: unsupported type text`；
- DDL 按名字寻址，而文本索引没有名字，即使类型支持也无名可写。

更麻烦的是**建索引失败也不回滚**：`build_text_index` 没有 try/catch，`createIndex` 先注册、后续构建失败后注册留在 registry 里且没有任何发布快照。结果是 `list_indexes` 会看到一行 `index_type = text`、`status = NULL`、`index_files = 0`，而同列重试会被 "An index already exists for column …" 拒绝。

所以在当前版本上：**建文本索引前请确认这张表可以重建**。真需要清理时只能走 Java API（`SidecarIndexCatalog.dropIndex(tableLocation, indexUuid, true)`）或手工删 `indexes/<uuid>/` 并修 registry。这是已知缺口。

---

## 6. 查看与治理

四个 `CALL` 回答四个不同的问题。它们都用调用里写明的 catalog 定位表，所以 `CALL local.system.check_indexes(table => 'db.t')` 在会话 catalog 是 `spark_catalog` 时也照样查 `local.db.t`。

`list_indexes` 有一个 DDL 等价物，返回同样的列：

```sql
SHOW INDEXES FROM local.db.docs;   -- 只有 FROM，没有 IN
```

| 想知道 | 命令 | 关键列 |
|---|---|---|
| 这张表有哪些索引、参数是什么、建到哪个快照 | `CALL … list_indexes(table => 'db.t')` / `SHOW INDEXES FROM …` | `index_uuid, column, index_type, source_snapshot_id, status, index_files, entries, definition, index_name` |
| 索引还能不能被这个版本读、落后多少 | `CALL … check_indexes(table => 'db.t')` | `readable, writer, writer_version, reader_version, stale, suggestion` |
| 构建到哪一步、上次为什么失败 | `CALL … index_build_status(table => 'db.t')` | `state, started_at, updated_at, age_ms, stale, files_built, files_total, files_failed, rows, error` |
| 集群缓存装了什么、命中率如何 | `CALL … index_cache_status()` | `executor_id, host, epoch, mode, entries, bytes, hits, misses, loads, evictions, coverage, working_set_bytes, reported_at, age_ms, stale` |

`list_indexes` 的 `status` 只有 `PUBLISHED` 或 `NULL`（没有快照时），**不会显示构建进度**——那要看 `index_build_status`。`definition` 是 JSON 字符串，取 `metric` 之类的字段要 `get_json_object`。

`index_build_status.state` 五种：`RUNNING`、`SUCCEEDED`、`FAILED`、`ABANDONED`（报告停止更新超过 5 分钟，通常是 driver 被杀）、`UNKNOWN`（没有报告，比如更早版本建的）。构建期间约每 30 秒一次心跳，可以反复 `CALL` 观察。

失败报告的位置：向量索引失败会回滚并留下报告在 `<table>/indexes/build-status/<column-field-id>/`；同列下次构建成功时清掉。文本索引失败既不留可读报告也不回滚（5.5）。

### 清理

```sql
CALL local.system.cleanup_index_files(table => 'db.docs');
```

输出：`index_uuid, expired_snapshots, deleted_files, deleted_bytes, abandoned_attempts, in_flight_builds`。

**`in_flight_builds > 0` 表示它检测到有构建可能正在写，于是跳过了整个孤儿清理**。这时唯一正确的做法是等构建结束后再跑一次。

索引文件在 `<table>/indexes/` 下，生命周期独立于表快照：`expire_snapshots`、`rewrite_data_files`、`rewrite_manifests` 不碰它；`remove_orphan_files` 会跳过整个 `indexes/` 目录（3.4 与 3.5 客户端都跳过）；清理只认 `cleanup_index_files`。存储侧规则（S3 lifecycle）需要显式排除这个目录。

---

## 7. 缓存与性能

缓存把索引文件常驻在 executor 堆里，索引侧从"每次查询重读几 GB"降到几十毫秒。默认**关闭**。

| 配置 | 默认 | 说明 |
|---|---|---|
| `spark.iceberg.index.cache.enabled` | `false` | 总开关 |
| `spark.iceberg.index.cache.bytes` | 自动（按工作集推导，下限 512MB） | 每 executor 预算 |
| `spark.iceberg.index.cache.heap-fraction` | `0.25` | 自动预算的堆占比上限 |
| `spark.iceberg.index.cache.mode` | `auto` | `PIN_ALL` / `PIN_HOT` / `STREAM` |
| `spark.iceberg.index.cache.idle-ttl` | `30m` | 条目空闲淘汰 |
| `spark.iceberg.index.schedule.locality` | `true` | 把 task 派到已持有该索引文件的 executor |

自动预算按**整个工作集**（所有索引文件 prepare 后的常驻内存）× 1.2 估算，再被 `堆大小 × heap-fraction` 截住。不是"每个 executor 分 1/N"——任何一个 task 都可能被派到任何 executor。

手工驻留：

```sql
CALL local.system.load_index(table => 'db.docs', index => '<uuid>', retain_for => '1h');
CALL local.system.offload_index(table => 'db.docs', index => '<uuid>');
```

`retain_for` 接受 `0`（不保护）、`forever`、`<n>smhd`（如 `30m`、`1h`），不给则保持当前保护期。输出是每文件一行的放置结果。

当前版本的行为（不是建议）：

- executor 被杀后，driver 只有在**收到下一次上报**时才把它标记为丢失；在那之前它仍在账上，但已超过 10 秒没上报的节点不再是放置目标。
- 调用方等待放置完成的超时是 60 秒；超时后调用方返回，计划在 driver 侧仍可能保持挂起（已知缺口）。
- 内存评估依赖上报的运行时布局。**JVM 不是 17 时（比如 21），内存模型会报容量未知，缓存与驻留退化成"每查询重读"**。当前版本的镜像请用 JDK 17。

---

## 8. 配置总表

### 8.1 查询期（会话级，`SET` 时用反引号）

| 键 | 默认 | 作用 |
|---|---|---|
| `spark.iceberg.index.vector.over-sample` | `1` | 候选放宽倍数 |
| `spark.iceberg.index.vector.name` | 不设 | 同列多索引时选哪个 |
| `spark.iceberg.index.cache.enabled` | `false` | 缓存开关 |
| `spark.iceberg.index.cache.bytes` | 自动 | 每 executor 预算 |
| `spark.iceberg.index.cache.heap-fraction` | `0.25` | 自动预算的堆占比 |
| `spark.iceberg.index.cache.mode` | `auto` | 缓存档位 |
| `spark.iceberg.index.cache.idle-ttl` | `30m` | 条目空闲淘汰 |
| `spark.iceberg.index.schedule.locality` | `true` | 按持有情况派 task |

索引名与 over-sample 的优先级：`CALL` 的 `index => '…'` 最高，其次是 session `SET`，再次是 `spark-submit --conf`。

### 8.2 executor 侧的调优键（有陷阱）

| 键（前缀 `spark.iceberg.index.cache.`） | 默认 | 是否真的生效 |
|---|---|---|
| `control.poll-interval` | `1s` | 生效（executor 轮询周期） |
| `control.max-commands` | `64` | 生效 |
| `load.max-concurrent` | `1` | 生效（加载线程池大小） |
| `load.timeout` | `600s` | 生效 |
| `load.max-attempts` | `3` | 生效 |
| `control.stale-after` | `10s` | **解析后没有读者，设了没用** |

**陷阱**：executor 侧只认 Spark 注入的 `spark.plugins.internal.conf.<IndexCachePlugin 的类名>.<key>`，只有 driver 侧才回退到裸键 `spark.iceberg.index.cache.*`。直接用裸键 `--conf` 传时，executor 收到的仍是默认值。要改 executor 行为，必须用注入形式。

### 8.3 建索引时写死、事后不可改

`metric`、`structure`、`quantizer`、`max-m`、`ef-construction`、`ef-search`、`nlist`、`nprobe`、`rabitq-bits`、`max-entries-per-index-file`，以及文本侧的 `k1`、`b`（入参名 `length_norm`）、`min-token-length`、`lowercase`、`stopwords`。

其中 `nprobe` 与 `ef-search` 是**查询时读取但值来自定义**，所以想调整它们只能重建索引。

---

## 9. 已知限制

**9.1 空向量行不进结果集。** 建索引前就存在的 `NULL` 向量行不在索引里，查询的残差分支也会过滤掉。这是产品定义（没有向量的行没有"距离"），但要注意它与"未建索引时 Spark 用 `ASC` 默认 `NULLS FIRST` 把 null 排最前"不一致——同一个查询，加不加索引，行集可能不同。

**9.2 单索引文件 2 GB。** 读 payload 走 `byte[]`，超限在查询时报错；规避方式是调小 `max_entries_per_index_file`。

**9.3 非 identity 分区不改写。** 见 3.3。

**9.4 索引不自动维护。** 见 3.4。

**9.5 文本索引无法删除、无法按名刷新，且建索引失败不回滚。** 见 5.5。

**9.6 中文不可用。** 见 5.1。

**9.7 过程与地址类表函数不补候选。** `vector_search` 拿到 top-k 后如果有行已被删除，不回头补第 k+1 名，那些位置以 `row = NULL` 返回；改写路径会补。要严格 k 条用改写写法。

**9.8 地址类表函数用索引最新快照**，不按源表快照对齐，删除后的行仍可能返回命中。

**9.9 大索引的跨节点放置。** 装不进单节点缓存的规模（如 100M HNSW），目前只能"装得下多少装多少"，没有分片放置与均衡调度。

**9.10 构建部分失败仍报成功。** 如果某些索引文件构建失败但整体任务没抛异常，状态会显示 `SUCCEEDED`。判断完整性看 `entries` 与 `files_built`，或跑 `check_indexes`。

---

## 10. 排错

| 现象 | 原因 | 处置 |
|---|---|---|
| `already exists` | 该列已有索引 | 传 `name => '…'`，或先 drop |
| 建索引 `FAILED` | 常见是 `dimension` 与数组长度不符 | 看 `index_build_status.error` |
| `ABANDONED` | driver 在构建中退出 | 重新构建，再 `cleanup_index_files` |
| `No vector index exists` | 列上没有向量索引 | `build_vector_index` |
| `does not match index dimension` | 查询向量长度与索引维度不符 | 对齐长度 |
| 查询没走索引（`EXPLAIN` 有 `TableScan`） | 形态/分区/metric 不满足，或没加载扩展 | 按 4.3 逐条排查 |
| 召回明显偏低 | 近似结构下 over-sample 太小 | 设到 10，1bit 需要 50–100 |
| 查询慢、`coverage` 低 | 缓存没开或预算不足 | 开 `cache.enabled` |
| `SET` 报 `INVALID_SET_SYNTAX` | 键名有 `.` 和 `-` | 用反引号包键名 |
| `text_score needs a text index … none was found`（但索引存在） | 索引落后，或 `match` 与别的谓词并列，或零命中 | 先 `refresh_text_index`；把 `match` 单独用（5.4） |
| 文本查询搜不到中文 | 分词不支持 | 见 5.1 |
| 文本建索引失败后无法重试 | 注册未回滚且无删除入口 | 见 5.5，需手工清理 |
| `cleanup_index_files` 报 `in_flight_builds > 0` | 有构建可能正在写 | 等构建结束后重跑 |
| `index_cache_status()` 返回 0 行 | 缓存没开 | 先开缓存 |
| 过程与改写结果不同 | 过程不做候选放宽也不做精确重排 | 要精确用改写或 `FROM vector_search(...)` |

---

## 11. 速查

### 过程（`local.system.` 省略）

| 过程 | 必填参数 | 可选参数 |
|---|---|---|
| `build_vector_index` | `table, column, dimension` | `metric, structure, quantizer, options, max_m, ef_construction, ef_search, name, max_entries_per_index_file, mode` |
| `build_text_index` | `table, column` | `k1, length_norm, min_token_length, lowercase, stopwords, max_entries_per_index_file` |
| `refresh_vector_index` | `table, column` | `index` |
| `refresh_text_index` | `table, column` | — |
| `drop_vector_index` | `table, column` | `index, purge` |
| `list_indexes` | `table` | — |
| `check_indexes` | `table` | — |
| `index_build_status` | `table` | — |
| `index_cache_status` | — | `refresh` |
| `vector_search`（过程） | `table, column, query, k` | `index` |
| `vector_search_batch` | `table, column, k` | `queries, query_table, query_id_column, vector_column, index` |
| `text_search` | `table, column, query, k` | — |
| `text_search_batch` | `table, column, queries, k` | — |
| `load_index` | `table, index` | `retain_for` |
| `offload_index` | `table, index` | — |
| `cleanup_index_files` | `table` | — |

### 表函数（参数必须是常量）

| 函数 | 参数 | 返回 |
|---|---|---|
| `vector_search` | `table, query_vector, k, vector_column`（位置序） | 业务行 + `_distance` |
| `vector_index_search` | `table, column, query(array\<double\>), k` | `data_file, pos, score, row` |
| `vector_index_search_batch` | `table, column, queries(array\<string\>，逗号分隔), k` | 同上 + `query_id` |
| `search` | `table, query, search_columns(array\<string\>，恰好 1 列), k` | 业务行 + `_score` |
| `text_index_search` | `table, column, query(string), k` | `data_file, pos, score, row` |

### SQL 表达式与 DDL

`vec_distance(vector, query [, metric])` 越小越近，metric 接受 `cosine|cos`、`dot|dot_product|inner_product|ip`、`euclidean|l2`。
`text_score(column, query)` 越大越相关。`match(column, query)` 布尔。

| 语句 | 支持的类型 | 说明 |
|---|---|---|
| `SHOW INDEXES FROM <table>` | 全部 | 等价 `list_indexes`，只有 `FROM` |
| `ALTER TABLE <t> CREATE INDEX <name> USING vector (<col>) WITH (…)` | vector | `WITH` 键＝过程形参名；`dimension` 必填 |
| `ALTER TABLE <t> CREATE INDEX USING fts (<col>)` | text | 不能带名字 |
| `ALTER TABLE <t> REFRESH INDEX <name>` | 仅 vector | 文本报 unsupported type |
| `ALTER TABLE <t> DROP INDEX <name>` | 仅 vector | 无 `purge` 参数，默认删文件 |

---

延伸阅读：`vector-index-guide.md`（向量参数曲线、改写规则细节、性能实测）、`docs/vector-index-deployment-guide.md`（上线、镜像、发布流程）。
