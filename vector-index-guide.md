# 向量索引操作手册

适用于本分支新增的向量检索能力：建表、写入、建索引、查询、查看状态、调优与维护。所有操作走 Spark SQL 的 `CALL` 过程（`<catalog>.system.*`）或一条普通 SQL 查询（自动改写）。英文参考实现细节在 `docs/docs/spark-procedures.md`，本手册是按"要做一件事该敲什么"组织的。

文中 `local` 指会话里的 Iceberg catalog 名，实际用 `spark_catalog`、`hive`、`rest` 等都可以。

范围：只覆盖向量索引。文本索引（`build_text_index` / `text_search` / `text_score` / `match`）是另一套过程，参考 `docs/docs/spark-procedures.md` 里的 text search 一节。

## 0. 前置条件

Spark 3.5（Scala 2.12）并且加载了 Iceberg 的扩展，否则 `vec_distance` 与自动改写都不存在：

```bash
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/tmp/warehouse
```

索引自身的类型注册由每次 `CALL` 内部完成，不需要额外配置；但**自动改写**要求上面那个 `spark.sql.extensions`。

## 1. 一分钟跑通

```sql
CREATE TABLE local.db.docs (id bigint, title string, embedding array<double>) USING iceberg;

INSERT INTO local.db.docs VALUES
  (1, 'iceberg tables', array(1.0, 0.0)),
  (2, 'vector search',  array(0.9, 0.1)),
  (3, 'parquet files',  array(0.0, 1.0));

CALL local.system.build_vector_index(
  table => 'db.docs', column => 'embedding', dimension => 2, metric => 'cosine');

SELECT id, title FROM local.db.docs
ORDER BY vec_distance(embedding, array(0.95d, 0.05d))
LIMIT 3;

CALL local.system.index_build_status(table => 'db.docs');
```

## 2. 建表与写数据

向量列是 `array<float>` 或 `array<double>`，索引内部一律按 `float` 处理，所以 `double` 列在索引里会损失精度（对 128 维的近似检索几乎无影响）。建索引时声明的 `dimension` 必须等于数组长度，长度不符的构建会在写出索引文件时失败——这是最常见的建索引失败原因。

```sql
CREATE TABLE local.db.docs (
  id bigint,
  title string,
  embedding array<double>,
  created_at timestamp
) USING iceberg;
```

索引不改变表结构，也不需要额外列；`id`、`title` 等普通列照常写入。写入路径没有限制（`INSERT`、`MERGE`、Spark 写入、其他引擎写入都行），**但索引不会随写入自动维护**：索引绑定的是建索引那一刻的表快照，新数据要等 `refresh_vector_index`（见第 5 节）。

关于分区：只有 **identity 分区**（值本身在路径里）的表能被自动改写识别。`bucket`、`truncate`、`years/months/days/hours` 这些变换分区会让改写在规划阶段直接让开，查询退化成全表扫描——结果仍然正确，只是慢。这一条只影响"自动改写"这条入口，显式的 `vector_search` 不受影响。

## 3. 建索引

```sql
CALL local.system.build_vector_index(
  table => 'db.docs',
  column => 'embedding',
  dimension => 768,
  metric => 'cosine',            -- cosine | euclidean | dot_product，默认 cosine
  structure => 'IVF',            -- FLAT | HNSW | IVF，默认 FLAT
  quantizer => 'RABITQ',         -- NONE | RABITQ，默认 NONE
  options => map('nlist', '0', 'nprobe', '32', 'rabitq-bits', '1'),
  name => 'arm_ivf_1bit',        -- 可选，同列多索引时用来区分
  max_entries_per_index_file => 1000000);
```

一次构建返回一行：`index_uuid`、`source_snapshot_id`、`index_files`、`entries`。

### 3.1 结构怎么选

| structure | 打分方式 | 什么时候用 | 代价 |
|---|---|---|---|
| `FLAT` | 精确，逐条比 | 只用 `vector_search` 做小规模验证/对拍 | 搜索量等同全扫，**自动改写在规划时直接跳过它**（注释里写明"改写只会更慢"） |
| `HNSW` | 近似，图 | 追求高召回、内存装得下 | 索引最大（10M/128 维约 2.2 GB），100M 规模装不进单节点缓存 |
| `IVF` | 近似，倒排+质心 | 大表、需要控制索引体积 | 需要调 `nprobe` 和查询期 over-sample，召回不是白给的 |

`quantizer => 'RABITQ'` 只对 `IVF` 有意义：把每个向量压成每个维度 1–8 bit 的量化码，体积降一个数量级，打分变成 popcount。代价是召回要靠**候选放宽**补回来。

### 3.2 参数与默认值

| 参数 | 默认 | 说明 |
|---|---|---|
| `metric` | `cosine` | `cosine`/`dot_product` 分数越大越近；`euclidean` 分数越小越近 |
| `structure` | `FLAT` | 见上表 |
| `quantizer` | `NONE` | `RABITQ` 仅配合 `IVF` |
| `max_m` / `ef_construction` / `ef_search` | 16 / 200 / 50 | 仅 `HNSW` 使用 |
| `options.nlist` | 0（自动） | IVF 分区数，0 表示按条目数推导 |
| `options.nprobe` | 8 | 每查询访问的分区数；实测 32 与 150 差 ≤0.004 但快 2–3 倍 |
| `options.rabitq-bits` | 1 | 1–8 |
| `max_entries_per_index_file` | 1,000,000 | 索引文件的分组预算：默认一个数据文件一个索引文件；调大可以把小文件合并、图更大召回更好，代价是并行度下降 |
| `name` | 无 | 同列多索引的名字；不传则要求该列还没有索引，否则报"already exists" |

同列多索引（对比不同参数）在探测里是常规用法：第二次构建传 `name`，查询时用 `index => '…'` 指定，或在会话里设 `spark.iceberg.index.vector.name`。

## 4. 查询：三条入口

| 入口 | 形态 | 扫描? | 精确重排 | 索引落后时 |
|---|---|---|---|---|
| 自动改写 | `SELECT … ORDER BY vec_distance(col, q) LIMIT k` | 只读命中的行；未覆盖文件做残差扫描 | 有：候选放宽后按真实距离重排 | 索引答能答的部分 + 扫未覆盖文件，结果完整 |
| 过程 | `CALL … vector_search(…)` | 不扫表 | 无：直接返回 kernel 的分数与顺序 | 只从索引快照取候选；已删除的行仍占一行但 `row` 为 NULL，也不补候选 |
| 表函数 | `SELECT … FROM vector_index_search(…)` | 同过程 | 无 | 同过程 |

这三条的差别是实测出来的，不是措辞上的：同一张 `IVF·RaBitQ 4bit` 的表上，`vector_search` / 表函数 给出的 top-1 可能与真实最近邻不同（kernel 没放宽候选、也没重排），而改写路径在 `over-sample=10` 时能给出与全表扫描一致的第一名。要"结果正确优先"就用改写写法，要"拿到 kernel 自己的排序/分数"或"批量取行引用"就用过程。

### 4.1 自动改写（推荐日常使用）

```sql
SELECT id, title FROM local.db.docs
ORDER BY vec_distance(embedding, array(0.95d, 0.05d))
LIMIT 10;
```

`vec_distance(vector, query)` 恒定"越小越近"（`cosine` 是 `1-cos`，`dot_product` 是 `-dot`，`euclidean` 是 L2），所以写法永远是升序。改写发生的条件：

1. 列上有向量索引（多个索引时用 `spark.iceberg.index.vector.name` 指定，否则规则不动手）；
2. 索引类型已在会话注册；
3. 表只有 identity 分区（bucket/truncate/时间 transform 不行，它们的列值不在数据文件路径里）；
4. 形状是"单表 + 单一升序距离 + 可折叠的正数 `LIMIT`"，从 `LIMIT` 到扫描之间只有投影、子查询别名、以及向量列上的 `IS NOT NULL`。

距离写在 `ORDER BY` 里，或者先 `SELECT ... AS d` 再 `ORDER BY d`，两种都改写。反过来，`Join`、`Aggregate`、`Window`、`OFFSET`、多排序列（`ORDER BY dist, id`）、以及任何其它过滤条件都会让规则退让——带普通 `WHERE` 谓词的查询会退回全表扫描，这是已知取舍（谓词下推常把 `isnotnull` 并进同一个 `Filter`，所以 `WHERE embedding IS NOT NULL AND id < 50` 也不命中）。索引覆盖率为 0 时仍然改写：候选分支空转，未覆盖部分由残差扫描答全表。用 `EXPLAIN` 验证是否改写：没有 `TableScan` 就说明命中了。

`FLAT`（精确索引）也参与改写。早先的规则因为"`FLAT` 每个条目都要打分、代价等同扫描"把它排除在外，现在这条成本判断已移除——能不能用索引只看正确性，不看代价。所以 `FLAT` 也会走到索引路径，只是省不了多少开销：候选就是全部条目，重排后仍然是全量打分。做"不走索引"的对照时，用改写认不出来的写法（如距离后加 `+ 0.0d`）。

近似结构（`HNSW`/`IVF`）需要放宽候选再精确重排，候选倍数靠会话配置：

```sql
SET `spark.iceberg.index.vector.over-sample`=10;   -- 默认 1，即不放宽
```

配置名里带点和短横，`SET` 必须用反引号把它包起来，否则解析器直接报语法错误（`SET key=value` 的 key 只接受标识符）。第一节里列的其它配置同理。

经验值：`IVF·3bit` 约 10 倍、`IVF·1bit` 约 50–100 倍才能把真实 top-10 包进候选集；`HNSW` 一般 1–3 倍。放宽不是免费的，倍数直接乘在回表行数上。

`EXPLAIN` 会把这个决定摊开：`VectorSearchSource` 带 `result=exact` 或 `result=approximate`。只有 `FLAT` 且无量化时才是 `exact`（候选枚举完整）；看到 `approximate` 又没设 `over-sample`，就是明确接受了"候选集里可能没有真正的最近邻"——分数和顺序仍然精确，因为排序节点会对着原始向量重算，但被粗筛漏掉的行不会回来。

默认值 1 意味着"每个结果只要一个候选"，对近似 kernel 就是不开候选放宽——实测 32 行的 `IVF·4bit` 表在 over=1 下第一名是 29、真实最近邻是 30，over=10 才对上。所以近似索引上不要把默认值当成够用。

### 4.2 过程（要行引用、或要批量）

```sql
CALL local.system.vector_search(
  table => 'db.docs', column => 'embedding',
  query => array(0.95d, 0.05d), k => 10, index => 'arm_ivf_1bit');

CALL local.system.vector_search_batch(
  table => 'db.docs', column => 'embedding',
  queries => array('0.95,0.05', '0.0,0.95'), k => 10);
```

输出 `data_file, pos, score, row`（批量多一列 `query_id`）。`row` 为 NULL 表示这条命中指向的行已经不在表里（建索引之后被删除，或所在文件已经不属���当前快照）。`row` 是整行的 JSON 字符串，用 `get_json_object(row, '$.id')` 取字段。`score` 的方向随 metric：`cosine`/`dot_product` 越大越近，`euclidean` 越小越近；行的顺序永远"最靠前的在前"。

批量入口的意义不只是省事：它对同一批查询只把每个索引文件加载一次、每个命中的数据文件只读一次，所以 N 条查询的代价接近一次单查询（实测 10M 上批量摊销后每查询 105–169 ms，单查询是 3.6–6.4 s）。

也可以用 `query_table => 'db.queries', query_id_column => 'qid', vector_column => 'qvec'` 直接从一张表取查询向量，省掉拼字符串。

注意 `query_id` 列输出的**是查询的行序**（0、1、2…），不是 `query_id_column` 的值：那一列目前只用来决定读取顺序。要把结果映射回自己的 id，请自己按顺序对齐，或者改用表函数再 join。

### 4.3 表函数（要 join / filter 时）

```sql
SELECT get_json_object(row, '$.id') AS id, score
FROM vector_index_search('local.db.docs', 'embedding', array(0.95d, 0.05d), 100)
WHERE score > 0.5
ORDER BY score DESC
LIMIT 5;
```

过程是独立语句，结果不能再过滤；表函数可以在 `FROM` 里当普通表用。代价是参数必须是常量（在规划期于 driver 上求值）。

## 5. 数据变化之后

索引是快照级的：`build_vector_index` 绑定当时的表快照，`refresh_vector_index` 把索引推进到当前快照，并且**只重建受影响的索引文件**，没被新快照触及的索引文件原样保留。

```sql
INSERT INTO local.db.docs VALUES (7, 'puffin blobs', array(0.92, 0.12));

CALL local.system.refresh_vector_index(table => 'db.docs', column => 'embedding');
```

刷新输出与构建一样（`index_uuid`、`source_snapshot_id`、`index_files`、`entries`）。

落后不刷新也不会查错：自动改写会用索引答能答的部分，再扫索引没覆盖的文件（残差扫描），两者合并后重新排序。慢，但结果完整。当索引落后的部分大到等于全表时，改写会直接让开、退回全表扫描。

被删除的行：查询走的是与建索引相同的 delete-aware 读取路径，建索引之后被删除的行不会返回。

## 6. 查看索引状态

四个 `CALL`，分别回答四个不同的问题：

这几个 `CALL` 都用**调用里写明的 catalog** 定位表，所以 `CALL local.system.check_indexes(table => 'db.t')` 在会话当前 catalog 是 `spark_catalog` 时也照样查 `local.db.t`。

| 想知道 | 命令 | 关键列 |
|---|---|---|
| 这张表有哪些索引、参数是什么、构建到哪个快照 | `CALL … list_indexes(table => 'db.t')` | `index_uuid, column, index_type, source_snapshot_id, status, index_files, entries, definition, index_name` |
| 索引还能不能被这个版本读、落后了多少 | `CALL … check_indexes(table => 'db.t')` | `readable, writer_version, reader_version, stale, suggestion` |
| 正在进行的构建到哪一步了、上次为什么失败 | `CALL … index_build_status(table => 'db.t')` | `state, started_at, updated_at, age_ms, stale, files_built, files_total, files_failed, rows, error` |
| 集群里索引缓存装了什么、命中率如何 | `CALL … index_cache_status()` | `executor_id, mode, entries, bytes, hits, misses, evictions, coverage, working_set_bytes, age_ms, stale` |

### 6.1 `index_build_status`：构建进度

`state` 有五种取值：`RUNNING`（正在跑）、`SUCCEEDED`、`FAILED`、`ABANDONED`（报告停止更新，通常是 driver 被杀）、`UNKNOWN`（这个索引没有报告，比如由更早的版本构建）。

构建期间每 30 秒有一次心跳，`files_built / files_total` 会往前走，可以反复 `CALL` 观察。`ABANDONED` 是读的时候按 `updated_at` 超过 5 分钟推出来的，不会回写。

报告存在表位置下的两个地方：

```
<table>/indexes/<index-uuid>/status/build-status-{0,1}.json      -- 进行中与成功的索引
<table>/indexes/build-status/<column-field-id>/build-status-{0,1}.json -- 失败并被回滚的尝试
```

失败时索引注册与索引目录会一起回滚，所以第二份是唯一还能读到"为什么失败"的地方；同一列下一次构建成功时会把它清掉。

### 6.2 `index_cache_status`：缓存命中

不加参数读的是各 executor 上次任务结束时上报的快照（几乎零成本，但可能是几分钟前的）；要当前值就 `index_cache_status(refresh => true)`，代价是一个小作业。`coverage` 是"本次查询的索引文件有多少至少被一个 executor 驻留"，`stale=true` 表示这份报告已经超过 10 分钟。

## 7. 缓存与性能配置

缓存把索引文件常驻在 executor 堆里，索引侧从"每次查询重读几 GB"降到几十毫秒（实测 10M：冷启动 16.9 s、热查询 0.7 s，索引腿本身 41–74 ms）。默认**关闭**。

| 配置 | 默认 | 说明 |
|---|---|---|
| `spark.iceberg.index.cache.enabled` | `false` | 总开关 |
| `spark.iceberg.index.cache.bytes` | 不设=自动 | 每 executor 预算；显式给值就用该值，低于工作集的 0.9 会 WARN |
| `spark.iceberg.index.cache.heap-fraction` | `0.25` | 自动预算的堆占比上限 |
| `spark.iceberg.index.cache.max-entry-bytes` | `256m` | 单个索引文件上限，超过的不进缓存 |
| `spark.iceberg.index.cache.mode` | `auto` | `auto`/`PIN_ALL`/`PIN_HOT`/`STREAM` |
| `spark.iceberg.index.cache.idle-ttl` | `30m` | 空闲多久后报告视为过期 |
| `spark.iceberg.index.schedule.locality` | `true` | 把 task 派到已持有该索引文件的 executor |
| `spark.iceberg.index.vector.over-sample` | `1` | 查询期候选放宽倍数（近似结构必调） |

自动预算的口径值得记住：它按**整个工作集**（所有索引文件 prepare 之后占的内存）× 1.2 算，再被 `堆大小 × heap-fraction` 截住。不是"每个 executor 分 1/N"——因为任何一个 task 都可能被派到任何 executor。10M（39 个索引文件、prepared 约 2.4 GB）在 4 × 16g 上因此能装下全部文件，coverage 实测 1.00；而这个策略在 100M HNSW（prepared 远超 4 GB 上限）上**不成立**，那时只会加载装得下的部分。

参考性能（同表精确召回，来自 benchmark 报告，注意口径）：10M Lance `IVF·1bit over=100` 4.4 s、相对全扫 4.3×；100M Lance 13.5 s、9.9×；100M Parquet 同一索引 93.3 s——差 6 倍的原因在回表：Parquet 按页随机读，Lance 能按位置只读命中的页。

## 8. 维护

```sql
-- 删索引；purge 默认 true，连索引文件一起删
CALL local.system.drop_vector_index(table => 'db.docs', column => 'embedding', purge => true);

-- 清掉过期索引快照与失败构建留下的文件；不要在构建/刷新进行中执行
CALL local.system.cleanup_index_files(table => 'db.docs');
```

索引文件在 `<table>/indexes/` 下（`indexmeta.v{N}` 注册表、每个索引一棵 `metadata/snapshots/data` 树，加上 `status/` 里的构建报告），生命周期独立于表快照：`expire_snapshots`、`rewrite_data_files`、`remove_orphan_files` 都不碰它，否则会把每个索引文件都当成孤儿。清理只认 `cleanup_index_files`；`drop_vector_index` 带 `purge => true` 时连构建报告一起删。

存储侧的规则（比如 S3 lifecycle policy）需要显式排除这个目录。

## 9. 已知限制

- **单索引文件 2 GB**：读 payload 走 `byte[]`，超过上限会在查询时报错，规避方式是调小 `max_entries_per_index_file`。建索引时目前没有预检护栏。
- **非 identity 分区不改写**：`bucket`/`truncate`/时间变换分区的表走自动改写会退化成扫描。
- **`FLAT` 索引不参与改写**：它逐条打分，改写只会更慢。
- **索引不自动维护**：新数据要手动 `refresh_vector_index`，没有后台自动刷新。
- **大索引的跨节点放置**：`100M HNSW` 这类装不进单节点缓存的规模，目前只能"装得下多少装多少"，没有分片放置与均衡调度。
- **文本索引的失败记录**：构建失败时索引目录回滚，它的失败报告不会留在表级路径上（向量索引会留）。
- **`refresh` 的进度口径**：`files_total` 是本次要重建的索引文件数，不是全量。
- **过程入口不补候选**：`vector_search` 拿到 top-k 后如果有些行已被删除，它不回头补第 k+1 名，那些位置会以 `row = NULL` 返回；自动改写路径会补（最多重试 8 次）。需要严格 k 条时用改写写法。

## 10. 排错

| 现象 | 原因 | 处置 |
|---|---|---|
| `already exists` | 该列已有未命名索引 | 传 `name => '…'` 建第二个，或先 `drop_vector_index` |
| 建索引失败，`index_build_status` 显示 `FAILED` | 常见是 `dimension` 与数组长度不符 | 看 `error` 列，改对 `dimension` 重建 |
| `ABANDONED` | driver 在构建中退出 | 重新构建；用 `cleanup_index_files` 清掉半成品 |
| `vector_search` 返回行数少于 `k` | 索引落后，或命中行已被删除 | `refresh_vector_index`，或改用自动改写（会补残差扫描） |
| `No vector index exists` | 列上没有向量索引 | `build_vector_index` |
| `does not match index dimension` | 查询向量长度与索引维度不符 | 对齐长度 |
| 查询没走索引（`EXPLAIN` 里有 `TableScan`） | 结构是 `FLAT`、分区不是 identity、索引没覆盖任何文件、类型未注册 | 按第 4.1 节逐条排查；确认会话加载了 `spark.sql.extensions` |
| 召回明显偏低 | 近似结构下 over-sample 太小 | `SET spark.iceberg.index.vector.over-sample=10`（1bit 需要 50–100） |
| 查询很慢、`index_cache_status` 显示 `coverage` 低 | 缓存没开或预算不足 | 开 `cache.enabled`，别把 `cache.bytes` 设得比工作集小 |
| `index_cache_status()` 返回 0 行 | 缓存没开，或配置用 `--conf` 传但版本较旧 | 用会话 `SET` 或新版镜像 |
| `SET spark.iceberg...` 报 `INVALID_SET_SYNTAX` | 配置名里有 `.` 和 `-` | 用反引号：``SET `spark.iceberg.index.vector.over-sample`=10`` |
| 过程/表函数的 top-1 与改写结果不同 | 过程不做候选放宽也不做精确重排 | 要精确第一名用改写写法；过程看的是 kernel 排序 |

## 11. 速查

过程一览（`<catalog>.system.` 前缀省略）：

| 过程 | 必填参数 | 可选参数 |
|---|---|---|
| `build_vector_index` | `table, column, dimension` | `metric, structure, quantizer, mode(旧), options, max_m, ef_construction, ef_search, name, max_entries_per_index_file` |
| `refresh_vector_index` | `table, column` | `index` |
| `drop_vector_index` | `table, column` | `index, purge` |
| `list_indexes` | `table` | — |
| `check_indexes` | `table` | — |
| `index_build_status` | `table` | — |
| `index_cache_status` | — | `refresh` |
| `vector_search` | `table, column, query, k` | `index` |
| `vector_search_batch` | `table, column, k` | `queries, query_table, query_id_column, vector_column, index` |
| `cleanup_index_files` | `table` | — |

表函数：`vector_index_search(table, column, query, k)`、`vector_index_search_batch(table, column, queries, k)`。

SQL 表达式：`vec_distance(vector, query)`（越小越近，可直接 `ORDER BY`）。

会话配置：`spark.iceberg.index.vector.over-sample`、`spark.iceberg.index.vector.name`、`spark.iceberg.index.cache.*`、`spark.iceberg.index.schedule.locality`。
