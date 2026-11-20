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
  table => 'db.docs', column => 'embedding', dimension => 2, metric => 'l2');

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

建索引时声明的 `dimension` 必须等于数组实际长度。长度不符是最常见的失败原因：那份数据文件写不出索引文件，该组（可能不止一个文件）的构建以失败告终。**向量索引的构建会把失败组排除在外、照常发布其余部分**（6.1），所以长度不符不再让整次构建白跑，但会留下覆盖缺口；`index_build_status.error` 指出原因与部分文件路径，`check_indexes` 的 `uncovered_data_files` 说明缺口有多大。修完数据再 `refresh_vector_index`，刷新期间坏文件还在的话它仍会失败。

值为 `NULL` 的行不被索引、不计入条目数，查询里直接不是候选（见 9.1）。

### 3.2 文本列

必须是 `string`。`NULL` 文档被当作"没有词的空文档"计入语料统计，建索引与查询两侧一致。

### 3.3 分区

只有 **identity 分区**（值本身出现在数据文件路径里）的表能被自动改写识别。`bucket`、`truncate`、`years/months/days/hours` 会让改写在规划阶段让开，退化成全表扫描——**结果仍然正确，只是慢**。这条只影响自动改写。

### 3.4 索引不自动维护

索引绑定建索引那一刻的表快照，之后的写入都不更新索引，需要 `refresh_*_index`（4.5 / 5.5）。落后不刷新也不会查错：向量改写会答索引能答的部分、扫未覆盖的文件，合并后排序；文本是例外（5.4）。

表没动、只是索引没覆盖全（部分构建、或旧的构建漏了文件）时，`refresh_vector_index` 也会补：它只重建没被覆盖的数据文件，然后**替换**同一份表快照上的那一版索引，不会为同一份表快照留下两版（4.5、6.1）。

---

## 4. 向量索引

### 4.1 结构与量化怎么选

| `structure` | 打分方式 | 什么时候用 | 代价 |
|---|---|---|---|
| `FLAT` | 精确，逐条比 | 小规模对拍、要求绝对精确 | 搜索量等同全扫，省不了时间 |
| `HNSW` | 近似，图 | 追求高召回、内存装得下 | 索引最大（10M/128 维约 2.2 GB） |
| `IVF` | 近似，倒排 + 质心 | 大表、要控制索引体积 | 召回靠 `nprobe`；**带量化时**还要靠查询期 refine-factor 补 |

`quantizer => 'RABITQ'` 只对 `IVF` 有意义：每维压到 1–8 bit，体积降一个数量级，分数变成估计值，召回靠候选放宽补。

三种结构都参与自动改写。`FLAT` 也会走索引路径，只是候选就是全部条目、重排后仍是全量打分——把它当"可对照的精确实现"，不是加速手段。

### 4.2 建索引

```sql
CALL local.system.build_vector_index(
  table => 'db.docs',
  column => 'embedding',
  dimension => 768,
  metric => 'l2',               -- l2 | cosine | dot，默认 l2
  structure => 'IVF',           -- FLAT | HNSW | IVF，默认 FLAT
  quantizer => 'RABITQ',        -- NONE | RABITQ，默认 NONE
  options => MAP('nlist', '64', 'rabitq-bits', '1'),
  name => 'ivf_1bit',
  max_entries_per_index_file => 1000000);
```

`metric` 接受 `l2`、`cosine`、`dot`，大小写不敏感。早期索引定义里写下的 `euclidean`、`dot_product` 仍能读出来（分别等价于 `l2`、`dot`），写入时统一用新名。

DDL 写法，`WITH` 的键就是上面这些参数名：

```sql
ALTER TABLE local.db.docs CREATE INDEX emb_ivf
  USING vector (embedding)
  WITH (dimension = 768, metric = 'l2', structure = 'IVF',
        options = MAP('nlist', '64'));
```

DDL 必须给名字；`CALL` 在列上还没有索引时可以不命名。**`dimension` 是必填的**，`ALTER TABLE … CREATE INDEX i USING vector (embedding)` 不带 `WITH (dimension = …)` 会失败。

| 参数 | 默认 | 说明 |
|---|---|---|
| `metric` | `l2` | `l2` / `cosine` / `dot` |
| `structure` | `FLAT` | 见 4.1 |
| `quantizer` | `NONE` | `RABITQ` 仅配合 `IVF` |
| `options` | 空 | 结构/量化器选项；最后应用，会覆盖同名具名参数 |
| `max_m` / `ef_construction` / `ef_search` | 16 / 200 / 50 | 仅 `HNSW` 时写入 |
| `options.nlist` | 0（按 `round(sqrt(entries))` 推导） | IVF 分区数 |
| `options['ivf-list-blocks']` | `'false'` | 设为 `'true'` 时，在一个 Puffin 文件内将 IVF 列表写成可独立读取的块。 |
| `options.rabitq-bits` | 1 | 1–8 |
| `options['ivf-train-samples-per-list']` | `'39'` | 训练每个 list 使用的样本数（样本总数 = `nlist × 该值`，不超过文件条目数）。调小能省训练时间，质心更粗。 |
| `options['ivf-train-max-iterations']` | `'25'` | Lloyd 迭代轮数上限，1–100。调小能省训练时间，质心更粗。 |
| `options['ivf-train-convergence-tolerance']` | `'0.0'` | 相邻两轮质心相对位移小于该值即提前停止，取值 0–0.1；`0` 表示默认的“质心逐位相等”判据。调大通常能早停，质量影响需用 recall 验证。 |
| `options['ivf-assign-kernel']` | `'exact'` | 归属（最近质心）内核：`exact` 是标量 double 路径，`simd` 用 float32 + Vector API 加速。**只对 `l2` 生效**，`cosine` / `dot` 仍走各自的原路径。`simd` 需要执行器 JVM 带 `--add-modules jdk.incubator.vector`；不带时自动回退 `exact` 并在日志里留一条 WARN，索引文件 `structure` 的 `assign-kernel-simd` 记录实际是否跑成。**`simd` 不保证跨 CPU 架构逐字节可复现**：它按 float32 分 lane 累加再水平归约，加法结合顺序随 AVX-512 / AVX2 / NEON 的 lane 数变化，近等距的行可能落到不同 list；要跨机型复现同一份 payload 就用 `exact`。 |
| `options['ivf-build-kernel']` | `'java'` | IVF 构建内核：`java` 是默认实现，`native` 用 Rust 内核（**只对 `l2` 生效**，payload 与 `java` 逐字节相同）。native 需要执行器 JVM 能加载 `libiceberg_ivf_native`（先在 `java.library.path` 找，再找 jar 内 `/org/apache/iceberg/index/vector/native/<os>-<arch>/` 下的副本），且库与本桥的 ABI 版本一致；库缺失、ABI 不符、非 L2 或运行异常都会自动回退 `java` 并告警，索引文件 `structure` 的 `build-kernel-native` 记录实际路径（1/0）。 |
| `max_entries_per_index_file` | 1,000,000 | 索引文件的条目预算，决定如何分组；不拆单个数据文件 |
| `name` | 无 | 同列多索引时必须给 |
| `mode` | 无 | 旧名，`EXACT`→`FLAT`、`HNSW`→`HNSW`；与 `structure` 同时给会报错 |

输出四列：`index_uuid`、`source_snapshot_id`、`index_files`、`entries`。

三个 `ivf-train-*` 参数与 `ivf-assign-kernel` 只影响构建（训练预算、收敛判据与归属内核），不改 payload 格式；不设置时同一批 entries 交给内核会写出与旧版本相同的字节。实际生效的样本数、轮数、收敛阈值与 SIMD 归属是否跑成记录在索引文件的内存结构里（`train-samples`、`train-iterations`、`train-tolerance-ppm`、`assign-kernel-simd`）；`list_indexes` 的输出没有 `structure` 列，这些值当前没有查询入口——构建日志里有实际轮数与样本数（每写一个 IVF 文件一条 `Encoded an IVF payload … over N rounds and M samples`），SIMD 是否跑成与收敛阈值只在 `structure` 里。要查某个索引用了什么参数，读 `definition` 里**请求的**值即可。带新键的定义旧版本读不了，滚动升级窗口内不要用新参数构建。注意还有一条跨版本口径：`#3153` 之后同一索引组的数据文件按路径排序后读取，所以**升级后用同样的定义重建同一张表，payload 可能不同于升级前那份**（内容等价、字节不必相同）；同一个版本内重跑才是逐字节可复现的。候选推荐值 `8` / `3` / `0.001` 已在 oss32（10,784,907 行、33 个索引文件）上实测：`build_ms` 506,546 对默认 740,691，`recall@10` 0.98 对默认 0.97（100 个答案的分辨率是 1 个百分点，因此只能表述为“该分辨率下无可见质量损失”）。换数据集仍要用 `build_ms` 与 `recall@10` 复核，掉点超过 0.5 个百分点的组合不要上生产；延迟对比需要独占集群窗口，共享负载下不可归因。

同列多索引时，查询用 `index => '…'` 指定，或在会话里设 `spark.iceberg.index.vector.name`。

### 4.2.1 用 native 内核构建（实验，默认关闭）

IVF 的构建默认由 JVM 完成（`ivf-build-kernel=java`）。把它设为 `native`，训练、归属与量化三段的算术改由 Rust 内核执行：Java 侧只负责规划、把每条 entry 的 payload 原样按块送过去、接收回传的索引 payload，**不再在 Java 侧解码整单元向量**（早期版本会先解成 `float[][]`，那条路径现在只留给桥接层自己的测试）。写出的 payload 与 Java 内核逐字节相同，`structure` 里只有 `build-kernel-native` 一项不同（1 表示 native 真的跑了，0 表示回退）。native 只实现 `l2`，`cosine` / `dot` 会忽略该键并回退 Java。

#### 部署：库从哪来

加载顺序是固定的：先试 `java.library.path` 上的 `libiceberg_ivf_native`，失败再找 runtime jar 里按平台放的副本（`/org/apache/iceberg/index/vector/native/<os>-<arch>/libiceberg_ivf_native.so`，`<os>` 取 `linux` / `macos` / `windows`，`<arch>` 取 `x86_64` / `aarch64`）。两条都不成、或库能加载但 ABI 版本对不上（桥当前读 ABI 2），该次构建打一条 WARN 说明原因并回退 Java——**产物照样正确，只是丢掉性能收益**。

实际部署有两种做法，选一种即可：

1. **打进 jar（推荐，也是我们镜像的做法）**：把 release 库放到 `index-vector/src/main/resources/org/apache/iceberg/index/vector/native/linux-x86_64/`，再构建 runtime jar；shaded jar 会把它带上，执行器不需要任何额外配置。
2. **放节点目录 + JVM 参数**：库放在所有节点都能读到的路径，再给 driver 与 executor 加参数。

```bash
# 仓库自带的 gradle 任务只构建 dev 档（opt-level=1，供测试用），性能对比必须自己建 release
cd index-vector/native && cargo build --release
# 产物：target/release/libiceberg_ivf_native.{so,dylib}（Windows 为 .dll）
```

```text
spark.driver.extraJavaOptions    -Djava.library.path=/opt/iceberg/native
spark.executor.extraJavaOptions  -Djava.library.path=/opt/iceberg/native
```

**平台与工具链**：linux x86_64 与 linux aarch64 是一等目标（x86_64 运行时按 AVX-512 → AVX2 → 标量分派，aarch64 走 NEON），macOS 只作开发用。x86_64 上的编码器用了 AVX-512 intrinsics，需要 Rust ≥ 1.89 才能编出那条路径；在没有 AVX-512 的机器上库照常工作，但编码退回自动向量化的标量路径，实测约 63.7 µs/向量（仍有约 1.7×，AVX-512 上约 22 µs）。

#### 启用方式

建索引时写进定义，建完不可改：

```sql
ALTER TABLE db.t ALTER COLUMN embedding CREATE INDEX idx
  USING vector (embedding)
  WITH (dimension = 2560, structure = 'IVF', quantizer = 'RABITQ',
        options => map('rabitq-bits', '4', 'ivf-build-kernel', 'native'));
```

`CALL system.build_vector_index(...)` 的 `options` 参数同样接受这个键。

#### 怎么确认跑成

构建日志里每条索引文件会有一条 `Built an IVF payload … with the native kernel`（native 路径没有 decode 段；Java 路径才有 `decode N ms`）；失败时有一条 WARN 说明回退原因，例如库缺失、`java.library.path` 上没有、jar 里缺该平台副本、ABI 版本不符。索引文件 `structure` 的 `build-kernel-native` 为 1 才算真的 native；`list_indexes` 看不到 structure，只能从 `definition` 读到请求的值。

#### 性能

单元级（x86 单机、40,000 行 × 2560 维、nlist=570、4-bit、4 线程、release 库、同一 JVM 背靠背）：整单元 209,616 ms → 32,082 ms（6.53×；train 6.65×、assign 7.33×、encode 约 10×），两边 payload 长度与 SHA-256 相同。

集群（10,784,907 行 × 2560 维、33 个 340k 行单元、12 executor × 4c × 24g + 12g、`task_cpus` 4）：

| 运行 | build_ms | 说明 |
|---|---|---|
| `97dbce8eec` 镜像 native（同镜像 A/B 的 native 臂） | 246,084 | 与下一行的 java 臂同一镜像、同一窗口 |
| `97dbce8eec` 镜像 java（同镜像 A/B 的 java 臂） | 1,097,833 | **4.46×** |
| `071617711d` 镜像 native（A 线合入后复测） | **236,687** | 单臂复测，比上一行 native 快 3.8%；跨 run，不与 java 臂算倍数 |

`071617711d` 那轮 33/33 索引文件 `build-kernel-native=1`、无回退，索引共 17,890,137,090 B；每单元中位 74.1 s，其中约 34.5 s 是 train/assign/encode/read，其余约 40 s 是每个单元约 542 MB 索引文件写对象存储与发布段——**这时候瓶颈已经不在算术**，再压内核收益有限；74.1 s 也说明 60 s/单元的目标在集群上尚未达到。

读这几个数时注意口径：单元级是单机共享负载（load≈4.7）下的背靠背测量，集群是同一窗口串行两臂；跨 run 的 payload 字节会因数据文件分组不同而不同（总字节差约 0.001%），同一次构建内 java 与 native 才保证逐字节一致。也不要用 dev 档的库去对比性能。

#### 边界

实验能力，**默认仍是 java**，逐表切 native。回退是静默的（日志留 WARN）：库缺失、ABI 不符、非 L2、运行时异常（含内存分配不出来）都会退回 Java，一致性由测试保证（golden 逐字节对比 + 真实数据 SHA-256），但回退会丢掉性能收益，并且一次运行期失败会让该单元在建 Java 路径之前先白跑一遍 native。带 `ivf-build-kernel` 的定义旧版本读不了，滚动升级窗口内不要用。

**内存**：native 构建会把整个单元的向量读进本进程的 native 堆，峰值约 `4 × entries × dimension` 字节，再加一份编码后的 list body 与一份 payload；这部分**不在 `-Xmx` 里**，容器内存上限要按它留余量，且同一 executor 上可能同时有多个单元。分配不出来时内核返回错误并由 Java 内核接手，不会 abort 进程。

### 4.3 查询：五种入口，先分清再用

这是最容易用错的地方。**同名的东西有两种**：`CALL system.vector_search(...)` 是过程，返回索引命中的地址；`FROM vector_search(...)` 是表函数，返回业务行。它们不是一回事。

| 入口 | 写法 | 返回什么 | 是否精确 top-k | 索引落后时 |
|---|---|---|---|---|
| **ORDER BY 改写** | `SELECT … ORDER BY vec_distance(col, q) LIMIT k` | 表的列 | **是**（候选放宽后按真实距离重排） | 残差扫描补全，结果完整 |
| 过程 | `CALL … vector_search(…)` | `data_file, pos, score, row` | 否（候选来自索引；`table`/`indexed` 返回活行并按精确距离给分，`index_hits`/`positions` 是索引自己的分与序） | 由 `coverage` 决定：默认 `table` 会扫未覆盖文件；`index_hits` 只取索引快照、不补残差 |
| 表函数（业务行） | `SELECT * FROM vector_search(…)` | 表的列 + `_distance` | **是**（metric 与索引不一致时退扫描，仍是精确值） | 见下 |
| 表函数（地址） | `SELECT * FROM vector_index_search(…)` | `data_file, pos, score, row` | 否 | 用索引**最新**快照，不补未覆盖文件；删除行保留 `row = NULL` |
| 表函数（批量地址） | `SELECT * FROM vector_index_search_batch(…)` | 同上 + `query_id` | 否 | 同上 |

**要结果正确就用改写写法或 `FROM vector_search(...)`**；要"拿到索引自己的排序"或"批量取行引用"才用过程和地址类表函数。

`coverage` 参数**只存在于过程**（`vector_search` / `vector_search_batch`），决定"命中之后要不要回表、要不要判活行"：

| `coverage` | 返回什么 | 回表 | 快照/删除判定 | 分数 |
|---|---|---|---|---|
| `table`（默认） | 表的活行（`row` 是业务 JSON） | 是 | 是 | 真实距离 |
| `indexed` | 只覆盖索引覆盖文件里的活行 | 是 | 是 | 真实距离 |
| `index_hits` | 索引命中（地址 + 分数 + `row`），`row` 可空 | 是（为了 `row`） | 否 | 索引估计 |
| `positions` | 只有地址与分数，`row` 恒为 null | **否** | 否 | 索引估计 |

`positions` 是给大批量准备的（见 4.8）：不读数据文件、不算 JSON，代价是分数是索引估计，且建索引后被删除的行仍会出现。要精确值或要行内容，就用 `table` / `FROM vector_search(...)`。

**地址类表函数没有 `coverage` 参数**，固定是 `index_hits` 语义：用索引**最新**快照、不扫未覆盖文件、被删除的行保留为 `row = NULL`、分数是索引估计、顺序是索引自己的顺序。`CALL` 里那种"要活行 / 按覆盖范围收紧 / 只要位置"的选择只属于过程。

三个表函数的参数（全部必须是常量，不能是列或子查询）：

```sql
SELECT * FROM vector_index_search('local.db.docs', 'embedding', array(0.95d, 0.05d), 3);
SELECT * FROM vector_index_search_batch('local.db.docs', 'embedding', array('0.95,0.05', '0.1,0.9'), 2);

-- 位置参数之后可以写命名选项，三个函数共用同一套
SELECT * FROM vector_index_search(
  'local.db.docs', 'embedding', array(0.95d, 0.05d), 3,
  nprobe => 32, refine_factor => 10);
```

注意批查的 queries 是**字符串数组**，每个字符串是逗号分隔的数字，不是二维数组。

```sql
-- 命名参数形式；位置形式是 (table, query_vector, k, vector_column)
SELECT id, title, _distance FROM vector_search(
  table => 'local.db.docs',
  query_vector => array(0.95d, 0.05d),
  vector_column => 'embedding',
  k => 3,
  -- 查询期参数，可选项：不写就跟随会话配置
  nprobe => 32,
  refine_factor => 10);
```

`nprobe` / `refine_factor`（以及多索引时的 `index`）是**逐语句**参数：优先级是
**语句里写的 > 会话配置（`SET`）**（索引定义目前不记录这三项的默认值）。写错值会直接报错，
不会回退到会话值。`nprobe` 以调用值为准；只写 session 时，session 值只对 IVF 索引生效，
非 IVF 索引上**显式**传 `nprobe` 是错误（不会静默忽略）。

`filter_mode` **不是**调用参数：`CALL` 和表函数都不接受它，只有会话配置
`spark.iceberg.index.vector.filter-mode`（见 4.6）。

> **升级提醒（本轮两处 SQL 表面变更）**：① `filter_mode => ...` 作为调用参数已移除，旧语句会直接报“无此参数”，改用会话配置 `SET spark.iceberg.index.vector.filter-mode=pre|post`；② 三个地址类表函数（`vector_index_search` / `vector_index_search_batch`）固定为 `index_hits` 语义——用索引最新快照、不补未覆盖文件、被删行保留 `row = NULL`、分数是索引估计，**不再**按整表覆盖回答。要活行或精确分数用 `CALL ... coverage => 'table'|'indexed'` 或 `FROM vector_search(...)`。

`vector_search` 表函数的 `distance_type` 可以显式指定 `l2` / `cosine` / `dot`（早期的 `euclidean`、`dot_product` 等写法仍接受）；**不写它就跟随索引的 metric**（索引是 `l2`，返回的就是 L2 距离）；写了但与索引不一致时，改写会退让并做全表扫描，但**答案仍是你要的那个 metric 的精确值**。

#### 改写发生的条件（全部满足）

1. 列上有向量索引；同列多个时必须用 `spark.iceberg.index.vector.name` 指定，否则不动手；
2. 表只有 identity 分区；
3. 形态是"单表 + 唯一一个升序距离排序 + 可折叠的正数 `LIMIT`"，中间只有投影、子查询别名，以及能被转换成索引侧表达式的过滤条件（向量列上的 `IS NOT NULL` 是其中最简单的一种）；
4. 显式 `metric`（如果写了第三参数）与索引的 metric 一致，否则只 WARN 并退扫描；
5. 索引存在、覆盖当前快照，就参与改写——**是否设置 `refine-factor` 不影响走不走索引**，只影响候选宽度（4.4）。

反过来，`DESC`、多个排序键、`OFFSET`、join、聚合、窗口、无法转换成索引侧表达式的过滤（子查询、UDF、跨表列）、非 identity 分区、`LIMIT` 不是字面量——都会让规则退让，退回全表扫描。

标量过滤（`WHERE year = 2020`、`WHERE price < 100`）**不再导致退让**：它会被带进检索，在 task 内按会话配置 `spark.iceberg.index.vector.filter-mode` 决定先于打分还是后于打分生效（4.6）。这也意味着**带过滤的查询结果是近似的**（与不带过滤时同为索引本身的近似程度），而不再是全表扫描的精确值；过滤后符合条件的行不足 k 时，返回找到的那些行而不是补齐。

一个容易忽略的细节：改写后排序键会被绑定成**索引的 metric**。所以不带第三参数的 `ORDER BY vec_distance(col, q)` 是按索引的 metric 计算的，与"没有索引时全扫描默认 l2"可能不是同一个量。

### 4.4 候选宽度：默认值从哪里来，什么时候要自己调

**索引总是被使用**（前提是它存在、覆盖当前快照、形态匹配）。`refine-factor` 不是"允许近似"的开关，它是**候选宽度**：向索引多要一些候选，交给上面的精确重排去挑。因此它决定的是召回与延迟的折中，不是走不走索引。

计划里标 `exact` 还是 `approximate` 由另一件事决定——**候选集是否完整**：只有 `FLAT` 且无量化才枚举全部条目（`exact`）；`IVF` 只访问 `nprobe` 个分区，`HNSW` 只走图的一部分，两者都是表的一个样本（`approximate`）。近似与否是索引形态的属性，不是参数设置的属性。

默认值为 `1`，基础候选预算为 `K`，执行计划显示 `refineFactor=1 [DEFAULT]`。`0` 与 `1` 等价，都表示不放大：两者算出的候选预算都是 `K`。业务行查询仍按原始向量精排，过滤或删除导致活行不足时可以扩窗。显式正整数按 `K × refine-factor` 计算候选预算。SQL 改写与批量 `CALL` 两条入口取的是同一个窗口，同一个倍数在两边取到同样多的候选（这也是同一问题在两个入口给出同一答案的前提）。使用量化索引时，扩大候选窗口可能提高召回率，也可能增加回表成本。可以为会话显式设置倍数。

这个参数此前叫 `over-sample`（配置键 `spark.iceberg.index.vector.over-sample`），现已统一改名为 `refine_factor`（配置键 `spark.iceberg.index.vector.refine-factor`）。旧配置键不再读取：会话或提交参数里还留着旧键时，查询会直接报错并指出新名字，而不是静默退回默认值。`CALL` 过程和表函数里的旧参数 `over_sample` 也不再被接受：两者都会直接报错并提示改叫 `refine_factor`（过程保留旧参数名只为了给出这条报错，它不会生效）。

```sql
SET `spark.iceberg.index.vector.refine-factor`=10;
```

配置名里有点和短横，`SET` 必须用反引号包起来，否则解析器报语法错误。

放宽不是免费的，倍数直接乘在回表行数上。无量化 `IVF` 与 `FLAT` 设不设这个倍数，答案都一样（只是多读一些行回来重排）。

也可以逐语句指定：`CALL … refine_factor => 10`、`SELECT … FROM vector_search(…, refine_factor => 10)` 或地址类表函数的同名命名选项；不写就跟随会话配置。

`EXPLAIN` 会标出来：计划里的 `VectorSearchSource` 带 `result=exact` 或 `result=approximate`。只有 `FLAT` 且无量化才是 `exact`；看到 `approximate` 说明候选集仍可能漏掉真正的最近邻（分数和顺序是精确的，被粗筛漏掉的行不会回来）。没走索引的计划里两者都没有。

### 4.5 刷新与删除

```sql
CALL local.system.refresh_vector_index(table => 'db.docs', column => 'embedding', index => 'ivf_1bit');
```

只重建受影响的索引文件：新数据文件会补上段，内容变过的数据文件（出现了删除文件，或被重写换名）会把它所属的组整体重建，没被新快照触及的索引文件原样保留。输出与构建一致；只有当前快照与父索引快照相比什么都没变时才不产新快照，返回 `index_files = 0, entries = 0`。

**表没动也会补。** 如果当前表快照已经有索引、但那版索引没有覆盖全部数据文件（上一次构建是部分的，见 6.1），刷新会以那一版为基线，只重建没被覆盖的数据文件，然后替换同一表快照上的那一版索引——不会出现"同一份表快照两版索引"让查询挑；`list_indexes` 的 `index_snapshot_id` 会前进一格，`source_snapshot_id` 不变。什么都没缺时仍然返回 `index_files = 0, entries = 0`。

```sql
CALL local.system.drop_vector_index(table => 'db.docs', column => 'embedding', index => 'ivf_1bit');
CALL local.system.drop_vector_index(table => 'db.docs', column => 'embedding', index => 'ivf_1bit', purge => false);
```

`purge` 默认 `true`：注销注册并**立即删除**索引文件，不可恢复。`purge => false` 只注销、留文件——但**本分支没有任何入口把索引重新注册回来**，所以留文件的实际用途只是"确认没问题后手工删"。

注意 `ALTER TABLE … DROP INDEX <name>` **没有 `purge` 参数**，它落到默认值 `true`，即直接删文件。

---

### 4.6 过滤落在检索的哪一侧：`filter-mode`

带标量过滤的向量查询有两种走法，由会话配置 `spark.iceberg.index.vector.filter-mode` 指定。它是**每个 task 内部的策略**：task 拿到的是自己那份索引分片和它覆盖的数据文件，取值决定它怎么处理这一份局部数据，而不是"全局先算一遍再检索"。

这个配置**只在 `ORDER BY vec_distance(...)` 改写、且查询的过滤被带进检索时**才有可观察差别；`CALL` 的两个过程与三个表函数都不接受 `filter_mode` 调用参数，也没有把标量过滤带进检索的入口，因此不受它影响。（本节下面描述的都是改写路径。）

| 值 | task 做什么 | 代价 |
|---|---|---|
| `pre`（默认） | 先读**本 task 覆盖的数据文件**里过滤需要的列，算出被放行的行位置，再让 kernel 只给这些行打分 | 一次过滤列的读（只读过滤要的列，且只读自己覆盖的文件），不读被拒行的向量列 |
| `post` | 先对整个索引分片打分，取回候选行（过滤列和向量在同一次读里取回），逐行判定；被拒的行不占窗口，窗口不满就在本 task 内扩大预算重搜 | 不读全量过滤列；代价是被拒行要靠扩样补，选择性越低扩样越多，扩到该分片探完仍可能填不满 |

两者的区别不在"过滤条件是否被满足"（都必须满足），而在**过滤相对于打分的位置**：`pre` 把过滤放在打分之前，所以检索空间就是被放行的行，窗口不受选择性影响；`post` 把过滤放在取行之后，所以打分范围是整个分片，窗口要靠拒绝后再扩样来补。正因为如此，`pre` 是默认，`post` 是为宽过滤列（大字符串 `LIKE` 之类）准备的特化选择。

**这两条链路现在都不额外起 job。** 早先 `pre` 要在 driver 上把全表过滤列扫一遍、把候选位置集合广播下去（实测多 2 个 job，且集合超过 200 万行时直接失败）；融合之后这段下推到 task 内，集合只存在于读到它的那个 task 里，规模是该 task 覆盖文件的放行行数，因此不再有全局上限，也不会因为"超限"而失败。

`pre` 之所以适合当默认，是因为它**不丢行**：它限制的是打分范围而不是裁剪已找到的结果，窗口从被放行的行里填，填不满就如实返回少的行（`numIndexUnderfilled` / `numCandidateRounds` 记着为什么）。

**指定的链路一定会被遵守，不会静默换成另一种方式。** Unknown 取值（包括已废弃的 `auto`）直接失败。早先 `auto` 按密度阈值在两者之间选，已经去掉：同一句 SQL 换一张表换一种分布可能走不同链路，而用户从 SQL 里看不出差别，这种不可预测比"少省一点"更贵。需要按代价自动选择时，那属于优化器的事（#3014）。

### 4.7 一次查询的时间花在哪：分阶段指标

融合查询（`SELECT … FROM vector_search(…)`）的每个 task 会上报四段墙钟——读索引 `indexReadTimeNs`、探测打分 `probeScoreTimeNs`、回表 `fetchBackTimeNs`、合并 `mergeTimeNs`——外加对账用的 `taskTimeNs` 和索引读取的字节 / 请求数。它们默认开启、无需配置，Spark UI 的 SQL 页和 `spark.eventLog` 里都能读到。

四段之和覆盖 `taskTimeNs` 的绝大部分（验收容差 <5%），但**不要把它当严格等式**：回表之后的过滤/去重计入 merge，块缓存的锁等待计入 probe-score，这两段因此都比字面含义宽。看到"慢"时可以先定位到段，再决定是读多了、算慢了还是回表贵了。

候选侧还有几个累计计数（每个索引文件的任务各加一份）：

| 指标 | 含义 | 容易误读的点 |
|---|---|---|
| `numCandidates` / `numCandidateRequests` | 本轮请求的候选窗口 / 最后一轮的窗口 | 被 `refine_factor` 放大，不等于 k |
| `numLiveCandidates` | 任务**保留**的活行（至多 k） | 不是"回表读到的行数"；打开精排后窗口是 8k、它仍然只是 k |
| `numFetchedRows` | 任务**回表读取**的行数（按轮次累计） | 这才是回表成本的度量，用来折算回表字节 / 行数 |
| `numIndexUnderfilled` | 最后一轮仍凑不满 k 的索引文件数 | 只描述索引分支，残差扫描可能已补齐 |
| `numCandidateRounds` | 每个索引文件跑了几轮扩探 | 轮次多说明窗口被删除/过滤吃掉了 |

整列表区回退（一次读整个 list 区而不是选读）由 `numIndexWholeListAreaReads` 计数；条件是选中的范围**密集**（未读部分已占 list 区的大部分，逐段读并不比整区读省）或**散扩**（范围分散成多段、待读字节已接近整个 list 区）。range=on 的查询会复用 executor 的 **list 块缓存**（`numIndexBlockCacheHits` / `numIndexBlockCacheMisses` / `numIndexBlockLoads` / `numIndexBlockEvictions`，都是 **per-search** 的计数）：块与整文件缓存共用 `spark.iceberg.index.cache.*` 的开关和同一份预算——块从整文件占用后的剩余预算里装入，整文件装入需要空间时也会回收块；块计入 `index_cache_status` 上报的常驻字节，空闲 TTL 淘汰与 `offload_index` 会清掉对应的块。回退前会先查共享缓存（全热的探测不读存储），但回退本身不回填。命中块时不再重复访问对象存储。完整的指标口径、event log 读法、与 Lance 侧指标的对应关系，以及已知边界（回表字节未覆盖等）见 `docs/index-query-metrics.md`。

### 4.8 批量查询：块、输出模式与内存

批量入口有两个：过程 `CALL … vector_search_batch(…)` 和表函数 `FROM vector_index_search_batch(…)`。**两者的输出形状与消费方式不同**，大批量时应先看清这一点。

```sql
-- 过程：返回结果集，行内带 query_id
CALL local.system.vector_search_batch(
  table => 'db.docs', column => 'embedding',
  queries => array('0.95,0.05', '0.1,0.9'), k => 10);

-- 也可以从表读查询；query_id 始终是查询的行序，query_id_column 只决定读表的顺序
CALL local.system.vector_search_batch(
  table => 'db.docs', column => 'embedding',
  query_table => 'db.queries', query_id_column => 'id',
  vector_column => 'vector', k => 10);

-- 表函数：在 Spark 里消费；按块转换后各成一个分区
SELECT * FROM vector_index_search_batch(
  'local.db.docs', 'embedding', array('0.95,0.05', '0.1,0.9'), 10);
```

**执行是分块的。** 每次取一块查询，共享一次索引文件加载、一次回表与（`table` 模式）一次未覆盖文件扫描；没凑够 k 的查询才进入下一轮扩探，最多 10 轮。块大小由三个预算里最紧的一个决定（块内广播 8 MiB、driver 行+JSON 8 MiB、shuffle 128 MiB），上限 128：

- 2560 维、k=10、带行 JSON 时约 **9–11 条/块**（每行约 `20 B/dim` 的 JSON 把 driver 预算吃掉了）；
- 用 `positions` 时不持有行与 JSON，块可到 **128**，且省掉了回表与行 JSON 两段工作（实测总作业数从 27 降到 7）。

**内存的口径。** 查询向量在 driver 上按 4 B/dim 常驻（10k×2560 ≈ 102 MB）；读查询表用逐行迭代，但仍持有全部向量，没有按块流式。输出侧：带行 JSON 时每行约 51 KB（2560 维），按行宽常量推算 10k×k=10 约 5 GB 量级；`positions` 下每行约 30 B，同规模约 3 MB 量级（两组总量都是推算，不是实测）。

**表函数和过程的差别在输出侧，但表函数不是流式**：表函数按块把结果转成 `UnsafeRow`、每块 `.coalesce(1)` 成一个分区、最后 `union`；driver 在 `union` 执行前仍持有整批的 `UnsafeRow`，峰值是 **O(整批)** 而不是 O(一块)，真实收益是每块的 `Row`+JSON 在转换后即释放、分区数等于块数。过程是 Spark 的结果集 API，driver 必须收全。10k 级批量优先用表函数，或把过程的 `coverage` 设为 `positions`。块是串行执行的，块内任何一条查询失败都会让整块失败（重试整个 stage），没有逐查询隔离。

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

BM25 v1 默认将每个词的倒排独立压缩，查询按字节范围读取所需词项。构建和刷新都会为索引快照发布全局 DF、文档数和总 token 数，查询直接读取所需统计。此前实验格式写出的索引文件需要重新构建。

仅重写 manifest 也会产生新的表快照。运行 `refresh_text_index` 后，新索引快照会复用相同的倒排分片和语料统计。

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

`search` 在执行阶段用全局 BM25 统计给各索引分片打分，只回表读取全局命中行。它返回最多 `limit` 条正分结果，要求索引完整覆盖查询快照。索引落后时先运行 `refresh_text_index`。

要在某个业务范围内选 top‑k，把条件作为 `filter` 参数传入。参数是一个 SQL 字符串，内容可以是引用顶层标量列的确定性布尔表达式。只有确认安全的条件才用于文件裁剪，Spark 对候选行计算完整条件。过滤列可以不出现在 `columns` 中。过滤影响入选资格，BM25 的语料统计仍来自完整快照。外层 `WHERE` 则过滤已选出的结果。

```sql
SELECT id, _score FROM search(
  table => 'local.db.papers',
  query => 'table format',
  search_columns => array('body'),
  columns => array('id'),
  filter => 'year >= 2020',
  limit => 10);
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
| 这张表有哪些索引、参数是什么、覆盖哪份数据、是哪一版 | `CALL … list_indexes(table => 'db.t')` / `SHOW INDEXES FROM …` | `index_uuid, column, index_type, source_snapshot_id, status, index_files, entries, definition, index_name, index_snapshot_id` |
| 索引还能不能被这个版本读、落后多少、漏了多少文件 | `CALL … check_indexes(table => 'db.t')` | `readable, writer, writer_version, reader_version, stale, uncovered_data_files, suggestion` |
| 构建到哪一步、上次为什么失败 | `CALL … index_build_status(table => 'db.t')` | `state, started_at, updated_at, age_ms, stale, files_built, files_total, files_failed, rows, error` |
| 集群缓存装了什么、命中率如何 | `CALL … index_cache_status()` | `executor_id, host, epoch, mode, entries, bytes, hits, misses, loads, evictions, coverage, working_set_bytes, reported_at, age_ms, stale` |

`source_snapshot_id` 是这份索引覆盖的**表**快照，`index_snapshot_id` 是这个索引**自己**那一版的快照 id：`REFRESH INDEX` 会在同一个 `index_uuid`、同一个名字下发布新的 index snapshot，所以只有 `index_snapshot_id` 能说清「测的是哪一版索引」（验收套件在跑之前就把它冻住，刷新过的索引会被拒绝，而不是被当成准备好的那份）。

`list_indexes` 的 `status` 只有 `PUBLISHED` 或 `NULL`（没有快照时），**不会显示构建进度**——那要看 `index_build_status`。`definition` 是 JSON 字符串，取 `metric` 之类的字段要 `get_json_object`。

`index_build_status.state` 六种：`RUNNING`、`SUCCEEDED`、`PARTIAL`、`FAILED`、`ABANDONED`（报告停止更新超过 5 分钟，通常是 driver 被杀）、`UNKNOWN`（没有报告，比如更早版本建的）。构建期间约每 30 秒一次心跳，可以反复 `CALL` 观察。`PARTIAL` 的含义见 6.1。

失败报告的位置：向量索引**一个数据文件都没索引成**时会回滚并留下报告在 `<table>/indexes/build-status/<column-field-id>/`；同列下次构建成功时清掉。文本索引失败既不留可读报告也不回滚（5.5）。

### 6.1 部分构建：建到一半也能用

一次向量构建的时间按小时计。如果其中某个数据文件建不出来（最常见的 `dimension` 与数组长度不符，也可能是那一组把 executor 撑死了），**不必等整张表重来**：

- 建不出来的那一组（数据文件）不进索引，其余照常发布。命令正常返回，`index_files` / `entries` 只算发布出去的部分。
- `index_build_status` 的 `state` 是 `PARTIAL`（不是 `SUCCEEDED`，也不是 `FAILED`）：索引可用，但没盖满。`files_failed` 是**失败的组数**（与 `files_total` 同单位，不是数据文件数），`error` 保留缺口说明与第一个失败原因，并列出若干未能索引的数据文件路径。
- **漏了多少数据文件看 `check_indexes.uncovered_data_files`**，不是看 `files_failed`。失败的粒度是组，一组可能覆盖多个数据文件，所以两个数字通常不相等（默认 `max_entries_per_index_file` 为 100 万条目，小文件会被大量并组）。
- 补缺口用 `refresh_vector_index`（4.5），但**它只在缺口的原因消失之后才管用**：坏文件还在那份表快照里时，刷新仍会去建它、仍然失败。所以顺序是"先修数据（修好或删掉、重写那批文件），再 `refresh_vector_index`"。两类原因要分开处理：`dimension` 不符靠修数据；`payload 超限`是相对于 `max_entries_per_index_file` 的大小，而 `refresh_vector_index` 继承同一个预算，所以重跑会再次超限——那类缺口要用更小的 `max_entries_per_index_file` 重建。
- `build_vector_index` 的返回行与完整构建同形（`index_uuid, source_snapshot_id, index_files, entries`），**脚本不能靠它判断完整性**，要判断请读 `index_build_status.state`。

三个边界必须知道：

1. **一个数据文件都没索引成时不发布**，整个构建失败并回滚注册——空索引比没有索引更难解释。这种时候 `index_build_status` 是 `FAILED`。
2. **部分构建不会替换已经发布的索引**。同一份表快照上如果已有一版（哪怕是更早的完整版），部分构建不覆盖它，因为那会拿掉查询的覆盖；只有一次完整构建才替换。（当前 `build_vector_index` 每次都新建索引，不会走到这条，属防御。）
3. **部分提交保住的是构建时间，不是坏数据**。会进"部分提交"的只有数据本身建不出来的失败（维度不符、payload 超限这类重试也不会变的）；对象存储抖动、task 被抢占、引擎 bug 仍会让构建失败并由 Spark 重试，这是有意的——否则一次瞬时故障会被永久记成数据缺口。而且没进索引的那些行，查询残差扫描时照样会读到，脏数据该报的错还是会报。

文本索引不参与部分构建：BM25 要求完整覆盖才能绑定，建得半截的倒排既用不上、全局统计也会偏，所以文本索引仍是"整次成功才发布"；失败时既不留报告也不回滚（5.5）。

### 清理

```sql
CALL local.system.cleanup_index_files(table => 'db.docs');
```

输出：`index_uuid, expired_snapshots, deleted_files, deleted_bytes, abandoned_attempts, in_flight_builds`。

**`in_flight_builds > 0` 表示它检测到有构建可能正在写，于是跳过了整个孤儿清理**。这时唯一正确的做法是等构建结束后再跑一次。

索引文件在 `<table>/indexes/` 下，生命周期独立于表快照：`expire_snapshots`、`rewrite_data_files`、`rewrite_manifests` 不碰它；`remove_orphan_files` 会跳过整个 `indexes/` 目录（3.4 与 3.5 客户端都跳过）；清理只认 `cleanup_index_files`。存储侧规则（S3 lifecycle）需要显式排除这个目录。

---

## 7. 缓存与性能

缓存把索引文件（以及 range 路径用到的 list 块）常驻在 executor 堆里，索引侧从"每次查询重读几 GB"降到几十毫秒。默认**关闭**。

| 配置 | 默认 | 说明 |
|---|---|---|
| `spark.iceberg.index.cache.enabled` | `false` | 总开关；关掉时 executor 不广告预算，`load_index` 以 `CACHE_DISABLED` 失败 |
| `spark.iceberg.index.cache.bytes` | 自动（按工作集推导，下限 512MB） | 每 executor 预算 |
| `spark.iceberg.index.cache.heap-fraction` | `0.25` | 自动预算的堆占比上限 |
| `spark.iceberg.index.cache.mode` | `auto` | `PIN_ALL` / `PIN_HOT` / `STREAM` |
| `spark.iceberg.index.cache.idle-ttl` | `30m` | 整文件条目与 list 块的空闲淘汰 |
| `spark.iceberg.index.schedule.locality` | `true` | 把 task 派到已持有该索引文件的 executor |

自动预算按**整个工作集**（所有索引文件 prepare 后的常驻内存）× 1.2 估算，再被 `堆大小 × heap-fraction` 截住。不是"每个 executor 分 1/N"——任何一个 task 都可能被派到任何 executor。

手工驻留：

```sql
CALL local.system.load_index(table => 'db.docs', index => '<uuid>', retain_for => '1h');
CALL local.system.offload_index(table => 'db.docs', index => '<uuid>');
```

`retain_for` 接受 `0`（不保护）、`forever`、`<n>smhd`（如 `30m`、`1h`），不给则保持当前保护期。输出是每文件一行的放置结果。

**开关是两侧共读的。** `spark.iceberg.index.cache.enabled=false` 时，executor 不广告预算、`load_index` 以 `CACHE_DISABLED` 失败，且**查询侧也不会去租用已经驻留的副本**（向量和文本一致）——所以"关掉开关回到冷读"是字面意思，不会因为之前开过、内存里还留着副本而继续命中。反过来，开关从开到关的切换，只对**下一次查询**生效：已经借出的副本会随租约结束释放，不会在查询中途被抽走。

BM25 和向量索引共用 executor 的容量预算、LRU 和状态入口。用文本索引的 UUID 调用同一个 `load_index`。启用调度局部性时，查询优先调度到已确认持有分片的 executor。分布式 BM25 查询命中驻留时直接从内存读取分片，未命中时按范围读取且不自动回填。查看合计占用使用 `CALL local.system.index_cache_status(refresh => true)`。

**range=on 的查询还会用到 list 块缓存，它与整文件条目共享同一份预算。** 块只从整文件占用后的剩余预算里装入，整文件装入需要空间时也会回收块；块计入 `index_cache_status` 的 `bytes`（常驻字节，与整文件条目合计），空闲 TTL 淘汰与 `offload_index` 会清掉对应的块。因此缓存预算要按"最大索引 ÷ 执行器数"给时，看到的是整文件与 list 块合计的占用。

当前版本的行为（不是建议）：

- executor 被杀后，driver 只有在**收到下一次上报**时才把它标记为丢失；在那之前它仍在账上，但已超过 10 秒没上报的节点不再是放置目标。
- 调用方等待放置完成的超时是 60 秒；超时后调用方返回，计划在 driver 侧仍可能保持挂起（已知缺口）。
- 内存评估依赖上报的运行时布局。**JVM 不是 17 时（比如 21），内存模型会报容量未知，缓存与驻留退化成"每查询重读"**。当前版本的镜像请用 JDK 17。

---

## 8. 配置总表

### 8.1 查询期（会话级，`SET` 时用反引号）

| 键 | 默认 | 作用 |
|---|---|---|
| `spark.iceberg.index.vector.ivf.range-read.enabled` | `false` | 整索引缓存未命中时，按字节范围读取分块 IVF 索引。 |
| `spark.iceberg.index.vector.ivf.nprobe` | `8` | 每次查询在每个 IVF 索引文件中初始探测的列表数。使用正整数；调用里显式写的值优先，本项 session 值只对 IVF 生效（非 IVF 索引上显式传 `nprobe` 报错）。 |
| `spark.iceberg.index.vector.refine-factor` | `1` | 基础候选预算为 K。正整数按 K 乘倍数计算；`0` 与 `1` 等价，都是 K。SQL 改写与批量 `CALL` 用同一个窗口。 |
| `spark.iceberg.index.vector.name` | 不设 | 同列多索引时选哪个 |
| `spark.iceberg.index.vector.filter-mode` | `pre` | task 内过滤落在打分前（`pre`）还是后（`post`）：`pre` 只给放行行打分，`post` 打分后逐行判定再扩样；只在 `ORDER BY vec_distance` 改写带过滤时生效，不是调用参数（见 4.6），非法值直接失败 |
| `spark.iceberg.index.cache.enabled` | `false` | 缓存开关；关掉时 `load_index` 报 `CACHE_DISABLED` |
| `spark.iceberg.index.cache.bytes` | 自动 | 每 executor 预算 |
| `spark.iceberg.index.cache.heap-fraction` | `0.25` | 自动预算的堆占比 |
| `spark.iceberg.index.cache.mode` | `auto` | 缓存档位 |
| `spark.iceberg.index.cache.idle-ttl` | `30m` | 整文件条目与 list 块的空闲淘汰 |
| `spark.iceberg.index.schedule.locality` | `true` | 按持有情况派 task |

查询期参数分两层，优先级从高到低：

1. **语句里写的**：`CALL … vector_search(..., nprobe => 32, refine_factor => 10, index => 'name')`；表函数同理（位置参数之后写 `nprobe => 32` 等命名选项）。非法值直接失败，不会回退。
2. **会话配置**：下面表里的 `SET spark.iceberg.index.vector.*`（以及 `spark-submit --conf`）。

索引定义目前不记录 `nprobe` / `refine_factor` 的默认值，所以实际就是上面两层；`nprobe` 的历史索引期选项已退役，会被忽略。`nprobe` 以调用值为准，只写 session 时该值只对 IVF 索引生效；非 IVF 索引上显式传 `nprobe` 是错误。`refine_factor`、`index` 也适用这条链；`index` 的语句级写法在过程和表函数里都支持。

`filter_mode` 不在这条链里：它不是调用参数，只有会话配置 `spark.iceberg.index.vector.filter-mode`，而且只作用于 `ORDER BY vec_distance` 改写带过滤的查询（4.6）。

### 8.2 executor 侧的调优键（有陷阱）

| 键（前缀 `spark.iceberg.index.cache.`） | 默认 | 是否真的生效 |
|---|---|---|
| `control.poll-interval` | `1s` | 生效（executor 轮询周期） |
| `control.max-commands` | `64` | 生效 |
| `load.max-concurrent` | `1` | 生效（加载线程池大小） |
| `load.timeout` | `600s` | 生效 |
| `load.max-attempts` | `3` | 生效 |

**陷阱**：executor 侧只认 Spark 注入的 `spark.plugins.internal.conf.<IndexCachePlugin 的类名>.<key>`，只有 driver 侧才回退到裸键 `spark.iceberg.index.cache.*`。直接用裸键 `--conf` 传时，executor 收到的仍是默认值。要改 executor 行为，必须用注入形式。

### 8.3 建索引时写死、事后不可改

`metric`、`structure`、`quantizer`、`max-m`、`ef-construction`、`ef-search`、`nlist`、`rabitq-bits`、`ivf-train-samples-per-list`、`ivf-train-max-iterations`、`ivf-train-convergence-tolerance`、`ivf-assign-kernel`、`ivf-build-kernel`、`max-entries-per-index-file`，以及文本侧的 `k1`、`b`（入参名 `length_norm`）、`min-token-length`、`lowercase`、`stopwords`。

`ef-search` 的值来自索引定义，调整它需要重建。IVF 的 `nprobe` 是查询期参数（调用参数优先，否则读 session 设置），历史定义中的值会被忽略。旧脚本须从构建选项移除 `nprobe`，并在查询会话设置原值。

---

### 8.4 内部测量开关（不是用户参数）

以下三项是 **JVM 系统属性**（`-D...`，或测试/测量程序里 `System.setProperty`），不是 session conf，也不在过程/表函数参数里；默认关闭，**不要写进生产配置**：

| 属性 | 默认 | 作用 |
|---|---|---|
| `iceberg.index.batch.queries-per-block` | 未设（按预算推导，上限 128） | 覆盖批量块大小，只给压测/测试用 |
| `iceberg.index.vector.refine` | `false` | 打开 IVF+RaBitQ 的"宽候选 + 精确重排"；回表行数约 ×8 |
| `iceberg.index.vector.refine.candidates` | `8` | 精排每 k 的候选倍数，仅在 `refine=true` 时生效 |

部署侧的同一条记录见 `docs/vector-index-deployment-guide.md` §5.6。

## 9. 已知限制

**9.1 空向量行不进结果集。** 建索引前就存在的 `NULL` 向量行不在索引里，查询的残差分支也会过滤掉。这是产品定义（没有向量的行没有"距离"），但要注意它与"未建索引时 Spark 用 `ASC` 默认 `NULLS FIRST` 把 null 排最前"不一致——同一个查询，加不加索引，行集可能不同。

**9.2 单索引文件 2 GB。** 读 payload 走 `byte[]`，超限在查询时报错；规避方式是调小 `max_entries_per_index_file`。构建期有检查，但它在 payload 已经编码成 `byte[]` 之后才跑：超限时整次构建失败，而不是在分组阶段就提示。

**9.3 非 identity 分区不改写。** 见 3.3。

**9.4 索引不自动维护。** 见 3.4。

**9.5 文本索引无法删除、无法按名刷新，且建索引失败不回滚。** 见 5.5。

**9.6 中文不可用。** 见 5.1。

**9.7 过程按 `coverage` 决定“给命中还是给活行”。** 两个过程（`vector_search` / `vector_search_batch`）的 `coverage` 都有四个值，默认 `table`：

| `coverage` | 返回什么 | 被删行怎么处理 |
|---|---|---|
| `table`（默认） | 表的**活行**（`row` 是业务 JSON） | 被丢弃；不足 k 条时用未覆盖文件/更深候选补齐 |
| `indexed` | 活行，但只在索引覆盖的文件里找，不扫未覆盖文件 | 被丢弃，可能不足 k 条 |
| `index_hits` | 索引命中本身（地址 + 分数 + `row`） | 保留该 hit，`row = NULL` |
| `positions` | 只有地址 + 分数（`row` 恒为 NULL） | 保留该 hit；不做快照/删除判定 |

所以要“命中里能看到占位空行”用 `coverage => 'index_hits'`；要“像查询一样拿可用的活行”用默认的 `table`。改写路径（`ORDER BY vec_distance`）等价于 `table`，会补候选；`index_hits` 不补，因此可能少于 k 条。

`positions` 与 `index_hits` 的差别只在"要不要行"：`index_hits` 会回表取 `row`（取不到就是 NULL），`positions` 完全不读数据文件，所以分数是索引估计、建索引后被删除的行也会出现——这是它为大批量省掉回表与 JSON 的代价。

**9.8 地址类表函数固定 `index_hits` 语义，没有 `coverage` 参数。** `vector_index_search` / `vector_index_search_batch` 用索引**最新**快照、不补未覆盖文件、不按源表快照对齐；被删除的行保留为 `row = NULL`，分数是索引估计、顺序是索引自己的顺序。要活行、要 `table`/`indexed`/`positions` 的差别，只能走过程或改写。

**9.9 大索引的跨节点放置。** 装不进单节点缓存的规模（如 100M HNSW），目前只能"装得下多少装多少"，没有分片放置与均衡调度。

**9.10 缓存关掉时状态里看到的是“预算 0 + STREAM”。** `cache.enabled=false` 时 executor 不广告预算、`load_index` 以 `CACHE_DISABLED` 失败，但上报的档位字段仍是 `STREAM`（内部“关闭”配置沿用了它）。所以 `index_cache_status` 里出现“预算 0 + STREAM”时，先看 `cache.enabled`：分不清“忘了开”和“选了 STREAM”是当前已知的表述限制，状态词表不在这版改动范围内。

**9.11 部分构建报 `PARTIAL`，不再报 `SUCCEEDED`。** 一次向量构建里某组数据建不出来时，那一组排除在外、其余照常发布，报告是 `PARTIAL`（6.1）；一个都没建成时才 `FAILED` 并回滚注册。判断覆盖看 `check_indexes` 的 `uncovered_data_files`（未覆盖数据文件数）与 `suggestion`（会给 `refresh_vector_index` 的补齐语句）；`index_build_status` 的 `files_failed` 是失败的**组数**，不是文件数。driver 日志里那条 WARN 会给出缺口文件数与若干具体路径，完整的失败堆栈只在该组的 executor 日志里。**失败以"组"为单位，默认预算很大**：`max_entries_per_index_file` 默认 100 万条目，会把大量小数据文件并成一组，一组失败带走整组——实测 4 个文件、预算 2 时，1 组失败让 2 个数据文件同时失去索引。把预算设小（如 `max_entries_per_index_file => 1`）可以让失败只影响单个文件，代价是索引文件更多、HNSW 图更碎。刷新补缺口时成本按组算，不是按新增文件算。

另注：`refresh_*_index` 报的 `SUCCEEDED` 说的是"这次刷新重建的那部分都成功了"，它的 `files_built`/`files_total` 只覆盖本次重建的子集，不是全表覆盖。全表覆盖看 `check_indexes`。

**9.12 批量过程没有逐块输出，表函数也不是流式。** `CALL … vector_search_batch` 是 Spark 的结果集 API，driver 必须收全结果才能返回；表函数 `FROM vector_index_search_batch(…)` 按块转换成 `UnsafeRow`、每块 `.coalesce(1)` 成一个分区、最后 `union`，但 driver 在 `union` 执行前仍持有整批的 `UnsafeRow`（峰值 O(整批)，不是 O(一块)），真实收益是每块的 `Row`+JSON 转换后即释放。大批量优先用表函数，或把过程的 `coverage` 设为 `positions`（每行约 30 B；带行 JSON 时约 `20 B/dim`，10k×k=10 的总量按常量推算约 5 GB 量级，未实测）。

**9.13 批量块是串行的，失败不隔离。** 块内任何一条查询失败都会让整块失败并重试整个 stage；块大小由内部预算推导（2560 维、带行的 `table`/`index_hits` 约 9–11 条，`positions` 最多 128），只可用内部 JVM 属性覆盖，不是用户参数。

---

## 10. 排错

| 现象 | 原因 | 处置 |
|---|---|---|
| `already exists` | 该列已有索引 | 传 `name => '…'`，或先 drop |
| 建索引 `FAILED` | 一个数据文件都没建成，常见是 `dimension` 与数组长度不符 | 看 `index_build_status.error` |
| 建索引 `PARTIAL` | 部分数据文件没建成，索引可用但没盖满 | 修数据或删掉那些文件，再 `refresh_vector_index`；缺口看 `check_indexes.uncovered_data_files` |
| `refresh_vector_index` 返回 0 文件但 `check_indexes` 仍报缺口 | 缺口不在当前表快照里（比如缺口是更早快照的事） | 确认 `source_snapshot_id` 与 `uncovered_data_files` 是否对同一份表快照 |
| `ABANDONED` | driver 在构建中退出 | 重新构建，再 `cleanup_index_files` |
| `No vector index exists` | 列上没有向量索引 | `build_vector_index` |
| `does not match index dimension` | 查询向量长度与索引维度不符 | 对齐长度 |
| 查询没走索引（`EXPLAIN` 里没有 `VectorSearchSource`） | 形态/分区/metric 不满足，或没加载扩展 | 按 4.3 逐条排查 |
| 召回明显偏低 | `nprobe` 太小，或带量化时倍数太小 | 先调 `nprobe`（无量化 `IVF` 只有这一个旋钮）；带量化再调倍数，1bit 可到 50–100 |
| 查询慢、`coverage` 低 | 缓存没开或预算不足 | 开 `cache.enabled` |
| `SET` 报 `INVALID_SET_SYNTAX` | 键名有 `.` 和 `-` | 用反引号包键名 |
| `text_score needs a text index … none was found`（但索引存在） | 索引落后，或 `match` 与别的谓词并列，或零命中 | 先 `refresh_text_index`；把 `match` 单独用（5.4） |
| 文本查询搜不到中文 | 分词不支持 | 见 5.1 |
| 文本建索引失败后无法重试 | 注册未回滚且无删除入口 | 见 5.5，需手工清理 |
| `cleanup_index_files` 返回 `in_flight_builds > 0` | 有构建或刷新正在写入或等待发布 | 等发布完成后重跑 |
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
| `vector_search`（过程） | `table, column, query, k` | `index, coverage, nprobe, refine_factor` |
| `vector_search_batch` | `table, column, k` | `queries, query_table, query_id_column, vector_column, index, coverage, nprobe, refine_factor` |
| `text_search` | `table, column, query, k` | — |
| `text_search_batch` | `table, column, queries, k` | — |
| `load_index` | `table, index` | `retain_for` |
| `offload_index` | `table, index` | — |
| `cleanup_index_files` | `table` | — |

### 表函数（参数必须是常量）

| 函数 | 参数 | 返回 |
|---|---|---|
| `vector_search` | `table, query_vector, k, vector_column`（位置序）＋ `nprobe` / `refine_factor` / `index` / `distance_type` / `columns` 命名选项；没有 `coverage` | 业务行 + `_distance` |
| `vector_index_search` | `table, column, query(array\<double\>), k`（位置序）＋ 同一组命名选项；没有 `coverage` | `data_file, pos, score, row`（固定 `index_hits` 语义） |
| `vector_index_search_batch` | `table, column, queries(array\<string\>，逗号分隔), k`（位置序）＋ 同一组命名选项；没有 `coverage` | 同上 + `query_id`（固定 `index_hits` 语义） |
| `search` | `table, query, search_columns(array\<string\>，恰好 1 列), k` | 业务行 + `_score` |
| `text_index_search` | `table, column, query(string), k` | `data_file, pos, score, row` |

### SQL 表达式与 DDL

`vec_distance(vector, query [, metric])` 越小越近，metric 接受 `l2`、`cosine`、`dot`，默认 `l2`；早期写法 `euclidean`、`cos`、`dot_product`、`inner_product`、`ip` 仍被接受。
`text_score(column, query)` 越大越相关。`match(column, query)` 布尔。

| 语句 | 支持的类型 | 说明 |
|---|---|---|
| `SHOW INDEXES FROM <table>` | 全部 | 等价 `list_indexes`，只有 `FROM` |
| `ALTER TABLE <t> CREATE INDEX <name> USING vector (<col>) WITH (…)` | vector | `WITH` 键＝过程形参名；`dimension` 必填 |
| `ALTER TABLE <t> CREATE INDEX USING fts (<col>)` | text | 不能带名字 |
| `ALTER TABLE <t> REFRESH INDEX <name>` | 仅 vector | 文本报 unsupported type |
| `ALTER TABLE <t> DROP INDEX <name>` | 仅 vector | 无 `purge` 参数，默认删文件 |

---

延伸阅读：`vector-index-guide.md`（向量参数曲线、改写规则细节、性能实测）、`docs/index-query-metrics.md`（查询分阶段指标与 event log 归因）、`docs/vector-index-deployment-guide.md`（上线、镜像、发布流程）。
