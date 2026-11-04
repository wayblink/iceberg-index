# 向量索引上线与运维手册（Spark 3.5 / Iceberg）

面向系统运维与平台方：这套东西怎么装、用户怎么用、发布走哪条路、出问题怎么处置。

**事实基线（2026-10-28）**：源码基线是**厂内** Iceberg `step-1.9`（不是上游 tag，见 §7.1）；验证集群是 `sh-a-spark` 的 `spark` 命名空间（Spark 3.5.7-STEP-rc3）；当前候选镜像是 `hub.i.basemind.com/spark/sparkann-e2e:2769-5f646565`（由指定 Spark 源码构建，见 §7.5.1）。本文逐项标注了哪些结论来自实测、哪些仍是建议——**"建议"部分在采纳前需要评审**；涉及基线、镜像内容与验收的结论，一律以 §7 的实测为准。

---

## 1. 这套东西是什么

它是 Iceberg 表上的一等（first-class）索引层：索引作为表元数据的一部分被管理，而不是外部系统里的一份旁路数据。当前有两个 kernel——向量（IVF × RaBitQ / HNSW / FLAT）与文本（BM25）——它们共享同一套索引元数据、快照、覆盖范围（coverage）、缓存与运维过程。

对用户来说只有两件事是新的：**建索引**（`CALL ... build_vector_index`）和**一条普通的最近邻 SQL**（`ORDER BY vec_distance(col, ...) LIMIT k`）。后者被优化器改写成索引检索 + 精确重排，用户不需要写任何新语法，也不需要改自己的查询框架。

四个部件，职责分开，故障时要能分辨是谁的问题：

| 部件 | 位置 | 职责 |
|---|---|---|
| 索引元数据层 | `core/`（`org.apache.iceberg.index.*`） | 索引定义/快照/清单/注册表的读写与格式版本 |
| kernel | `index-vector/`、`index-text/` | 打分、编码、payload 编解码（纯计算，不含 Spark） |
| Spark 集成 | `spark/v3.5/` | `vec_distance` 函数、改写规则、物理算子、`CALL` 过程、执行器侧缓存 |
| 发布产物 | `spark/v3.5/spark-runtime`（shadow jar） | 一个 shaded runtime jar，替换厂内镜像里的同名 jar |

---

## 2. 用户怎么用

### 2.1 建索引

有两种写法，语义相同、由同一套过程执行：

**（1）`CALL` 过程**（原有写法，参数最全，仍在支持）
**（2）`ALTER TABLE … CREATE INDEX`**（DDL 写法，见 2.1.1）

> 注意：**索引只能建在 Iceberg catalog 的表上，并且语句里的表名要带 catalog 前缀**（例如 `hms.db.docs`）。如果把 Iceberg 表建在会话目录（`spark_catalog`，即 Hive 会话目录）里，建表会成功，但之后的任何读取都会报 `iceberg is not a valid Spark SQL Data Source`——这不是索引层的问题，是 1.9.1 的 `IcebergSource` 只实现了 V2 provider，而 Hive 表读走 V1 路径。两种解法：始终用 Iceberg catalog 限定名，或把 `spark.sql.catalog.spark_catalog` 配成 `org.apache.iceberg.spark.SparkSessionCatalog`。（实测。）

```
CALL prod.system.build_vector_index(
  table                    => 'db.docs',
  column                   => 'embedding',
  dimension                => 128,
  metric                   => 'EUCLIDEAN',   -- EUCLIDEAN | COSINE | DOT_PRODUCT，默认 COSINE
  structure                => 'IVF',         -- IVF | HNSW | FLAT
  quantizer                => 'RABITQ',      -- RABITQ | NONE
  options                  => map('rabitq-bits', '1', 'nprobe', '32', 'nlist', '0'),
  name                     => 'docs_ivf_1bit',
  max_entries_per_index_file => 262144);
```

必填只有 `table`、`column`、`dimension`；其余都有默认值。`name` 强烈建议填：不填时索引只能用 UUID 寻址，运维和比对都很难看（benchmark 里踩过这个坑）。

**参数怎么选（来自实测，详见 `docs/sift-vector-index-benchmark.html`）**

| 场景 | 建议 | 实测依据 |
|---|---|---|
| SIFT 10M～100M，默认 | IVF + RaBitQ **1bit**，`nprobe=32`，over-sample **100** | 10M 4.4 s、100M 13.5 s，均为精确 recall；比全表扫描快 4.3×／9.9× |
| 表是 Parquet 且 ≥100M | IVF + 3bit，`nprobe=32`，over-sample 10 | 1bit 在 Parquet 上每个候选要贵 34–47 ms，宽候选付不起；3bit 只需 10 倍候选 |
| 表 < 10M 且索引能常驻 | HNSW 可用 | 10M 上 HNSW 最快 10–13 s，仍比 1bit 慢 2 倍；100M 上等于全表扫描，**不要用** |
| 需要"不走索引"的对照 | FLAT | 只作标尺；但注意它现在也会被改写成索引路径，做对照要自己写不命中改写的表达式（如距离后加 `+ 0.0d`） |

`4bit` 在四张实测表上没有一处优于 3bit（recall 不需要它、体积最大），**不建议作为默认**。

**埋点提示**：`rabitq-bits` 与 `nprobe` 写在索引定义里，同一个列上可以并存多个不同参数的索引（用 `name` 区分），这是做 A/B 对比的正确方式，也是 benchmark 的做法。

### 2.1.1 索引 DDL

```
SHOW INDEXES FROM hms.db.docs;

ALTER TABLE hms.db.docs
  CREATE INDEX docs_ivf_1bit USING vector (embedding)
  WITH (dimension = 128, metric = 'euclidean', structure = 'IVF',
        options = map('nprobe', '32', 'rabitq-bits', '1'));

ALTER TABLE hms.db.docs CREATE INDEX USING fts (body);   -- 文本索引暂时没有名字

ALTER TABLE hms.db.docs REFRESH INDEX docs_ivf_1bit;      -- 补段：只追未覆盖文件
ALTER TABLE hms.db.docs DROP INDEX docs_ivf_1bit;
```

行为要点（2026-10-30 起）：

- `CREATE INDEX` 的 `WITH` 键就是 `CALL` 过程的参数名，值按 SQL 表达式解析（支持 `map(...)`）；未知键、缺 `dimension`、重复键、非法 method 都在**分析期**报错。
- **文本索引还没有名字**：`CREATE INDEX USING fts` 不能带名字，带名字会明确报错并提示改用 `CALL build_text_index`；`REFRESH/DROP INDEX` 也按名寻址不到它。
- **`REFRESH` 是“补段”**：只为当前快照中未被子段覆盖的数据文件建新段，已有段原样保留。已覆盖文件的内容发生变化（被 rewrite 替换、或有了删除文件）时**不会再重建**，只打 WARN 并提示重建——这是与旧实现的语义差异，旧作业若依赖“刷新会顺带修正旧组”需要改成显式重建。
- `DROP INDEX` 沿用既有默认 `purge=true`（连索引文件一起删），不会只摘掉登记。
- 未实现、不要承诺：`OPTIMIZE INDEX`、短语/多列/混合检索。

### 2.2 查询

```
SELECT id, title
FROM db.docs
ORDER BY vec_distance(embedding, array(0.12d, 0.98d, ...))
LIMIT 10;
```

改写规则命中后，这次查询变成"索引检索候选 → 回表取行 → 精确重排 → 取前 k"。命中条件（`RewriteVectorSearch`）：

1. 该列上存在向量索引；
2. 至少有一个数据文件被索引覆盖（未覆盖的部分仍会残余扫描，所以索引落后于快照也能用）；
3. **表的非恒等分区不能存在**：数据文件里不含分区列的值，只有恒等分区能从路径里还原，所以 `bucket(...)`、`days(...)` 之类的表上规则会让路、查询退回扫描。恒等分区（`PARTITIONED BY (dt)`）可以。这条是集群验收时实测到的（`RewriteVectorSearch` 里的 `hasOnlyIdentityPartitions`），不是猜测——用 `bucket(4, id)` 建表时 `EXPLAIN` 里没有 `VectorSearchSource`，去掉分区后立刻出现。

**FLAT 也参与改写。** 早先的规则因为"FLAT 每个条目都要打分、代价等同扫描"把精确索引排除在改写之外；现在这条成本判断已被移除——能不能用索引只看正确性，不看代价（`TestVectorIndexGuide.theRewriteUsesWhateverStructureTheIndexHas` 断言了 `EXPLAIN` 里出现 `VectorSearchSource`）。所以 `ORDER BY` 自动改写对 FLAT 也生效，只是它省不了多少开销：候选就是全部条目，重排后仍然是全量打分。用户手册里"索引不能是 FLAT"的说法属于旧行为，以本条为准。

**没有向量的行不参与向量检索。** 向量索引的每个条目都要存一个向量，所以 `NULL` 向量在建索引时被跳过（构建日志里会有一条 WARN 说明跳过了多少行），查询时也不会成为候选。这带来一处需要知道的差异：对**可空**的向量列，如果没有写 `IS NOT NULL`，走索引的查询只返回**有向量**的行，而不走索引的全表扫描仍会返回 `NULL` 行、并且因为 `ORDER BY ... ASC` 默认 `NULLS FIRST` 而排在最前面。想让两者逐行一致，把 `WHERE embedding IS NOT NULL` 写进查询即可——索引路径与扫描路径对它的处理完全相同。文本索引不受此影响：`NULL` 文本仍是一条零 token 的文档，计入文档数与平均文档长度。

一个列上有多个索引时，`ORDER BY` 里没地方写索引名字，必须用配置指定，否则规则不动手（避免猜）：

```
SET spark.iceberg.index.vector.name = docs_ivf_1bit;
SET spark.iceberg.index.vector.over-sample = 100;
```

`over-sample` 的含义是"向索引要 k×over 个候选，再取前 k"。对 `ORDER BY` 的自动改写，取前 k 之前会**精确重排**，所以放大倍数提高的是**召回**；对 `CALL vector_search` / 两个向量 TVF，结果合同是"索引自己的原始分数、按索引顺序取前 k、删掉的行以 `row = null` 保留"，**不精确重排、不扫未覆盖文件**，因此放大倍数只扩大候选范围、不会让结果变成"精确 Top-K"（近似 kernel 扩大范围后候选集本身可能变，但不保证与旧 `C=k` 逐条一致）。

它是唯一在查询时能调的召回/延迟旋钮：一次设成 100 是安全的默认（实测 10M/100M 的 1bit 都要 100 倍候选才追平精确召回）。未设置时用兼容基线 1，并在计划里标成 `COMPAT_DEFAULT(no-calibrated-policy)`——那不是"已-calibrated 的最优值"。显式设置（包括显式写 1）一律保留；空串、非整数、0、负数会报错而不是被静默改成 1。

**近似 kernel 要拿到与精确扫描一致的结果，必须放大 over-sample。** 集群验收实测：200k 行、16 维、IVF+RaBitQ 4bit、`nprobe=1024`、`nlist=256` 时，`over-sample` 为默认值 1 的 Top-10 与 L2 真值**不同**（召回不满）；把 `spark.iceberg.index.vector.over-sample` 设成 4000 后，Top-10 与 L2 真值逐行一致。也就是说"索引结果 = 精确结果"不是默认行为，而是候选预算足够的产物。

**注意度量**：`vec_distance` 默认算余弦，只有在被改写命中时才特化成索引的 metric。所以"手写一个不走索引的对照查询"时，`vec_distance(...) + 0.0d` 这种写法其实是余弦排序——若索引是 EUCLIDEAN，两者排序不同，会出现"精确索引也差一行"的假象。做对照必须自己写 L2 表达式。

### 2.1.1 索引 DDL（2026-10-30 起）

```
SHOW INDEXES FROM hms.db.docs;

ALTER TABLE hms.db.docs CREATE INDEX docs_ivf_1bit USING vector (embedding)
  WITH (dimension = 128, metric = 'euclidean', structure = 'IVF',
        options = map('nprobe', '32', 'rabitq-bits', '1'));
ALTER TABLE hms.db.docs CREATE INDEX USING fts (body);   -- 文本索引暂时没有名字
ALTER TABLE hms.db.docs REFRESH INDEX docs_ivf_1bit;      -- 补段 + 重建内容变过的组
ALTER TABLE hms.db.docs DROP INDEX docs_ivf_1bit;
```

要点：`WITH` 的键就是 `CALL` 的参数名，值按 SQL 表达式解析（支持 `map(...)`），未知键/缺 `dimension`/重复键/非法 method 都在**分析期**报错；**文本索引不带名字**（带名字会明确报错并提示用 `CALL build_text_index`），`REFRESH/DROP` 也按名寻址不到它；`DROP` 沿用默认 `purge=true`；`OPTIMIZE INDEX`、短语/多列/混合检索未实现。

### 2.3 运维过程（都是 `CALL`）

| 过程 | 用途 | 什么时候用 |
|---|---|---|
| `list_indexes(table=>...)` | 列出索引与状态 | 日常巡检；**失败要当故障处理**（见 §6） |
| `check_indexes(table=>...)` | 体检：是否需要重建 | 定期跑；返回可粘贴的 `refresh_vector_index(...)` 建议 |
| `index_build_status(...)` | 构建进度 | 大表构建期间；状态落文件，随时可查 |
| `index_cache_status()` | 各执行器缓存占用/覆盖 | 排查"为什么某次查询慢" |
| `refresh_vector_index(...)` | 补段 + 重建内容变过的组 | 数据变更后；索引落后不阻塞查询，但覆盖率下降会变慢 |
| `load_index` / `offload_index` | 手动驻留/卸载 | 见 §5.3；**当前仍在完善**（§7） |
| `cleanup_index_files(...)` | 清理孤儿文件 | 构建失败或重建后的收尾 |
| `drop_vector_index(...)` | 删索引 | 下线/换参数 |

索引注册表位置由表属性 `index.registry` 指定（如 `indexmeta.v3-4c8c1719.json`），是版本化文件——**不要手工改这个属性或这些文件**；回退单文件布局（`index-registry.json`）只在属性丢失时由程序自己降级。

---

## 3. 上线形态：合不合厂内仓库

这是运维最需要拍板的一件事。三个选项，实测与推理如下。

| 选项 | 做法 | 优点 | 代价 | 判断 |
|---|---|---|---|---|
| **A. 打进 Spark 镜像**（当前做法） | 基础镜像上只删掉 vendor 的 `iceberg-spark-runtime`，把我们的 shaded runtime jar 放进 `/opt/spark/jars`；Spark 自带的 `parquet-*`、`avro-*` **不动**（见 §7.1） | 不改厂内 Spark；一个 jar 覆盖全部；已验证可跑 | 镜像与代码版本强绑（#2756 就是这么来的）；每次升级重建镜像 | **短期采用** |
| **B. 合入厂内 Iceberg fork** | 把 `core/index-*`、`spark/v3.5` 的改动作为 fork 的常规分支维护 | 版本可追溯、可发版、CI 覆盖 | 需与上游保持 rebase；需要 revapi 门禁 | **中期采用**（本质上仓库已经是 fork，正式化即可） |
| **C. 合入厂内 Spark 仓库** | 修改 Spark 内核 | —— | 我们只注入 session extension、函数、一条 optimizer rule 和一个 planner strategy，**不修改 Spark 任何代码** | **不做**，没有理由 |

**结论**：A 现在用，B 收口长期，C 明确否掉。理由是"只要不改 Spark 内核，就不要碰 Spark 的发布节奏"——我们与 Spark 的契约只是 `spark.sql.extensions` 注入点，这个契约稳定且不需要 fork。

**为什么必须删 vendor 的 jar**（不是可选优化）：厂内镜像的 `/opt/spark/jars` 在 JVM 自己的 classpath 上，`iceberg-spark-runtime-1.9.1` 会遮蔽我们的 jar，导致 session extension 解析到 1.9.1 那份、`vec_distance` 不存在、所有查询退化成扫描；（不涉及 parquet/avro：回溯版不替换它们，见 §7.1。）用 `spark.jars` 传递不行：child-first loader 加载不到 session extension。所以只能烘进镜像并删除被替换的 jar。

### 3.1 版本与产物命名

- 仓库版本号沿用 Iceberg 的（回溯到 1.9.1 基线后是 `1.9.1` 一线），**不要另起版本号体系**，否则依赖解析与 revapi 都会乱；对外用带来源的后缀区分（见下一条）。
- Scala 版本必须与厂内 Spark 发行版一致：`_2.12`（仓库默认 `defaultScalaVersion=2.12`，厂内 Spark 也是 2.12）。
- 发布产物只有一个：`iceberg-spark-runtime-3.5_2.12-<version>.jar`（shadow jar，已包含 `iceberg-index-vector`、`iceberg-index-text`、`iceberg-index-spark`）。
- **版本号不得伪装成上游版本**：它是「厂内 1.9.1 + 索引能力」，由分支上的 `version.txt` 决定，取 `1.9.1-shb.N`（递增与发布说明的要求见 §7.8 决定 C），并在发布说明里写明基线是厂内 `step-1.9` 的哪个 commit、不支持 variant 类型。
- 镜像 tag 建议 `<日期><序号>`（如 `20261027a`），并在镜像 LABEL 里写入源码 commit、jar 的 sha256 与内部 shaded 的 Avro/Parquet 版本（§7.8 决定 D），便于回答"这个镜像到底是哪版"。

---

## 4. 发布流程

### 4.1 构建

```
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
./gradlew :iceberg-spark:iceberg-spark-runtime-3.5_2.12:shadowJar -DsparkVersions=3.5
./gradlew spotlessCheck revApiCheck            # 格式与 API 兼容门禁
./gradlew :iceberg-core:test :iceberg-index-vector:test \
          :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "*Vector*" --tests "*Index*"
```

集群侧另有需要单独构建的两个测试工具（不进发布产物）：`cluster-build.jar`（建索引的 driver）与 `bench/vector-benchmark.jar`（测量）。**它们必须与镜像里的 jar 来自同一个 commit**——#2756 的直接原因就是这两者不同步。

### 4.2 镜像

```
cd /data/workspace/sparkann
# Dockerfile 只做两件事：删掉 vendor 的 iceberg-spark-runtime、拷入我们的 jar
# parquet / avro 不替换：Iceberg 自带 shaded 依赖在 runtime jar 内部（见 §7.1）
docker build -t hub.i.basemind.com/spark/sparkann-e2e:<tag> .
docker push hub.i.basemind.com/spark/sparkann-e2e:<tag>
```

### 4.3 验收（缺一条就不发）

在单表上按顺序跑，全部通过才算可发：

1. `CALL system.list_indexes(table => 'db.t')` **返回全部行**（这条就是 #2756 的回归点）；
2. `CALL system.build_vector_index(...)` 建一个新索引，`index_build_status` 能看到进度与终态；
3. `ORDER BY vec_distance(...) LIMIT 10` 走索引（执行计划里出现索引算子），结果与全表扫描的 L2 对照一致；
4. `CALL system.check_indexes(...)` 返回 needs_rebuild=false；
5. 删掉一个 executor 再查询，结果逐行一致（缓存丢失不应改变答案）；
6. `CALL system.index_cache_status()` 能报出占用；未设预算时不 OOM。

验收脚本建议直接复用工具仓 `iceberg-tools`（与本仓并列）里的 `vector-benchmark/`（`run-benchmark.sh` + `collect-results.sh`）：它能同时给出"走索引/不走索引"的召回与延迟，比手工 SQL 更不易漏。工具仓不在本仓内，也不参与本仓的构建与 CI。

### 4.4 灰度与回滚

- **灰度**：先单表建索引 → 观察一周（查询延迟、`check_indexes`、缓存占用）→ 再扩到同库其它表 → 最后才改默认配置（`over-sample`、缓存预算）。
- **回滚**：镜像回退到上一 tag 即可，索引数据不用动——读侧同时接受 manifest v1 与 v2（`IndexManifest.read` 断言 `version == 1 || version == 2`），旧镜像能读旧索引；**但新镜像建的 v2 索引，旧镜像读不了**（写入无条件写 v2）。所以回滚窗口内不要再建新索引，或回滚后对相关表执行一次 `cleanup_index_files` + 重建。
- **向前兼容责任**：新增字段必须走"写入端可跳过、读取端可缺省"的路子，否则就是又一次 #2756。

---

## 5. 运维要点

### 5.1 容量

索引体积随量化位数近似线性（同一份 SIFT 数据实测）：

| 数据量 | 1bit | 2bit | 3bit | 4bit | 未量化 | HNSW |
|---|---|---|---|---|---|---|
| 10M × 128 维 | 323 MB | 418 MB | 630 MB | 752 MB | 1.55 GB | 2.36 GB |
| 100M × 128 维 | 3.21 GB | 4.16 GB | 6.28 GB | 7.49 GB | 15.42 GB | 23.7 GB |

规划时按 `1bit ≈ 30 字节/行`、`3bit ≈ 60 字节/行` 估算（含索引文件头与量化参数）。索引文件数由 `max_entries_per_index_file` 决定（benchmark 用 262144，10M 表 38 个文件、100M 表 256 个）。

**单个索引文件的上限是 2 GB（2147483647 字节）。** 查询时一个执行器要把整个 payload 读进一个数组，超过这个长度就永远读不了，所以构建会在写盘前就失败，并在错误里给出"按当前每文件条目数推算、能保证不超限的 `max_entries_per_index_file` 建议值"。按上面的体积表，100M × 4bit 分成 256 个文件是 29 MB/文件、留了约 70 倍余量；真正会撞上的是"少文件布局"（把 `max_entries_per_index_file` 调得很大，让多个数据文件合成一个索引文件）或本身含几千万行的超大单个数据文件。撞上时唯一的解法是**把分组预算调小**（得到更多、更小的文件），而不是切分索引文件——索引文件是一个整体，图或倒排表被切开就不再是一个索引。

### 5.2 缓存预算

`spark.iceberg.index.cache.enabled`（**默认 false**，必须显式打开；只设 `bytes` 不会启用）、`spark.iceberg.index.cache.bytes`（每个执行器的预算，默认按工作集与堆推导）、`spark.iceberg.index.cache.mode`。开关是两侧共读的：关掉时执行器不广告预算，`load_index` 会以 `CACHE_DISABLED` 失败，查询也不会使用缓存——不会出现"加载报 COMPLETE、查询却看不到缓存"的分裂。缓存关闭时 `index_cache_status()` **返回 0 行**（它不会报"未启用"），2026-10-30 在 A 集群实测：只设 `bytes` 时 0 行，加上 `enabled=true` 后每个执行器与 driver 各一行（`mode=PIN_ALL`、`entries`、`bytes`、`coverage`）。经验：**索引装不进缓存时会退化成"每次查询读自己的索引文件"**，100M 的 HNSW（23.7 GB / 6 执行器）就是这样废掉的——它的延迟等于全表扫描。所以缓存预算要按"最大索引 ÷ 执行器数"给，宁多勿少。

### 5.3 驻留与放置

`load_index` / `offload_index` 用于手动固定/卸载某个索引的驻留，`spark.iceberg.index.schedule.locality` 影响放置。**这套能力仍在收口**（`#2391`，7/9 子卡完成），当前可用的稳定部分是"自动缓存 + 查询时按需加载"；把 `load_index` 用于生产前，请先按 §7 的缺口确认。

### 5.4 监控与日志

- `CALL system.index_cache_status()`：每执行器占用/命中；建议采集为指标。
- `CALL system.index_build_status(...)`：构建进度与失败原因。
- 日志关键字：`INDEX_CACHE`（每次查询的缓存摘要）、`TIMING search+resolve` / `TIMING merge`（改写路径的分段耗时）。
- 告警建议：`list_indexes` 连续失败（§6 第一行）；查询 P99 突增且 `index_cache_status` 显示占用远小于索引体积（缓存没生效）；`check_indexes` 报 needs_rebuild。

### 5.5 故障处置表

| 症状 | 最可能原因 | 处置 |
|---|---|---|
| `list_indexes` 抛 `Unsupported index manifest format version: 2` | 该索引的清单是旧构建器写的 v2（每文件内嵌 memory），镜像里的 `IndexManifest` 只认 v1（#2756） | 重建镜像；**不要手改清单文件、也不要靠重建索引**——重建只会再写一次 v2。当前构建器已回到 v1 + 尾部附加段，新写的清单旧读者也能读 |
| 查询没走索引、计划里是扫描 | 列上无索引 / 索引快照对读快照没有覆盖 / 一列多索引但没设 `spark.iceberg.index.vector.name` / 形状不命中白名单（Join、聚合、窗口、OFFSET、多排序键、非纯属性投影） | 先看 `EXPLAIN EXTENDED` 里有没有 `VectorSearchSource`，再按 §2.2 的命中条件逐条查 |
| 结果比全表扫描少几行 | 用了 `vec_distance` 的默认余弦做对照，而索引是 EUCLIDEAN | 用 L2 表达式做对照（§2.2 末尾） |
| 构建中断后索引"存在但查不到" | 早期版本会留下空注册 | 已修（失败构建会 `dropIndex(..., true)`），仍建议跑 `cleanup_index_files` |
| 100M 级查询退化成扫描速度 | 索引装不进缓存（HNSW 典型） | 换 IVF + 量化；或提高缓存预算 |
| executor 挂掉后结果不一致 | 不应该发生（缓存丢失只影响速度） | 按缺陷处理，附 `index_cache_status` 与两次结果 |

---

## 6. 安全与权限

- 建索引会写表元数据（提交一次 `MetadataUpdate`），因此**必须由表 owner 或具备修改表元数据权限的 principal 执行**，不能用只读账号。
- 索引文件写在表目录下（`<table>/indexes/<uuid>/`），权限沿用表存储的权限模型；**不要给它单独开匿名读**——索引文件里是原始向量的编码，泄露它的后果与泄露数据列一样。
- `vector_search` 过程会返回 `row` 列（JSON），等于绕过列级权限直接返回行内容；对外暴露该过程前需要确认列权限语义。
- 不要提交任何包含凭据的 catalog 配置到仓库或镜像。

---

## 7. 版本基线与升级路径

这一节是部署前必须拍板的部分。下面每个数字都来自 2026-10-28 在**真实厂内基线上跑出来的实测**（源码 `gitlab.basemind.com/storage/iceberg` 的 `step-1.9`，实验树独立 worktree），不是估算；还没做的是集群验收，范围见 7.5。

### 7.1 源码基线：厂内 `step-1.9`，不是上游 tag

厂内 Spark 钉死在 3.5.7，随带的 Iceberg 是厂内自建的 1.9.1。回溯目标因此是**厂内那份 1.9.1**，而不是"换成 1.12/1.11 的 runtime"——换 jar 换的是整个集群的 Iceberg 运行时，写侧会获得更高 format-version 的能力，一旦某张表被写成新版本，跑旧 runtime 的其它读方直接读不了。这个风险与索引功能无关，不该被"装索引"顺带引入。

实测出的厂内基线与上游 tag 的关系：

| 项 | 值 |
|---|---|
| 厂内仓库 / 分支 | `gitlab.basemind.com/storage/iceberg`，`step-1.9`，tip `d2e4ba7e4` |
| 与上游 `apache-iceberg-1.9.1`（`f40208ae6`）的关系 | **0 behind / 7 ahead**（merge-base 就是 tag 本身） |
| 差异规模 | 23 个文件，+1290 / −70 |
| 7 个提交 | executor cache 独立线程池、Parquet 小文件修复、split-size session conf、duplicate DV 回填（2 个提交 + 2 个 merge）、amoro 默认值与版本号 |

结论有两条。**第一，移植基线取 `step-1.9` 的 tip，不取 tag**：这 7 个补丁就是厂内生产在跑的行为，站在 tag 上回溯等于把生产已经修掉的问题重新引入一份。**第二，"厂内有没有偷偷改方法体"这个悬而未决的风险消失了**：不需要再逐类 diff 字节码，源码就是答案。

**内部 shaded 依赖不跟着外部 Spark 走。** 之前"把 parquet 降到 1.13.1、avro 降到 1.11.4"的结论作废，原因是搞混了两层命名空间：Iceberg runtime jar 内部打包的是 shaded 的 Avro **1.12.0** 与 Parquet **1.15.2**（jar 内 `pom.properties` 实读），而 Spark 侧 `pom.xml` 声明的 Avro 1.11.4 / Parquet 1.13.1 只影响 Spark 自己的类加载，改不了已经编译进 fat jar 的字节码。所以回溯版保留内部 1.12.0 / 1.15.2，同时**不替换** Spark 的 `avro` / `parquet` jar——这也正好是厂内现状。

**JDK 目标是 17。** 上游 1.9.1 的 `build.gradle` 里是 `options.release = 11`，而索引代码用了 1.11 时代的语言特性（`record`、`instanceof` 模式、switch 箭头）。集群运行时用 JDK 17，因此回溯分支把编译目标改成 17，不做 Java 11 降级改写。

顺带确认一个原先以为必须打补丁的问题并不存在：1.9.1 全仓 0 处 `variantType(`，它建 variant 列时只建普通 group，所以回溯版**不需要**任何 variant 补丁。

### 7.2 迁移闭包与实测冲突面

索引能力在 fork 里的真实边界是**一段提交区间**，不是"几个文件"：`7447be7cd..main`，277 个提交，全部作者 `wayblink`。落到文件级是 **278 个（265 新增 + 13 修改）**，其中在 1.9.1 里已经存在的**只有 11 个**。这 278 个里有一部分是 Lance 文件格式与文档，按 §7.3 的决定不进回溯版：**实际要搬的是 250 个**（219 个产品/测试代码 + 29 个文档与报告），构成是去掉 `lance/` 22 个、`core/.../formats/` 2 个、4 个 `TestLance*` 测试。

两个必须先记住的教训。**其一，闭包必须钉在具体 commit 上**：我开工时 main 是 `c8dcf1a83`，做完实验它已经前进到 `12f4a04f9`（#2796/#2797 的修复），清单会漂；移植卡必须写固定快照，不能写 `main`。**其二，之前"只改了 3 个既有上游文件"的结论不成立**：那是按提交信息过滤出来的，真正的文件级差异是 13 个；按 §7.3 去掉 Lance 与 format-model 之后实际要搬 250 个文件，另外还差一个测试支撑类（见 7.4）。

移植方式：不做 277 次 cherry-pick，改为**按固定快照做文件级搬运**（265 个新文件整文件放入，13 个改造文件对 1.9.1 版做三路合并）。实测代价：

| 13 个改造文件 | 结果 |
|---|---|
| 干净应用 7 个 | `FileFormat`、`build.gradle`、`settings.gradle`、`spark/v3.5/build.gradle`、`IcebergSparkSessionExtensions.scala`、`TestRemoveOrphanFilesProcedure`、`SparkProcedures` |
| 冲突 4 个，都是小冲突 | `.gitignore`、`docs/docs/spark-procedures.md`、`gradle/libs.versions.toml`、`DeleteOrphanFilesSparkAction`（只差两行 import） |
| 1.9.1 里不存在、按新增处理 2 个 | `core/.../formats/FormatModelRegistry.java`、`ReadBuilder.java` |

也就是说"277 个提交重放"的工作量被压成"250 个文件 + 4 个小冲突"，冲突面比 cherry-pick 路线低一个量级。这是本方案的实质结论之一。

### 7.3 两个已定的取舍：不移植 format-model，不支持 Lance

**决定一：回溯版不移植 1.11 的 format-model 子系统，索引侧改成 Parquet 直读。** 索引代码原本通过 `FormatModelRegistry.readBuilder(...)` 读数据文件与 delete 文件，这是一个 1.11 才有的子系统（`formats/` 7 个文件 + 各格式的模型实现：Parquet 430 行、ORC 316 行、Avro 277 行 + Generic/Spark 注册类）。实测把它整体搬过来会遇到两层问题：Parquet 模型直接引用 variant 时代的 API（`PARQUET_SHRED_VARIANTS`、`VariantShreddingAnalyzer`、`BufferedFileAppender`），这些在 1.9.1 里不存在，等于要边搬边删代码；而 Avro/ORC 模型还要求把若干 `private` 内部类提权。代价明显高于收益，因为**我们的生产表都是 Parquet**，索引读文件只需要 Parquet 一条路径。

替代实现只有一处改动：`IndexEntries` 里两个调用点改成 `Parquet.read(input).project(projection).createReaderFunc(GenericParquetReaders.buildReader(...))`，并在模块依赖里加 `iceberg-parquet`。**代价必须写清**：1.9.1 没有"只读指定 row 位置"的能力，取回（fetch-back）会退化为整文件读，命中行由调用方按位置过滤——结果正确（代码里本来就有"格式忽略位置则按顺序重新编号"的兜底），慢多少要在 10M 基准回归里量（第 8 章已知缺口）。如果后续必须做位置裁剪，再评估把 1.11 的 Parquet 位置读单独摘出来。**这条改动是实测出来的，不是设想**：改造前 `spark-extensions` 有 27 项因 `Format model is not registered for format PARQUET and type interface org.apache.iceberg.data.Record` 失败，改造后这些用例全部通过（另一次 62 项的抽查 0 失败），日志里 `FormatModelRegistry` 命中 0 次。

**决定二：回溯版不支持 Lance 文件格式。** Lance 模型依赖 1.11 的 format-model 子系统、Arrow 18（本 fork 其余部分在 Arrow 15）和一个本地打过补丁的 `lance-core`。实验里 4 个 Lance 测试的失败是 `UnsupportedOperationException: Unsupported data file format: LANCE`，属于"没搬这条线"的预期结果。Lance 目前只用于基准对比，不在生产路径上，因此回溯版明确不支持，`FileFormat.LANCE` 枚举保留（写侧不会产出），Lance 相关模块不进 1.9.1 分支。

### 7.4 清单外的必需补入与小改

照 7.2 的清单搬运**还不够**。实测踩出下面这些，移植卡必须照做；反过来，"不移植 format-model"这个决定也消掉了一批原本要补的东西，两条一起看。

| 类别 | 项 | 为什么必须 |
|---|---|---|
| 清单外文件（1） | `spark/.../DummyMetricsServlet`（测试支撑） | 被移植测试以**字符串**类名引用（`spark.metrics.conf.*.sink.servlet.class`），编译期发现不了，运行时 `ClassNotFoundException` |
| 仓库内小改 | `api/Files.java`：`localInput` / `localOutput` 支持 `file:` 前缀 | 1.9.1 不剥 `file:` 会把路径当相对路径写到当前目录，索引状态文件因此"写了但读不到"（`TestIndexBuildStatus` 6 项失败，修完归零） |
| 仓库内小改 | `spark/.../procedures/BaseProcedure.java`：补 `requiredInParameter` / `optionalInParameter` 两个助手 + `ProcedureParameter` import | 移植过来的 20 余个过程全靠这两个助手声明参数，1.9.1 没有 |
| 仓库内小改 | `gradle/libs.versions.toml`：加 `junit-pioneer 2.3.0` | 移植的测试用它；属 1.11 的测试基建 |
| 仓库内小改 | `build.gradle`：`options.release` 与 scalac `-release` 改 17；`index-spark` 增 `iceberg-parquet` 依赖；删掉 `iceberg-lance` 模块与 `spark/v3.5` 对它的 `testImplementation` | 见 §7.1（JDK 17）与 §7.3（Parquet 直读、不支持 Lance） |
| **陷阱** | `.gitignore`：删掉 1.9.1 的 `gradle/` 忽略行 | 1.9.1 整目录忽略 `gradle/`，保留它会让 `gradle/libs.versions.toml` 的改动静默不被提交——版本目录改了等于没改 |
| 不需要了（因 §7.3 决定） | `core/.../formats/` 7 个文件、`PositionDelete.deleteClass()`、`FileWriterBuilderImpl` 的 `FileAppender` import 与泛型强转、Avro 内部类可见性放宽、Parquet/ORC/Avro 三个格式模型（430/316/277 行） | 这些都是 format-model 子系统的接口；索引侧改 Parquet 直读后整条链不再需要 |

**一个通用教训写在这里**：那 13 个"改造文件"绝对不能整文件复制主仓版本，必须和 1.9.1 版做三路合并。例如主仓的 `PositionDelete` 在 1.11 里已被上游改过，整文件覆盖会丢掉 1.9.1 里还在用的 `set()` / `row()`。同理 `DeleteOrphanFilesSparkAction` 要保留 1.9.1 的 `FileURI` / `FileSystemWalker`，只并入索引相关的两行 import。

### 7.5 已完成的验证与还没做的验证

已完成（实验树 `exp/2763-closure`，基线 `d2e4ba7e4`，JDK 17，`-DsparkVersions=3.5 -DscalaVersion=2.12`，全部 `--no-build-cache` + `cleanTest`，数字取自 test-results XML）：

| 范围 | 结果 |
|---|---|
| 编译（含测试代码） | `core`、`index-vector`、`index-text`、`index-spark`、`spark-3.5`、`spark-extensions` 全部通过 |
| `core` 索引单测 | 86 项，0 失败 |
| `index-vector` | 89 项，0 失败 |
| `index-text` | 6 项，0 失败 |
| `index-spark` | 271 项，0 失败 |
| `spark-3.5`（`*Vector*` / `*Index*`） | 197 项，0 失败，39 跳过（跳过的都是 Parquet 向量化读的参数化用例，与索引无关） |
| `spark-extensions` 抽查（等值删除 / 检索执行 / 带删除的检索） | 62 项，0 失败 |
| `spark-extensions` 全量（`*Vector*` / `*Index*`，含 16 个 `CALL` 过程） | 486 项里 49 失败：**45 项是嵌入 Hive metastore 的 `Broken pipe`**（隔离单跑该类 44 项 0 失败，属测试环境问题，不是移植代码）；**4 项是 Lance 写路径**（`BaseFileWriterFactory.newDataWriter` 没有 LANCE 分支，按 §7.3 决定不支持） |

### 7.5.1 目标 Spark 发行版与候选镜像（2026-10-28）

上游 tag 与本机构建是两件事，这里只记录**用指定源码构建出来的制品**：

| 项 | 值 |
|---|---|
| Spark 源码 | `gitlab.basemind.com/storage/spark` 的 `step-spark-3.5`，`5f6465658458da1b30f8de27bf4657c2c193e6e1`（live `git ls-remote` 复核） |
| 构建方式 | 厂内 `step-build.sh`（默认全 profile：`-Phive -Phive-thriftserver -Pconnect -Pkubernetes -Phadoop-cloud -Pndp-repo -Pjvm-profiler`），JDK 17.0.18、Scala 2.12.18、bundled Maven 3.9.6 |
| 发行包 | `spark-3.5.7-STEP-rc3-bin-3.3.9-STEP-rc2.tgz`；`dist/RELEASE` = `3.5.7-STEP-rc3 (git revision 5f64656584)`；`spark-submit --version` 报 `Revision 5f6465658458da1b30f8de27bf4657c2c193e6e1` |
| 发行包自带的关键依赖 | 已含厂内 fork：`gluten-package-1.6.0-spark35-celeborn050-step-rc4`、`celeborn-client-spark-3-shaded 0.5.0-STEP-rc3`、`alluxio-mount-aware-client-assembly 2.9.0-STEP-rc4`、`openlineage-spark 1.49.0-step1`，以及**待替换的** `iceberg-spark-runtime-3.5_2.12-1.9.1-STEP-rc3`、`avro-1.11.4`、`parquet-column-1.13.1` |
| 候选镜像 | `iceberg-backport:1.9.1-shb.1-5f646565`（由该 dist 生成的 `spark:1.9.1-shb.1-5f646565` 再加一层：删 vendor Iceberg runtime、放入我们的 `1.9.1-shb.1`） |
| 镜像内 runtime | 只有一个：`iceberg-spark-runtime-3.5_2.12-1.9.1-shb.1.jar`，sha256 `e7cc11d9c25c5e8fd9cca02814a06c72ce60d6bd31e9fca35c45d2955d581bca` |
| 镜像内 Spark 库 | `parquet-column-1.13.1.jar`（sha256 `3ff4222b…`）、`avro-1.11.4.jar`（sha256 `eeba11b7…`），与 7.1 的"不动 Spark 外部 jar"一致 |

**目标镜像上跑过的真实验收（`spark-sql -f`，本地 hadoop catalog，`local[2]`）**：建向量索引 → `list_indexes` 返回 1 行 → `EXPLAIN EXTENDED` 出现 `VectorSearchSource[metric=EUCLIDEAN, uncoveredFiles=0, indexSnapshot=1]` → `ORDER BY vec_distance(...) LIMIT 2` 返回 `1, 2` → `check_indexes` 返回 `needs_rebuild=false` → `index_cache_status` 可读；文本侧建 BM25 → `text_search` 返回 2 条带分数 → `check_indexes` 不再抛类型转换异常（即 §7.4 里那条 `#2796` 修复已在目标镜像中）。

**仍未做的**：集群六条验收（k8s + 生产 catalog）与 10M 基准回归 —— 上面是单机 `local[2]` 的制品验收，不能替代 #2769。

**这些通过只证明"代码在 1.9.1 上活着"，不证明生产可用。** 还没做、且必须做的：集群六条验收（`list_indexes`、建索引、SQL 查询走索引且与 L2 全扫逐行一致、`check_indexes`、杀 executor 后一致、缓存状态可读）、10M 基准回归（Parquet × 量化 1/2/3/4bit，并量出 7.3 里取回退化的实际代价）、镜像重建（此时不再需要替换 parquet/avro，只需删 vendor 的 `iceberg-spark-runtime` 并放入我们的 jar）。基线一换，之前所有集群召回/延迟数字全部失效。

### 7.5.2 集群六条验收（2026-10-28，`sh-a-spark` / ns `spark`）

镜像 `hub.i.basemind.com/spark/sparkann-e2e:2769-5f646565`（digest `sha256:09f0d367…`），8 executor × (8g+2g)/2core、driver 6g，表 200k 行 × 16 维，`IVF × RaBitQ 4bit`、`nlist=256`、`nprobe=1024`、`over-sample=4000`。驱动 `cluster/ClusterIndexAcceptance`（工具仓），每项打印一行 `CHECK n PASS/FAIL`。

| 卡上的验收项 | 结果 | 证据 |
|---|---|---|
| 1 `list_indexes` 返回全部行 | PASS | 1 行，含 `PUBLISHED`、`200000` 条、定义 JSON |
| 2 建索引成功 + `index_build_status` 终态 | PASS | `SUCCEEDED`，起止时间与 200000 行 |
| 3 计划里有索引算子 | PASS | `EXPLAIN` 出现 `VectorSearchSource[... metric=EUCLIDEAN, uncoveredFiles=0 ...]`，日志 `RewriteVectorSearch: rewrote a vector query into VectorSearchSource` |
| 4 与 **L2 全扫**逐行一致 | PASS | 索引 Top-10 与 L2 表达式 Top-10 完全相同 `[129507, 18804, 57749, 90562, 162320, 168452, 96694, 51617, 24936, 123375]` |
| 5 杀掉 executor 后仍一致 | PASS | 第 1 次迭代后 `Lost executor 2 ... deleted by a user`，其后 5 次迭代 Top-10 与 L2 真值逐行相同 |
| 6 `index_cache_status` 可读、不 OOM | PASS（附带说明） | 可读、无 OOM；返回 **0 行**，因为该路径 `driverMaterialization=true`，索引检索在 driver 侧完成，执行器没有缓存条目可报。这不是缺陷，但意味着"缓存状态"目前不能用来判断这个计划下的缓存占用 |

**验收过程中学到的两条（已写回 §2.2）**：非恒等分区（`bucket(...)`）会让改写规则让路；近似 kernel 要拿到与精确扫描一致的结果，必须把 `over-sample` 放大（默认 1 时 Top-10 与真值不同，4000 时一致）。

**本节不做的事**：没有测 100M、没有测多索引并存、没有更新集群默认配置。验收用的表与索引留在 HDFS `/user/wanganyang/iceberg-acc/warehouse` 作为证据。

### 7.5.3 10M 基准回归（2026-10-28）

目的只有一个：换了基线之后，**召回与延迟有没有退化**。用同一套 harness（工具仓 `vector-benchmark`）在同一张 `sift10m_base`（Parquet，10M × 128 维）上重建索引再测，6 个查询 × 2 轮 = 每格 12 次测量，metric 传 `euclidean`（真值按 L2 排序）。

| arm | over | 中位延迟 | 召回 |
|---|---|---|---|
| IVF·1bit | 1 / 10 / 100 | 3.4 s / 6.5 s / 6.3 s | 0.333 / 0.850 / **0.983** |
| IVF·2bit | 1 / 10 / 100 | 3.6 s / 6.3 s / 6.2 s | 0.467 / 0.917 / **0.983** |
| IVF·3bit | 1 / 10 / 100 | 3.2 s / 6.5 s / 6.6 s | 0.667 / 0.967 / **0.983** |
| IVF·4bit | 1 / 10 / 100 | 3.1 s / 6.8 s / 7.0 s | 0.867 / 0.983 / **0.983** |

与 `docs/sift-vector-index-benchmark.html`（旧基线）同一格的对比：

| arm（over=10） | 旧基线 | 回溯版 | 差异解释 |
|---|---|---|---|
| IVF·1bit | 0.883 / 7.3 s | 0.850 / 6.5 s | 召回略低、延迟略快 |
| IVF·3bit | 0.983 / 8.5 s | 0.967 / 6.5 s | 同上 |
| IVF·4bit | 0.983 / 9.6 s | 0.983 / 6.8 s | 延迟更快 |
| 天花板（over=100） | 0.983 | **0.983** | 完全一致 |

结论：**没有退化**。天花板都是 0.983（这是 `sift10m_gt` 本身的上限，两版一致），延迟同量级或更快。两三格上的 0.01–0.03 召回差有一个确定的原因：本次索引用 `nprobe=32` 建，旧报告用 `nprobe=150`（旧报告 §6 也记录过"150→32 快约 2.2 倍、recall 几乎不变"），不是基线变化带来的。

**这一卡没做的、以及为什么**：
- **Lance 臂没有跑**：回溯版不支持 Lance 文件格式（§7.3 决定），硬跑会在写路径上直接失败。旧报告的 10M Lance 行（4.4 s / 323 MB）因此没有对应数字，不能拿来对比。
- **HNSW / FLAT / 未量化臂没有跑**：这三臂在这版里不是推荐路径，10M 上旧报告已显示它们更慢（HNSW 17.4 s、暴力 12 s、未量化 46 s）。要补齐需要再排一轮集群时间，价值低。
- **100M 不重跑（显式决定）**：理由有三——10M 没有退化；100M 单次查询 8–900 s，一轮要几十小时；而且本分支的取回（fetch-back）退化为整文件读（§7.3），100M 上这个代价会被放大，属于**已知差异而不是回归**。等 #2769 之后的发布节奏与 100M 数据可用性再定；如果那时要做，先补 Parquet 的位置读再做 100M，否则测出来的数是"少了位置裁剪"的数，不是基线的数。

### 7.6 升级矩阵

| 组件 | 跟随谁升级 | 升级时要做什么 | 验证 |
|---|---|---|---|
| `core` 索引层、`index-vector`、`index-text` | 厂内 `step-1.9` 分支 | 厂内补丁更新时 rebase 到新 tip，重放 250 文件快照 | 单测 + revapi（`api/`） |
| `spark/v3.5` 集成、`index-spark` | Spark 3.5.x **和** 厂内 Iceberg | Spark 补丁版升级：重建镜像即可；Spark 主版本升级：**移植项目**（我们站在 Catalyst 内部 API 上：`injectFunction`/`injectOptimizerRule`/`injectPlannerStrategy`、`CodegenFallback` 表达式、物理算子） | 六条集群验收 |
| parquet / avro | **跟随 Iceberg runtime 内部版本**（1.15.2 / 1.12.0），不动 Spark 外部 jar | 无需动作 | 编译 + 写读 smoke |
| 镜像基础层 | 厂内 Spark 镜像 | 重建镜像、重跑验收 | 六条集群验收 |
| `IndexManifest` 二进制格式 | **我们自己**（与 upstream 无关） | 改格式必须保证读兼容旧版本 | 版本化注册表测试 |

### 7.7 基线变更后的重新验证范围

最小集合是四项：编译与单测（`core` 索引、`index-vector`、`index-text`、`spark/v3.5` 的 `*Index*`/`*Vector*`）；集群六条验收（`list_indexes` 返回全部行 → `build_vector_index` 成功且 `index_build_status` 终态正确 → `ORDER BY vec_distance` 走索引且与 L2 全扫逐行一致 → `check_indexes` 无 needs_rebuild → 杀掉一个 executor 后结果逐行一致 → `index_cache_status` 可读且不 OOM）；10M 基准回归（`iceberg-tools` 的 `vector-benchmark`，Parquet，量化 1/2/3/4bit，与本章引用的数字对比，退化必须定位到改动）；镜像重建与版本一致性检查。100M 待 10M 通过后再决定。

### 7.8 收口决定（分支、发布命名、补丁风险）

§7.1–7.5 是实测，这一节由实测推出四个决定，每个都写清"不这么做会怎样"。

**决定 A：移植方式 = 按固定快照做文件级搬运，不重放提交。** 依据是 §7.2 的冲突数据（13 个改造文件里 4 个小冲突、7 个干净）。反例有两头：重放 277 个提交，每个提交都要在 1.9.1 的上下文里解一次冲突，而同样这几个文件会被反复碰到（`build.gradle`、`settings.gradle`、`spark/v3.5/build.gradle`），冲突不是解一次就完；反过来"纯手写重建 index 包"要重写约 5000 行实现加全部测试，既无法与 fork 对账（以后 fork 前进就再也合不回来），也丢掉已经通过的 452 项测试。

**决定 B：长期维护一条 `step-1.9-index` 分支，基线跟厂内 `step-1.9`，不跟上游。** 厂内 `step-1.9` 每次前进时在该分支上 `git rebase step-1.9`，冲突面按 §7.2 预期仍集中在那 13 个改造文件；rebase 后必须重跑 §7.5 的单测矩阵。不这么做的反例是"把索引改动做成一个 250 文件的 patch 序列发给厂内"：厂内一旦改了 `step-1.9`，patch 会静默不再 apply，而没有一个分支去承接 rebase，问题要等到下次发版才暴露。**明确不做的事**：不把这条分支 rebase 到我们主仓的 `main`（1.12 基线），那等于把新基线带回 1.9.1。

**分支与已发布产物的绑定（2026-10-29 补齐）。** `step-1.9-index` 就是发布基线，**任何对外产物都必须能从它的某个提交构建出来**。这条在收口时补过一次：镜像与 10M 回归用的 jar 来自一个临时分支（比长期分支多两个提交：`version.txt=1.9.1-shb.1` 与 `#2796` 的 `check_indexes` 修复），已经把这两个提交 cherry-pick 回 `step-1.9-index`（`48b1699f2`、`6ae54c7cb`），并核对了重建 jar 与已部署 jar 的条目：23797 个条目里只有 `iceberg-build.properties`（记录 commit id）不同，其余逐字节相同。教训：**"验收用的产物"和"长期分支"一旦分叉，下次发版就会悄悄退回带缺陷的版本**。

**与主仓 `main` 的关系：不合并，只单向搬。** `main` 是 1.12 fork，与 `step-1.9-index` 的共同祖先停在 `apache-iceberg-1.9.0`，中间隔了 2600 多个提交；把 `main` 合并进来等于把 1.12 的写侧能力塞进 1.9.1 产品，正是本方案要避免的。需要复用 `main` 上的修复时，用 `git cherry-pick -x` 单独搬（`#2796` 就是这么来的），并在提交信息里留来源。

**决定 C：发布物命名由 `version.txt` 决定，取 `1.9.1-shb.N`。** 厂内 `step-1.9` 自己就是用 `version.txt` 定版本的（当前是 `1.9.1-STEP-rc3`），`build.gradle` 的 `getProjectVersion()` 先读 `version.txt`、读不到才从 git tag 推 MAJOR.MINOR+1-SNAPSHOT。所以回溯分支只需把 `version.txt` 改成 `1.9.1-shb.N`，产物自然是 `iceberg-spark-runtime-3.5_2.12-1.9.1-shb.N`，Scala 固定 `_2.12`。N 的规则：每个对外发布（镜像或仓库制品）递增 1，同一 N 不得重复发布；发布说明必须写三件事——基线 `1.9.1`（厂内 `step-1.9` 哪个 commit）、**不支持 variant**、**仅 Spark 3.5 / Scala 2.12**。不这么做的反例：靠 git tag 推版本会得到 `1.10.0-SNAPSHOT` 这种"看起来比厂内还新"的号，运维在集群里看到两个 Iceberg 版本时无法判断谁是我们的构建。

**决定 D：厂内补丁风险从"未知"降级为"有源码"，但仍要记录制品与源码的对应关系。** 源码已在 `gitlab.basemind.com/storage/iceberg` 的 `step-1.9`，不再需要"向厂内索取补丁清单"。剩下的风险是**制品与源码的对应关系没有记录**：基础镜像里那个 `1.9.1-STEP-rc1` jar 的 manifest 没有 commit 信息，而当前源码声明的版本号是 `1.9.1-STEP-rc3`，两者不是同一个构建。所以每份发布记录必须写：源码 commit、jar 的 sha256、jar 内 `pom.properties` 里的 shaded Avro/Parquet 版本。验收以厂内 jar 的实际行为为准，不以"类集合一致"为由跳过集群验收。

**附带决定：Spark 编译基线先用 catalog 里的 3.5.5，不用厂内 `3.5.7-STEP` 私有构建。** 理由：`libs.versions.toml` 里的 `spark35 = "3.5.5"` 能直接解析，厂内 3.5.7-STEP 只在厂内仓库；编译期 API 差异由集群验收兜住（若出现编译期缺符号，再把厂内 jar 装进本地 maven 并改用）。不这么做的反例是把私有构建塞进公共依赖解析，任何外部协作者（包括 CI）都无法构建。

## 8. 已知缺口（上线前必须知道）

这些不是"以后再说"，而是会影响运维决策的现状：

0. **构建“成功”不等于“全覆盖”**（2026-10-30）。`state=SUCCEEDED` 只表示本次构建跑完并发布；某个索引文件写失败时 `files_failed` 非 0，`check_indexes` 的 `uncovered_data_files` 报漏掉的数据文件数并给出 `refresh_vector_index` 建议（漏掉的文件没被覆盖过，刷新只需补段，便宜）。查询结果不受影响，只是这些文件每次被扫描。运维应把 `files_failed>0` 或 `uncovered_data_files>0` 当告警，而不是成功。注意这与“删改过的文件”不是一回事：后者在刷新时会重建它所在的组，见 §2.3 的 `refresh_*_index` 说明。

1. **镜像与构建器版本必须对齐**（#2756）。当前没有自动门禁，只靠发布清单里的 smoke。**建议在 CI 增加断言**：镜像内 `IndexManifest` 可读版本集合 ⊇ 构建器 `FORMAT_VERSION`。
2. **驻留管理未收口**（#2391，7/9）。`load_index`/`offload_index` 的语义已定（按 index 粒度、固定保护期、索引级 LRU），实现仍在推进；生产先别依赖手工驻留。
3. **nprobe 未系统扫**。旧报告 10M 用 150、100M 用 32；回溯版的 10M 回归用的是 **32**，over-sample 100 时 recall 天花板 0.983 与旧基线一致（§7.5.3），说明 32 在 10M 可用，但**没有做 32↔150 的同轮对照**，也没扫 100M。这是上线后第一个该调的旋钮，调它要按 §7.5.3 的口径同时记录 recall 而不是只看延迟。
4. **只在 Hive catalog 上验证过**。REST catalog 的元数据更新路径（`MetadataUpdate` 序列化）有单测覆盖，但没有端到端跑过；用 REST catalog 上线前要单独验收。
5. **DDL 与 `CALL` 并存**（2026-10-30 起）。`SHOW/`CREATE/`REFRESH/`DROP INDEX` 已实现并经 A 集群镜像 smoke；`CALL` 过程仍是参数最全的入口。文本索引尚无名字，`OPTIMIZE INDEX` 与短语/多列/混合检索未实现。
6. **`FLAT` 不作为生产查询手段**（它在规模上省不了开销），但它**不再被改写规则拒绝**：旧规则里"FLAT 代价等同扫描所以跳过"的判断已删除，`TestVectorIndexGuide.theRewriteUsesWhateverStructureTheIndexHas` 断言 FLAT 也会生成 `VectorSearchSource`。早先"FLAT 走 `ORDER BY` 拿不到结果"的观察对应旧行为；用户手册里"索引不能是 `FLAT`"一句已过期，改文档时以本条为准。
7. **Iceberg 表必须建在 Iceberg catalog 下**。建在会话目录（`spark_catalog`/Hive）的 Iceberg 表能建不能读，报 `iceberg is not a valid Spark SQL Data Source`；这类表上也不能建索引。见 2.1 的说明。
8. **删除表/删除行的边界**：equality delete 的多键集已支持，`timestamp_nano` 键被明确拒绝（避免同微秒多行误删）；带删除的表上建索引前请确认删除文件格式（跟随数据格式，Lance 表是 `.lance`）。
9. **取回（fetch-back）的位置读**：命中后按位置只读需要的那几页——`FormatModelRegistry.readBuilder(...).positions(...)`，格式忽略位置时整文件读一遍再按位置过滤，结果不变。
10. **Lance 数据文件只承载扁平列和它们的列表**（首版边界，不是待修 bug）：`struct`、`map`、元素为 struct 的 list 既写不进 Lance 文件也读不回来，顶层字段名也不能含点号（格式按路径解析）；建 writer/reader 时就报错并点名具体列（`LanceRowTypes`）。需要嵌套列的表请用 `'write.format.default' = 'parquet'`。这条影响验收口径：嵌套 JSON 值保真只在 Parquet 上验，Lance 侧验的是"清晰拒绝"而不是"能写能读"；写进发布清单（release manifest）的限制说明以此为准，别把 Lance 嵌套列算进本版的覆盖范围。

---

## 9. 上线检查清单（一页版）

- [ ] 镜像 tag 记录了 git commit，且与 `cluster-build.jar` 同一 commit
- [ ] §4.3 六条验收全绿，特别是 `list_indexes` 返回全部行
- [ ] 单表灰度一周：延迟、`check_indexes`、缓存占用都有数
- [ ] 缓存预算 ≥ 最大索引 ÷ 执行器数
- [ ] `over-sample` 默认值写进集群配置（建议 100），并在用户文档里说明它是什么
- [ ] 建索引用的 principal 有表元数据写权限
- [ ] 回滚方案演练过：旧镜像 + 新索引的唯一问题是读不了 v2，确认窗口内不建索引
- [ ] 基线符合 §7.1：Iceberg 取厂内 `step-1.9`；**不替换** Spark 的 parquet/avro jar（内部 shaded 1.15.2 / 1.12.0 随 runtime jar 走）
- [ ] 镜像内只有一个 Iceberg runtime，且其 sha256 与发布记录一致；`RELEASE` 里的 Spark revision 与指定源码一致（§7.5.1）
- [ ] 集群六条验收在**该镜像**上重跑过（§7.5.2），不是只跑过单测
- [ ] §7.7 的四类验证都重跑过（基线变更会让旧的召回/延迟数字失效）
- [ ] 10M 回归与旧报告对比过（§7.5.3）；要发 100M 就先补 Parquet 位置读，再测
- [ ] 监控采集 `index_cache_status` 与查询 P99
- [ ] 用户文档按 `CALL` 过程写，附"参数怎么选"表（§2.1）

---

**参考**：基准报告 `docs/sift-vector-index-benchmark.html`（10M/100M × Parquet/Lance × HNSW/IVF/暴力 × 1/2/3/4bit × over-sample）；用户过程手册 `docs/docs/spark-procedures.md`；改写规则 `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/RewriteVectorSearch.scala`；索引格式 `core/src/main/java/org/apache/iceberg/index/IndexManifest.java`；集群侧工具（建索引入口、镜像、测量）在并列的工具仓 `iceberg-tools`。
