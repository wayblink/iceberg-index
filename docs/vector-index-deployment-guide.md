# 向量索引上线与运维手册（Spark 3.5 / Iceberg）

面向系统运维与平台方：这套东西怎么装、用户怎么用、发布走哪条路、出问题怎么处置。

**事实基线**：仓库 `~/workspace/iceberg`（fork 自 Apache Iceberg，版本号沿用），验证集群为 shai 上的 k8s Spark 3.5.7（`spark.kubernetes.namespace=spark`），验证用镜像是 `hub.i.basemind.com/spark/sparkann-e2e:20261023b`。本文标注了哪些结论来自实测、哪些仍是建议——**"建议"部分在采纳前需要评审**。

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

没有 `CREATE INDEX` 这样的 DDL——**全部通过 `CALL` 过程**。这是有意的：索引参数多、且需要回显构建结果，过程比语法更好演进。

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

### 2.2 查询

```
SELECT id, title
FROM db.docs
ORDER BY vec_distance(embedding, array(0.12d, 0.98d, ...))
LIMIT 10;
```

改写规则命中后，这次查询变成"索引检索候选 → 回表取行 → 精确重排 → 取前 k"。命中条件（`RewriteVectorSearch`）：

1. 该列上存在向量索引；
2. 至少有一个数据文件被索引覆盖（未覆盖的部分仍会残余扫描，所以索引落后于快照也能用）。

**FLAT 也参与改写。** 早先的规则因为"FLAT 每个条目都要打分、代价等同扫描"把精确索引排除在改写之外；现在这条成本判断已被移除——能不能用索引只看正确性，不看代价（`TestVectorIndexGuide.theRewriteUsesWhateverStructureTheIndexHas` 断言了 `EXPLAIN` 里出现 `VectorSearchSource`）。所以 `ORDER BY` 自动改写对 FLAT 也生效，只是它省不了多少开销：候选就是全部条目，重排后仍然是全量打分。用户手册里"索引不能是 FLAT"的说法属于旧行为，以本条为准。

一个列上有多个索引时，`ORDER BY` 里没地方写索引名字，必须用配置指定，否则规则不动手（避免猜）：

```
SET spark.iceberg.index.vector.name = docs_ivf_1bit;
SET spark.iceberg.index.vector.over-sample = 100;
```

`over-sample` 的含义是"向索引要 k×over 个候选，再取前 k"。对 `ORDER BY` 的自动改写，取前 k 之前会**精确重排**，所以放大倍数提高的是**召回**；对 `CALL vector_search` / 两个向量 TVF，结果合同是"索引自己的原始分数、按索引顺序取前 k、删掉的行以 `row = null` 保留"，**不精确重排、不扫未覆盖文件**，因此放大倍数只扩大候选范围、不会让结果变成"精确 Top-K"（近似 kernel 扩大范围后候选集本身可能变，但不保证与旧 `C=k` 逐条一致）。

它是唯一在查询时能调的召回/延迟旋钮：一次设成 100 是安全的默认（实测 10M/100M 的 1bit 都要 100 倍候选才追平精确召回）。未设置时用兼容基线 1，并在计划里标成 `COMPAT_DEFAULT(no-calibrated-policy)`——那不是"已-calibrated 的最优值"。显式设置（包括显式写 1）一律保留；空串、非整数、0、负数会报错而不是被静默改成 1。

**注意度量**：`vec_distance` 默认算余弦，只有在被改写命中时才特化成索引的 metric。所以"手写一个不走索引的对照查询"时，`vec_distance(...) + 0.0d` 这种写法其实是余弦排序——若索引是 EUCLIDEAN，两者排序不同，会出现"精确索引也差一行"的假象。做对照必须自己写 L2 表达式。

### 2.3 运维过程（都是 `CALL`）

| 过程 | 用途 | 什么时候用 |
|---|---|---|
| `list_indexes(table=>...)` | 列出索引与状态 | 日常巡检；**失败要当故障处理**（见 §6） |
| `check_indexes(table=>...)` | 体检：是否需要重建 | 定期跑；返回可粘贴的 `refresh_vector_index(...)` 建议 |
| `index_build_status(...)` | 构建进度 | 大表构建期间；状态落文件，随时可查 |
| `index_cache_status()` | 各执行器缓存占用/覆盖 | 排查"为什么某次查询慢" |
| `refresh_vector_index(...)` | 增量/重建 | 数据变更后；索引落后不阻塞查询，但覆盖率下降会变慢 |
| `load_index` / `offload_index` | 手动驻留/卸载 | 见 §5.3；**当前仍在完善**（§7） |
| `cleanup_index_files(...)` | 清理孤儿文件 | 构建失败或重建后的收尾 |
| `drop_vector_index(...)` | 删索引 | 下线/换参数 |

索引注册表位置由表属性 `index.registry` 指定（如 `indexmeta.v3-4c8c1719.json`），是版本化文件——**不要手工改这个属性或这些文件**；回退单文件布局（`index-registry.json`）只在属性丢失时由程序自己降级。

---

## 3. 上线形态：合不合厂内仓库

这是运维最需要拍板的一件事。三个选项，实测与推理如下。

| 选项 | 做法 | 优点 | 代价 | 判断 |
|---|---|---|---|---|
| **A. 打进 Spark 镜像**（当前做法） | 基础镜像上删掉 vendor 的 `iceberg-spark-runtime`、`parquet-*`、`avro-*`，把我们的 shaded runtime jar 放进 `/opt/spark/jars` | 不改厂内 Spark；一个 jar 覆盖全部；已验证可跑 | 镜像与代码版本强绑（#2756 就是这么来的）；每次升级重建镜像 | **短期采用** |
| **B. 合入厂内 Iceberg fork** | 把 `core/index-*`、`spark/v3.5` 的改动作为 fork 的常规分支维护 | 版本可追溯、可发版、CI 覆盖 | 需与上游保持 rebase；需要 revapi 门禁 | **中期采用**（本质上仓库已经是 fork，正式化即可） |
| **C. 合入厂内 Spark 仓库** | 修改 Spark 内核 | —— | 我们只注入 session extension、函数、一条 optimizer rule 和一个 planner strategy，**不修改 Spark 任何代码** | **不做**，没有理由 |

**结论**：A 现在用，B 收口长期，C 明确否掉。理由是"只要不改 Spark 内核，就不要碰 Spark 的发布节奏"——我们与 Spark 的契约只是 `spark.sql.extensions` 注入点，这个契约稳定且不需要 fork。

**为什么必须删 vendor 的 jar**（不是可选优化）：厂内镜像的 `/opt/spark/jars` 在 JVM 自己的 classpath 上，`iceberg-spark-runtime-1.9.1` 会遮蔽我们的 jar，导致 session extension 解析到 1.9.1 那份、`vec_distance` 不存在、所有查询退化成扫描；`parquet-1.13.1` 也无法服务后续需要的 parquet-variant。用 `spark.jars` 传递不行：child-first loader 加载不到 session extension。所以只能烘进镜像并删除被替换的 jar。

### 3.1 版本与产物命名

- 仓库版本号沿用 Iceberg 的（回溯到 1.9.1 基线后是 `1.9.1` 一线），**不要另起版本号体系**，否则依赖解析与 revapi 都会乱；对外用带来源的后缀区分（见下一条）。
- Scala 版本必须与厂内 Spark 发行版一致：`_2.12`（仓库默认 `defaultScalaVersion=2.12`，厂内 Spark 也是 2.12）。
- 发布产物只有一个：`iceberg-spark-runtime-3.5_2.12-<version>.jar`（shadow jar，已包含 `iceberg-index-vector`、`iceberg-index-text`、`iceberg-index-spark`）。
- **版本号不得伪装成上游版本**：它是「上游 1.9.1 + 索引能力」，公开发布时用带来源的后缀（如 `1.9.1-shb.1`），并在发布说明里写明基线是 1.9.1、不支持 variant 类型（见 §7.1）。
- 镜像 tag 建议 `<日期><序号>`（如 `20261027a`），并在镜像 LABEL 里写入 jar 的 git commit，便于回答"这个镜像到底是哪版"。

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
# parquet / avro 跟随 Spark 自带版本，不再替换（见 §7.1）
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

`spark.iceberg.index.cache.enabled`（默认 true）、`spark.iceberg.index.cache.bytes`（每个执行器的预算，默认按工作集与堆推导）、`spark.iceberg.index.cache.mode`。经验：**索引装不进缓存时会退化成"每次查询读自己的索引文件"**，100M 的 HNSW（23.7 GB / 6 执行器）就是这样废掉的——它的延迟等于全表扫描。所以缓存预算要按"最大索引 ÷ 执行器数"给，宁多勿少。

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

这一节是部署前必须拍板的部分，结论已经确定，依据和代价写在这里以便复核。

### 7.1 版本基线（已决定）

厂内 Spark 主版本是钉死的（3.5.7，随带 Iceberg 1.9.1），因此**不跟随上游 Iceberg 升级**：把索引能力回溯到厂内 1.9.1 基线，而不是把集群的 Iceberg 运行时从 1.9.1 跳到 1.12。

**为什么不是"直接发一个 1.12 的 jar 换进去"。** 换 jar 换的是**整个集群的 Iceberg 运行时**，不只是带索引的表：写侧会获得 1.10–1.12 的新能力（本 fork 的 `SUPPORTED_TABLE_FORMAT_VERSION = 4`、`MIN_FORMAT_VERSION_ROW_LINEAGE = 3`），一旦某张表被写成 format-version 3，跑 1.9.1 的其它读方会直接读不了。这个风险与索引功能无关，不该被"装索引"顺带引入。

**厂内 1.9.1 的实测核对**（从基础镜像 `spark:3.5.7-STEP-rc2-tpcds-dsdgen-lance-20260416a` 取出 jar 比对）：

| 项 | 结果 |
|---|---|
| jar 名 | `iceberg-spark-runtime-3.5_2.12-1.9.1-STEP-rc1.jar`（44 MB，厂内自建） |
| 是否带索引能力 | **否**：`org/apache/iceberg/index/` 包计数 0 |
| 类集合 vs 上游 tag `apache-iceberg-1.9.1` | **完全一致**：22997 个 class 中，排除 shaded guava（`relocated`）、`shaded`、Immutables 生成的 `Immutable*` 与 `package-info` 后，厂内独有类为 **0** |
| `META-INF/MANIFEST.MF` | 只有 `Manifest-Version: 1.0`，无版本/commit 信息 |

结论：厂内 1.9.1 是**同名重建**，没有新增功能或 API。但类名一致**不能排除方法体被改**；要排除这一点需要本地构建上游 1.9.1 再逐类 diff 字节码，目前未做——因此验收以"厂内 jar 的实际行为"为准，源码基线用 tag。

**parquet / avro 基线：用 Spark 自带版本（1.13.1 / 1.11.4）。** 依据不是偏好，而是厂内现状：

| | 上游 1.9.1 编译基线 | 厂内镜像实际 | 我们采用 |
|---|---|---|---|
| parquet | 1.15.2 | **1.13.1** | 1.13.1 |
| avro | 1.12.0 | **1.11.4** | 1.11.4 |

也就是说厂内的 1.9.1 runtime jar 本来就是"编译于 1.15.2、跑在 1.13.1 上"。选这个基线不是引入新风险，而是与厂内既有组合保持一致，镜像里不再需要替换 parquet/avro。

顺带一个原先以为必须处理的问题自动消失：**1.9.1 不使用 Parquet 的 VARIANT 逻辑类型**。`git grep variantType(` 在 tag `apache-iceberg-1.9.1` 的整个 `src/main` 里为 0 处；1.9.1 的 `TypeToMessageType.variant()` 只建普通 group（`metadata`/`value`/可选 `typed_value`）。我们用到的 `.as(LogicalTypeAnnotation.variantType(...))` 是后来才引入的，只存在于当前 1.12 基线的 fork 里。所以回溯之后**不需要为 variant 打任何补丁**。

待验：`iceberg-core` 把 avro 当 `api` 级依赖，1.9.1 原基线是 1.12.0；用 1.11.4 编译能否通过、manifest 读写是否正常，是回溯后第一个必须跑通的关口。

### 7.2 回溯计划与工作量

索引功能在 fork 里的落点（实测清点）：

| 部分 | 规模 |
|---|---|
| `core/src/main/java/org/apache/iceberg/index/` | 28 个文件、约 4956 行 |
| `index-spark` / `index-vector` / `index-text` 三个模块 | 新增模块 |
| `spark/v3.5` 的索引与过程（`spark/index`、`procedures/*Vector*`、`*Index*`） | 41 个文件 |
| 被改动的**既有**上游文件 | **3 个**：`api/.../FileFormat.java`（+2/−1）、`core/.../formats/FormatModelRegistry.java`（+26/−11）、`core/.../formats/ReadBuilder.java`（+20） |

那 3 个既有文件的改动都是 **format-model / Lance 相关**（`FileFormat.LANCE`、`LanceFormatModels` 注册、按格式的读取构建），不是索引逻辑；最小回溯可以先把 Lance 那条线留在后面。

顺序建议：①在 1.9.1 基线上把 parquet 1.13.1 / avro 1.11.4 的编译跑通（不含索引）→ ②移植 `core` 的 index 包 → ③加三个模块 → ④移植 `spark/v3.5` 集成（函数、改写规则、物理策略、16 个过程）→ ⑤补测试。

主要风险：索引代码可能用到 1.9.1 之后才有的上游 API。目前看常用工具都在（`Tasks`、`CloseableIterable`、`Preconditions`），但 `MetadataUpdate`、`TableMetadata`、`TableOperations` 的新增能力需要逐个确认——这是移植阶段最容易卡住的地方。

### 7.3 升级矩阵

| 组件 | 跟随谁升级 | 升级时要做什么 | 验证 |
|---|---|---|---|
| `core` 索引层、`index-vector`、`index-text`、`iceberg-lance` | Iceberg | 换基线、重放索引改动（3 个既有文件 + 新目录） | 单测 + revapi（`api/`） |
| `spark/v3.5` 集成、`index-spark` | Spark 3.5.x **和** Iceberg | Spark 补丁版升级：重建镜像即可；Spark 主版本升级：**移植项目**（我们站在 Catalyst 内部 API 上：`injectFunction`/`injectOptimizerRule`/`injectPlannerStrategy`、`CodegenFallback` 表达式、物理算子） | 六条集群验收 |
| parquet / avro | **Spark 自带版本**（不独立升级） | 无需动作 | 编译 + 写读 smoke |
| 镜像基础层 | 厂内 Spark 镜像 | 重建镜像、重跑验收 | 六条集群验收 |
| `IndexManifest` 二进制格式 | **我们自己**（与 upstream 无关） | 改格式必须保证读兼容旧版本 | 版本化注册表测试 |

### 7.4 基线变更后的重新验证范围

基线一换，之前所有集群结论（召回、延迟、索引体积）都失效，必须重跑。最小集合：

1. 编译与单测：`core`（索引）、`index-vector`、`index-text`、`spark/v3.5`（`*Index*`、`*Vector*`）。
2. 集群六条验收：`list_indexes` 返回全部行 → `build_vector_index` 成功且 `index_build_status` 终态正确 → `ORDER BY vec_distance` 走索引且与 L2 全扫逐行一致 → `check_indexes` 无 needs_rebuild → 杀掉一个 executor 后结果逐行一致 → `index_cache_status` 可读且不 OOM。
3. 基准回归：用 `iceberg-tools` 仓库的 `vector-benchmark` 跑 10M 网格（Parquet + Lance，量化 1/2/3/4bit），确认延迟与召回相对本手册引用的数字没有退化；100M 待 10M 通过后再决定。
4. 镜像：重建（此时 `Dockerfile` **不再需要**删除 parquet/avro，只需删除 vendor `iceberg-spark-runtime` 并放入我们的 jar），并按工具仓 `iceberg-tools` 的 `cluster/README.md` 里的版本一致性检查复核。

## 8. 已知缺口（上线前必须知道）

这些不是"以后再说"，而是会影响运维决策的现状：

1. **镜像与构建器版本必须对齐**（#2756）。当前没有自动门禁，只靠发布清单里的 smoke。**建议在 CI 增加断言**：镜像内 `IndexManifest` 可读版本集合 ⊇ 构建器 `FORMAT_VERSION`。
2. **驻留管理未收口**（#2391，7/9）。`load_index`/`offload_index` 的语义已定（按 index 粒度、固定保护期、索引级 LRU），实现仍在推进；生产先别依赖手工驻留。
3. **nprobe 未系统扫**。10M 只测了 150、100M 只测了 32；已有独立测量显示 10M 从 150 降到 32 还能快约 2 倍而 recall 几乎不变——**默认值很可能偏保守**，这是上线后第一个该调的旋钮。
4. **只在 Hive catalog 上验证过**。REST catalog 的元数据更新路径（`MetadataUpdate` 序列化）有单测覆盖，但没有端到端跑过；用 REST catalog 上线前要单独验收。
5. **无 DDL**。所有索引操作是 `CALL` 过程，用户文档要按过程写，不要承诺 `CREATE INDEX`。
6. **`FLAT` 不作为生产查询手段**（它在规模上省不了开销），但它**不再被改写规则拒绝**：旧规则里"FLAT 代价等同扫描所以跳过"的判断已删除，`TestVectorIndexGuide.theRewriteUsesWhateverStructureTheIndexHas` 断言 FLAT 也会生成 `VectorSearchSource`。早先"FLAT 走 `ORDER BY` 拿不到结果"的观察对应旧行为；用户手册里"索引不能是 `FLAT`"一句已过期，改文档时以本条为准。
7. **删除表/删除行的边界**：equality delete 的多键集已支持，`timestamp_nano` 键被明确拒绝（避免同微秒多行误删）；带删除的表上建索引前请确认删除文件格式（跟随数据格式，Lance 表是 `.lance`）。

---

## 9. 上线检查清单（一页版）

- [ ] 镜像 tag 记录了 git commit，且与 `cluster-build.jar` 同一 commit
- [ ] §4.3 六条验收全绿，特别是 `list_indexes` 返回全部行
- [ ] 单表灰度一周：延迟、`check_indexes`、缓存占用都有数
- [ ] 缓存预算 ≥ 最大索引 ÷ 执行器数
- [ ] `over-sample` 默认值写进集群配置（建议 100），并在用户文档里说明它是什么
- [ ] 建索引用的 principal 有表元数据写权限
- [ ] 回滚方案演练过：旧镜像 + 新索引的唯一问题是读不了 v2，确认窗口内不建索引
- [ ] 基线符合 §7.1：Iceberg 取厂内 1.9.1、parquet/avro 取 Spark 自带版本；镜像不再替换 parquet/avro
- [ ] §7.4 的四类验证都重跑过（基线变更会让旧的召回/延迟数字失效）
- [ ] 监控采集 `index_cache_status` 与查询 P99
- [ ] 用户文档按 `CALL` 过程写，附"参数怎么选"表（§2.1）

---

**参考**：基准报告 `docs/sift-vector-index-benchmark.html`（10M/100M × Parquet/Lance × HNSW/IVF/暴力 × 1/2/3/4bit × over-sample）；用户过程手册 `docs/docs/spark-procedures.md`；改写规则 `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/RewriteVectorSearch.scala`；索引格式 `core/src/main/java/org/apache/iceberg/index/IndexManifest.java`；集群侧工具（建索引入口、镜像、测量）在并列的工具仓 `iceberg-tools`。
