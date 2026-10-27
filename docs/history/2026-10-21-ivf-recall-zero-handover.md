# 交接：IVF × RaBitQ 在真实规模上 recall = 0

> **归档说明（2026-10-27 入库）**：本文是 2026-10-21 的排障交接记录，用一个已定位并修复的缺陷说明
> 「recall = 0」的根因与排除过程：命中集在改写规则里按 score 降序重排，对 EUCLIDEAN（越小越近）等于
> 把最差候选排到前面。修复后这段逻辑已被阶段A的物理执行路径取代（`VectorSearchExec` 负责候选与回表，
> `RewriteVectorSearch` 只生成计划），文中引用的 `liveHitRows`、`searchLimit` 等符号已不存在。
> 保留它是为了留下「怎么排除 kernel/量化/nprobe」的判断依据，不要按文中行号去改现在的代码。
>
> 原文放在仓库根目录且长期未跟踪，本次随多 agent 收口一并入库。


> ## 已定位（2026-10-21，接手会话）
>
> **根因不在 `IvfKernel`，也不在分布式归约、量化或 nprobe。** 在 Spark 查询改写规则
> `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/RewriteVectorSearch.scala`
> 的 `liveHitRows`：命中集取回后被按 **score 降序** 重排，再 `take(limit)`。
>
> score 的方向由 metric 决定：COSINE/DOT 是“越大越近”，EUCLIDEAN 是“越小越近”。所以对
> EUCLIDEAN 而言，这段重排其实是**把最差的候选排到了前面**，于是 over-sample > 1 时
> （`searchLimit = limit × over`）最终返回的是那 100 个候选里**最远的 10 个**。
>
> 这解释了 §4 的全部现象：
> - `recall` 恒为 **0.0000** —— 返回的行与正确答案本来就无关（不是空断言）；
> - nprobe 200/1000、over-sample 500 都无效 —— 好候选在排序这一步就被丢掉了；
> - 单 index 文件也 0 —— 与归并无关；
> - IVF×NONE 精确打分也 0 —— 与量化无关；
> - 早前 HNSW 的 98.25% 是 over-sample=1（当时还没有这个配置，默认 1）时测的 —— 那时
>   `searchLimit == limit`，被重排的正好就是全部候选，所以看不出问题；
>   **如果当时给 HNSW 也设 over-sample=10，它会同样掉到 0。**
> - 现有测试全部走 `vector_search` 过程或默认 COSINE 的 4 行小表，两种都不经过
>   “over-sample > 1 + EUCLIDEAN + 候选数 > limit” 这条路。
>
> 本地复现（真实 SIFT20k，单 index 文件，经**改写规则**那条 SQL）：
>
> | 设置 | 修复前 | 修复后 |
> |---|---|---|
> | IVF × NONE，over-sample=10 | **0.000** | **0.998** |
> | IVF × RABITQ(3bit)，over-sample=10 | **0.000** | **0.856** |
> | 两者，over-sample=1 | 0.998 / 0.856 | 0.998 / 0.856 |
>
> 确定性的回归测试：`TestRewriteVectorSearch.overSamplingKeepsTheNearestCandidatesRatherThanTheFarthest`
> （400 行、IVF nlist=4/nprobe=4 全访问 ⇒ 候选集精确，修复前返回 90..99，修复后返回 0..9）。
> **集群重测已完成（镜像 `sparkann-e2e:20261021a`，含修复）**：1M `recall=0.8500`（原 0.0000，1.63× 加速）；
> 10M `recall=0.7000`（原 0.0000，nprobe=50，median 7.82 s vs 扫描 16.93 s，2.16×），
> 索引 630 MB / 63 B per entry；nprobe 50→150 与 over-sample 10→100 都不提高 recall。
>
> **第二个 bug 已定位并修复（`51469127a`）**：`liveHitRows` 在精确重排前 `take(limit)` 把候选集截断了，
> 于是 over-sample 拉来的候选全被丢掉，0.70 其实是“3 bit 码自己的 top-10 准确率”，不是量化天花板。
> 修复后（镜像 `20261021b`，不需重建索引）：10M `recall=0.9833`、1M `recall=0.9875`。详见 §3.4。
>
> §9 的实验全部作废；“§12 直觉”也不成立——payload 的行引用是对的。

> 交接时间：2026-10-20 · 交接人：pi（当前会话）· 面向：接手分析的模型
> 相关卡片：`#1632`（本次验证）· `#1486`（量化器）· `#1485`（IVF）· `#1575`（过采样参数）

## TL;DR

> **（已被 §0 推翻）** 本节的结论是“根因未定位”。实际根因是 Spark 改写规则里的排序方向，见文首。

在 Apache Iceberg 的索引层里新增了 **IVF 分区结构** 和 **RaBitQ 量化**，构建侧全部验证通过、
体积和抖动收益都实测到了。但**查询结果的 recall 在集群规模上恒为 0**（返回的行与正确答案完全无关）。

已修掉一个真 bug（码的位累积），但**根因仍未定位**。已排除的假设清单在 §6，按信息量排序的
下一步实验在 §9。**请先看 §6 和 §9，然后直接怀疑 §7 列出的代码。**

---

## 1. 项目背景

这是一个 Apache Iceberg 的分支，在 `core` 之上加了一层**引擎中性的索引层**：

- **通用索引层**（`core/.../index/`）：索引元数据、快照、manifest、侧车目录（`<table>/indexes/`）、
  分布式检索与归约。
- **适配器层**：
  - `index-vector/`：向量索引（结构 × 量化 × kernel）
  - `index-text/`：BM25 文本索引
  - `index-spark/`：Spark 实现（`DistributedIndexSearch`、`IndexEntries` 等）
- **Spark 集成**（`spark/v3.5/`）：TVF、过程（`build_vector_index` 等）、**查询改写规则**
  （`ORDER BY vec_distance(col, q) LIMIT k` → 直接返回索引命中的行）。

关键不变量（改动必须守住）：

| 编号 | 内容 |
|---|---|
| D29 | 分布式检索的单位是**索引文件**；一个文件整体读、整体搜；并行度 = 索引文件数 |
| D33 | `max_entries_per_index_file` 只**合并**数据文件、永不拆分 ⇒ 一个 group 就是一个或多个完整数据文件 |
| D30 | 归约是有界 top-k：每个 task 保留自己文件的 `topK+1`，树形 `takeOrdered` 归约，不是全量回 driver |
| — | 条目的 `rowPosition` = **文件内行序号**，已用 Iceberg `_pos` 交叉验证（#1099） |
| — | `IndexAdapter.resultComparator()` 是 SPI 契约：按 score（EUCLIDEAN 升序 / 其余降序）+ dataFile + rowPosition 破平 |

## 2. 本次会话做了什么（都已提交、测试绿）

| commit | 内容 |
|---|---|
| `55c24976b` | 定义里把 `IndexMode{EXACT,HNSW}` 拆成 `VectorStructure{FLAT,HNSW,IVF}` × `VectorQuantizer{NONE,RABITQ}` |
| `64c003bf1`(被他人带入) / — | 算法参数（`max-m`/`ef-*`）从定义字段搬到 `HnswOptions` / properties |
| `cc970e806` | blob 类型去掉算法名（`iceberg-vector-auxiliary-v1`），算法进 payload 头；`readableBlobTypes()` |
| `cbef2e861` | **`VectorKernel` SPI**：`BruteforceKernel` / `HnswlibKernel` 从 655 行的适配器里搬出来 |
| `c1331c639` | **`IvfKernel`**（per-index-file IVF，`nlist` 按组大小推导，k-means 确定性训练） |
| `e43c550e0` | **`RaBitQQuantizer`**（1 bit/维 + 3 个浮点/向量） |
| `96ad03d1` | `rabitq-bits` 做成索引参数 |
| `9813ae723` | `spark.iceberg.index.vector.over-sample` 参数化 |
| `916442800` | **IVF × RaBitQ 接入**（每列表自己的中心，payload format 2） |
| `f16c0f96c` | 选项跨两条轴；over-sample 同时读 SparkConf 与 SQLConf |
| `925f8edb2` | **修掉码的位累积 bug**（见 §5.1） |
| `925f8edb2` 之后 | 本地测试全部通过；集群 recall 仍 0 |

## 3. 测得的收益（这些是可信的）

### 3.1 体积与构建（SIFT10M，39 个数据文件 1:1）

| | HNSW × NONE | IVF × RaBitQ（3 bit, nprobe=50） |
|---|---|---|
| 构建耗时 | 1733 s | **452 s** |
| **索引体积** | **2.19 GB**（826 B/条目） | **601 MB**（60 B/条目）= **3.6× 小** |
| 覆盖 | 10,000,000 | 10,000,000（PUBLISHED） |

> ⚠️ 早前曾报 164 MB / 13.7×——那是 §5.1 那个 bug 的产物（位累积让码几乎全 1、压缩率虚高）。
> 真实数字是 601 MB / 3.6×。

### 3.3 修复后在集群上重测（2026-10-21，镜像 `sparkann-e2e:20261021a`）

代码 = 当前 main（含修复 `9c8276736`）。构建：`STRUCT=IVF QUANT=RABITQ OPTS="'rabitq-bits','3','nprobe','<n>'"`；
探针：`OVER=<over> ./probe10m-submit.sh 10 6 3`（10M）/ `OVER=10 ./probe1m-ivfq.sh 10 8 3`（1M）。

**1M（28 个 index 文件，nprobe=50）**：recall **0.8500**（修复前 0.0000），索引体积 62 MB（62 B/条目），
index median 1.68 s（max 4.31）vs scan 2.73 s，**1.63×**，7/8 次更快。

**10M（39 个 index 文件，nlist=512/文件）**：索引体积 **630 MB（63 B/条目）**，与修复前一致。

| nprobe | over-sample | recall@10 | index median | index min/max | scan median | 加速比 |
|---|---|---|---|---|---|---|
| 50 | 10 | **0.7000** | 7.82 s | 6.56 / 9.54 | 16.93 s | 2.16× |
| 150 | 10 | **0.7000** | 8.23 s | 7.11 / 8.62 | 16.25 s | 1.97× |
| 150 | 100 | **0.7000** | 8.08 s | 7.50 / 10.33 | 16.94 s | 2.10× |

要点：

- recall 从 0 变成有意义的数字，**构建耗时 452 s、体积 63 B/条目都和修复前一致**（这个 bug 只影响查询侧）。
- 抖动收窄依旧成立：max 9.54 s（修复前同一配置报 15.1 s，HNSW 报到 37.2 s）。注意这几次运行之间
  代码不同（缓存/调度那批提交现在也在镜像里），所以延迟**不能**和 §3.2 逐格对比。
- **3 bit 的 recall 天花板是 0.70**：nprobe 50→150（覆盖 10%→29%）、over-sample 10→100（候选
  100→1000）都不动它。也就是说丢掉的那 30% 不是“没扫到”，而是**码的排序能力**——真邻居在估计距离上
  掉到了 1000 名之外。下一步该动的是量化（位数/每列表中心/残差编码），不是 nprobe。

### 3.4 第二个 bug：over-sample 的候选在重排前被截断（commit `51469127a`）

诊断方法：在集群上直接对比“改写规则返回的 pk”与“精确前 30 名 + 距离”（`IvfDiagnose2`）。

- 返回的 10 行与精确答案的距离差在小数第三位：q0 命中精确第 1–8 名，却用第 11、18 名代替了第 9、10 名。
  **不是并列（tie）、也不是没扫到，而是排序错了。**
- 把 over-sample 从 10 改到 100，**返回的 pk 完全一样**。这说明 over-sample 根本没起作用。

根因：`liveHitRows` 末尾 `Some(rows.take(limit))` 在**调用方精确重排之前**就把候选集截回 `limit`。
所以 over-sample 拉来的候选被读完、解析完行、然后丢弃，精确重排只能在“kernel 估计的前 limit 个”里
换顺序，永远无法把估计排第 11 的真邻居提上来。修复：直接返回全部活行，由调用方排序取 top-k。

修复后（同样镜像流程，`20261021b`，**不需要重建索引**）：

| 表 | nprobe | over-sample | recall@10 | index median | scan median | 加速比 |
|---|---|---|---|---|---|---|
| 10M | 150 | 10 | **0.9833** | 6.82 s | 16.69 s | **2.45×** |
| 10M | 150 | 100 | **0.9833** | 7.14 s | 16.36 s | 2.29× |
| 1M | 50 | 10 | **0.9875** | 1.55 s | 2.27 s | 1.47× |

剩下那 1/60 的 miss 是 3 bit 码真正的上限（over-sample 100 ≡ 10，说明它已经在码的 top-100 之外），
要再提高只能动量化的分辨率（位数/残差编码），而不是 nprobe 或 over-sample。

### 3.2 延迟与抖动（10M，HNSW 基线 vs IVF×RaBitQ）

> ⚠️ 下表是**修复前**（bug 仍在、且缓存/调度那批提交还不在镜像里）测的，只留作历史记录；
> 当前数字见 §3.3。

| 指标 | HNSW | IVF × RaBitQ |
|---|---|---|
| min | 8.9 s | 4.9 s |
| median | 9.3 s | 8.4 s |
| p75 | 15.9 s | 9.5 s |
| **max** | **37.2 s** | **15.1 s** |
| 全表扫描 median | 18.1 s | 16.1 s |

**抖动确实收窄了**（max 37.2 → 15.1 s），这正是"把索引从 2.19 GB 减到 601 MB 就不再是 I/O 瓶颈"
的预期效果。

## 4. 问题：recall 恒为 0

`ClusterSift10mProbe` 的 `recall` = 索引 top-10 与**精确全表扫描** top-10 的交集比例。实测：

| 表 | 布局 | 结构 × 量化 | 关键参数 | recall |
|---|---|---|---|---|
| `sift10m_base` | 39 文件 | IVF × RABITQ | nprobe=50/512, over=10 | **0.0000** |
| `sift10m_base` | 39 文件 | IVF × RABITQ | nprobe=50/512, over=10（修复后） | **0.0000** |
| `sift1m_base` | **1 文件** | IVF × RABITQ | nprobe=50/1000, over=10 | **0.0000** |
| `sift1m_base` | **1 文件** | IVF × RABITQ | **nprobe=200/1000**, over=10 | **0.0000** |
| `sift1m_base` | **1 文件** | IVF × RABITQ | nprobe=50/1000, **over=500** | **0.0000** |
| `sift1m_base` | **1 文件** | **IVF × NONE（精确打分）** | nprobe=50/1000, over=10 | **0.0000** |

**单文件也 0**（归并是平凡的）⇒ 与分布式归约无关。
**精确打分也 0** ⇒ 与量化无关。
**nprobe 200/1000（20% 覆盖）和 over-sample 500（5000 候选）都无效** ⇒ 不是覆盖不足、不是排序精度。

### 顺带测到：`IVF × NONE` 比全表扫描慢 6.5 倍（16.4 s vs 2.5 s）
因为 1M 的向量列表 = 524 MB，每查询全载入。⇒ **IVF-Flat 在单查询场景不可行**，唯一可行路径是量化版
⇒ 这个 bug 必须解掉。

## 5. 已修复的真 bug（供参考它暴露问题的形态）

### 5.1 码的位累积（commit `925f8edb2`）

`RaBitQQuantizer.encode` 里的 `setLevel` **只置位、从不清零**。而
`IvfKernel.writeQuantizedList` **复用同一个 `byte[] code` 写整个列表**：

```java
byte[] code = new byte[listQuantizer.codeBytes()];      // 每个列表只创建一次
for (int member : members) {
  listQuantizer.encode(vectors[member], code, 0, factors);   // 只置位
  out.write(code);                                           // ⇒ 第 N 个码 = 前 N 个的按位或
}
```

修复：`encode` 开头 `Arrays.fill(code, offset, offset + codeBytes(), (byte) 0)`。

**它只解释了一部分现象**：修复后本地 `narrow recall` 从 0.170 → **1.000**，
但集群上仍是 0 ⇒ **还有一个独立的、规模相关的 bug**。

## 6. 已排除的假设（每条都有实测依据）—— 这是本文档最有价值的部分

| # | 假设 | 排除依据 |
|---|---|---|
| 1 | RaBitQ 估计器/数学错 | 128 维单测（`TestRaBitQQuantizer`）：估计值与真实距离 **correlation 0.9918**，平方距离误差 **5.3%(1bit)/1.6%(3bit)**，`真top10 ⊂ 估计top100 = 1.000`@3bit |
| 2 | **量化本身导致** | **`IVF × NONE`（列表里存原始向量、精确打分）在集群上也是 recall 0** |
| 3 | **nprobe 太小** | **nprobe=200 / nlist=1000（20% 覆盖）仍 0** |
| 4 | 排序精度不够 | `over-sample=500`（5000 个候选）仍 0 |
| 5 | 跨文件覆盖退化（每文件 10% ⇒ 全局 0.1^10） | **1M 单文件**（归并平凡）也 0 |
| 6 | 分布式归约 / `takeOrdered` / `resultComparator` | 同上，单文件也 0 |
| 7 | 属性没进定义 | `list_indexes` 确认 `{"nprobe":"200","rabitq-bits":"3"}` |
| 8 | 镜像/索引过期 | 镜像 `sparkann-e2e:20261020d` 已重建推送；索引已按新代码重建（build 452 s） |
| 9 | 位累积（§5.1） | 已修，且 `IVF × NONE` 不走量化同样 0 |
| 10 | 一个 task 起 Spark job / 嵌套读取 | 没有做 task 内回表，回表仍是 driver 侧一个 Spark job |
| 11 | 位置语义（rowPosition）总体错 | #1099 已用 Iceberg `_pos` 逐行验证；且 HNSW/FLAT 在同一套 `IndexEntries` 上 recall 98.25% |

## 7. 需要怀疑的代码（按可疑度排序）

文件：`index-vector/src/main/java/org/apache/iceberg/index/vector/IvfKernel.java`

### 7.1 `build`（payload 写出）

```java
int listCount = options.nlistFor(count);                       // clamp(round(sqrt(n)),1,n)
float[][] centroids = listCount == 0 ? new float[0][] : train(vectors, listCount);
int[][] assignments = assign(vectors, centroids);
...
VectorPayloads.writeHeader(out, NAME, payloadVersion());
out.writeInt(dimension);
if (isQuantized()) { out.writeInt(quantizer.bits()); out.writeLong(quantizer.seed()); }
out.writeInt(centroids.length);
for (float[] centroid : centroids) { for (float v : centroid) out.writeFloat(v); }
out.writeInt(dataFiles.size()); for (String f : dataFiles) out.writeUTF(f);
out.writeInt(count);
for (int list = 0; list < centroids.length; list++) {
  int[] members = assignments[list];
  if (isQuantized()) writeQuantizedList(out, dataFiles, materialized, vectors, members);
  else { out.writeInt(members.length); for (int m : members) { /* fileId, pos, dims floats */ } }
}
```

### 7.2 `writeQuantizedList` 每列表的头部

```java
RaBitQQuantizer listQuantizer = RaBitQQuantizer.train(selected, dimension, quantizer.seed(), quantizer.bits());
for (float value : listQuantizer.centerValues()) out.writeFloat(value);   // dimension 个 float
out.writeInt(members.length);
// 每条目：int fileId, long pos, byte[codeBytes], float×3
```

### 7.3 `load`（payload 读回）

```java
VectorPayloads.readHeader(in, NAME, payloadVersion(), "IVF");
int dims = in.readInt();
int bits = 0; long seed = 0L;
if (isQuantized()) { bits = in.readInt(); seed = in.readLong(); /* == quantizer.bits() */ }
int listCount = in.readInt();
float[][] centroids = new float[listCount][dimension]; /* 读 listCount*dimension 个 float */
String[] dataFiles = readDataFiles(in);
int entryCount = in.readInt();
List<List<IvfEntry>> lists = ...;
float[][] listCenters = new float[listCount][];
for (int list = 0; list < listCount; list++) {
  if (isQuantized()) { 读 dimension 个 float 进 listCenters[list]; }
  lists.add(readList(in, dataFiles));      // 读 size 再读条目
}
// 然后为每个列表 new RaBitQQuantizer(dimension, seed, bits, listCenters[list])
```

### 7.4 搜索

```java
int visited = Math.min(options.nprobe(), loaded.centroids.length);
for (int list : nearestCentroids(queryVector, loaded.centroids, visited)) {
  if (isQuantized()) scoreQuantizedList(queryVector, loaded, list, results);
  else for (IvfEntry e : loaded.lists.get(list)) results.add(... scoring.score(queryVector, e.vector) ...);
}
results.sort(scoring.comparator());
return 截断 topK
```

`nearestCentroids` 用 `scoring.compareScores` 排序并按 list 下标破平。

### 7.5 `train` / `assign` 的可疑点（**我重点怀疑这里**）

```java
private float[][] train(float[][] vectors, int listCount) {
  int sampleCount = IvfOptions.samplesFor(vectors.length, listCount);  // min(n, max(listCount, listCount*39))
  int stride = Math.max(1, vectors.length / sampleCount);
  // 取 sample[i] = vectors[min(n-1, i*stride)]，i < sampleCount
  // 初始质心 = sample[i*sampleCount/listCount]
  // 25 次 Lloyd：nearestCentroid(sample, centroids) → 均值 → 早停
}
```

- `IvfOptions.samplesFor(1_000_000, 1000)` = 39,000；`stride` = 25 ⇒ 只覆盖到第 974,975 行（差 2.5%，
  应该无害）
- **质心初始化只取 `sample` 里的 1000 个点，且这些点按 stride=25 均匀取 ⇒ 初始质心本身就均匀覆盖数据**
- `assign(vectors, centroids)` 对 **全部 1M** 向量做一次最近质心分配 ⇒ 1000 个 `int[]`，每个约 1000 个元素
- **没有任何测试覆盖 nlist≫20 的情况**（见 §8）

## 8. 为什么现有测试没抓住它（方法论教训，接手者请勿重复）

| 测试 | 规模 | 缺陷 |
|---|---|---|
| `TestIvfKernel.quantizesTheVectorsWhenAskedTo` | 400 条目 × **8 维**（后来改 128 维），**nlist≈20，每条列表约 1 个向量，nprobe=全部** | **访问全部分区不可能失败**——扫全部当然找到全部。它完全没有检验码的排序能力，也没检验 nprobe 选择 |
| `TestIvfKernel.findsTheNearestNeighbours...` | 4000 条目，nlist=20，nprobe=2 | 能抓住 §5.1 的位累积 bug（0.170→1.000），但**数据是 `sin` 曲线**——128 维里的**1 维流形**，k-means 分区过于容易 |
| `TestRaBitQQuantizer` | 2000 向量，聚类合成数据 | 只测量化器本身，不经过 IVF 分区 |

**共同缺陷：所有本地测试的条目数比真实小 250 倍、`nlist` 小 50 倍、数据分布比真实简单得多。**

## 9. 建议的下一步（按信息量排序）

### 9.1 最高信息量：让 `IVF × NONE` 在集群上访问**全部**列表
把 `nprobe` 设成 ≥ `nlist`（例如 1000），单文件 1M：
- **若 recall = 1.0** ⇒ payload 里的 `(dataFile, rowPosition)` 与条目内容都是对的，
  问题在**质心/列表选择**（`nearestCentroids` 或 `centroids` 的读回）
- **若仍为 0** ⇒ payload 的**行引用或码本身**错了，问题在 `build`/`load` 的字节布局

这一步能把范围砍一半，且不需要重新构建（nprobe 是**定义属性** ⇒ 需要重建，约 7 分钟）。

### 9.2 加一条"行引用自校验"测试（本地，几分钟）
写入一个 IVF payload 后读回，断言每个 entry 的 `(dataFile, rowPosition)` 与写入时一一对应，
且 `dataFile` 都在字典内。这能直接判掉"行引用错位"。

### 9.3 在本地复现 nlist 的规模效应
20,000 条目 / `nlist=141`（sqrt）/ `nprobe=30`，仍用 `sin` 数据：
- 失败 ⇒ 是 `nlist` 规模问题，可在本地逐位调试
- 通过 ⇒ 是真实数据分布问题 ⇒ 需要真实 SIFT fixture

### 9.4 若 9.3 通过：真实数据 fixture
从 `sift1m_base` 抽 2 万行（128 维 float）写成测试资源，替掉 `sin` 数据。

## 10. 复现环境（厂内）

### 主机与集群
- `ssh shai`（所有输出需 `| grep -v "bad signature"`）
- 构建：直接跑 `spark-submit --master local[*]` 或提交到 A 集群（k8s）
- 集群提交：`export KUBECONFIG=/home/wanganyang/workspace/k8s-config/a-spark-ops-amdin-kubeconfig`，
  `--master k8s://https://10.130.17.1:6443 --deploy-mode cluster`
  （`brainctl spark submit` 不可用：其 kubeconfig 同时带 token 与 basicAuth）
- 镜像：`hub.i.basemind.com/spark/sparkann-e2e:20261020d`（含本批全部代码）
  - **重要（D37）**：jar 必须烤进镜像；`spark.jars` 无法让 session extension 生效
- 表：生产 HMS `thrift://10.130.17.34:9083`，`hms.sparkann.sift10m_base`（10M×128，39 文件）、
  `hms.sparkann.sift1m_base`（1M×128，28 文件）、`sift10m_query`、`sift10m_gt`
- HDFS：`hdfs://hdfs-namenode-0001.host.platform.basemind.com:8020`（HA 名 `stepcluster` 在 shai 上不解析）

### 资产目录 `/data/workspace/sparkann/`
| 文件 | 用途 |
|---|---|
| `cluster-build-submit.sh` | 构建索引。环境变量 `STRUCT`/`QUANT`/`OPTS` 指定结构与量化，例：`STRUCT=IVF QUANT=RABITQ OPTS="'rabitq-bits','3','nprobe','50'"` |
| `ClusterIndexBuild.java` / `cluster-build.jar` | 集群构建作业（内部调 `CALL build_vector_index(...)`） |
| `probe10m-submit.sh` / `probe1m-ivfq.sh` | 基准探针。`OVER` 环境变量设 over-sample |
| `ClusterSift10mProbe.java` | 探针本体：预热 N 个 + 交替顺序测 M 个 + 分位数 + recall（对精确扫描） |
| `Dockerfile` / `image-jars-new/`（36 jar） | 镜像构建 |
| `/tmp/build-deploy-jars.sh`（本机） | 暂存 29 个部署 jar |
| `data/` | SIFT 原始二进制与 loader |

### 常用命令
```bash
# 清索引（失败构建会留下空注册，重试前必须清）
hdfs dfs -rm -r -skipTrash $NN/spark-warehouse/sparkann.db/sift10m_base/indexes

# 建索引（7-9 分钟）
cd /data/workspace/sparkann && STRUCT=IVF QUANT=RABITQ \
  OPTS="'rabitq-bits','3','nprobe','50'" \
  ./cluster-build-submit.sh hms.sparkann.sift10m_base 262144 8g 2g 1 6

# 测（~8 分钟）
OVER=10 ./probe10m-submit.sh 10 6 3
```

## 11. 本地能跑通的、可作参照的测试

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17
./gradlew :iceberg-index-vector:test --tests "org.apache.iceberg.index.vector.TestIvfKernel"
```
当前结果：`IVFQ narrow recall=1.000`、`IVFQ narrow recallUnquantized=1.000`、
`exactTop10InsideQuantizedTop100=1.000`（4000 条目 / nlist=20 / nprobe=2 / 合成 `sin` 数据）

## 12. 我对根因的直觉（供参考，不要被它锚定）

`IVF × NONE` 精确打分 + 20% 覆盖仍然 0，最自然的解释是**返回的行引用与索引里存的行引用不一致**，
而不是"没找到"。但 `readList` 与 `build` 的字节布局我逐行核对过、看起来一致，
本地小规模也验证过。

第二大可能是 **`train`/`assign` 在 nlist=1000 时产出退化的分区**（例如大量空列表，
导致 `nprobe=200` 实际只覆盖极少数非空列表）。**这一点我在本地没有测过，值得优先验证**：
在 `build` 后打印"非空列表数 / 最大列表大小 / 最小列表大小"就能立刻看出来。
