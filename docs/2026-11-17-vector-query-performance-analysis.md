<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# 向量查询性能诊断与改造方向

近期评审后的实施优先级与对照实验见[向量查询性能优化路线图](2026-11-17-vector-query-performance-roadmap.md)。

状态：**分析结论，未实现、未验收。** 本文是给实现方的输入，不是完成报告；不含工期承诺，也不含任何已批准的新增参数。

基线：产品分支 `index` @ `9d2a50ebf5`（本文合并到的提交；测量所用的镜像是 `67e0122317`，两者之间只有缓存驻留与 RaBitQ 记账两类提交，索引布局未变）。下文行号均以 `9d2a50ebf5` 为准。测量数据取自 shai 上
`~/probe3042/vb/runs/oss-24g-1117-1019/results-parquet.json` 与
`~/probe3042/vb/runs/hdfs-24g-1117-1019/logs/parquet.driver.log`。
文中凡标「实测」的是测量产物里的数字，标「推算」的由代码布局与行数算出，两者的可信度不同，不要混用。

## 1. 结论

瓶颈不在算法、不在 SIMD、也不在硬件，而在**索引的物理布局**：每个 data file 都被写成一个自带一套
IVF 质心的独立索引文件，于是 `nprobe` 要对**每一个文件**各探一次。这条设计的扫描量公式是
`候选数 = nprobe × N / sqrt(G)`（`G` = 每个索引组覆盖的行数，默认等于一个 data file 的行数），
与「全局一套质心」相比差**组数倍**——本例差 586 倍。

由此导出三条杠杆，按收益排序：

1. **解耦「质心空间」与「payload 分片」**：质心全局（或两级）训练，payload 按 list 分片，查询只读被探测的
   分片。收益是数量级的（每查询 IO 从 22 GB 降到百 MB 级），代价是索引格式与元数据语义变更。
2. **两阶段检索（码粗筛 + 精确重排）**：现在量化估计值直接当最终分返回，没有 refine 阶段，于是只能靠
   加大 `nprobe` 与 `over` 硬顶 recall；加重排后 `nprobe` 可降，候选数与打分量随之下降。
3. **打分循环的算术与指令级优化**：内层每坐标一次 `decodeLevel` 里的**除法**、每坐标一次 bit 解包分支、
   经 `intAt` 逐 4 字节读取，都没有向量化。去除法是纯 Java 的 3 倍级收益，SIMD/native 再 3–5 倍，
   但它是前两条之后的乘数，单独做收效有限。

不建议把「把 group 调大」当作解决路径：它的收益是 `sqrt` 级，且被构建期内存硬顶住（见 §4.3）。

## 2. 实测事实

表与负载：`qwen_3_4b_embedding`（2560 维，EUCLIDEAN），10,784,907 行 / 586 个 data file / 61.71 GB，
10 条查询，真值 k=10。资源：12 executor × 4 core，每 executor 12g 堆 + 12g overhead，driver 4 core/12g，
`task_cpus=1`，`cache_budget_bytes=0`（**本批次缓存关闭**），Spark 3.5.7-STEP-rc3。

索引参数：`structure=IVF`、`quantizer=RABITQ`、`rabitq-bits=4`、`nlist=0`（按组派生）、`nprobe=32`。
建索引 1,197,232 ms（约 20 分钟）。

| 臂 | 样本 | p50 | recall@10 | used_index |
|---|---|---|---|---|
| `count(*)` | 2 | 313 ms | — | 0 |
| `scan-all`（count + sum(txt_len)，非向量扫描） | 2 | 730 ms | — | 0 |
| `index/over=1` | 20 | **27,933 ms** | **0.98** | 1.0 |
| `index/over=10` | 20 | 46,542 ms | 0.98 | 1.0 |
| `index/over=1/filter=pre` | 20 | 27,940 ms | 0.99 | 1.0 |
| `index/over=1/filter=post` | 20 | **134,012 ms** | 0.99 | 1.0 |
| `index/over=10/filter=post` | 20 | 148,650 ms | 0.99 | 1.0 |

两点必须先说清：`scan-all` 是一列标量聚合，**不是**全表向量扫描，不能拿来当「暴力检索只需 0.7 s」的比较对象；
`over=10` 与 `over=1` 的 recall 相同（0.98），说明 recall 已经饱和，它不再是调 `nprobe`/`over` 能推高的量。

同一套代码在带缓存的运行里是秒级（3.7–4.8 s 量级，不同表与不同批次，仅作量级参照），说明「把 payload
读进来」这一项占了当前延迟的绝大部分，而算法本身还有秒级余量。

## 3. 现状：写侧与读侧各做了什么

写侧（`index-vector`、`index-spark`）：

- 组 = data file：`IndexGrouping.assign:63`，`max-entries-per-index-file` 缺省为 0 时「一个 data file 一个组」。
- 每组一套 IVF：`IvfKernel.buildWithMemory:132` 里 `int listCount = options.nlistFor(count)`，
  `IvfOptions.nlistFor:119` 的规则是 `nlist > 0 ? nlist : round(sqrt(entries))`，再 `min(wanted, entries)`。
  **`nlist` 不是全表常数，而是每个组各自的 √组内行数。**
- 每个 list 单独训一个 RaBitQ 量化器：`IvfKernel.writeQuantizedList:263`，
  `RaBitQQuantizer.train(selected, dimension, seed, bits)`，list 的均值作中心。
- 构建期把组内**所有**向量解码成 `float[][]`：`IvfKernel.decodeAll:227`。

读侧：

- 每个索引文件的 payload 整体载入并复制：`IvfKernel.prepare:305` → `load(byte[]):432`，
  `VectorPayloads.toBytes:176` 是整块 `byte[]` 拷贝。
- `nprobe` 是**每个 payload 各自**的探测数：`IvfKernel.prepare:317` 取 `options.nprobe()`，
  `ListSearcher.search:755` 以它为起点。
- 过滤/召回不足时翻倍：`search:764-768`，`top.size() < topK` 就 `probed *= 2`，直到凑够或扫完本文件所有 list。
- 量化路径没有任何 refine：`scoreQuantizedLists:849` 把 `estimatedDistanceSquared` /
  `estimatedInnerProduct` 的结果直接 `top.offer`。
- 内层循环：`RaBitQQuantizer.projection:424` 每坐标解包一次 bit（含 `shift + bits > 8` 的跨字节分支）、
  调 `decodeLevel:388`（其中 `2.0 * level / (levels - 1)` 是**运行期双精度除法**），再乘一个 float。
- 字节读取走 `VectorPayloadReader.floatAt:190` → `intAt:175`，每 4 字节一次边界检查 + `intBitsToFloat`。
- 回表：`VectorIndexSearch:33-40` 说明每个命中的 data file 只读一次、所有查询共享，
  `IndexEntries.positionsMatching:872` 用位置集合取行。

## 4. 定量诊断

### 4.1 一条公式解释全部现象

每组行数 `G`，每组 list 数 `sqrt(G)`，每 list 行数 `sqrt(G)`，组数 `N/G`。查询探 `nprobe` 个 list：

```
候选行数 = nprobe × sqrt(G) × (N/G) = nprobe × N / sqrt(G)
扫描比例 = nprobe / sqrt(G)
```

本例 `G = 10,784,907 / 586 = 18,404` → `nlist = round(sqrt(18404)) = 136`，每 list 约 135 行。
`nprobe=32` → 每文件 32/136 = **23.5%** → 全表 **254 万行**候选。

| 组大小 G | 每文件 list 数 | nprobe=32 的扫描比例 | 全表候选 | 索引文件数 | 构建期 decodeAll |
|---|---|---|---|---|---|
| 18.4k（现状） | 136 | 23.5% | 254 万 | 586 | 0.19 GB |
| 100k | 316 | 10.1% | 109 万 | 108 | 1.0 GB |
| 500k | 707 | 4.5% | 49 万 | 22 | 5.1 GB |
| 2M | 1,414 | 2.3% | 24 万 | 6 | 20.5 GB |
| 10.78M（整表一组） | 3,283 | 0.97% | 10 万 | 1 | 110 GB |

（`decodeAll` = `G × 2560 × 4 B`；executor 堆 12g，故 2M 以上不可行。）

### 4.2 索引体积与每查询扫描量（推算）

- 每候选 `4 B`（data file 序号）`+ 8 B`（行位置）`+ 2,048 B`（4 bit × 4,096 补零维）`+ 12 B`（三个因子）
  `= 2,072 B`。
- 码总量 `10,784,907 × 2,072 ≈ 22.4 GB`；另加每文件 header 的 k-means 质心（136 × 2560 × 4 = 1.4 MB）
  与每 list 的量化器中心（136 × 10,240 B = 1.4 MB），合计约 **1.6 GB**；索引总计 ~24 GB，是表数据
  （61.7 GB）的 39%。
- 每查询实际要打分的码：23.5% × 22.4 GB ≈ **5.3 GB**。
- 每查询实际要读的 index 字节：`cache_budget_bytes=0` 时是每个文件整块载入，即 **22 GB 级**，
  与 `nprobe` 无关——这是「调小 `nprobe` 省不掉 IO」的原因。

### 4.3 三个设计点对比（同样 10.78M 行）

| 设计 | 质心规模 | 每 list 行数 | nprobe=32 的候选 | 被探测分片字节（32 个 list） |
|---|---|---|---|---|
| 现状：586 组 × 136 list | 79,696（分散） | 135 | 254 万 | 5.3 GB（且实际读 22 GB） |
| 全局质心，list 长度保持 135 | 79,696 | 135 | 4,320 | 8.9 MB |
| 全局质心，nlist≈sqrt(N) | 3,283 | 3,284 | 105,381 | 217 MB |

第一行与第二行的差别只是「质心是否共享」，扫描量差 586 倍；这就是本文主张的改造点。

## 5. 澄清与已否证的判断

实现方容易在以下五处走弯路，逐条给依据。

**（1）加大 `nprobe` 不会提高 recall。** `over=1` 与 `over=10` 都是 0.98（实测）。recall 的天花板来自
量化误差，不是漏簇：每 list 只有 135 个样本，它的中心（list 均值）与三个因子都从这 135 行估出来
（`writeQuantizedList:263`），样本量偏少；旋转符号走的是共享 seed，不是每 list 各自估计。因此「加重排」比「加 `nprobe`」有效，且重排还能顺带把 `nprobe` 降下来。

**（2）调小 `nprobe` 省不掉 IO。** payload 是整文件载入的（`prepare:305`→`load:432`），
22 GB 与 `nprobe` 无关；`nprobe` 只决定内层打分次数。

**（3）过滤查询里 `nprobe` 是地板不是天花板。** `search:764-768` 在 `top.size() < topK` 时把 `probed`
翻倍。过滤把命中摊薄，于是翻倍一路推到接近全扫——`filter=post` 由 27.9 s 变 134 s（实测）就是这条路。
**靠调小 `nprobe` 救不了过滤**，要改的是过滤与候选集的接线（本仓已有 `IndexCandidates` 的 join 路径
与 pre/post 两种形态，可直接做代价比较后择一）。

**（4）每 list 135 行本身不算少。** Faiss 在 10M 规模上推荐的 nlist 是 4k–64k，对应 list 长度 150–2,500，
135 在正常区间。问题不在 list 长度，而在质心被按文件切碎成 586 份互不相干的副本，
以及「组内共享质心」这个前提在数据随机写入时并不成立。

**（5）几何下界剪枝在 2560 维上没有价值。** 用「质心距离 − 簇半径」或 sign-based 下界剪枝，
在高维下几乎剪不动任何东西。有效的「剪枝」只有一种含义：**让不需要的字节根本不被读**，
也就是 §6 的 P1，而不是在查询期算一个更紧的界。

## 6. 候选方案

### P1：质心全局化 + payload 按 list（或簇）分片（结构性，收益最大）

做法：k-means 在抽样上训练一套（或两级）全局质心，与「哪个文件装哪些 list」解耦；payload 按 list
或簇分片，元数据记录每个分片覆盖的 list id 与质心，查询期先选 list、再只取对应分片。

改造点：`IndexFile.coveredDataFiles` 的覆盖语义要能表达「一段 list 键空间」（`IndexFile`、
`IndexMetadataParser`、`IndexSnapshot`、`IndexGrouping`、`SparkIndexBuilder`）；`IvfKernel` 的 payload
header 已有 list 目录（`listStarts`/`listCounts`），可扩展为「本分片持有 list 的键区间」；
读侧 `DistributedIndexSearch`/`IndexFileRdds` 要能按 list 选择分片；`ExecutorIndexCache` 与
`IndexPlacementPlanner` 的驻留单位从「文件」变成「分片」。

代价与风险：索引格式与元数据语义变更，已建索引要重建；小分片数量会上升，需要按簇合并到合理粒度；
写入侧的增量维护要回答「新文件的行属于哪些 list、是局部重建还是重写分片」；REST 侧要产出对应的
metadata update。收益：按 §4.3，每查询被读字节从 22 GB 降到百 MB 级，`nprobe` 从此是真正的 IO 旋钮。

### P2：两阶段检索（码粗筛 + 精确重排）

做法：粗筛返回 `topK × refine` 个候选及其估计分数，然后在回表阶段用精确向量重算距离、重新排序。
现在回表本来就只读一次命中文件（`VectorIndexSearch:33-40`），重排只是多一次 2560 维点积，
增量成本很小，却能把 `nprobe` 从 32 降到 4–8。

改造点：`VectorScoring.score:51` 作为唯一评分口径（保证重排与暴力真值同序）；`IndexSearch`/
`VectorIndexSearch` 增加 refine 阶段；`IndexQuery`/SQL 入口透传 refine 倍数（**新增参数需批准**）。

风险：recall 必须与暴力真值重跑对照；「分数更准」不等于「召回更高」，验收口径不能混。

### P3：打分循环的算术与指令级优化

三步，按性价比排序：

1. **去除法**：`decodeLevel:388` 改成 16 项 LUT（`levels ≤ 256`，可在训练时算好），
   并把 `projection:424` 的每坐标 bit 解包改成按位平面累加，消除 `shift + bits > 8` 的分支。
   纯 Java，预期 2–4 倍。
2. **向量化**：Panama Vector API（JDK 17 为 incubator，需 `--add-modules`）实现 FastScan 式
   「字节 LUT + shuffle 累加」；或直接调 Lance 的 Rust `ivf_rq` kernel（镜像里已有 `lance-core` native）。
   预期再 3–5 倍。
3. **减少拷贝与读取开销**：`prepare` 里对每个 payload 做整块 `toBytes` 拷贝（38 MB/文件），
   命中缓存时也一样；`intAt` 每 4 字节一次边界检查，可用 `ByteBuffer`/`MemorySegment` 直接读 float。

风险：这一层是前两条之后的乘数。在 22 GB IO 之上做 5% 的 CPU 优化没有意义，先做 P1/P2 再评估。

### P4：组预算与 `nlist` 显式化（零代码即可做对照）

`max-entries-per-index-file`（`IndexMetadata.MAX_ENTRIES_PER_INDEX_FILE_PROPERTY:53`）已支持合并组，
设成 500,000 即从 586 组降到 22 组、`nlist` 升到 707、扫描比例从 23.5% 降到 4.5%（§4.1）。
代价：构建并行度从 586 个 task 掉到 22 个；单文件 payload 从 38 MB 涨到 ~1 GB，缓存与加载粒度变粗；
每 task `decodeAll` 需 5 GB 堆。`nlist: 0`（按 √G 派生）建议改为「按目标 list 长度显式反推」，
避免组大小变化时粒度失控（**改动语义需批准**）。

### P5：降维（可选）

2560 → 512（Hadamard 采样或随机投影）后编码，每候选从 2,072 B 降到约 280 B，索引再缩 7 倍。
配合 P2 重排，recall 通常掉不超过 1 个点。风险：需要额外的降维矩阵与版本管理，且降维本身会损失
对细粒度相似度的分辨力，必须与真值对照。

### P6：任务内并行与 GPU（可选，注意口径）

`task_cpus=1` 是测量口径的约束，不是产品约束；生产上按 list 并行扫是最省事的 4 倍。
GPU（faiss-gpu / cuVS）在 10M 规模是几十毫秒级，但引入 CUDA 栈与调度改造，只在「在线 QPS 是主指标」
时值得。两条都会改变「资源对齐」的前提，做对照必须重新声明资源口径。

## 7. 推荐顺序

1. **先补测**（§8），把 27.9 s 拆成「payload 加载 / 码打分 / 回表」三段，确认 IO 与 CPU 的真实占比。
2. **P4 对照**（零代码、可回滚），用现有属性验证 §4.1 的 √G 公式是否成立。
3. **P2 重排**（改动小、直接把 recall 从「靠堆候选」变成「靠精排」）。
4. **P1 分片**（结构性改造，需单独立项与格式评审）。
5. **P3 指令级优化**（在前几步之后作为乘数，先做去除法，再评估 SIMD/native）。
6. P5/P6 视业务指标（存储成本、在线 QPS）决定。

不推荐：把 HNSW 换作主结构（本仓已有 `HnswlibKernel`，但 10M 规模下的内存与过滤 recall 都不如
IVF + 量化 + 重排，且与 snapshot 语义的增删维护代价高）；在高维上做半径/符号下界剪枝（§5.5）。

## 8. 先决测量（当前缺失）

- **`nprobe` 曲线**：现存所有 run 都是 `nprobe=32`，没有 1/4/8/16/32 的延迟—recall 曲线。
  `nprobe` 是查询期参数，改索引定义即可，不需重建 payload，成本约每点 20 样本。
- **三段耗时分解**：payload 加载（含 `toBytes` 拷贝）、码打分、回表（`positionsMatching` + 行读取）
  各自的占比。建议在 executor 侧加临时计时（或看 task 的 `duration` 与 `bytes read`）。
- **缓存开关对照**：同一批次 `cache_budget_bytes=0` 与开启后的差异，量化「22 GB/查询」这一项。
- **组预算对照**：`max-entries-per-index-file` = 0 / 100k / 500k 三点的延迟与 recall。

## 9. 验收口径（任何一条方案的报告都要满足）

1. recall@10 必须与同一批查询的暴力真值对照，且声明真值的 k、候选宽度与距离函数。
2. 报告必须同时给出：`used_index`、每查询候选行数、每查询读取的 index 字节数、payload 加载字节数、
   资源口径（executor 数 × core × 堆、`task_cpus`、缓存预算、镜像 digest 与产品提交）。
3. 只报延迟不报 recall 的结果无效；只报 recall 不报资源与候选量的结果也无法比较。
4. 任何新增的用户可设置参数（`refine` 倍数、`nlist` 语义、分片粒度等）都必须先经批准，
   不得以「留个开关」「方便调优」为由自行加入。
5. 现有 `over` 与 `nprobe` 的语义不得静默改变；改变即视为不兼容，需要显式版本化。

## 10. 开放问题

1. 全局质心的训练样本规模与两级（粗簇 → 簇内细 list）是否必要？单级全局质心在 2560 维上是否够？
2. payload 分片以 list 为单位还是以「簇」为单位？分片过小会导致元数据与请求数爆炸，
   过大则失去剪枝价值，需要给出目标分片大小（建议按「单分片 1–8 MB」反推）。
3. 写侧增量维护：新文件的行落在哪些 list 由全局质心决定，是否允许「跨文件重写一个分片」？
   与 `IndexSnapshotWriter` 的事务边界如何配合？
4. 过滤与向量检索的顺序：`filter=pre` 与 `filter=post` 差 4.8 倍（实测 27.9 s vs 134 s），
   代价模型放在哪一层（优化器、规划期、还是 `IndexCandidates` 的运行时选择）？
5. 是否需要保留「按文件独立 IVF」作为一种兼容模式？已建索引的迁移路径与重建窗口怎么安排？
