<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# 热查询评分优化与精排（设计与最小实现）

状态：**分析 + 设计；评分常量提升与精排最小实现已落地，等级查表/SIMD/FastScan 与格式变更未落地。**
本文展开[向量查询性能优化路线图](../../2026-11-17-vector-query-performance-roadmap.md)§3「热查询优化评分并验证精排」，
性能背景与「实测/推算」口径见[向量查询性能诊断](../../2026-11-17-vector-query-performance-analysis.md)（下称「诊断」）。
基线为 `index` 分支 `888675fed`，行号以本文提交时的 `roadmap3-refine` worktree 为准。
本文不含工期承诺，也不提出新的用户可设参数；实验开关均为内部系统属性。

## 1. 结论

1. **评分常量提升（已落地）。** RaBitQ 的 DOT 与 COSINE 估计里，有四项只依赖（查询×列表）而不依赖候选：
   旋转后的查询、中心化查询的模长平方、查询自身模长平方、列表中心与中心化查询的内积。
   其中「中心与中心化查询的内积」和「中心模长平方」原本在**每个候选**的内积估计里各跑一遍完整维度循环
   （`RaBitQQuantizer.estimatedInnerProduct`）。把它们和旋转查询一起在列表评分前算一次
   （`RaBitQQuantizer.prepareQuery`），内层循环保留原有加法顺序与精度，结果 **bit 级不变**。
   实测（维度 2560、padded 4096、4 bit、8000 个候选）：DOT/COSINE 内积循环 51.1 ms → 23.0 ms，**2.22×**；
   L2 距离循环 23.8 ms → 23.8 ms，**1.00×**（L2 本来就没有这两段每候选循环，见 §3.4）。
2. **精排最小实现（已落地，默认关闭）。** 全仓真正「仍用索引估计值决定保留哪 k 行」的路径，是 SQL 重写计划
   在**每个索引文件的任务内**按估计分排序后截断到 k（`DistributedIndexSearch.rowsOfIndexFile` 末尾的 `byScore`
   + 前 k 行）；算子之上的 `Sort` 会把留下来的行精确重排，但被估计分挤出 k 的行再也回不来。
   已实现「候选宽度与 k 分离 → 已回表的原向量上精确重排 → 只保留 k」：窗口取 `max(调用方窗口, c×k)`，
   默认 `c=8`，由内部系统属性开关控制。关闭时行为与现在逐字节一致（含指标）。
   一个 1 bit、两维的确定性 fixture 上：关闭时索引返回估计值的第一名（错误行），开启后返回真正的最近行，
   `numCandidates/numCandidateRequests` 从 `1/2` 变为 `8/8`（窗口与回表量有记录）。
3. **未落地（按 profile 决定）。** 等级查表（LUT）、位平面解包、SIMD/FastScan、native kernel 都是内层循环的
   乘数，但都在 IO 与候选量之后（诊断 §1、§6 P3）。本次实测 LUT + 预计算地址数组在同样的内积循环上是
   中性偏负（L2 距离 22.9 → 23.3 ms，0.98×），因此**没有**落地；取舍分析见 §6。

## 2. 现状：哪些路径已经精确，哪些还按估计值定序

先把「评分」与「排序」分开：索引可以返回一个精确的分数，也可以返回估计的分数；而「谁被保留」可以在
任务内按估计值决定，最后再由上层算子精确重排。三条入口的现状：

| 入口 | 候选如何产生 | 谁按什么分数决定保留 | 最终顺序 |
|---|---|---|---|
| SQL 重写：`ORDER BY vec_distance(...) LIMIT k` | `VectorSearchSource` → `VectorSearchExec.indexRows` → `DistributedIndexSearch.searchWithRows`；每个索引文件一个 task，task 内按索引估计分取窗口并回表 | **任务内**：`rowsOfIndexFile` 的 `found` 按估计分 `byScore` 排序后取前 k；窗口不足时在本文件内扩探 | 算子之上的 `Sort` 用 `vec_distance` 精确重算并排序（`VectorSearchExec` 类注释：「the operator above the source re-scores and sorts exactly」） |
| `CALL vector_search`，`coverage=table/indexed` | `VectorSearchBridge.nearestRowsInBlock` → `searchBatchWithOutcomes` | 回表后用 `distanceOf`（`VectorDistance.distance`）精确重算，`CandidateOrdering=(distance,file,pos)` 取 k | **已精确**；`table` 另加未覆盖文件扫描，两者合并精确取 k |
| `CALL vector_search`，`coverage=index_hits`（及批量入口） | 同上，但契约是「索引自己的命中」 | 索引自身的估计分与顺序（`hitsForBlock` 保留首见顺序，按索引顺序取 k） | **按设计返回估计值**；契约不能改 |

结构层面的补充：

- `FLAT` + `NONE` 存原始向量、逐条精算（`VectorSearchSource.enumeratesEveryCandidate` 即此判定），没有任何估计；
- `IVF` + `NONE` 的 `scorePlainLists` 在原始向量上打分，同样是精确值；
- HNSW 在返回前用存储向量按精确度量重排（`HnswlibKernel` 类注释），也不需要精排；
- 真正带估计的只有 `IVF` + `RABITQ`（以及任何将来把码当分数的量化结构）。

所以「仍用估计值定序」的路径只有两处，性质不同：

1. **任务内的每文件 top-k 截断**（上表第一行）。这是本卡精排的目标：行**已经回表**，向量就在手上，
   精确重排只多一次维度点积，却决定「估计分排第 k+1 的真近邻」是否还能进入答案。
2. **`index_hits` 契约**。它承诺返回索引的原始分数与原始顺序（诊断 §9.5 的兼容口径），
   改成精确值就是改契约，本卡不动。

`IvfKernel` 内部（`scoreQuantizedLists`）不能做精排：它只有码和三个因子，原向量在回表之后才有；
任务内的 `rowsOfIndexFile` 是第一个同时持有「索引命中」与「原向量」的位置。

## 3. 评分：把（查询×列表）常量提到列表评分一次

### 3.1 估计式的分解

RaBitQ 为每个列表保留一个中心 `c`、一组旋转符号、每候选三个因子。中心化数据向量记 `o' = o - c`，
旋转记为 `r(·)`（对角符号 + 归一化 Walsh–Hadamard），码的每个坐标取 `level ∈ [0, levels-1]`，
`decodeLevel(level) = -1 + 2·level/(levels-1)`。查询侧：

```
rq = r(q - c)                     // 旋转后的查询
proj(x, rq) = Σ_i decodeLevel(x_i) · rq_i
|q'|² = Σ_d (q_d - c_d)²
inner = |o'| · proj / corr        // corr = factors[1] = <x, u>
```

三种度量分别用到：

| 度量 | 最终分数 | 与候选无关、只依赖（查询×列表）的量 |
|---|---|---|
| L2 | `sqrt(|o'|² + |q'|² - 2·inner)` | `rq`、`\|q'\|²` |
| DOT | `\|c\|² + Σ_d c_d(q_d - c_d) + factors[2] + inner` | `rq`、`\|c\|²`、`Σ_d c_d(q_d-c_d)` |
| COSINE | `inner / (sqrt(\|v\|²)·sqrt(\|q\|²))`，`\|v\|² = \|o'\|² + 2·factors[2] + \|c\|²` | `rq`、`\|c\|²`、`\|q\|²`、`Σ_d c_d(q_d-c_d)`（DOT 式内积还要用） |

`|o'|`、`corr`、`factors[2]` 是每候选的因子；`proj` 是每候选每个坐标的乘加（内层循环本身，保持不动）。

### 3.2 提取点

`IvfKernel.scoreQuantizedLists`（现 `IvfKernel.java:1061`）在进入记录的循环之前，对每个列表调用一次
`RaBitQQuantizer.prepareQuery(queryVector)`，得到 `RaBitQQuantizer.ListQuery`：

```java
// RaBitQQuantizer.java:417
RaBitQQuantizer.ListQuery prepared = listQuantizer.prepareQuery(queryVector);
// ...进入每候选循环...
case L2 -> Math.sqrt(listQuantizer.estimatedDistanceSquared(payload, codeBase, factors, prepared));
case DOT -> listQuantizer.estimatedInnerProduct(payload, codeBase, factors, prepared);
case COSINE -> {
  double inner = listQuantizer.estimatedInnerProduct(payload, codeBase, factors, prepared);
  double vectorSquared = RaBitQQuantizer.squaredLength(factors, prepared.centerSquared());
  yield vectorSquared <= 0.0 || prepared.querySquared() <= 0.0 ? 0.0
      : inner / (Math.sqrt(vectorSquared) * Math.sqrt(prepared.querySquared()));
}
```

`ListQuery` 持有 `rq`、`|q'|²`、`|q|²`、`|c|²`、`Σ c_d(q_d-c_d)` 五个值；它们原先散在
`estimatedInnerProduct` 的每候选调用里（`queryDotCenter` 与 `centerSquared` 两段维度循环），
以及 `scoreQuantizedLists` 的每列表前置代码里（`|q'|²`、`|q|²`、`|c|²`）。
每个量化器仍然每列表构造/取用一次，与现有布局一致：缓存路径用 `loaded.listQuantizers[list]`，
范围读路径按读到的中心 `new RaBitQQuantizer(...)`。

### 3.3 为什么等价（浮点顺序）

被提取的每个量都覆盖一个**独立的累加器**，提取只在「多久算一次」上变化，不改变任何一个累加器的
求和顺序、元素类型和运算顺序：

- `|q'|²`：`for d: double difference = q[d] - c[d];`——先在 `float` 里相减、再拓宽为 `double`，然后平方累加。
  新代码保持同样的 `float` 相减，不改成 `double` 相减（那会改变舍入）。
- `|c|²`：`(double) c[d] * c[d]`——先把 `c[d]` 拓宽为 `double`。新代码保持同一表达式。
- `Σ c_d(q_d-c_d)`：`(double) c[d] * (q[d] - c[d])`——同样先 `float` 相减、再用 `double` 乘。
- `|q|²`：`(double) q[d] * q[d]`，与 `|c|²` 在同一循环里但各自独立累加，元素顺序不变。
- `inner` 的组合顺序不变：`|o'| · proj / corr`，`|o'|² + |q'|² - 2·inner`，`|c|² + Σ + factors[2] + inner`
  都按原来的结合顺序书写。

这些量本身与候选无关，因此对任意候选，提取前后参与运算的是**同一批 double 值**，且乘加顺序相同，
所以 `Double.doubleToLongBits` 逐位相等；测试直接断言这一点（§5）。内层 `projection` 完全未改，
连等级解码里的 `2.0·level/(levels-1)` 除法也保留（LUT 的实测结论见 §6）。

### 3.4 实测

在 `index-vector` 内用临时测量程序（不入库）跑同一批码：维度 2560（padded 4096）、4 bit、
200 个候选 × 40 条查询 × 5 轮，JIT 预热后取均值；「旧」路径按现网代码逐候选算常量，
「新」路径用 `prepareQuery` + prepared 重载：

| 度量 | 旧（ms/轮） | 新（ms/轮） | 加速比 |
|---|---|---|---|
| DOT / COSINE 内积 | 51.1 | 23.0 | **2.22×** |
| L2 距离 | 23.8 | 23.8 | **1.00×** |

解释：L2 的 `estimatedDistanceSquared` 本来就没有每候选的维度循环，旧代码只是在每列表里算
`|q'|²/|c|²/|q|²`；新路径多算一个每列表的 `Σ c_d(q_d-c_d)`（诊断场景 32 个 list × 2560 ≈ 8 万次运算，
相对于 254 万候选 × 4096 坐标可忽略）。按每候选折算，DOT/COSINE 从 6.4 µs 降到 2.9 µs（2560 维）。
**收益只在 DOT/COSINE**；诊断 §2 的表里 `EUCLIDEAN` 是 L2，所以这条优化在实测负载上不改变 L2 的
端到端时间，它的价值在 COSINE/DOT 索引的查询，以及为后续打分优化腾出的每候选预算。

另外一处本轮落地的小项：过滤触发的扩探原先每轮都重新给全部质心打分并整体排序
（`IvfKernel.nearestCentroids` 对 `nlist` 个质心全量排序，再取前 `probed` 个），
现在 `ListSearcher.search(..., candidates)` 只排一次，扩探取同一顺序的前缀（`IvfKernel.java:959`）。
排序键相同时的次序由 `(score, list index)` 决定，与逐轮重排完全一致，所以扩探访问的列表序列不变。

## 4. 精排：宽候选 → 回表原向量 → 精确重排到 k

### 4.1 候选宽度与 k 分离

现状（`over=0` 默认，即 `VectorCandidateBudget.derive` 的 `k·max(1, over)`）：调用方窗口
`opening = k`，`searchWithRows` 再取 `initial = max(opening, k+1) = k+1`，任务问索引要 `k+1` 个命中，
回表后按估计分留下 k。也就是说：**估计分决定谁进 k，精确分只决定进了 k 的人怎么排。**

精排把窗口与 k 拆开：

```
W = min(entries, max(initial, c·k))        // c = 8（内部属性，默认 8）
任务问索引要 W 个命中 → 全部回表 → 精确距离排序 → 只留前 k
```

- `W` 在第一轮一次问满，不走「先要 k+1 再翻倍」的老路：扩探一轮会把同一批候选**再读一遍**，
  精排窗口本身就要一次读到位（`DistributedIndexSearch.java:250` 附近的 `requested`）。
- 窗口只增不减：调用方显式给了更宽的 `over`（例如 10）时 `W ≥ 10k`，精排不会缩小它。
- 行不足 `W`（删除、过滤、文件本身不够）时，原有扩探循环照旧向本文件更深要命中，
  目标是 `W` 而不是 `k`；轮数上限 `MAX_ROUNDS=10` 不变。
- `index_hits` 与批量入口不走这条路径，契约不变。

### 4.2 精确重排的位置与口径

在 `rowsOfIndexFile`（`DistributedIndexSearch.java:493`）收集完回表行之后：

- 没有精排时：`byScore(higherScoreFirst)`（索引比较器的方向）排序取前 k，与现在完全一致；
- 有精排时：对每个已回表行从它自己的行里取向量列，用 `VectorDistance.distance(metric, vector, query)`
  算精确距离（L2/COSINE/DOT 统一「越小越近」），按 `(distance, dataFile, rowPosition)` 排序取前 k
  （`DistributedIndexSearch.refinedTopK`，`DistributedIndexSearch.java:601`）。

口径与上层一致：SQL 重写把 `vec_distance` 绑到索引的度量（`RewriteVectorSearch.bindMetric`），
`VectorDistance` 正是 `vec_distance` 的实现（`VectorSearchBridge.distanceOf` 也用它），
所以任务内精确重排与算子之上的精确排序是同一个函数、同一个方向。
`(distance, dataFile, rowPosition)` 的次序也与 `byScore` 的既有 tie-break 一致：
距离相同就按位置，而不是按任务取到的先后。

代价：每个已回表候选一次长度 `d` 的点积（2560 维约 2.6 k 次乘加），相对该候选的码打分
（padded 4096 次解码乘加 + 读码 + 读因子）更便宜；**不增加任何读取**，因为行与向量本来就在回表里。
回表字节随窗口从 `min(entries, k+1)` 行增加到 `min(entries, c·k)` 行；这些行在既有指标里可读：
`numCandidates`（本轮请求窗口）、`numCandidateRequests`（最后一轮窗口）、`numLiveCandidates`（解析出的活行），
按投影行宽折算即回表字节的量级；精确的存储端字节数仍需在目标环境用存储计数器测（见 §5）。

### 4.3 开关、默认值与适用条件

- `iceberg.index.vector.refine=true` 打开精排，默认关闭；
- `iceberg.index.vector.refine.candidates=<正整数>` 是每 k 的候选数，默认 8；
- 二者都是**内部系统属性**，只给测量与测试用，不是新的用户参数（诊断 §9.4 的要求）；
  关闭时走的是与现在完全相同的代码路径（排序比较器、窗口、指标都不变）。
- 只在能确定「确实仍用估计值定序」的形状上生效：`VectorIndexAdapter` 且 `IVF` + `RABITQ`；
  向量列必须落在任务保留的前缀列里（否则回表行里没有向量可重排，例如投影裁掉了向量列）；
  只对单查询的 `searchWithRows` 生效（多查询的同一 task 会把多条查询的命中合并进一个 top-k，
  没有「哪条查询的窗口」可分）。
- 关闭时的等价性由既有测试锁定（`TestVectorSearchExecution` 的指标断言，如
  `aFullyCoveredQueryReportsNoResidualBranch` 的 `numCandidates=1/numCandidateRequests=2`）。

### 4.4 recall 与字节的预期

- **recall@K**：精排只能纠正「已被回表候选内部」的相对次序。真近邻若不在窗口 `W` 内（估计分把它排到
  `W` 之外，或探测阶段根本没有访问到它所在的 list），精排救不回来；这仍然要靠 `nprobe`/探测改进
  （诊断 §5.1：`over=1` 与 `over=10` 都是 0.98，说明 recall 的天花板不是 `over`，而是码误差与候选集）。
  `W` 从 `k+1` 增到 `8k` 会显著扩大「候选集」这一侧，所以固定 `nprobe` 时 recall 应不降；能提升多少，
  取决于估计分把真近邻排在 `(k, W]` 的比例，这正是 §5 要测的量。
- **回表字节**：干净文件上每文件从 `k+1` 行增到 `c·k` 行（默认 8k）；索引读取字节不变
  （payload 整块/按范围读与窗口无关，诊断 §5.2）；打分开销增加每候选一次 `d` 维距离。
  因此净收益要求：`Δrecall` 换来的质量提升 ≥ 回表行数增加的成本；`c` 必须由实验定，不能拍脑袋固定。
- **`over=0` 对照**：现有 `over` 是用户参数，本设计不动它。对照实验固定 `over=0`（即默认）与同一 `nprobe`，
  只切内部属性 `refine`/`refine.candidates`，因此「精排」与「用户调参」不混在一个变量里；
  需要更宽窗口时先试 `refine.candidates`，确认收益后再讨论是否把宽度做成参数（那需要批准，诊断 §9.4）。

## 5. 实验方案

目标：先证明「同输入同布局同探测参数下，分数与排序逐项一致」，再在相同 recall@K 下比较打分 CPU、
探测量与回表字节，最后决定是否值得把精排默认打开、把窗口做成可配置。

### 5.1 变量与固定项

- 自变量：`nprobe ∈ {4, 8, 16, 32}` × 候选宽度 `c ∈ {1(off), 2, 4, 8, 16}`（即 `W = c·k`）；
  过滤器（无 / pre / post）与删除存在与否各一组。
- 固定：同一表快照、同一索引快照（`nlist`、bit 数、分组都不变）、同一距离函数、同一 `k=10`、
  `over=0` 默认、同一资源配置（executor 数 × core × 堆、`task_cpus`、缓存预算、镜像 digest），
  Spark 作业与任务数按诊断 §9.2 的口径一并报告。
- 真值：同一快照、同一距离函数上的暴力检索，`recall@10` 与候选宽度/`nprobe` 一起报告（诊断 §9.1）。

### 5.2 测量项

| 量 | 怎么测 |
|---|---|
| recall@K | 与暴力真值逐查询比较，声明 K、候选宽度、距离函数 |
| 打分 CPU | 两段：`RaBitQQuantizer` 微基准（每候选 ns，§3.4 的方法，固定维度/bit/候选数）＋ 任务内 `search` 的 CPU 时间（`System.nanoTime` 临时计时或 task duration 分解） |
| 探测量 | 每查询访问的 list 数、打分的码数（`nprobe` 与 `W` 的乘积关系；可用现有 `numCandidates`/`numCandidateRequests` 与临时计数器） |
| 回表字节 | `numLiveCandidates`（活行）与 `numRowsAtFiles`（文件数）× 投影行宽；存储端用 S3/HDFS 计数器另记实际请求与字节（诊断 §8） |
| 总延迟 | p50/p95，冷读与确认命中的热查分开；索引读取（payload 加载）与回表、打分分段计 |

### 5.3 分段与验收

1. **等价性**：同一布局与探测参数下，优化前后每条查询的分数与排序逐项 bit 级一致；精排关闭时
   计划与指标（`numCandidates` 等）也一致。已由 §6 的测试覆盖，实验里再跑一次同快照对照。
2. **recall 不降**：固定 `nprobe`，`c` 从 1 增到 8/16 时 `recall@K` 单调不降（允许并列分数抖动，
   但不得出现显著回退）。
3. **成本**：给出 `Δrecall` 与 `Δ(回表字节、打分 CPU、总延迟)` 的对照表，选一个 `c` 与
   `nprobe` 的组合；`over=0` 为对照，不要求用户调参。
4. 若精排带来的 recall 提升在噪声内、而回表字节明显上升，则保持默认关闭，只保留设计与最小实现。

## 6. profile 驱动的 LUT / SIMD / FastScan 取舍（本轮不落地）

诊断 §6 P3 给出一条明确的性价比顺序：去乘法/除法 → 向量化 → 减少拷贝与读取。本轮只做了
「每候选的循环次数」这一层（§3），因为它是纯 Java、零格式变更、可 bit 级证明的提取。其余三项的取舍：

- **等级 LUT + 预计算地址数组**（去掉内层 `2.0·level/(levels-1)` 与每坐标的 `(i·bits)` 计算）：
  本轮实现并**实测**过：把等级值放 `double[levels]`、把 shift/byte/straddle 放共享数组，
  L2 距离循环 22.9 → 23.3 ms（0.98×，多轮稳定），没有收益。原因是该循环受码与查询数组的加载支配，
  JIT 已把常量坐标算术消掉，LUT 反而多一次数组寻址。结论：在 4096 padded 维、4 bit 的布局上**不做**；
  只有 profile 显示除法/地址计算占比可观（例如更高 bit、更小维度）时再评估。
- **位平面累加**（消除 `shift + bits > 8` 的跨字节分支）：需要按位平面重排码或改解码循环的写法，
  本身不改变字节数但改变布局的可读性；只有在等级/地址优化之后内层仍有明显分支代价时才做。
- **FastScan 式「字节 LUT + shuffle 累加」/ Panama Vector**：需要把码按 4/8 bit 分块重排（构建期多一次
  转置、格式版本升级），收益 3–5×，但它是**乘数**：在 IO 占主导的现状（诊断 §1）下单独做收效有限。
  取舍点：当且仅当「payload 加载 + 回表」被分片/缓存解决、码扫描成为端到端主项时立项；
  JDK 17 上 Panama 还是 incubator（需要 `--add-modules`），部署面也要计入。
- **native / Lance `ivf_rq` kernel**：收益同量级，代价是 native 依赖与版本/平台矩阵，且与现有
  `VectorKernel` 抽象的资源口径（`estimatedBytes`、缓存驻留单位）要对齐，放在分片之后。
- **减少拷贝与读取**（`prepare` 的整块 `toBytes` 拷贝、`intAt` 的逐 4 字节边界检查）：
  与格式/缓存路径相关，属于路线图 §2 的范围，本卡不动。

判定顺序：先测「码扫描占端到端时间的比例」（profile 分段）→ 若 <20%，不做指令级优化；
若 ≥20%，按「LUT/地址（仅当实测正收益）→ 位平面 → SIMD」推进，每一步都以同输入 bit 级等价与
recall@K 对照作为门槛。

## 7. 测试与证据

### 7.1 评分等价性（index-vector）

- `TestRaBitQQuantizer.thePreparedQueryScoresTheSameBitsAsThePerCandidatePath`
  （`TestRaBitQQuantizer.java`）：dimension 129（padded 256，覆盖补零与跨字节位）、bits ∈ {1,2,4,8}、
  60 个候选、4 条查询；逐候选断言 L2 距离、内积、余弦分数的 `Double.doubleToLongBits` 与
  逐候选旧路径相等，并断言 `|c|²`/`|q|²` 两个常量与旧路径的内联计算 bit 相等。
- `TestIvfInPlaceSearch.readsACodeInPlaceExactlyAsTheCodeArrayWasRead`：三种度量各跑一遍
  prepared searcher（新路径），与用旧的逐候选方法算出的参考排序逐 rank 比较行位置与分数 bit。
- 既有 `scoresEveryRecordTheWayTheExactScanDoes`（全探测 vs 精确扫描，三种度量）继续通过，
  说明新路径与「精确答案」的距离没有被改变。

### 7.2 精排（index-spark + spark-extensions）

- `TestVectorRefine`（10 个用例）：精确排序的次序与 `(distance, file, pos)` tie-break；
  默认关闭、打开后只对 `IVF+RABITQ` 生效；投影裁掉向量列/`k<=0` 时不生效；窗口大小
  （`max(opening, c·k)`、不超过文件条目数、不窄于调用方窗口）；非法属性值报错；
  从行里取向量并算精确距离（含 null 向量 → `+∞`）。
- `TestVectorSearchExecution.aQuantizedSearchReRanksItsFetchedCandidatesExactly`（Spark 3.5 定向）：
  - 1 bit、两维、`nlist=1`、20 行的确定性 fixture；估计分的第一名**不是**真正最近行；
  - 精排关闭：SQL 重写路径返回错误行，`numCandidates=1`、`numCandidateRequests=2`；
  - 精排打开：同一查询返回与「不重写、全表扫描」完全一致的行，`numCandidates=8`、
    `numCandidateRequests=8`、`numLiveCandidates=1`，即窗口与回表量有记录。
  - 该用例同时证明「关闭时与现在一致」的指标面（其他既有指标断言也全部保持通过）。

### 7.3 跑过的套件

| 命令 | 结果 |
|---|---|
| `:iceberg-index-vector:test` | 194 tests，0 失败 |
| `:iceberg-index-spark:test` | 414 tests，0 失败 |
| `:iceberg-spark:iceberg-spark-3.5_2.12:compileScala` | 通过 |
| `:iceberg-spark:iceberg-spark-extensions-3.5_2.12:test --tests TestVectorSearchExecution` | 24 tests，0 失败 |

（`--offline` 在解析构建插件时缺缓存依赖，按约定去掉重试一次；其余同上。）

## 8. 未完成与后续

1. **没有在 10.78M 行真实负载上跑 §5 的对照**：本环境没有该表/集群。实验方案与指标口径已固定，
   等待在目标环境执行；`c` 与是否默认开启由那轮结果决定。
2. **精排只覆盖单查询的 SQL 重写路径**。`index_hits` 按契约保留估计分与顺序；批量入口的每查询窗口
   若将来需要在任务内精排，需要先让 `searchWithRows` 的返回形状携带 `query_id`（现在一个 task 的
   top-k 会把多条查询的命中混在一起），那是另一个设计。
3. **没有格式/布局变更**，也没有 LUT/SIMD/FastScan/native 的落地；§6 给了判定阈值与顺序。
4. 精排没有新增 SQL 指标（只有既有 `numCandidates/numCandidateRequests/numLiveCandidates` 记录窗口与
   回表行数）；「回表字节」的精确值依赖存储端计数器，未在本环境采集。
