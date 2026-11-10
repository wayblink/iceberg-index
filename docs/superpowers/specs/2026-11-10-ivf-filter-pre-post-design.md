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

# IVF 索引的带过滤检索：前过滤与后过滤设计

日期：2026-11-10。关联：#3013（第一代实施）、#3014（第二代优化器）。

状态：待用户评审。本文件只覆盖第一代基本能力，第二代（三档选择器、稀疏候选精确打分、代价模型）在 #3014 另行设计。代码调查基线为 `dabb19dfea`。

范围收窄决定（用户 2026-11-10）：**只考虑 IVF**，先做基本能力，再做优化器。

## 1. 今天的行为，以及为什么要改

`RewriteVectorSearch` 的准入是一份白名单，只允许"向量列非空"这一种守卫：

```scala
// RewriteVectorSearch.scala
def guards(column: String): Boolean = filters.forall(filter => isVectorGuard(filter.condition, column))
if (!chain.guards(column)) return None
```

也就是说，只要查询里出现 `category = 'x'`、`price < 100`、`ts > ...` 这类过滤，整条查询就退回 Spark 全表扫描，索引一个字节都不读。而真实查询几乎从不只按向量搜：完整条件是"在这个租户、在售、价格低于 100 的里面找最像的 10 个"。今天这类查询完全没有索引收益，驻留、构建提速、宽度语义全都不起作用。

本设计让 IVF 索引在这类查询里**参与候选生成**，而不是整条让位。

## 2. 目标与非目标

目标：

- 带过滤的向量查询可以使用 IVF 索引，结果与今天（全表扫描后精确计算）在"返回哪些行、顺序如何"上一致，或者在无法保证时如实报告不足。
- 两种链路：前过滤（候选集先在，检索时跳过非候选）、后过滤（先检索，回表时过滤不足则扩样）。
- 不新增用户可调参数（沿用既有约束：多一个旋钮就多一处运维要理解、多一种静默退化）。

非目标：

- **不做 HNSW**。`hnswlib-core 1.1.0` 的 `Index.findNearest` 只有 `(TVector, int)` 一种签名，全库无过滤参数（已用 javap 核对 `Index`、`HnswIndex`、`HnswIndex$ExactView`）。要让 HNSW "遍历时跳过非候选"，只能自己改图遍历，等于不用这个库的检索。HNSW 的过滤另议，很可能走"超取 + 后过滤"。
- **不做优化器**：不做基于代价的三档选择，不做稀疏候选的精确打分，不做 NDV/直方图统计依赖。第一代用一条固定的内部规则选链路（见 §6）。
- 不做文本/BM25 与标量索引的过滤。索引条目仍然不带标量属性位（条目级属性是"向标量混合索引"的事，不在本代）。

## 3. 两条链路的语义边界

两条链路解决的是同一个问题的两端：过滤在检索之前还是之后。

**前过滤**先把标量条件作用到行上，得到"可能进入答案的行"的集合，再让 IVF 只在这个集合里找。它的价值是**不浪费候选名额**：检索阶段不会把 topK 位置花在注定被过滤掉的行上，而且一条倒排 list 里若没有候选，整条 list 可以跳过，省下的是读取与打分的实打实成本。

**后过滤**先正常检索 `topK × 放大倍数` 个候选，回表读取时按标量条件筛，凑不够 k 就扩大候选集再来一轮。它不需要额外的列扫描，但每个被过滤掉的候选都是一次白读。

两条链路都必须解决"过滤后不到 k 条"这个事实。今天的带过滤查询是全表扫描后精确计算，所以**总是**恰好 k 条（数据够的话）。换成索引路径后，第一代的口径是：

- 继续扩样/扩探，直到凑够 k，或者候选空间耗尽；
- 候选空间耗尽仍不足 k 时，返回已找到的（少于 k），并在结果里如实标注不足，而不是假装有 k 条。

这个口径是有代价的：极端低选择性加极端高 k，扩样会把成本推高到超过全表扫描。第一代接受这个代价（它至少不比全表扫描多读一遍数据，因为候选读取就是扫描本身），第二代用代价模型决定何时放弃索引。

**近似性要说清**：带过滤的查询结果仍然是 IVF 的近似结果，不是精确 top-k。这与今天的"带过滤 = 精确全表扫"不同，属于**用户可感知的语义变化**，必须写进用户指南，不能只在代码注释里。

## 4. 候选集从哪来

第一代：**Spark 侧预扫**，复用现有读路径。

预扫是一次只读列的任务：投影"过滤条件引用的列"加元数据列 `_pos`（`MetadataColumns.ROW_POSITION`），按数据文件分组求值过滤条件，产出每个数据文件中通过的行位置集合。

选这个方案而不选"索引任务内自己读列"的理由是正确性：现有 Iceberg 扫描会自动应用 position delete 与 equality delete，预扫产出的行集合天然不含已删除行；自己另建一条读路径则要重新实现删除过滤，且与扫描计划的文件裁剪不一致。代价是多一次列扫描，过滤列很宽时（大字符串 LIKE、JSON 内部字段）这份开销可能吃掉收益——这是第二代的优化点（#3014 第 3 项）。

预扫同时充当**密度探针**：它本来就知道通过的行数，用通过行数除以索引条目数得到密度，直接用于 §6 的链路选择，不需要额外统计信息。

## 5. 候选集的表示与下发

每个数据文件一个**升序去重的 `long[]`**（行位置是 long），按 `dataFile` 索引。

选择升序数组而不是位图的原因：候选集在"过滤有意义"的场景下是稀疏的，数组体积与候选数成正比（每个候选 8 字节），位图体积与总行数成正比。10M 行、1% 密度时，数组 800 KB，位图 1.25 MB；100M 行时差距拉大到 8 MB 对 12.5 MB。密集场景（>1/32）改用位图更省，但那正是索引收益消失的场景，第一代不为它优化。

查找用**每文件一个开放寻址的 long 集合**（不用第三方库；项目约束是新增依赖要先讨论，而 RoaringBitmap 这类库不值得为第一代引入）。集合容量取"下一个大于 2 倍候选数"的 2 的幂，负载因子 ≤0.5，单次查询 O(1)。这个选择要在实现时用实测核对：检索热循环里每行一次集合查找，若开销吃掉前过滤收益，就退回"排序数组 + 二分"或按文件的位图，二者都只是局部替换。

下发方式：候选集随索引扫描任务的闭包下发（`Serializable`），不经过 driver 广播大对象。若单分区候选集超过既定水位，则放弃前过滤、改走 §6 的后过滤分支——这是内部退化，不需要配置项。

## 6. 链路选择：一条固定规则

第一代只用一条内部规则，权重是一个内部常数（不是用户参数，不出现在任何 conf 里）：

```
density = 候选行数 / 索引条目数
if 预扫成功 && density <= 0.25  -> 前过滤
else                           -> 后过滤（放大倍数自适应）
```

0.25 这个位置是刻意的保守值：密度超过四分之一时，前过滤省下的候选名额抵不过它那一次列扫描的代价，而且 IVF 的倒排 list 大概率"条条都有候选"，跳 list 的收益也消失了。这个常数必须在实现时用实测标定一次（记录在卡上），第二代用代价模型替换它。

预扫失败的两种情形都退到后过滤，不报错：过滤条件引用了无法在扫描中求值的表达式（子查询、UDF、跨表列），或候选集规模超过水位。

## 7. 前过滤在内核里的形状

`PreparedSearch`（`core/src/main/java/org/apache/iceberg/index/IndexAdapter.java:145`）现在只有一个方法：

```java
interface PreparedSearch {
  List<IndexSearchResult> search(ByteBuffer query, int topK);
}
```

新增一个带候选集的重载，并给**默认实现**（项目规范：公开接口新增方法必须带默认实现，不能破坏既有实现）：

```java
default List<IndexSearchResult> search(ByteBuffer query, int topK, IndexCandidates candidates) {
  // 不支持前过滤的内核：照旧检索，由调用方按候选集过滤，并自行处理不足 k
  return candidates.filter(search(query, topK));
}
```

默认实现必须如实表达"这个内核不支持前过滤"，因此它不承诺凑够 k；调用方看到内核不支持时应该走 §6 的后过滤分支，而不是相信这个默认实现能给出 k 条。

IVF 的实现（`IvfKernel.ListSearcher`）天然适配：`scorePlainLists` / `scoreQuantizedLists` 逐条扫 list 时已经读到 `dataFileIndex` 与 `rowPosition`，在打分前查一次候选集合即可跳过。额外的收益是**整条 list 无候选时可以直接跳过读取**，以及**同样成本下可以探更多 list**（提高 `nprobe`）来补回过滤带来的召回损失。

前过滤的扩探策略：先按当前 `nprobe` 探，凑不够 k 就翻倍探（上限为 listCount），仍未凑够则返回已找到的。

## 8. 后过滤的扩样策略

起点 `topK × 4`，不足 k 则翻倍，上限与终止条件：

- 凑够 k → 返回；
- 候选已覆盖索引全部条目 → 返回已找到的，标注不足；
- 触到内部上限（避免无界扩张）→ 返回已找到的，标注不足。

放大倍数的起点、上限都是内部常数，不是配置项。

## 9. 删除与行位置语义

索引条目是 `(dataFile, rowPosition)`，其中 `rowPosition` 是数据文件内的位置（`IndexEntry`）。已删除行的位置仍可能存在于索引里。

- **前过滤**：候选集来自预扫，预扫走现有扫描路径、自动应用删除文件，所以候选集不含已删除行，索引返回的候选也只会落在候选集里。已删除行天然不会出现。
- **后过滤**：候选可能落在已删除行上，回表时扫描会把这些行过滤掉，表现为"这次候选名额被浪费"。正确性不受影响，代价计入放大倍数。
- 两条链路都不需要索引侧知道删除文件，也不需要在检索里再查一次删除。这是把过滤放在扫描层而不是检索层带来的好处，也是选 §4 方案的主要理由之一。

行位置的另一个约束：`_pos` 是文件内位置，与 `IndexEntry.rowPosition` 同源，因此候选集与索引结果可以直接按 `(file, pos)` 对齐，不需要再转换。

## 10. 可观测性

计划的绑定日志（`RewriteVectorSearch` 现有的 `vector search plan: bind=... overSample=... uncoveredFiles=...`）增加两项：走的是哪条链路（`preFilter` / `postFilter`）、密度或放大倍数。理由是链路选择是内部规则，用户看不到也调不了，一旦结果不如预期（比如大量查询悄悄退到后过滤、放大到 8 倍），必须能从日志看出原因。

## 11. 验收判据

- 带过滤的查询不再整条退回全表扫描：计划里有绑定证据（现有日志行）。
- 前过滤：返回结果必为候选集的子集；候选稀疏（构造用例，如 1% 密度）且 k 小于候选数时，结果与暴力精确 top-k 逐行一致。
- 后过滤：放大不足时能续取，凑够 k 或候选耗尽后如实返回。
- 已删除行不出现在结果里（构造 position delete 用例，两条链路各一条）。
- 无过滤时行为与结果不变（现有全部用例通过）。
- 密度规则的两个分支都有用例：`density <= 0.25` 走前过滤、`> 0.25` 走后过滤。
- 不新增任何用户可调参数；`search(query, topK)` 的两参数路径行为不变。

## 12. 留给第二代（#3014）

- 稀疏候选（<5%）时对候选集逐个精确打分，给出**真正精确**的 top-k，不碰倒排结构。
- 用代价模型替换 §6 的固定常数，需要列的 NDV/直方图或一次小样本试探。
- 预扫的额外列扫描代价优化（索引任务内读列）。
- 语义统一：同是加过滤，稀疏档精确、中间档近似，需要产品口径与可读出的字段。
