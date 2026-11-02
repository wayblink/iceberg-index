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

# 索引语义对齐差距：已一致的部分、仍缺的部分，以及 REFRESH／OPTIMIZE 的语义定案

本文回答一个问题：**我们的索引 SQL 语义和 Lance-Spark 还差哪些**。它同时记录一条已经确认的语义决策——`REFRESH` 是补段、`OPTIMIZE` 是合段与重分布——以及这条决策对现有实现的冲击。文中“已实现”指 `card/2821` 上的代码与测试（`47587b1aa`、`90980cdeb`、`6148dea7d`、`a43103d76`、`dbe993d12`、`647416e4a`、`23c17f10b`），不是主线交付，也未做集群验证。

## 1. 已经对齐的语义

| 语义 | 我们的做法 | 与 Lance 的关系 |
| --- | --- | --- |
| 索引动作的写法 | `SHOW INDEXES FROM`、`ALTER TABLE … CREATE/REFRESH/DROP INDEX` | 骨架相同，复用同一套解析与名称寻址 |
| 索引名的唯一性与歧义 | 同表内按名或 UUID 找唯一索引，跨列同名在分析期报歧义 | 比 Lance 更严格：它按名取第一个匹配 |
| 显式度量的权威 | 查询写了 metric，只在索引 metric 相同才接管，否则退扫描 | 与 `indexMetricIsUsable` 同构 |
| 未写度量时的行为 | 跟随索引自身的 metric | 与 Lance 一致 |
| 返回的业务行 | `vector_search`／`search` 返回表列 + 距离／分数 | 与 `VECTOR_SEARCH`／`SEARCH` 一致（列形状） |
| 按名字排序分数 | 排序键按属性 exprId 解析回产生它的别名表达式 | 相当于 Lance 直接按 `_score` 排序 |
| 不认识的参数 | 明确报错 | 比 Lance 严格，它部分参数会静默忽略 |

## 2. 查询面缺口：Lance 能表达、我们目前直接拒绝

| 能力 | 现状 | 影响 | 建议 |
| --- | --- | --- | --- |
| `filter` + `prefilter` | 无参数；业务 WHERE 位于 LIMIT 下时规则放弃改写 | **语义缺口**，用户会把“先取后筛”误当“筛后取” | #2849 过滤模式（见第 5 节） |
| `offset` | 未在允许键内，报错 | 分页要自己写 | 随 typed 接口补齐 |
| `version` | 只能查当前快照 | 结果不可复现，无法对齐历史读 | 需要把快照绑定参数化 |
| `_rowid` / `with_rowid` | 无 | 无法定位行 | 与 typed 输出一起加 |
| `nprobes` / `ef` / `refine_factor` | 写在索引 definition 里，改要重建 | **语义差异**：Lance 是查询期旋钮 | 需要把探测预算从定义里拆出来 |
| `fast_search` / `bypass_vector_index` / `bounds` | 无 | 无法表达“只要索引里的”“绕过索引” | 后置 |

## 3. 结果形状：数值与列语义

| 项 | 我们 | Lance | 风险 |
| --- | --- | --- | --- |
| `_distance` / `_score` 类型 | `DOUBLE` | `FLOAT` | 同数据末几位不同，跨引擎比对要容差 |
| L2 距离定义 | 平方和**开根号** | 平方和 | **阈值不能互通**，非负序一致 |
| `_score` 的列语义 | 普通函数与别名 | 隐藏 metadata 列：`SELECT *` 不含、建 FTS 时同名列会被拒 | 我们的 `SELECT *` 行为不同 |
| NULL 向量域 | 索引与残差跳过 NULL，SQL 需 `IS NOT NULL` 才与全扫可比 | 索引只含非空向量 | 需在文档与测试中显式声明可比域 |

## 4. 管理面与内核能力

管理面的差异不是写法，而是**索引身份与物理维护**：

| 差异 | 说明 |
| --- | --- |
| 文本索引没有名字 | 同一列只能有一个文本索引，`REFRESH`／`DROP` 无法按名寻址；这是身份语义缺口，不只是语法 |
| 标量索引类型 | Lance 有 btree/bitmap/zonemap/ngram/bloomfilter/rtree，我们没有任何一类 |
| CREATE／SHOW 返回列 | 我们返回 `index_uuid, source_snapshot_id, index_files, entries`；Lance 返回 `fragments_indexed, index_name` 与覆盖行数、百分比、段数 |
| `DROP` 的文件处理 | 我们默认连文件一起删（`purge`），Lance 只摘登记、文件留给 VACUUM |

内核能力的缺口只能靠引擎工程解决：`match` 的 `operator=AND`／`fuzziness`／`boost`、短语所需的位置索引、`multi_match` 的多列统计合同、`HYBRID_SEARCH` 的 RRF 融合。这些不属于语法对齐。

## 5. REFRESH 与 OPTIMIZE：定案与冲击

**决策（2026-10-30，#2871）：`REFRESH` = 补段 + 重建「内容变过的数据文件所在的组」；`OPTIMIZE` = 合段、重分布、重训。** 划分依据是「这条维护能不能在没人看着的时候跟着写入跑」：补段与按组局部重建都能，合段和重分布不能。

改动过的数据文件必须归 `REFRESH`，不能推给 `OPTIMIZE`：删了行如果不重建，查询会继续返回已经删掉的行，文本索引还会把这些行计进 df/avgdl 使分数也算错。代价是 delete 之后一次 `REFRESH` 从「纯补段」变成「补段 + 按组局部重建」：只重建内容真的变了的组（一个索引文件记录整批数据文件，不能只改一半），不是全量、也不是整组重排。组的大小由 `max_entries_per_index_file` 决定——很多小文件会合成一个组，这时刷新近似全量。

这条语义已在 1.9 基线上落地（`Index: Re-index a group whose data file changed`）：新增/未覆盖的数据文件只补段；出现过删除文件、或被重写换名的数据文件，其整组重建；被删空的数据文件重建后 entries=0，由空覆盖文件记成覆盖，索引因此能绑定新表快照；三者都为空时才返回原 metadata。

`OPTIMIZE` 的三种强度建议这样分（名称与取值待确认，本轮不实现、不加参数）：

| 强度 | 做什么 | 代价 | 适用 |
| --- | --- | --- | --- |
| 合段 | 把若干段合并为更少的段，不重训模型 | 重写被合并段 | 段数膨胀、查询扇出变大 |
| 重分布 | 重新划分段的边界/桶归属，使数据分布贴合查询与过滤 | 最重，可能读全部被重排数据 | 数据分布漂移、选择性过滤变差 |
| 重训（可选，更后） | 重新训练 IVF 质心或量化码本 | 最重且会改变召回特征 | 数据分布显著变化 |

在这条语义下，我们的引擎目前**只有「补段」与「按文件组重建」两种物理动作**，因此 `OPTIMIZE` 的所有强度都还没有实现基础：合段需要新的段合并器，重分布需要能重划段的边界，重训要替换已有的 IVF 质心或量化码本。这决定了命名先行、实现后置的顺序：`REFRESH` 的语义已经定下并落地，`OPTIMIZE` 仍是待实现的能力。

## 6. 建议的推进顺序

1. **#2849 过滤模式**（`scan`／`post`／`pre`）：把“先取后筛”和“筛后取”变成用户能表达的选择；默认仍是 `scan`，零回归。它同时回答了 Lance 的 `prefilter` 语义。
2. **`REFRESH` 语义**：已定案并落地（补段 + 内容变更组的局部重建），剩余工作是按新语义补齐验证（#2865）。
3. **文本索引命名**：补齐索引身份，让 `REFRESH`／`DROP` 能按名寻址，也为多文本索引留出空间。
4. **`version` 与 `_rowid`**：结果可复现与可定位，属于 typed 接口的完整性。
5. **`OPTIMIZE`**：在 1–4 之后再引入合段、重分布、重训三档。
6. 内核能力（AND/fuzzy/短语/多列/融合）独立立项，不与语法对齐混在一起。

相关文档：[完整索引体系对比](2026-10-28-lance-spark-ann-comparison.md)、[SQL 用法与语法对比](2026-10-28-lance-spark-sql-syntax-comparison.md)、[对齐可行性](2026-10-28-lance-spark-sql-alignment-feasibility.md)、[内核复用](2026-10-28-lance-index-kernel-reuse.md)。
