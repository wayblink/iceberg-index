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

# Iceberg BM25 的 Spark 执行设计

## 查询契约

`search(table, query, search_columns, limit, columns)` 返回指定 Iceberg 表快照中单个文本列的正分匹配，最多返回 k 行，并带有 `_score`。查询固定表快照和索引元数据。一个精确的业务行查询要求索引覆盖该快照的所有数据文件，并且每个分片都可读。索引缺失、落后或覆盖不足时，报告原因及可执行的 refresh 操作。同分以数据文件路径和行位置排序。

所有分片使用同一份快照语料统计。查询沿用文本索引的分词、重复查询词计分、空文档统计、k1 和 b。对同一快照，业务过滤只改变文档的入选资格，DF 和平均文档长度仍按快照语料定义。TVF 外层的 SQL WHERE 过滤 TVF 已选出的结果。`filter => 'tenant_id = 42'` 显式指定检索前过滤，支持引用顶层标量列的确定性布尔表达式。

`CALL text_search`、批量过程和 `text_index_search` 仍返回索引命中地址，保留它们对索引历史和无法回表的行的现有行为。原有 `match` 和 `ORDER BY text_score` 优化规则有独立的 SQL 契约。特别是后者可能需要补入零分行。

## 执行路径

```mermaid
flowchart LR
    P["规划：固定表与索引快照"]
    D["读取快照统计：N、总 token 数和查询词 DF"]
    Q["并行读取词项倒排，在分片内选 top-k"]
    G["按分数、文件和行位置归并"]
    F["针对全局命中按位置回表"]
    P --> D --> Q --> G --> F
```

规划期只读表、索引及覆盖元数据。物理执行从已发布的快照统计文件读取所需词项的统计，再广播给评分任务。每个任务处理一个索引文件。批量查询共享统计读取，并在每个分片只解码一次查询词倒排。局部 top-k 和全局归并使用同一个排序。Spark driver 接收最终的命中地址，业务行只对全局命中回表。

检索前过滤仅用语义兼容的 Iceberg 分区和文件统计裁剪评分任务。评分任务只读过滤列，按 Spark SQL 的规则求出合格位置，与当前快照的倒排交集后再打分。发布的全局统计包括未进入评分阶段的分片。已有的精确快照覆盖检查和回表时的删除检查继续适用。

## 后续决策

| 决策 | 比较方式 |
|---|---|
| 词典与 postings 布局 | 对照 Lucene 分片和可按词定位的自研格式。分别查询低频词和高频词，记录冷读请求数、读字节和 p95。 |
| 全局词典构建与刷新 | 对照相同快照的无过滤、强分区过滤和批查，同时记录全局词典构建与刷新的成本。 |
| 评分剪枝 | 以完整评分为真值。只有带安全上界的 Block-Max WAND 等优化可用于精确模式。 |

## 实施与验证

1. `TextIndexAdapter` 拆出分片统计汇总和按全局统计打分。分片单测验证跨文件排名与旧集中式实现完全一致。
2. `DistributedIndexSearch` 读取快照统计并执行 Spark 并行评分与批查归并。集成测试要求多文件、多个 k 值、空命中和统一的同分排序。
3. `search(...)` 绑定快照后生成逻辑叶子和物理算子。验证 EXPLAIN 不启动 Spark job、实际搜索只回表全局命中，且默认构建的索引可直接使用。
4. 增加检索前过滤和对应的删除、偏斜与 compaction 验收。对范围读取和全局统计布局的性能选择另作实验记录。

BM25 使用单一 v1 存储布局。验证沿用 `:iceberg-index-text:test` 和 Spark 3.5 的定向集成测试。

## 范围读取初测

用 [独立 Java 探针](../experiments/Bm25RangeProbe.java) 生成 20 万篇等长文档，每篇 16 个 token，词表 4,096 个词。分别查询 DF 为 1,000 和 160,000 的两个词，取 top‑10。对照组使用 Lucene 9.12.3 单 segment 和带 term offset 目录的未压缩 postings，每条 posting 为 4 字节 doc ID 加 1 字节 TF。两者使用同一个本地 HTTP Range 服务，块缓存每次查询重新创建。请求数包含 Lucene 打开 reader 时发出的 Range GET。文件列表和长度由本地文件系统提供，未计入请求数。

| 读块 | 查询 | Lucene GET / 字节 | 固定宽度 GET / 字节 |
|---:|---|---:|---:|
| 4 KiB | 低频词 | 8 / 18,017 | 3 / 12,288 |
| 4 KiB | 高频词 | 36 / 132,705 | 197 / 806,912 |
| 64 KiB | 低频词 | 6 / 165,473 | 1 / 65,536 |
| 64 KiB | 高频词 | 8 / 296,545 | 14 / 917,504 |
| 256 KiB | 低频词 | 6 / 558,689 | 1 / 262,144 |
| 256 KiB | 高频词 | 6 / 558,689 | 4 / 1,048,576 |

六组 top‑10 的文档及顺序均一致。等长文档使单词 BM25 的排序只取决于 TF，这里验证返回顺序。Lucene 和固定宽度镜像分别为 6,063,713 和 14,820,817 字节。64 KiB 时固定宽度布局有低频词冷读优势，Lucene 在高频词上少读约三分之二的字节。256 KiB 时高频词的 GET 数反转，说明需要同时衡量请求和字节。

下一轮用实际对象存储上的 Iceberg FileIO，对照 BM25 v1 和 Lucene。测量分片打开、全局统计归并和评分分别产生的请求数、字节和 p95，语料加入可变文档长度、多词查询和强分区过滤。用完整查询的成本评估 v1 的收益。

## v1 存储实现

Spark 默认写入 `iceberg-text-bm25-v1`。Puffin blob 保留可定位的字节偏移，内部有分桶词典和独立压缩的倒排。Spark 把完整语料的文档数、token 数和按词分桶的 DF 写入快照统计文件，与索引快照一起发布。增量刷新可复用未改变数据文件的分片。实验格式的文件需要重新构建。

构建和刷新从有效分片集合读取词典并归并词项统计。Spark 并行汇总，最终的词典由 driver 编码为一个文件。刷新耗时和 driver 内存会随整张表的词表增长。

集成验收用 Spark job 数验证持久化统计省掉查询时的分片归并，用对照查询验证分数和顺序。对象存储的 p95 仍需实测。

使用 JDK 17，将 `lucene-core-9.12.3.jar` 与 `lucene-analysis-common-9.12.3.jar` 放到当前目录。在仓库根目录执行：

```sh
java --add-modules jdk.httpserver -Xmx1g \
  -cp 'lucene-core-9.12.3.jar:lucene-analysis-common-9.12.3.jar' \
  docs/superpowers/experiments/Bm25RangeProbe.java
```

参考 [Elasticsearch 的分布式评分](https://www.elastic.co/docs/solutions/search/full-text/search-relevance/consistent-scoring)、[Lucene 的词典接口](https://lucene.apache.org/core/9_12_3/core/org/apache/lucene/index/TermsEnum.html) 与 [Tantivy 的分段结构](https://github.com/quickwit-oss/tantivy/blob/main/ARCHITECTURE.md)。
