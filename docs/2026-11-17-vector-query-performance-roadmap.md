<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# 向量查询性能优化路线图

本文以 `index` 分支的 `8207199a9` 和随后的评审修复为基线，记录四个优化方向的实现状态和验证计划。
现有索引可将多个数据文件分为一组，每组独立训练 IVF。分块格式在同一个 Puffin 文件里存储描述区和列表区，
冷查询可按范围读取，热查询可复用执行器上的整索引缓存。历史测量及其资源口径见
[向量查询性能诊断](2026-11-17-vector-query-performance-analysis.md)，当前布局见
[IVF 单文件分块与按范围读取](superpowers/specs/2026-11-17-ivf-list-range-read-design.md)。

## 1. 批量查询共享一次分布式执行

**已实现按块共享搜索。** SQL `vector_search_batch` 通过 `DistributedIndexSearch.searchBatchWithOutcomes`
在有界查询块内复用索引加载和搜索任务，按数据文件合并回表。默认的 `table` 模式还让块内查询共享对未覆盖文件的
扫描。后续搜索轮次只处理仍需扩大候选集的查询。展开设计和验收见
[批量查询共享一次分布式执行](superpowers/specs/2026-11-17-vector-batch-shared-execution-design.md)。

- **待做的分布式输出。** SQL 过程已接收查询表，结果仍在 Driver 汇集。后续可保留输入表的稳定
  `query_id`，输出可直接参与 Spark 计算的分布式结果，并用分区内聚合限制候选的内存开销。
- **待做的按列表评分。** 可先计算 `(query_id, list_id)`，再以列表为单位读取和评分。假设各查询独立且均匀选择列表，
  `nlist=707`、`nprobe=32` 时，100 条查询预期已覆盖约 99% 的列表。真实数据可能集中于少数热点列表，
  应先测列表覆盖率和负载倾斜，再决定按范围选读还是连续读。可参考 [Faiss 的批量 IVF 实现](https://github.com/facebookresearch/faiss/blob/main/contrib/README.md)。

以逐条查询为对照，检查每条查询的 topK、并列分数顺序和快照可见性。对于 10,000 条查询，记录 Spark 作业数、
各源文件的读取次数、Shuffle 体积、查询吞吐量和 Driver 峰值内存。

## 2. 冷查询减少存储请求和任务内存

选读会降低列表读取字节数，同时增加读取点。现有本地内存输入实验能说明两者的变化，存储端和端到端的延迟
需要在目标环境实测。

展开设计、实现与本地 I/O 对照见[冷查询减少存储请求与任务内存](superpowers/specs/2026-11-18-vector-cold-read-design.md)。

- 将 manifest 中已记录的 Puffin 文件长度传给 `FileIO.newInputFile(location, length)`。
  缺少长度记录的历史索引沿用现有打开逻辑。对 Iceberg `S3FileIO`，已知长度可以避免为取长度发起 HEAD。
- 统计打开索引、读 Puffin footer、读 IVF 描述区和读列表各自的请求与耗时。在保持现有格式可读的条件下，
  优先复用已取得的前缀和尾部字节，减少串行读取。相邻或间隔较小的列表可以合并读，
  合并阈值由目标存储的请求延迟、带宽和额外字节决定。
- 高探测覆盖率时比较一次性整读和有界连续窗口。当前稀疏范围缓存上限为每任务 8 MiB，
  单个超大列表或整读回退仍可能分配更大的缓冲区。以真实执行器任务并发度测峰值堆和 GC。

HDFS 与 S3 分别记录实际存储请求、客户端读字节、冷查询 p50/p95 和执行器峰值堆。
对照压缩格式整读、同一分块索引整读和选读，固定索引快照、`nprobe`、过滤和 Spark 资源。

## 3. 热查询优化评分并验证精排

展开设计、等价性与实验见[热查询评分优化与精排](superpowers/specs/2026-11-18-vector-refine-design.md)。

- 先做无需改存储格式的优化。RaBitQ 在 DOT 和 COSINE 评分时可把与候选无关的质心计算提到
  每个「查询 × 列表」执行一次，保留原有浮点计算顺序。过滤触发扩大探测时可复用质心排序。
- 将近似候选宽度与最终 `k` 分开，使用已有回表路径读取候选原向量，在确定 topK 前按索引的距离函数精排。
  逐项比较 `nprobe`、候选宽度对 recall 和回表的影响。精排能纠正已召回候选的相对次序，探测阶段遗漏的候选
  仍需通过扩大或改进探测找回。以现有 `over=0` 默认值为对照，实验不要求用户调参。
- 以 profile 判断是否进一步采用分块码布局、查表或 SIMD。可参考
  [Faiss FastScan](https://github.com/facebookresearch/faiss/wiki/Fast-accumulation-of-PQ-and-AQ-codes-%28FastScan%29)
  的布局和重排取舍。将这些改动的吞吐收益与格式迁移、构建成本和部署复杂度一起比较。

验收时先比较相同布局和探测参数下的分数及排序，再以相同 recall@K 比较打分 CPU、探测量、回表字节和总延迟。

## 4. 全局路由降低单查询扇出

独立索引组让构建、刷新和回表更易分割，但单条查询仍要向每组发起探测。原型可用同一个表快照的抽样向量训练共享粗质心，
让查询先选全局簇，再为命中的簇安排索引分片和列表读取。按文件合组、列表分块和全局路由可独立做对照，
以便量化任务调度、读取范围，以及候选量各自的收益。

展开设计、原型与扇出对照见[全局路由降低单查询扇出](superpowers/specs/2026-11-18-vector-global-routing-design.md)。

- 先试单层共享质心与簇内分片。用每簇的字节数和任务耗时评估热点簇，按需拆成多个分片。
  如果数据规模或倾斜成为限制，再考虑两级路由。
- 追加写入可沿用现有训练产物，为命中簇写不可变小分片。合并和重新训练由后台作业完成，
  通过 Iceberg 索引快照发布质心版本、分片位置和覆盖关系。查询冻结一个索引快照；
  回表继续按数据文件与行位置解析，并处理删除、重写和未覆盖文件。
- 第一轮用单快照原型验证扇出和召回。后续再定新格式、缓存粒度、刷新和重训策略，
  测量增量索引写放大、训练漂移和索引恢复的代价。

## 对照实验

先在同一份快照上比较 1、100、10,000 条查询，覆盖冷读和确认命中的热查，并加入有选择性的过滤和部分索引覆盖。
用同一距离函数的暴力检索作 recall@K 真值。记录提交、布局、过滤选择性和资源配置，报告吞吐量与 p50/p95、
Spark 作业和任务数、存储请求和字节、扫描向量数、回表字节、Shuffle 及 Driver/Executor 峰值内存。
10,000 条应同时报告整批用时与每秒完成的查询数。以上四个方向的收益以这一轮对照为准。
