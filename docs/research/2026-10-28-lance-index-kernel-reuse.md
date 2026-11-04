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

> **口径说明（2026-11-04）**：本文是外部调研/对比记录，用来判断要不要借鉴某项能力，**不构成产品语法口径**。产品面向用户的文档里不再把语法的来源写进口径（见 #2945）。


# Lance 索引内核复用可行性：保留 Iceberg 管理，借用索引算法

**状态：研究建议，非实施批准。** 本文属于 #2818 的文档交付；只读核查接口，没有加入依赖、编写 JNI、运行 POC 或测量性能。下述 probe 均待另行批准，不构成开发排期或收益承诺，也不提出新的用户参数、SQL conf 或表属性。

研究冻结我方 `83b1af39fa4200c926f23834fd6edd62ee35bd8d`、Lance 12 源码 `cbeec97cb893a66ad6b3db87f25a57ab0093f358`。外部依据为固定版本源码，研究没有扩大为全库评估。文末固定 SHA 链接可复查；我方链接相对本文所在目录，行号对应冻结版本，而非未来主线。

## 一、推荐什么，以及不推荐什么

**推荐先验证“Rust lance-index／lance-index-core + 薄 JNI + 我方 sidecar 管理”。** 目标不是换掉 Iceberg，而是让成熟倒排、图和量化算法替代一部分自研内核。Iceberg 仍决定哪些数据属于当前快照、索引覆盖哪些文件、删除怎样生效、结果怎样回表。Rust crate 作为版本固定的依赖复用，不复制源码；薄 JNI 只是缩小跨语言边界，不意味着工作量小，更不是现有 Java 接口的直接替换。

业务交付优先 FTS，完整 ANN 后置。搜日志中的“ERROR 1064”可以先受益于倒排检索；在一亿条向量中找最像的 100 条，还涉及分区训练、候选召回、过滤和重评分，不能因为都有 Arrow 输入就视为同一接入难度。建议用 BTree 小 probe 验证通用存储与 JNI 胶水，因为它的训练、文件输出和重开边界清楚；这不是把业务优先级改成 scalar，也不批准先建设完整标量产品。

| 路线 | 作用与便利 | 主要代价及反例 | 判断 |
| --- | --- | --- | --- |
| 每个 group 一个 Lance Dataset，调用 SDK | 现成创建、训练和查询入口，调用路径较短 | 同时维护 Iceberg 与 Lance manifest、版本及发布；投影到 Dataset 通常重复索引列数据；各 Dataset 的 BM25 统计不自动成为全表统计 | 非默认推荐，仅适合能接受双重管理的隔离场景 |
| Rust 内核 + 薄 JNI + 自有 sidecar | 保留 Iceberg 文件与快照管理，借用算法和索引文件读写 | Arrow、文件 store、row map、统计、释放及 ABI 都要适配；部分接口属内部 API | 最值得验证，但尚未证明可交付 |
| 复制源码后自行改造 | 短期可直接改内部边界 | 算法修复、文件兼容与安全补丁需自行跟进，分叉越深越难回流；一次升级就可能重做本地改动 | 不推荐作为起点；不能把分叉成本藏在“移植”一词里 |

Dataset 路线并非不能工作。例如每个 group 都导出文本和向量，SDK 能在自己的版本里建索引；但 Iceberg 发布成功、Lance 发布失败时，必须处理两边状态不一致。搜索两个 group 再合并局部 BM25 top-k，也可能让少量文档中罕见的词被过度加分，排序不是全表排序。因此“SDK 调用简单”不等于产品正确性简单。

## 二、实际接口支持哪一层复用

**标量与 FTS 已有不以 Dataset 为参数的入口。** `train_btree_index` 接受已排序的 `SendableRecordBatchStream`、`IndexStore`、批大小及可选 fragment/range 标识，返回索引文件列表；输入须满足排序、值列和 UInt64 row ID 契约，不能把任意批流直接交进去。[1][2] `BTreeIndexPlugin` 的 `ScalarIndexPlugin::load_index` 实现以 store、索引详情、row remapper 和 cache 重开索引（`btree.rs:3354–3362`），说明存在独立训练与加载的接缝，而不是只有 Dataset 上的建索引命令。

`InvertedIndexPlugin::train_inverted_index` 接受 Arrow 流、store、倒排参数、fragment IDs 和 progress，返回带 details/version/files 的 `CreatedIndex`，签名没有 Dataset。`InvertedIndex::load` 同样只需要 store、remapper 和 cache。[3][4] 这足以支持“可以验证独立 FTS 接入”，但不足以承诺现成 Java standalone API 或完全不改我方格式。

**IndexStore 是索引文件协议，不是裸 byte I/O。** `scalar.rs:150–303` 定义的 reader/writer/store 涵盖 RecordBatch 写入与按批、按行范围、投影读取、schema、元数据以及 global buffers；随后还有跨 store 复制、换名复制、rename、delete、文件大小列表和 I/O 优先级。[1] 我方需要满足实际使用的能力，默认“不支持”实现不能冒充完整可用。把它理解成“提供 read/write 两个字节方法”会漏掉 FTS 分区加载、训练收尾与持久化重开所需协议。

**向量算法能拆，完整 IVF builder 尚不能视为脱钩。** `Quantization::build` 的输入是 `&dyn Array`、距离类型和训练参数；其量化类型覆盖 Product、Scalar、Rabit 等，PQ/RQ 可从算法级验证。[6] `HnswBuildParams::build` 接受 `ArrayRef`，内部构造向量 storage 再建图，提供另一条不要求 Dataset 的算法入口。[7] 但完整 `IvfIndexBuilder` 虽在 `builder.rs:271` 附近保存 `Option<Dataset>`，`new` 在 377 行附近仍必需 `Dataset`，并从中取得 object store 和文件版本。[8] 一个字段可空只说明某些内部阶段允许无 Dataset，不能推导完整训练、shuffle、分区存储和重开已经独立可用。

现有 JNI 也不是捷径：`blocking_dataset.rs:1039` 的 `nativeCreateIndex` 收取 Java Dataset 对象，1285 行附近从其 native field 取出 `BlockingDataset`，随后 `execute_create_index` 接受 `&mut Dataset`。[9] 就这些入口而言，没有可直接接到我方 adapter 的通用 standalone 索引 Java API；允许传 Arrow stream 并未取消 Dataset 对象。

## 三、保留什么，复用什么

```text
Iceberg 快照 / immutable inputs / coverage / 删除 / 发布与回表
                         │ 我方保留
                         ▼
我方 IndexAdapter：查询契约、全表统计协调、跨 group 合并
                         │ 需适配，不是现成接口
                         ▼
薄 JNI：Arrow 交换、句柄、错误、native 内存与释放
                         ▼
Rust BTree / FTS / HNSW / PQ、RQ 算法 + IndexStore
                         │
                         ▼
我方管理的 sidecar 文件集合 + row ID 映射 + 格式版本
```

我方 [`IndexAdapter:39–169`](../../core/src/main/java/org/apache/iceberg/index/IndexAdapter.java#L39-L169) 已将单 payload 检索、跨 payload 检索、比较器与 prepared search 分开；[`VectorKernel:66–108`](../../index-vector/src/main/java/org/apache/iceberg/index/vector/VectorKernel.java#L66-L108) 则是 `build(List<IndexEntry>, int parallelism) -> ByteBuffer` 和 `prepare(ByteBuffer)`。Lance store 管理原生索引文件集合，和 Puffin blob 并不互换。将文件整体塞进一个 payload 可能失去按范围读取的价值；引入文件集合引用又会影响 adapter、payload 和加载路径。所以这两个接口是适配起点，不是 drop-in 证明；不得承诺 metadata 无需修改，具体变化须另行设计和批准。

row ID 必须由我方定义、持久化并验证：传给内核的 `u64` 需映射到 Iceberg 的 `(data file, row position)`，不能直接当全表行位置或默认套用 Lance fragment 编码。HNSW 的节点 ID 是基底层索引坐标，图的 `entry_point` 为 `u32`（`vector/hnsw/builder.rs:195–210`）；它不是 Iceberg 的文件/行身份。应通过局部映射表转换，并在适配器入口检查节点容量和整数可表示性，禁止溢出或截断业务 ID。覆盖、删除、索引发布和 immutable inputs 的认定仍属于我方；即使算法训练完成，也不能自动使其成为当前快照可见索引。

## 四、FTS 的难点在全语料，不只在倒排文件

我方 [`TextIndexAdapter:137–261`](../../index-text/src/main/java/org/apache/iceberg/index/text/TextIndexAdapter.java#L137-L261) 写出局部 postings、文档长度和行映射，再在查询时合并统计；[`searchStatistics:301–309`](../../index-text/src/main/java/org/apache/iceberg/index/text/TextIndexAdapter.java#L301-L309) 供回表后的评分共享同一语料口径。替换倒排实现不能丢掉这一合同，否则索引返回的分数和查询表达式计算的分数会不同。

Lance `build_global_bm25_scorer` 在 `inverted.rs:381` 起聚合多个 segments 的统计，其实调用 `prepare_bm25_query`；后者将最终 token vocabulary 与 scorer 放进 `PreparedBm25Query`。[3] 必须提供目标语料的全部相关 segments 或等价完整统计，不能只给某个 task 当前搜索的文件。模糊检索更不能只广播 scorer：各 group 若各自扩词，同一个拼写错误可能得到不同候选词，词项频率与实际检索词不再一致。需要共同的扩词结果及查询位置语义，公开但标记 `doc(hidden)` 的入口也不代表稳定外部契约。

`BooleanQuery` 的 `should/must/must_not` 是 FTS 子句组合（`scalar/inverted/query.rs:669–712`），不是业务 SQL filter，也不是 Iceberg delete。比如“文本包含崩溃，而且租户是 A，并排除已删除行”，文本组合只能解决第一部分；后两部分仍要由我方生成并验证行掩码及 row map。`PreFilter` 明确用于删除和预过滤，并声明无 API 稳定保证。[10] ANN 过滤发生的阶段还可能改变候选数量和召回，不能以结果最后过滤替代原有合同。

## 五、能力的相对难度与优先级

下表是接口接入的相对难度，不是性能预测，也不包含“几天完成”的估算。低难度仅表示依赖边界较容易验证，不代表上线风险低。

| 能力 | 相对难度 | 可复用部分 | 仍需承担的关键工作 | 顺序 |
| --- | --- | --- | --- | --- |
| BTree 小 probe | 较低 | sorted stream trainer、插件加载、范围搜索 | 排序输入、store 持久化、row ID 与释放 | 先核胶水，非业务转向 |
| FTS 精确词项与短语/组合 | 中高 | 分词、postings、检索执行 | analyzer 一致性、全语料 BM25、filter/delete、评分合同 | 业务优先 |
| FTS fuzzy 跨 group | 高 | 扩词和 prepared query | 共享词表及位置、全局统计、分布式一致性 | FTS 分阶段验证 |
| PQ/RQ、局部 HNSW | 中高 | Arrow 量化训练、图构建与搜索 | storage、局部节点映射、native 记账、重评分 | 算法级先行可研究 |
| 完整 IVF + ANN | 很高 | IVF、shuffle、子索引算法 | Dataset 边界、分区模型、文件加载、过滤召回及兼容 | 完整交付后置 |

## 六、待批准 probe、风险与退出条件

最小 probe 建议先以小规模、已排序 BTree 输入完成训练、持久化、释放全部句柄并从文件重开，随后比较范围查询真值。必须用非连续 `u64` row IDs，核查返回映射、空值、边界、delete/filter，以及失败时不发布半成品。它只验证共同胶水；BTree 通过不能证明 FTS 或 ANN 通过。FTS 另需两个统计分布明显不同的 group，核跨 group 统计、统一扩词与回表评分；ANN 另核局部节点、候选过滤和距离方向。以上均为待批准范围，本轮未执行。

Arrow 交换要核所有权、生命周期和是否实际发生复制；native cache、训练临时内存与 Rust 运行时线程必须纳入我方记账，释放须覆盖正常关闭、失败和取消。现有 `PreparedSearch` 只有搜索和估算接口，不能假定它天然提供 native free 合同。文件格式版本、JNI ABI、crate 版本要一并约束；Linux/macOS、不同架构的 JNI 制品、依赖体积及镜像分发也都是接入成本，而非一个 Java jar 的问题。

Apache-2.0 允许在遵守许可、归属和适用 NOTICE 要求下复用，不等于所有传递依赖已经审计，更不保证 API 稳定。`VectorStore` 在 `vector/storage.rs:412–417` 明示内部 API，`PreFilter` 同样无稳定性保证。[10] 若必须长期分叉才能训练/重开、不能保持全表评分与删除合同、释放和内存无法可靠验证，或版本升级需要不可承受的重写，应停止扩展该路线，保留现有内核作为退路。推进的门槛是可复现的正确性和接入证据，不是“Rust 更快”或“Lance 已经有这个功能”。

相关文档：[完整索引体系对比](2026-10-28-lance-spark-ann-comparison.md)、[Lance-Spark SQL 用法与我方语法对比](2026-10-28-lance-spark-sql-syntax-comparison.md)。前者解释数据与索引的管理边界，后者区分内核功能、SQL 表达与执行合同；本文只判断内核复用的接缝，不把新的 SQL 或依赖视为已批准。

## 固定源码证据

以下均固定到 Lance SHA `cbeec97cb893a66ad6b3db87f25a57ab0093f358`；范围含入口或对应协议。伴随文件的精确位置已在正文注明，不据此宣称全库没有其他路径。

1. [IndexWriter / IndexReader / IndexStore，scalar.rs:150–328](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index-core/src/scalar.rs#L150-L328)。
2. [BTree stream trainer，btree.rs:2515–2611](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/scalar/btree.rs#L2515-L2611)；同文件插件加载见 3354–3362。
3. [PreparedBm25Query、全局统计与 FTS trainer，inverted.rs:43–438](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/scalar/inverted.rs#L43-L438)。
4. [InvertedIndex::load，inverted_index.rs:365–469](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/scalar/inverted/index/inverted_index.rs#L365-L469)。
5. [BooleanQuery，query.rs:669–712](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/scalar/inverted/query.rs#L669-L712)。
6. [Quantization Arrow 接口，quantizer.rs:32–65](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/vector/quantizer.rs#L32-L65)。
7. [HNSW Arrow 构建，builder.rs:166–203](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/vector/hnsw/builder.rs#L166-L203)。
8. [完整 IVF builder 及 Dataset constructor，builder.rs:271–419](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/index/vector/builder.rs#L271-L419)。
9. [Dataset JNI 建索引及 native 对象调用，blocking_dataset.rs:1039–1338](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/java/lance-jni/src/blocking_dataset.rs#L1039-L1338)。
10. [PreFilter 与稳定性警告，prefilter.rs:10–69](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/prefilter.rs#L10-L69)。
