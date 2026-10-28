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

# Iceberg 与 Lance-Spark 索引体系：能力、执行、维护与部署对比

**阅读导航：**先读第 1 节的分工与能力表，再按关注点深入：第 2–4 节回答“能搜什么、在哪里计算、返回什么”；第 5–7 节回答“数据变更后怎样维护、文件与缓存有什么限制、开源闭环怎样部署”；第 8–9 节给出场景选择、比较方法和待验事项。源码证据集中在文末，正文的流程图与表格用于先建立整体认识。

**我方是在已有 Iceberg 表旁自建向量、BM25、索引元数据和 Spark 接入；Lance-Spark 是让 Spark 编排 Lance 原生 Dataset 的索引与查询；LanceDB Enterprise 则是另一条服务产品路线。**三者不是同一个产品换了内核。我方保护既有表治理和文件路径，但承担算法、查询正确性及维护协议的成本；Lance 原生索引种类和文本表达更丰富，但 Spark 暴露范围小于底层能力，采用它也意味着接受 Dataset 的版本、地址和删除模型。当前最值得优先解决的不是宣称 ANN 谁更快，而是 BM25 的结果与 SQL 合同、driver 成本，以及 ANN 的过滤、回表与加载粒度。全文没有同质量比较 benchmark，不报告速度胜负；存在源码、已有测试定义和生产可用是三种不同证据。

## 0. 范围与状态：只比较冻结实现，不借后续修复补齐能力

本文基于冻结源码、既有研究与独立审稿，不开展构建、测试或集群验证。我方固定 `6fcb2901e8ddf65fc5de524ac888d483b92c39db`，connector 固定 `ed2f49f94dfdb4ad81a2153cb658443864cd4d54`，其 Lance 12.0.0 底层固定 `cbeec97cb893a66ad6b3db87f25a57ab0093f358`。“已实现”只指冻结源码具有调用链；“接入有限”指内核存在但入口、条件或制品闭包受限；“测试跳过”是已有用例主动不执行；“本次未运行”适用于全部测试；“计划未合”不计当前能力。我方不是上游 Apache Iceberg 的标准索引功能，证据因此采用仓库相对路径，不给自定义提交伪造公开 fork 链接。

旧报告的我方基线是 `e1bebd7e6c5f425867788fb221df0c9e96c9ab33`。新基线补入文本检查的 adapter 分派、文本单查共享删除感知回表，以及 `INDEX_HITS` 请求叶子的行数统计估计；ANN 搜索、冷准备和缓存主体未因此改变。请求行数上界帮助 Spark 规划 join，不是实测基数，也不是查询去 driver 化。**#2798 的 `text_score` 投影修复、#2799 的 Lance 类型边界后续处理、#2800 的插件自动预算修复，以及 #2773 的加载与内存优化都不能记作此冻结点已交付。**下文讨论这些问题时保留当前限制，不借分支测试给主线背书。

本节证据归组：([新请求统计][i-bridge])([文本查询与删除合同][i-text-search])([文本改写冻结路径][i-text-rewrite])([插件旧预算][i-plugin])；基线差异与分支状态来自已综合的 ANN、文本及主会话笔记，不代表后续主线状态。

## 1. 五分钟总览：先分清索引是谁建、在哪里查、由谁维护

我方 sidecar 的“旁路”是指索引不替代 Iceberg 数据文件和表元数据：Spark 从表快照读活行，Java 内核生成 Puffin 载荷，独立清单登记文件覆盖与索引快照，查询再按数据文件和行位置回表。它既不是只给成熟搜索库套一层 SQL，也不是仅有 ANN；向量和 BM25 的算法、索引管理、Spark 规则都是我方交付的一部分。Lance-Spark 则把真实建索引和搜索交给 Lance native Dataset，Spark 做任务分配、排序、收集与 SQL 接入。这里的 Dataset 不是某个数据文件，而是一组由 manifest 管理版本、fragment、索引段和行可见性的数据。

LanceDB Enterprise 进一步把查询和后台索引维护作为服务组织，面向不想自行运营重计算任务的应用。它不等于开源 connector 已附带一个可部署的后台 indexer，也不等于 namespace 自带调度能力。选择这条路线要另验服务接口、访问方式和 pinned Spark 的兼容性，不能把企业产品描述移植到开源源码能力表。理解这三种分工后，才有意义讨论“哪些活可以少自己做”：我方最少改动现有表治理，Lance 原生路线最少自建索引内核，服务产品路线最少自管后台维护，但分别把成本留在兼容实现、Dataset 迁移和服务接入上。

| 索引类别 | 我方算法与构建 | 我方查询与刷新 | Lance 原生算法与 Spark 构建 | Lance-Spark 查询与维护交付 |
|---|---|---|---|---|
| ANN | Java HNSW、IVF NONE/RaBitQ、FLAT；Spark 建文件组 | 窄范围 SQL 改写及 CALL/TVF；refresh 重建受影响组 | FLAT/PQ/SQ/RQ 与 HNSW 组合；首次 ANN CREATE 未暴露，需 SDK | 显式 VECTOR_SEARCH；已有 ANN 可用 OPTIMIZE INDEX 调 native 追加/合并；没有 ANN REFRESH 或 Spark retrain 选项 |
| BM25/FTS | 自写单列倒排与 BM25；Spark 分布式构建 | driver 合并评分；SQL 合同有缺口；按 live rows 刷新 | 多列、词查询、短语及 fuzzy；Spark CREATE 分段交 SDK | 本地 fragment 与 namespace 分路；REFRESH 补未覆盖 fragment；namespace FTS 测试跳过 |
| Scalar | 没有已交付 BTree/bitmap kernel | 不把文件统计、删除 bitmap 当标量索引 | BTree、bitmap、zonemap、label-list、ngram、bloomfilter、rtree 等 CREATE 白名单 | 谓词与类型边界逐项核验；RTree native feature 闭包未证 |
| Hybrid | 没有据本研究可确认的统一融合排名交付 | 手工拼 SQL 不等于正确融合召回 | 存在 HYBRID_SEARCH 入口，与 structured FTS 路径不同 | 不用 hybrid 用例证明 namespace SEARCH 或 lance_match 已可用 |

这张表刻意把算法、建、查、刷新拆开。Lance 有 RQ，不代表 Spark 能 `CREATE INDEX` 创建 RQ；我方有分布式建 BM25，不代表 BM25 查询在 executors 分布式评分；native 有候选剪枝，也不代表 connector 已下推全局 TopK。**尤其要分开记录两件事实：namespace `SEARCH` 用例被 `@Disabled`，namespace `lance_match` 整组用例以 `assumeTrue(false)` 跳过；与此同时，固定 Lance 12 的目录 namespace 源码已有 structured FTS 查询转换与执行支持。**测试中的“尚不支持”理由不能独自证明当前能力不存在。连接器 Java namespace-impl 制品与底层之间的实际调用闭包、本次端到端结果仍未验证。本地 match、phrase、业务 filter 还有另一组测试定义，SDK 也有分段 FTS 测试定义；这些证明不同路径具备实现或验收意图，不能抹平制品验证缺口。

五分钟内可据此形成初步判断：已有大量 Iceberg 表、多格式与历史治理时，先保留 sidecar、把正确性和成本补实；新应用本来就愿用 Lance Dataset，且需要丰富文本和标量索引时，原生路线值得优先评估；需要在线服务和后台持续维护时，再单独评估 Enterprise。这个推荐没有预设我方一定赢：保留现状减少迁移，不会自动减少自建维护，也不能替代 benchmark。

本节证据归组：([我方构建与注册][i-build])([Lance CREATE 白名单][c-create])([REFRESH 边界][c-refresh])([native 优化入口][c-optimize])([SEARCH 跳过][c-fts-disabled])([namespace match 跳过][c-match-disabled])([Enterprise 路线说明][s-enterprise])。

## 2. 算法要看具体能力：同叫 ANN 或 BM25，不等于能回答同一种问题

向量搜索里，我方 HNSW 使用 Java 图库，存 float 向量，候选命中后对存储向量重打分，不调用 Lance 或 Faiss。IVF 是先训练中心、把向量分入桶，再查询部分桶；NONE 保存不量化向量，RaBitQ 在桶内编码估距，各文件组独立训练，不是全表共享一个模型，也没有桶内 HNSW。FLAT 是我方全比较内核，不该因同名写成 Lance 实现。HNSW 的近似来自图搜索遗漏，IVF 即使用原向量算桶内距离，也会漏掉未探测桶；RaBitQ 又引入估距误差，三个问题要分别看，不能只说“最后精排所以完全准确”。

Lance 12 已有 IVF_FLAT、IVF_PQ、IVF_SQ、IVF_RQ 和 HNSW 组合。PQ、SQ 和 RQ 是不同编码方式，不是所有量化都叫 PQ；Java `ivfRq(...)` 和 `RQBuildParams` 已存在，后者称 Rabit Quantizer，**不能再说 RaBitQ 是我方独有**。同一家族并不保证训练、码格式、精度或内存等价。另一个容易夸大的点是 `IVF_HNSW_FLAT`：枚举列出它，但 Java typed 参数校验要求 HNSW 搭配 PQ 或 SQ，所以枚举存在不等于该 builder 已能创建。算法矩阵必须同时看类型、校验和实际入口。

文本搜索的差别更直接。我方不是 Lucene 包装，而是自写词到文档及词频的倒排，文档长度、文件和位置也写入载荷。一个 definition 对应一个字段；查询词任意一个命中就累加分数，相当于 token OR，没有短语位置、fuzzy、显式 token AND 或跨列权重。分析器按连续字母数字切词、默认小写、最短长度二、默认无停用词集合，不做词干化。搜日志里的“ERROR 1064”很适合说明它的作用：词能命中、词频和稀有程度影响排名，但两个词都出现不是当前 OR 的必然要求，也不能表达二者必须相邻。

“程序老崩溃”揭示另一边界：连续中文文字会被当成一个长 token，不会自然切成“程序／崩溃”。因此我方 BM25 不是中文全文搜索能力的证明。Lance 有 token AND/OR、boost、fuzziness、prefix、multi_match 和独立 phrase 查询，短语要建索引时存位置，可带 slop；多列也是各列统计评分后组合，不等于把所有列合成一篇文档。Lance tokenizer 有多种构造分支，中文 jieba/lindera 受 feature 与词典目录约束，固定 Java 制品是否带齐仍未证。core 默认启用的英文处理也不能套到显式改过配置的 Spark 示例。NGram 用于另一类文本匹配，不等于带全局词频统计的 BM25，标量列表里的 NGram 不能拿来替代 FTS 能力说明。

算法选择因而要从业务问题反推。机器日志里的错误码检索可以先把我方 OR 合同讲清；需要“两个词必须一起出现”、跨标题正文加权或容错搜索时，Lance 能减少自建功能；中文自然语言检索仍需检查实际分词和词典，不应只看 API 名字。向量侧也应先决定内存、召回和回表允许什么取舍，再比较同类配置，Java/native 不是速度裁决标准。

标量索引则解决另一类问题：`tenant_id=42` 是精确资格筛选，BTree/bitmap 的作用是减少为了判断条件而读取的数据；`event_time` 范围适合讨论范围与分区裁剪；ZoneMap/Bloom 更像先排除不可能命中的数据范围，候选仍要校验。它们不替代 BM25 排相关性，也不替代 ANN 找相似向量。Lance-Spark 有这些类型的创建入口，但能否用上仍取决于列类型、谓词及扫描路径；我方当前依赖 Spark/Iceberg 的扫描、分区和文件统计，不能把这些能力冒称为交付了独立标量索引。特殊 range-BTree 构建还由 Spark 按 fragment 分组并在组内按值排序，并非连接器自己实现 BTree 或把全表按索引键任意切段。

本节证据归组：([我方图内核][i-hnsw])([IVF 训练与编码][i-ivf])([Lance RQ builder][l-rq])([HNSW typed 校验][l-vector-validate])([我方分析器][i-analyzer])([倒排与评分][i-text-adapter])([Lance 文本表达][c-fts-rule])([tokenizer 与中文约束][l-tokenizer])([标量类型与构建][c-create])([range-BTree 编排][c-build])。

## 3. ANN 双流程：先全局截候选与先任务内精排，交换的是召回风险和回表量

我方普通 SQL 流程是：绑定表快照和可用索引覆盖，driver 做冷工作集准备，每索引文件一个 executor 任务生成候选，以近似分数有界选出全局 C，executor 回表后把活行收成 driver Map，再形成 RDD，与未覆盖活行残差合并，最后用原距离排序取 k。C 由 k 与 overSample 决定，默认过采样仍为一；每文件请求 C+1，全局 `takeOrdered(C+1)`，不是把所有文件命中无界 collect。但 `rowsAt` 的活行 collect 和重建 RDD 是真实 driver 屏障，宽行、batch 和多轮补候选会增加成本，不能只按最终一百行估内存。

```text
我方普通向量 SQL
  driver：绑定表/索引快照，冷态可能先完整读取并准备索引
      ↓
  executors：各索引文件生成候选
      ↓
  全局按索引分数选 C → driver 保存候选地址
      ↓
  executors：读取命中行 → collect 到 driver，检查删除/可见性
      ↓
  活行重新形成 RDD ＋ 未覆盖文件的分布式残差扫描
      ↓
  Spark 按原向量距离精排 → LIMIT k
```

普通 SQL 的“活行结果”与 CALL/TVF 的 `INDEX_HITS` 必须分开。前者补扫未覆盖文件，并在删除导致候选不足时增加预算，有轮数与停止条件；后者最多返回索引自己的 k 个 hit，保留 raw score，删除或不可见命中允许 `row=null`，不补新数据，也不按回表原向量精排。batch bridge 逐查询走 scalar search，再共用位置回表，不是全面改用底层 batch 内核。nullable 也影响比较域：改写不要求 schema 非空，但索引和残差跳过 NULL，与普通 Spark 升序 NULLS FIRST 可能不同，比较时要显式排除 NULL 并统一 metric。

Lance 的默认 VECTOR_SEARCH 是 namespace 查询，一个 Spark 输入分区取完整 Arrow 字节结果；后端能否远端并行不由这个分区数决定。可选 distributed 路径按 live index segment 一个任务、未覆盖 fragment 一个平扫任务，任务内 native 搜索、按 refine 配置回取原向量重打分，再输出局部 k+offset，Spark 按 distance、rowid 全局排序取结果。**一个整表大 segment 不会自动按 IVF 桶拆成多台 Spark 任务。**不能说默认路径已经跨 executor，也不能因分布式计划存在就保证任意布局铺满集群。

```text
Lance-Spark 的可选分布式向量查询
  driver：打开 Dataset、固定版本、选择段与未覆盖 fragment
      ├─ indexed task：限定一个 segment → ANN → 按 refine 配置回表精排
      └─ fallback task：限定未覆盖 fragment → 原向量平扫
                      ↓
  各 executor 输出局部 k+offset（含业务列/距离/内部 row ID）
                      ↓
  Spark 全局按 distance、row ID 排序 → offset → k

默认路径则是：一个 Spark partition → namespace.queryTable → Arrow 结果。
```

两边精排位置有实质取舍。我方先按近似分数全局截 C，再精排，未进入 C 的真近邻救不回来，尤其要验证各独立模型量化分数混排的误差。Lance refine_factor 在每任务扩大候选，先精排留局部 k，再全局合并，段多时放大原向量读取；混合 indexed+flat 且未指定 refine 时自动 factor=1，只校准已有候选，不扩大池子，纯 indexed 不自动这样做。我方较集中地回表不是天然更优，Lance 较早校准也不是无代价，应该同时看召回、回表量和总延迟。

一亿条里找最像的一百条，若没有业务过滤，我方每个索引文件都参与，少探桶能减少打分，但当前冷读取仍是整载荷；driver 未知工作集先 `loadFile().bytes()`，会整容器读入并 prepare，executor 之后还要加载。Lance 按 IVF partition 读取，能避免读未探桶，却受段数、桶偏斜、mask 和随机回表影响。warm 查询也不免费：我方仍生成候选对象并全排序、同文件搜索持锁，Lance 仍可能为 refine 读原向量。原语层面的 IO 差别值得研究，但没有实测就不转换成“一亿行快几倍”。

业务过滤决定检索资格，而非只影响性能。我方普通距离 SQL 只允许非常窄的单表、单升序距离、常量向量和正 LIMIT 形状，LIMIT 到 scan 的 Filter 只能是向量非空 guard；`tenant_id=42` 会阻止改写，保留 Spark 扫描过滤算距离。把 tenant WHERE 移到 TVF 外，只是在已经全局取出的 hit 上筛，可能剩零行，不是等价加速。Lance distributed 要用函数内 filter 并显式 prefilter=true；默认 namespace 可后过滤而短返，专用 search scan 也不把外 WHERE 当函数内资格。示意调用如下，假设已完成 catalog 与扩展配置；首行启用的是连接器已有的分布式开关。**本次未运行**，参数展示不代表部署配置已批准。

```sql
SET spark.sql.lance.search.distributed.enabled=true;

SELECT id, _distance
FROM VECTOR_SEARCH(
  table => 'lance.default.items',
  query_vector => array(0.12, 0.34, 0.56),
  vector_column => 'embedding', num_results => 100,
  filter => 'tenant_id = 42', prefilter => true,
  columns => array('id')
);
```

预过滤也不保证精确或足额。没有适用 scalar index 时要读取条件列；图搜索遇极稀疏合法集合可退到该存储的 flat，量化存储并不因此成为原向量精确距离。固定 probe 预算可能阻断补探，极稀疏 shortcut 的占位距离要经精排才有业务意义。Lance 的 nprobes、ef、refine 是 query-time 参数，我方 nprobe、efSearch 来自索引定义，扩大 overSample 不会自动扩大探桶；比较时不能把这些预算当同一个旋钮。

### 返回结果和分数不能混着比较

| 入口 | 返回合同 | 分数/距离及主要边界 |
|---|---|---|
| 我方普通向量 SQL | 表的活行业务结果，补扫未覆盖文件后精排 | SQL distance 越小越近；ANN 候选外漏召回仍存在 |
| 我方向量 CALL/TVF | 索引命中地址、raw score、JSON row | cosine/dot 的原向量 score 越大越近，L2 越小越近；量化 score 可为估计，失效 hit 可 row=null |
| Lance VECTOR_SEARCH | 选定 Dataset 列和 `_distance` | 按距离取近邻；未显式 refine 的量化分支不保证原向量距离，fast 可省略新增数据 |
| 我方文本 CALL/TVF | 索引命中和 BM25 score、JSON row | score 越大越相关，但基于索引语料，不补未索引文档或删除后的足额活行 |
| Lance FTS 查询 | 业务列和显式选择的 `_score` | BM25 越大越相关；统计域、partial coverage 和 backend 影响结果与执行 |

我方 SQL 的 cosine distance 是 `1-similarity`，dot distance 是 `-dot`，不能把 CALL 的 similarity/raw score 当 SQL distance。L2 还有数值差异：我方对平方和开根号，固定 Lance 底层 L2 计算返回平方和。例如两向量差距为 3 时，一个返回 3、另一个返回 9；非负距离上的排序可以相同，但“距离小于 4”的条件、分数误差和阈值不能原样互比。比较精确值还需统一 float 输入与累加精度，量化估计值必须单列。文本 SQL 的 `text_score` 则另有本冻结版本的绑定缺陷，不能用上表的理想评分合同掩盖第 4 节的问题。

本节证据归组：([我方安全改写][i-vector-rewrite])([活行执行][i-vector-exec])([文件计算与冷准备][i-distributed])([回表 collect][i-rows])([Lance 路由与过滤][c-search])([segment 规划][c-ann-plan])([任务内精排参数][c-ann-worker])([native 精排与资格][l-scanner-ann])([补探边界][l-knn])([我方分数与距离][i-vector-distance])([Lance 平方 L2][l-l2])。

## 4. BM25 是全局语料统计问题，更是 SQL 正确性与 driver 成本问题

完整覆盖语料时，**双方都不是每个小文件各算 BM25 后直接混排**。我方合并全部 payload 的 N、总 token 数与每词 df，avgLen 是总 token 数除总文档数；Lance 对已提交 partition/segment 汇总文档数、token 数与 posting 长度，建立全局 scorer，compound 路径要求该列所有 committed segments。idf 都使用包含 N、df 的对数表达式，局部倒排只告诉“哪些文档出现了词”，全局统计才告诉这个词对整个语料有多稀有。这里的全局是“传入并且完整、不重叠的索引语料”，不自动等于任意时刻当前全表活行。

以 ERROR 1064 为例：旧日志里 1064 很少，新追加的一批日志几乎全是这个错误。若各文件各算，旧文件会把 1064 当稀有词、新文件把它当常见词，同一句查询用两把尺子排；仅改变分文件方式就可能改变前十名。合并 N 和 df 后，新增文档会降低这个词对整体的区分力，旧文档分数也随之变化。因此“索引追加成功”不能只验证新文档能命中，还要验证旧分数随全局语料正确变化。两边源码有统计汇总，我方也有手算及单／双 payload 同分测试定义，但本次没有执行，更没有完整追加删除对 rebuild 的等价实验。

我方 BM25 查询当前在 driver 读取全部相关 payload，汇总统计、逐 posting 累加全部匹配文档分数、全排序再截 k；没有用 WAND 或块上界在候选阶段免掉大量评分。普通文本改写还有一次更早的读取：绑定阶段先通过 `searchStatistics` 读取并解码全部 payload，之后搜索再走读取与解码路径。统计 API 已存在不等于复用了 prepared 对象，也不等于实现了分布式评分。`searchesAllPayloadsTogether` 分支直接交给 driver IndexReader，不进入 ANN 文件 RDD。建索引时 Spark 分布式读 live rows、按文件组写载荷，因此“构建分布式”成立，“查询分布式评分”不成立。一亿日志找一百条时，最终输出小不能消除全部倒排、统计和排序的工作集，常见词与多查询会进一步放大成本。

SQL 层比评分公式更需要先闭环。普通 text rewrite 要求精确源快照和全覆盖，否则不改写；但未绑定 `text_score` 求值会报需要 text index，**不改写不代表有一个独立的全表 BM25 fallback**。这里既包括排名路径，也包括 match 路径：`TextMatchExpression.eval` 仍调用其中的 score 求值，不能把未绑定失败描述为自动退成普通词法搜索。排名路径会在优化期搜索和回表成 LocalRelation，冻结 `text_score` 投影及多处绑定未闭环，#2798 修复未合。业务 filter 也没有进入 adapter 的 TopK 资格，排名路径替换 limit 子树的方式不能声称完整保留业务 WHERE；它与向量规则遇业务过滤主动拒绝改写的安全策略不同。不能把“搜出了 hit”当成“投影分数、排名及租户语义都正确”。

CALL/TVF 更清楚但合同更窄：查 latest index snapshot，回表用当前 table snapshot，hit 可以保留而 row=null，不补新增未索引文档，也不补删除后的 live TopK。普通 SQL 的精确覆盖前置条件与这个命中入口不可互换。日志用户应知道 ERROR 和 1064 当前是 OR 命中；把 `tenant_id=42` 放在 text TVF 外仍是先截 hit 后筛，不回答“这个租户里最相关的一百条”。这也是为什么优先处理文本 SQL 合同比先做 ANN 加载优化更重要：业务拿到错误分数或错误资格集合，速度越快也不会变成正确答案。

Lance 本地 fragment scanner 传 native FullTextQuery、业务 SQL filter 和 prefilter，保留 Spark residual，显式投影 `_score`，SELECT * 默认不展示它。本地 FTS 的 TopN 下推被拒绝，排序仍由 Spark 做，不能把 native bounded collector 写成 connector 已下推全局 k。namespace 则通常是单 partition queryTable，能否把条件和排名推下去取决于 backend 与 residual；有 Spark residual 时拒绝 TopK 下推，请求更大结果再由 Spark 筛排。SEARCH 走 structured FullTextQuery，hybrid 的 string-query 分支不同；这些是查询路径与制品验证的边界；跳过测试不能抵消固定目录 namespace 已实现 structured FTS 的源码事实，也不能替它提供端到端成功证据。

```text
我方 BM25
  Spark 分布式构建文件组 → Puffin 词表/posting/文档长度
  查询时 driver 读取全部 payload → 合并 N、df、avgLen
      → 全部匹配 posting 评分 → 排序截 k → 回表/SQL 输出
  普通 SQL 绑定还会预先读取统计；不是 ANN 的文件级分布式评分。

Lance FTS（两条路径不要混用）
  本地 fragment scan → native FTS + 条件 → 匹配行及 _score
      → Spark 全局排序/limit
  查询型 namespace → structured FTS/条件/可下推 TopK → queryTable 结果
      → Spark 保留的 residual/排序/limit
  顶层 k 是否真正进入底层、统计是否覆盖当前活行，要逐路径验证。
```

追加与删除还要明确统计域。Lance partial coverage 默认有未索引 fragment/stale rows 的 flat 分支，fast_search 可省略；特定优化明确是 matching exact、ranking approximate until indexed，旧索引用 committed scorer、残差扩统计后混排，不能说当前全表统一精确 BM25。删除 mask 让旧行不返回，但持久 N、df 汇总本身不接 row mask，删除安全不等于实时扣掉统计。我方 refresh 重建受影响组时按 live rows 更新载荷，保留不变文件；这是更新统计的实现机制，不足以未经实验承诺任意 append/delete 和全量 rebuild 同排名。应该把覆盖、可见性、评分语料与重建一致性分别验收。

本节证据归组：([我方全局统计与排序][i-text-adapter])([手算测试定义][i-text-tests])([driver 查询分支][i-text-driver])([冻结 SQL 缺口][i-text-rewrite])([Lance scorer][l-fts-scorer])([全段 scorer 合同][l-fts-compound])([partial 排名限制][l-fts-exec])([覆盖与补扫][l-scanner-fts])([本地 FTS 与 namespace 下推][c-fts-scan])([本地 scanner][c-fragment])([目录 namespace structured FTS][l-dir-fts])([统计与搜索的独立读取][i-text-reader])([match 依赖 score 求值][i-text-match])。

## 5. 生命周期必须逐种变更看：发布成功、数据可见、物理维护不是同一件事

我方 text 和 vector 共用构建骨架：固定表快照、读删除感知活行、按文件或文件组生成 Puffin、写 manifest list 和索引元数据，再由 procedure 发布。refresh 复用不变文件，新增、重写及适用 delete-file 集合变化触发重建；`deleteChanged` 会沿旧 index coverage 扩到整个旧组，所以删除一行也可能重建宽组，不是往图或倒排原地插删。Lance scalar/FTS 的 Spark CREATE/REFRESH 则冻结 Dataset 版本，executor 用 SDK 生成未提交 segment，driver 校验并提交；ANN 首次创建仍需 SDK；已有 ANN 索引则可以通过 Spark `OPTIMIZE INDEX` 调用 native `optimizeIndices`，追加未覆盖向量或合并已有段。这是已追踪到的维护调用链，不是 Spark 分布式 ANN 训练；当前 Spark 只暴露合并参数，未暴露 SDK 的 `retrain`，且本次没有运行 ANN 维护用例。

| 变更 | 我方维护与查询边界 | Lance Dataset／Spark 边界 |
|---|---|---|
| 首次构建 | live rows 建新根与独立索引快照；尚未发布文件不可用 | scalar/FTS 分段 build、driver commit；ANN 由 SDK caller 建 |
| Append | 新文件建索引；ANN 活行 SQL 残差接新增，hit 接口不补 | scalar/FTS 用 REFRESH 补 uncovered fragment；已有 ANN 可用 OPTIMIZE INDEX 调 native 增量；默认查询补扫，fast 可跳过 |
| Delete | 回表排除旧行；refresh 因 delete 集合变化扩大旧组重建 | visibility mask 排除；持久统计与物理删项另需维护 |
| Update／compaction | 旧文件移除、新文件接入，受影响组重建 | segment 覆盖、更新 overlay 与 fragment reuse 决定保留／修剪；不是无条件兼容 |
| Merge／retrain | 文件组重建是现有机制，不宣传原地全表模型演进 | Spark OPTIMIZE INDEX 可请求 native 段追加/合并；SDK 可请求重训，Spark 未暴露 retrain；本次未运行 |
| 历史读 | EXACT 与祖先链 AT_OR_BEFORE 分开，过期源文件有保留约束 | Dataset version 绑定 manifest；旧版本与清理节奏共同决定可读性 |
| 并发与 cleanup | registry CAS 不保护同 index refresh；清理不能并行 build | manifest 事务与冲突 rebase 有明确范围；未发布孤儿不自动即时回收 |

### Lance 维护接口怎么对应

| 要做的事 | 现有入口 | 计算在哪运行 |
|---|---|---|
| 第一次创建 ANN | Lance SDK `create_index` / Java `createIndex` | SDK 所在作业；分布式构建需调用方编排 |
| 已有 ANN 增量追加或合并 | Spark `ALTER TABLE ... OPTIMIZE INDEX ...`；或 SDK `optimize_indices` | Spark 命令在 driver 调 native，不自动分发 ANN 构建到 executors |
| 创建/刷新 scalar 或 FTS | Spark `CREATE INDEX` / `REFRESH INDEX` | connector 分配 fragment 任务，executor 调 SDK，driver 提交 |
| 显式重训 ANN | SDK 的 retrain 能力 | 原生维护作业；当前 Spark OPTIMIZE 不接受该参数 |

```sql
-- 已有 ANN 索引时，用现有参数请求追加，不合并旧段；本次未执行
ALTER TABLE lance.db.items OPTIMIZE INDEX embedding_idx
WITH (num_indices_to_merge = 0);
```

这个区分修正了“ANN 都要另外跑 SDK”的过宽理解：首次构建和显式重训需要 SDK 入口，日常增量可选择 Spark OPTIMIZE，但它仍是 driver 内原生重计算，资源隔离和调度需求并没有消失。

Lance 提交保留不相交旧段、移除重叠旧段并装入新段，校验 UUID、字段、类型、源版本和 coverage；不能只替换宽段一小部分，让剩余 fragment 被孤立。发布前发生压缩或改写会据历史与当前文件身份修剪覆盖，全部消失可拒绝、部分消失会警告。并发 append/delete 有 rebase 路径，但 Update、DataReplacement、compaction、NGram、Overwrite/Restore 各有冲突条件，不是“所有变更自动一致”。FTS REFRESH 要求配置一致，Spark 不会自动替用户复制全部旧参数，构建后仍可能在提交校验被拒。

我方 registry 用 table property 指针 CAS 登记“有哪些索引”，单索引 `updateIndex` 却读当前 generation 后写 metadata 与交替槽，未比较构建起点 metadata，写入仍可覆盖。因而两个同 index refresh 不能借 registry CAS 推导冲突检测，也不能说索引发布与数据 snapshot commit 是同一事务。Lance atomic 主要在 Dataset manifest 的索引集合安装范围；namespace 通常定位或提供凭据，managed_versioning 可安装 external commit handler，让 catalog 参与版本发布，但仍不能推广为任意跨系统事务。

失败恢复同样不能只看正常返回。未发布 segment 不可见，并不等于没有孤儿；Lance cleanup 根据保留 manifest 引用回收，无法确认是否仍在写的文件通常保留七天，强删须排除并发更新。我方 task collect 后才登记已知路径，任务重试或 driver 提前退出可能留未登记文件，abort 只能删已知文件；cleanup 扫历史和 staging，明确不能与 build/refresh 并行，且依赖 Hadoop FS listing，不是任意 FileIO 都适用。check_indexes 对向量检查首个 payload header，对 text 检查声明 blob 类型，不是全文件健康、召回或 SQL 正确性验证；stale 提示也只是源快照与当前不同。

本节证据归组：([我方 refresh 与删除扩大组][i-build])([live rows][i-live])([sidecar 发布范围][i-sidecar])([Lance 构建与提交][c-build])([Spark OPTIMIZE 参数与执行][c-optimize-options])([native ANN 增量分支][l-ann-append])([segment 校验][l-commit-index])([冲突解析][l-conflicts])([managed versioning][l-managed])([Lance 清理][l-cleanup])([我方清理][i-cleanup])([检查范围][i-check])。

## 6. 文件格式、回表与缓存：能按范围读 Lance 文件，不等于能用 Dataset ANN

我方可以把索引命中位置合并为行范围，通过 LanceFileReader 读取指定列和行，再转成记录；这属于文件级 IO，向量索引仍由 Java kernel 写入 Puffin。Lance Dataset 的 native segment 绑定 manifest、field、fragment、row ID 和删除规则，不能消费我方 Puffin，也不能把一个 Lance 数据文件直接当成已管理的数据集。**“索引回表采用 Lance 格式”与“采用 Lance 原生索引体系”是两项不同决策。**前者换读写接口，后者还要接管版本、覆盖、行地址与维护；不能以文件读性能为理由，跳过这些兼容性问题。

格式支持也要把入口存在与表语义完整分开。我方 Parquet 路径直接读取，非 Parquet 通过 FormatModelRegistry；直读未接齐 table nameMapping 与 encryption 解密合同。Lance 模型拒绝加密、忽略 byte split 和 nameMapping，转换器无 STRUCT/MAP，LIST 只覆盖有限基本元素。Parquet 的复杂列路径不能拿来证明 Lance 复杂列同样可用，#2799 后续处理未合这个冻结点。索引只涉及一个简单向量或文本列，回表仍可能要求复杂业务列；“能建索引”不能推出“查回整行值保真”。字段重命名、按 ID 映射、加密和分片范围都应独立验证，不把格式枚举注册当兼容性验收。

缓存的当前成本可以更具体地说。我方保存整容器 byte[] 加 prepared 对象，模型仍为一；IVF prepare 展开 entry、code、factors，查询再生成探测候选并全排序。driver 冷 workingSet 估算先整文件读并 prepare，发生在 lazy 配置解析前，所以默认 lazy=false 不免这笔成本。记账容量只约束缓存条目，不覆盖暂态解压、scratch、回表行、Arrow 或整个 JVM 堆。native 按 IVF partition 缓存，miss 读取该分区范围和量化 storage，但也有模型、mask、临时计算及回表成本；两者预算都不是进程硬上限。

显式 load 的成功应答不等于下一次查询一定暖命中。我方 plugin ACK 进入驻留控制，query locality 却使用任务 accumulator 的 host hints，最多两个主机，不与 ACK placements 接成 executor 强路由。同一主机多个 executor、首次无提示或调度变化都可能冷读。冻结插件又只读原始整数预算，未设值时广告零，与 query 侧推导预算不同；#2800 修复不能借用。#2773 的原地载荷、模型二、有界堆、resolver 和 driver estimate 优化也未合，因此不能把这些设计收益填到现状表。

这组差异的实际含义是：一亿行查询只回一百条，仍要问冷加载读了多少容器、prepared 占多少堆、精排随机读了多少文件，以及候选和活行经过 driver 几次。native 按需读给出减少无关桶 IO 的机制，却不证明某个部署更快；Java 整体加载更粗，也不意味着 warm 小索引一定吃亏。要在同质量、同布局和真实冷暖条件下测，不能用语言或实现层级代替测量。

本节证据归组：([读取分派与边界][i-format-read])([Lance 文件模型限制][i-lance-format])([Arrow 转换范围][i-lance-converter])([我方缓存对象与锁][i-cache])([冻结模型一][i-memory])([旧插件预算][i-plugin])([主机提示][i-locality])([Lance partition cache][l-cache])([共享 Session][c-session])。

## 7. 开源部署推荐任务化建索引：重计算需要隔离，不必先做常驻服务

如果选择 Lance 原生 Dataset，开源部署可以把 SDK 建索引放进 pinned pylance 容器，以 Kubernetes Job 执行，沿用已有调度；没有现成调度时再评估 Argo，索引 job 和 Spark 查询共享 Dataset 存储及 catalog。这里是**部署建议，不是仓库已经交付的镜像、工作流或批准配置**。

```text
已有调度平台（没有时再评估 Argo）
   ├─ 首次构建 Job：pylance → 训练/构建 → 校验 → 提交索引
   ├─ 增量维护 Job：SDK optimize；或独立 Spark 作业执行 OPTIMIZE INDEX
   └─ 重训/清理 Job：分别安排，避免与发布和在写任务冲突
                         ↕
共享存储上的同一 Lance Dataset（数据 + manifest + 索引段）
                         ↕
catalog/namespace 解析与凭据 → Lance-Spark 查询

namespace 不是调度器；Kubernetes Job 也不会仅凭 replicas 自动完成分布式索引。
```

重计算独立出去的作用是给训练、编码、合并明确资源与失败边界，不是要求接口服务自己承担计算。接口可以接收任务、返回任务状态并触发 job，进程不必常驻持有全部向量，也不必先引入一个新的长期索引服务。

为什么先推荐单 job 闭环？它先验证输入版本、输出段、发布及查询之间的完整合同，避免一开始把分布式训练、任务重试和事务恢复同时摊开。这个建议不承诺一亿行能在单机内存 fit：若训练样本、编码或回表验证超资源，就应由实测决定怎样拆分。Lance SDK 的 distributed indexing 提供 worker 与 segment 组织接口，官方说明不提供完整 scheduler 或端到端编排；现行 main 指南只支持这种分工认识，不能据其中新 API 宣称 pinned 12 全部相同。SDK caller 自己组织任务，也不是配置几份 replicas 就自动完成并行训练。

首次构建任务应固定 Dataset 源版本、字段和索引配置，完成训练、编码、coverage 校验，再由一个发布步骤安装结果；增量任务可以直接调用 SDK，也可以由独立 Spark 作业执行 `OPTIMIZE INDEX`；后者在 Spark driver 内调用 native，不因处在 Spark 应用里就自动获得分布式构建。两种路径都要明确处理未覆盖 fragment，不能偷换成每轮全量训练。合并／重训任务为段数量、模型质量或布局调整服务，成本可能大于增量，也要明确哪些旧段保留。校验任务应检查覆盖、活行可见性、样本结果和统计域，而不是只做 SHOW。互斥、幂等任务身份、失败重放及“提交结果未知”的恢复都需要实现和验收，这些是建议补齐的任务合同，不是现成部署产物。

重试尤其要分清写段和发布。worker 写出了文件却失败，可以留下未发布段；driver 已发布但回执丢失，再跑一遍不能默认安全替换同名索引。索引名、源版本、配置和已提交 segment 身份应能够帮助恢复判断，清理又必须避开正在写的任务。单 job 先跑通能暴露这些问题，多任务化之后还要控制训练产物共享、fragment 分配不重叠和最终 commit。分布式构建 caller 需要这套组织工作，namespace 只会定位 Dataset 或参与版本提交时并不会自动替它调度。

布局还反过来影响查询。把全表建成一大段可以减少段元信息与局部结果合并，却让 Spark distributed 查询只有一个索引任务；多段能并行，也会增加每任务局部 k、refine 读取、调度和段维护。因而建索引 job 不能只追求“最快生成一个文件”，还要满足查询并行与 IO 的目标。这里不提出新 segment 数量、资源上限或默认值；应先用现有接口做受控实验，再由结果决定布局，避免把一个未经批准的参数清单当架构方案。

如果应用必须有持续后台维护和服务化查询，LanceDB Enterprise 是另一候选：官方架构描述独立 query/indexer 与共享对象存储，后台增量由产品承担，OSS 则自管维护节奏。它减少用户自己编排的工作，但引入服务接入和产品运行边界；本研究没有验证与这个 pinned connector 的互通。不能把 Enterprise indexer 当开源 namespace 的一部分，也不能为省掉调度工作就宣布可以直接接现有 Spark。对已有调度和对象存储的团队，Job 路线通常更易先做端到端验证；对需要在线服务的团队，服务路线值得另案评估，而非强行让 Spark 充当在线服务。

本节证据归组：([Spark 分段构建及发布][c-build])([segment 提交校验][l-commit-index])([分布式查询受布局影响][c-ann-plan])([官方 distributed indexing 分工][s-distributed])([Enterprise 架构][s-enterprise])([OSS 与 Enterprise 维护分工][s-reindex])。最后三项是既有笔记中的现行部署资料，不是固定源码的 API 验收。

## 8. 按场景选型：保留 Iceberg 是兼容性理由，不是“我们一定赢”的结论

对已有 Iceberg 表、文件格式治理、历史快照与 Spark 作业的团队，推荐先保留 sidecar，并优先修正 BM25 SQL 和 driver 成本。理由不是算法更先进，而是替换到 Lance Dataset 不仅换索引，还要迁移行身份、删除、压缩、历史读和发布协议。反例很具体：表里复杂业务列、等值删除和历史查询已经依赖 Iceberg，仅能按范围读 Lance 文件不足以接管这些合同。保留路线的弱点同样明确：自写文本能力窄、BM25 driver 工作集大、ANN 过滤和冷准备受限、同 index 发布缺少冲突保护。若这些成本补不实，兼容性理由也不足以无限维护一套自建体系。

对新应用已经接受 Lance Dataset 的团队，若要同时做向量、标题正文检索、短语或 fuzzy 以及 tenant 标量过滤，推荐认真评估 native 路线。理由是已有多类索引和较完整的表达，能减少自建 BTree、倒排表达及任务内精排的工作；不推荐再只为“我们也有 RaBitQ”而复制整套体系，Lance 12 同样有 RQ。弱点是 Spark 首次 ANN 创建和显式重训仍需 SDK caller，已有 ANN 的 Spark OPTIMIZE 仍要安排重计算资源；namespace FTS 的制品端到端证据、中文配置闭包和 partial 排名也需要验证，多段布局与发布编排仍须自管。需要强在线服务和后台索引的团队应单独评估 Enterprise，不能因名称相近就免掉 pinned Spark 接入验证。

三个业务场景可帮助把这些建议落地。一亿条找最像的一百条，首先统一非空 float 向量、metric 与覆盖域，再看过滤、候选预算和回表；单大段或整载荷冷 prepare 可能成为瓶颈，结论不能先写成某内核获胜。ERROR 1064 日志搜索，首先确认 OR/AND、分词和全局统计，追加大量同错误日志后旧分数应如何变化；我方当前 SQL 投影、资格及 driver 评分比“BM25 名字相同”更值得先处理。tenant_id=42 内找一百条，首先确保条件在候选前，TVF 外 WHERE 不等价；我方普通向量 SQL 放弃改写保语义，Lance 要验函数内 prefilter 和 sparse 预算，不以少量返回证明过滤正确。

据此，推荐顺序应从原报告的 ANN 单线转为**先 BM25 的全局统计合同、SQL 正确性和 driver 成本，再 ANN 过滤与回表，随后按需加载**。全局统计机制双方都有，下一步不是重新证明公式，而是检验追加删除、投影与排名的端到端结果；先修正确性不代表延后所有性能观察，driver 数据量与排序成本应同步记录。ANN 过滤改变业务资格，executor 内回表可减少 driver 往返，但必须审查局部 refine 的 IO 放大；按需加载能减少冷无关桶读取，但不能遮住格式、删除或历史合同。直接换 native kernel 则应另作兼容决策，不能借一次缓存优化顺便接管 Dataset。

| 比较项目 | 必须先固定的质量与语义 | 同时记录的成本与结果 |
|---|---|---|
| ANN | metric、float/NULL 域、原向量真值、probe/ef、refine/oversample 含义、filter 选择率、coverage 与 fast/hit 合同 | recall@k、足额率、端到端延迟分位、driver/executor/native 峰值、索引与回表 IO、冷暖真实命中 |
| BM25 | tokenizer、OR/AND/phrase、多列规则、N/avgLen/df 统计域、append/delete、filter 与投影正确性 | 匹配集合、排名与分数误差、刷新对 rebuild、全部 payload 与排序成本、driver 内存、查询延迟 |
| Scalar | 支持类型及谓词、NULL、相同 tenant 分布，Iceberg 扫描／文件统计基线与 Lance 标量路径分开 | 过滤正确性、读取行／字节、索引 build/refresh 代价、条件列扫描和 mask 开销 |
| 维护与资源 | 同快照、段／文件组布局、硬件、并发、对象存储、重复方法、失败与清理节奏 | 首次／增量／合并时间、读写量、资源占用、重试与恢复；不混算成本和召回 |

benchmark 不能只跑内核。冷态应包括 driver estimate、打开模型、存储读与 prepare；暖态需检查真实 executor 落点和 cache hit，不以 load ACK 代替。统计范围包括候选、回表、补扫和排序，输出一百行不是资源计量范围。构建分布式、native 线程多、SQL 使用了索引，都不是延迟或吞吐结果；低召回换低延迟、fast_search 省掉未覆盖数据、hit 接口省掉活行补足，都不能作同质量胜利。BM25 的 exact matching 与 approximate ranking 也要分列报告，不能把命中正确混写成排序正确。

本节证据归组：([我方 SQL 资格边界][i-vector-rewrite])([文本评分与 SQL 缺口][i-text-adapter])([冻结投影路径][i-text-rewrite])([Lance 过滤参数][c-search])([partial FTS 排名][l-fts-exec])([格式边界][i-lance-format])。选型与比较表是从这些已定位差异推导的建议，不是 benchmark 结果或已批准实施计划。

## 9. 五条结论与待验清单：把“有源码”推进为可审查的交付合同

**结论一：三条路线按责任分工选择，不按品牌选胜者。**我方负责 Iceberg 旁路算法、元数据与 Spark；Lance-Spark 编排 Dataset native；Enterprise 承担另一种服务维护。**结论二：Lance 索引广度更强，但底层能力不能全部记作 Spark 交付。**RQ 已存在，首次 ANN CREATE 未暴露、已有索引可经 OPTIMIZE 维护；namespace FTS 的测试与制品验证边界仍要保留。**结论三：我方 BM25 合并全局统计是实现事实，SQL 投影、业务资格和 driver 成本仍是优先缺口。**这比追加一个 ANN 优化段更影响当前报告的可用性。

**结论四：ANN 的质量与成本来自执行顺序和覆盖域，不来自 Java/native 标签。**先全局近似截断与先局部 refine 各有回表和遗漏风险，过滤在函数内与外 WHERE 也不是同一问题。**结论五：维护与部署应按任务合同验收，不能把 manifest 原子、CAS、load ACK 或 check 当成全链保证。**开源先做 SDK Job 闭环是建议，既不承诺单机容纳一亿行，也不表示已经提供自动 indexer。

待验项应保留到后续获准执行，不在本次补跑。文本侧先验冻结 SQL 的 score 投影、多处绑定、业务 filter 保留，以及完整／partial、追加／删除下统计与 rebuild；Lance namespace structured SEARCH 和 lance_match 必须单独验证，不能拿跳过用例及 hybrid 背书。ANN 侧验 NULL/metric 同域、稀疏 tenant 的足额与距离、不同 refine 顺序的召回和回表量，再验 driver 冷 prepare、宽行 collect 与真实 cache 落点。格式侧验复杂列、nameMapping、encryption 和 split；维护侧验同 index 并发发布、提交未知重放、过期历史及 cleanup 与构建隔离；部署侧验 pinned SDK 制品、中文 feature/词典及 Enterprise 接入。

这些待验都对应上文证据中的具体边界，不能先写入已实现清单。报告改变的判断是：索引选型不再由“一亿行 ANN 快不快”单独决定，而由文本结果、过滤资格、活行覆盖、发布维护和资源成本共同决定。先让答案与业务问题一致，再用同质量数据决定哪些机制值得优化、哪些自建责任值得移交，这样才不会用未经验证的性能预期先替团队选定数据模型。

本节证据归组：([文本 SQL][i-text-rewrite])([namespace 跳过][c-fts-disabled])([ANN 活行与残差][i-vector-exec])([冷准备][i-distributed])([发布边界][i-sidecar])([cleanup 限制][i-cleanup])；上述是有出处的待验问题，不是测试通过声明。

## 证据索引（不计入正文篇幅）

我方冻结 `6fcb2901e8ddf65fc5de524ac888d483b92c39db`；全部我方源码链接从 `docs/research` 经 `../../` 回仓库根，行号只适用于此冻结点，不依赖 worktree 绝对路径或虚构的公开 fork。外部源码分别锁 connector `ed2f49f94dfdb4ad81a2153cb658443864cd4d54` 与 Lance 12 `cbeec97cb893a66ad6b3db87f25a57ab0093f358`。引用按节归组，以下仅保留可定位关键差异的入口；伴随范围仍属同一冻结文件。部署资料是既有 coordinator 笔记引用的现行官方说明，不冒充 pinned API；本文没有新访问这些资料。

### 我方：算法、查询、维护和格式

[i-bridge]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorSearchBridge.scala#L90-L265 "INDEX_HITS 与 batch；238–255 新 computeStats"
[i-text-search]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/IndexSearch.java#L134-L177 "latest 索引、current 回表与 null；TextSearchProcedure.java:119–134 单查共享路径"
[i-text-rewrite]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/RewriteTextSearch.scala#L95-L156 "排名与投影；191–207 all-match；237–276 exact snapshot；同包 TextScoreExpression.scala:57–69 未绑定拒绝"
[i-plugin]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/IndexCachePlugin.java#L124-L160 "worker 与旧 budgetBytes；IndexCacheDriverControl.java:43–55 ACK 通道"
[i-build]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/SparkIndexBuilder.java#L169-L257 "增量构建；626–715 分组与 delete 集合；80–83 注册；VectorKernels.java:108–115 内核注册"
[i-hnsw]: ../../index-vector/src/main/java/org/apache/iceberg/index/vector/HnswlibKernel.java#L109-L181 "Java 图构建；337–373 ef、候选与重打分"
[i-ivf]: ../../index-vector/src/main/java/org/apache/iceberg/index/vector/IvfKernel.java#L116-L197 "独立训练编码；365–397 全排序；450–488 prepare 展开；612–664 训练"
[i-analyzer]: ../../index-text/src/main/java/org/apache/iceberg/index/text/TextAnalyzer.java#L32-L59 "连续字母数字切词；同模块 TextIndexDefinition.java:159–167 默认值"
[i-text-adapter]: ../../index-text/src/main/java/org/apache/iceberg/index/text/TextIndexAdapter.java#L133-L377 "133–186 倒排；203–297 全局 N、avgLen、df；363–377 全排序；build.gradle:851–870 无 Lucene 依赖"
[i-text-tests]: ../../index-text/src/test/java/org/apache/iceberg/index/text/TestTextIndexAdapter.java#L62-L128 "手算与单／双 payload 测试定义，本次未执行"
[i-text-driver]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/DistributedIndexSearch.java#L265-L276 "searchesAllPayloadsTogether 的 driver 分支"
[i-vector-rewrite]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/RewriteVectorSearch.scala#L81-L175 "SQL 形状与 metric；251–316 业务 filter guard"
[i-vector-exec]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorSearchExec.scala#L128-L286 "活行补候选、非空残差与 driver rows 重建 RDD"
[i-distributed]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/DistributedIndexSearch.java#L151-L243 "文件任务及全局 takeOrdered；486–543 locality；573–632 冷 estimate 整 load"
[i-rows]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/IndexEntries.java#L248-L328 "executor rowsAt 后 driver collect"
[i-live]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/IndexEntries.java#L172-L209 "源快照活行；474–510 删除感知读取"
[i-sidecar]: ../../core/src/main/java/org/apache/iceberg/index/SidecarIndexCatalog.java#L255-L264 "单 index update；296–334 registry CAS；437–453、672–681 metadata／槽覆盖；同包 IndexReader.java:278–321 EXACT 与祖先选择"
[i-cleanup]: ../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/CleanupIndexFilesProcedure.java#L65-L265 "历史、staging 清理与不能并行 build/refresh；SparkIndexBuilder.java:529–537 已知路径 ledger"
[i-check]: ../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/CheckIndexesProcedure.java#L165-L405 "adapter 分派、首个 header 与 text 声明检查"
[i-format-read]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/IndexEntries.java#L514-L555 "Parquet／FormatModel 分派；721–789、815–840 读取与范围"
[i-lance-format]: ../../lance/src/main/java/org/apache/iceberg/lance/LanceFormatModel.java#L421-L466 "Lance 文件范围回表；235–241、337–339、391–393 加密／split／nameMapping；530 起类型转换"
[i-lance-converter]: ../../lance/src/main/java/org/apache/iceberg/lance/LanceArrowConverter.java#L136-L234 "列值转换；351–373 LIST 边界"
[i-cache]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/ExecutorIndexCache.java#L378-L444 "整 bytes 加 prepared 与独占锁；595–650 暂态加载；1311–1321 计费"
[i-memory]: ../../index-vector/src/main/java/org/apache/iceberg/index/vector/VectorMemoryEstimates.java#L28-L48 "冻结 model=1"
[i-locality]: ../../index-spark/src/main/scala/org/apache/iceberg/index/spark/IndexFileRDD.scala#L37-L75 "一文件一 partition，最多两个 host hints"

### Lance-Spark：入口、执行和测试边界

[c-create]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/AddIndexExec.scala#L582-L636 "类型白名单；ANN 不在 Spark CREATE"
[c-refresh]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/RefreshIndexExec.scala#L44-L204 "分布式刷新、类型限制、uncovered 与配置校验"
[c-optimize]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/OptimizeIndexExec.scala#L144-L180 "native optimizeIndices 通用入口"
[c-build]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/AddIndexExec.scala#L474-L569 "SDK 分段构建；92–242 版本与发布；297–455 range 按 fragment 分组；693–815 coverage 修剪"
[c-fts-disabled]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/test/java/org/lance/spark/search/BaseSparkSearchTableFunctionTest.java#L88-L104 "SEARCH @Disabled；测试理由不能覆盖固定底层的 structured 实现事实"
[c-match-disabled]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/test/java/org/lance/spark/read/BaseFtsMatchPushdownTest.java#L52-L58 "assumeTrue(false)，namespace lance_match 整组跳过"
[c-fts-rule]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/catalyst/optimizer/AbstractLanceFtsPredicateRule.scala#L31-L110 "SQL FTS 提取；182–241 AND/OR 与 fuzzy；275–345 phrase/multi_match"
[c-fts-scan]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/read/LanceScanBuilder.java#L540-L584 "namespace 与 residual；629–680 limit/TopN 分支"
[c-fragment]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/internal/LanceFragmentScanner.java#L93-L140 "本地 FTS、filter、prefilter 与显式 _score"
[c-search]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/lance/spark/search/LanceSearchTableFunctions.scala#L268-L381 "VECTOR_SEARCH 路由、全局排序、函数内 filter；102–176 SEARCH；248–262 hybrid 另一分支"
[c-ann-plan]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchScan.java#L112-L222 "segment／未覆盖 fragment 任务及 mixed refine=1"
[c-ann-worker]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchColumnarPartitionReader.java#L114-L193 "任务内 scanner、nprobes、ef、refine"
[c-session]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/LanceRuntime.java#L232-L280 "每进程每 catalog 共享 native Session"
[c-ann-default]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceSearchColumnarPartitionReader.java#L79-L97 "namespace.queryTable 完整 byte[] 与 ArrowFileReader"
[c-search-builder]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchScanBuilder.java#L20-L33 "专用 search scan 没有外 WHERE 下推；同目录 LanceSearchScanBuilder.java:20–33"
[c-fts-local-tests]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/test/java/org/lance/spark/read/BaseFtsCatalogOnlyNamespaceTest.java#L146-L165 "本地 match/phrase 定义；230–239 业务 filter；本次未执行"

### Lance 12：内核、统计、事务和缓存

[l-rq]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/java/src/main/java/org/lance/index/vector/VectorIndexParams.java#L108-L135 "ivfRq 与 withIvfRqParams；同目录 RQBuildParams.java:18–27 Rabit Quantizer"
[l-vector-validate]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/java/src/main/java/org/lance/index/vector/VectorIndexParams.java#L41-L50 "HNSW typed 校验要求 PQ/SQ；IndexType.java:28–35 类型枚举不是 builder 保证"
[l-tokenizer]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/scalar/inverted/tokenizer.rs#L611-L651 "core 默认；1011–1060 过滤；1081–1136 中文／ICU 与 feature"
[l-fts-scorer]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/scalar/inverted/scorer.rs#L166-L239 "全局 N、avgLen、df；同目录 cross_column.rs:497–579 各列 scorer"
[l-fts-compound]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/scalar/inverted/compound.rs#L4210-L4223 "全部 committed segments 与 global collector；4365–4366 ready mask"
[l-fts-exec]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/io/exec/fts.rs#L948-L971 "residual 排名近似；1103–1155 两套 scorer；1380–1444 删除与分段约束"
[l-scanner-fts]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/dataset/scanner.rs#L4332-L4350 "matching exact／ranking approximate；4997–5148 partial/fast；5153–5260 live flat"
[l-scanner-ann]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/dataset/scanner.rs#L5439-L5487 "原向量 refine；5787–5896 mixed fallback；6926–6995 prefilter 来源"
[l-knn]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/io/exec/knn.rs#L1199-L1229 "deletion prefilter；1902–2032 late-search 预算与 INFINITY shortcut"
[l-hnsw-filter]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/vector/hnsw/builder.rs#L1612-L1674 "稀疏 mask 与 storage flat_search"
[l-commit-index]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/index.rs#L2011-L2213 "segment commit、类型和 coverage 校验；235–446 同名段替换"
[l-conflicts]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/io/commit/conflict_resolver.rs#L693-L930 "CreateIndex 与更新／压缩等冲突范围；io/commit.rs:1457–1475、1580 起 rebase loop"
[l-managed]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/dataset/builder.rs#L587-L607 "managed_versioning 安装 external manifest commit handler"
[l-cleanup]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/dataset/cleanup.rs#L7-L34 "引用回收、七天 unverified 保留与强删限制"
[l-cache]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/index/vector/ivf/v2.rs#L1536-L1638 "IVF partition cache／range 加载；2040–2059 子索引 refine 预算"
[l-scalar-types]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/registry.rs#L10-L18 "scalar 插件；158–160 RTree geo feature"

### 独立审稿补充证据

[i-vector-distance]: ../../index-vector/src/main/java/org/apache/iceberg/index/vector/VectorDistance.java#L47-L118 "hit score、SQL distance 和开平方 L2"
[i-text-reader]: ../../core/src/main/java/org/apache/iceberg/index/IndexReader.java#L130-L203 "search 与 searchStatistics 各自读取 payload；配合 RewriteTextSearch.scala:260–273"
[i-text-match]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/TextMatchExpression.scala#L35-L52 "match 求值仍依赖 score"
[c-optimize-options]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/OptimizeIndexExec.scala#L54-L91 "仅支持 num_indices_to_merge；144–160 driver 调 native"
[l-ann-append]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/index/append.rs#L895-L950 "已有 ANN 的未覆盖向量增量段构建"
[l-dir-fts]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-namespace-impls/src/dir.rs#L5500-L5509 "structured FTS 执行；6078–6157 模型转换"
[l-l2]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-linalg/src/distance/l2.rs#L106-L140 "L2 底层平方和；474–479 f32 scalar 调用"

### 部署资料：仅支持分工建议，不证明 pinned 产品兼容

[s-distributed]: https://github.com/lance-format/lance/blob/main/docs/src/guide/distributed_indexing.md "既有笔记所读现行 main 指南；不提供完整 scheduler，不能据新 API 承诺 pinned 12"
[s-enterprise]: https://docs.lancedb.com/enterprise/architecture.md "既有笔记所读 Enterprise query/indexer 与共享存储架构；不是 connector 源码"
[s-reindex]: https://docs.lancedb.com/indexing/reindexing.md "既有笔记所读 Enterprise 后台增量与 OSS 自管维护；未验 pinned Spark 接入"
