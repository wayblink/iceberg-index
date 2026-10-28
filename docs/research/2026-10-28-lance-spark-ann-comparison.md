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

# Lance Spark ANN 源码调研与 Iceberg 方案对比

**lance-format/lance-spark 已支持显式 `VECTOR_SEARCH` 查询，并可开启按索引段跨 Spark executor 并行；它不自动把普通距离排序改写为 ANN，也没有提供 Spark SQL 向量索引创建。**默认入口只有一个 Spark 输入分区，底层线程并发不能算成跨机器查询。与我方相比，值得借鉴的是过滤条件参与候选资格、执行器内回表精排、按需加载索引分区，不宜直接替换的原因是 Lance ANN 绑定自己的数据集清单、覆盖、行标识、删除和版本，不能消费我方 Puffin 索引。推荐保留 Iceberg 独立 sidecar 兼容路径，先补过滤和执行边界，再评估加载粒度。这个建议不意味着自建内核更快：维护成本与真实性能仍缺同条件证据，保留路径主要为了保护已有表语义及数据格式兼容性。

本文仅综合三份既有源码笔记，无此前报告，不开展新研究、网络、构建、测试或实测。connector、Lance 12.0.0 和我方完整冻结 SHA 见证据索引。“实现”只表示存在可追踪源码，“测试定义”不表示本次跑通过，未合入优化不计入当前能力。我方链接使用相对仓库路径并注明行号，不依赖临时工作树。([依赖][c-version])

## 显式入口能查询，不等于普通 SQL 或向量创建都已支持

连接器的当前入口是 `VECTOR_SEARCH(table, query_vector, k)`，向量须为常量或可折叠表达式，表须解析成具有命名空间和表标识的 Lance 数据集，不能直接传任意文件路径。旧 DataFrame 的 `nearest` 读取选项被明确拒绝，错误消息要求使用表函数。普通扫描的 TopN 下推只接受字段排序，不把距离表达式解析成最近邻查询；表函数外按距离排序只是排序已返回候选，不能宣传为普通 SQL 自动命中 ANN。搜索可传度量、过滤、版本、探测桶数、图搜索及精排参数，但普通扫描和搜索是不同调用链。([入口][c-entry])([表函数参数][c-function])([普通排序][c-topn])

我方反而提供普通 SQL 自动改写，但限制很窄：单表、单一升序距离、正数常量 LIMIT、常量查询向量，还要符合扫描形状及恒等分区限制。位于该 LIMIT 与扫描之间的业务 WHERE、连接、聚合、窗口、偏移或第二排序键会阻止改写，向量列 `IS NOT NULL` 允许。向量 TopK 子查询上面再包聚合或连接可以组合，只是不从这些算子内部任意抽取扫描。距离别名按属性身份识别，不按同名猜测。因此它减少了业务更换入口的负担，却不能被讲成任意带过滤 SQL 已高效使用向量索引。([改写][i-rewrite])

**我方普通 SQL 与 CALL/TVF 的合同不同。**普通 SQL 在有效非空向量域内使用索引活行候选、未覆盖文件残差和原向量精排。CALL/TVF 只返回 `INDEX_HITS`，包括文件、位置、分数和行内容；不补扫、不回表精排，删除或不可见命中允许 `row=null`。表函数外 WHERE 只能过滤已有命中，不能成为全表过滤后搜索。nullable 列另有差异：索引与残差均跳过 NULL，冻结规则却不要求列声明非空，所以可能不同于原 Spark 升序 NULL 优先的数量与顺序。严谨对照应显式限制非空域，并统一距离度量，不能把 NULL 排除说成已经实现逐行等价。([执行][i-exec])([命中合同][i-bridge])([NULL 测试定义][i-null])

创建与维护也应分开。Lance Spark 的 CREATE/REFRESH INDEX 白名单只有标量和全文索引，没有向量训练参数及 ANN 创建、刷新链路；已有向量查询测试使用 Java SDK 创建索引。不过通用 `OPTIMIZE INDEX` 在驱动端调用原生优化，没有同样的类型门禁，固定底层也有向量增量段、合并、重训及桶调整能力。因此不能说完全没有 ANN 维护入口，只能说本次没有其端到端验证。我方则提供向量构建、刷新和文件组重建；这是本分支的索引能力，不是上游 Iceberg 标准。([创建][c-create])([刷新][c-refresh])([通用维护][c-optimize])([我方构建][i-build])

## 两条执行流程显示并行单位与精排位置并不相同

**Lance 分布式流程：**表函数 → 驱动端打开数据集、冻结版本、选索引并解析覆盖 → 每个有效索引段一个任务、每个未覆盖 fragment 一个原向量平扫任务 → 执行器限定索引段或片段，执行本地最近邻和回表，按精排配置重打分 → 各任务都返回局部 `k+offset` → Spark 按距离、行标识全局排序截断，处理偏移并投影。连接器没有显式把回表结果全收进驱动端再重建 RDD。底层本地合并与 Spark 全局合并是两件事，每任务的候选预算也不是全表共享预算。([规划][c-plan])([任务读取][c-worker])([全局合并][c-merge])

默认分布式开关关闭，查询只有一个 Spark 输入分区，在读取器中调用命名空间查询并读取完整 Arrow 字节结果。后端可以另有服务或线程并发，但不是连接器跨执行器调度。开启已有开关后，并行粒度仍是 segment，不按桶数自动分任务；一个整表大段仍只有一个搜索任务。主会话补充的固定底层静态证据尤其重要：`scanner.rs:5650–5680` 对显式 `indexSegments` 且没有片段限制的查询跳过补扫。因此并不是每个索引段任务都重复补扫其他段，未覆盖片段由独立平扫任务处理。([默认路径][c-default])([范围限定][l-segment])

**我方普通 SQL 流程：**安全 SQL 绑定表快照、祖先索引快照和覆盖 → 驱动端冷工作集估算可能整文件读取并准备搜索对象 → 每索引文件一个执行器任务，读取 Puffin 并生成候选 → 有界全局选择 `C+1` 个命中回驱动端 → 执行器回表，但结果收集为驱动端映射，检查删除及可见性并按需补候选 → 活行重新形成 RDD，与分布式残差合并 → Spark 用原向量距离排序截断。这里 C 为结果数乘过采样，默认过采样为一。不能因驱动端屏障说索引计算全在驱动端，也不能因搜索分布式说全链没有驱动端物化。([搜索][i-search])([回表][i-rows])([执行][i-exec])

我方默认每数据文件一个索引文件，也能分组，任务不拆组内图或索引载荷。多个文件提供更多并行机会，同时带来独立模型训练、任务扇出和合并成本；Lance 大段减少段开销，却限制跨执行器并行。两者都不是自动分片，不能仅凭任务数判断速度。局部候选合成全局结果的正确性也只针对实际返回集合，不保证近似搜索召回了全表真实最近邻。本地候选预算还要区分搜索与精排：连接器显式探测桶数在每个索引段独立生效；我方过采样扩大候选量，却不会自动扩大倒排探测桶数，也没有已实现的自适应召回校准策略。([我方预算][i-budget])

算法不能混称。Lance IVF_PQ 先选桶，再使用分段码本估距；IVF_HNSW 是先选桶、桶内走图，可组合 PQ/SQ。我方 HNSW 使用纯 Java 库，只支持不量化；IVF 与 RaBitQ 的训练、编码和打分由我方实现，不是 Lance、Faiss 或 PQ。IVF_FLAT 即使桶内按原向量算距离，也仍可能漏掉未探测桶；原向量全表平扫才是另一种合同。这些算法的质量、内存和构建成本不能用一个 ANN 名称抹平。([PQ][l-pq])([桶内图][l-hnsw])([我方内核][i-kernels])([我方 IVF][i-ivf])

| 维度 | Lance Spark 与固定底层 | 我方冻结方案 | 含义 |
|---|---|---|---|
| 查询入口 | 显式表函数，拒绝旧选项 | 安全 SQL 改写；另有索引命中入口 | 先统一结果合同 |
| 并行粒度 | 默认单任务；可按索引段及未覆盖片段并行 | 每文件或文件组一个任务 | 大对象不会自动拆分 |
| 候选资格 | 用户过滤与删除掩码参与搜索 | 业务 WHERE 阻止改写；删除回表后发现 | 过滤是实质缺口 |
| 精排顺序 | 本地精排，再 Spark 全局合并 | 近似分数先全局裁剪，再回表精排 | 质量与回表量有取舍 |
| 未覆盖数据 | 默认补扫，快速模式省略 | 普通 SQL 补扫；命中入口不补 | 不能混用完整域与命中合同 |
| 缓存加载 | 按桶加载与缓存 | 整容器加搜索对象，冷估算也准备对象 | 暖态不能遮住冷成本 |
| 维护入口 | SDK 创建向量索引，通用原生优化 | 向量构建与刷新，受文件组影响 | 底层不等于连接器暴露 |
| 地址与版本 | 数据集清单、片段及行标识 | 表快照、文件位置和独立清单 | 无法直接交换索引文件 |

## 一亿条租户筛选和追加数据揭示正确性边界

设一亿条向量中要找租户 A 最相近的一百条。先全局搜一百条再筛 A，与先确定 A 的合法集合再搜一百条，是两个业务问题：前者可能只剩三条甚至零条，扩大精排参数也不改变过滤在截断之后的含义。Lance 分布式必须把条件放进搜索过滤参数，并显式 `prefilter=true`；省略或为假会报错。表函数的专用扫描没有外层过滤下推，所以外 WHERE 不能替代它。默认命名空间路径则可以后过滤，不能与分布式准入语义混为一谈。([门禁][c-filter])([扫描边界][c-builder])

下面是“租户 A（tenant_id=42）内找最像的 100 条”的入口示例，假设已配置 Lance catalog、Spark 扩展并通过 SDK 建好适用索引。它说明已有参数的用法，不表示本次运行验证；省略具体探测与精排数值，避免把任意调优值当成推荐默认。

```sql
SET spark.sql.lance.search.distributed.enabled=true;

SELECT id, _distance
FROM VECTOR_SEARCH(
  table => 'lance.default.items',
  query_vector => array(0.12, 0.34, 0.56),
  vector_column => 'embedding',
  num_results => 100,
  distance_type => 'cosine',
  filter => 'tenant_id = 42',
  prefilter => true,
  columns => array('id')
)
ORDER BY _distance;
```

原生预过滤通过精确标量索引或扫描条件列生成合法行标识，再与删除、更新掩码相交；平扫在候选入堆前检查资格，图搜索也有稀疏过滤分支。这不是简单多取一点再丢，而是知道哪些行允许竞争。但预过滤不保证精确召回或无条件足额：固定探测预算可能不再补桶，图搜索仍有近似遗漏，合法行本身也可能不足结果数。对极稀疏合法集合的底层占位距离，还不能不经原向量校准就解释成精确距离。([资格来源][l-filter-source])([补探与占位][l-late])

我方普通 SQL 在 LIMIT 下看到租户条件会放弃向量改写，保留扫描、过滤和排序的原语义。移到 TVF 外虽能用索引，却换成先搜后筛。这个缺口应明确承认，不能拿非空过滤、外层 WHERE 或过采样冒充租户过滤 ANN。推荐首先借鉴过滤感知的候选资格机制，正是因为它决定回答的集合是否符合问题，而不仅决定延迟。与此同时，任意业务条件如何变成合法行集合、怎样处理代价和近似预算，仍须独立论证，不能把建议当成现有实现。

再设旧索引覆盖一百个数据片段，追加两个，最近答案恰在新数据。Lance 默认搜旧索引并扫新片段，分布式生成独立平扫任务；快速模式刻意省略它们。我方普通 SQL 从冻结表文件减去覆盖集合，新增文件进入删除感知残差；CALL/TVF 仍只有旧索引命中。两种完整域路径都避免新文件整批不可见，却不能修复旧覆盖区内的近似漏召回。追加不断累积会扩大补扫，维护减少的是这部分成本，不是让新数据第一次可见。([底层补扫][l-fallback])([我方覆盖][i-bind])([我方残差][i-exec])

删除和重写更能体现模型区别。Lance 依据当前数据集清单，把删除向量、缺失片段、稳定行标识可见性和更新覆盖纳入掩码；物理地址与稳定行标识不是同一种东西，后者需先解析。固定版本已有更新旧行屏蔽及补查能力，不能把它归到未来版本，但也不能据此说连接器提供对应创建接口。我方按冻结文件位置回表，排除快照外旧文件及位置、等值删除；删除命中占预算时普通 SQL 多轮补候选，有轮数和预算停止条件，不能保证高删除率足额。压缩后的新文件靠残差接住，刷新可能重建受影响整组，并非产品已原地追加图节点。([删除掩码][l-mask])([行标识][l-take])([更新覆盖][l-overlay])([我方执行][i-exec])([刷新][i-build])

两边冻结快照都不意味着旧文件永远不被清理。索引文件的覆盖与当前活行集合也是不同证据：文件声明被索引，不等于每个候选仍有效。对我方还不能因为注册有比较交换，就推导单索引刷新与数据提交属于同一原子事务。它们各自的失败窗口、旧版本文件保留和并发发布，需要独立验收；源码中有回滚或版本字段，不能代替完整运行证据。([我方发布][i-sidecar])

精排顺序同样有取舍。Lance 显式精排时扩局部候选、读取原向量、重打分后留局部结果；因子为一只校准已有候选，不扩召回。混合量化索引与平扫的分布式计划默认给索引侧因子一，避免量化距离与精确距离混排；纯索引未设置精排时仍可能返回量化距离。我方 RaBitQ 先按估分全局淘汰，后续精排救不回落选向量，但回表池更集中。段很多时本地精排放大回表；先全局裁剪减少读取，却更受跨文件估分误差影响。**本地先精排不是必然更好，应同时比较召回、回表量与端到端代价。**([混合校准][c-mixed])([底层精排][l-refine])([我方预算][i-budget])

## 按需加载值得学，直接换 ANN 却会改变表模型

Lance 原生 Session 共享缓存，按 IVF 分区键查找，未命中才读取对应索引行范围和量化存储；冷查询不是必然整索引载入。缓存容量也不是进程总内存上限，回表、临时计算、训练及仍被引用对象另有成本。连接器接入每 catalog 的进程内会话，不等于提供跨执行器副本一致性或统一驻留服务。我方懒缓存默认关闭，开启后保存完整 Puffin 字节与准备好的搜索对象，并有单搜索对象锁；未知工作集估算会在驱动端整文件读取、解码和准备对象，关闭缓存也不能说驱动端只读清单。当前 IVF 查询还生成探测候选对象并全排序。([按需加载][l-cache])([连接器会话][c-session])([我方缓存][i-cache])([冷估算][i-search])

我方确有插件式加载、卸载、预算、命令绑定、会话及代际检查和确认应答协议，不应忽略。但查询位置提示只消费任务报告并返回主机，不是按确认副本进行执行器强路由。同主机多执行器、首次无提示或节点变化，都能使驻留完成不等于下次暖命中。预算约束缓存记账，不约束所有暂态对象和未缓存查询总内存。#2773 的原地目录、有界候选堆、独占所有权、内存模型二及驱动端读取优化未合入冻结版本，本报告不拿设计或分支进度当当前性能。([驻留确认][i-residency])([位置提示][i-rdd])([冻结模型][i-memory])

我方采用 Lance 数据格式，利用的是**文件级按行范围回表**：把 Iceberg 命中位置合并成范围，通过文件读取器读列和行，再转为记录；不调用 Lance Dataset 最近邻。向量索引仍由 Java 内核写入 Puffin，覆盖和索引快照属于我方独立清单。读写 Lance 格式需要其 SDK/native，但这不改变索引内核身份，也不自动获得数据集的索引功能。数据文件格式只回答怎样读取这个文件，并没有替 Iceberg 接管整张表的身份和生命周期。([格式回表][i-lance])([读取分派][i-rows])

Lance Dataset 则需要自己的版本清单、片段列表、覆盖、索引标识、行地址和删除规则。**一个 Lance 数据文件不等于 Lance Dataset，原生 ANN 也不认识我方 Puffin 内容。**直接替换不仅要转换索引字节，还要映射 Iceberg 删除、压缩、历史读和发布维护，已经超过换一个成熟内核。对本来就由 Lance 数据集管理、愿意使用显式入口的应用，原生组合能减少自建工作，应认真考虑。保留我方路径的弱点是 Java 算法、分组训练、清单演进、删除补候选及缓存协议都要持续维护；当前证据不能证明它的效率、量化质量或运维成本更优。

## 优先补资格和执行边界，收益必须按同条件裁决

建议优先级为**先候选过滤资格，再执行器内回表精排与 Spark 全局合并，随后按需加载**。第一项让租户问题不被偷换成先搜后筛；第二项减少驱动端活行往返，把可见性与重打分更靠近数据，但必须审查局部预算及回表放大；第三项针对整索引热对象与冷准备成本，减少无关桶读取和反序列化。这里只推荐借鉴机制，不提出新参数、不改默认值、不实施设计。兼容路径继续服务现有 Iceberg 文件；是否引入原生内核应作为独立兼容性决策，而不是借文件回表名义接管数据集。

未来比较必须先约定同一数据、快照、维度和有效非空域，统一 float 与距离语义，区别欧氏距离与平方距离、余弦距离与内积分数方向；原向量全扫、无量化结构及各自量化配置分开，不能用不量化图对量化倒排得出引擎倍率。固定桶数、`nprobe/nprobes`、图搜索预算、`oversample/refine` 候选含义，并以同域精确真值衡量召回。过滤必须同条件且在候选前，固定选择率和租户分布；删除、追加覆盖、段及文件组布局也相同，快速模式或只命中入口不能和完整域查询混比。

冷态应包含驱动端估算、存储读取和准备对象；暖态记录真实执行器缓存命中和调度落点，不以加载确认代替暖态。端到端延迟分位、吞吐及召回同时报告，并记录存储读量、回表行数与文件量、驱动端和执行器峰值、构建和刷新成本，固定硬件、资源与重复方法。局部内核很快却回表昂贵，或降低召回换来更小延迟，都不构成同质量收益。当前没有这份比较实测合同的结果，因而不报告倍率或生产验收通过。

## 结论

决定是否采用 Lance 的核心不是名字，而是愿不愿让另一套数据集接管地址、覆盖和可见性。现有 Iceberg 表最有价值的是借鉴机制，而非交换索引文件；已有 Lance 数据集则能直接利用原生组合减少自建负担。下一步先让合法集合正确进入搜索，再让冷加载和回表成本可审计，最后用同质量端到端证据决定优化或替换哪部分。兼容性不是性能证明，尚未验证的性能预期也不应先决定数据模型。

## 证据索引（链接不计入正文篇幅）

冻结 connector：`ed2f49f94dfdb4ad81a2153cb658443864cd4d54`；依赖 Lance 12.0.0，底层：`cbeec97cb893a66ad6b3db87f25a57ab0093f358`；我方：`e1bebd7e6c5f425867788fb221df0c9e96c9ab33`。我方相对链接从本文目录回到仓库根；以下行号均针对冻结提交，不用稍后分支替代。

我方索引行号：改写 81–175、182–249、264–333；执行 128–286；命中桥接 90–216；NULL 测试定义 235–284；构建/刷新 169–253、340–554；分布式搜索 131–243、573–632；回表 248–273、520–555、722–789；内核注册 72–115；IVF 116–260、365–397；绑定覆盖 93–127；候选预算 34–90；缓存 161–266、378–444、1311–1321；驻留确认 566–735、878–919；位置提示 57–75；内存模型 28–48；Lance 文件回表 421–466、542–546；sidecar 48–85、255–264、296–335、437–453、672–681。

[c-version]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/pom.xml#L57-L61
[c-function]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/lance/spark/search/LanceSearchTableFunctions.scala#L36-L104
[c-entry]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/LanceSparkReadOptions.java#L572-L577
[c-topn]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/read/LanceScanBuilder.java#L600-L705
[c-create]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/AddIndexExec.scala#L582-L599
[c-refresh]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/RefreshIndexExec.scala#L136-L204
[c-optimize]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/OptimizeIndexExec.scala#L144-L180
[c-plan]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchScan.java#L112-L222
[c-worker]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchColumnarPartitionReader.java#L114-L193
[c-merge]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/lance/spark/search/LanceSearchTableFunctions.scala#L297-L347
[c-default]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceSearchScan.java#L50-L58
[c-filter]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/lance/spark/search/LanceSearchTableFunctions.scala#L349-L381
[c-builder]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchScanBuilder.java#L20-L33
[c-mixed]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceDistributedSearchScan.java#L159-L189
[c-session]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/LanceRuntime.java#L232-L280
[l-segment]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/dataset/scanner.rs#L5650-L5680
[l-pq]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-index/src/vector/pq.rs#L300-L420
[l-hnsw]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/java/src/main/java/org/lance/index/vector/VectorIndexParams.java#L139-L174
[l-filter-source]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/dataset/scanner.rs#L6926-L6995
[l-late]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/io/exec/knn.rs#L1902-L2032
[l-fallback]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/dataset/scanner.rs#L5787-L5896
[l-mask]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/index/prefilter.rs#L290-L419
[l-take]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/dataset/take.rs#L551-L580
[l-overlay]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/dataset/scanner.rs#L5444-L5463
[l-refine]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/dataset/scanner.rs#L5439-L5487
[l-cache]: https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance/src/index/vector/ivf/v2.rs#L1536-L1638
[i-rewrite]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/RewriteVectorSearch.scala
[i-exec]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorSearchExec.scala
[i-bridge]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorSearchBridge.scala
[i-null]: ../../spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestVectorSearchSharedRules.java
[i-build]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/SparkIndexBuilder.java
[i-search]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/DistributedIndexSearch.java
[i-rows]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/IndexEntries.java
[i-kernels]: ../../index-vector/src/main/java/org/apache/iceberg/index/vector/VectorKernels.java
[i-ivf]: ../../index-vector/src/main/java/org/apache/iceberg/index/vector/IvfKernel.java
[i-bind]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/BindVectorSearch.scala
[i-budget]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorCandidateBudget.scala
[i-cache]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/ExecutorIndexCache.java
[i-residency]: ../../index-spark/src/main/java/org/apache/iceberg/index/spark/IndexResidencyCoordinator.java
[i-rdd]: ../../index-spark/src/main/scala/org/apache/iceberg/index/spark/IndexFileRDD.scala
[i-memory]: ../../index-vector/src/main/java/org/apache/iceberg/index/vector/VectorMemoryEstimates.java
[i-lance]: ../../lance/src/main/java/org/apache/iceberg/lance/LanceFormatModel.java
[i-sidecar]: ../../core/src/main/java/org/apache/iceberg/index/SidecarIndexCatalog.java
