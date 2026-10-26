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

# #2023：向量查询的共享优化规则与物理规划

状态：用户已于2026-10-26授权阶段A开发。阶段A已实现并合入 main（共享参数决策、冻结绑定、SQL 逻辑/物理计划分离、CALL/TVF 共用计划），其上的可观察性与执行机制（EXPLAIN 决策、Spark metrics、补候选去重）已补齐并测试。代码核对基线：`d2e2727591f47abeffb96c0847475be7cb042450`（2026-10-26）；本轮实现基线 `a1477fa64`。本设计取代卡片原来的“成本过高就让开全扫”方向。阶段B（自动倍数调优、校准与质量目标）仍未开始。

## 1. 推荐方案与边界

复用 Catalyst：入口生成向量逻辑节点，共享规则决定未设置的参数和执行结构，Planner Strategy 生成物理算子，执行阶段才搜索和回表。不另造与 SparkSQL 并行的优化器。

用户已确认：

- 显式参数必须保留，包括显式设置成默认值；未设置的才由规则决定。
- 普通向量 SQL 与 `CALL vector_search` 共用参数决策规则。
- 对完整表查询，覆盖部分走索引，未覆盖部分扫描，局部合并后整体合并；不因覆盖率低而切换为全表扫描。
- 文件分布和小文件问题归后台整理；优化器只消费已有统计。

本草案进一步推荐：先交付可解释、参数不丢失、规划不执行查询的框架，再引入有校准证据的自动调参。不能把“1bit 取50倍”或“每候选6ms”当作通用定律。

本期不改文本查询、不重构 kernel/cache、不新增依赖、不新增公开 Java API，不做在线采样搜索或执行中自动重调参数。范围以 Spark3.5 为首个接入版本，不宣称已经覆盖其他 Spark 版本。

## 2. 必须正视的现状与兼容差异

| 现有路径 | 已有行为 | 设计处理 |
| --- | --- | --- |
| `RewriteVectorSearch` | 已注册为 Optimizer Rule，但 rule 内搜索、回表、残差扫描并生成结果 `LocalRelation` | 改成生成计划，不在 rule 内读业务数据或启动 job |
| 普通向量 SQL | 活行候选精确重打分，与未覆盖文件扫描结果合并 | 保留此完整表查询合同 |
| `CALL vector_search` / batch | 搜索索引快照的候选，返回原始索引 score；在当前表快照回表，已删除命中仍可能以 `row=null` 返回；不扫未覆盖文件 | 首期保留此索引命中/诊断合同，不暗中改成完整表查询 |
| 向量 TVF | 目前调用旧搜索 API，函数构造时就可能执行 | 一并改成生成共享请求节点，保留 CALL 型输出合同 |
| `index-spark` Java API | 泛型 `IndexSearch` 同时服务文本与向量 | 保持签名和既有行为，不反向依赖 Spark 主模块 |

**规则统一不意味着两个现有结果合同被强行合并。** CALL 目前不是普通 SQL 的另一种输出格式。为兼容，公共搜索/参数子计划共用；候选之后的完成步骤依据合同不同。若希望 CALL 也扫描未覆盖文件、移除 tombstone、精确重排并与 SQL 同结果，需要另行批准行为变更，不能只修改 `score` 字段含义。

同样，普通 SQL 的所有形状不能都贸然改写。首期采用明确白名单：单表、可折叠正数 LIMIT、单一升序向量距离、常量查询向量、可绑定的列/投影。Join、Aggregate、窗口、Offset、非确定性表达式以及尚未证明安全的过滤，不进入向量计划。保留原 Spark 计划是查询资格限制，不是成本规则。

## 3. 整体结构

```text
普通 SQL ──识别已分析的 TopK──┐
CALL ──在 procedure.call 内构造内层请求──┼── VectorSearchRequest
向量 TVF ──只构造请求─────────┘       │
                            共享 Vector Rule 链
                                     │
                         参数、来源、冻结快照、结果合同
                                     │
                         LowerVectorSearchPlan
                                     │
                         VectorSearchStrategy
                                     │
                         Spark 物理执行与结果适配
```

CALL 外层保持 `CallExec -> procedure.call()`；procedure 内通过 Spark 主模块的 `private[spark]` bridge 创建内层 QueryExecution，执行共享向量计划后转换为现有 `InternalRow[]`。不改通用 Procedure SPI，不在 ResolveProcedures 中调用搜索。

TVF不调用这个执行型bridge。`IndexTableFunctions`的两个向量builder只检查arity/常量参数类型、折叠查询向量，返回带已知output属性的`VectorSearchRequest(contract=INDEX_HITS)`叶子；表名作为待绑定的请求值，不调用`IndexSearch.search`、createDataFrame或内层QueryExecution。scalar输出保持`SINGLE_RESULT_SCHEMA`四列，batch输出保持`RESULT_SCHEMA`五列及query_id。共享optimizer直接处理这种请求（不要求外面有GlobalLimit），在`BindVectorSearch`中加载表元数据并冻结上下文。strategy最终以`ExistingResultFormat`物理节点输出对应schema，复用候选和legacy回表子节点。外层Project/Filter/Join按普通Spark计划处理，不由向量规则吞掉；输出ExprId从builder到物理节点保持一致。表/索引/维度错误在绑定时报告，业务文件与payload仍只在物理执行时读取。

依赖方向保持 `spark -> index-spark -> core/index-vector`。不能让 `VectorIndexSearch` 回调上层 Scala planner，否则形成模块环。新 Catalyst 类型、规则和 bridge 都在 Spark 主模块；extensions 模块只注册 rule/strategy。

## 4. 参数必须携带来源，而不是只有一个数值

推荐内部不可变表示 `ParameterValue(value, origin)`；未设置时保留缺席状态，不提前塞默认值。来源至少区分 `CALL_ARGUMENT`、`USER_CONF`、`SESSION_CONF`、`SUBMIT_CONF`、`INDEX_DEFINITION`、`AUTO`、`COMPAT_DEFAULT`。不使用公开配置接口或 Java Optional。

优先级：当前入口显式参数 > 当前 session 显式设置 > submit 配置 > 适用于该参数的索引定义显式值 > 自动决策/兼容默认值。下层显式值被上层显式值取代是用户配置优先级，不是优化覆盖；决策记录应保留被遮蔽来源。

- 现有 `index => ...` 参数优先于 session 的索引选择。CALL 首期不新增 `over_sample` 参数，通过已有 session key 设置。
- `spark.iceberg.index.vector.over-sample` 与 `.name` 两入口统一读取。session 后设置的值优先于 submit；现有 helper 是反过来的，这属于需要批准的行为修正。
- 仅从 raw conf 中判断用户是否设置，不能从带默认值的 getter 反推。Spark bootstrap可能把submit值复制进session；当两者同值且无法辨别时记为 `USER_CONF(session-or-submit)`，仍锁定，不虚报一定发生过SET。不引入session写入监听器追溯来源。若session与submit值不同，则使用session并记录遮蔽；raw两处都没有才是未设置。
- 显式 `over-sample=1` 与未设置后得到1是不同状态。规则写入只能针对缺席项；写完后的自动值也不允许被下一条规则反复改写。
- 空字符串、非整数、非正数不是未设置。推荐报错，不静默改成1。现有0/负数被归一成1的行为及对应测试需要明确迁移，不能冒称没有兼容变化。
- `nlist`、量化位数、图构建参数属于索引结构事实，不由查询规则改变。`nprobe` 和 `ef-search` 当前也从定义进入 kernel，首期保留既有有效值，不声称已支持按查询调整。后续若引入 query-time options，先设计 kernel 入参和 cache 身份隔离，不能修改共享定义或缓存中的 searcher。

本期规则可决策的参数是 `over-sample`；索引名称未设置且有多个候选时不新增未经校准的索引自动选择。CALL 保留已有“唯一索引或显式指定，否则报歧义”的行为；普通 SQL 保留多索引未指定时不改写。共享决策器返回同样的 `AMBIGUOUS_INDEX` 状态，由入口合同决定报错或保留原 SQL，不暗选一个索引。

## 5. 共享上下文与快照固定

`VectorSearchContext` 至少包含：请求向量/批次、K、字段ID、输出属性、所选索引 UUID/定义、结果合同、原始参数及来源、读取表快照、索引快照、文件覆盖集合、可用条目统计、计划解释记录。

普通 SQL 从 SparkTable 的 pinned snapshot、branch head 或 current snapshot 解析一次。索引按该表快照祖先链选择 `AT_OR_BEFORE`；快照ID不能按数值或时间戳直接比较。

CALL 保留索引最新快照搜索与当前表回表的合同，但把两个快照ID在内层规划时一起固定。原 API 搜索结束后才读取 currentSnapshot 的时序不再作为承诺；计划形成后的 append/delete/refresh 不应让同次查询混读新快照。

规划允许读取表/索引元数据和 manifests，不读取数据文件、索引 payload，不 prepare kernel，不预热 cache，不启动 Spark job。复制需保留的 manifest 对象，关闭 CloseableIterable，不能保留容器复用引用。物理阶段不重新选择 latest snapshot；已固定资源被清理或失效时明确失败，不静默改用新索引或新表快照。

冻结的不只是ID。绑定时生成不可变`ResolvedExecutionInputs`：保存`SerializableTable.copyOf(table)`及其固定metadata location、已解析的表/索引snapshot对象、captured IndexMetadata、manifest-list URI、复制的可搜索index文件描述符、FileScanTask列表、读schema以及coverage集合。绑定时预先解析SerializableTable的所需snapshot/祖先元数据，不能把对原可刷新Table的延迟读取留到执行期。回表和残差使用保存的scanTasks overload，不重新planFiles或查看当前branch head。

首期候选API可能仍重新读取index manifests，但必须传captured IndexMetadata和冻结Table，使IndexReader重放同一snapshot/manifest URI；coverage和任务集合以保存值为准，不重新绑定。依赖Iceberg元数据URI不可变的契约，不支持原地覆写同一URI，也不增加内容hash/资源租约承诺。新的refresh不会进入captured metadata。

绑定时所需表元数据/已引用manifest无法读取或损坏，规划立即失败；“根本没有适配读快照的索引snapshot”才是扫描退化，而非吞掉文件错误。绑定成功后只是catalog将snapshot标记过期、但保存资源仍可读时，旧计划仍可执行；若后续metadata重读、payload、数据文件或delete文件已被清理，则在首次物理读取时失败并报告保存的snapshot与资源位置，不刷新表或切换新快照。无需再访问live catalog检查是否过期，因此不声称snapshot lease或保证过期查询成功。

## 6. Rule 链：每条规则有单一产出

首期用一个注入的 Catalyst 规则作为入口，内部按固定顺序组合下面的向量规则。这仍是 Catalyst Rule，不是另一个独立优化器；能控制执行顺序，也避免依赖多个注入规则的隐含排序。

| 顺序 | 规则 | 读什么 | 产生什么/限制 |
| --- | --- | --- | --- |
| 1 | `ExtractVectorSearch` | 已分析的普通 SQL 或入口请求 | 统一请求；不跨越 Join/Filter 等算子寻找“第一个 scan” |
| 2 | `BindVectorSearch` | 表、字段、索引选择和元数据 | 固定字段、metric、快照及结果合同；不会因覆盖少取消索引路径 |
| 3 | `ValidateVectorParameters` | 原始参数、维度、K | 命名具体参数的错误；不修正非法显式值 |
| 4 | `ChooseUnsetParameters` | 未设置项、已有可信信息 | 决定 over-sample 并记录依据；不写锁定项 |
| 5 | `DeriveCandidateBudget` | K、最终 over-sample、条目上界 | 计算请求候选数，不修改参数本身 |
| 6 | `BuildCoverageBranches` | 固定快照的覆盖元数据 | 完整表查询生成索引分支与残差分支；CALL 合同只有索引命中分支 |
| 7 | `LowerVectorSearchPlan` | 上述不可变上下文 | 生成可规划逻辑节点；处理空分支，不执行数据查询 |

每条规则声明读/写字段；只有第4条能填未设置的可调项。降低后的节点标记为已决策，不再次提取。反复优化得到同样参数/计划，不再读取 session 值，也不重复元数据解析。规则本身不写 table properties/session conf。

如果没有索引、没有可回答当前读快照的索引快照或覆盖为0，完整表查询只有扫描分支。这是集合为空的退化，不是“低覆盖率成本太大”。有一部分覆盖时索引分支必须保留。删除原来的仅因 `FLAT` 结构而拒绝改写的成本 guard；FLAT 也能生成索引分支，明确更新相应旧测试。

## 7. 未设置参数怎么决定：先守住证据边界

首期可直接交付的策略：未设置 over-sample 时使用现有兼容基线1，来源标为 `COMPAT_DEFAULT(no-calibrated-policy)`。这不是声称1最优，也不是新的召回保证。框架、计划解释和预算推导先可用，自动调参不能靠随手写的10/50/100兑现。

有校准数据后的策略应是独立 `ChooseUnsetParameters` 实现：对于同一索引/数据范围和查询分布，测不同倍数的召回与延迟，在不低于已批准质量基线的可行集合里选代价较小者。没有匹配证据则回到兼容基线，不跨数据集套单价。

校准策略的保存格式/发布方式、质量目标和候选倍数集合属于后续调优阶段，本期不新增一组 profile 配置或公共 API。**若用户要求本次就上线自适应倍数，则校准与质量目标必须补齐，不能仅实现框架后声称任务已完成。**

近似索引未召回的行无法由精确重打分补回。混合执行也不因此变成全表精确 TopK；任何解释不得写成“over-sample=100保证95%召回”。不做查询时探测/搜索以决定倍数，因为那又把执行塞回了优化器。

## 8. 候选预算不是修改用户参数

令 `C_requested = K × overSample`。计算顺序固定为：先检查long乘法；再按已知M收缩；最后检查有效预算是否能用现有int接口表示。若long乘法本身溢出，即使M很小也报错；若请求仅超过int、但M=20，则允许收缩成20后执行。M未知且请求超过int，或收缩后仍超过int，才报有效预算不可表示。不静默降低用户参数。

同一批次内的预算和 TopK 均按 `query_id` 分组，不能把全批次只截成K条。CALL 的非正K与空批次保留既有空结果合同；它们在向量参数校验前归一为空请求。SQL LIMIT 的非法值由 Spark 原有分析处理，LIMIT 0无需搜索。

如果元数据给出了所选索引快照可靠的条目总上界 M，可以执行 `C_initial=min(C_requested,M)`；unknown 时不得把它当0。M是同一索引搜索范围的全局条目上界，每个query各自使用此上界，不是全批次共享额度，也不是按查询向量预测命中数。按唯一可搜索entry-bearing文件描述符汇总entryCount，重复描述符/辅助文件不计；缺失或不可靠计数则M未知。该上界可以偏大但不能偏小。这个收缩仅表示“总共就这么多候选”，解释仍显示用户值100，而不是把 over-sample 改成2。

例如 K=10、用户倍数100、索引最多20条：参数仍是100，请求预算1000，实际最多20条。若只知道文件数、字节数，不能据此推断M或削减候选。

完整表合同中的delete-aware补候选最多10轮；CALL/TVF的INDEX_HITS合同不补删除命中，仍返回nullable row。补候选算法按每个query定义：

1. 第1轮累计请求量`C_1=C_initial`；每轮重新请求累计Top-`C_r`，不是只请求差额，也不是使用offset。全局合并后最多C_r个候选，不把C_r当硬性driver峰值限制。
2. 按固定快照回表，新位置与前轮已解析活行按(query_id,file,pos)合并；近似kernel不承诺扩大C时严格保持旧候选前缀，因此保留此前找到的活行。设已累计找到的唯一活行数为V_r。
3. V_r达到K即完成；否则`moreHits=false`表示kernel报告无更多候选，或已知M且C_r>=M表示预算已覆盖全部条目上界。两者分别记录`kernel_exhausted`与`entry_bound_reached`，不把它们混为精确全表召回证明。
4. 未停止则`C_next=C_r+max(1,K-V_r)`，再按已知M收缩、检查有效int范围，必须大于C_r。不能表示下一预算时明确失败，不产生绕回的负数。第10轮仍不足且未耗尽时返回已找到活行，标记`underfilled_retry_limit`；不额外做第11轮。

补候选属于填补删除/不可见命中的执行机制，不重调over-sample。每轮合并批次的新positions再回表，复用已解析活行；首期普通SQL是单查询，CALL batch不进入上述补候选循环。保留10轮不足时返回已有活行的既有合同，但累计请求严格递增及保留前轮活行是明确的执行安全修正，需测试，不冒称逐条旧结果完全不变。严格足额合同需要另行设计，不偷偷扫描已覆盖部分兜底。

## 9. 物理计划与执行边界

完整表合同的目标形状：

```text
GlobalTopK(K, exact distance)
  Union
    LocalTopK(K, exact distance)
      ExactRescore
        DeleteAwareRowsAt
          VectorIndexCandidates(C_initial, fixed index snapshot)
    LocalTopK(K, exact distance)
      VectorResidualScan(uncovered files, fixed table snapshot)
```

CALL/TVF 的兼容形状：

```text
ExistingResultFormat(data_file, pos, raw score, nullable row[, query_id])
  LegacyRowsAt(fixed table snapshot)
    IndexRankTopK(K, raw adapter ordering)
      VectorIndexCandidates(C_initial, fixed index snapshot)
```

两个合同共享参数/预算和索引候选节点；CALL 不把原始 score 当 `vec_distance`，不扫描未覆盖文件，不用精确重排替换原始候选排名。因此 CALL 中较大的 over-sample 可能只增加候选工作，不改变最终诊断结果；近似 kernel 也可能因搜索范围扩大而返回不同候选，不保证显式大倍数时仍与旧 C=K 的候选逐条相同。计划需要如实说明，而不是让用户以为已经提高了完整表召回。未来统一结果合同需要显式批准。

Planner Strategy 只处理自己的向量节点；排序、投影、Union、TopK 尽量交给 Spark 自带算子。不能在构造物理节点、`doPrepare` 或 explain 时调用 `search`、`rowsAt`、`collect`、`takeOrdered`。

**首期执行允许受控 driver barrier。** `DistributedIndexSearch` 当前返回 driver 合并的候选，`IndexEntries.rowsAt` 返回 driver Map。物理节点在 driver 的实际执行方法中调用这些现有 API，再把结果作为 RDD 提交给后续算子。不能把调用放到 executor task，那里不得创建嵌套 Spark job。这是最小兼容接入，不虚称已把回表完全分布式化。

此时 driver 会保留候选与回表数据，成本与请求预算、命中文件数和批次数有关，没有新硬内存保证。Explain 标明 `driver-materialization=true`，metrics 记录候选/回表量。只移动执行时机，不承诺立刻减少候选×文件成本。

后续消除 barrier 的 RDD 化回表/候选接口属于独立优化，不在本期顺手扩大公共 API。批次仍应共用索引加载和回表，每轮合并所有 query 的 positions，同一命中文件该轮最多读取一次，不能退化成每个 query 单独 CALL。补候选发现同一文件的新位置时，现有 reader 可能再次读取它，不承诺全查询该文件始终只读一次。

## 10. 正确性资格与结果处理

- SQL 分支选择只改变获取候选的方式，不吞掉投影、过滤、ExprId 或原排序约束。首期不做跨任意算子提取。
- 相同 `(query_id,data_file,pos)` 的重复命中在行语义上只算一次；不同位置即使列值相同仍是不同表行。去重规则需在候选/局部合并处一致，不按业务列去重。
- COSINE/DOT_PRODUCT 原 score 越大越好，EUCLIDEAN 越小越好；候选使用 adapter comparator，完整表重排使用 `VectorDistance.distance`，不能简单反转一个通用比较器。metric 在绑定后固定，未进入向量计划的 SQL 保持原 expression 语义。
- 同距离且没有显式次级排序时不承诺新的跨运行稳定顺序；保留 CALL 现有 comparator 约定，不偷偷增加面向用户的排序条件。
- 空向量/NULL/错维度不能随意替换成最大距离。普通 SQL 的 NULL 排序与索引只保存可用向量的行为未必等价。首期必须证明输入向量域有效，或采用可证明安全的 `embedding IS NOT NULL` 等域约束；没有充分元数据/约束时保留 Spark 计划，而不是全表采样检测。不得仅凭“已有索引”声称这些 SQL 都能等价改写。该资格检查可能缩小旧规则的改写范围，需明确测试与说明。
- delete-aware reader 和纳秒 equality-delete 拒绝保护复用现有逻辑；不在宽泛 `catch NonFatal` 中把权限、损坏或不支持类型错误吞成扫描 fallback。

首期的NULL资格判定具体为：向量列有可信非空schema约束，或在唯一表扫描之上直接存在且仅存在 `IsNotNull(vectorAttribute)` 域过滤；该过滤保留并应用于两个分支。其他任意WHERE不进入首期白名单。不能从索引存在推断全表没有NULL。残差与回表后的数值/维度校验使用同一距离表达式，不将非法值归一成最大距离；可搜索的覆盖文件以现有build/encoding校验合同为前提，测试需证明覆盖输入中非法元素不会被静默索引成另一种值。未进入向量计划的SQL仍由Spark原表达式处理。

## 11. Explain 与可观察性

普通 SQL 的 `EXPLAIN EXTENDED` 应显示索引身份、两种快照、结果合同、metric、参数最终值/来源、请求/有效候选预算、覆盖分支和 driver barrier。比如：

```text
overSample=100 [SESSION_CONF, locked]
requestedCandidates=1000, initialCandidates=1000
contract=TABLE_LIVE_TOPK, driverMaterialization=true
```

未设置则显示 `overSample=1 [COMPAT_DEFAULT, no-calibrated-policy]`。不在日志/explain 输出查询向量或完整业务行。

阶段A已落地为 `VectorSearchSource` 的计划文本：`contract`、`column`、`k`、`metric`、`overSample=<值> [<来源>]`（用户显式设置时带 `locked`）、`requestedCandidates`、`initialCandidates`、`index`、`tableSnapshot`、`indexSnapshot`、`uncoveredFiles`、`driverMaterialization=true`。

物理算子 `VectorSearchSourceExec` 以 Spark metrics 报告：`numCandidates`（首轮请求）、`numCandidateRequests`（末轮累计请求）、`numCandidateRounds`、`numLiveCandidates`、`numDeletedOrInvisibleHits`（按 `(file,pos)` 每位置只计一次）、`numHitsOutsideSnapshot`（命中指向冻结快照已没有的数据文件，例如建索引后做了 compaction；同样每位置只计一次）、`numRowsAtFiles`（去重后的回表文件数，不是轮次累计）、`numResidualFiles`、`numIndexUnderfilled`、`numOutputRows`（source 产出的行数，在 Sort/Limit 之前，不等于最终查询结果行数）。`numIndexUnderfilled` 只表示**索引分支**在最后一轮仍不足 K（残差扫描可能已补齐完整答案），不是整条查询结果不足；整条查询的输出行数由 `numOutputRows` 与结果本身给出。underfill 的具体原因（`satisfied`/`kernel_exhausted`/`entry_bound_reached`/`underfilled_retry_limit`/`budget_not_representable`/`empty_request`）写在执行日志里，因为 Spark metric 只能携带数值；规划（绑定耗时）与执行（候选、残差耗时）分开记录。

当前 parser 只接管以 CALL 开头的语句，不能承诺 `EXPLAIN CALL` 已被支持；即使补解析，外层 CallExec 也不会自动展开执行时创建的内层计划。首期 CALL 在内层执行前记录相同的计划摘要，测试直接对 bridge 的内层 QueryExecution 检查。不新增 explain procedure，不把 SQL EXPLAIN CALL 的 parser 改造混入本期。

运行 metrics 至少包括：候选数、存活/删除/不可见候选数、补候选轮次、回表文件数、残差文件数、输出数、underfill原因。规划耗时与搜索/回表耗时分别记录。

## 12. 验收与分阶段落地

用户已授权开始阶段A开发，实施计划与验收按以下要求展开；本节不展开产品补丁。

1. 参数测试：未设置与显式默认值不同；CALL参数/session/submit遮蔽；非法显式值；用户100不被降低；统计unknown与候选乘法溢出。
2. 规则测试：SQL/CALL/向量TVF 同输入得到同参数决策；固定点/幂等性；决策后SET不改变已有计划；不把显式值标为AUTO。
3. 规划纯度：SparkListener 断言分析、优化、生成物理计划和普通SQL Explain期间无job；带 fail-on-read 的数据/索引payload输入证明没读业务数据，允许元数据读取。CALL测内层规划，不能用 `spark.sql(CALL)` 返回时刻冒充尚未执行，因为 command 可 eager execution。
4. 计划形状：部分覆盖始终有两分支；全覆盖无残差；零覆盖扫描退化；FLAT不因成本guard被排除；不符合正确性资格的形状不改写。
5. SQL真实测试：append、position/equality delete、覆盖落后、rollback、branch/time-travel、过滤资格、metric、投影/ExprId、NULL域。对精确可穷举fixture与扫描比结果；近似大集同时报告召回，不能把两者混称精确等价。
6. CALL兼容测试：输出schema不变、原score方向、deleted hit的nullable row、批次query_id与单次一致；显式索引和session配置共享；索引快照/当前表快照在计划后不漂移。
7. 性能回归：固定输入/索引/缓存状态比较相同显式参数的旧新路径，分别看索引搜索、回表、残差和driver驻留量。只验证架构迁移没有无解释退化；自动倍数质量/性能验收另走校准阶段。
8. 补充边界：请求超过int但M很小可收缩、long溢出不得收缩、M未知、累计补候选单调进展/10轮终止/非前缀候选保留；规划后refresh/expire及资源删除的失败时机；scalar/batch TVF外层投影与过滤的schema/ExprId、优化无job和CALL型结果一致性。

推荐分两阶段：A完成共享规则、参数来源、物理规划和现有结果合同迁移；B在明确质量目标与校准证据后加入真正自动调参。A完成不等于“最优参数已经实现”。阶段A如获授权，采用独立worktree、先测试后产品修改、fresh SQL/CALL smoke、独立复审，最后才合并。

## 13. 阶段A的实施取舍

本草案推荐先保留 CALL 的索引命中合同、未设置倍数的兼容基线1和首期 driver barrier。这三项让架构迁移不夹带结果语义/召回/公开API重构，但意味着首期不是全自动调参，也不是完全无driver物化的执行链。

显式非法倍数报错、session覆盖submit、冻结CALL两个快照、移除FLAT成本guard，以及更严格的SQL资格检查，均是有意提出的兼容修正，不能标注为“行为完全不变”。阶段A实施与回归测试必须覆盖这些取舍；开发授权不代表实现或验收已完成。

## 13a. 阶段A已实现的兼容变化（必须向用户说明）

以下行为在阶段A实现中确实改变，不能描述成「行为完全不变」：

| 变化 | 之前 | 现在 |
| --- | --- | --- |
| 非法 over-sample | 空串/0/负数被静默改成 1 | 报错并指出配置名与值 |
| session 与 submit 同时设置 | submit 优先 | session 优先，被遮蔽的一侧记录在参数来源里 |
| FLAT 索引 | 因「搜索代价等于扫描」跳过改写 | 参与改写；成本不是能否用索引的判据 |
| SQL 资格 | 在计划里找第一个 scan | 白名单：单表、单一升序距离、可折叠正数 LIMIT、只有投影/别名/向量列 `IsNotNull` 过滤；Join/Aggregate/Window/Offset/任意过滤/多排序不改写 |
| CALL 与 TVF | 自己解析倍数、构造时可能已搜索 | 与 SQL 共用同一参数决策与绑定；TVF 只返回请求，搜索发生在物理执行 |
| CALL 读取快照 | 搜索后回表时才取 currentSnapshot | 规划时同时固定索引快照与表快照 |
| 可观察性 | 规则日志只有改写结果，执行量不可见 | EXPLAIN 显示参数值/来源/锁定、metric、请求与实际候选预算、两个快照、覆盖分支、driver barrier；执行以 Spark metrics 报告候选、轮次、活/失效命中、回表与残差文件、underfill |
| 补候选计数 | 每轮独立推导，失效命中会跨轮重复计数 | 复用优化阶段推导的预算；每轮累计请求；`(file,pos)` 每位置只计一次，失效命中不再重复计入 |
| underfill 口径 | 无 | `numIndexUnderfilled` 只报告索引分支不足 K，不冒充整条查询结果不足；残差分支补齐答案时该值仍为 1 |
| 快照外命中 | 静默丢弃 | `numHitsOutsideSnapshot` 计数（compaction 后索引条目指向已被替换的文件），答案仍由残差分支补齐 |

`CALL` 的输出合同没有变：仍是原始索引 score、nullable row、只返回索引命中，不扫未覆盖文件、不精确重排。倍数调大可能改变候选集合，因此不保证与旧 `C=K` 逐条一致。

## 14. 代码依据

以下路径均相对于仓库根，基线见文首：

- `spark/v3.5/spark-extensions/src/main/scala/org/apache/iceberg/spark/extensions/IcebergSparkSessionExtensions.scala`：当前optimizer注册；拟增加planner strategy。
- `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/RewriteVectorSearch.scala`：配置读取、FLAT guard、优化期搜索/回表/残差物化。
- 同目录 `IndexRewriteSupport.scala`、`VectorDistanceExpression.scala`、`IndexTableFunctions.scala`：快照、行转换、metric与TVF入口。
- `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/VectorSearchProcedure.java`、`VectorSearchBatchProcedure.java`：CALL输入与输出合同。
- `spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/execution/datasources/v2/CallExec.scala`：`run()`中调用procedure。
- 同extensions树 `catalyst/parser/extensions/IcebergSparkSqlExtensionsParser.scala`：CALL解析接管边界。
- `index-spark/src/main/java/org/apache/iceberg/index/spark/IndexSearch.java`、`VectorIndexSearch.java`、`DistributedIndexSearch.java`、`IndexEntries.java`：现有物理执行依赖及driver返回类型。
- `core/src/main/java/org/apache/iceberg/index/IndexReader.java`：按祖先链解析索引快照、元数据与coverage。
- `index-vector/src/main/java/org/apache/iceberg/index/vector/{VectorIndexDefinition,IvfOptions,HnswOptions,VectorDistance}.java`：定义与kernel参数的现有绑定。
- `spark/v3.5/build.gradle`、根`build.gradle`：模块依赖方向。
