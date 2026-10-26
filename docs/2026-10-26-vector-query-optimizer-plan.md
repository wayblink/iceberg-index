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

# #2023 Vector Query Optimizer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付阶段A：SQL、CALL、向量TVF共享参数规则，优化阶段只生成计划，搜索/回表在物理执行阶段发生。

**Architecture:** Catalyst内依次提取、绑定、校验、填缺席参数、推导预算、生成覆盖分支、降低计划。候选获取共用；SQL按活行精确重排并合并残差，CALL/TVF保留原始索引命中合同。首期保留可见的driver物化，不承诺硬内存上限或最优自动倍数。

**Tech Stack:** Java 17、Scala 2.12、Spark 3.5、Iceberg现有index-spark接口、JUnit 5、AssertJ。

**Spec:** `docs/2026-10-26-vector-query-optimizer-design.md`。用户2026-10-26授权阶段A开发；本计划待交用户核对，阶段B不在本次实施范围。

## Global Constraints

- 显式参数必须保留，包括显式设置成默认值；未设置的才由规则决定。
- 普通向量SQL与CALL共用参数决策，向量TVF也使用同一规则。
- 完整表查询固定覆盖部分走索引、未覆盖部分扫描，局部再整体合并；不加成本二选一。
- 未设置over-sample使用1，来源COMPAT_DEFAULT(no-calibrated-policy)，不称自动最优。
- CALL/TVF保留raw score、nullable row、仅索引命中；不精确重排或扫描残差。
- 不改文本、kernel/cache，不新增依赖或公开Java API；Scala内部类型用private[spark]。
- 依赖保持spark -> index-spark -> core/index-vector；extensions仅注册规则和策略。
- 显式非法倍数报错、session优先submit、CALL冻结两个快照、FLAT可改写、SQL更严格资格均须回归说明。
- 每项先正式测试和记录RED，再实现；已有保护先GREEN再mutation RED，不把编译失败/临时撤保护冒称产品缺陷。
- 开发/构建/测试只在独立worktree；只add具体路径；末尾独立复审、rebase main、ff-only合并，不碰其他任务。
- 每次测试使用独立fixture并关闭Spark/metastore；服务级smoke须使用新代码重启，无法部署则明确未完成。

## Review Focus

1. submit值被Spark复制到session：同值来源不虚报SET，不同值session胜出（Task1）。
2. 冻结后refresh、expire与资源删除：不换快照，不吞损坏；元数据失效和物理读取失败时机区分（Task2/5）。
3. NULL向量、额外过滤、Join/Offset隐藏在TopK下面：不得跨算子错误提取（Task2/3）。
4. 近似候选扩预算不保持前缀：前轮活行不得丢失，累计请求严格递增且最多10轮（Task3）。
5. TVF外层投影/过滤与batch空请求：保持ExprId/schema/query_id，空请求不搜索、不提前校验倍数（Task4）。

## Commands and Evidence

在worktree中设置 `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home`。下文Spark任务统一加 `-DsparkVersions=3.5 -DscalaVersion=2.12 --no-daemon`。

- Spark主模块测试：`./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests '*TestVectorQueryParameters' --tests '*TestVectorCandidateBudget'`。
- extensions单类：`./gradlew :iceberg-spark:iceberg-spark-extensions-3.5_2.12:test --tests '*<ClassName>'`。
- 阶段回归：运行涉及模块全test，不以单文件GREEN代替模块回归。全仓耗时/环境无法完成须记录范围，不声称全仓全绿。
- 证据持久目录：`/Users/jyxc-dz-0101035/workspace/iceberg-reviews/2023/`，每次留命令、SHA、日志、XML汇总及RED种类。
- 基线2881451ee已跑四类：RewriteVectorSearch 15、VectorIndexProcedures 44、VectorIndexSearchBatch 24、IndexTableFunctions 6，共89项0失败/错误/跳过。日志为`2023-baseline.log`。

## Task 1: 纯参数规则与候选预算

**Files:**
- Create: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorQueryParameters.scala`
- Create: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorCandidateBudget.scala`
- Test: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/index/TestVectorQueryParameters.java`
- Test: `spark/v3.5/spark/src/test/java/org/apache/iceberg/spark/index/TestVectorCandidateBudget.java`

**Interfaces:** 内部不可变`VectorParameter(value: String, origin: String, locked: Boolean, shadowedOrigin: String)`；`RawVectorParameters(overSample: VectorParameter, indexName: VectorParameter)`，缺席字段为null。`VectorQueryParameters.capture(session: java.util.Map[String,String], submit: java.util.Map[String,String], indexArgument: String): RawVectorParameters`；`decide(raw: RawVectorParameters): ResolvedVectorParameters`，后者提供`overSample: Long`、来源/锁定与索引选择。`VectorCandidateBudget.derive(k: Int, overSample: Long, entryUpperBound: java.lang.Long): VectorCandidateBudget`，返回requested:Long、initial:Int、可空上界；`next(current: Int, live: Int, k: Int, entryUpperBound: java.lang.Long): Int`。不读取SparkSession或表，不存用户Map引用。

- [ ] 写测试`explicitDefaultIsLocked`：显式"1"锁定，缺席得到1但来源COMPAT_DEFAULT；capture不提前填默认。
- [ ] 写`sessionWinsSubmit`、`copiedSubmitOriginIsAmbiguous`、`argumentIndexWinsSession`：分别验证session=100/submit=2使用100、两处同值标USER_CONF(session-or-submit)、入口index遮蔽session并记录来源。
- [ ] 参数化`invalidExplicitOverSampleFails`：""、空白、"abc"、"0"、"-1"、超过Long均报含key和值的错误；用户Map改变不影响已capture/decide结果。
- [ ] 写`entryBoundDoesNotChangeMultiplier`：K=10、倍数100、M=20得requested=1000/initial=20，参数仍100；M未知不当0，M=0得0。
- [ ] 写预算边界：int上界以上但M=20允许20；long乘法溢出即使M=20仍报错；下一累计预算不能溢出或不进展。
- [ ] 运行上述两类确认预期RED；新类型未定义的编译失败单独标记，不能称运行时失败。
- [ ] 实现capture/validate/choose/derive纯函数，规则只能填缺席项，错误不用宽catch改默认。
- [ ] 运行两类GREEN与Spark主模块全test、spotlessCheck；提交仅这四路径，含Generated-by: pi。

## Task 2: 请求识别与冻结执行输入

**Files:**
- Create: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorSearchPlan.scala`
- Create: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/BindVectorSearch.scala`
- Modify: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/IndexRewriteSupport.scala`
- Test: `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestVectorSearchPlanning.java`

**Interfaces:** `VectorResultContract`有TABLE_LIVE_TOPK/INDEX_HITS两种。`VectorSearchRequest(input: VectorRequestInput, output: Seq[Attribute]): LeafNode`；input保存tableName、可空Table、可空指定读snapshotId、column、深拷贝queries、topK、indexArgument、contract；TVF未加载表时Table为null，SQL传已分析SparkTable对应表和固定读snapshotId。`BindVectorSearch.bind(spark: SparkSession, request: VectorSearchRequest): VectorBindingResult`返回Bound(context)、NoApplicableIndex或AmbiguousIndex。`ResolvedExecutionInputs`保存SerializableTable、固定metadata location、table/index snapshot、captured IndexMetadata、manifest URI、复制index文件、scanTasks、schema、coverage、可靠M；`VectorSearchContext`还保存Task1的参数/预算、metric、原输出属性及合同。

- [ ] 写快照与上下文测试：SQL pinned snapshot/branch按祖先链选AT_OR_BEFORE；CALL最新索引/当前表分别固定；绑定后SET/refresh不改变已有上下文。
- [ ] 写元数据-only测试：捕获业务文件/payload路径的fail-on-read FileIO，绑定允许manifest但不读payload/data，不启动job。
- [ ] 写M统计测试：唯一entry-bearing描述符计数；重复/辅助文件不计；未知计数整体unknown，不能低报。
- [ ] 写资格测试：Join、Aggregate、Window、Offset、任意WHERE、多排序不进入请求；只接受可信非空或唯一直接IsNotNull域过滤，保留属性ID。
- [ ] 运行本类RED；实现单表路径识别，不再collectFirst穿透任意算子；SQL无/歧义索引保留原计划，CALL对应状态报错。
- [ ] 用SerializableTable和captured metadata冻结输入；提前解析所需祖先，复制需保留manifest条目并关闭iterable。候选执行可重读同一不可变URI，但不得refresh或重新选latest。
- [ ] 运行GREEN、原四类89项及相关模块回归，记录规划损坏报错与不存在适用索引的区别；提交列出的路径。

## Task 3: 固定规则链与SQL物理执行

**Files:**
- Modify: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/RewriteVectorSearch.scala`
- Modify: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorSearchPlan.scala`
- Create: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorSearchStrategy.scala`
- Create: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorSearchExec.scala`
- Modify: `spark/v3.5/spark-extensions/src/main/scala/org/apache/iceberg/spark/extensions/IcebergSparkSessionExtensions.scala`
- Modify: `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestRewriteVectorSearch.java`
- Test: `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestVectorSearchExecution.java`

**Interfaces:** `RewriteVectorSearch`依次组合Extract/Bind/Validate/ChooseUnset/DeriveBudget/BuildCoverage/Lower，仅生成带已决策context的节点。`VectorSearchStrategy(spark: SparkSession): SparkStrategy`只接自己的逻辑节点。VectorIndexCandidates、DeleteAwareRowsAt、ExactRescore、VectorResidualScan各有对应物理节点，`doExecute(): RDD[InternalRow]`；标准排序/投影/Union/TopK尽量用Spark节点，保留原output ExprId。`VectorSearchExec`候选driver helper消费context和累计budget，返回按(query_id,file,pos)区分的命中；只在doExecute driver调用DistributedIndexSearch与保存tasks版本rowsAt/liveRows。

- [ ] 写规划纯度RED：分析/优化/executedPlan/EXPLAIN EXTENDED无job且fail-on-read业务文件未触发；重复优化不重绑、不重读SET。
- [ ] 写形状RED：部分覆盖两分支、全覆盖仅索引、零覆盖扫描、FLAT保留索引分支；计划显示参数来源、两个快照、合同及driver-materialization。
- [ ] 写delete-aware RED：累计Top-C而非差额；删除后补足；候选不稳定时保留前轮活行；重复位置去重、不同位置相同行值不去重；至多10轮，第10轮underfill返回已有活行并标underfilled_retry_limit。
- [ ] 实现物理节点与策略，移除rule搜索/回表/takeOrdered/LocalRelation结果物化和FLAT成本guard；只在执行阶段调用旧driver API，不在构造/doPrepare/executor任务启动嵌套job。
- [ ] 显式域过滤用于两分支，非法向量沿原距离表达式错误语义；不吞权限/损坏/nanos保护错误。
- [ ] 加候选、活/删/不可见、轮次、回表/残差文件、输出等metrics；explain/log不泄露查询向量或业务行。分开记录规划与执行耗时。
- [ ] 跑GREEN，更新旧FLAT/非法倍数预期并说明兼容变化；运行Rewrite/Deletes/EqualityDeletes及extensions全test，格式通过后提交。

## Task 4: CALL与TVF接入共享计划

**Files:**
- Create: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorSearchBridge.scala`
- Modify: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/IndexTableFunctions.scala`
- Modify: `spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorSearchExec.scala`
- Modify: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/VectorSearchProcedure.java`
- Modify: `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/VectorSearchBatchProcedure.java`
- Modify: `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestVectorIndexProcedures.java`
- Modify: `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestVectorIndexSearchBatch.java`
- Modify: `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestIndexTableFunctions.java`

**Interfaces:** Spark内部`VectorSearchBridge.plan(spark: SparkSession, table: Table, column: String, indexArgument: String, queries: java.util.List[Array[Float]], k: Int): QueryExecution`只生成INDEX_HITS请求；procedure.call实际执行内层计划再转现有InternalRow[]。TVF builder直接返回Task2的请求，不调用执行bridge。ExistingResultFormat/LegacyRowsAt物理完成步骤保持SINGLE_RESULT_SCHEMA四列与RESULT_SCHEMA五列，query_id逐query独立；Task1规则统一解析raw conf。不改index-spark向量/文本公开搜索API。

- [ ] 写相同参数来源的SQL/CALL/TVF决策对比RED，显式index入口优先session，批次每query自己的K/M预算。
- [ ] 写CALL内层plan的纯度测试，不能用spark.sql(CALL)已返回来冒充尚未执行；内层优化后再append/delete/refresh，候选及回表快照不漂移。
- [ ] 写raw score metric方向、deleted hit的nullable row、scalar/batch schema与单次/批次一致性；不加残差、不精确重排、不补删除。
- [ ] 写TVF构造/优化/物理规划无job，外层Project/Filter/Join保留ExprId/schema；非正K/空批次返回空且不搜索，保持现有空合同。
- [ ] 实现bridge和两procedure接入，修改仅向量TVF builder；批次合positions统一回表，不退化逐query CALL；执行前记录不含向量的计划摘要。
- [ ] 跑上述三类、Task1/2/3新测及extensions/index-spark模块回归，格式通过后提交；不承诺EXPLAIN CALL parser支持。

## Task 5: 完整回归、真实smoke与独立复审

**Files:**
- Modify: `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestVectorSearchExecution.java`
- Modify: `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestVectorSearchPlanning.java`
- Modify: `docs/2026-10-26-vector-query-optimizer-design.md`（实施与验收状态）
- Evidence: `/Users/jyxc-dz-0101035/workspace/iceberg-reviews/2023/RESULT.md`

**Interfaces:** 消费Task1–4的同一context/物理计划；不新增产品接口。

- [ ] 加rollback、branch/time travel、append落后、position/equality delete、NULL域、cosine/dot/euclidean、投影/ExprId等组合回归；精确小fixture与扫描比结果，近似结果不称严格等价。
- [ ] 加规划后catalog expire但资源仍在可执行、真正删除payload/data/delete资源首次物理读失败的测试，禁止静默换快照。
- [ ] 跑Spark主模块、extensions、index-spark、index-vector全test；先fresh局部回归后尝试全仓测试，所有失败/跳过/超时按名记录。
- [ ] fresh JVM运行SQL/CALL/TVF真实fixture smoke，清理表/索引/临时目录并停止会话；对应服务若需外部部署先确定实例与镜像SHA，重启后测，不以旧集群代替新代码。
- [ ] 固定相同数据/索引/显式参数/缓存状态，比较旧新搜索、回表、残差耗时与driver量；无数据则只报告功能验收，不声称性能通过。
- [ ] 独立reviewer在独立worktree核对全分支需求与实现、运行重点探针；有实质问题修复再复核，限流/turn limit不能算通过。
- [ ] 全spotlessCheck、git diff --check、新鲜证据汇总；rebase当前main再重新跑关键回归，ff-only合并并移除工作树；#2023转review，不自行宣告规模/自动调优验收done。

## Self-review / Current Status

任务共享接口已核对：Task1的参数/预算由Task2/3/4消费；Task2统一request/context由Task3/4消费；Task3的strategy/exec由Task4补INDEX_HITS完成步骤；Task5只验收，不另造接口。SQL资格、快照失败时机、NULL、非前缀候选、TVF/空请求均对应Review Focus的测试项。

目前只有隔离环境、89项基线和实施计划；尚未新增产品实现或测试代码。按计划先做Task1，再依次集成，最终独立复审。不将阶段A框架交付称为阶段B自动最优倍数。
