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

# SQL 向 Lance-Spark 对齐：可行性、工作量与收益

**结论：值得分层对齐，不建议原样复制。优先交付语义可审查、返回 typed 业务行的向量检索；索引管理采用相近的 DDL 骨架，旧 CALL、hit TVF 和 batch 保持兼容。**只改名称收益有限，完整复制 Lance 的过滤、全文与融合能力又不是语法工程。

本文是 #2820 的研究建议，不是实施批准。没有修改产品、加入依赖、运行 SQL、构建、测试或集群验证；不定义新的参数名、默认值或完整未来签名。工作量是源码走查后的估算，不能作为已验证交付日期。

## 1. 约束与可行性

研究冻结我方 `3fb029dce393fb42e30cacaba1d847afce88a4df`、Lance-Spark `ed2f49f94dfdb4ad81a2153cb658443864cd4d54`。部署约束沿用已确认的厂内 Spark 3.5.7、Scala 2.12、JDK 17，Iceberg `step-1.9@d2e4ba7e4` 基于 1.9.1；不修改 Spark、不新增第三方依赖，不改旧 metadata 或配置默认。Lance 文件格式不属于当前 1.9.1 回溯交付。

**DDL 外观在平台上可行。**我方已经有 grammar → AST → analyzer → strategy → command exec，Lance 也用自定义 logical command 实现索引动作，不要求依赖 Spark 原生索引管理接口。可以借鉴结构化命令与标识符解析，不直接复制同名 `org.apache.spark` 类；Spark 4.1 的参数化 parser 接口不能搬回 3.5。[1][2]

检索侧已有 `TABLE_LIVE_TOPK`、绑定快照、活行回表、补候选、未覆盖残差和精排部件，因此 typed 向量关系不是从零写引擎。但新 TVF 必须分析真实表 schema、稳定 exprId 和读快照，不能沿用旧 hit 请求在执行期重新加载表，再把输出改个 schema 就算完成。[3][4]

## 2. 对齐范围：三层不能混为一谈

| 层次 | 可以借鉴什么 | 不能随包装自动获得什么 |
|---|---|---|
| 语法外观 | 表＋动作＋索引名；函数命名参数归一化 | 新索引类型、typed 行、过滤前 Top-K |
| 分析与结果合同 | 明确 metric/索引来源、类型与未知参数报错、业务列、EXPLAIN | 无条件足额或精确 ANN、无 driver 中转 |
| 执行能力 | 过滤资格、全文绑定、融合与按需加载的设计思路 | 现有 token-OR 自动变成 phrase/fuzzy/多列；旧 hit 自动变成全表查询 |

Lance 本版没有 ANN CREATE，不能照抄不存在的向量建索引语法。我们已有维度、结构与量化分离的构建定义，其方法映射必须另审。全文过程不接受名称，旧索引也可能无名；跨列允许相同向量索引名，只有“表＋名称”的 DDL 因而可能歧义。元数据已有可选名称字段，但不能据此静默补名或改写旧文件；新建和旧索引寻址规则仍需审议。[5][6][10][11]

管理结果不能换名冒充：我方 files/entries 与 Lance fragments/segments/覆盖率不同。现有 vector DROP 默认 purge，Lance DROP 留文件给清理，两者不能默认为同义。旧默认保持；新管理入口的删除合同需批准。SHOW、check、build status 和 cache 驻留报告各自服务不同任务，不应强行统一成一张状态表。[7][12]

## 3. 推荐范围与兼容边界

推荐先做**单查询 typed 向量检索＋分析校验＋组合兼容**。先明确普通 SQL 的距离与评分合同，并获批必要的兼容迁移，再实施对齐；不能把本研究中的修复预算视为改变旧行为的授权。不把完整 FTS/HYBRID、业务预过滤和所有标量类型塞进第一批。随后按管理需求补 DDL。若只交付 SHOW 和已有命名向量的 REFRESH/DROP，应明确这是小范围入口，不宣传完整 CREATE 对齐。

```text
旧 CALL / hit TVF / batch：保持原参数、四/五列、JSON 与 ordinal 合同
新检索入口：分析表/schema/快照 → 复用活行候选/残差/精排 → typed 行
管理 DDL：结构化解析 → 名称/类型校验 → 复用 backend → 独立结果适配
```

仅包旧 hit backend 仍返回地址、raw score、nullable JSON，不扫新增文件。把外 WHERE 当过滤前 ANN 会答错：全表先取十条后筛租户，可能只剩一条。第一期不具备的资格过滤必须明确拒绝或明确结果域，不能因写法类似 Lance 就宣称等价。

**距离语义是前置决策。**当前 `vec_distance` 默认 COSINE，rewrite 却按选中索引重绑 metric。同一句 SQL 的值可能因索引改变。须决定数学合同与历史迁移策略，并验证无索引、兼容/不兼容索引及 alias；不能未经批准直接改变已有行为。文本投影、绑定和缺失 fallback 也不能用改名隐藏；若其他卡已修复，只计算本任务集成差额。[3][8]

实施前还须明确无可用索引、空表、历史快照和源文件已清理时的行为及结果 schema。当前估算以已有可用索引的向量入口为基线；不能默认继承旧 hit 的空结果，也不能未经设计就承诺无索引自动精确扫描。若要求新增全扫描回退、更多历史 schema 情况或其他结果保证，应核实复用路径并重估增量。上述情况都要进入验收矩阵。

新关系应保护必要向量列以便精排，处理历史 schema、重命名、分数列碰撞、SELECT*、COUNT/JOIN 和物理序列化。COUNT 只是返回集合的计数，JOIN 不是逐行关联检索。EXPLAIN 不启动 job，并展示快照、metric、预算来源、覆盖与 driver 边界。不能照抄 Lance 部分未知键静默忽略、重复键覆盖的处理。[4][9]

## 4. 工作量估算

按一名熟悉本仓库及 Catalyst 的工程师估净人日，包含针对性 tests、文档、review/返工；不含新内核、生产性能实验、集群发布、等待及排期。**置信度为早期粗估，不是报价。**走查的是主线实现，尚未在实际厂内 1.9.1 发布闭包上编译验证；历史 schema、名称兼容和旧 SQL 行为是主要上浮风险。正式排期前应固定实施分支、验收矩阵及接口决策，重新估算。

| 工作包 | 净人日 | 可审查交付 |
|---|---:|---|
| DDL grammar、路由、AST | 3–5 | 保留普通 SQL 委托、关键词/标识符兼容 |
| SHOW＋已命名向量 REFRESH/DROP | 3–5 | 名称歧义、backend、结果和删除合同 |
| 小范围 DDL 兼容闭环 | 2–3 | 旧 CALL/TVF、引号、catalog、错误回归 |
| CREATE 映射与适配 | 4–7 | 既有向量/文本输入与结果，不添加算法 |
| vector/text 通用管理 | 7–12 | 文本命名、旧无名索引、类型与删除分派 |
| 现有管理入口收口 | 1–3 | 清单/诊断/驻留边界，不能伪造统计 |
| 检索正确性基线 | 4–7 | metric、文本绑定/投影/alias、明确拒绝 |
| 检索参数与分析合同 | 3–5 | arity、类型、未知/重复键、维度字段 |
| 单查询 typed 向量关系 | 10–17 | 快照/schema、属性、回表/残差/距离、序列化 |
| 检索组合与旧接口兼容 | 3–5 | SELECT*、碰撞、COUNT/JOIN、旧 batch 合同 |

**包价与重叠：**小范围管理入口前三项为 **8–13 人日**；完整现有 vector/text 管理前六项为 **20–35 人日，已经包含前者，不能再加 8–13**。推荐检索范围后四项为 **20–34 人日**。只加 alias 在公共校验就绪后增量 **2–4**；后续单列 typed 文本与延期执行增量 **8–14**，不含高级 FTS。

这里的“完整管理”仅指现有向量/文本的创建、查看、刷新和删除入口覆盖，以及名称和结果适配；不包含重做并发发布事务、后台自动维护或全部故障恢复窗口。

若同时要求完整管理和推荐检索，两条独立估算未扣共享工作前为 **40–69 人日**，不是确定总价。共同兼容测试、结果模型、文档和已交付修复应在设计后去重；当前不凭空给扣减量。高级 FTS、filter-aware ANN、HYBRID 的底层范围尚未评估，不能用这个总数覆盖。

## 5. 收益与代价

最直接收益是少写 JSON 解码和业务 schema、少依赖会话 conf、按索引名称管理，以及把拼错参数从“看似成功但无效”变成分析期错误。用户找一百个近邻后关联业务表时，typed 行比先解析 JSON 更自然；但 schema/快照和分数必须可信，外观简洁不是理由。

最大收益是降低错题风险：metric 不应因索引加速悄悄变义，外 WHERE 不应被误认为检索资格，索引 hit 不应被当完整活行结果。新增入口也有代价：新旧共存、兼容维护、历史 schema 和不同结果域的解释都需要投入。

**没有直接性能承诺。**typed 输出可能省掉部分 JSON 操作，现有回表、残差和 driver 物化仍可能存在。DDL 一般不减少索引训练成本；高级过滤更不是换一个函数名就有。建议分别记录常用任务是否减少手工步骤、参数错误是否及时暴露、同 SQL 值与排名是否稳定、EXPLAIN 是否清楚，再另做同质量性能验证。

## 6. 推进与退出条件

先确认结果域、metric 迁移、名称歧义、删除语义和第一期不支持的能力，再定义测试；不要先搭一个 CREATE 骨架后才决定含义。实现时优先修正确性和 typed 检索，再按收益安排管理 DDL；这不是开发任务已批准。

若厂内只能升级 Spark 才能实现、必须改旧 metadata/默认值、无法保持旧 CALL/batch，或新关系仍依赖分析时与执行时不同 schema，应收窄或停止该范围。若预期只是省过程名，则不要投入完整 typed 引擎的预算；若要全部 Lance 检索功能，则另立底层能力项目，不能继续称“小语法对齐”。

相关资料：[现有 SQL 对比](2026-10-28-lance-spark-sql-syntax-comparison.md)、[内核复用可行性](2026-10-28-lance-index-kernel-reuse.md)、[既有回溯计划](../superpowers/plans/2026-10-27-index-backport-to-191.md)。

## 固定源码证据

[1]: ../../spark/v3.5/spark-extensions/src/main/antlr/org.apache.spark.sql.catalyst.parser.extensions/IcebergSqlExtensions.g4#L68-L132
[2]: ../../spark/v3.5/spark-extensions/src/main/scala/org/apache/spark/sql/execution/datasources/v2/ExtendedDataSourceV2Strategy.scala#L64-L85
[3]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/RewriteVectorSearch.scala#L123-L160
[4]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/BindVectorSearch.scala#L39-L137
[5]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/AddIndexExec.scala#L582-L646
[6]: ../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/BuildTextIndexProcedure.java#L47-L72
[7]: ../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/DropVectorIndexProcedure.java#L51-L114
[8]: ../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/RewriteTextSearch.scala#L95-L164
[9]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/lance/spark/search/LanceSearchTableFunctions.scala#L392-L419

[10]: ../../core/src/main/java/org/apache/iceberg/index/IndexMetadataParser.java#L80-L140
[11]: ../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/BuildVectorIndexProcedure.java#L287-L313
[12]: https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/DropIndexExec.scala#L25-L45

正文证据编号对应文件与行范围，不表示本次运行测试通过。估算由两项只读 Plan 研究提供，主会话核 parser、strategy、metric 绑定与已有快照路径；完整实现和厂内部署尚未验证。
