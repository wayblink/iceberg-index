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

# 索引发布验收套件：用例、结果合同、基线与门禁

状态：**用户已于2026-10-27批准实施；套件尚未完成。** 今天先编译冻结最新代码的新镜像、重建10M测试输入，开发并迭代standard；100M工具入口可开发，真实执行留到后续窗口，不自动启动。对应 #2787，子卡 #2788–#2792；本次修订落实评审 #3095。设计核对基线为产品 `a682bc368`，首轮运行冻结产品 `f33843cd3`，工具来源为独立仓库 `iceberg-tools`。本文中的执行规则是实施合同；资源、时间和性能均不得冒充实测。固定厂内 Spark 3.5 / Scala 2.12，不讨论其他 Spark 主版本。

## 1. 意图与范围

把分散在 #1632、#1924、#2760、#2411、#2296 的验收要求合并为可重跑套件：一条命令生成原始证据、机器可读判定和报告；发布前检查功能、质量与驻留行为；通过且经确认的结果成为下一版基线。旧卡按用户要求关闭仅代表**范围转移**，不表示它们未完成的检查或 RED 回归已解决。

产品实现和方案文档留在 Iceberg 仓库；提交/探针/报告生成/门禁脚本在 `iceberg-tools` 仓库，不能回流产品仓 `tools/`。不新建调度框架、CI 服务或多集群编排；不借验收实现后台再平衡、跨应用持久化、自动故障修复。不修改产品默认值以令验收达标。

现有报告只能作历史参考：10M 的 `0.9833` 已确认来自余弦真值与 EUCLIDEAN 搜索错配，不能作质量天花板；100M Parquet 的 1bit/3bit 排名曾在不同运行中翻转，不能固定“谁必须更快”；旧大表磁盘体积不能等同实际驻留内存。

## 2. 两个结果合同，分别验收

| 入口 | 产品合同 | 正确断言 |
|---|---|---|
| `CALL vector_search[_batch]` 与向量 TVF | `INDEX_HITS`：索引原始命中/原始 score；已删除或已不存在行的命中可保留为 `row=null`；不扫描未覆盖文件 | 同一索引/查询/参数/冻结快照下 CALL 与对应 TVF 的行数、顺序、score、JSON/null 一致；对照索引参考实现，而不是完整表答案 |
| 普通 `ORDER BY vec_distance(...) LIMIT k` | `TABLE_LIVE_TOPK`：候选回表、删除过滤、精确打分重排、未覆盖文件残差扫描 | 以完整表相同 metric 的精确答案衡量召回；返回行必须存活、无重复、符合精确排名与约定质量目标 |

**禁止**把三入口的 `(data_file,pos,score,row)` 一概断言相等。普通 SQL 默认输出也不是 CALL 的四列 schema。量化近似 score、删除命中和残差文件都能导致合法差异。

绑定时点必须按入口核对：普通SQL在优化器改写为绑定的VectorSearchSource后已冻结；CALL在其实际plan/bind时冻结；TVF的VectorSearchHitsRequest逻辑计划只是请求，直到执行开始才绑定。因此“取TVF optimizedPlan后append，执行应该还看旧快照”是错误断言。TVF执行前的数据变化可被看到；绑定后的冻结需要先取得真实绑定context再验证，不能拿纯逻辑计划代替。

当前文本API以实际注册为准：build_text_index无name参数，提供text_index_search单查询TVF和text_search_batch过程，不提供text_index_search_batch TVF；SQL匹配函数名为`match`，不是text_match。这些缺少的可选接口记capability=false，不制造回归fail；已有分数/删除/JSON合同仍严格验。

跨合同比较只在专门的精确 fixture 上进行：完整覆盖、无删除、精确 kernel，并投影共同的行身份/值，重新计算同 metric 的距离；不把原始近似 score 当精确 score。不兼容 payload 的明确拒绝与缺少索引时的合法扫描回退也要分别断言。

## 3. 输入冻结与真值

每个 run 固定：产品 SHA、工具 SHA、镜像 digest、JVM/Spark/Scala、codec、executor 规格、缓存/插件参数，以及 table UUID、table snapshot、index UUID、index snapshot、definition hash。输入资产包含查询 ID 和向量内容 hash（按 ID 排序、规范编码）、topK、metric、seed、真值算法版本。并发 append/refresh 不得改变已规划的向量TABLE_LIVE_TOPK/INDEX_HITS查询读取的快照；用独立 fixture 验证此行为。文本入口按§6 L1.7的统计/回表合同单独检查，不把向量规划合同自动套用到文本。

大表可复用冻结 snapshot，但不得自动写、删除或重建共享索引。需要新 writer 验证时用 run 命名空间内的小表；若大表索引必须重建，需要显式资产授权或独立副本。资产变更不得静默写入旧比较序列。

真值是**当前冻结完整表**在**同一 metric**下的精确 topK。EUCLIDEAN 必须显式计算 L2，不能用默认余弦的 `vec_distance(...) + 0.0d`。参考表达式不得被改写成索引计划，需记录 executed plan 并检查；可用共享的精确算分参考工具以避免接口歧义。COSINE、DOT_PRODUCT、零向量、null、数值精度均有单独 fixture。

不默认使用 SIFT 11M 的 gt 过滤后当作 10M 精确 top10；过滤可能不足 k，且度量不一致。外部 gt 只在提供生成算法、metric、源数据 snapshot/hash、topK 和查询集映射，并逐项核对后可用。

### 3.1 度量定义

- 精确路径：结果集合与同度量真值相同，要求 recall=1.0；若 topK 边界同距，采用 tie-aware 判定：所有严格优于第 k 距离的行必须存在，其余结果距离不得超过边界，数量为 min(k,有效行数)，身份不得重复。容差固定在 spec 并有边界 fixture，不能按本轮结果调大。
- 近似路径：逐查询记 `found`、`returned_ids`、`truth_ids`、`eligible_count`、边界距离与 recall；分母为 min(k,有效行数)，空真值时 recall 为 null，单独断言无返回行，不虚构 recall=1。汇总记录 query-mean、worst-query、样本数，不能用重复同查询的轮次冒充更多查询。
- 推荐配置质量门：每个固定查询达到配置目标；初始目标采用同度量修正后的认可数据，不硬编码历史错误数字。近似算法不因“默认推荐”就对任意数据保证满召回。
- 曲线格子：低 over 本来用于展示量化损失，**不要求所有格子满召回**；与同键认可基线比较逐查询命中及质量指标。允许下降的命中数/容差按配置写入 spec，精确 fixture 容差为0。首版没有可比较数据时标明首次观测，由人工认可后建立基线，不假装完成跨版不退化验证。

## 4. 执行与资源

### 4.1 明确的两档节奏（本修订的推荐）

| 档位 | 必跑内容 | 说明 |
|---|---|---|
| `standard` 每版 | L0、L1 小数据全构建/合同检查、L2.1 10M骨架、L3.1–L3.4 10M驻留、G与报告 | 100M 不在此档必跑集合；无100M新数据就不生成本版100M取舍倍数 |
| `extended` 重大改动/大版本 | standard + L2.2 100M两格式推荐对照 + L3.5/L3.6 | payload/reader、内存模型、放置机制发生变化必须用此档；选择记录在本次 release manifest，不能运行中因排不上而降档 |
| 探索 case | L2.4/L2.5，通常的L2.6/L2.7 | `--only` 显式选用；内存/加载重大变更时L2.6加入extended必跑，HNSW变更时L2.7的HNSW查询实例加入extended必跑。最终expected集合在启动前展开写入manifest，不能运行中删掉 |

档位及触发条件按已批准设计执行；今天先standard，100M执行另安排窗口。预计数小时仅用于排队规划，不作验收SLA。`--only L0,L1` 或 `--only L1.3` 是诊断运行，不是完整发版验收。

每个 case 一个逻辑 run-id；需要冷热、插件启停或异预算时，case 内可有多个独立 Spark 应用，分别记录身份。曲线 run 可把同表多个臂按查询交错测，保持可比较性；驻留状态测试不与曲线混在同一应用。存在跨臂固定顺序和冷热影响时记录顺序并做轮次轮换；不能用“同轮”断言负载影响已抵消。

| profile | executors | heap / overhead | cache.bytes | 插件 |
|---|---|---|---|---|
| `P10` | 6 | 7g / 2g | 省略，自动推导 | L3启用IndexCachePlugin；L1/L2显式记录启停 |
| `P100-CURVE` | 4 | 16g / 4g | 显式8g/节点 | 记录启停，稳态先实测驻留覆盖与准入 |
| `P100-AUTO` | 4 | 16g / 4g | 省略，自动推导 | 驻留case启用IndexCachePlugin |

`P100-AUTO` 替代旧名 P100-NOCACHE：省略预算不等于禁用缓存。显式8g也不证明HNSW或某位宽全驻留，需按实际解压结构与各节点预算验证。异预算case在子规格中列出每节点预算，不能默认齐次 profile 就覆盖。

### 4.2 资源占用、超时与清理

首版复用平台已有预留队列/排队窗口；工具仓不引入自己的分布式锁服务。若没有排他资源保障，manifest记录 `isolation=observed_quiet` 而非 `exclusive`；性能仅作为带此标记的观测数据。

占用判断使用应用身份/资源队列/运行标签，不能只凭 `sparkann-*` 名称；区分本套件与其他应用、Pending/Running与终态。保留Completed历史pod不阻塞开跑。提交前、executor就绪后及运行中持续采样；后来出现竞争、监控断线或资源不可确认时标记证据受影响，性能比较无效，不自动进入性能基线。环境不足导致未完成必跑case则 blocked。

manifest声明排队、应用启动、case执行、状态收敛、kill后等待的超时；不无限sleep，不能超时后“继续反正测一下”。全部错误有case/run-id和原因，失败后可收集日志，但停止有依赖的case，不把没运行标成pass。

只有 `isolation=reserved` 且全程无观察到竞争/监控空洞的运行才可更新性能基线；`observed_quiet`/受竞争影响但功能用例完整通过的运行，允许人工批准发布功能结果并更新功能基线，性能字段标 `comparison_valid=false`，保留历史性能基线来源，不用本轮数字替换它。若本版变更有已批准的性能/容量收益要求，相关测量是必跑assert：资源隔离不足时该项blocked，不能以功能通过替代。以上资格分别写入机器结果，禁止用全局pass隐藏性能未验证。

并发套件使用独立输出目录、application label、表/索引名；cleanup只删本run拥有的资产，finally执行并记清单；共享大表和非本run的pod永不删除。租约/队列预留不覆盖所有工作负载时如实记录限制。

## 5. 输入与输出结构

### 5.1 当前发布清单与上一版基线职责不同

`release-manifest.json` 描述本次待发布产物和验收要求：schema_version、release_id、产品/tool SHA、expected image digest与jar哈希、平台版本、spec hash、档位、profiles、冻结资产、expected case-instance与每实例query/repeat集合、质量目标、可用性超时、输出生成时间。

L0核验的是**实际运行产物与本次manifest**。上一版image digest/build SHA不同是升级的正常现象，不能判blocked。metadata/payload读写兼容必须有实际往返fixture，不可用jar字符串搜索代替。

`results.json` 保存每个case-instance的 `pass/fail/blocked/not_requested`，证据链接/hash、原因、断言明细和metrics。`not_requested`仅允许用于manifest未要求的case；只要必跑case缺失就不能pass。

case-instance键采用规范化结构而非一个 `case_id`：至少包含case_id、dataset_id/format、logical index arm、definition hash、metric、result contract、topK、over、nprobe、profile_id、scenario。新增run的物理UUID仅作provenance；不能因每次重建UUID不同而找不到可比较键。数组/数值/省略参数统一规范化，参数缺省后的实际值也入档。

以下都是无效证据：缺少必填字段、非法数值或NaN/Infinity、重复键/重复(query,repeat)、缺query/轮次、日志截断、expected臂缺失、没有实际执行计划证明走目标路径。拒绝而非取交集凑出比较结果。适用指标按case schema声明，未定义的recall/coverage用null，不把缺值当0或1。

### 5.2 基线晋升

1. 执行前解析manifest，生成expected-case清单和各case样本集合；baseline仅包含上一版已认可的数据。
2. 验证输入和完整性，再运行各assert；功能错误fail，环境/证据不完备blocked，未请求case not_requested。
3. 每个必跑case必须pass；质量目标和逐查询比较满足；所有需人工处置的性能告警有签名/说明；cleanup与证据归档完成。否则发布判定非通过。
4. 独立保存不可变 `results.json`，只有上述条件满足且有显式promotion批准，才能原子更新该档位/比较序列的基线指针。临时文件→rename，并保留历史baseline及原始日志hash；并发更新有锁或expected-prev-hash检查，不允许最后写覆盖另一发布。
5. `--only` 子集永远不晋升完整发布基线；extended完整通过可为standard建立其完整子集基线，standard不能覆盖extended历史结果或令100M数据看似本版跑过。
6. 首版/metric变更/数据变更/spec门槛变更建立新比较序列，人工确认并说明不能跨序列计算加速/不退化；不从失真的旧0.9833日志直接初始化基线。普通版本只比较相同输入/合同/profile实例。

质量spec作为L0卡的固定输入实现，初始规则如下：精确小fixture要求全部结果正确；数值同score参考容差为 `abs(actual-expected) <= 1e-6 * max(1,abs(expected))`，非有限值按明确fixture合同检查，不进入指标JSON；topK边界tie距离使用同一固定容差。SIFT推荐配置的每个固定查询目标为recall=1.0，达不到即fail而非现场调低目标；这是发布质量要求，不是对所有数据的算法保证。曲线逐查询相对同序列基线允许损失命中数为0；已有认可基线可用于比较，没有基线时只能记录首次观测。若这些推荐门槛需放宽，必须设计/spec审批并建立新比较序列，工具实现者无权自行调整。

首版SIFT质量实例映射固定如下（topK=10、query ID为1–6，每查询2轮，EUCLIDEAN，普通SQL `TABLE_LIVE_TOPK`）：

| 数据/格式 | logical臂 | nprobe / over | 绝对质量门 |
|---|---|---|---|
| 10M Parquet与Lance | IVF1bit（推荐） | 150 / 100 | 每查询recall=1.0 |
| 10M Parquet与Lance | IVF3bit（对照） | 150 / 10 | 每查询recall=1.0 |
| 100M Parquet与Lance | IVF1bit（推荐对照） | 32 / 100 | 每查询recall=1.0 |
| 100M Parquet与Lance | IVF3bit（推荐对照） | 32 / 10 | 每查询recall=1.0 |

其余L2.1/L2.4/L2.5位宽和over实例仅受完整证据及同键逐查询零退化基线门约束，不额外套上满recall门；同一实例若命中上表，质量门和基线门都要通过。10M默认150沿用当前大表冻结定义，不据独立nprobe实验把32静默当已测推荐；定义改变建立新实例/新序列。L1精确FLAT、小表穷尽所有list的IVF-NONE及独立数值参考仍要求精确正确；不把非穷尽IVF-NONE声明为任意数据的精确搜索。

首版只有在全部绝对功能/质量要求通过、曲线证据完整且人工认可首次观测后才能建立基线；报告必须写“首次建立，未验证跨版不退化”。已知缺陷必须列在known-issues清单并有阻断与修复去向。不能把“不报错”当“值正确”。例如#2784点号字段真实值丢失，L1.3需保留值断言和明确fail；若平台批准限制支持范围，manifest列出限制并加边界拒绝测试，不能悄悄弱化一个case后称全支持通过。

### 5.3 返回码与可复现报告

拟定退出码：0=本次请求的全部case通过；1=功能/质量断言fail；2=环境/证据blocked；3=manifest/schema/用法错误。多个问题优先报输入错误，其次fail，其次blocked；机器结果保留全部原因。诊断子集即使返回0，也必须显示 `release_eligible=false`、`promotable=false`。

生成报告固定manifest生成时间、稳定排序、稳定摘要与抖动hash，禁止datetime.now()/进程随机hash影响重生；相同results/manifest/tool版本生成逐字节相同报告。另将同一原文通过umem upsert-doc存档并sha256核对；这是副本一致性，不代替重生确定性测试。报告显示每条数据的来源run、样本、metric、合同、档位、fresh/stale，不混历史与本版数字。

## 6. 用例清单

### L0：产物与输入（每版）

| id | 内容与断言 |
|---|---|
| L0.1 | 实际digest/SHA/JVM/Spark/Scala/jar哈希对当前manifest；reader可读版本及writer能力由fixture验证；工具资产为cluster/image-jars.txt与list-image-jars.sh，未知provenance blocked |
| L0.2 | 队列预留/观测空闲、运行标签、持续采样、启动就绪、超时与竞争标记；terminated旧pod不阻塞；监控不可用不假定空闲 |
| L0.3 | 冻结四张大表的实际snapshot/文件清单/index definition及check_indexes；不自动改共享索引。standard只要求所用10M资产，extended才要求100M |
| L0.4 | 规范查询向量hash/ID、metric/topK/真值算法及源snapshot；变更建立新比较序列，不静默复用旧gt |

### L1：小数据功能与兼容（每版）

| id | 内容与断言 |
|---|---|
| L1.1 | 独立小表每支持kernel新建：IVF-NONE、RaBitQ1/2/3/4bit、HNSW、FLAT，Parquet/Lance适用路径；包含BM25写读。记录built_by_SHA；各自writer与reader往返；旧payload/manifest fixture兼容不要求重建 |
| L1.2 | CALL↔对应TVF单/批查询INDEX_HITS严格一致，含raw score、顺序、null行；普通SQL TABLE_LIVE_TOPK对同metric真值独立验，精确fixture才比较跨合同共同字段 |
| L1.3 | 真实行值/JSON正确（null/嵌套/decimal/timestamp/binary、辅助列同名及特殊列名），对照原始独立fixture与正常SELECT，不用同样丢值的旧rowsAt作唯一真值；#2028 regression、#2784值断言保留 |
| L1.4 | position/equality delete与未覆盖新文件：SQL不含删除行且补残差，CALL/TVF保留missing hit为row=null；快照冻结与并发append/refresh不混读 |
| L1.5 | 空表、NULL向量、零向量、维度不符、k=0/大于有效行数、同距边界、无索引/多索引歧义、损坏/不兼容payload、失败构建注册清理；检查规定错误或合法回退，不以“不NPE”作为唯一正确性 |
| L1.6 | offload/重新load与禁用缓存前后，合同各自结果一致；无插件普通查询可用，load/offload按接口明确拒绝 |
| L1.7 | BM25：null/空文档仍计语料统计、词频/分数/单批结果与参考一致、更新/删除契约；不强行套向量的距离/驻留机制 |

L1.7的具体参考：每个文本case无外部并发写，记录建索引的source snapshot及metadata统计。null/空文本均计入文档总数，其token数为0；词频、文档频率与平均长度按该索引snapshot的语料从独立fixture算得，不以被测实现的输出反向当oracle。CALL/对应TVF返回该索引统计下的原始分数/命中顺序，删除后未refresh时统计仍为旧索引语料，最新表已删除命中回表为null（不存在文件同理），不能要求此时分数等于当前存活表重算。refresh后统计和命中按新索引snapshot重验，包含新文档及删除。普通SQL文本改写的存活行/残差与统计绑定按现有批准合同单独测试，不与CALL强制相等；若接口无法固定所需快照，则以独立静态阶段串行运行并记录限制，不凭此声称并发文本快照隔离已通过。

普通SQL文本 `text_score/match` 的独立合同：`RewriteTextSearch` 必须绑定**与读取表snapshot相同**的index snapshot及CorpusStatistics，并要求该snapshot数据文件完整覆盖；它不同于向量残差路径，不能把旧统计用于补扫未覆盖新文件。oracle用当前绑定snapshot全部存活文档计算N/df/avgdl以及固定tokenizer/BM25参数，核对排名/分数；null文本的text_score为null，text_match的对应null/filter行为单列断言。append/delete后未refresh或覆盖缺失时检查规则不使用旧索引；未绑定统计的非null text_score须按当前接口明确拒绝，不能声称普通SQL自动全扫成功。refresh到当前snapshot且覆盖完整后再核对SQL排名。CALL/TVF仍按旧索引snapshot原始命中合同独立测试，不与SQL强制等价。文本并发规划冻结超出当前接口已保证范围时显式不覆盖，不凭静态fixture声称通过。

数值与JSON比较在schema中声明规范：精确数值或固定容差、timestamp时区、null策略、binary/decimal编码。CALL和TVF必须使用相同session参数，已删除行null不等于其JSON字段为null。

### L2：参数与格式

| id | 内容 | 节奏与判定 |
|---|---|---|
| L2.1 | 10M Parquet/Lance × IVF1/2/3/4bit/NONE、HNSW、FLAT；近似over1/10/100，6查询×2轮 | standard；每个实例明确metric/真值，按逐查询基线而非全格满recall；FLAT为直接调用单点，并另验完整表exact基线 |
| L2.2 | 100M两格式各比较1bit@100与3bit@10，6查询×2轮，同run完整表exact scan参照 | extended；两臂都入表，质量目标/配对时间；不锁定Parquet赢家，未在本轮跑FLAT/HNSW就不报本轮相对倍数 |
| L2.3 | 全部raw evidence/results/hash、详细+摘要两报告、docs/umem同版 | 各发布档必跑；缺required证据不能晋升 |
| L2.4 | nprobe8/16/32/64/128 × bits1/3，over10/100，10M | 探索；曲线不把每点都设满recall门，配置改变要重建/绑定相应索引定义 |
| L2.5 | 100M over3/30补点与位宽扩展 | 探索；明确其expected集合与已测/未测范围 |
| L2.6 | P100-CURVE的冷load、确认驻留后的暖态、prepare、kernel与SQL分开量 | 内存/加载重大改动extended必跑；准入不足不得冒充完全稳态 |
| L2.7 | 100M FLAT/HNSW精确对照与性能取舍 | 探索；HNSW改动则extended加入实际HNSW查询回归，不用“曾经很慢”豁免正确性 |

L2.1与L2.2的近似臂使用普通SQL改写路径 `TABLE_LIVE_TOPK`；表是冻结静态fixture，无并发删除/追加。FLAT分成两个实例：`INDEX_HITS`直接调用（用于kernel读写/精确原始命中对照），以及不走索引的同metric完整表exact scan（用于完整表基线）。每个实例写明入口/合同，不能混合耗时或以CALL近似命中对完整表强制满召回。L2.2只报告本轮确实测过的参照。

性能无统一硬阈值：记录p50/p95/p99、query/sample、冷/热状态、分配与峰值；相对上版超1.5×或不满足性能变更卡已批准目标时，需要独立复核/处置，报告不可自动晋升覆盖。功能fail不能用性能改善抵消；同轮比值不是负载免疫证据。

### L3：驻留、故障与容量

| id | 内容与断言 | 档位 |
|---|---|---|
| L3.1 | P10自动预算：load确认副本、实际bytes/预算、coverage并集与重复副本；在预先证明能装下的fixture要求coverage≥0.95，不改变默认值凑达标 | standard |
| L3.2 | 重复load幂等及参数语义：首次省略0/显式值，重复省略保留截止/显式重计；固定保护期不因query续期；到期仅可淘汰，压力时索引级LRU；永久保护与offload解除 | standard |
| L3.3 | kill、executor ID复用、迟到ACK/旧epoch、使用中offload与安全释放：有界等待状态收敛，按合同完整行/身份/score与故障前一致；不要求完成后的load自动修复副本 | standard |
| L3.4 | 双索引共享key与异预算；重复load新snapshot复用共有、旧独有待引用释放；部分load失败保留成功；两条现RED（70次复用误降PENDING、未经新owner ACK即COMPLETE）保持断言，修前fail而非skip | standard |
| L3.5 | P100-AUTO HNSW+IVF3bit：按实际文件驻留大小/节点预算规划、合法COMPLETE或PARTIAL、缺口FILE_TOO_LARGE/容量/任务失败分清，已ACK副本才算持有；显式load规划的均衡与冷/驻留查询一致 | extended |
| L3.6 | P100-AUTO预算估计与真实准入：实测C/U/结构及workingSet、逐节点预算、coverage并集；有容量且可放置时要求≥0.95，装不下时验证PARTIAL与精确缺口，不以低coverage直接定为产品缺陷 | extended |

均衡判定先固定可放置任务集合、保护占用和文件粒度约束。可均匀分配且容量充足的fixture要求占用/预算比例不超过均值1.25倍；文件巨大、保护固定或偏斜预算不能满足时输出约束与规划对照，不用绝对<50%或>90%本身判失败。容量准入永不超预算；合法PARTIAL不能同时要求每文件有副本。coverage按目标文件的**集群确认副本并集**/目标文件总数，offload完成后归属holders为0；有query引用的pending释放与已释放区分，不能提前当空闲。

每case状态等待必须绑定目标index snapshot/epoch，释放ACK后才验byte回收。查询只观察既有驻留、不主动追加规划；二轮查询bytesIn/out稳定仅在声明无miss/无驱逐、全驻留的fixture要求，不把后台再平衡当已存在功能。

## 7. 门禁实现与反例测试

L0卡先固定manifest/results schema与expected-case，并用离线fixture验证字段和身份；G卡随后按该协议实现断言与整合。两者不得各定义一套字段。门禁位于工具仓，不以当前脚本无修改直接作发布门。

现有 `vector-index-gate.py` 解析 `arm`，harness写 `index`，多臂会折叠成同一key且median掩盖单臂下降。G必须接受规范case身份并严格校验迁移旧log字段，不能既忽略index又声称按臂比较。旧脚本“只比较交集”只适合探索，发布模式必须expected全集。

| id | 验收 |
|---|---|
| G.1 | schema版本/复合键/metric与合同/样本数/缺值规则验证；缺case、缺query、重复sample、NaN、未知schema拒绝 |
| G.2 | 逐查询质量、exact/tie与适用coverage、状态/缺口/ACK断言；不同contract不比较；单臂recall下降0.02能命名case/臂/query并失败；低over正常不被错判 |
| G.3 | promotion审批、首版/新序列建立、原子指针与并发expected-prev检查；fail/blocked/子集不能晋升；警告需处置，原始证据不可覆盖 |
| G.4 | BENCHRESULT/SMOKE适配与退出码；精确render确定性、双份sha256；临时fixture证明旧多臂漏报修复（1bit0.8→0.4、其余不变不能pass） |

还必须覆盖：新镜像digest不同于旧版但匹配当前manifest正常通过；错装镜像与当前manifest不符blocked；无baseline不假装比较；完全不共享key不是成功；故意缺必跑100M不能被standard结果覆盖；Completed历史pod不阻塞、监控失败不假定空闲；删除命中与SQL合法差异不误fail；artifact不支持文件明确报告；special-column null丢值不能被“不异常”标pass。

## 8. 旧要求承接与依赖

| 来源 | 新落点 |
|---|---|
| #1632 | L1正确metric、L2.1骨架、L2.4 nprobe、L2.6稳态；未完成点有独立结果，不沿用旧天花板 |
| #1924 | L2.2两格式两臂明细、L2.5补点、L2.3证据与报告；不混本版/历史倍数 |
| #2760 | L3.1/L3.6自动预算，省略cache.bytes的实际行为，无新增默认值 |
| #2411 | L3.2–3.4固定保护/LRU、新snapshot、partial、并发offload、kill/迟到ACK、双owner两RED与完整行比较；不能仅移卡便视为修绿 |
| #2296 | L3.5可放置与预算约束下均衡、COMPLETE/PARTIAL、逐文件ACK、实际查询；不承诺自动重平衡 |
| #2028/#2784 | L1.3 JSON/辅助字段和special-name真实值；已知读取丢值保留fail或批准的支持范围限制 |
| #2756 | L0.1真实镜像读写同版/旧版兼容；不能绕过list_indexes失败继续标整个套件通过 |

实施前完成本设计复审；开发依赖沿用L0协议/环境→L1、L2、L3→G完整断言/整合，各层先验证自己的schema和原始证据。执行时序为L0→各必跑case→G校验/门禁→归档/晋升，二者不混淆、不形成环。工具构建/测试在工具仓独立worktree，产品修复由明确修复卡负责，不混入验收套件实现。

## 9. 收口条件与限制

- 指定档位从空输出目录完整执行→证据校验→门禁→报告归档→批准晋升，至少一次闭环；只生成文件不等于发布套件可用。
- 每个必跑case可单跑复现，失败有明确原因；清理仅本run资产。
- 文档仓库与umem原文逐字节同版；原始结果留工具/证据仓，文档不代替原始日志。
- 部署基线移植到厂内1.9.1、metric/布局/模型改变，分别建立相应新比较序列并重验；未实现能力不得在报告里宣称已上线。
- standard/extended分工及初始质量spec已经在整体实施请求中获准；实际case目标仍按固定manifest检验，不能因测试失败现场修改。若没有排他队列，性能数据只可作带竞争标记的观察，不承诺测出绝对排名。
