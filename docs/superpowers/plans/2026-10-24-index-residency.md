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

# 索引驻留管理 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现按 index 管理的手动加载/卸载、固定保护期、LRU 与可靠跨 executor 均匀放置。

**Architecture:** 构建端逐文件写容量估计；driver 管政策与规划；executor 插件异步执行带 epoch 的定向命令，本地缓存做准入和安全释放。查询消费确认副本，不自行破坏规划。

**Tech Stack:** Java 17、Scala、SparkPlugin 公共 API、现有 Iceberg FileIO/Puffin、JUnit 5 + AssertJ，不新增第三方依赖。

**Spec:** `docs/superpowers/specs/2026-10-24-index-residency-design.md`（2026-10-24 用户已批准）。

## Global Constraints

- 管理单位 index，放置单位 index file；内部 table UUID + index UUID，不能以列名替代身份。
- 命令 `load_index` / `offload_index`；首次省略保护为0，显式按值；重复省略保留策略/截止点，显式替换并重新计时；访问不续期。
- 到期只允许淘汰，永久不参与容量驱逐；offload可以解除永久驻留；部分load保留确认成功文件。
- 重复load新快照替换旧目标，共有文件复用，旧独有文件等使用引用结束释放；不累积历史，不回退旧目标算完成。
- 同应用内管理；不自动预热、不后台重平衡、不跨应用持久化、不自动修复已结束load的节点故障。
- 每文件一个规划副本，按总占用/预算比例均衡；迁移先建后删，释放ACK后才计为空闲。
- 配置插件是应用启动步骤；未配置不影响旧查询，但load/offload必须明确拒绝。
- 每卡独立worktree；具体路径git add；审查后rebase main、主树ff-only合并并清理worktree；不碰其他Agent改动。
- 各卡TDD、独立审查；完成产品代码后部署对应新产物、重启应用并真实smoke；没有集群证据不关闭验收卡。
- 新接口方法default，core无Spark依赖；不加Jackson注解、不提升已有内部类可见性、不修改受保护文件或版本目录。

## Review Focus

1. 同列同名/未命名索引身份歧义：拒绝不明确选择，不把A的访问/卸载算到B（Task6）。
2. 旧manifest、未知模型或CPU/JVM profile差异：不按0分配，不在driver全量prepare（Tasks1、2、5）。
3. offload/超时与晚完成load或executor ID复用：旧命令不能回填或删除新副本（Tasks3、4、5）。
4. 有限保护在load执行中到期且节点仍有搜索引用：不自我淘汰、不虚构已释放空间（Tasks3、4）。
5. 新快照部分失败/扩容/节点退出：不漏搜、不以历史副本算覆盖、不后台偷偷恢复（Tasks4、6、7）。

---

## 文件结构、平台与依赖

现有索引procedure与测试在 `spark/v3.5/`；`iceberg-index-spark` 当前 compileOnly Spark 3.5，默认Scala为2.12。主仓默认Spark版本为4.1，但4.1的procedure使用不同UnboundProcedure接口，且尚未接入现有索引procedure。**本计划沿用现有索引集成的Spark 3.5 / Scala 2.12**，所有Gradle命令显式 `-DsparkVersions=3.5 -DscalaVersion=2.12`；不把本需求扩大成全部索引入口的4.1迁移。最终smoke必须先记录镜像实际Spark/Scala版本，不匹配则阻塞部署，不能把3.5产物塞入4.1镜像。

新增公共跨模块类型仅 core `IndexFileMemory`、`IndexBuildResult` 和 index-spark `IndexCachePlugin`/`IndexCacheControl`；planner、协调器、protocol、worker、所有权管理默认package-private。所有新文件Apache license header。

任务顺序：A(#2405)和C(#2407)是独立起点；B(#2406)依赖A；D(#2408)依赖A+C；E(#2409)依赖B+C+D；F(#2410)依赖E；G(#2411)依赖F。所有实施卡先等待计划卡#2395获批完成。伞卡#2391支持原需求#2296/#1571，原卡只在整合验收后收口。

## Task 1 / #2405：容量元数据与manifest兼容

**Files:** 新建 `core/src/main/java/org/apache/iceberg/index/IndexFileMemory.java`；修改同目录 `IndexFile.java`、`IndexManifest.java`；测试 `core/src/test/java/org/apache/iceberg/index/TestIndexFileMemory.java`、现有 `TestIndexManifestFiles.java`。

**Interfaces:** Consumes：现有IndexFile/manifest；Produces：`IndexFileMemory.builder()`；getter `Long fileBytes()`（构建阶段可缺，写入manifest前必须填实际容器长度）、`long payloadBytes()`、`long preparedBytes()`、`int modelVersion()`、`String readerId()`、`String runtimeProfile()`、`Map<String,Long> structure()`；`toBuilder()`。`IndexFile.memory()` nullable，`IndexFile.Builder.memory(IndexFileMemory)`；blob-buffer驻留估计用checked payload+prepared求和；当前Spark持有整个Puffin容器时，由Task2转换成checked fileBytes+prepared，不同时加容器和blob。

- [ ] **RED:** `memoryRoundTrip`断言各字段往返相等；`legacyManifestHasNoMemory`使用手工v1 fixture，断言memory为null且原字段不变；`invalidSizesAndUnknownManifestVersion`拒绝负值、溢出及format=99；模型未知版本可读但不可直接准入。

```java
assertThat(IndexManifest.read(legacyV1).indexFiles().get(0).memory()).isNull();
assertThat(readBack.memory().payloadBytes()).isEqualTo(1024L);
assertThat(readBack.memory().preparedBytes()).isEqualTo(4096L);
assertThatThrownBy(() -> IndexFileMemory.builder().payloadBytes(-1L).build())
    .isInstanceOf(IllegalArgumentException.class);
```
- [ ] **Run RED:** `./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 :iceberg-core:test --tests '*TestIndexFileMemory' --tests '*TestIndexManifestFiles'`，必须看到新增行为失败而非环境/编译基础设施错误。
- [ ] **Implement:** 保留现有MAGIC；writer写format2，reader显式分支1/2；每条旧字段后v2写presence和容量字段。structure按稳定键序列化，验证条目数和值；已有IndexFile builder不要求传memory。
- [ ] **GREEN:** 同命令全通过；补截断/重复结构键fixture，不改变coverage语义。
- [ ] **Commit/review:** 只add上述路径，`Index: Persist versioned file memory estimates`，带`Generated-by: pi`；独立审核格式向后读取与新reader拒绝异常路径。

## Task 2 / #2406：构建估计与传播

**Files:** 新建 `core/src/main/java/org/apache/iceberg/index/IndexBuildResult.java`；修改同目录 `IndexAdapter.java`、`IndexSnapshotWriter.java`；修改 `index-vector/src/main/java/org/apache/iceberg/index/vector/{VectorKernel,VectorIndexAdapter,BruteforceKernel,HnswlibKernel,IvfKernel,PreparedStructureSize}.java`；必要时新建同目录 `VectorMemoryEstimates.java`；修改 `index-spark/src/main/java/org/apache/iceberg/index/spark/SparkIndexBuilder.java`；新建同目录内部 `IndexCacheMemory.java`，修改 `ExecutorIndexCache.java` 与 `DistributedIndexSearch.java` 统一实际容器记账/加载路径；新建同包测试 `TestIndexCacheMemory.java`。

**Interfaces:** Consumes：Task1的IndexFileMemory；Produces：`IndexBuildResult.of(ByteBuffer payload, IndexFileMemory memory)`（memory允许null），`payload()`、`memory()`；`IndexAdapter`新增default `encodeEntriesWithMemory(List<IndexEntry>)`和`encodeAuxiliaryPayloadWithMemory(List<IndexEntry>,int)`，旧方法保持；`VectorKernel`新增default `buildWithMemory(List<IndexEntry>,int)`。各default仅委托旧编码并返回null估计。VectorIndexAdapter转发kernel结果，writer补实际fileBytes。`payloadBytes`保持encoded blob window长度；内部`IndexCacheMemory.residentBytes(IndexFileMemory)`在调用者已匹配reader/model/profile且fileBytes已知后，将其转换为当前整个容器缓存的checked fileBytes+prepared计费，与`ExecutorIndexCache.loaded`复用求和及原prepared未知回退；不得重复计入blob。未知fileBytes不能按0使用。

- [ ] **RED:** 新 `index-vector/src/test/java/org/apache/iceberg/index/vector/TestVectorBuildMemory.java`：各kernel产物prepare后的`estimatedBytes`与同profile构建prepared估计一致；assert payload.remaining与payloadBytes相等；Flat共享路径、IVF量化bits/分区、HNSW M/maxCount/CPU profile分别改变正确项；旧adapter default不要求实现新增方法。

```java
IndexBuildResult built = kernel.buildWithMemory(entries, 1);
assertThat(built.memory().payloadBytes()).isEqualTo(built.payload().remaining());
assertThat(built.memory().preparedBytes())
    .isEqualTo(kernel.prepare(built.payload()).estimatedBytes());
```
- [ ] **Run RED:** `./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 :iceberg-index-vector:test --tests '*TestVectorBuildMemory' :iceberg-core:test --tests '*TestIndexSnapshotWriterAttempts'`。
- [ ] **Implement:** 在已有build结构可用时生成模型version1、reader/kernel格式标识、profile（引用宽度/8字节对齐/compact strings/CPU数）及模型所需结构计数；build和prepare复用估计模型，不把输入对象或构建临时缓冲计为loaded结构。旧`build`仍只返回payload，避免新旧委托递归。
- [ ] **Propagation tests:** `IndexSnapshotWriter`和SparkIndexBuilder executor输出将memory结构完整传回driver；扩大Row schema但不得丢旧五字段。用新 `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestIndexBuildMemory.java`证明SQL构建后的manifest实际有memory；构建不得调用prepare来“测量”。
- [ ] **Container contract tests:** 四种向量索引 × NONE/ZSTD，真实写入并读取manifest/Puffin，转换后的规划值须等于真实`CachedFile.bytes()`、缓存stats及现有working-set计费；未知容器长度与溢出必须拒绝。RaBitQ增加dimension17、bits1/8的独立4/32-byte code及每条24-byte prepared差值断言，以共同模型忽略bits的受控mutation验证测试能失败；此mutation不是原实现缺陷。
- [ ] **GREEN:** 上述测试及 `:iceberg-spark:iceberg-spark-extensions-3.5_2.12:test --tests '*TestIndexBuildMemory'`全通过。
- [ ] **Commit/review:** 精确add修改文件，`Index: Estimate retained index memory while building`；审查不能用序列化长度倍数代替结构模型、不能额外完整prepare。

## Task 3 / #2407：保护策略与本地缓存所有权

**Files:** 新建 `index-spark/src/main/java/org/apache/iceberg/index/spark/IndexResidencyPolicy.java`、同目录 `IndexCacheOwnership.java`；修改同目录 `ExecutorIndexCache.java`；新测试 `index-spark/src/test/java/org/apache/iceberg/index/spark/TestIndexResidencyPolicy.java`、同目录 `TestIndexCacheOwnership.java`，扩展同目录 `TestExecutorIndexCacheReservations.java`。

**Interfaces:** Consumes：现有Config/CachedFile及B3预算记账；Produces：`IndexResidencyPolicy.parse(String)`：null表示参数省略；值接受0/正数s,m,h,d/forever，使用checked转换纳秒。policy提供`forever()`、`durationNanos()`；deadline在driver赋值。`IndexCacheOwnership.applyTarget(String indexUuid,long epoch,Set<String> cacheKeys)`、`offload(String,long)`、`mayAdmit(String,long,String)`、`release(String,long)`（解除所有权，不提前计空闲）。缓存新增package-private `acquireManaged(String indexUuid,long epoch,String cacheKey) -> ExecutorIndexCache.Lease`（miss返回null）、`installManaged(String indexUuid,long epoch,String cacheKey,Config,Loader) -> CachedFile`与`releaseIndex(String indexUuid,long epoch) -> void`；嵌套Lease implements AutoCloseable，提供`file()`与幂等`close()`。在查找缓存时原子获取使用引用，调用者try-with-resources包围整个searching，不能先返回裸CachedFile再补pin；不公开缓存实现。额外提供package-private `recordIndexAttribution(String indexUuid,Set<String> cacheKeys)`与`indexAttribution() -> Map<String,Set<String>>`：只登记当时驻留文件的可选table-qualified索引归属，返回深不可变快照；不授予managed ownership、不改变table公平份额、访问时间或驱逐，文件实际移除时清理归属。

- [ ] **RED:** parse(null)为null，parse("0")非null，30m=1800s，forever；拒绝-1/1x/溢出。`oldLoadCannotPublishAfterOffload`用latch停loader，offload后放行仍不准入；`sharedFileKeepsOtherIndexOwner`卸A不清B；`releaseWaitsForSearch`仅使用结束后空闲增加；`leaseBeforeSearchingBlocksRelease`在查找取得Lease后、实际searching前暂停，offload仍不得提前计空闲。

```java
assertThat(IndexResidencyPolicy.parse(null)).isNull();
assertThat(IndexResidencyPolicy.parse("0").durationNanos()).isZero();
assertThat(IndexResidencyPolicy.parse("30m").durationNanos())
    .isEqualTo(TimeUnit.MINUTES.toNanos(30));
assertThat(IndexResidencyPolicy.parse("forever").forever()).isTrue();
ownership.offload("index-A", 2L);
assertThat(ownership.mayAdmit("index-A", 1L, "file-A")).isFalse();
```
- [ ] **Run RED:** `./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 :iceberg-index-spark:test --tests '*TestIndexResidencyPolicy' --tests '*TestIndexCacheOwnership' --tests '*TestExecutorIndexCacheReservations'`。
- [ ] **Implement:** 原子同步epoch检查/准入/所有权转移；保持B3精确预留释放，固定锁顺序；高水位tombstone防旧加载回填。已管理文件不受原idle sweep、公平份额file驱逐或查询自主插入破坏保护/规划；未管理项维持原行为但有可上报索引归属。索引policy不是本地file TTL。托管复用也检查本次max-entry与预算（含其他未释放预留），拒绝返回null且不释放已有Lease或其他owner。legacy的两次命中查找都须与managed检查同锁；首次收编前已经发出的裸引用仍需调用者完成迁移/等待旧查询结束，不能据此宣称原API全生命周期安全。
- [ ] **GREEN:** 上述并发确定性测试全通过；关闭缓存、旧file(loader)契约和现有预算测试不回归。尚未接入调用者时，只提供经测试内部能力，不提前改变所有查询。
- [ ] **Commit/review:** 精确add，`Index: Fence managed cache ownership and safe release`；独立审核任何释放不能破坏正在使用或其他所有者引用。

## Task 4 / #2408：协议DTO、driver规划与LRU

**Files:** 新建 `index-spark/src/main/java/org/apache/iceberg/index/spark/IndexCacheProtocol.java`、同目录 `IndexPlacementPlanner.java`、`IndexResidencyCoordinator.java`；测试 `index-spark/src/test/java/org/apache/iceberg/index/spark/TestIndexPlacementPlanner.java`、同目录 `TestIndexResidencyCoordinator.java`。

**Interfaces:** Consumes：Task1容量信息经Task2转换后的容器计费与Task3保护策略；Produces：Protocol拥有Serializable嵌套DTO `NodeReport`（session/id/incarnation/sequence/budget/used/reserved/releasing/profile/holders/accessAge）、`Target`（indexUuid/snapshotId/files/definition/FileIO读取上下文）、`Command`（session/incarnation/indexUuid/epoch/planId/commandId/type/file/estimate）、`Reply`（ACK/commands）、`Result`（状态/覆盖/缺口/节点/驱逐迁移）。全部builder创建，collection defensive copy。planner `plan(Target,List<NodeReport>) -> Plan`。coordinator `acceptLoad(Target,IndexResidencyPolicy,long nowNanos) -> String planId`、`acceptOffload(String indexUuid,long nowNanos) -> String planId`、`report(NodeReport) -> Reply`、`ack(Ack) -> void`、`result(String planId) -> Result`。Ack、Plan及枚举亦由本任务定义，不由后续plugin重定义。Plan提供`Map<String,String> placements()`（file→executor）、`Map<String,String> rejected()`（file→reason）；Ack提供`commandId()`、`epoch()`、`status()`、`bytes()`与reason，状态枚举包含LOADED/RELEASED/REJECTED/FAILED。

- [ ] **RED planner:** 等预算节点6/2/2/0已有占用，新2GiB先放空节点；异预算按比例；所有文件只一个目标；总量够但单file过大报FILE_TOO_LARGE；释放未ACK仍不可用；有双份空间才迁移，优先大file且稳定tie-break。

```java
assertThat(plan.placements()).containsEntry("new-2g", "executor-4");
assertThat(oversizedPlan.rejected()).containsEntry("oversized", "FILE_TOO_LARGE");
assertThat(plan.placements()).hasSize(target.files().size());
```
- [ ] **RED coordinator:** fake monotonic clock验证首次/重复参数和原截止点；保护期内/永久不淘汰，过期无新load不主动卸；访问年龄而非报表到达更新LRU；执行租约避免加载中自淘汰。新snapshot A+B→B+C复用B、撤销A，partial只统计B/C；旧epoch/旧incarnation/重复ACK不能改变覆盖；timeout保留成功并fence其余。
- [ ] **Run RED:** `./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 :iceberg-index-spark:test --tests '*TestIndexPlacementPlanner' --tests '*TestIndexResidencyCoordinator'`。
- [ ] **Implement:** 纯Java状态机和确定性大文件装箱；全节点预算比例且checked算术；索引LRU只选择有助目标容量的victim，按整个index解除所有权；计划事务串行，但RPC report/ACK与offload可推进。ACK结果才进holders，报表过时不乐观分配。目标替换与物理释放分开，不将policy epoch/plan取消混为删除成功副本。
- [ ] **GREEN:** 测试全通过，增加load排队与offload抢占测试，所有Result终态可对账。
- [ ] **Commit/review:** 精确add，`Index: Plan confirmed residency and index-level eviction`；协议DTO在此卡定义，plugin依赖此卡，禁止循环依赖。

## Task 5 / #2409：SparkPlugin与执行worker

**Files:** 新建 `index-spark/src/main/java/org/apache/iceberg/index/spark/IndexCachePlugin.java`（public SparkPlugin入口）、同目录 `ExecutorIndexCacheWorker.java`（internal）；复用Task4的Protocol/Coordinator；必要修改 `build.gradle` 的index-spark测试依赖，仅使用已有Spark版本catalog；新 `index-spark/src/test/java/org/apache/iceberg/index/spark/TestIndexCachePlugin.java`、同目录 `TestExecutorIndexCacheWorker.java`。

**Interfaces:** Consumes：Task2构建估计/adapter及`IndexCacheMemory`容器转换、Task3缓存准入、Task4所有protocol DTO/协调器；Produces：`IndexCachePlugin.driverPlugin()`/`executorPlugin()`公共Spark接口；driver.receive(Object)处理注册/report/ACK；executor PluginContext.ask请求Reply。worker `execute(Command) -> Ack`、`close()`；读取Target所带FileIO及definition，经existing prepare路径产生CachedFile，调用Task3 installManaged；driver不转发payload。对外控制门面留Task6。

- [ ] **RED:** fake PluginContext重复命令只加载一次；慢loader时状态上报仍能响应；ID复用拒绝旧command；关闭停止线程；控制ACK仅携带状态不含payload/凭证。旧model/profile走安全限并发探测，不允许prepare在driver；无法安全估计返回SIZE_UNKNOWN。

```java
assertThat(worker.execute(command).status().name()).isEqualTo("LOADED");
assertThat(worker.execute(command).status().name()).isEqualTo("LOADED");
assertThat(loaderCalls.get()).isEqualTo(1);
assertThat(worker.execute(oldIncarnationCommand).status().name()).isEqualTo("REJECTED");
```
- [ ] **Run RED:** `./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 :iceberg-index-spark:test --tests '*TestIndexCachePlugin' --tests '*TestExecutorIndexCacheWorker'`。
- [ ] **Implement:** 默认poll=1s、stale-after=10s、max commands/reply=64、每executor max concurrent loads=1、operation timeout=10m、单命令最多3次；配置分别为 `spark.iceberg.index.cache.control.poll-interval`、`.control.stale-after`、`.control.max-commands`、`.load.max-concurrent`、`.load.timeout`、`.load.max-attempts`（后五项使用同一前缀），验证非正值。控制与加载线程分离，有界队列；临时内存额外预留。查询scratch不可与load峰值混为resident size。
- 释放语义（Task4 移交）：命令携带其安装时的 `epoch`；执行器只释放"本地副本记录 epoch ≤ 命令 epoch"的同一 cacheKey，绝不因旧 epoch 释放更高 epoch 安装的副本。`mode=ORPHAN` 的墓碑用于已无归属的迟到副本，必须走同一 epoch 规则并允许重复投递（幂等）。命令携带受限读取上下文（definition + 受控 FileIO 还原），不得把 FileIO 或凭证写入状态日志，也不得广播 `IndexCacheProtocol.Target`。
- [ ] **GREEN:** fake transport及引用延迟释放测试通过；真实SparkPlugin local-cluster测试归Task7，不能用fake宣称节点定向保证已完成。
- [ ] **Commit/review:** 精确add，`Index: Execute planned cache commands through SparkPlugin`；核查目标3.5公共API与shutdown/FileIO传播，不依赖Spark private RPC。

## Task 6 / #2410：SQL、查询与状态集成

**Files:** 新建 `index-spark/src/main/java/org/apache/iceberg/index/spark/IndexCacheControl.java`（public facade）；修改同目录 `DistributedIndexSearch.java`、`IndexCacheLocations.java`、`IndexCacheStatus.java`、必要的 `VectorIndexes.java`，及 `index-spark/src/main/scala/org/apache/iceberg/index/spark/IndexFileRDD.scala`；新建 `spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/{LoadIndexProcedure,OffloadIndexProcedure}.java`，修改同目录`SparkProcedures.java`、`IndexCacheStatusProcedure.java`；新 `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestIndexResidencyProcedures.java`。

**Interfaces:** Consumes：Task4协调器/结果及Task5插件；Produces：facade `load(SparkSession,Table,IndexMetadata,String retainFor) -> IndexCacheControl.Result`、`offload(SparkSession,Table,IndexMetadata) -> IndexCacheControl.Result`、`status(SparkSession,boolean refresh) -> List<IndexCacheControl.Status>`。面向procedure提供public immutable嵌套`Result`/`FileResult`/`Status`：Result含状态、目标快照/plan、保护文本和截止点、逐文件FileResult；FileResult含fileLocation/executorId/bytes/reason；Status含索引/节点/政策/访问年龄/新鲜度/used/reserved/releasing。facade转换内部Protocol DTO，不返回package-private类型。SQL必填table、index，选定源table当前snapshot，resolve name/UUID要拒歧义；retain_for可省略，类型String。输出每file一行，含index_uuid/snapshot_id/plan_id/status/protection/deadline/file_location/executor_id/bytes/reason；空目标也有索引摘要行，成功/缺口/unknown可对账。

- [ ] **RED:** named和unnamed UUID选择；同列两index独立，index名碰到另一UUID拒绝；首次传30m/重复省略/显式0/forever；missing plugin拒绝且不建立policy；partial保留并列missing，不凭旧快照报COMPLETE；offload重复幂等并展示releasing。

```java
assertThatThrownBy(() -> spark.sql(
    "CALL catalog.system.load_index(table => 'db.t', index => 'idx')").collect())
    .hasMessageContaining("SparkPlugin");
assertThat(offloadedSearchRows).containsExactlyElementsOf(uncachedSearchRows);
assertThat(otherIndexResidentBytes).isEqualTo(beforeOffloadOtherIndexBytes);
```
- [ ] **Run RED:** `./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 :iceberg-spark:iceberg-spark-extensions-3.5_2.12:test --tests '*TestIndexResidencyProcedures' --tests '*TestIndexCacheStatus'`。
- [ ] **Implement:** 接入现有3.5 BaseProcedure参数/结果模式与注册；generic index resolver不只vector；共享facade转换内部DTO。查询传index identity/epoch，只使用确认holders和本地acquireManaged使用租约，miss仍搜索捕获全部file但不驻留；登记实际索引使用；未管理缓存读取后可用捕获的cache keys登记纯索引归属，不能将其当managed readiness或访问更新。首次收编须阻止新增旧裸引用路径并等待已发出引用的旧查询结束（或升级为原子Lease路径），之后才应用托管目标。status不更新访问，不依赖旧accumulator做预算。无需重写fetch-back或搜索算法。
- [ ] **GREEN:** `vector_search`及batch、ORDER BY改写现有回归与新增cache-miss/offload/delete查询结果一致。不能只测SQL解析不测缓存状态。
- [ ] **Commit/review:** 精确add，`Spark: Add explicit index load and offload procedures`；检查本地STREAM/disabled预算：load明确拒绝不可驻留，offload仍能安全释放已有项，retain不改变预算。

## Task 7 / #2411：多executor、部署与真实验收

**Files:** 新建 `spark/v3.5/spark-extensions/src/test/java/org/apache/iceberg/spark/extensions/TestIndexResidencyCluster.java`，`tools/vector-benchmark/IndexResidencySmoke.java`、`tools/vector-benchmark/run-index-residency-smoke.sh`、`docs/superpowers/verification/2026-10-24-index-residency.md`。脚本不得沿用固定共享build目录或覆盖他人镜像，所有产物在本卡worktree/run-id目录。

**Interfaces:** Consumes：Tasks1–6已集成产品；Produces：可复现smoke参数 `--table --index --other-index --output --executors --cache-bytes`；启动必须有 `--conf spark.plugins=org.apache.iceberg.index.spark.IndexCachePlugin`，原image/runtime版本先核对；run-id与输出携带application ID、executor incarnations、构建SHA和镜像digest。

- [ ] **RED cluster:** local-cluster[2,1,1024]多JVM测试真实命令到目标node；两个index的容量驱逐；保护/永久与partial；offload时搜索；扩容后显式load重规划；kill executor后覆盖变unknown、查询仍完整、不后台加载；迟到旧命令无回填。每测试finally停Spark/清临时索引、文件与测试应用。

```java
assertThat(actualHolderByFile).isEqualTo(plannedHolderByFile);
assertThat(afterOffloadRows).containsExactlyElementsOf(uncachedRows);
assertThat(afterExecutorLossRows).containsExactlyElementsOf(uncachedRows);
assertThat(backgroundLoadCommandsAfterCompletion).isZero();
```
- [ ] **Run RED/GREEN:** `./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 :iceberg-spark:iceberg-spark-extensions-3.5_2.12:test --tests '*TestIndexResidencyCluster'`；环境资源不足不得降成local[2]冒充多executor。
- [ ] **Build/compatibility:** 在本卡worktree运行前三module全回归、对应Spark扩展回归、spotlessCheck，及适用的revApiCheck；新增格式/API异常不得忽略。准确Gradle revapi task若不适用记录原因，不新增例外绕过。
- [ ] **Deploy/smoke:** 构建新jar与唯一SHA镜像，记录digest；重启对应Spark应用并带插件；真实≥2executor比较uncached/loaded/partial/offloaded查询逐行结果。记录每节点budget/used/reserved/releasing、目标与实际holders、max占用比例及缺口；容量不足不保证100M HNSW全部驻留，不用旧23.67GB数字代替当前模型测量。
- [ ] **Finish:** 独立整分支审查，记录命令、exit code、清理/失败与未完成项；reviewer确认后关闭G和伞卡，才收口#2296/#1571。集群无权限/镜像不可拉/版本不匹配则保持阻塞，不宣称完成。

## 自检与执行交接

设计条款映射：身份/命令/保护→C+D+F；容量/旧manifest→A+B+E；确认分布/LRU/迁移→D+E；并发/安全释放→C+D+E；新快照→D+F；查询正确性/结果状态→F+G；插件与应用生命周期→E+G。五项Review Focus均有所属任务测试。

所有跨任务类型明确由A/B/C/D/F拥有，E不重定义protocol；facade的公共Result与内部Result显式转换，不能泄露package-private返回类型。测试遵循task独立RED/GREEN而非先写全部实现最后补测试。

本计划待用户确认，之后开始A，再沿依赖实施。推荐由主助手逐卡实现，复杂卡完成后用fresh reviewer独立审查；避免多个实现Agent同时修改共享预算/接口。此建议不是自动授权大规模多Agent workflow；不用SubagentWorkflow。实现卡只有上游done后才认领，review不是依赖已完成。
