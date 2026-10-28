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

# #2780 通用所有权与内存模型迁移 Implementation Plan

> **For agentic workers:** 使用 superpowers:executing-plans 按任务顺序执行；每项先观察行为RED，再GREEN，整卡完成后独立全分支复审。进度记在#2780和本计划专属ledger，不按计划存在推断实现完成。

**Goal:** 缓存只保留搜索真正需要的状态，model=2完整计算这些状态，旧估计或未知估计不能被当作精确容量。

**Architecture:** core默认adapter建立防御性快照；各kernel报告当前表示的独占可达状态P；index-spark统一加载、收费和估算解析，resident=P+实际额外E。C不改变IVF的逐条对象表示，D才以U+M替代S。

**Tech Stack:** Java17、现有JUnit5/AssertJ、Spark3.5/Scala2.12、现有FileIO/Puffin；不新增依赖。

**Spec:** `docs/2026-10-27-ivf-payload-in-place-design.md` §3/7/8，用户2026-10-28确认C/D边界修订。

## Global Constraints

- 独立`card/2780`，工作树`/Users/jyxc-dz-0101035/workspace/iceberg-wt/2780`，基底`ede87bcd8ff2bfc8f1d3d67d20ac97038104865d`。只add具体文件，不自动merge/push main。
- C当前IVF的P为独立对象图S，不人为长期保留已解码的U；D才持U+M，届时必须完整记U及数组头。HNSW图/映射、FLAT entries/字节分别按实际引用计算。
- 保持payload编码/manifest格式，不重建旧索引、不强制重写旧manifest。保留IndexBlob/IndexEntry公开复制合同。
- 不新增public接口、配置或依赖，不改默认值，不重写BM25统计、图算法或调度策略。现有默认prepare方法内部实现可修改，签名不变。
- model=2的P包含自有数组，不含已释放外部输入/query scratch。C/U长度仅诊断；固定JVM布局的结构估计不是精确retained heap实测。当前vector实现只对检测通过的HotSpot17/64bit/压缩引用及class pointers/8字节对齐/compact strings给known估计；HNSW还限定已确认的hnswlib1.1.0与Eclipse容器表示，不支持时null/0表示unknown。
- 数组按身份去重，slice保留完整backing必须完整计费；未知不能填0，managed不得把未知当可装下。
- plain的≥30%适用范围尚未调整，本计划不改收益门槛；E使用同基底配对，不把B14f7到当前main的无关差异归因C。

## Review Focus

1. 只读buffer仍共享可写数组；caller清空/修改或adapter消耗buffer不得影响已prepare内容及后续搜索。
2. HNSW/稀疏IVF存在共享数组、字典或collection slack；build=runtime不能代替独立可达下界。
3. 同一URI换adapter/definition/model/runtime不得复用旧估计；损坏文件失败不得缓存0。
4. 释放C不能释放searcher需要的U；offload/clear期间lease仍可用且最后引用关闭前仍收费。
5. 缺model/profile的外部adapter仍可查询，但不能被managed声称容量已知或沿用伪造小估计。

## Task 1: 默认adapter输入快照

**Files:** 修改`core/src/main/java/org/apache/iceberg/index/IndexAdapter.java`；测试`core/src/test/java/org/apache/iceberg/index/TestIndexBuildResult.java`。

**Interfaces:** 保持`prepare(ByteBuffer):PreparedSearch`和`prepareAuxiliary(ByteBuffer):PreparedSearch`；默认`estimatedBytes():long`的0仍代表unknown，不能把快照长度冒充全部searcher内存。

- [ ] 在上述测试增加两角色×heap window/slice/read-only/direct输入。使用真实TestingIndexAdapter编码条目；prepare后改原输入并清position/limit，返回的文件/行号/score必须与独立fixture一致。
- [ ] 增加会消耗收到payload窗口的测试adapter；连续search仍收到完整窗口，caller position/limit保持不变。先运行目标测试，确认RED来自借用/窗口耗尽而非编码错误。
- [ ] 默认prepare只复制有效窗口到独占数组，捕获输入ByteOrder；每次search传新可写duplicate view并显式恢复该字节序（duplicate本身不会保留输入order）。私有helper共享两个角色实现，不增加public API。未知估计保持原行为。搜索SPI没有只读限制，不能强制read-only；用会写入相同内容的adapter验证两角色、四buffer形态仍可搜索。输入快照只隔离调用方，不额外承诺adapter自己修改状态后的结果不变。
- [ ] 运行目标类和core索引测试全集，随后三个索引模块回归；确认GREEN，再提交具体两文件，提交含`Generated-by: pi`。

## Task 2: 各kernel可达结构与model=2

**Files:** `core/.../IndexFileMemory.java`；`index-vector/.../{VectorMemoryEstimates,PreparedStructureSize,IvfKernel,BruteforceKernel,HnswlibKernel,RaBitQQuantizer}.java`及`TestIndexFileMemory`、`TestVectorBuildMemory`、`TestPreparedStructureSize`。

**Interfaces:** 保持`buildWithMemory(List<IndexEntry>,int):IndexBuildResult`和`PreparedSearch.estimatedBytes():long`。P为当前searcher全部独占可达状态，不引入U+S。

- [ ] RED：三kernel、空/稀疏索引、共享字符串/primitive数组；独立遍历fixture可达数组和根对象得下界，报告不得小于下界。build/runtime同profile一致；不要复用被测size helper计算oracle。
- [ ] RED：model2的diagnostic payloadBytes+preparedBytes超过long不应因不参与resident的相加而拒绝；旧model1原合同保持可读，字段自身仍非负。
- [ ] 补全实际引用、数组头/对齐/集合容量和身份去重的计算，再一起升级model标记与javadoc。profile描述真实适用的布局；不能证实布局时memory为unknown，不能宣称精确。
- [ ] 单类→vector全部/core索引全部GREEN后提交。C不改list布局或打分算术。

## Task 3: 缓存去除无用C及统一收费

**Files:** `index-spark/.../{DistributedIndexSearch,ExecutorIndexCache,IndexCacheMemory}.java`；`TestIndexCacheMemory`、`TestExecutorIndexCacheReservations`。

**Interfaces:** 保持`loadFile(FileIO,String,IndexAdapter):CachedFile`，包内`loaded(...)`根据实际持有状态调整；`residentBytes(IndexFileMemory):long`消费model2的P，不无条件加fileBytes。

- [ ] RED：真实NONE/ZSTD容器，相同prepared表示收费一致；缓存根不得持有不需要的整容器C。默认adapter保留的独占U可搜，不能在去C时损坏它。
- [ ] RED：同一searcher共享所有权包装不重复收费；不以caller输入长度或压缩长度代替实际P。
- [ ] 加载完成后仅保存prepared及确实需要的额外状态。known路径收P+E；unknown保持明确不精确的legacy fallback且不得进入managed严格准入。租约释放之前不能清空prepared。
- [ ] 相关缓存测试及index-spark全部GREEN后提交；同步费用公式及注释，不删公开复制。

## Task 4: 统一resolver与旧估计失效

**Files:** `index-spark/.../{IndexCacheMemory,DistributedIndexSearch,IndexCacheController,IndexCacheProtocol,IndexCacheExecutorLoop,IndexCacheReadContext}.java`；`TestDistributedIndexSearchSizing`及protocol/controller测试。

**Interfaces:** 新增包内`IndexCacheMemory.resolve(FileIO,IndexFile,IndexAdapter):Long`，null表示unknown。query helper和Controller共用解析结果，不把新bytes配旧model/profile。旧long消费者需要显式传播unknown，而非0。

- [ ] RED：model1/99/null、reader/profile mismatch及伪造小memory，不能被直接信任；坏payload/IO失败不得缓存0，重试成功可重新测量。
- [ ] RED：同URI换reader/definition/model/runtime，估计必须失效。仅缓存成功且可证实适用的结果，绑定不可变文件身份和读取上下文。
- [ ] 初期允许全量load/prepare重估。现有public API没有通用当前模型描述器，不能只凭blobType信任manifest；无法证明兼容者重估或unknown，不擅加接口。G才负责减少首次读取。
- [ ] 去掉workingSet中无依据的“header read”及“measured”措辞；正确报告真实全量prepare/结构估计与失败。executor profile不得为空或伪称兼容。
- [ ] 单类→index-spark全测试GREEN后提交。

## Task 5: 容量、lease及整卡验收

**Files:** `index-spark/.../{ExecutorIndexCacheWorker,IndexResidencyCoordinator,IndexCacheLocations,ExecutorIndexCache}.java`及对应tests；工具/集群证据只在独立iceberg-tools分支。

**Interfaces:** 复用现有protocol的known/unknown状态、SIZE_UNKNOWN失败和Lease，不增加配置或主动重平衡。

- [ ] RED：unknown不选PIN_ALL，不在managed开始load或宣称COMPLETE；loader调用0、缺口明确。错误profile同样不能证明装下。
- [ ] RED：offload/clear后在用lease仍可搜索、仍收费，最后close才归零；共享物理文件多owner去重、reservation/admission/eviction一致。
- [ ] 修复费用/unknown传播，保持原保护/LRU/ACK合同。分别回归IVF/HNSW/FLAT，文本只回归适用公共读取。
- [ ] 所有受影响单测、三索引模块、Spark3.5索引集成、spotlessCheck与API检查执行并归档；失败明确记录，不能以仅目标类绿替代整卡。
- [ ] 独立新JAR/镜像重启本任务测试应用并真实smoke，核来源、回收本run资源，不能替换其他应用。完整收益/配对由E验收。
- [ ] 全分支独立review，修复重要问题；设计/本计划的仓库副本与umem逐字节hash一致。#2780交review，不自动done或合main。

## Verification Commands

全部在本卡worktree，JAVA_HOME为已有Homebrew JDK17。

```bash
./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 :iceberg-core:test --tests org.apache.iceberg.index.TestIndexBuildResult
./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 :iceberg-core:test --tests 'org.apache.iceberg.index.*'
./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 :iceberg-index-vector:test :iceberg-index-spark:test :iceberg-index-text:test
./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 spotlessCheck :iceberg-core:revapi :iceberg-api:revapi
```

Expected：测试实际执行且XML 0 failures/errors；编译失败不算行为RED，缺测/跳过明确记录。初始新树89+271+6=366测试通过仅作前置基线，不代表本计划完成。
