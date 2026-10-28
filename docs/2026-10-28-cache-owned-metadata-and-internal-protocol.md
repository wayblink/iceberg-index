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

# #2780 缓存owned metadata与内部消息记账

状态：用户已确认该内部协作调整纳入C；尚未实现完成。当前源码基底`ac9052058d353a8afa9a93895394befb309b855e`；正式缓存根用例已观察RED：收费296B，独立下界28,488B。本补充设计落实独立复审第1项，不将第2/3项修复或普通suite GREEN冒充完整E通过。

## 目标与约束

- 在同一executor按可达组件集合计算P和E，保留完整key、owner、index身份和真实backing，共享按identity去重。不同executor副本独立收费。
- C仍使用当前IVF独立S，D才U+M；不改图、评分、BM25、payload、manifest、SQL/CALL及配置默认值，不新增第三方依赖或公开Java方法。
- 采用当前明确支持的JVM结构模型，不把它称为精确retained实测；包装layout不能证明时unknown。
- 保留索引resident最后lease释放后归零；仍需保留的控制状态另占总预算，不宣称JVM或管理器完全无内存。
- 只修改现有driver/executor内部消息内容，复用通信通道、计划/命令身份与现有epoch/sequence；不新增网络服务、自动修复、重平衡或保护策略。
- 每项先RED→GREEN，所有已实现修改在独立worktree；成果不自动merge/push main。

## 1. 费用与归属

将已用拆成R和Q。

- **R：索引驻留组件。** 每个独占prepared P、缓存包装/锁/receipt、仍由驻留或lease保留的key/owner、容器节点和关联组件。共享组件在同一收费快照中按identity只计一次。
- **Q：必要控制组件。** 没有resident也必须保留的最新epoch、paused/managed拒绝旧请求策略以及其增长的容器、完整文本和关联状态。Q并非免费，也不应混称为索引文件仍驻留。
- **H：预留增量。** prospective mutation尚未分配/提交的正增量；已真实分配的reservation元数据属于R/Q，不能再重复计入H。
- **基底：**固定空cache管理器、全局锁和计数器。已扩大的table/backing不是固定空基底；clear/release后必须实际释放可释放的状态，或继续收费。

预算比较`used=R+Q`、`effective=used+H`；releasing是used中等待最后引用的子集，不是第三份已用内存。`Stats.bytes()`等已有索引驻留口径及per-index result使用R组件归属；内部节点预算占用使用used。多index的单独诊断可能引用同一个共享组件，不能把诊断逐行相加当全局总额。

所有组件有明确的归属及liveness。一个file卸载而另一个仍保有owner或lease时，共享组件不释放；最后使用者释放后才消失。新增可选observational attribution超预算则不保存；新增必要ownership超预算拒绝新接纳且不产生半提交。释放和缩小状态不应因无空闲预算被拒绝。

## 2. owned元数据

新增包内`IndexCacheMetadata`，定义缓存内完整身份文本、引用、状态及费用投影。

- 同一完整文本可规范成一个owned对象；lookup不长期保留调用方输入。`new String(String)`不证明独占backing；使用明确的新存储，完整计header、backing和对齐。不同对象共享backing时不能双方都按完整新array收费。
- 采用容量明确且可计的内部map/set，或显式记录其真实分配容量及重建规则，不靠当前size假定backing恰好同长。
- 保持probation/protected访问顺序、asked语义、索引保护和归属判断，不借内存重构改变LRU/调度算法。
- `clear`后必须区分保留的控制策略与释放的resident组件，不能因为HashMap.clear清空元素就假定大table已释放。
- `loading`/reservation/pending/attribution/ownership增长也有受控费用及生命周期，不只计CachedFile直接字段。

包内接口决策：

```text
IndexCacheMemory.project(MetadataState before, Mutation mutation): ChargeProjection
ChargeProjection: after-state, residentBytes, controlBytes, reservedBytes,
                  releasingBytes, identity-components, attribution
ExecutorIndexCache.chargeSnapshot(): ChargeSnapshot
ChargeSnapshot: epoch/version, usedBytes, residentBytes, controlBytes,
                reservedBytes, releasingBytes, holders, shared-group refs
```

类型仅在index-spark内部使用。可以细化为针对file/owner/control的typed mutation，但不能把完整E退化为逐file常数。投影的component counts、backing容量和identity归属必须与实际提交表示一致；仅两个调用同一函数相等不足以证明正确。

## 3. 原子接纳与释放

`ExecutorIndexCache`在mapLock内统一处理reserve、put、applyTarget、addManagedKeys、recordIndexAttribution及release/clear的元数据投影。

1. 基于当前state和mutation计算after及正增量，先检查预算/现有预留。
2. 只有可接纳时才发布after、更新liveness及收费快照。读者不能拿到未收费的已增长状态。
3. 不可接纳则保持before和已有leases；不强制清除在用searcher。
4. 最后lease关闭后按实际剩余组件重算，不用旧`file.bytes()`机械递减。

保留已实现的current reader/profile验证：暖复用和冷加载发布前均验证真实receipt，unsupported或unknown不准入managed。新metadata状态不能使这些检查再次被绕过。

## 4. 内部消息闭环

现有三参数`workingSetBytes(FileIO,List<IndexFile>,IndexAdapter)`缺owner/group，不能证明完整resident时返回显式unknown；prepared P可单独作诊断，不填owner零值。

新增包内owner-aware入口：

```text
workingSetBytes(FileIO io, List<IndexFile> files, IndexAdapter adapter, String owner)
```

其消费者迁移必须包括普通query和searchBatch，而不是只改Controller。

- **Controller/Target：**显式保存owner、index及共享组形状；File的memory P与诊断C/U保持原语义。文件私有组件费和集合总费用区分，不重新无条件加C。
- **NodeReport：**提供当前metadata形状、group引用及同版本收费快照；仅较新快照可更新预算视图。
- **Planner/Coordinator：**使用同一project为每节点组合已持有和新file/owner组件。首次owner创建、已有file新增ownership、迁移到另一节点都有不同增量；释放共享file不一定释放整个group。
- **Worker：**统一由cache预留/提交，不把独立promised/Installed.bytes求和当完整内存。
- **ACK：**保留现有command/plan/epoch/sequence确认coverage；附提交后的收费快照或其明确版本，重复/迟到消息不重复加预算。容量不靠盲加command.bytes更新。
- **Locations/结果：**消费cache快照总费和目标组件归属；全局预算与per-index resident诊断不能混算。

缺owner/group或不适配的profile保持unknown，不能从mode字段拆出一个猜测值，也不能把原helper的诊断数字当完整容量。

## 5. TDD实施顺序

### E1: owned元数据与缓存根下界

- [ ] 保留现有正式`TestIndexCacheMemory.cacheRootChargeIncludesLongKeyAndOwnerStorage`，先将其接到真实R及Cache-root oracle，禁止跳过。
- [ ] 添加Latin/中文长key和owner、相同/不同backing、容器扩容及清空样本。oracle独立遍历实际根、按identity计算组件下界，不调用project/production size helpers。
- [ ] 实现owned文本、容量明确结构与基础project；验证单状态完整E后才提交。

### E2: 原子预算与共享生命周期

- [ ] RED：共享owner不能每file复制/重复收费，新增attribution/ownership必须收费；超预算接纳不留下增长，释放不被拒绝。
- [ ] RED：clear不能保留未收费大backing；共享offload、双owner、在用lease末次释放保持正确。
- [ ] 接所有mutations及chargeSnapshot，保留resident零口径并单列control预算；相关全suite GREEN后提交。

### E3: owner-aware规划与消息

- [ ] RED：两file同owner在一节点共享一次，在两节点各计一次；已有file新增owner和缺owner不能误报可装下。
- [ ] 扩展内部Target/NodeReport等typed shape和owner-aware helper，迁移所有普通query/searchBatch及Controller入口。
- [ ] Coordinator/Worker/Locations共同使用project及较新快照，拒绝重复/倒序容量更新，原coverage/epoch/ACK断言保持。

### E4: 全分支验证

- [ ] core索引、index-vector、index-spark、index-text及Spark3.5相关集成回归；真实root oracle GREEN，所有Important有行为证据。
- [ ] 检查格式/API；既有未修改的TestRowsAtJson格式失败单独记录，不为整体绿混入无关修改。
- [ ] 独立核验完整费用与共享、unknown及暖复用；新JAR/镜像真实SQL smoke及清理仍须完成后才能验收C。

## 6. 取舍与限制

拒绝逐file均摊owner费用：首file失败或已卸载、第二file仍有lease时，均摊归属不能表达真实liveness。拒绝每file全额收费：把共享误报为复制。拒绝截短key/复制大owner来迁就模型：分别丢身份信息和新增无用内存。

集合模型需要更多内部状态/消息变更，并增加metadata投影成本。这些成本须在E验证及后续性能测量中报告，不能以正确记账掩盖查询回归。本轮不承诺收益、不调整plain的30%验收范围，也不自动合main。
