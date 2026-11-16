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

# 让文本索引进入驻留缓存

行号对应 `index` 分支 `022d0ce55b`。本文只描述设计与施工顺序，不含实现。

## 1. 为什么做

BM25 的查询成本现在全部压在「每查询、每分片、按字节范围重读所需词项的倒排」上：`DistributedIndexSearch.searchText` 对每个分片 `io.newInputFile(location)` → `TextIndexAdapter.searchShardBatch(InputFile, …)` → 先读 Puffin blob 元数据，再开流，再按词 Range 读。

同一个库里，向量索引从驻留缓存里拿到了全部好处（`ExecutorIndexCache` 把 prepared 结构按 executor 常驻，`load_index` 预热，命中后查询不碰 `FileIO`）。文本索引一行都没享受到。在对象存储上这不是延迟小账：#3042 记录过每任务约 2.8 MB/s，读的就是同一批分片字节，而热查询本可以完全不读。

## 2. 现状（已在代码上核对）

### 2.1 文本查询的读路径

- `DistributedIndexSearch.searchText`（`index-spark/.../DistributedIndexSearch.java:1239`）读快照统计文件 → broadcast → 每索引文件一个 task → `TextIndexAdapter.searchShardBatch`（`index-text/.../TextIndexAdapter.java:255`）。
- 该入口每调用一次就 `rangeBlob(file)`（`:270`，一次 `Puffin.read(file)` 拿 blob 元数据）+ `file.newStream()`（`:262`）+ `TextIndexPayload.open(source, size)`（`:289`）。
- 与驻留缓存唯一的接触是 `IndexCacheControl.recordAccess`（`DistributedIndexSearch.java:1177`），只用于 LRU 记账，不取数据。

### 2.2 缓存是怎么工作的

`load_index` 由控制通道下发命令（`IndexCachePlugin` → `IndexCacheReadContext.load`），executor 侧 `DistributedIndexSearch.loadFileState`（`:1702`）读整个文件 → `prepareFile`（`:1748`）取第一个可读 blob，调 `adapter.prepareAuxiliary(payload)` 得到 `IndexAdapter.PreparedSearch` → `ExecutorIndexCache.loaded(payloadBytes, prepared)` 常驻。查询侧用 `cache.acquireManagedKey(cacheKey(owner, adapterKey, location))`（`ExecutorIndexCache.java:1763`）拿租约，命中即用缓存里的 prepared 结构，未命中才读文件。缓存键是 `owner|adapterKey|location`（`DistributedIndexSearch.java:1551`），其中 `adapterKey` 来自 `IndexCacheMemory.readerKey(adapter)`（`:687`）。

### 2.3 今天 load 一个文本索引会失败，不是静默占预算

三处前置全缺：

1. **没有 prepared 形态**。`TextIndexAdapter` 未覆写 `prepareAuxiliary`，默认实现（`core/.../IndexAdapter.java:122`）返回的 searcher 其 `estimatedBytes()` 是默认 `0L`（`:192`）。而 `IndexCacheReadContext.load` 要求 `loaded.preparedBytes() > 0`（`IndexCacheReadContext.java:70`），于是抛 `Index prepared memory is unknown: <file>`。
2. **缓存键构造不出来**。`IndexCacheMemory.currentDescriptor(adapter)`（`:562`）要求 adapter 能产出 model 2 描述符（`descriptor()` 走 `adapter.encodeAuxiliaryPayloadWithMemory(List.of(), 1)`）；文本没有内存估计，`readerKey` 返回 null，`owner|null|location` 不是合法键。
3. **规划价是上界**。manifest 里没有 memory 记录，`IndexCacheMemory.resolveFile` 退到 `fileBytes × 2`（`:487`），于是 `load_index` 的规划数字是两个 `fileBytes`。

由此更正一条早前的错误判断：2026-11-16 我曾把「文本索引 load 会成功并静默吃掉预算」写进卡片，代码显示它其实在 executor 侧就失败了，**不存在静默占预算**。因此也不需要一张「先加护栏」的卡。

### 2.4 同业对照：预热倒排是常规做法

- Elasticsearch：`index.store.preload` 把 Lucene 的 `.tim` / `.doc` / `.pos` 预热进 page cache；另有 request cache 与分片级 query cache。
- Lucene：本身无块缓存，靠 mmap + OS page cache。
- Lance 12：有完整的 FTS prewarm 子系统，源码路径出现在 native 库字符串里（`rust/lance-index/src/scalar/inverted/index/posting_prewarm.rs`），事件是 `fts index prewarm started` / `fts partition posting lists prewarmed` / `fts index prewarm finished with partial residency`，状态字段含 `resident posting lists`、`resident positions`、`resident document prewarm completion`、`shared scalar index prewarm state`，失败信息 `Failed to prewarm index: cache is no longer available`；另有一套「准备好的 BM25 查询」概念（`prepare_bm25_query`、`PreparedBm25Query`、`LANCE_FTS_REUSE_PREPARED_SCORER`）。

即：把倒排预热进常驻内存是主流做法，且允许「部分驻留」。我们不做的唯一原因是这套缓存当初只为「单文件能独立作答」的索引设计。

## 3. 设计

### 3.1 四步施工

**S1 · 文本内存模型（前置）**
给 `index-text` 加 `TextMemoryEstimates`（对照 `index-vector/.../VectorMemoryEstimates.java:20-70` 的形状），并覆写 `TextIndexAdapter.encodeAuxiliaryPayloadWithMemory(entries, parallelism)`，产出的 `IndexFileMemory` 满足：`modelVersion = 2`、`payloadBytes = payload.remaining()`、`preparedBytes = payload.remaining() + 固定头`、`readerId = "text/bm25/<payloadFormatVersion>"`、`runtimeProfile` 与向量侧同一套「hotspot-17-64;…;processors=N」常量串、`structure = {"payload-bytes": …}`。

没有这一步，S2/S3 全都不成立（缓存键和 `preparedBytes > 0` 两道门都靠它）。

**S2 · prepared shard**
`TextIndexAdapter.prepareAuxiliary(payload)` 返回一个私有 `PreparedShard`：持有解压后的 payload 字节，`estimatedBytes()` 报字节数 + 固定头，`search(query, topK)` 明确抛错并指向分片入口（单分片在没有语料统计时无法打分，静默返回错分数比抛错更糟）。配套加一个 `searchShardBatch(byte[] payload, queries, limits, statistics, candidates)` 重载，内部用已存在的 `TextIndexPayload.source(ByteBuffer)`（`TextIndexPayload.java:117`）与 `open(...)`，与流式路径共用同一套解码。

**S3 · 查询命中缓存**
`searchText` 的 task 内：构造 `cacheKey(owner, readerKey(adapter), location)` → `cache.acquireManagedKey(...)`；命中则用 `PreparedShard` 的字节打分（零 `FileIO`），未命中走今天的 Range 路径。**不在 miss 时填充**（见 D2）。文本查询此刻才第一次真正共享向量索引那套预算、租约与淘汰。

**S4 · 定价与可观测**
manifest 有了 memory 记录后，`load_index` 对文本索引的规划价从 `fileBytes × 2` 变成实测值；`index_cache_status` 里的容量也随之为真值。可选顺手项（不阻塞）：把「这份容量是实测还是上界」读进 `IndexCacheProtocol.Result`（原 #2993 的一半）。

### 3.2 目标状态

```
load_index(table, text index)
  driver 规划：读 manifest memory 记录 → 每文件 preparedBytes（实测）
  executor：读文件一次 → prepareAuxiliary → PreparedShard（持 payload 字节）常驻、计费
query
  task：acquireManagedKey 命中 → 直接从字节打分（无 FileIO）
       未命中 → 今天的 Range 读（结果相同，只是慢）
```

## 4. 决策

- **D1 · prepared 只持解压后的 payload 字节，不建解码字典。** 省掉的是每次查询的 Range 读（对象存储上最贵的那段），解码与打分的 CPU 不变。把字典/倒排解码成常驻结构能进一步省 CPU，但那会让单文件驻留远大于 payload，预算模型要重做，留给二期。
- **D2 · 不自动预热、不在 miss 时填充。** 保持 2026-10-24 的驻留设计决定（「已管理索引的 query miss 不自主新增驻留，LRU 后显式 load 恢复」）。收益由 `load_index` 触发，查询路径保持只读。
- **D3 · 不做「短期护栏」卡。** 用户 2026-11-16 判定为浪费时间：现状 load 已经明确报错，护栏是重复劳动。
- **D4 · 不新增用户可调参数。** 复用现有 `spark.iceberg.index.cache.*`（SPEC 2026-10-27：新增参数须先获批）。
- **D5 · 单分片 prepared 的 `search()` 抛错而不是降级。** 缓存契约的 `search(query, topK)` 没有语料统计的位置，返回任何值都是错的分数。

## 5. 否决的替代

- **只补 memory 记录、不接查询。** 数字变好看，查询一行没变快；这正是本轮被推翻的旧方案。
- **只把 `fileBytes × 2` 换成 `fileBytes × 1.2`。** 换的是规划余量，不是读取次数。
- **在 driver 上常驻整个快照的倒排。** 回到 BM25 v1 之前的形态（`searchesAllPayloadsTogether` 的 driver 合并路径），也是 #2869 关掉的那条内存上界问题。
- **把语料统计也塞进每个文件的 prepared。** 统计是每快照一份、跨分片共享的，塞进文件级 prepared 会重复 N 份，且刷新语义更乱。
- **查询 miss 时顺手填充。** 违反 D2，且把查询变成写路径，故障面变大。

## 6. 验收

1. `CALL load_index` 对文本索引成功：`index_cache_status()` 出现驻留条目，charge ≈ 解压后 payload 字节（不再是 `fileBytes × 2`）。
2. 预热后同一查询**不再产生分片 Range 读**：用记录型 `FileIO` 替身，或 MinIO/OSS 的请求日志，对比 load 前后每查询的 GET 次数与字节数。
3. 结果不变：同一快照、同一统计下，命中路径与 Range 路径返回的行、顺序、`_score` 逐位一致。
4. 向量索引零回归：`load_index` / 查询命中 / `index_cache_status` 的既有用例全绿。
5. 预算口径：一个真实文本索引文件的「解压 payload 字节 / 文件字节」比例记录下来（决定 `fileBytes × 2` 上界是否够，见风险 R2）。

## 7. 风险与未验证

- **R1 · `runtimeProfile` 常量复制会漂移。** `PreparedStructureSize` 是 `index-vector` 的包私有类，`index-text` 用不到，只能复制 `REFERENCE_BYTES` / `ALIGNMENT` / 头大小与 profile 串。常量一旦与向量侧不一致，`compatible()` 会判不兼容，记录静默失效退回上界——不会算错，但会悄悄失去精确值。缓解：要么把常量提到公共位置（推荐），要么在两侧各留一条指向对方的注释与一条对拍测试。
- **R2 · payload 字节比压缩文件大。** blob 默认 ZSTD，解压后可能显著大于文件。若实测比例超过 2，`fileBytes × 2` 上界反而不够，planning 会低估 → 需要在 S4 里用实测值取代上界（本来就该有 memory 记录），并记录比例。
- **R3 · 键一致性。** `load_index` 写入的 key 与查询时 `acquireManagedKey` 的 key 必须逐字一致（`owner|adapterKey|location`）。`adapterKey` 来自 `readerKey`，对文本是新引入的，若两侧在不同 JVM 上算出不同 `readerId`/profile，就会永远 miss 且没有任何报错——**这是本设计最容易静默失败的地方**，验收第 2 条就是为它设的。
- **R4 · 文本索引是 auxiliary blob。** `load_index` 的 target 用 `file.kind()` / `file.blobType()` 规划，需确认文本（`writesAuxiliaryFiles() == true`）在放置与租约路径上与向量走同一套代码，没有按 kind 排除的分支。
- **未验证（本文按代码推断，未实跑）**：文本索引 `load_index` 的实际报错文本与失败位置；单文件解压 payload 与文件字节的比例；预热后 p50 的真实下降幅度。

## 8. 参考

- `index-spark/src/main/java/org/apache/iceberg/index/spark/DistributedIndexSearch.java:1239 / 1177 / 1551 / 1702 / 1748`
- `index-spark/src/main/java/org/apache/iceberg/index/spark/IndexCacheReadContext.java:70`
- `index-spark/src/main/java/org/apache/iceberg/index/spark/IndexCacheMemory.java:487 / 562 / 687`
- `index-spark/src/main/java/org/apache/iceberg/index/spark/ExecutorIndexCache.java:475 / 1763`
- `core/src/main/java/org/apache/iceberg/index/IndexAdapter.java:122 / 192 / 359`
- `index-text/src/main/java/org/apache/iceberg/index/text/TextIndexAdapter.java:255 / 270 / 289 / 296`
- `index-text/src/main/java/org/apache/iceberg/index/text/TextIndexPayload.java:117`
- `index-vector/src/main/java/org/apache/iceberg/index/vector/VectorMemoryEstimates.java:20-70`（要照抄的形状）
- 相关卡：#2992（本设计）、#2869（driver 侧 payload 上界的旧问题，已关）、#3042（每任务 2.8 MB/s 的实测来源）、#3043（存储配置通道）
