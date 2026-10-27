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

# IVF 索引常驻结构改造设计（#2773）

状态：设计待评审。对应卡片 #2773（原属 #2020 伞卡，2026-10-27 移出为独立卡）。相关：#2024（缓存按真实占用记账）、#2772（list 级 Range 读）。

## 1. 一句话

让 IVF 的「prepared 结构」不再是逐条解码出来的对象图，而是**同一份 payload 字节 + 一张 list 目录**；查询时按偏移算术定位打分，只给幸存者建结果对象。目标不是让单条向量算得更快，而是让同一台 executor 能常驻的索引文件数变成约 2.7 倍。

## 2. 现状与账

### 2.1 一次查询现在付什么

读一份 IVF payload 的链路（坐标已核对）：

| 位置 | 做什么 |
|---|---|
| `index-spark/.../DistributedIndexSearch.java:657` `readPayload` | 整个 Puffin 容器 → `byte[]` |
| `core/.../PuffinIndexFileContainer.java:96-98` `read` | 解压后的 blob → `new byte[remaining]` → `IndexBlob.of(...)` |
| `core/.../IndexBlob.java:41,54` | 构造函数 `payload.clone()`、`payload()` 再 `clone()` |
| `index-spark/.../DistributedIndexSearch.java:694` `prepareFile` | `blob.payload()` 的那份 clone → `ByteBuffer.wrap` → `adapter.prepare` |
| `index-vector/.../VectorPayloads.java:159` `toBytes` | 再复制一份，喂给 `DataInputStream` |
| `index-vector/.../IvfKernel.java:270` `prepare` | `load()` 逐条解码 |
| `index-vector/.../IvfKernel.java:469` `readList` | 每条记录 `code.clone()` + `new float[3]` + 一个 `IvfEntry` |
| `index-vector/.../IvfKernel.java:374-392` `ListSearcher.search` | 被探测到的 list 里**每一条**都 `IndexSearchResult.of(...)`，最后对全部结果 `sort` 再截断 |

三个 kernel（IVF / HNSW / 暴力）都在 `toBytes` 处各复制一次；HNSW 还会把 graph 再拷进自己的一块 `byte[]`（`HnswlibKernel.java:206-214`）。所以“拷贝链”是共用基础设施层面的问题，不只是 IVF 的问题。

同一时刻最多有 4 份 payload 存活：容器 `content`、`IndexBlob` 里那份、`payload()` 返回的那份、`toBytes` 返回的那份。一个 752MB 的文件 prepare 期间峰值约 3GB——这解释了为什么单文件上限只能挂在 2GB 附近（`IndexPayloadLimits.java:32` `MAX_PAYLOAD_BYTES = Integer.MAX_VALUE`）。

### 2.2 常驻字节：每条目的账

按 `PreparedStructureSize` 的模型（压缩引用、8 字节对齐、compact string）复算，128 维、单条目：

| 配置 | payload/条目 | 结构/条目（现在） | 结构/条目（改后） | 常驻/条目 现在 → 改后 | 降幅 |
|---|---|---|---|---|---|
| 4 bit（码 64B） | 88 B | 152 B | ≈0（目录按 list 摊） | **240 → ~90** | 62% |
| 1 bit（码 16B，现场推荐） | 40 B | 104 B | ≈0 | **144 → ~42** | 71% |
| 未量化（码 512B） | 524 B | 568 B | ≈0 | **1092 → ~524** | 52% |

其中 `preparedBytes()` 现在报的是 payload 的 1.78 倍（4bit）／2.6 倍（1bit）／1.09 倍（未量化）；缓存记账是 `payload.length + preparedBytes`（`IndexCacheMemory.java:36-39`），所以预算看到的是 payload 的 2.78／3.6／2.09 倍。

**位数越低收益越大**，而现场推荐配置恰恰是 1bit + over=100——这是这条改造最划算的地方。

### 2.3 换算成真实机器

以报告里那套环境（6 executor × 12GB 堆，缓存预算假设每台 6GB）为例：

- **100M × 4bit**：索引合计 7.49GB、256 个文件、每文件约 390k 条。现在每文件常驻约 94MB，一台机器只装得下 64/256 个 → coverage 0.25；改后每文件约 35MB，装 171 个 → coverage 0.67。
- **10M × 4bit**：索引合计 752MB（磁盘，压缩后），十几个文件，一台机器早就全装下 → coverage 1.0。**这种场景下这条改造几乎换不来延迟**，只有 prepare 首查和 p99 抖动的收益。

所以要认清卡的定位：它是**容量/规模卡**。单表 10M、单索引、已经全常驻的场景不是它的价值场景；价值场景是 100M、多表多索引并存、以及“堆不变的前提下留出安全边际”。

### 2.4 每查询的候选数（卡里那个数字要改）

被打分并建结果的条数是每个文件 `nprobe × (entries_file / nlist_file)`，而 `nlistFor` 默认取 `sqrt(entries_file)`（`IvfOptions.java:119-126`，按文件算），于是总数 ≈ `nprobe × sqrt(entries × files)`：

- 100M × 256 文件 × nprobe=32 → 32 × 625 × 256 ≈ **5.1M**
- 10M × 16 文件 × nprobe=150 → 150 × 790 × 16 ≈ **1.9M**
- 10M × 单文件 × nprobe=150 → **47 万**

原卡写的“80 万”对应“100M 只装 6 个文件”的形态，与现场 256 文件不符。这个数字只影响对“分配”这条收益的打分，不影响方案本身：500 万次分配约 50ms，相对 9.6s／13.5s 是零头，所以**分配是过程指标，不是主收益**。

## 3. 目标与非目标

目标：

1. prepared 结构不再按条目物化：payload 字节 + 每 list 的偏移目录 + dataFile 字典。
2. 打分循环零分配；只有最终 topK 的幸存者解码成 `IndexSearchResult`。
3. `preparedBytes()` 按新布局重写，记账口径与实际保留一致。
4. **payload 格式不变**（`FORMAT_VERSION`／`QUANTIZED_FORMAT_VERSION` 都不动）——不需要重建任何索引，这是本设计的硬约束。

非目标：不改召回与打分口径；不引入 direct memory／mmap；不改 HNSW 的图外置方式；不做 list 级 Range 读（#2772 会复用本设计的目录，但不在本卡做）。

## 4. 方案

### 4.1 P0：拷贝链收敛到一份（前置条件）

必须先做，否则后面的收益兑现不了一半（见 5.1）。

1. `PuffinIndexFileContainer.read` 不再 `new byte[]` + `IndexBlob.of` 的两次复制；读出的 blob 以「源数组 + 偏移 + 长度」的只读视图表示。
2. `IndexBlob` 现在的 `clone()` 是为“调用方不能改”服务的契约（`IndexBlob.java:41,54`）。改成共享视图需要换一个契约（只读视图 + 长度限定），这属于 core 公开类的语义变更，要过 revapi 讨论；备选是不动 `IndexBlob`，只在索引读路径新增一个包内可见的“零拷贝 blob”类型。
3. `VectorPayloads.toBytes` 改成“能共享就共享”（`ByteBuffer.hasArray()` 时直接取数组 + `arrayOffset`），三个 kernel 一起受益；`prepareFile` 直接把 `(byte[], offset, length)` 交给 adapter，避免 `toBytes` 那一份。
4. 决定 `ExecutorIndexCache.CachedFile.payload`（`ExecutorIndexCache.java:364,385`）的去留：它在生产代码里没有读方（只有测试读），却全额进 `residentBytes`。要么删掉并改记账，要么让 prepared 直接持有它（4.2 的选择）。

P0 自身收益：prepare 峰值 ≈ 4 份 → 1 份（752MB 文件从 ~3GB 到 ~0.76GB），少一份常驻；行为零变化，三个 kernel 都受益。

### 4.2 P1：payload 原地 + 目录 + 有界堆

**内存表示。** `PreparedSearch` 持有：payload `byte[]`（共享自容器，见 P0）+ `int[] listOffsets`（长度 `nlist + 1`）+ `int[] listCounts`（或由偏移差推导，读时校验 header 里的 count）+ dataFile 字典 `String[]` + 每 list 一个 `RaBitQQuantizer` 的**轻量**形式。

payload ≤ 2GB（`IndexPayloadLimits`），所以偏移用 `int` 就够，目录本身 `nlist × 4~8 B`：nlist=2915 时约 12–23KB，相对 752MB 可忽略。

**定长记录按算术定位。** 每个 list 的 header 与条目布局都不变：

- 量化：list header = `dim × float`（list 中心）+ `int count`；条目 = `int dataFileIndex` + `long rowPosition` + `code(codeBytes)` + `3 × float factors`
- 未量化：list header = `int count`；条目 = `int dataFileIndex` + `long rowPosition` + `dim × float`

条目在 list 内定长，所以第 i 条地址 = `listStart + headerBytes + i × entryBytes`。`rowPosition` 落在 4 mod 8 的偏移上：`byte[]` 视图的 `VarHandle` 普通 `get/set` 允许非对齐（原子操作不允许，本设计不用原子操作），用 `MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN)`。

**打分循环不建对象。** `RaBitQQuantizer` 的打分签名已经是 `(byte[] code, int offset, float[] factors, …)`，`offset` 是 code 的相对字节偏移（内部按 `offset * 8` 算位），所以传 payload 绝对偏移即可直接复用。`factors` 是数组，两个选项：

- **推荐**：给 `estimatedDistanceSquared`／`estimatedInnerProduct` 增加 `(byte[] payload, int factorsOffset, …)` 重载，用 `Float.intBitsToFloat` 读 3 个数。理由：不引入可变状态，`PreparedSearch` 保持“无隐藏状态、可并发调用”的性质。
- 否掉：per-prepared 的 `scratch float[3]`。它只有在“同一 prepared 不被并发搜索”时才正确，今天靠 `CachedFile.searchLock`（`ExecutorIndexCache.java:388-398`）兜住，将来谁放开并发就静默算错分。

`rotateQuery` 每探测一个 list 会分配一个 `float[padded]`；可以顺手改成复用一个 per-search 的 scratch（生命周期在单次 `search` 内，没有并发问题）。

**有界堆 + 总序。** 维护大小为 `topK` 的堆，键是 `(score, visitRank)`，`visitRank` = 候选被访问的全局自增序号（list 访问序 → list 内条目序）。这**必须**与今天的“按访问顺序 add + 稳定排序”等价：今天同分候选的顺序由访问顺序决定，`nearestCentroids` 还专门为可复现写了固定 tie-break 的注释（`IvfKernel.java:592-598`），所以同分必须在堆里也用同一总序，否则同分候选的**集合**和顺序都会变。实现用 `double[]` 分数 + `int[]` 序号，避免为候选建对象；出堆后按同一总序升序输出。内存上只有 `2 × topK × 8 B`。

**顺手一起改的两处（同一个文件，同一轮测试）**：

- `nearestCentroids`（`IvfKernel.java:581-603`）每查询为每个 list 装箱一个 `Integer` 再排序（10M 单文件 nlist=2915）。改成 `double[]` + `int[]`（或用大小 `nprobe` 的选择堆），零风险。
- 每个 list 的 `RaBitQQuantizer` 都重算一份 `signs(seed, padded)`（`RaBitQQuantizer.java:90`），nlist=2915 时是 1.5MB 的重复数组；`center` 也已经存在 payload 里（`center.clone()` 又是一份）。改成 signs 共享一份、centre 直接读 payload 偏移。

### 4.3 P2：用 header 估大小，省掉 driver 的全量 prepare

`workingSetBytes`（`DistributedIndexSearch.java:573`）为了算预算，把每个索引文件完整读一遍并 prepare（串行、每 JVM 一次，结果只进 `KNOWN_PREPARED_BYTES`）。100M × 4bit 那是 7.5GB 的纯浪费读数（1bit 是 3.2GB）。

P1 之后 `estimatedBytes` 完全由 header 决定（list 数、条目数、每条目字节、字典长度），所以这一步可以退化成“尾部读 footer + 头部读 payload header”，不需要读 blob 正文。这条可延后，但它正好与 #2024 的“按真实占用记账”是同一件事。

## 5. 风险与反例

### 5.1 拷贝链决定成败（最重要）

只有当 prepared **共享**容器里的那段字节时，常驻才会从 `payload + 1.78×payload` 变成 `payload + 目录`。如果实现时“从 payload 拷一份出来自己持有”，常驻变成 `容器 + 拷贝 = 2×`，只降 28%——**低于本卡 30% 的验收线**。所以 P0 不是可选项，而是 P1 收益的前提；评审时要按这个顺序卡。

### 5.2 记账口径必须跟着改

`PreparedSearch.estimatedBytes()` 的语义是“除了调用方已经记账的 payload 之外，prepared 还额外 retains 多少”。这条不变式要写进 javadoc 并用测试卡住：

- 共享：`estimatedBytes ≈ 目录`（KB 级），`resident = payload.length + 目录`。
- 拷贝：`estimatedBytes` 必须包含那份拷贝，否则缓存会低估、把预算撑爆。

`TestVectorBuildMemory.java:239` 断言“build 时的 preparedBytes 估算 == prepare 时 `estimatedBytes()`”，两种路径都要按同一模型算。

### 5.3 别把热循环搬到 ByteBuffer／direct memory

热循环用 `byte[]` + `int` 偏移 + `VarHandle`：JIT 的边界检查消除和向量化都在 `byte[]` 上最好。`ByteBuffer` 会引入额外类型检查与调用，direct memory 还逃出堆预算（`IndexPayloadLimits` 的存在正说明“堆内 ≤2GB 就够”）。

### 5.4 非量化分支也要做，但别用 4bit 的数字验收

未量化每条目 payload 524B、结构 568B，改后降到 524B（降 52%）——收益仍然显著，但绝对字节大、结构占比小，看起来“变化不大”。验收要写绝对阈值（B/条目），不要只写比例。

### 5.5 磁盘字节 ≠ 内存字节

Puffin 容器默认压缩（`PuffinIndexFileContainer.java:62-66`），磁盘上 752MB 的文件，解压后的 payload 更大。核算“降了百分之多少”时只能用内存里的 payload 字节，拿磁盘大小会算出错误的百分比。

### 5.6 反例（什么情况下这张卡不值得）

- 单表、单索引、预算已经全装下（10M 现状 coverage=1.0）：只剩 prepare 首查与 p99 抖动的小钱。
- 若将来把 payload 搬到 mmap/direct memory 并改按页记账，本设计的“字节账”要重做。
- 若把 IVF 换成“索引文件更大、文件数更少”的布局，`sqrt(entries × files)` 变小，分配收益进一步缩小（但常驻收益不变，因为它是逐条目线性的）。

## 6. 分步落地与工作量

| 步骤 | 内容 | 估计 | 可独立合并 |
|---|---|---|---|
| P0 | blob 共享视图、`toBytes` 不再复制、`prepareFile` 传切片、`CachedFile.payload` 去留 | 1–2 天 | 是（行为零变化） |
| P1 | `IvfKernel` 原地布局 + 无分配打分 + 有界堆 + `preparedBytes` 重写 + 口径同步 | 3–5 天 | 是 |
| P2 | header 估大小，去掉 driver 侧全量 prepare | 1–2 天 | 是 |

P1 只动 `index-vector`（`IvfKernel` 814 行、`RaBitQQuantizer` 加两个重载、`PreparedStructureSize` 口径），不碰 Spark 调度；P0 会碰 core 的 `IndexBlob`／`PuffinIndexFileContainer`，是唯一需要 API 讨论的地方。

## 7. 测试与验收

正确性：

- `TestIvfKernel` 现有 recall／`searchBatch`／determinism 测试全部通过（召回不许变）。
- 新增 tie-break 测试：构造同分候选（重复向量），断言“堆实现的输出顺序 == 全量排序实现的输出顺序”，含 topK 边界上的同分。
- `TestVectorBuildMemory:239` 的 build 估算 == prepare 估算仍成立。
- `TestPreparedStructureSize` 增加 B/条目的绝对断言与模型注释（4bit ≤ 100B、1bit ≤ 60B、未量化 ≤ 560B）。

资源：

- **峰值**：给 750MB 级 payload 的 prepare 单测加 `-Xmx` 约束（如 1.2 × payload）；现状会 OOM，改后通过——这是“拷贝链真的只剩一份”的硬证据。
- **prepare 时间**：890 万条 payload 从“逐条解析”降到“读 header + 目录”，断言 ≤ 50ms 量级。
- **分配**：`ThreadMXBean.getThreadAllocatedBytes`，单次 search ≤ 100KB（现状是每条目一个对象）。

延迟（**这条要按配对测量写**）：

- 同一轮里 A/B 交替跑，报告 10M×4bit 与 100M×4bit／1bit 的中位与 p99；判据是“中位比 ≤ 1.05 且两轮符号一致”，不接受“12 次测量差 5% 以内”。理由：benchmark 报告里同一格在不同时段出现过 13.5s vs 51.7s、首查 356s 的差异，用噪声阈值判回归没有分辨力。

容量（真正的主指标）：

- 同一 100M×4bit 表、同 6×12GB 环境，`coverage` 前后对照（#2024 的探针）。**这条也是本卡的优先级关口**：如果实测 100M 已经 coverage ≥ 0.95，本卡降级为“顺手做”，先落 P0+P2。

## 8. 被否掉的备选

| 备选 | 否掉的理由 |
|---|---|
| 把 list 目录写进 payload（升 `FORMAT_VERSION`） | 目录能从 header 在 prepare 时用 `nlist` 次读算出来，代价可忽略；升版意味着所有索引重建（100M 约 40 分钟/轮），得不偿失 |
| payload 走 direct／mmap | 热循环 JIT 更差、逃出堆预算、`Integer.MAX_VALUE` 的堆内上限本来就够 |
| 只做有界堆，不动布局 | 常驻一点没降（仍是 152B/条目），只拿到 ~50ms 的延迟与 p99 抖动，不值一次改动 |
| 保留对象图但丢掉 `CachedFile.payload`（240 → 152，降 37%） | 便宜但没有终点：结构仍是每条目 152B；只在 P0 受阻时作为退路 |
| per-prepared `scratch float[3]` 复用 factors | 引入可变状态，正确性依赖“同一 prepared 不并发搜索”，将来会被静默破坏 |

## 9. 待定问题

1. **100M 的 coverage 实测**（决定 P1 优先级；用现成探针半天可测）。
2. `IndexBlob` 共享视图的 API 形态：改现有类的契约，还是在索引读路径新增内部类型（影响 revapi 与 core 的公开 API 稳定性）。
3. `PreparedSearch` 是否需要显式区分“我持有 payload”与“我不持有”，让 `IndexCacheMemory` 不必猜（当前靠 `preparedBytes <= 0` 的兜底，`ExecutorIndexCache.java:1311-1321`）。
4. 目录要不要顺手为 #2772 的 list 级 Range 读留出“每 list 的字节区间”语义（本设计的偏移差已经够，但要在命名上体现）。
