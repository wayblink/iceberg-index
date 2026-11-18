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

# 冷查询减少存储请求与任务内存

本文是[向量查询性能优化路线图](../../2026-11-17-vector-query-performance-roadmap.md) §2 的设计，性能背景与测量口径见
[向量查询性能诊断](../../2026-11-17-vector-query-performance-analysis.md)，当前索引布局见
[IVF 单文件分块与按范围读取](2026-11-17-ivf-list-range-read-design.md)。

状态：**设计已实现第一步并自测**。本文描述现状、改动与测量方法；生产规模（10.78M 行 / 586 个索引文件）的
HDFS 与对象存储数字**尚未实测**，文中推算值均标注「推算」。

## 1. 目标与边界

冷查询（执行器整索引缓存未命中）每个索引文件至少要做两类请求：确定文件长度、读取文件内容。索引文件按源数据
文件分组，一次查询要打开全部 586 个文件，所以「每个文件多一个请求」就是每次查询多 586 个请求。
本设计只做三件事：

- **已知长度**：把索引 manifest 已记录的文件长度传到 `FileIO.newInputFile(location, length)` 与 Puffin
  读取器，避免为取长度发 HEAD。缺失长度的历史索引保持现有行为。
- **前缀复用**：range 读取时把已读的 12 字节块前缀并入描述区，不再重复读同一字节范围；打开容器 footer
  所需的长度由 manifest 提供，不再为定位尾部再问一次文件长度。
- **可测量的 I/O**：按 blob 区域（容器 footer / IVF 描述区 / 列表）统计请求与字节，按任务可读；相邻列表
  合并的间隔阈值做成包内可测常量。

不在本设计内的：索引格式的字节布局（不变）、`nprobe`/`over` 语义（不变）、新增用户参数（不加）、
质心全局化与列表分片（路线图 §4 的 P1，另立项）。

## 2. 现状：打开一个索引文件的请求与字节

一个 Puffin 索引文件由三部分组成：4 字节前导 magic、IVF blob（描述区 + 列表区）、footer。
`PuffinReader` 用「文件长度 − footer 结构长度」定位 footer 结构（12 字节），再读整个 footer，
然后按 blob 偏移读 blob。查询侧有两条打开路径（`DistributedIndexSearch.openSearch`）：

```text
整读路径（rangeRead=false，或缓存未命中且文件不是分块格式）
  sizeHint:                   newInputFile(location).getLength()   → 1 次 HEAD
  loadFileState:
    newInputFile(location).getLength()                            → 1 次 HEAD
    prepareFile → PuffinReader 构造器: inputFile.getLength()      → 1 次 HEAD
    PuffinReader.fileMetadata(): 尾部 12 字节 + 整个 footer       → 2 次读
    PuffinReader.readAll(): 按偏移读 blob                          → 1 次读
选读路径（rangeRead=true 且 blob 未压缩、分块格式）
  PuffinReader 构造器: inputFile.getLength()                      → 1 次 HEAD
  fileMetadata(): 尾部 12 字节 + 整个 footer                      → 2 次读
  prepareRange: file.getLength()                                  → 1 次 HEAD
  IvfKernel.prepareRange:
    read(0, 12) 前缀                                              → 1 次读
    read(0, metadataEnd) 描述区（含前缀 12 字节）                 → 1 次读
  搜索: 每个被探测列表一个范围读（相邻的合并）                    → nprobe 次读上下
```

以产品测量（[诊断](../../2026-11-17-vector-query-performance-analysis.md) §2：586 个文件、`nprobe=32`）推算，
每次冷查询：

| 路径 | 长度探测（HEAD） | 打开读请求 | 打开 + 列表字节 |
| --- | ---: | ---: | ---: |
| 整读，未记录长度（改动前） | 586 × 3 = 1,758 | 586 × 3 = 1,758 | ≈ 39.4 MB/文件，整表 ≈ 23 GB |
| 选读，未记录长度（改动前） | 586 × 2 = 1,172 | 586 × 4 + 列表 | 描述区 ≈ 1.39 MB + 32 个列表 ≈ 8.95 MB/文件 |
| 选读，已记录长度（改动后） | 0 | 586 × 4 + 列表 | 描述区（少 12 B）+ 列表 |

另有重复读取：

1. **range 打开的前缀重复**：`read(0, 12)` 与随后的 `read(0, metadataEnd)` 重叠 12 字节，即每文件多 12 字节。
   请求数不变（2 次），字节数多 12。
2. **Puffin footer 结构重复**：`PuffinReader` 先读尾部 12 字节，再读包含这 12 字节的整个 footer，每文件多 12 字节。
   这段逻辑在 core，不在本次改动范围（见 §7）。
3. **range 回退重复**：range 开关打开但文件不是分块格式时，`prepareRange` 读完 footer 返回 null，整读路径再读一遍
   footer。只有「定义声明分块但存在旧文件」时才发生。

## 3. 设计

### 3.1 已知长度：从 manifest 到 `newInputFile`

长度来自 `IndexFile.memory().fileBytes()`：构建完成写入 blob 后立刻取一次长度
（`IndexSnapshotWriter` 与 `SparkIndexBuilder`），随 manifest 的尾部可选段发布
（`IndexManifest.write`）。这是已有字段，**不需要给 `IndexFile` 加字段，也不改索引格式字节布局**；
旧 manifest 没有 memory 段，读出 `memory() == null`，长度视为未知。

传递链：

```text
IndexFile.memory().fileBytes()            （缺失 → 0，未知）
  → IndexFileTask(location, entryCount, fileLength, tasks)   （Scala，随任务序列化）
  → DistributedIndexSearch.openSearch(..., knownLength)
      → inputFile(io, location, knownLength) = io.newInputFile(location, length)
      → sizeHint / loadFileState 直接用长度，不再 getLength()
      → Puffin.read(input).withFileSize(length)              （不再让 PuffinReader 定位尾部）
      → VectorIndexAdapter.prepareRange(input, knownLength)  （range 路径同样用已知长度校验 blob 范围）
```

`S3FileIO.newInputFile(path, length)` 返回的 `S3InputFile` 在已知长度时 `getLength()` 不发请求；其它
`FileIO` 的默认实现忽略长度，所以代码里对已知长度**显式短路**，不依赖实现。`loadFileState` 与
`VectorIndexAdapter.prepareRange` 在收到已知长度时都不再调用 `getLength()`。

缺失长度时保持现有行为，并顺手去掉一次重复探测：range 路径改为先取一次长度，再用
`Puffin.read(...).withFileSize(length)`，探测从 2 次降为 1 次；整读路径仍是 3 次（预留提示、payload 上限、
footer 定位各一次），因为整读路径的预留提示必须先于读取。

已知长度的收益（每索引文件）：

| 路径 | 改动前探测 | 改动后（已知长度） | 改动后（缺失长度） |
| --- | ---: | ---: | ---: |
| 整读 | 3 | 0 | 3 |
| 选读 | 2 | 0 | 1 |

按 586 个文件推算，一次冷查询少发 1,758（整读）或 1,172（选读）个 HEAD。对象存储上 HEAD 与 GET 一样有请求
延迟与计费，这是本设计的主要收益；字节数基本不变（整读仍是整个 payload，选读仍是描述区 + 列表）。

**信任边界**：manifest 记的长度与数据文件长度同级，是发布时写入的权威元数据。长度错误会让 footer 定位失败
（Puffin 的 magic/长度校验会报错），不会静默读错内容。索引文件不可变，替换文件即发布新 manifest，不存在
「同一路径长度变化」的正常路径。

### 3.2 前缀与尾部字节复用

- **前缀复用**：`IvfKernel.prepareRange` 先读 12 字节前缀判断是否分块格式、解析出 `metadataEnd`，然后把
  剩余 `metadataEnd − 12` 字节直接读入描述区数组的对应位置，前缀字节原样复用。描述区请求仍是 2 次（前缀
  1 次、其余 1 次），字节数从 `metadataEnd + 12` 降为 `metadataEnd`。
- **尾部复用**：定位 footer 需要文件长度；已知长度时这一项不再读文件（§3.1），未知时只探测一次而不是两次。
  读到的 footer 元数据本来就被用来定位 blob（`blob.offset()/length()`），描述区与列表读取只落在 blob 内，
  不重复读 footer 字节。
- **缓存有界**：列表范围缓存仍是每任务 8 MiB / 256 个范围（`IvfListRanges`），描述区数组按 `metadataEnd`
  一次性分配（与改动前相同），不新增常驻状态；`OpenReadMetrics` 只是计数器，不持有字节。

### 3.3 相邻与近邻列表合并

现状（`IvfListRanges.planInReadOrder`）只合并**首尾相接**的选中列表，并在两种稠密情形整段读列表区：

- 待读字节占列表区 ≥ 75% 时整读列表区（`mostlyUnread`）；
- 扩探时已读字节 ≥ 75% 且仍缺 ≥ 50%、且计划范围 ≥ 4 段时整读（`scatteredExpansion`）。

新增的是**带间隔的合并**：相邻两段之间若间隔不超过阈值，就作为一段读，间隔字节读入但不评分。阈值是包内
常量 `IvfListRanges.MAX_MERGE_GAP_BYTES`，并有包内构造函数供测量使用：

| 常量 | 默认值 | 含义 |
| --- | ---: | --- |
| `MAX_MERGE_GAP_BYTES` | 0 | 0 表示只合并相接范围，与改动前行为一致 |

默认值取 0 的原因：合并是「用字节换请求」，而两者在目标存储上的价格不同。一次列表读约 273 KiB（§4），
S3 上一次额外 GET 的延迟约 30–50 ms；把间隔 g 并进来相当于多读 g 字节、少一次请求，值得合并的条件约是

```text
g < 请求延迟 × 单流带宽 / 并发修正
```

S3 上按 40 ms、100 MB/s 估，盈亏平衡点约 4 MB；HDFS 上按 1–2 ms、200 MB/s 估约 200–400 KB。真实值受任务并发
共享带宽影响，必须先实测（§5），所以默认不改变现有行为。合并段仍受 8 MiB 上限约束（`MAX_CACHED_BYTES`），
不会一次读入超大区间。相邻合并已在基线中由 `8207199a9`（本分支 `index`，非 apache/iceberg 仓库）实现，本设计只补间隔阈值。

### 3.4 读取统计

已知长度不只用于 `newInputFile`，也传给 `PuffinIndexFileContainer.read(input, size)`，由后者交给
`PuffinReader.withFileSize`：即使 FileIO 忽略长度提示，整读也不会再为 footer 取长度。测试用
“忽略长度提示的 FileIO”锁定了这一点。

`VectorIndexAdapter.RangeSearch.openReadMetrics()` 返回一次打开的
`VectorIndexAdapter.OpenReadMetrics`，按区域分类：

| 字段 | 含义 |
| --- | --- |
| `opens` | 本次打开覆盖的索引文件数（1） |
| `lengthRequests` | 长度探测次数；已知长度时为 0 |
| `footerBytes` / `footerRequests` | 定位 payload 所需的容器 footer 字节 / 请求 |
| `descriptionBytes` / `descriptionRequests` | IVF 描述区（格式标记、质心、列表目录）字节 / 请求 |
| `listBytes` / `listRequests` | 选中列表（含为合并而读的间隔）字节 / 请求 |

`bytes()` 是三者之和，`requests()` 不含长度探测。整读路径的请求由计数 `FileIO` 直接观察（见 §4）。
`DistributedIndexSearch.logReadTimes` 在 debug 级别按文件打印这些计数；`RangeSearch.listReadMetrics()`
保持原语义（仅列表，且含耗时），既有调用点与测试不变。

## 4. 本地 I/O 对照方法与实测

用计数 `InputFile` / `FileIO` 在内存或本地文件上复现请求序列，不依赖存储实现：

- `TestIvfRangeOpenReads`（index-vector）：对同一个分块 IVF 文件比较「已知长度打开」与「未知长度打开」。
  断言已知长度时 `getLength()` 调用为 0；描述区字节恰为 `metadataEnd`（前缀未被二次读取）；footer +
  描述区 + 列表的字节与计数文件实际读取的字节逐项相等；选读结果与整读 `prepareAuxiliary` 的结果逐项一致。
- `TestIvfRangeRead`（index-vector）：既有选读/整读一致性、目录越界、块截断用例；新增间隔合并用例：
  4 KiB 间隔、选中列表 `{0, 2}` 时，阈值 4 KiB 合并为 1 次读 12 KiB，默认阈值保持 2 次读 8 KiB。
- `TestDistributedIndexSearchColdOpen`（index-spark）：计数 `FileIO` 区分「未知长度」的 HEAD 与「已知长度」的
  无请求取值。断言整读路径 3 次探测 → 0、选读路径 1 次探测 → 0（改动前为 2），并用同一查询对比两条路径的
  命中结果逐项一致。

本机（合成数据：2 维、`nlist=8`、64 条记录、每个列表 8 条）实测：

| 路径 | 长度探测 | footer 字节/读 | 描述区 字节/读 | 列表 字节/读 | 合计 字节/读 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 选读，改动前（未知长度） | 2 | 197 / 2 | 187 / 2 | 124 / 1 | 508 / 5 |
| 选读，缺失长度（改动后） | 1 | 197 / 2 | 175 / 2 | 124 / 1 | 496 / 5 |
| 选读，已知长度（改动后） | 0 | 197 / 2 | 175 / 2 | 124 / 1 | 496 / 5 |

整读路径的 3 次探测在已知长度后降为 0，读取仍是 3 次共 1,684 字节（footer 结构 12 字节、footer 185 字节、
blob 1,487 字节），与改动前一致。

（文件 1,676 字节，描述区 175 字节，列表区 1,312 字节。）

按产品布局推算（2,560 维、每组 `nlist=136`、每列表 135 行、4-bit RaBitQ、`nprobe=32`）：描述区约 1.39 MB
（质心占 1,392,640 字节），单列表约 273 KiB（2,072 字节/条），32 个列表约 8.54 MB，整文件 blob 约 37.6 MiB。
即选读每文件读约 9.9 MiB、36 次请求；整读每文件读约 37.6 MiB、3 次请求，并额外承担 3 次长度探测。两者请求数
相差一个量级，这就是 §3.3 阈值要按目标存储定价的原因：`nprobe` 继续增大时 `mostlyUnread` 会切到整列表区
一次读，稀疏时列表读次数由合并阈值决定。

## 5. HDFS 与 S3 的测量口径

固定索引快照、`nprobe`、过滤条件与 Spark 资源，关闭执行器整索引缓存（`cache_budget_bytes=0`）确认冷读，
每臂至少 20 次查询，记录：

| 指标 | HDFS 口径 | S3 口径 |
| --- | --- | --- |
| 请求数 | NameNode 审计日志的 `getFileStatus`（HEAD 等价）与 `open` 次数；DataNode 读块数 | 存储侧访问日志按路径前缀统计 `HEAD` / `GET` 数与 `GET` 字节 |
| 客户端读字节 | `FileSystem` 读统计 / `ReadStatistics` 字节 | `S3FileIO` 的 `readBytes` 计数（`MetricsContext`）或访问日志 `bytes-sent` |
| 冷 p50/p95 | 每臂 20+ 次查询的墙钟分位数（含任务调度与回表） | 同左 |
| 峰值堆 | executor GC 日志 + Spark `ExecutorMetrics.JVMHeapMemory` 峰值；固定 task 并发度 | 同左 |

对照矩阵：

| 臂 | 读法 | 长度 | 间隔阈值 |
| --- | --- | --- | --- |
| 1 | 整读 | 未知 | — |
| 2 | 整读 | 已知（manifest） | — |
| 3 | 选读 | 已知 | 0 |
| 4 | 选读 | 已知 | 64 KiB |
| 5 | 选读 | 已知 | 1 MiB |
| 6 | 选读 | 未知 | 0（回退对照） |

每个臂报告：请求数（HEAD/GET 分开）、客户端字节、冷 p50/p95、executor 峰值堆与 GC 暂停；同时确认 recall@K
与基线一致（本设计不改评分或探测集合，任何差异都应视为缺陷而不是收益）。

## 6. 任务内存

本设计**不增加**常驻状态：

- 描述区、footer、列表都是打开或搜索期间的临时分配，生命周期与改动前相同；列表缓存仍是有界的 8 MiB / 256 段。
- 已知长度让缓存预留（`sizeHint`）在打开前拿到准确下界，避免把整个预算预留给一个文件、让同一查询的其它
  加载排队；这不改变峰值堆的量级，但缩短单文件加载占住缓存的时间。
- 选读路径本来就不物化整个 payload，任务堆与文件大小解耦；整读回退与整索引缓存仍按现有预算管理，本次不做
  新的内存策略。

## 7. 已知未完成项与风险

1. **footer 结构的 12 字节重复读**在 core 的 `PuffinReader` 内（先读尾部结构、再读整个 footer）。改动它影响
   所有 Puffin 使用者，未纳入本设计；每文件每打开多 12 字节。
2. **range 回退重复 footer**：range 开关打开但文件非分块格式时，footer 被读两次（`prepareRange` 一次、整读
   加载一次）。只在「定义已声明分块、但目录里还有旧文件」时发生；未合并两条路径，避免把整读加载改成范围
   读取或让 range 路径持有整读缓存职责。
3. **文本索引只覆盖分片打开**：`searchText` 的分片读取已用 manifest 长度（`newInputFile(location, length)`），
   但 `mergeTextStatistics` 仍按 location 列表打开文件，未传长度；文本路径的 footer 定位仍可能发 HEAD。
4. **间隔阈值默认 0**：没有目标环境实测，按路线图要求由存储请求延迟、带宽与额外字节决定，先保留与现状一致的
   行为；`IvfListRanges` 的包内构造函数与本文 §5 的矩阵是启用前的测量入口。
5. **recorded length 的可信性**：长度由发布路径写入；若外部系统改写索引文件而不发布新 manifest，读取会失败
   并报错（footer magic 校验），不会返回错误结果。这与数据文件长度字段的信任级别一致。
6. **生产规模未实测**：本文的 586 文件收益是推算；HDFS/S3 的 HEAD 延迟、列表读次数与合并阈值的真实盈亏平衡点
   需要按 §5 补测后沉淀回本文（不新增用户参数，阈值只改常量）。

## 8. 改动清单

- `index-vector`：`VectorIndexAdapter`（已知长度重载、计数 InputFile、`OpenReadMetrics`）、`IvfKernel`
  （描述区前缀复用、打开成本传递）、`IvfRangeInput`（可写入目标数组的范围读）、`IvfListRanges`（间隔合并
  常量与包内参数）。
- `index-spark`：`DistributedIndexSearch`（`IndexFileTask` 携带长度、`openSearch`/`loadFileState`/
  `sizeHint` 使用已知长度、读取统计日志）、`IndexFileTask`（Scala，长度字段）、`IndexCacheReadContext`
  （预热命令携带长度）。
- 测试：`TestIvfRangeOpenReads`、`TestIvfRangeRead`（新增间隔合并用例）、
  `TestDistributedIndexSearchColdOpen`。
