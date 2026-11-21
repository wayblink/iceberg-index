# BM25 索引 payload v2 设计（替换逐条行引用）

卡片 #3179（子卡 #3180–#3185）。本文记录 v2 磁盘布局、读路径、兼容策略、实测证据与被否方案，供实现评审与后续维护使用。

## 1. 问题

同一份 megamath 语料（10,784,907 行）上，iceberg-index 的 BM25 文本索引占 11.75 GiB（1,169 B/行），而 lance 原生倒排占 4.11 GiB（409 B/行）。#3176 把 11 个索引文件的 19 个对象逐个解析后得到确定结论：

| 成分 | 字节 | 占比 |
|---|---:|---:|
| posting 区间（每 term 一条 zlib 流） | 12,182,022,615 | 96.58% |
| 11 份分片词典 | 311,677,779 | 2.47% |
| 快照统计文件 | 119,639,190 | 0.95% |
| puffin footer + manifest | 93,491 | 0.001% |

每条 posting 平均 8.25 B。真实 posting 区间解压后做字段消融，成本归属是：**绝对行号 4.29 B（51%）+ 每条 posting 的文档长度 1.79 B（21%）+ 数据文件路径 2.53 B（30%）**。也就是说 v1 把三个"属于文档"的值写进了每一条"属于 (词, 文档)"的记录里：文档平均 137 个不同词，这些值就重复 137 次。

同时发现的约束：v1 的 136 字符路径、8 字节绝对行号、4 字节长度、4 字节 tf 全部定宽，没有 varint、没有位打包；每个 term 一条独立的 zlib 流（空区间也要 8 字节，df=1 时一条 posting 要 134 字节）。

## 2. v1 布局（仍然可读）

```
header(280B): int magic(0x424D3101) | long documents | long tokens | 65 × int bucketOffsets
dictionary:   每 term: writeUTF(term) | long df | int rangeOffset | int rangeLength
postings:     每 term 一条 zlib 流，逐条: writeUTF(dataFile) | long rowPosition | int length | int tf
```

统计文件复用同一布局、postings 段为空。

## 3. v2 布局

所有整数 big-endian（与 v1 的 `DataOutputStream` 一致）。magic 里的版本号使读者用一个 int 分派。

```
header (308 B)
  int    magic                   0x424D3201
  long   documents
  long   tokens                  总词数，用于 avgdl
  int    fileCount
  int    chunkCount
  int    chunkDocs               每块文档数（当前 4096）
  int    filesLength             文件表字节数
  int    docDirectoryLength      chunkCount * 4
  int    dictionaryStart         绝对偏移
  int    postingsStart           绝对偏移
  int[65] bucketOffsets          绝对偏移，bucketOffsets[64] == postingsStart

file table        fileCount × (varint 字节长 + UTF-8 路径)
doc table dir     chunkCount × int（相对 chunk 段起点）
doc table chunks  每块 = zlib( 逐文档: varint fileId | varint rowPosition | varint length )
dictionary        64 桶；桶内按 term 排序；每项:
                    varint sharedPrefixLength
                    varint suffixLength
                    bytes  suffix
                    varint documentFrequency
                    varint rangeOffsetDelta   桶内单调递增，相对上一项
                    varint rangeLength
postings          每 term 一条 zlib 流，逐条: varint docIdDelta | varint tf
                  首条 posting 写 docId 绝对值
```

统计文件复用同一布局（`fileCount=0`、`chunkCount=0`、postings 段为空），所以读者只需要一套格式。

### 3.1 为什么行引用进"文档表"而不是内联在 posting 里

第一版方案是 posting 内联 `(fileId delta, 行号 delta)`、只把长度移进分块表。放弃它的原因是**顺序假设**：fileId delta 只有在 entries 严格按 `(dataFile, rowPosition)` 排序时才非负，而这个顺序是构建侧的实现细节，不是格式契约；一旦不成立，负 delta 会被写成巨大的 varint 而静默错位。文档表方案把 `(fileId, rowPosition, length)` 收敛成"每个文档一条"，posting 只剩 `(docId delta, tf)`，既不依赖排序，也更小（外推每条 posting 从 3.37 B 降到约 1.0–1.6 B）。

### 3.2 读路径

- 查询只需要它的词：读 header（308 B）→ 读这些词所在桶的字典段（最多 64 段中的命中桶）→ 读每个 term 的 posting 区间。
- 文档表的块偏移目录一次读入（chunkCount × 4 B，约 1 KB/百万文档），之后**只读命中文档所在的块**。稀疏词的查询只碰 1–2 个块，常见词的查询可能碰完所有块（约 4 MB/百万文档），但仍小于 v1 为同一批 posting 内联路径所需读的字节。
- 读分片时不在内存里按文档数建表：各 term 的 docId 列表本身升序，先合并成升序命中表，按块解码时用指针在合并表上前进（#3186）。200,000 文档分片上单词项查询的一次读分配约 206 KB，而按文档数建定位数组仅数组本身就是 800 KB。
- 打分与 v1 完全共用：v2 的 reader 与 v1 的 reader 都实现包私有接口 `TextIndexShard`（`counts(terms)` / `shard(terms, statisticsOnly)`），adapter 的 `scoreCandidates`/`mergeStatistics`/`resultComparator` 一行未改，因此分数与同分排序在构造上相同。

### 3.3 兼容策略

- 写：`auxiliaryBlobType()` 返回 `iceberg-text-bm25-v2`。
- 读：`readableAuxiliaryBlobTypes()` 返回 `{iceberg-text-bm25-v1, iceberg-text-bm25-v2}`，`IndexReader.readable()` 按这个集合过滤（引擎无需改动）；payload 的 magic 决定用哪个 reader。
- 因此旧索引继续可回答；同一快照里 v1 与 v2 文件可以共存（refresh 只重写受影响的文件时就会这样）；`CheckIndexesProcedure.readablePayload` 也按 adapter 的集合判断，v1 仍算可读。
- 统计文件是裸 payload（无 puffin 类型），同样按 magic 分派。
- 驻留记录的 `readerId` 保持 `text/bm25/1` 不变：同一个 reader 解两种布局，改名会让既有 v1 索引的驻留记录全部失配（升级后静默退回整文件读）。

## 4. 实测证据（合成语料，确定性）

2 万文档、4 分片 × 5 千文档、598.4 tokens/文档、137 postings/文档、123 字符路径、词形均值 11.90 字符（对标 #3176 的语料参数）：

| | v1 | v2 | 比值 |
|---|---:|---:|---:|
| 字节/行 | 5,557.8 | 957.4 | 0.172 |
| posting 字节 | 94,245,308 | 9,878,137 | — |
| 词典字节 | 16,909,954 | 9,194,996 | 0.544 |
| 文档表 | — | 70,877 | — |

v2 内部构成：posting ranges 51.6%、**词典 48.0%**、文档表 0.4%、文件表 0.01%。**词典已经是下一轮的最大头**。

正确性：20 条查询（df 从 20,000 递减到 1）的 top-100 在 `(dataFile, rowPosition, score)` 上与 v1 **逐位一致**（score 用 `doubleToLongBits` 比较），顺序一致；分片统计合并后与生成语料的 documents/tokens/逐 term df 全等；损坏 payload 的处理：坏 magic、截断、桶偏移越界、range 越界、chunk 越界、zlib 损坏、非法 varint 都抛 `IOException`；尾随垃圾字节被忽略（见 §5），这是有意的语义。

注意口径：上面的 0.172 是"5 千文档/分片"的小分片数字（分片内本地 df 偏小，v1 每条 posting 被路径放大到 34.4 B）。把探针的各 df 档 v2/v1 比值作用到 #3176 实测的生产各档 v1 字节上，外推生产约 **1.87 GiB（187 B/行，6.3×）**；若 posting 成本比探针差 1.5 倍则 2.68 GiB（4.5×）。区间 **1.9–2.7 GiB**，真实值必须由 10M 集群复测（#3185）钉死。

## 5. 被否方案

| 方案 | 否掉的原因 |
|---|---|
| posting 内联 `(fileId, rowPos)` + 分块长度表 | fileId delta 依赖 entries 的 `(file,row)` 排序，负 delta 会静默错位；实测也比文档表方案大 |
| 直接用 lance 的 Rust 倒排实现 | 它不是可独立使用的库：读写绑定 lance dataset/segment/fragment 模型，Java 侧只有 `Dataset.createIndex` 与 `FtsQuery` 两个 dataset 级入口；引入要带 189 MB JNI 原生库、`lance-core/table/io/datafusion/arrow 58` 依赖树，并把索引格式的所有权交给上游（v1→v2→v3→v4 一年三变） |
| 换 Lucene | 本地目录 + mmap 形态，对象存储上要整段下载；段/commit 语义还要和 Iceberg 快照绑定另做一层。它是"自研成本失控"时的后备候选 |
| 词典用 writeUTF（v1 做法） | 64 KB 上限；v2 改 varint 长度 + UTF-8，顺带把短词的长度字段从 2 字节降到 1 |
| 尾随垃圾字节校验 | 需要新 header 字段或在 open 时读完整词典（查询热路径变贵）；Puffin 的 blob 长度是权威长度，多出的字节只可能是写端损坏。保持"忽略"并在测试里钉住 |

## 6. 顺带修掉的既有缺陷

`TextIndexAdapter.encodeTokens` 用 `writeShort` 存 token 长度：**超过 65535 字节的 token 会回读成另一个词**——构建成功、查询永远 miss，属于静默答错。entry payload 只在构建任务内流转（文本索引 `encodeEntries` 直接抛异常，落盘只有 auxiliary 文件），所以把长度改成 32 位不改变任何盘上契约；解码侧同时加了 token 数与长度的越界护栏。

## 7. 风险与未验证项

1. **10M 生产 B/行未实测**——外推 1.9–2.7 GiB，只有 #3185 能钉死。
2. **词典占比 48%**：前端编码已做，但没有按 token id 去重/共享后缀；4M 词表的硬成本仍在。
3. **分块大小 4096** 是稀疏查询放大与压缩率之间的折中：df=1 的一次查询要读整块（约 3.5 B/文档 × 4096 ≈ 14 KB 解压后更小），换来目录更小、压缩更好。没有做成可调参数（新增用户参数需要单独批准）。
4. **v1 与 v2 并存的快照**：单测已覆盖"两种 payload 一起查询时答案不丢行"和"`CheckIndexesProcedure` 两种布局都可读"；集群侧的混合快照未构造。
5. 改格式需要重建旧索引才能拿到空间收益；不重建也可读可查，只是继续占 v1 的体积。

## 8. 验收口径

- 单元/等价：`./gradlew :iceberg-index-text:test --no-build-cache`（该仓 `org.gradle.caching=true`，不加会命中缓存并残留旧 XML）。
- 质量：`:iceberg-index-text:spotlessCheck`；该模块 checkstyle 基线即为红（`TextIndexPayload.java:55` 等既有项），本次新增文件 0 违规。
- 集群（#3185）：沿用 #3174 口径，10,784,907 行、k=10、20 查询 × 5 次、12 executors × 4c/16g + 12g driver、两侧不并发；对照 v1 的 619.4 s / 12,613,433,075 B，并要求 top-10 与 v1 逐条一致。
