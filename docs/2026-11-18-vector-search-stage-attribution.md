# 向量检索的分阶段归因：时间花在读索引、打分还是回表（#3120 阶段一，2026-11-18）

## 结论

对 10.78M 行、586 个数据文件、33 个索引文件的 `lance-iceberg` 表，用 #3119 的埋点在真机 event log 上读到：

- **range=on（nprobe=8）**：fetch-back（回表）占 task 墙钟的 **88–92%**，index-read 6–9%，probe-score 2–6%，merge <0.01%；
- **range=off**：index-read 占 **55–66%**（每查询读 22.7 GB 整索引文件），fetch-back 32–43%，probe-score 2–3%。

由此得到两条可执行判断：**块级缓存（#3121）在 range=on 上的收益上限就是 index-read 的 6–9%**，它不是 12% 差距的主项；**回表重写（#3122）才是主项**。这与“猜 probe-score 高、去优化 4bit 解码/距离内核”的初始假设相反——打分只占 2–6%。

进一步看回表的成本形状：每个查询 363 个候选行，分散在 **266–277 个数据文件**里（每文件 1–2 行），回表累计 126–298 s，等于**每个文件 0.45–1.1 s、每行 0.27–0.82 s**。每个数据文件平均 142 MB，量级上更像“每打开一个文件付一次固定成本”，而不是“每解码一行付成本”。这个判断还需要 fetch-back 分段与 profile 证据确认（见“待补证据”）。

## 数据与口径

| 项 | 值 |
|---|---|
| 产品提交 | `7f6487dcc1`（index 分支，含 #3119） |
| 镜像 | `oss-bench-20261118-7f6487dcc1-3119`，digest `sha256:084ac1d7…` |
| 索引 | `runs/oss32-lb-build-2115`（`fmt-ivf-lb-4b`，list-blocks，33 文件） |
| 参数 | nprobe=8，over_sample=0，bits=4，索引缓存关，range 轴 off/on 各 2 查询 + warmup 1 |
| 样本 | 6 个索引执行（每臂 3 个），每个 33 tasks，`numCandidateRounds=33`（每个索引文件一轮够） |
| 计数 | `numCandidates=330`、`numFetchedRows=363`、`numRowsAtFiles=266–277`、`numLiveCandidates=330` |
| 对账 | 四段之和 vs `taskTimeNs`：0.001%–0.039%（#3119 验收 <5%） |

对账差异只说明这一轮测量里四段覆盖了 task 墙钟，**不是“四段严格互斥”的承诺**：回表之后的
过滤/去重会计入 merge，块缓存的锁等待会计入 probe-score，两段因此都比字面含义宽；换一个
数据形状，差异可能落在容差内但不会恒等于 0。

运行与证据目录：`/Users/jyxc-dz-0101035/workspace/iceberg-reviews/3119-stages/`（SUMMARY.md、原始 event log、attribution、results、plan、report）。该 run 的 p50 是 range=off 12,236 ms / range=on 7,514 ms，但只有 2 条查询且与他人作业并发，只作方向。

## 阶段表（33 tasks 的墙钟占比）

| 执行 | 臂 | duration ms | taskTime s | index-read | probe-score | fetch-back | merge |
|---|---|---|---|---|---|---|---|
| 8 | range=off | 17,361 | 391.6 | 65.7% | 2.0% | 32.3% | 0.00% |
| 9 | range=off | 17,008 | 304.6 | 54.6% | 2.4% | 43.0% | 0.00% |
| 10 | range=off | 11,849 | 234.9 | 54.9% | 3.0% | 42.1% | 0.00% |
| 11 | range=on | 8,930 | 201.6 | 8.9% | 3.5% | 87.6% | 0.00% |
| 12 | range=on | 13,074 | 324.0 | 5.7% | 2.3% | 92.0% | 0.00% |
| 13 | range=on | 7,148 | 147.5 | 6.0% | 6.0% | 88.0% | 0.00% |

IO 计数（同一执行）：range=off 每查询 `numIndexFileBytesRead = 22,731,539,822`（22.7 GB）；range=on 为 `numIndexRangeBytesRead = 559,868,394 / 632,189,482`、`numIndexRangeReadRequests = 392`、`numIndexListBlocksRead = 260`、整文件字节为 0。注意这一轮的 `numIndexListBlocksRead = 260` 是**当时的计数口径**（list 读的 range 请求数）；现在的口径是搜索**消费**的 list 块数（缓存命中或从存储读，每次 `IvfListRanges.read` 计 1），I/O 请求数统一看 `numIndexRangeReadRequests`，两者不要混用。cache 关闭时 `numIndexCacheMisses = 33`（range 臂也计 miss，因为 range 不回填整文件缓存，见 #3121）。

## 代码路径

- 阶段计时都在 `index-spark` 的 `DistributedIndexSearch.searchWithRows`：task 内 `openSearch`（index-read）、`CachedFile.searching`（probe-score，range 路径扣掉 list 读）、`RowReadPlan.candidatesOf` + `rowsOf`（fetch-back）、排序/refine/组装（merge）。
- 四段的边界有两处**已知小缺口**：回表之后的过滤/去重（以及 `rowsOfIndexFile` 里候选准入、`putIfAbsent` 那段）计入 merge 而不是 fetch-back；块缓存的锁等待计入 probe-score 而不是 index-read。因此四段能覆盖 task 墙钟，但不要把段名当成严格的语义抽屉。
- 回表链路：`RowReadPlan.rowsOf` → 按数据文件分组 → `IndexEntries.liveRowsAt` → `readFileWithFormat`。后者对**每个命中的数据文件**新建 `ReadBuilder`、`setAll(storageProperties)`、`positions(wanted)`、`build()`，再流式读记录；文件打开、footer、storage 属性设置都发生在每次调用里。
- `IndexEntries.readFileWithFormat` 里已经有一个只在 `-Diceberg.index.fetch-back.timings=true` 时打印的分段点（`conf+input / builder / open / read`，以及行数），它正是这次要补的证据，不需要新代码。

## 下一步与优先级

1. **先做回表（#3122）**：① 用 positions 模式（不回表）跑同一批查询，量化“完全砍掉回表”能省多少；② 打开 `iceberg.index.fetch-back.timings`，把 126–298 s 拆成 open/read；③ 若 open 占大头，方向是复用文件打开/footer（每文件只读 1–2 行，读放大不在行数而在次数）；若 read 占大头，方向是列裁剪（是否读了 2560 维向量之外的大 JSON 列）与 positions 命中率。
2. **块级缓存（#3121）降级**：range=on 的收益上限是 index-read 的 6–9%，低于回表；它在 range=off 的整文件读场景仍有意义（index-read 55–66%），但那是另一个臂的问题，不应与“12% 差距”混为一谈。
3. **JFR/async-profiler（用户安排到整体测试一起做）**：确认 fetch-back 内部是对象存储等待、文件打开还是行对象/JSON 构造；同时复核 probe-score 只有 2–6% 这一反直觉结论（range 路径的 list 读已归到 index-read，不是打分的锅）。
4. 不要先做“4bit 解码/距离内核优化”：当前证据下它的上限是 2–6%。

## 已知边界

- 这份 run 每个臂只有 2 条查询 + 1 warmup，且与他人作业并发；**占比结论比延迟数字更可信**（占比来自同一应用内的四段墙钟计时，不是跨应用比较）。
- 四段计时覆盖 task 墙钟，但有已知小缺口（回表后过滤/去重进 merge、缓存锁等待进 probe-score）；不要用这些指标断言“merge 只包含排序”或“probe-score 只包含打分”。
- `numIndexCacheHits` / `numIndexWholeListAreaReads` 为 0 时 Spark 不写 accumulable，event log 里缺失即 0。
- 本轮没有 JFR/profile 证据，fetch-back 的内部拆分是推断；第 1 步的 timings 与第 3 步的 profile 是它的验证。
