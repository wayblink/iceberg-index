# IVF list 块缓存：让 range 读与缓存叠加（#3121，2026-11-18）

## 问题

整文件缓存（`ExecutorIndexCache.CachedFile`）只在 range=off 时生效：range=on 的查询根本不读整文件，走 `prepareRange` 按 list 块读，所以缓存永远 miss；而 range 路径读过的 list 块又被 `IvfListRanges` 的 per-search 缓存（8 MB / 256 条）在 task 结束时丢掉，下一次查询重新读对象存储。结果是「range 快但缓存不生效」「缓存有用但只能用在不走 range 的查询上」，两者无法叠加。

## 设计

**缓存单元是一个 list 的字节块，不是整个 list 区。** 键为 `(index file location, list 块的字节偏移, 长度)`。list 的边界在索引文件里是固定的，所以同一个 list 块在两次查询里 key 相同；块的字节内容也是固定的（索引文件不可变），不需要失效。

**位置在 executor 级缓存里，与整文件条目共享同一份预算。** `ExecutorIndexCache` 新增一块 LRU（`listBlocks`）与计数，开关复用 `spark.iceberg.index.cache.enabled`、预算复用 `spark.iceberg.index.cache.bytes`（不新增任何配置项）。约束是**双向**的：

- 块只从**整文件占用后的剩余预算**里装入：`spare = budget(maxBytes) - wholeFileBytes - listBlockBytes`；块淘汰腾出 spare。
- 整文件装入前若剩余预算不够，会先按 LRU 回收块让这个文件放得下，再走原有的整文件逐出逻辑。

所以块缓存不能把整文件缓存挤出预算，整文件缓存也不会被块缓存无限挤压：两边按同一份预算、各自的 LRU 记账，不会出现两份缓存各自按全预算计费的重复占用。块占用同时计入 executor 上报的常驻字节（`index_cache_status` 的 `bytes` 是整文件 resident bytes 与 list 块字节之和），因此 executor 预算绑定与状态/告警看到的是两者合计。

**命中路径。** `IvfListRanges.read(list)` 的顺序是：per-search LRU（同一 task 内不重复读）→ 共享块缓存 → 按计划读（可能合并相邻 list）。共享缓存未命中时会读磁盘并把**这次读覆盖到的所有 list 片段**都放进去，而不只是当前请求的那一个——否则一次合并读之后，它的相邻 list 在下一个 `read()` 里还会再读一遍。读一次 list 就算“消费一个块”（无论来自块缓存还是存储），由 `numIndexListBlocksRead` 计数。

**关闭与并发。** `cache.enabled=false` 时 `openSearch` 不创建块缓存视图，行为与 #3121 之前完全一致。所有块读写都在 `ExecutorIndexCache` 现有的 `mapLock` 下；同一块被两个 task 同时 miss 时会各读一次（put 幂等，不会重复计费），这是重复 IO 而不是正确性问题。single-flight 没有做：3120 的测量显示 range=on 的 index-read 只占 6–9%，为它引入等待与超时逻辑的收益低于复杂度，留待有测量证据时再加。

**生命周期。** 块记录最后一次访问时间，空闲超过 `spark.iceberg.index.cache.idle-ttl` 的块会被 sweep 逐出；和整文件条目的空闲淘汰一样，这种逐出不增加预算逐出计数。`releaseIndex` / `releaseCopy` / 目标集合变化（`applyTarget` 里被移除的旧 key）释放索引文件时，会同时清掉该文件 location 下的块；仍然被其它目标瞄准的文件的块不动。`clear()` 一并清空块与时间戳。`offload_index` 也按索引 ID 清理仅由范围查询填充的块，并阻止已打开的旧范围读取视图在卸载后回填。后续查询仍可重新预热。块写入计算可用容量时也会扣除整文件加载已经预留的字节。

**回退路径不填充，但先查共享缓存。** `IvfListRanges` 在选中的范围**密集**（未读部分已占整个 list 区的 3/4 以上，逐段读并不比整区读省）或**散扩**（选中的范围分散成多个不连续段、且待读字节已接近整个 list 区）时，会退回一次读整个 list 区（`wholeListAreaReads`）。规划 missingBytes 之前会先查共享块缓存：已经在缓存里的 list 不算 missing，所以一个全热的密集探测可以完全不读存储、也不会触发回退。回退本身仍然不往共享缓存放块：整区读是一次大读，逐 list 填充的收益小于开销。回退次数由 `numIndexWholeListAreaReads` 计数。

## 指标

四个新指标（默认开，随 #3119 的四段一起进 Spark UI 与 event log）。它们都是 **per-search** 的：每次搜索（`openSearch`）建一个自己的块缓存视图，视图里的计数只属于这次搜索，task 可以把它打开的索引文件的计数相加；不是 executor 生命周期的累计。

| 指标 | 含义 |
|---|---|
| `numIndexBlockCacheHits` | 本次搜索要读的 list 块已在共享缓存里的次数 |
| `numIndexBlockCacheMisses` | 本次搜索中块未命中、需要从存储读的次数 |
| `numIndexBlockLoads` | 本次搜索中**真正进入**共享缓存的 list 块数 |
| `numIndexBlockEvictions` | 本次搜索装入时按预算逐出的块数（per-search；空闲 TTL 淘汰不计入） |

list 块消费数另由 `numIndexListBlocksRead` 计：缓存命中或从存储读都算，每次 `IvfListRanges.read` 计 1，它**不是** range 读请求数（请求数看 `numIndexRangeReadRequests`）。命中率 = hits / (hits + misses)；命中时 `numIndexRangeBytesRead` 只剩容器 footer 与描述区。

## 代码落点

- `index-vector`：新增 `ListBlockCache`（get/put/contains/stats，`Stats` 是 per-view 的）；`IvfRangeInput` 暴露 location；`IvfListRanges` 支持共享缓存（命中/回填/覆盖列表、`listsRead` 计数、规划前用 `contains` 查缓存）；`IvfKernel.prepareRange` 与 `VectorIndexAdapter.prepareRange` 增加带缓存的重载；`RangeSearch.blockCacheStats()` 与 `listBlocksRead()` 暴露本次搜索的计数。
- `index-spark`：`ExecutorIndexCache` 新增块 LRU、最后访问时间戳、双向预算记账（块装入占 spare、整文件装入回收块）、释放时清理块，`listBlocks(config)` 返回 per-search 视图；`DistributedIndexSearch.openSearch` 在 range 分支取视图并传入，`SearchStages` 上报四个指标与 `numIndexListBlocksRead`。
- 测试：`TestIvfRangeRead`（跨两次 `prepareRange` 共享块、无缓存时每次读、全热密集探测不回退）、`TestExecutorIndexCache`（保留/预算双向/逐出/关闭/释放清块/空闲 TTL/per-view 计数）、`TestVectorSearchExecution`（同一 SQL 跑两次，第二次 hits > 0 且 range 读字节下降；cache=off 时 hits/loads 为 0）。

## 真机实测（2026-11-18，run oss32-blocks-1118-1945）

同一个 range=on 查询连续跑 5 次（2 查询 × 2 次重复 + warmup，nprobe=8，cache 预算 22 GiB，33 tasks/次）：

| 执行 | 顺序 | hits | misses | range 读字节 | 请求数 |
|---|---|---|---|---|---|
| 9 | 冷 | 0 | 259 | 558,911,130 | 391 |
| 10 | 热 | 16 | 244 | 540,675,242 | 376 |
| 11 | 热 | 72 | 187 | 462,594,274 | 319 |
| 14 | 冷 | 0 | 260 | 572,229,946 | 392 |
| 15 | 热 | 40 | 221 | 512,167,306 | 353 |

表中的 hits/misses 是每次执行里所有 task 的求和（per-search 口径，一个 task 加总它打开的索引文件）；“请求数”是 range I/O 请求数（`numIndexRangeReadRequests`），与消费的 list 块数（`numIndexListBlocksRead`）不是同一个量。**块缓存功能正确，但命中率只有 6–28%（热执行合计 16.4%，全轮 9.9%）**，range 读字节降 3–19%。原因不是缓存实现，而是调度：每个 task 处理一个索引文件，块缓存在 executor 本地，而第二次查询的同一文件 task 是否落回同一个 executor 由调度决定（12 个 executor、33 个文件，约 1/12 的概率），且不同查询选中的 list 只部分重叠。整文件缓存有 `IndexCacheLocations` 把 executor 持有的 key 上报给 driver 参与 locality；**块缓存没有接这条上报**，所以调度层看不见它。

这与 #3120 的归因一致：range=on 时 index-read 只占 6–9%，块缓存最多省掉这一段的存储读。要让命中率接近理论上限，需要把块缓存内容接进 `IndexCacheLocations` 的 executor 上报与 task locality（独立工作量）；在那之前，本卡的收益是"可测但小"，优先级低于回表重写（#3122）。

## 已知边界与未做

- 只有 range=on 的查询用块缓存；range=off 仍是整文件缓存，两者不互相迁移（不做整文件↔块的自动转换）。
- 块与整文件条目共享同一预算：块装入只用整文件占用后的 spare，整文件装入前会先回收块；块计入 `index_cache_status` 上报的常驻字节，`offload_index` / 释放索引会清块，空闲 TTL 会逐出块（见“设计”）。
- `whole-list-area` 回退不填充；但回退前会查共享缓存，全热的探测不读存储也不会回退（见上）。
- 没有跨 executor 共享：每个 executor 各自缓存，与整文件缓存一致。
- 没有接 `IndexCacheLocations` 的 locality 上报，这是命中率低的主因（见实测）。
- 没有 single-flight 与预取（见上）。
- 真机验收已完成（命中率/字节/请求数有数据，p50 方向性改善），但卡里"7.8 s 压到 7 s 以下"的目标没有成立：本次 run 与那次不可比（资源/并发不同），且收益上限受 index-read 占比约束。
