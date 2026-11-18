# IVF list 块缓存：让 range 读与缓存叠加（#3121，2026-11-18）

## 问题

整文件缓存（`ExecutorIndexCache.CachedFile`）只在 range=off 时生效：range=on 的查询根本不读整文件，走 `prepareRange` 按 list 块读，所以缓存永远 miss；而 range 路径读过的 list 块又被 `IvfListRanges` 的 per-search 缓存（8 MB / 256 条）在 task 结束时丢掉，下一次查询重新读对象存储。结果是「range 快但缓存不生效」「缓存有用但只能用在不走 range 的查询上」，两者无法叠加。

## 设计

**缓存单元是一个 list 的字节块，不是整个 list 区。** 键为 `(index file location, list 块的字节偏移, 长度)`。list 的边界在索引文件里是固定的，所以同一个 list 块在两次查询里 key 相同；块的字节内容也是固定的（索引文件不可变），不需要失效。

**位置在 executor 级缓存里，与整文件条目共享预算。** `ExecutorIndexCache` 新增一块 LRU（`listBlocks`）与计数，开关复用 `spark.iceberg.index.cache.enabled`、预算复用 `spark.iceberg.index.cache.bytes`（不新增任何配置项）。块只从**整文件占用后的剩余预算**里分配：

```
spare = budget(maxBytes) - wholeFileBytes - listBlockBytes
```

块装入挤占 spare，块淘汰腾出 spare；整文件条目不会因为块缓存被逐出。这样 range 路径的缓存不会破坏整文件缓存的语义，也不会出现两份缓存各自按全预算计费的重复占用。

**命中路径。** `IvfListRanges.read(list)` 的顺序是：per-search LRU（同一 task 内不重复读）→ 共享块缓存 → 按计划读（可能合并相邻 list）。共享缓存未命中时会读磁盘并把**这次读覆盖到的所有 list 片段**都放进去，而不只是当前请求的那一个——否则一次合并读之后，它的相邻 list 在下一个 `read()` 里还会再读一遍。

**关闭与并发。** `cache.enabled=false` 时 `openSearch` 不创建块缓存视图，行为与 #3121 之前完全一致。所有块读写都在 `ExecutorIndexCache` 现有的 `mapLock` 下；同一块被两个 task 同时 miss 时会各读一次（put 幂等，不会重复计费），这是重复 IO 而不是正确性问题。single-flight 没有做：3120 的测量显示 range=on 的 index-read 只占 6–9%，为它引入等待与超时逻辑的收益低于复杂度，留待有测量证据时再加。

**回退路径不填充。** `IvfListRanges` 在稀疏选择时会退回读整个 list 区（`wholeListAreaReads`），这条路径不往共享缓存放块：整区读本来就是一次大读，逐 list 填充的收益小于开销；回退次数由 `numIndexWholeListAreaReads` 计数。

## 指标

四个新指标（默认开，随 #3119 的四段一起进 Spark UI 与 event log）：

| 指标 | 含义 |
|---|---|
| `numIndexBlockCacheHits` | 要读的 list 块已在 executor 缓存里的次数 |
| `numIndexBlockCacheMisses` | 块未命中、从存储读的次数 |
| `numIndexBlockLoads` | 加入块缓存的 list 块数 |
| `numIndexBlockEvictions` | 按预算逐出的块数 |

命中率 = hits / (hits + misses)；命中时 `numIndexRangeBytesRead` 只剩容器 footer 与描述区。

## 代码落点

- `index-vector`：新增 `ListBlockCache`（get/put/stats）；`IvfRangeInput` 暴露 location；`IvfListRanges` 支持共享缓存（命中/回填/覆盖列表）；`IvfKernel.prepareRange` 与 `VectorIndexAdapter.prepareRange` 增加带缓存的重载；`RangeSearch.blockCacheStats()` 暴露本次搜索的命中计数。
- `index-spark`：`ExecutorIndexCache` 新增块 LRU 与预算记账、`listBlocks(config)` 视图；`DistributedIndexSearch.openSearch` 在 range 分支取视图并传入，`SearchStages` 上报四个指标。
- 测试：`TestIvfRangeRead`（跨两次 `prepareRange` 共享块、无缓存时每次读）、`TestExecutorIndexCache`（保留/预算/逐出/关闭）、`TestVectorSearchExecution`（同一 SQL 跑两次，第二次 hits > 0 且 range 读字节下降；cache=off 时 hits/loads 为 0）。

## 已知边界与未做

- 只有 range=on 的查询用块缓存；range=off 仍是整文件缓存，两者不互相迁移（不做整文件↔块的自动转换）。
- `whole-list-area` 回退不填充（见上）。
- 没有跨 executor 共享：每个 executor 各自缓存，与整文件缓存一致。
- 没有 single-flight 与预取（见上）。
- 真机验收还没跑：需要 cache=on × range=on 连续两次查询的 benchmark 数据，确认命中率、`numBytesRead`/请求数下降与 p50 改善（卡 #3121 的验收条件）。
