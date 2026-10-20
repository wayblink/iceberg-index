# 设计：executor 侧索引缓存（#708）

> 状态：设计草案（未实现）。对应卡片 #708，依赖今天的回表/索引延迟实测。
> 目标读者：接着实现的人，以及评审者。

---

## 1. 为什么现在做

今天之前，索引侧的时间线是：

| 阶段 | 单查询 10M 耗时 | 占比 |
|---|---|---|
| plan / uncovered | 5–1320 ms | 小 |
| **索引搜索（读 payload + prepare + 遍历）** | **19.8–28.3 s** | **≈82%** |
| 回表 | 3.2–4.3 s | ≈15% |

批处理（`searchBatch`，把 prepare 的摊销从"每查询每文件"降到"每批次每文件"）已经拿到 **84x**（22320 ms/条 → 266 ms/条）。但那只是把 *prepare* 摊到 100 条查询上；**批内仍然每个文件 prepare 一次**，而交互式单查询连这个摊销都没有。

今天的回表结论把优化空间进一步收窄了：回表成本 = 命中文件数 × ~150 ms（原因未定位），**不是靠改代码能随手压掉的**。于是"重复查询"这条路的性价比凸显出来——它不依赖任何未解的成本项。

**#708 的本质**：把"索引文件的解析结果"从**每次查询的临时对象**变成**executor 上的常驻资源**。这确实让 executor 从无状态变有状态，所以当初排在最后；但今天的结论是：剩下的大头就是它。

---

## 2. 缓存什么：三层，按收益/成本排

| 层 | 缓存内容 | 省掉什么 | 难点 | 建议 |
|---|---|---|---|---|
| **L1 字节** | index 文件的 `byte[]`（Puffin 容器字节） | 每次查询的 `readPayload`（I/O + 网络） | 内存 = payload 大小 | **先做**（最简单、与 kernel 无关） |
| **L2 预处理** | `IndexAdapter.PreparedSearch`（HNSW 图/矩阵，已反序列化） | 每次查询的 `prepare`/构建 | 内存更大；需线程安全；与 kernel 生命周期绑定 | **主目标** |
| L3 向量矩阵 | kernel 内部解码后的向量（float 数组 / SQ 码） | 每次搜索的重打分 | kernel 私有，跨 kernel 不可移植 | 不做，交给 kernel 自行缓存 |

L2 的收益就是卡片里写的"4.4 s → <0.5 s"（省掉重建，只付遍历）。L1 是 L2 的前置（L2 命中时通常也需要 L1 以便重建/重试）。

**不做**：缓存 search 结果（query → hits）。理由：命中率低、失效复杂（一个 delete 提交就变）、且掩盖不了"索引侧本该快"的事实。

---

## 3. 键与失效

```
CacheKey = (indexFileLocation, contentLength, adapterFingerprint)
```

- **location**：索引文件在 Iceberg 里是不可变对象（每次写/重写产生新路径）。所以路径本身就编码了版本。
- **contentLength**：防御性校验（同路径异常重写下结论仍安全）。
- **adapterFingerprint**：同一个 index 文件可能被不同的定义/kernel 读取（例如改了 `distance` 或换 kernel）。取 `(kernel.name(), kernel.payloadFormatVersion(), definition 的关键字段)` 的哈希——用现成的 `VectorIndexDefinition` 序列化字节算 hash 即可。
- **不需要 snapshotId**：snapshot 变化只意味着出现新的 index 文件；旧文件要么被新文件替代（新 key），要么仍被引用（仍有效）。**不做主动失效**，靠 LRU 淘汰 + `contentLength` 兜底。
- **不需要 mtime**：对象存储的 mtime 不可靠，且路径不可变已足够。

---

## 4. 内存与淘汰

```
iceberg.index.cache.enabled      默认 false（先用于实验；产品化时再讨论默认值）
iceberg.index.cache.bytes        默认 512MB（或 min(25% executor memory, 2GB)）
iceberg.index.cache.maxEntry     默认 256MB（单文件超过则不缓存，直接流式处理）
```

- 记账：`payload.length`（L1）+ 条目估算（L2）。L2 的占用由 kernel 报告：
  - 给 `VectorKernel` 加 **可选** 方法 `long preparedBytes(PreparedSearch)`（default 返回 payload 长度 × 系数 1.0），或让 `PreparedSearch` 带 `estimatedBytes()`（default 方法，不破坏已有实现）。
  - **这一条必须在设计评审时确认**：不能靠猜系数，否则 OOM 的锅会落到缓存上。
- 淘汰：全局 LRU（按字节），访问即更新。并发安全用 `ConcurrentHashMap` + 单链表锁，或直接引入一个小的 LRU 实现（避免引入新依赖，手写即可）。
- **不缓存**：单个 payload 超过 `maxEntry` 的文件（大索引文件仍走原路径）。

---

## 5. 并发与生命周期

- **单飞（single-flight）**：同一 key 的加载只做一次，其余线程等待结果（`ConcurrentHashMap.computeIfAbsent` 会持锁，可能阻塞过久；用 per-key `FutureTask` 更好）。
- **线程安全**：`VectorKernel.prepare` 的契约目前是"may be searched from one thread at a time"。Spark 一个 executor 有 N 个 core → N 个 task 线程可能同时命中同一 entry。两个方案：
  1. per-entry 锁，`search` 在锁内执行（search 很快，2 core 场景可接受）；
  2. 缓存 `(key, threadId)` 的组合（每个线程一份，内存 ×cores）。
  建议 **方案 1**（内存友好），并在 kernel 契约里明确"PreparedSearch 不要求线程安全"，由缓存层串行化。
- **生命周期**：静态 map，随 executor JVM 存活。executor 退出即释放。动态执行器场景下命中率下降，靠指标观测（见 §7）。
- **失败不污染**：加载抛异常时不写入缓存，下次重试。

---

## 6. 放在哪一层

**放在 `index-spark`（`DistributedIndexSearch` 的 task 内），不放在 kernel 里。**

原因：
1. kernel 只关心"payload → prepared"，不知道文件、snapshot、生命周期；
2. 隔壁正在把 `VectorIndexAdapter` 重构成 `VectorKernel` SPI（#1477/#1478），缓存放在 SPI 之上不受重构影响；
3. fetch-back 侧将来若要复用同一套缓存（Lance reader 复用），可以复用同一个 `ExecutorCache` 抽象。

API 草案（`index-spark`）：

```java
final class ExecutorIndexCache {
  static ExecutorIndexCache get();                 // per-JVM 单例
  PreparedSearch prepared(CacheKey key, Supplier<PreparedSearch> loader);
  byte[] payload(CacheKey key, Supplier<byte[]> loader);
  Stats stats();                                   // hits, misses, bytes, evictions
}
```

调用点改造（`DistributedIndexSearch.searchBatch` 与 `search`）：

```java
CacheKey key = CacheKey.of(location, contentLength, adapter);
byte[] content = cache.payload(key, () -> readPayload(fileIO, location));
PreparedSearch prepared = cache.prepared(key, () -> prepareFrom(content));
prepared.search(query, topK);   // ← 内部串行化，见 §5
```

---

## 7. 可观测性（决定能不能上生产）

必须能回答四个问题，否则不要合入：

1. **命中率**：按 executor 统计 `hits/misses`，以及"因超 maxEntry 未缓存"的次数。
2. **常驻字节**：当前缓存字节 / 上限，淘汰次数。
3. **加载耗时**：每次 miss 的 `readPayload` 与 `prepare` 各自耗时（复用 `fetch-back.timings` 的模式）。
4. **正确性**：缓存命中与不命中的查询结果**逐条相等**（测试里断言，不是人工看）。

落地形式：Spark accumulators（driver 侧可见）+ DEBUG 日志。可选：一个 `CALL system.index_cache_stats()` 过程，返回每个 executor 的统计（需要额外一次 task 广播，可延后）。

---

## 8. 验收标准（必须先定，否则又会"优化了但说不清"）

在 10M `sift10m_lance_zstd` + HNSW 上：

| # | 场景 | 期望 |
|---|---|---|
| A1 | 冷启动单查询（executor 首次） | 与现状同量级（不劣化） |
| A2 | **同一 executor 上第 2 条查询** | **索引搜索段 ≥5x 下降**（对应卡片 4.4s → <0.5s） |
| A3 | 100 条查询流的 P50/P95 | P95 明显低于现状；报告命中率 |
| A4 | 内存 | 常驻字节 ≤ `cache.bytes`，无 GC/OOM 异常 |
| A5 | 正确性 | 命中与不命中的结果集完全一致（含 delete 过滤后） |
| A6 | 并发 | 2 个 task 线程共享同一 entry 时不出现错误结果或异常 |

注意 A2 的判据要**分段测**（索引搜索 / 回表 / 合并），不能再犯今天"把并行 wall time 当串行"的错误：缓存测试必须**固定 executor 数、固定 slot**，并报告 min + 命中率。

---

## 9. 分阶段落地

| 阶段 | 内容 | 预估 | 风险 |
|---|---|---|---|
| P0 | L1 字节缓存 + 指标 + 正确性测试 | 1–2 天 | 低 |
| P1 | L2 prepared 缓存（含 §4 的字节上报、§5 的串行化） | 2–3 天 | 中（内存、线程安全） |
| P2 | 实验：10M / 100 条查询流，A1–A6 | 1 天 | 低 |
| P3 | 产品化讨论：默认开关、executor 有状态的运维含义 | — | 高（定位问题） |

**P0/P1 的开关默认关闭**，通过 session conf 打开，避免影响其它测试。

---

## 10. 需要评审确认的开放问题

1. **L2 的字节如何上报**：给 `VectorKernel` 加 default 方法（改 SPI），还是让 `PreparedSearch` 自带 `estimatedBytes()`（改 API）？我倾向后者（`PreparedSearch` 是 `IndexAdapter` 的内部接口，改动面更小）。
2. **缓存粒度**：按 index 文件（本设计）还是按"表的全部索引文件"（一次全量预热）？全量预热在 10M 上是 800MB/executor，超过默认上限；建议按文件 + LRU。
3. **是否需要跨查询的 query 侧缓存**（例如相同 query 的短期结果缓存）：**建议不做**，见 §2。
4. **executor 有状态的运维含义**：弹性扩缩容时命中率如何、是否需要预热任务、失败重试后的冷启动成本——这些是产品化前必须回答的，不属于 P0–P2。

---

## 11. 附：与今天结论的关系

- 今天的回表结论（文件数 × ~150 ms，原因未定位）**不影响**本设计：索引侧缓存省的是 prepare/I/O，与回表正交。
- 但两者会在"端到端延迟"上相加：缓存把索引侧从 ~20 s 压到 ~0.5 s 后，回表（单查询 2.8–6.4 s）会**成为最大项**。届时如果还要继续压，就只有两条路（减少文件数 / 定位那 150 ms），需要另开卡片。
- 这也是为什么本设计必须**分段测量**（§8 A2/A3），否则会出现"索引侧 20x 提升，端到端只有 3x"的错觉。

---

## 12. 实现记录（P0/P1 已落地，待集群 A/B）

提交 `bf374ff7d`。

**已实现**
- `ExecutorIndexCache`（`index-spark`，JVM 单例）：`LinkedHashMap(accessOrder)` 做 LRU、按字节记账、per-location 单飞（`ConcurrentHashMap<String, Object>` 作 monitor）、`skippedTooLarge`、`hits/misses/loads/evictions/bytes` 统计。
- 接入两处调用点：`DistributedIndexSearch.search`（单查询）与 `searchBatch`（批）。原来"读 payload + Puffin 解析 + prepare"整体移进 `Loader`，只在 miss 时执行。
- 配置（driver 读、随 job 广播）：`spark.iceberg.index.cache.enabled`（默认 false）、`.bytes`（默认 512m）、`.max-entry-bytes`（默认 256m），支持 k/m/g 后缀。
- 缓存键 = `adapter 类名 | blob 类型` + 文件 location（索引文件不可变，不主动失效）。
- 并发：`CachedFile.searching(...)` 持 per-file 锁，满足 kernel"单线程使用 PreparedSearch"的契约。
- 计数：`PreparedSearch.estimatedBytes()` 默认 0，缓存回退到"payload 长度的两倍"。

**测试**
- `TestExecutorIndexCache`（7 个）：命中、按 location/adapter 隔离、LRU 淘汰、超限不缓存、关闭时每次重建、两线程并发只 prepare 一次（单飞）、同一文件搜索不并发（锁）。
- `TestVectorIndexSearchBatch.cachedBatchSearchReturnsTheSameHits`：开缓存后重复批查询，逐行相同。另 12 个既有用例全绿。

**尚未做**
1. **集群 A/B 实测**（本设计的验收 A1–A6）：10M 表上，冷启动 vs 同一 executor 上第 2 条查询，分段（索引搜索/回表/合并）报告。
2. 真实字节上报：`VectorKernel`/kernel 还没重写 `estimatedBytes()`，目前按"payload × 2"记账。
3. 指标上报到 driver（现在只有 `stats()` 与 DEBUG 日志；executor 侧需要保留 pod 才能读）。

---

## 13. 集群验证结果（A1–A4）：本地证明有效，集群未见收益

### 已验证 ✅

| 项 | 结果 |
|---|---|
| A5 正确性（无 delete） | `cachedBatchSearchReturnsTheSameHits`：冷/热/关缓存三者逐行相同 |
| A5 正确性（有 delete） | `cachedBatchSearchAppliesDeletes`：**暴露并修掉了一个真 bug**（见下） |
| A6 并发 | 单测：8 线程共享同一 entry，搜索不重叠；两线程同时请求只 prepare 一次 |
| 缓存真的命中 | 集成测试断言 `stats().hits() > 0`（第一版测试顺序写错，把 cold 放在开启缓存之前，导致永远 miss——已修） |

**顺带修掉的产品 bug**：`IndexSearch` 解析命中行时用的是**索引快照对应的表快照**，
所以索引建好之后删除的行仍会带着数据返回。改为按**当前表快照**解析：命中仍来自索引快照，
但被删除的位置返回"无 row 的 hit"（正是该 reader 文档里写的契约）。

### 集群 A1–A4 ❌ 未见收益

3 executors × 2 cores，10M `sift10m_lance_zstd`，38 个索引文件：

| 测量 | 结果 |
|---|---|
| COLD（缓存首次） | 7821 ms |
| WARM（同 query 第 2 次） | 7589 ms |
| WARM2 | 7653 ms |
| 100 条流 | min 6852 / P50 7107 / P95 7359 ms |
| planning（`indexFiles`） | 8–66 ms |

**没有加速**。而本地已证明缓存命中，所以两种解释尚未区分：
1. 集群上缓存没命中（配置/类加载/键不一致）；
2. 命中了，但 `prepare` 不是那 ~1 秒的主体。

### 关键的环境发现（决定了后续怎么做）

| 发现 | 证据 |
|---|---|
| `spark.driver.extraJavaOptions` 在 k8s **cluster 模式被忽略** | 埋点属性从未到达 driver JVM |
| `--driver-java-options` 在该集群同样未生效 | 同上 |
| executor 的 `System.err`/INFO 日志读不到 | pod 日志里搜不到我们打印的行 |
| **Spark task 本身只要 1.5–2.5 ms** | 空 38-task 作业 60 ms（热） |
| 单查询索引搜索 ∝ `ceil(文件数/slot 数)` | 6 slot 6.9 s，12 slot 4.1 s，38 文件 |

因此**报告通道改为累加器**（`CollectionAccumulator<String>`，driver 侧打印 `CACHE ...`），
并且上报条件改成"缓存开启就上报"，不再依赖任何系统属性。

### 下一步（#1570 承接）

一次干净的运行：缓存开启 → 冷/热各一次 → 读 driver 日志里的 `CACHE executor-*: hits=…`。
- 若 hits=0 → 修键/配置（工程问题，半小时内可定位）；
- 若 hits>0 而耗时不变 → 那 ~1 秒不是 prepare，去查 `readPayload`（整文件读取）与 Lance native open。

---

## 14. 集群验证（终）：靠得住的结果 —— P50 4.6x，但预算必须装得下工作集

### 之前四轮为什么是空的

两个原因叠加，都已定位：

1. **类加载**：k8s cluster 模式下 `spark.driver.extraClassPath` 被吞、`--files` 不参与 driver 类加载，
   driver 一直用镜像自带的 `/opt/spark/jars/iceberg-index-spark-*.jar`（没有缓存类）。
   修法：`--jars` 提供整套 iceberg jar + `spark.driver/executor.userClassPathFirst=true`
   （注意必须**整套**替换，只换部分会 `NoSuchMethodError` / `IncompatibleClassChangeError`）。
   验证手段：打印 `Class.getProtectionDomain().getCodeSource().getLocation()`。
2. **预算**：默认 512MB 装不下工作集。

### 结果（10M，38 个索引文件，每个 ~120MB，3 executors × 2 cores）

| 配置 | 冷 | 热 | 10 条查询流 |
|---|---|---|---|
| 缓存 512MB（默认） | — | — | **命中率 0**：`hits=0 misses=26 loads=13 evictions=9 entries=4 bytes=512MB`（纯抖动） |
| 缓存 3GB | 5216 ms | 3672 / 3626 ms（1.42x） | **min 751 / P50 1529 / P95 5328 / mean 1870 ms** |
| 无缓存（同轮基线） | 5902–7039 ms | — | P50 7039 ms |

缓存 3GB 时 executor 上 `hits=137 misses=130 loads=65 evictions=40 entries=25 bytes≈3.1GB`。

**结论：P50 从 7039 → 1529 ms（≈4.6x），min 751 ms（≈9x）**，与 #708 卡片预期的
"4.4s → <0.5s" 量级一致。收益随查询流稳定（P50 明显低于冷启动）。

### 必须写进产品设计的教训

1. **预算必须按工作集定**：`38 × 120MB ≈ 4.5GB`。预算不足时不是"收益变小"，而是**命中率归零**——
   每次查询把刚加载的条目淘汰掉，比不开缓存还费（多了加载 + 淘汰开销）。
   → 建议：`enabled` 打开时若 `bytes < 估计工作集`，要么自动放大，要么明确报错，不要静默抖动。
2. **默认 512MB 对 10M 规模的向量索引是错的**；建议默认按 `min(25% executor 堆, 索引大小)` 推导。
3. **注入类路径是集群实验的前置条件**：必须整套 iceberg jar + `userClassPathFirst`，
   并用 code-source 打印确认。
