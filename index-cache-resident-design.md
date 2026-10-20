# 设计：常驻索引缓存 + 位置感知调度（v2）

> 状态：设计草案，未实现。承接 `index-cache-design.md`（v1 已实现并实测：3 GB 预算下 10 条查询流 P50 7039 → 1529 ms，≈4.6x；512 MB 预算下命中率归零）。
> 本文回答五个问题：常驻缓存怎么管理/怎么上报、miss 怎么办、容量怎么管、装不下怎么换入换出、LRU 与空闲卸载；并把"弹性 + executor 会挂"当作一等约束。

---

## 0. 一句话架构

**数据面在 executor（payload + prepared 结构），控制面在 driver（谁有哪个文件 + 谁应该有哪些文件），两者之间用"随任务回传的快照"同步；调度用 Spark 原生的 locality hint 把 task 推到持有文件的 executor，hint 落空时一律降级为"本地加载"，绝不失败。**

```
                ┌──────────────── driver ────────────────┐
                │ 位置表: executorId → {host, epoch, keys}│
                │ 规划器: 工作集大小 / 热门集 / 归属分配   │
                │ 计数器: hits/misses/evictions/hintMiss  │
                └───────▲─────────────────────┬──────────┘
            任务回传快照 │                     │ RDD locality hint
                ┌───────┴─────────────────────▼──────────┐
                │ executor JVM                             │
                │ ExecutorIndexCache (LRU, 字节预算)        │
                │  entry = payload bytes + PreparedSearch  │
                └──────────────────────────────────────────┘
```

---

## 1. 目标 / 非目标

**目标**

| # | 目标 |
|---|---|
| G1 | 重复查询时索引文件不再重复读 + 不再重复 prepare（已实现，实测 4.6x） |
| G2 | 跨查询稳定命中：热文件常驻，冷文件不污染（避免 512 MB 那种抖动） |
| G3 | 工作集 > 集群容量时**有明确策略**，而不是碰运气 |
| G4 | executor 增减、重启、被抢占时**不产生错误结果**，只损失缓存命中 |
| G5 | 所有决策可观测：命中率、hint 命中率、常驻字节、换出次数 |

**非目标**

- 不做跨 executor 的缓存复制/迁移（索引文件在 HDFS/对象存储上，重新加载比传块更简单）。
- 不做跨应用共享（每次 Spark 应用重新预热；如需常驻服务另议）。
- 不改索引格式与查询语义。

---

## 2. 组件与状态

### 2.1 executor 侧：`ExecutorIndexCache`

```java
final class Entry {
  byte[] payload;                  // L1
  PreparedSearch prepared;         // L2（不要求线程安全，由 entry 锁串行化）
  long bytes;                      // accounting: payload + prepared.estimatedBytes()
  long lastAccessNanos;            // 空闲卸载 / LRU
  long loadNanos;                  // 观测
}
Map<String /*cacheKey*/, Entry> files;   // LinkedHashMap(accessOrder=true)
```

- `cacheKey = adapterFingerprint + "|" + location`（location 不可变；adapter 指纹区分读取者）
- 单飞：`ConcurrentHashMap<key, Monitor>`，同 key 只加载一次
- `loading` 与 `ready` 分离：加载中的 entry **不可被别的 task 拿到半成品**

### 2.2 driver 侧：位置表（新增）

```java
final class ExecutorView {
  String executorId; String host; long epoch;  // epoch = 该 JVM 启动 nonce
  long seq;                                    // 单调递增，丢弃乱序的旧快照
  Set<Long> keys;                              // location 的 64 位哈希（省内存/带宽）
  long bytes, hits, misses, evictions;
  long lastSeenMillis;
}
Map<String /*executorId*/, ExecutorView> views;
```

driver 上**只放元信息**（每个 executor 几十个 8 字节哈希），永不持有 payload。

---

## 3. 问题 1：常驻缓存怎么管理，怎么同步上报

### 3.1 上报通道：随任务回传，不额外起作业

每次索引搜索的 task 结束时，把**当前持有的 key 快照**塞进 `CollectionAccumulator`：

```java
reports.add(new Report(executorId, host, epoch, seq++, ExecutorIndexCache.keysDigest(), stats()));
```

| 方案 | 优点 | 缺点 | 采用 |
|---|---|---|---|
| **随任务回传快照** | 零额外作业；天然与查询同频 | 每次搜索都要带上；失败 task 的更新丢失 | ✅ 主通道 |
| 周期性心跳作业 | 空闲期也能刷新 | 额外作业、额外调度 | 仅在"空闲卸载后"补一次 |
| 独立 RPC 服务 | 实时 | 引入服务/端口/安全面 | ❌ |

- **快照而非增量**：`keys` 是"我现在实际持有的集合"，天然自愈（丢一次上报、淘汰、重启都不会让 driver 记错）。
- **乱序与重试**：`(epoch, seq)` 单调；driver 只接受 `epoch` 更大或 `seq` 更大的报告；失败 task 的重复上报被 `seq` 去重。
- **executor 重启**：新 JVM `epoch` 不同 → driver 直接整条替换，不保留旧 keys（**这是"executor 会挂"的第一道防线**）。
- **executor 退出**：`SparkListener.onExecutorRemoved` → 删除该 executor 的 view；`lastSeen > TTL`（如 10 min）也视为失效。
- 带宽：38 个 key × 8 B ≈ 304 B/executor/查询；1000 个文件 ≈ 8 KB，可接受。

### 3.2 上报也可用于**预热**

driver 在查询前若判定需要（见 §6），可以起一个"prefetch 作业"：把该查询的工作集按 task 分发到**归属 executor**（§5），任务体只做 `cache.preload(key)`，不返回结果。这样第一条查询就不再是冷的。

---

## 4. 问题 2：cache miss 怎么处理

miss 分五类，处理方式不同：

| 类型 | 判定 | 处理 |
|---|---|---|
| **absent** | 本地无 entry | 单飞加载 → 成功则 put（受准入控制）→ 返回 |
| **loading** | 同 key 正在被别的 task 加载 | 等待同一个 `Future`（不重复加载） |
| **evicted** | 之前有、被淘汰 | 同 absent |
| **too large** | `bytes > maxEntry` | **不缓存**，本次流式使用（读→prepare→search→丢弃），并计入 `skippedTooLarge` |
| **failed** | 加载抛异常 | 不写缓存；本次查询**直接把异常抛给上层**（不能静默返回空结果）；可选"降级重试一次（不缓存）" |

原则：

1. **miss 绝不改变结果**（命中与不命中逐行相同，已有测试锁定）。
2. **miss 绝不失败**：除"加载本身失败"外，任何缓存内部错误（记账溢出、淘汰竞态）都降级为"本次不缓存"。
3. **加载失败不做负缓存**（否则一次 HDFS 抖动会让该文件长时间不可用）；最多允许一次立即重试。
4. **加载在 entry 锁之外做**（避免长时间持锁），但 put 是原子的：写 map 与记账在同一临界区。
5. **内存异常兜底**：`OutOfMemoryError`/容器内存压力 → 清空缓存并重试一次（不缓存）。这条必须显式实现，否则"缓存把 executor 弄死"是最坏结果。

---

## 5. 问题 3：容量怎么管

### 5.1 预算来源（优先级从高到低）

1. session conf：`spark.iceberg.index.cache.bytes`
2. 表属性：`index.cache.bytes`（按表隔离时用）
3. 自动：`min(25% × executor 堆, 本次查询索引总大小 × 1.2)`
4. 硬上限：`maxEntryBytes`（单文件超过则流式）

### 5.2 准入控制（避免"装不下还硬装"）

driver 在 plan 阶段已知道**本次查询的索引文件清单与大小**（`IndexFile.length`）：

```
workingSet = Σ size(本次查询的索引文件)
if workingSet ≤ totalBudget        → 全量缓存（pin，见 §6.2）
else if workingSet ≤ totalBudget×2 → 只缓存"热"子集（§6.3），其余流式
else                               → 关闭缓存（流式），并记录原因
```

**硬性要求**：`enabled && bytes < workingSet` 时必须**显式告警**（#1655），不能静默抖动；`evictions>0 && hits==0` 视为"配置错误"级告警。

### 5.3 记账与隔离

- 记账 = `payload.length + prepared.estimatedBytes()`（后者默认回退 payload 长度，kernel 可上报真实值）。
- heap 与 off-heap 分开记：JNI kernel（hnswlib）的 native 内存不计入 Java heap，但计入预算（否则容器 OOMKilled）。
- 多表/多列共享一个 executor 时按 **table id 公平配额**（每表至少 1/N 预算，未用部分可被借用），避免一个大索引饿死其它查询。

### 5.4 内存压力阀

每 N 秒（或每次 put 前）检查 `Runtime.freeMemory + 剩余预算`；低于阈值（如 15% 堆）→ 主动淘汰到 50%。这是"容器内存限制"下唯一的自救手段。

---

## 6. 问题 4：工作集超过集群容量，怎么换入换出

这是本设计的核心分歧点：**纯 LRU 在不满足容量的工作集上必然抖动**（已实测：512 MB 预算、4.5 GB 工作集 → `hits=0`）。因此分三档策略。

### 6.1 档位判定（driver 每次查询）

```
capacity  = Σ(各 executor 的可用预算)        // 由位置表 + 配置得出
workingSet = 本次查询索引文件总大小
if workingSet ≤ capacity:        PIN_ALL      // 全部常驻
elif workingSet ≤ 2×capacity:    PIN_HOT      // 只 pin 热子集，其余流式
else:                            STREAM       // 不缓存，等价于今天的行为
```

### 6.2 PIN_ALL：归属分配（home assignment）—— 消除"随机落点"导致的重复加载

每个索引文件按 `hash(location) % N` 分配一个**归属 executor**（N = 参与计算的 executor 数），task 的 locality hint 指向它（§7）。

- 每个 executor 只需要装下 `workingSet / N`，容量需求**可事先算出**；
- 同一个文件永远由同一个 executor 加载 → 不会有"两个 executor 各存一份"的浪费；
- executor 增减时 `N` 变化 → 归属重算，旧缓存按 LRU 自然淘汰（**不迁移**）。

### 6.3 PIN_HOT：只常驻热子集

driver 维护"文件访问频次"（按 key 的访问计数，来自上报），把工作集分成：

- **hot**（频次 ≥ 阈值或属于最近 K 次查询）→ 常驻，参与 hint；
- **cold** → 每次流式读取（读→prepare→search→丢），不占预算。

频次用**带衰减的计数**（每天减半），避免历史热点永远霸占（admission filter，思路同 TinyLFU）。

### 6.4 STREAM：装不下就不缓存

不缓存时保持今天的行为：读整个文件 → prepare → 搜索 → 丢弃。**不引入磁盘二级缓存**（见下）。

### 6.5 显式换入换出（可选，进阶）

| 手段 | 说明 | 何时值得 |
|---|---|---|
| **Preload 作业** | 查询前把工作集按归属分发预加载，搜索时全命中 | 交互式场景，愿意付一次预热成本 |
| **查内分轮（chunking）** | 一次查询拆成 ⌈workingSet/capacity⌉ 轮，每轮只加载一块，块内并行搜索，块间合并 top-k | 大工作集 + 小集群，保证"总有一部分命中"而不是全 miss |
| **本地磁盘二级缓存** | 把 payload 原始字节写到 executor 的 `emptyDir`（NVMe），内存不够时从盘读（省网络，但仍需 prepare） | 只在"重复读很远、prepare 很快"时成立；目前 prepare 是大头 → **不建议** |
| **格式侧压缩** | RaBitQ/IVF 让 payload 小 20–50x（#1486/#1485） | **最优解**：把 workingSet 压到能全量常驻，上述策略都不需要 |

> 结论：容量不足的**第一优先**是减小 payload（量化/IVF），第二是 PIN_HOT，第三才是分轮/预加载。磁盘换页是最后手段。

---

## 7. 位置感知调度（让缓存"用得上"）

### 7.1 为什么必须有

实测：38 个文件撒在 3 个 executor 上，每个只见过自己执行的 ~13 个（`entries≈25`）；task 落点随机 → 总有一部分文件落到**没缓存的 executor**，重新读一遍。位置表 + hint 能把这部分消掉。

### 7.2 实现

```java
final class IndexFileRDD extends RDD<String> {
  @Override public Seq<String> getPreferredLocations(Partition p) {
    String loc = ((IndexFilePartition) p).location();
    // 归属 executor 优先；其次任何已持有的 executor；都没有则空（同今天）
    List<ExecutorView> candidates = driverTable.holdersOf(loc);
    return candidates.stream()
        .limit(2)                                   // 热点倾斜保护：最多 2 个候选
        .map(v -> TaskLocation.ExecutorCacheTaskLocation(v.host(), v.executorId()).toString())
        .toList();
  }
}
```

- Spark 会把这些解析成 `PROCESS_LOCAL` 提示，把 task 派给对应 executor（RDD cached block 用的同一机制）。
- 等待上限由 `spark.locality.wait` 控制（建议 2–3 s）：等太久会放大尾延迟，尤其集群繁忙时。

### 7.3 提示落空的处理

| 情况 | 行为 |
|---|---|
| 目标 executor 忙/队列满 | `locality.wait` 后按普通调度执行 → 该 task 本地加载（miss），结果仍正确 |
| 位置表过期（条目已淘汰） | 同上；executor 侧自校验（查 map 再看 hint），hint 只是提示 |
| executor 挂了 | 任务重跑，落到别的 executor → 加载；driver 收到 `onExecutorRemoved` 更新位置表 |
| 归属 executor 不存在（N 变小） | 归属按当前 N 重算，hint 指向新归属；旧缓存淘汰 |

**不变量：hint 只影响性能，永不影响正确性与可用性。**

---

## 8. 问题 5：LRU 与空闲卸载

### 8.1 为什么纯 LRU 不够

顺序扫描 38 个文件会把 LRU 表整圈冲掉（512 MB 实测 `hits=0`）。因此：

- **准入过滤**：只有"预计会被再次使用"的 entry 才进缓存（PIN_HOT 档）；PIN_ALL 档因为全放得下，LRU 不会抖。
- **两段队列（2Q）**：新 entry 先进 probation；第二次访问才晋升 protected。避免一次性扫描把常驻集冲掉。
- **保护 pin**：PIN_ALL/PIN_HOT 的 hot 集标记 `pinned`，不参与 LRU 淘汰（但仍受内存压力阀强制淘汰）。

### 8.2 空闲卸载

| 触发 | 动作 |
|---|---|
| `lastAccess` 超过 `cache.idle-ttl`（默认 30 min） | 后台线程（每 executor 一个）逐批淘汰，直到回到 0 或预算 |
| 查询结束后 `idle-queries`（默认 3）次未再访问 | 同上（可选，更激进） |
| driver 显式卸载 | `CALL system.index_cache_evict(table)` 发一个广播 + 心跳任务，各 executor 清理该表条目 |
| executor 退出/重启 | JVM 结束，内存自动释放（无需处理，但**必须**保证 off-heap 的 child allocator 已关闭，避免 native 泄漏计数） |

### 8.3 卸载与并发安全

淘汰只在 map 临界区内进行；正在被 `searching()` 持锁使用的 entry **不能**被释放（引用计数或"使用中"标记），否则会出现"搜索到一半结构被清空"。

---

## 9. 正确性（与缓存无关的不变量）

| 不变量 | 保证方式 |
|---|---|
| 结果与缓存无关 | 集成测试：冷/热/关闭三者逐行一致（已有）；再加"并发 + 淘汰中"场景 |
| 键唯一标识内容 | `location`（不可变）+ adapter 指纹；文件被覆写视为新 location |
| prepared 结构单线程使用 | entry 级锁（已有）；并发任务串行化搜索 |
| 半成品不可见 | 加载完成才 put；`loading` 与 `ready` 分离 |
| 删除/重写语义 | 命中来自索引快照、行按当前表快照解析（已修） |
| executor 重启不产生脏命中 | `epoch` 变化 → driver 丢弃旧位置；缓存本身就是空的 |

---

## 10. 弹性与不稳定（本设计的重点）

| 场景 | 设计应对 |
|---|---|
| **动态扩缩容** | 归属按当前 executor 数 N 重算；hint 落空 = 普通调度 + 本地加载；不迁移缓存 |
| **executor 中途挂掉** | ①任务重跑（Spark 保证）；②driver `onExecutorRemoved` 清位置；③新 executor 冷启动，首次加载最慢（可预加载缓解） |
| **task 重试/推测执行** | 同 executor 单飞避免重复加载；不同 executor 各自加载（可接受） |
| **上报丢失** | 快照 + `(epoch,seq)` + TTL，自愈 |
| **内存压力/OOM** | 压力阀提前淘汰；`OutOfMemoryError` → 清空 + 不缓存重试一次 |
| **热点倾斜** | hint 最多 2 个候选；必要时对最热文件做**副本**（预算允许时复制到第二个 executor） |
| **网络/存储抖动导致加载失败** | 不写缓存 + 一次重试；仍失败则把异常抛给查询（不返回错误结果） |
| **长时间空闲** | TTL 卸载（§8.2），避免"缓存常驻但没人用"占着内存 |

---

## 11. 可观测性

| 指标 | 粒度 | 用途 |
|---|---|---|
| `hits / misses / loads / evictions / skippedTooLarge` | executor | 基本健康度 |
| `bytes / budget` | executor | 容量是否够 |
| `load_ms`（P50/P95） | executor | 加载开销 |
| **`hintHit / hintMiss`** | 任务 | 位置调度是否真的把 task 送对了地方 |
| `workingSet / capacity / 档位（PIN_ALL/PIN_HOT/STREAM）` | 查询 | 解释"这次为什么没命中" |
| 告警 | driver | `hits==0 && evictions>0`（抖动）、`bytes ≥ 95% budget`（临界） |

暴露方式：`CALL system.index_cache_report()` 返回每 executor 的上述计数 + 当前档位 + 位置表摘要（driver 视图）。

---

## 12. 配置项

| 配置 | 默认 | 说明 |
|---|---|---|
| `spark.iceberg.index.cache.enabled` | false | 总开关 |
| `spark.iceberg.index.cache.bytes` | 自动：`min(25% heap, workingSet×1.2)` | 预算（**不再写死 512m**） |
| `spark.iceberg.index.cache.max-entry-bytes` | 256m | 单文件上限，超过则流式 |
| `...mode` | `auto` | `auto` / `pin-all` / `pin-hot` / `stream` |
| `...idle-ttl` | 30m | 空闲卸载 |
| `...hot-threshold` | 3 次/衰减窗口 | PIN_HOT 的热度阈值 |
| `spark.iceberg.index.schedule.locality` | true | 是否启用位置 hint |
| `spark.locality.wait` | 建议 2–3s | hint 等待上限（集群级） |

---

## 13. 失败模式矩阵

| 失败 | 影响 | 是否自愈 | 处理 |
|---|---|---|---|
| 位置表过期 | 少量 miss | 是（下次上报） | hint 落空 → 本地加载 |
| 缓存抖动（预算不足） | 命中率 0，比不开更慢 | 否 | 准入控制 + 告警 + 自动放大 |
| 加载失败 | 该次查询失败（如实抛错） | 是（重试） | 一次重试，不写缓存 |
| executor OOM | 任务失败、executor 可能被 kill | 部分 | 压力阀 + OOM 兜底清空 |
| executor 挂掉 | 任务重跑、缓存丢失 | 是 | Spark 重试 + `onExecutorRemoved` |
| 位置 hint 造成尾延迟 | P95 变差 | 是 | `locality.wait` 上限 + 最多 2 候选 |
| 多租户饿死 | 某表一直 miss | 否 | 每表公平配额 |

---

## 14. 分阶段落地

| 阶段 | 内容 | 依赖 | 预估 |
|---|---|---|---|
| **P0** | 容量自适配 + 抖动告警（#1655） | 已实现 v1 | 2–3 h |
| **P1** | 上报扩展为 `(epoch, seq, keys, stats)` + driver 位置表（不参与调度，仅观测） | P0 | 1 d |
| **P2** | `IndexFileRDD` + locality hint（PIN_ALL 档） | P1 | 1 d |
| **P3** | 档位判定（PIN_ALL/PIN_HOT/STREAM）+ 2Q/LRU 准入过滤 | P2 | 1–2 d |
| **P4** | 空闲卸载 + `index_cache_report()` + 每表配额 | P3 | 1 d |
| **P5** | 集群实验：hint 命中率、P50/P95、故障注入（kill executor） | P2+ | 1 d |

## 15. 验收（A1–A10）

| # | 判据 |
|---|---|
| A1 | 冷启动不比关缓存慢（<10%） |
| A2 | 同 executor 第二条查询索引段 ≥5x（3 GB 下已实测 4.6x P50） |
| A3 | 100 条流 P50/P95 记录，且命中率随流稳定上升后收敛 |
| A4 | `bytes ≤ budget`，无 OOM/Kill |
| A5 | 冷/热/关闭结果逐行一致（含 delete、含并发淘汰） |
| A6 | 两 task 共享同一 entry 无错误（已测） |
| A7 | **hint 命中率** ≥80%（PIN_ALL 档，工作集 ≤ 容量） |
| A8 | executor 被杀 → 任务重试成功、结果正确、位置表在 TTL 内自愈 |
| A9 | 预算 < 工作集 → 显式告警或自动放大，不出现 `hits=0 && evictions>0` 静默劣化 |
| A10 | 空闲 TTL 后常驻字节归零 |

---

## 16. 开放问题

1. **归属分配的 N 用什么**：当前 executor 数？slot 数（cores）？建议按 **slot**（并行度），因为一个 executor 上多个 task 会并发读不同文件。
2. **PIN_HOT 的热度从哪来**：只来自本应用历史，还是落盘持久化（跨应用）？建议先只在本应用内，跨应用预热留待"常驻服务"方案。
3. **是否复制热文件**：预算允许时复制最热的 1–2 个文件到第二个 executor，能抗热点倾斜，但增加一致性成本（两份独立 LRU）。建议 P5 之后评估。
4. **`estimatedBytes` 的真实上报**：kernel 目前默认回退 payload×2，需要 kernel 实现者补真实值（RaBitQ/IVF 的空间占用差异很大）。
