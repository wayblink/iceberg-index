# 测试报告：常驻索引缓存 + 位置感知调度（v2）

| 项 | 内容 |
|---|---|
| 报告日期 | 2026-10-20 |
| 被测对象 | Iceberg fork 的 `iceberg-index-spark` 索引缓存 v2（P0–P5）与 `#1715` 修复 |
| 设计文档 | `index-cache-resident-design.md`（HTML: `~/.memories/main/docs/index-cache-resident-design.html`） |
| 相关卡片 | 伞卡 #1673；子卡 #1655/#1674/#1675/#1676/#1677/#1678；缺陷卡 #1715；余项 #1706 |
| 环境 | 本地：JDK 17 + Spark 3.5 + Scala 2.12（`-DsparkVersions=3.5 -DscalaVersion=2.12`）；集群：K8s 集群 A，3 executor × 2 core，镜像 `sparkann-lance-e2e:v6`，表 `hms.sparkann.sift10m_lance_zstd` |
| 结论 | **通过**。本地 80 个测试全绿；集群验证：索引腿 19.6 s → 0.7 s（冷后首查）/ 41–74 ms（稳态），杀 executor 后结果逐行一致；发现并修复 4 个产品缺陷、5 个实现缺陷 |

---

## 1. 被测提交

| 提交 | 内容 |
|---|---|
| `bf374ff7d` | v1 缓存（`ExecutorIndexCache` + 两处调用点 + 配置 + 测试） |
| `bcfdb9f4a` | 命中行按**当前表快照**解析（删除的行不再返回）+ 计数回传 |
| `40dbbb468` | P0 容量自适配 + 抖动告警 |
| `d411e0919` | P1 位置表/上报、P2 `IndexFileRDD` + locality hint、P3 三档 + 2Q、P4 空闲卸载 |
| `9ac4a25f3` | #1715：读取按 adapter 可读类型集合匹配 + 不可解析 payload 显式报错 + Lance 端到端测试 |
| `9662a9d7c` | 移除 payload 向后兼容（一个角色一个类型，"以最新版本为尊"） |
| `b7a3aa516` | 集群最终数字（设计文档 §21） |

---

## 2. 本地测试

### 2.1 结果汇总（80 个，0 失败）

| 测试类 | 数量 | 结果 | 覆盖点 |
|---|---|---|---|
| `TestExecutorIndexCache` | 12 | ✅ | 命中/隔离、LRU 淘汰、超限不缓存、关闭时重建、并发单飞、搜索串行化、PIN_HOT 准入、PIN_ALL 不腾空间、2Q 晋升、空闲卸载 |
| `TestIndexCacheBudget` | 5 | ✅ | 按 per-executor 份额推导、堆比例上限、显式配置优先、工作集未知用下限、不超过堆比例 |
| `TestIndexCacheLocations` | 6 | ✅ | seq 单调、epoch 变化整条替换、holder 上限、TTL 过期、覆盖率统计 |
| `TestVectorIndexAdapter` | 12 | ✅ | 一个角色一个 blob 类型、HNSW 辅助载荷搜索与精确结果一致（各 metric） |
| `TestVectorIndexSearchBatch` | 24 | ✅ | 批查询结果、单/批一致、缓存冷/热/关闭逐行一致、delete 后一致、位置表非空 |
| `TestIndexTableFunctions` | 6 | ✅ | 表函数与过程结果一致、可与查询组合 |
| `TestRewriteVectorSearch` | 14 | ✅ | 重写/删除/更新后的搜索结果正确性 |
| `TestLanceVectorSearch` | 1 | ✅ | **新增**：Lance 表的端到端索引搜索（单查询 + 批查询 + 重复查询一致） |

### 2.2 关键断言（与设计验收对应）

| 验收 | 覆盖方式 | 结果 |
|---|---|---|
| A5 结果与缓存无关 | `cachedBatchSearchReturnsTheSameHits`（冷/热/关闭逐行相同）、`cachedBatchSearchAppliesDeletes` | ✅ |
| A5 删除语义 | 同上（删除的位置返回"无 row 的 hit"，不返回已删数据） | ✅ |
| A6 并发 | 8 线程共享同一 entry 搜索不重叠；两线程同时请求只 prepare 一次 | ✅ |
| 命中确实发生 | 断言 `stats().hits() > 0` | ✅ |
| 空/可选配置 | 未配置预算时按查询推导；`parseBytes(null)` 曾 NPE，已修 | ✅ |

---

## 3. 集群测试

### 3.1 前置：索引重建（必须）

集群表 `sift10m_lance_zstd` 的索引是**旧 payload 格式**（见 §4.1），当前 build 无法读取，因此重建：

| 步骤 | 参数 | 结果 |
|---|---|---|
| `drop_vector_index` + `build_vector_index`（第一次，漏了 `structure`） | 默认 **FLAT** | 132 s 完成 —— **语义与 HNSW 不同**，不可用于对比 |
| 重建（正确） | `structure=HNSW`, `max_m=16`, `ef_construction=200`, `ef_search=50`, `max_entries_per_index_file=262144` | **1772 s**，38 文件 / 10M 条目，PUBLISHED |

### 3.2 场景与结果

配置：缓存 6 GB/executor（覆盖工作集 4.7 GB），`spark.iceberg.index.schedule.locality=true`。

| # | 场景 | 指标 | 结果 |
|---|---|---|---|
| 1 | 无缓存（`SEGMENT OFF`） | 单查询索引腿 | min 17935 / **p50 19623 ms** |
| 2 | 冷启动（缓存开启后首查） | 单查询 | 16984 ms |
| 3 | 热查询（同 query 第 2/3 次） | 单查询 | **695 / 1251 ms**（≈24x） |
| 4 | 10 条查询流 | p50 / p95 | min 51 / **p50 74** / p95 227 ms |
| 5 | **300 条查询流，流中强杀一个 executor** | p50 / p95 / max | min 32 / **p50 41** / p95 73 / **max 9436**（恢复波峰） |
| 6 | 结果一致性（流前 vs 流后） | 逐行比较 | **`CONSISTENT true rows=10`** |
| 7 | hint 关闭对照（同预算） | 常驻字节 / 稳定性 | **29.6 GB**（每 executor 全量 38 份），集群扩到 9 executor 后 **executor JVM OOM（exit 52）** |
| 8 | hint 开启对照 | 常驻字节 / 稳定性 | **4.72 GB**（每文件 1 份），无 OOM |
| 9 | 覆盖率 | `INDEX_CACHE` 报告 | 显式 6g → **coverage 1.00**；自动推导（≈1.8g）→ 0.55 |
| 10 | 预算过小（512 MB，早期实验） | 健康度 | `hits=0 evictions=9` → **纯抖动**，比不开更慢 |

### 3.3 集群结论

1. **缓存有效性**：索引腿从 ~19.6 s 降到 0.7 s（冷后首查）/ 41–74 ms（稳态），约 **24x（单查询）/ 260x（300 条流 p50 对比无缓存单查询）**。
2. **位置调度是必需品**：没有 hint 时每个 executor 缓存全量文件，内存随 executor 数线性膨胀，扩容直接 OOM；有 hint 时每文件只有一份（4.72 GB）。
3. **正确性**：流中杀 executor 后结果逐行一致，位置表在 TTL 内自愈（`executors=3 files=38 coverage=1.00`）。
4. **预算必须覆盖工作集**：不足时不是"收益变小"，而是命中率归零或 OOM。

---

## 4. 发现的缺陷与修复

### 4.1 产品缺陷（影响用户可见行为）

| # | 缺陷 | 影响 | 修复 |
|---|---|---|---|
| D1 | 命中行按**索引快照**对应的表快照解析 | 索引建好后**删除的行仍被返回** | `bcfdb9f4a`：按当前表快照解析 |
| D2 | 读取路径只匹配"写入类型"的 blob | 旧索引**静默返回 0 行**（#1715） | `9ac4a25f3`：按 adapter 声明的可读类型集合匹配 |
| D3 | 旧 payload 无法按当前条目结构解析 | `ClassCastException`（远端堆栈，难以归因） | `9ac4a25f3`：转为明确错误并提示"Rebuild the index" |
| D4 | 默认预算 512 MB 对 4.5 GB 工作集 | 命中率 0 / 比不开更慢 / 极端情况 OOM | `40dbbb468`：按查询推导 + 预算不足告警 + 首次淘汰零命中告警 |

### 4.2 实现缺陷（本设计落地时发现）

| # | 缺陷 | 修复 |
|---|---|---|
| I1 | `PIN_ALL` 把全部条目 pin 住，使 2Q 的 LRU 保护失效 | 拆成 `pinned`（模式）与 `protectedRank`（2Q） |
| I2 | `put` 先插入再淘汰，装不下时预算被突破 | 淘汰后仍超预算则不保留新条目 |
| I3 | `derive(configured, parseBytes(configured, …))` 未配置时 **NPE**（Java 参数 eager 求值） | 先判空再解析 |
| I4 | 累加器经 `broadcast` 后 `add()` 不生效 | 闭包直接捕获 accumulator |
| I5 | `TaskLocation.ExecutorCacheTaskLocation` 是 `private[spark]`，Java/Scala 均不可用 | 改为**主机级** hint（k8s 一 pod 一节点时等价） |
| I6 | RDD 作为依赖被序列化 → `NotSerializableException` | 位置表字段 `@transient` |
| I7 | Scala 同名 companion 不生成静态转发器，Java 无法调用 | object 改名（`IndexFileRdds`） |

### 4.3 测试自身的缺陷（记录以免重犯）

| # | 缺陷 | 说明 |
|---|---|---|
| T1 | 缓存命中测试把 `cold` 放在**开启缓存之前** | 第二次查询才是首次，于是永远 `hits=0`；顺序修正后命中 |
| T2 | `spark.sql(sql, args)` 的 varargs 用法触发 `MatchError` | 统一改成 `String.format` |
| T3 | `CALL` 解析器只接受字面量 | `array(0.0d, 100.0d)`；`CAST(...)` 与十进制字面量都会失败 |
| T4 | 探针 jar 内嵌了旧类，遮住 `--jars` 的新代码 | 探针只打包自身类 |
| T5 | 断言"旧 blob 名仍可读" | 按"以最新版本为尊"改为"一个角色一个类型" |

### 4.4 环境陷阱（影响测量可信度）

| # | 陷阱 | 后果 / 规避 |
|---|---|---|
| E1 | k8s cluster 模式 **`spark.driver.extraJavaOptions` 被忽略**、`--driver-java-options` 亦无效 | 系统属性从未到达 driver，一轮实验变成空测；改用"不依赖系统属性"的配置与累加器 |
| E2 | driver 加载**镜像自带**的 `/opt/spark/jars/*` 旧 jar | 新代码不生效；改用 `--jars` + `userClassPathFirst=true`，并用 `getProtectionDomain().getCodeSource()` 打印确认 |
| E3 | 同一 jar 同时在 `extraClassPath` 与 `--jars` | 类被两个 loader 加载 → `ClassCastException`；去掉 `extraClassPath` |
| E4 | executor 的 `System.err`/INFO 在 pod 日志里读不到 | 计数改走 `CollectionAccumulator`，由 driver 打印 `INDEX_CACHE …` |
| E5 | 生产负载共存 | 只能同轮 A/B；取 min 而非中位 |
| E6 | 重建时漏 `structure` 参数 | 默认 FLAT（精确全扫），132 s 完成但语义不同 |

---

## 5. 未覆盖与残余风险

| # | 项 | 风险 | 计划 |
|---|---|---|---|
| R1 | 自动推导预算按 `payload × 2` 记账 | 覆盖率仅 0.55（每 executor 7/13 文件）；需要显式配置才能覆盖 | #1655 后续：`estimatedBytes()` 由 kernel 上报真实占用 |
| R2 | `index_cache_report()` SQL 过程、每表公平配额 | 多表共享 executor 时可能饿死 | #1706 |
| R3 | 多表并发、动态扩缩容稳态命中率 | 仅观测到扩容会放大重复缓存（E7 对照），未做稳态压测 | 建议加入后续实验 |
| R4 | off-heap kernel（hnswlib native / RaBitQ）内存记账 | 当前只记 payload+估算，native 内存可能被低估 | kernel 实现 `estimatedBytes()` |
| R5 | 集群数字为单轮测量 | 绝对值受生产负载影响，仅同轮 A/B 可信 | 已有对比设计，必要时重复 |
| R6 | 位置 hint 只到主机级 | 同主机多 executor 时精度下降 | 待 Spark 公开 executor 级 hint 或改用自定义调度 |

---

## 6. 复现方式

```bash
# 本地（JDK 17）
./gradlew -DsparkVersions=3.5 -DscalaVersion=2.12 \
  :iceberg-index-vector:test :iceberg-index-spark:test \
  :iceberg-spark:iceberg-spark-extensions-3.5_2.12:test \
  --tests "org.apache.iceberg.spark.extensions.TestVectorIndexSearchBatch" \
  --tests "org.apache.iceberg.spark.extensions.TestLanceVectorSearch" \
  --tests "org.apache.iceberg.spark.extensions.TestIndexTableFunctions" \
  --tests "org.apache.iceberg.spark.extensions.TestRewriteVectorSearch"
```

```bash
# 集群：先重建索引（必须显式给 structure）
spark-submit ... rebuildindex.jar "sparkann.sift10m_lance_zstd"
#   CALL drop_vector_index(...) + build_vector_index(structure=>'HNSW', max_m=>16,
#   ef_construction=>200, ef_search=>50, max_entries_per_index_file=>262144)

# 再跑探针（hint 开 / 关各一轮）
spark-submit --jars <整套 iceberg jar> --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.iceberg.index.cache.bytes=6g \
  [--conf spark.iceberg.index.schedule.locality=false] \
  lancepoc21.jar 2 hms.sparkann.sift10m_lance_zstd 300
```

判读：driver 日志里 `CLASS_DistributedIndexSearch ... <我们的 jar>`、`INDEX_CACHE mode=PIN_ALL ... coverage=1.00`、`SEGMENT ON_STREAM ...`、`CONSISTENT true`。

---

## 7. 判定

**通过，可以进入评审/合并。**

- 功能正确性：80 个本地测试全绿，含删除语义、并发、冷/热/关闭一致、Lance 端到端；
- 性能：索引腿 19.6 s → 0.7 s（冷后首查）/ 41–74 ms（稳态），hint 使常驻内存从 29.6 GB 降到 4.72 GB 并避免 OOM；
- 鲁棒性：查询流中强杀 executor，作业完成且结果逐行一致；
- 已知边界：自动预算记账（R1）、SQL 报告与配额（R2）、off-heap 记账（R4）、多表并发（R3）——均有卡片承接，不阻塞本设计验收。
