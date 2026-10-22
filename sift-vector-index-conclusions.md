# SIFT 向量索引：端到端结论与遗留清单

> 收口卡：#400（E2E-6）。数据来源：集群 A（K8s）、`hms.sparkann` 下的 1M/10M/100M 表、
> benchmark `tools/vector-benchmark` + `reports/vector-index-benchmark.html`。
> 所有数字都标了**测量口径**（单查询 / 批量摊销 / 索引腿），混用口径是这类对比最常见的错误来源。

---

## 一段话结论

**能上生产的组合已经找到，但不是"Lance 更快"，而是"量化 + 缓存 + 少文件"**：把索引从 HNSW（10M 2.19 GB）换成 IVF×RaBitQ（10M 601 MB、100M 3.2–7.0 GB）之后，索引侧从"每查询读几 GB、抖动 4 倍"变成"可常驻、热查询 41–74 ms"；单查询相对全表扫描仍有 **2.4–4.9×**，批量摊销下索引腿可到 **每查询 105–169 ms**。代价是召回要靠 **bits（8–10）与 over-sample（3–10）** 两个旋钮一起调，且 payload 变更必须**重建索引**（不做向后兼容）。

---

## 1. 加速比（按口径分开列，不可横向比较）

| 规模 | 口径 | 索引侧 | 全扫 | 比值 | 来源 |
|---|---|---|---|---|---|
| 10M | 单查询，HNSW（旧） | 9.3 s | 18.1 s | **1.9×** | #944 |
| 10M | 单查询，IVF×RaBitQ 3bit nprobe=150 **over=1** | 3.6 s | 14.4 s | **4.0×** | #1797 |
| 10M | 同上 **over=10** | 6.0 s | 14.4 s | **2.4×** | #1797 |
| 10M | 同上 **over=100** | 6.4 s | 14.4 s | **2.3×** | #1797 |
| 10M | **批量摊销**（100 条一次调用，每查询） | 105–169 ms | — | 相对单查询约 **84×** | #1203 |
| 10M | **索引腿**（缓存热，不含回表） | **41–74 ms** | — | 相对冷启动 **260×** | #708/P5 |
| 100M | 单查询，1bit nprobe=32 **over=1** | 37.2 s | 182.6 s | **4.9×** | #1797 |
| 100M | 单查询，**4bit** nprobe=32 over=1 | 43.8 s（冷读 7 GB） | 182.6 s | 4.2× | 本轮 |
| 100M | 单查询，1bit **over=100** | 101 s | 182.6 s | **1.8×** | #1797 |

**读法**：over-sample 越大召回越高、加速比越低；缓存只作用在"索引腿"，所以热查询的 41–74 ms 与表中"单查询"不可直接相减（后者含回表）。1M 上同构数据：索引 704 ms→1.3 s、扫描 1985 ms。

## 2. 召回

| 规模 | 臂 | over=1 | over=10 | over=100 |
|---|---|---|---|---|
| 10M | IVF×RaBitQ **1 bit** | 0.397 | — | — |
| 10M | IVF×RaBitQ **4 bit** | **0.890** | — | — |
| 10M | IVF×RaBitQ 3 bit（另一组口径） | 0.78 | 0.97 | 0.97 |
| 100M | **1 bit** nprobe=32 | 0.290 | 0.85 | **1.00** |
| 100M | **4 bit** nprobe=32 | **0.860** | 待测 | 待测 |

- 目标是 recall@10 ≥ 0.95：**10M 用 4 bit + over≈10**；**100M 用 4 bit + over≈3–10**（1 bit 需要 over≈100 才能到 1.0，延迟翻三倍，不划算）。
- **nprobe 在 32 已经足够**（150 与 32 差 ≤0.004，但快 2–3×）：瓶颈在"分区内如何排序"，不在"访问哪些分区"。

## 3. 踩过的坑（按类别）

### 3.1 部署 / 环境

| 坑 | 症状 | 处置 |
|---|---|---|
| 镜像缺 `iceberg-lance` | `FormatModel is not registered for LANCE and Record` | 镜像补齐；或用 `--jars` 整套注入 |
| 基础镜像的 lance bundle 无 HDFS provider | `No object store provider found for scheme: 'hdfs'` | 换厂内 300 MB bundle |
| cluster 模式 `spark.driver.extraJavaOptions` 被忽略 | 系统性开关从未生效，整轮实验变空测 | 用 session conf 或 `--jars` 注入，并打印 `getProtectionDomain()` 确认加载源 |
| 同一 jar 同时在 `extraClassPath` 与 `--jars` | 同名类两份 → `ClassCastException` | 只保留一条注入路径 |
| executor `System.err` 读不到 | 埋点"看不见" 被误判成"没有开销" | 计数走 `CollectionAccumulator` 回到 driver |

### 3.2 数据

| 坑 | 症状 | 处置 |
|---|---|---|
| **`sift10m_gt` 属于 11M 集合** | 逐位比对整体错位，recall 看似 6–10/10 甚至 0 | 过滤 `id < 10,000,000` 再取前 10 |
| **`sift100m_gt_100.bin` 是"10k×100 id 数组 + 同尺寸距离数组"** | 按交错或 int64 读得到 2e17 量级乱码 → recall 假 0 | `binaryRecords(400)` 取前 10k |
| SIFT100M 原始 12.8 GB 是单文件 | Spark 无法切分 → 单 task | 先按 50 MB 切成 256 个 part 再 `binaryRecords` |

### 3.3 代码

| 坑 | 症状 | 修法 |
|---|---|---|
| 改写规则按 score **降序**重排（EUCLIDEAN 是越小越好） | over>1 时 recall 恒为 0 | `9c8276736` |
| 精确重排前 `take(limit)` 截断 | over-sample 形同虚设 | `51469127a` |
| `setLevel` 只置位不清零 + 复用 code 数组 | 码是前后条目的按位或，体积虚高 | `925f8edb2` |
| 索引构建逐个 `union` 成左深链 | 256 层 RDD 图进 task binary → **反序列化 StackOverflow** | `4595a140a`（两两配对 + 每组一个 conf） |
| 失败构建留下空注册 | `snapshots: []` 挡住后续重建 | `a4f7444eb`（失败即 dropIndex） |
| 命中行按索引快照解析 | 建索引后删除的行仍被返回 | `bcfdb9f4a`（按当前快照解析） |
| 缓存预算按 `payload×2` 记账 | 自动推导偏小 ⇒ 覆盖率只有 0.55 | 显式 `cache.bytes`；真实上报待做 |

### 3.4 测量

| 坑 | 症状 | 处置 |
|---|---|---|
| 把并行 wall time 除以串行文件数 | "每文件 155 ms" 的伪结论 | 先确认 task 数与 slot 数，再算商 |
| 空 task 成本被高估 | "每 task 1 s" | 实测空 38-task 作业 **60 ms** |
| 生产负载共存 | 跨轮绝对数不可比 | 只做同轮 A/B，取 min 不用中位 |
| `CALL` 的 `array(1.0, 0.0)` 是**Decimal** | 过程参数类型不匹配 | 写 `1.0d` |
| Iceberg 快照 id 是**随机 long** | 用 `isGreaterThan` 断言 refresh 会随机失败 | 用 `isNotEqualTo` |

## 4. 距离生产还差什么（按优先级）

| # | 事项 | 现状 | 影响 | 承接 |
|---|---|---|---|---|
| **P0** | **payload 格式变更后的重建流程** | 不做兼容（"以最新版本为尊"），旧 payload 明确报错 | 升级必须重建：10M 约 7 min、100M 约 40 min | 已完成决策与错误信息 |
| **P0** | **缓存预算自动推导偏小**（`payload×2`） | 显式配置才够；自动值覆盖率 0.55 | 配错就抖动/OOM | #1655 后续 |
| **P1** | **transform 分区（day/hour/bucket）不支持** | 只支持 identity；其它 transform **让开改写 = 回退全扫**（不会错，但慢） | 分区表用户无索引加速 | #402（决策卡） |
| **P1** | **文本检索在 driver 侧归并统计 + 中文不分词** | `searchAuxiliaryAll` 在 driver 合并语料统计 | 大数据量 BM25 的 driver 成为瓶颈 | — |
| **P1** | **BatchScan：让 driver 完全不沾数据** | 读数据已在 executor；driver 仍做 manifest 规划与命中收集 | 大集群下的 driver 压力 | — |
| **P2** | **100M 的 nprobe 第二维 / bytes×over 网格** | bits 已测 1/4；nprobe 只测 32/150 | 缺少完整取舍表 | #1632 / #1948 |
| **P2** | **HNSW 的 `ef_search` 曲线** | 未测（每次重建 ~29 min） | HNSW 仍缺调参依据 | #1956 |
| **P2** | **refresh vs 残差扫描的阈值** | 未测（refresh 10M = 438 s） | 运维不知道何时重建 | #1978 |
| **P3** | **单索引文件 >2 GB 无法读入** | `readPayload` 明确报错并提示 | 100M 按 256 文件分（每个 ~86 MB），暂无风险 | 文档已记 |
| ~~P?~~ | ~~同列多索引命名歧义~~ | **已解决** | — | #1822 |
| ~~P?~~ | ~~`remove_orphan_files` 需手工排除 indexes/~~ | **已协同** | — | `93f0320b5` |

## 5. 复现入口

```bash
# 一条命令重跑（任一套表）
tools/vector-benchmark/run-benchmark.sh sift10m-over \
    hms.sparkann.sift10m_base hms.sparkann.sift10m_query 10 15 3 2 1,3,10,30,100
# 报告（自包含，无外网依赖）
reports/vector-index-benchmark.html
```

口径要求（写在报告里）：每条曲线标注 over-sample、轮次、查询数；同一张图上只放可比的臂；数字必须能由 driver 日志的 `BENCHRESULT`/`BENCHSUMMARY` 行重建。
