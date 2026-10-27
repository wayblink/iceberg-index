# 索引驻留：真实集群验收记录（#2411 / #2296）

本文件记录在 A 集群（公司 K8s，Spark 3.5.7-STEP-rc2）上，用**独立提交的应用**验证索引驻留控制面与查询路径的结果。所有数字都来自运行现场的 `SMOKE` 行与驱动日志，未用历史值代替；无法观测的项写 null 并说明。

## 环境与产物

| 项 | 值 |
|---|---|
| 集群 | A 集群（`shai` 提交，`spark` namespace，`hms` 生产 HMS） |
| 基础镜像 | `hub.i.basemind.com/spark/spark:3.5.7-STEP-rc2-tpcds-dsdgen-lance-20260416a` |
| 验收镜像 | 第一轮 `…:2411-802713c11`（digest `sha256:7e249a28…`）；最终轮 `…:2411-9de92a8fd`（digest `sha256:cee936278c069f858e87a3a41c7af6a5459b54cce5bf35264433ca0e8209b62e`） |
| 构建 | `922e6aa65` + `802713c11`（注册表按 SparkContext 键、执行器注册索引类型）+ `181fa1be1`（复用副本的执行器归属）+ `9de92a8fd`（按索引自有 key 查副本，修 LRU 在真实 key 上找不到牺牲者） |
| 启动 | `spark.plugins=org.apache.iceberg.index.spark.IndexCachePlugin`；`spark.sql.extensions=IcebergSparkSessionExtensions` |
| 工具 | 工具已在独立仓库 `iceberg-tools`：`vector-benchmark/IndexResidencySmoke.java`、`run-index-residency-smoke.sh`（产品仓库里 `tools/` 是一个指向它的本地符号链接，不入版本库） |
| 证据 | `shai:/data/workspace/sparkann/2411-922e6aa65/runs/<run-id>/{meta.properties,driver.log,smoke.jsonl}` |

镜像由本卡的类路径 jar 覆盖基础镜像中 Iceberg/parquet/avro 后重建；插件只能从镜像自身 classpath 载入，`spark.jars` 不足以让 `spark.plugins` 找到它。

## 本卡修出的四个真实缺陷

1. **驱动插件拿不到 application id**：`IndexCachePlugin` 在 `SparkContext` 创建期间初始化，此时 `sc.applicationId()` 仍为空，注册表以它为键导致 `IndexCacheControl` 永远找不到控制通道。改为以 `SparkContext` 身份为键（`IndexCacheDriverRegistry`）。
2. **执行器不认索引类型**：命令里带的 `definition` 由执行器重建适配器，但执行器 JVM 没有注册 `vector`/`text` 类型，每次加载都失败为 `Unknown index type: vector (registered types: [])`。新增 `IndexCacheTypeRegistration`，在插件两侧 `init` 注册。

3. **重用的副本没有执行器归属**：计划只命令"还要放的"副本，已常驻被复用的那些没有命令，于是 COMPLETE 的计划里有 7/13 个文件 holder 为空——而"实际 holders"正是放置核对要读的数。改为任何当前持有该 key 的执行器都回答问题（`181fa1be1`）。
4. **索引 LRU 在真实 key 上永远找不到牺牲者**（最严重）：`holders(indexUuid)` 靠"cache key 里含 indexUuid"匹配，而真实 key 是 `表位置|适配器类|blob 类型|文件`，不含索引 UUID，`canFree` 恒为空 ⇒ 容量驱逐静默失效。测试没抓到是因为夹具把 UUID 拼进了 key；夹具改成生产 key 形状后，旧实现立刻挂 10 个测试（`9de92a8fd`）。

前两个只在多 JVM 下暴露；后两个在单 JVM 也能复现，但被测试夹具的 key 形状掩盖了，单靠审查和单机测试都看不到。

## 观测结果

### 1. 加载、查询、卸载（10M，`acc10m-final-1`，镜像 `2411-9de92a8fd`）

| 观测 | 值 |
|---|---|
| 索引 | `hms.sparkann.sift10m_base` / `sink_b4`（IVF × RaBitQ 4bit，nlist=0 自动，nprobe=150，13 个索引文件，本次现场构建） |
| 应用 | `spark-*`，2 个 executor（各自独立 JVM），`spark.iceberg.index.cache.bytes=4 GiB`，executor 16 GB / overhead 4 GB |
| 加载 | 第 4 次 `load_index` 达到 **COMPLETE**，累计 23.9 s |
| 逐文件归属 | 13 个文件全部有执行器且无缺口：executor 1 → 8 个，executor 2 → 5 个（`reason` 全为 null） |
| 每节点驻留 | executor 1 `used 1,412,760,658 / budget 4,294,967,296`（0.329）；executor 2 `used 1,098,318,635`（0.256）；`reserved=0`、`releasing=0` |
| 均衡 | 最高/均值 = 1.13（#2296 定的门槛是 ≤1.25），且无"一台 >90% 另一台 <50%" |
| 查询（加载态） | top-10 = `[242670, 82189, 42071, 120756, 113129, 260847, 159246, 113752, 126087, 133819]`，49.1 s |
| 卸载 | `offload_index` → **OFFLOADED** |
| 查询（卸载后） | 与加载态**逐行一致**，19.5 s |

### 2. 丢失一个执行器（10M，`acc10m-kill-2`）

| 观测 | 值 |
|---|---|
| 加载 | 第 5 次调用 **COMPLETE**，累计 65.8 s；13 文件归属 executor 1 → 8、executor 2 → 5 |
| 动作 | 加载后删除一个 executor pod（`…-exec-2`） |
| 丢失后驻留 | 被杀节点最后一次报告 `stale=true`、`age 19,159 ms`、`holders 5`、`used 915,269,859`——即**其覆盖已不可信**；存活节点 `stale=false`、`holders 8`、`used 1,412,753,210` |
| 新执行器 | 自动补了一个 executor 3，`holders 0`、`used 0` ⇒ **没有后台重新加载** |
| 丢失后查询 | 与加载态**逐行一致**（78.2 s） |
| 卸载后查询 | 与加载态**逐行一致**（105.0 s） |

`stale` 的判定窗口是 10 s，所以第一次尝试（`acc10m-kill-1`）在杀完立刻读数时仍显示 `stale=false`，看不到"覆盖变未知"；本表的数是在等待 20 s 后取的，这也是把 `--stale-wait-seconds` 加进 smoke 的原因。

### 3. 现存索引无法被规划（影响 #2296 的前提）

10M 表上的历史索引 `b4_np150`、以及 100M 表的各个既有臂（含 HNSW `hnsw`）全部报 `SIZE_UNKNOWN`：它们是"逐文件容量元数据"（#2405/#2406）上线前构建的，manifest 里没有 `fileBytes`/`preparedBytes`，放置层拒绝把它们当作 0 字节，所以 `load_index` 直接 FAILED。**要验收放置与均衡，必须用当前代码重建索引**；这也解释了 100M × HNSW 的 23.67 GB 为什么不能直接拿来测。

另有两点在大表上暴露：`spark.iceberg.index.cache.bytes` 未配置时执行器上报 `budget_bytes=0`（插件按设计只在配置了预算时才通告容量），因此"不设预算"的验收需要改成显式预算；100M 的 Lance 表在当前基础镜像里缺 `org.lance.file.LanceFileReader`（基础镜像只带 lance bundle，未带 SDK），读取会 `ClassNotFoundException`。

## 复现方式

```bash
# 在运行机上（需要 kubeconfig 与仓库 checkout）
cd /data/workspace/sparkann/2411-922e6aa65
KUBECONFIG=... EXECUTOR_MEMORY=16g bash run-index-residency-smoke.sh \
  --run-id acc10m-final-1 --table hms.sparkann.sift10m_base --index sink_b4 \
  --query-table hms.sparkann.sift10m_query --executors 2 --cache-bytes 4294967296 \
  --load-attempts 8 --jars <本 SHA 的 jar 目录> \
  --image hub.i.basemind.com/spark/sparkann-e2e:2411-9de92a8fd \
  --image-digest sha256:cee93627… --build-sha 9de92a8fd --column vector --dimension 128 \
  --metric euclidean --out <输出目录>
```

`--load-attempts` 是必要的：单次 `load_index` 的等待窗口（60 s）短于一次真实装载，第一次调用返回 PARTIAL 属正常，重复调用会复用已确认副本。

## 未覆盖

- **100M / #2296 的放置与均衡**：按 2026-10-27 的决定，等代码稳定后用新镜像与新数据重测；现存 100M 索引因 `SIZE_UNKNOWN` 与 Lance SDK 缺失都无法直接验收。
- 两个并发索引的容量驱逐、快照替换、`OTHER` 驱逐（需要同表至少两个索引同时驻留，且预算要小到能触发驱逐）；本卡只验证了单索引的加载/卸载/节点丢失。
- `local-cluster` 多 JVM 集成测试 `TestIndexResidencyCluster` 已在仓库内，但需要有 Spark 发行版的机器；在本机（无 `SPARK_HOME`）以 assumption 跳过，不作为多 JVM 证据。
