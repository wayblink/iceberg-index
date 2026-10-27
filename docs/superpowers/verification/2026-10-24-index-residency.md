# 索引驻留：真实集群验收记录（#2411 / #2296）

本文件记录在 A 集群（公司 K8s，Spark 3.5.7-STEP-rc2）上，用**独立提交的应用**验证索引驻留控制面与查询路径的结果。所有数字都来自运行现场的 `SMOKE` 行与驱动日志，未用历史值代替；无法观测的项写 null 并说明。

## 环境与产物

| 项 | 值 |
|---|---|
| 集群 | A 集群（`shai` 提交，`spark` namespace，`hms` 生产 HMS） |
| 基础镜像 | `hub.i.basemind.com/spark/spark:3.5.7-STEP-rc2-tpcds-dsdgen-lance-20260416a` |
| 验收镜像 | `hub.i.basemind.com/spark/sparkann-e2e:2411-802713c11`，digest `sha256:7e249a28397a91cdf6cde9db25d8889e4ca19cc5044535cfb471508645db4552` |
| 构建 | main `922e6aa65` + 本卡 `802713c11`（注册表按 SparkContext 键、执行器注册索引类型） |
| 启动 | `spark.plugins=org.apache.iceberg.index.spark.IndexCachePlugin`；`spark.sql.extensions=IcebergSparkSessionExtensions` |
| 工具 | 工具已在独立仓库 `iceberg-tools`：`vector-benchmark/IndexResidencySmoke.java`、`run-index-residency-smoke.sh`（产品仓库里 `tools/` 是一个指向它的本地符号链接，不入版本库） |
| 证据 | `shai:/data/workspace/sparkann/2411-922e6aa65/runs/<run-id>/{meta.properties,driver.log,smoke.jsonl}` |

镜像由本卡的类路径 jar 覆盖基础镜像中 Iceberg/parquet/avro 后重建；插件只能从镜像自身 classpath 载入，`spark.jars` 不足以让 `spark.plugins` 找到它。

## 本卡修出的两个真实缺陷

1. **驱动插件拿不到 application id**：`IndexCachePlugin` 在 `SparkContext` 创建期间初始化，此时 `sc.applicationId()` 仍为空，注册表以它为键导致 `IndexCacheControl` 永远找不到控制通道。改为以 `SparkContext` 身份为键（`IndexCacheDriverRegistry`）。
2. **执行器不认索引类型**：命令里带的 `definition` 由执行器重建适配器，但执行器 JVM 没有注册 `vector`/`text` 类型，每次加载都失败为 `Unknown index type: vector (registered types: [])`。新增 `IndexCacheTypeRegistration`，在插件两侧 `init` 注册。

两个缺陷都只在多 JVM 下暴露，单 JVM 测试（`local[2]`）覆盖不到。

## 观测结果

（见下文各次运行）
