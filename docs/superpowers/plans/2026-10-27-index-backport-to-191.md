<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# 索引能力回溯到厂内 Iceberg 1.9.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让厂内 Spark 3.5.7 集群在**不改变 Iceberg 版本（1.9.1）**的前提下获得向量/文本索引能力，并且镜像里不再替换 parquet/avro。

**Architecture:** 以 `apache-iceberg-1.9.1` 为源码基线，把索引能力（`core` 的 index 包、`index-spark`/`index-vector`/`index-text` 三模块、`spark/v3.5` 的函数/改写规则/物理策略/16 个过程）移植过来；parquet/avro 采用 Spark 自带版本（1.13.1 / 1.11.4）。发布物是一个 shaded runtime jar，镜像只替换 vendor 的 `iceberg-spark-runtime`。

**Tech Stack:** Java 17、Scala 2.12、Spark 3.5、JUnit 5 + AssertJ、现有 Iceberg FileIO/Puffin，不新增第三方依赖。

**Design:** `docs/vector-index-deployment-guide.md` 第 7 章（版本基线与升级路径，2026-10-27 用户已拍板：Iceberg 回溯 1.9.1、parquet/avro 走 Spark 自带版本）。

## Global Constraints

- **不改 Iceberg 版本**：基线是厂内 1.9.1，验收以厂内 jar 的实际行为为准（厂内 jar 类集合与上游 tag 一致，但无法排除方法体补丁）。
- **parquet / avro 跟 Spark 走**：1.13.1 / 1.11.4，镜像不再替换它们；`iceberg-core` 的 avro 是 `api` 级依赖，先用 1.11.4 编译验证。
- **不需要 variant 补丁**：1.9.1 的 `src/main` 里 `variantType(` 出现 0 次，`.as(LogicalTypeAnnotation.variantType(...))` 只存在于 1.12 基线的 fork 中。
- Spark / Scala 固定 3.5 / 2.12，所有 gradle 命令显式 `-DsparkVersions=3.5 -DscalaVersion=2.12`。
- 每卡独立 worktree；只 `git add` 具体路径；审查后 rebase main、主树 `git merge --ff-only`、清理 worktree；不碰其他 Agent 的改动。
- 产品代码改动走 TDD；完成后部署真实镜像并做集群 smoke；没有集群证据不关闭验收卡。
- 新文件带 Apache license header；core 不依赖 Spark；不加 Jackson 注解、不提升已有内部类可见性。

## Review Focus

1. 移植过程中是否**悄悄带上了 1.9.1 之后的上游能力**（`MetadataUpdate`/`TableMetadata`/`TableOperations` 的新增方法）：带上了就会让写侧产出 1.9.1 读方读不了的元数据。
2. `IndexManifest` 读兼容：v1/v2 都必须能读，且旧镜像读不了 v2 这件事要在文档里写清。
3. 索引查询与全表扫描结果**逐行一致**（metric 必须对齐：EUCLIDEAN 用 L2 表达式做真值）。
4. parquet/avro 降级后写入路径是否仍能建表/写 parquet（1.9.1 编译基线是 1.15.2 / 1.12.0）。
5. 缓存与执行器故障下结果不变（丢缓存只影响速度）。

---

## Task 0：可行性实验（不产出产品代码）

**Files:** 临时 worktree，只改 `gradle/libs.versions.toml`。

**做法**：`git worktree add -b exp/191-base ../iceberg-wt/191-base apache-iceberg-1.9.1`，在 worktree 里把 `parquet = "1.13.1"`、`avro = "1.11.4"`，然后：

```bash
./gradlew :iceberg-core:compileJava :iceberg-parquet:compileJava -DsparkVersions=3.5 -DscalaVersion=2.12
./gradlew :iceberg-parquet:test --tests "*Parquet*" -DsparkVersions=3.5 -DscalaVersion=2.12
```

- [ ] **记录结论**：编译是否通过；若失败，列出每个缺失符号（预期最可能来自 avro 1.11 与 1.12 的差异）。
- [ ] **判定**：通过 → 进 Task 1；只在少数几处失败 → 评估补丁量，写清楚偏离上游的点；大面积失败 → 回到设计（可能必须保留 avro 1.12，只对齐 parquet）。

## Task 1：确定移植方式（cherry-pick 试运行）

**Files:** 临时 worktree（同上），不提交。

**做法**：把索引工作的提交（作者 `wayblink`，243 个）按时间顺序 `cherry-pick --no-commit` 到 1.9.1 基线上，只看冲突面：

```bash
git log --reverse --format=%H --author=wayblink > /tmp/index-commits.txt
# 逐个 cherry-pick --no-commit，记录冲突文件与冲突数
```

- [ ] **记录**：冲突集中在哪些文件、冲突总数、哪些是新增文件（预期 0 冲突）、哪些是那 3 个既有文件（`FileFormat.java`、`FormatModelRegistry.java`、`ReadBuilder.java`）。
- [ ] **判定**：冲突可接受 → 采用 cherry-pick；冲突漫延到索引无关文件 → 改为"在 1.9.1 上重建 index 包"（Task 2 的两种实现路径二选一）。

## Task 2：`core` 索引层移植

**Files:** 新增 `core/src/main/java/org/apache/iceberg/index/`（28 个文件，约 4956 行）及对应测试 `core/src/test/java/org/apache/iceberg/index/`。

**Interfaces:** Consumes：1.9.1 的 `TableMetadata`、`TableOperations`、`FileIO`、`MetadataUpdate`；Produces：索引定义/快照/清单/注册表读写，供三模块与 Spark 侧使用。

- [ ] **RED:** 先移植 `TestIndexManifestFiles`、`TestVersionedIndexRegistry`（若依赖 1.9.1 不存在的 API，则先用最小断言替代，并把差异记入计划）。
- [ ] **GREEN:** 移植实现，直到 `./gradlew :iceberg-core:test --tests "org.apache.iceberg.index.*" -DsparkVersions=3.5` 通过。
- [ ] **关键检查**：grep 新增代码里是否出现 1.9.1 没有的上游方法（对照 `git diff apache-iceberg-1.9.1 HEAD -- core/src/main/java/org/apache/iceberg/TableMetadata.java` 之类），有则改为 1.9.1 可用的写法。

## Task 3：三个 kernel / 执行模块

**Files:** `index-vector/`、`index-text/`、`index-spark/`（含 `settings.gradle`、`build.gradle` 的模块声明）。

- [ ] **GREEN:** `./gradlew :iceberg-index-vector:test :iceberg-index-text:test -DsparkVersions=3.5 -DscalaVersion=2.12` 通过。
- [ ] `index-spark` 依赖 `spark-hive_2.12` 与 parquet-column/hadoop；确认版本与镜像一致（parquet 1.13.1）。
- [ ] **注意**：`index-spark` 用到 `Tasks.foreach`、`CloseableIterable`；若 1.9.1 签名不同，在此处适配。

## Task 4：Spark 3.5 集成

**Files:** `spark/v3.5/spark/src/main/{scala,java}` 下 `spark/index/*`、`procedures/*Vector*`、`procedures/*Index*`（41 个文件）；`spark/v3.5/spark-extensions/.../IcebergSparkSessionExtensions.scala` 的函数与规则注入。

- [ ] **RED:** 移植 `spark/v3.5` 的索引测试（`TestVectorIndexProcedures`、`TestRewriteVectorSearch`、`TestNamedIndexes` 等），先跑出预期失败。
- [ ] **GREEN:** 逐个补齐实现：`vec_distance` 函数、`RewriteVectorSearch`、`VectorSearchStrategy`、`VectorSearchExec`、16 个 `CALL` 过程。
- [ ] **通过标准**：`./gradlew :iceberg-spark:iceberg-spark-3.5_2.12:test --tests "*Vector*" --tests "*Index*" -DsparkVersions=3.5 -DscalaVersion=2.12` 全绿。

## Task 5：发布产物与镜像

**Files:** `tools/cluster/Dockerfile`（工具仓 `iceberg-tools`）。

- [ ] `./gradlew :iceberg-spark:iceberg-spark-runtime-3.5_2.12:shadowJar -DsparkVersions=3.5 -DscalaVersion=2.12`
- [ ] `Dockerfile` 只删除 vendor `iceberg-spark-runtime`，**不再删除 parquet/avro**；`image-jars.txt` 重新生成（sha256 全变）。
- [ ] 镜像 tag 带 commit（`<日期>-<sha>`），并在发布记录里写明基线是 1.9.1。

## Task 6：集群验收（六条，缺一不发）

- [ ] `CALL system.list_indexes(table => 'db.t')` 返回全部行（#2756 回归点）。
- [ ] `build_vector_index` 成功；`index_build_status` 终态正确。
- [ ] `ORDER BY vec_distance(...) LIMIT k` 走索引；与 L2 全表扫描的结果**逐行一致**（不要用 `vec_distance` 的默认余弦做真值）。
- [ ] `check_indexes` 无 needs_rebuild。
- [ ] 杀掉一个 executor 后结果逐行一致。
- [ ] `index_cache_status` 可读；未设预算时不 OOM。

## Task 7：基准回归

**Files:** 工具仓 `iceberg-tools/vector-benchmark/`。

- [ ] 10M 网格（Parquet + Lance，量化 1/2/3/4bit，over-sample 1/3/10/30/100），与 `docs/sift-vector-index-benchmark.html` 的数字对比。
- [ ] 判定：召回一致、延迟同量级视为通过；出现显著退化必须定位到具体改动，不许"记录一下差异"就放行。
- [ ] 100M 待 10M 通过后另行决定。

## Task 8：文档收口

- [ ] `docs/vector-index-deployment-guide.md` 第 7 章补上 Task 0/1 的实测结论（avro 1.11.4 是否可用、cherry-pick 冲突面）。
- [ ] 更新 `docs/sift-vector-index-benchmark.html` 的数据来源说明（基线变了）。
- [ ] 按 SPEC 双份存档到 umem。

---

## 与看板的关系

本计划由 #2762（方案设计）产出；实施按 Task 0–8 拆卡，每卡独立 worktree、独立审查。#2762 在所有实施卡 done 之前不进 review。
