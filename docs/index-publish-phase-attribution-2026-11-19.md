# 索引构建的“发布/列表阶段”为什么从 2.6 s 涨到 42 s：对象存储每操作延迟，不是索引数

本文回答 #3146。现象是：同一张 `local.smoke.mm_lance` 上连续建索引，#3143 的 before/after 两次运行里构建 stage 从 731.99 s 降到 672.04 s（−8.2%），但 CALL 级 `build_ms` 只降 2.0%——差别落在“构建之后、CALL 之内”的那一段，从约 2.6 s 涨到约 42 s；同期独立 `CALL list_indexes` 从 0.9 s 涨到 9.7 s；表上索引数从 5 个涨到 9 个。卡上的问题是：这 42 s 花在哪几段、是否随索引数增长、有没有低风险优化。

本文用两类证据回答：四次历史运行的 CALL 边界（Spark event log 的 SQL 执行起止 + 表元数据提交时间），以及一次在分支 `card/3146` 上加“只打日志的分段计时”（不改语义）后、同表同 definition 的实测构建（2026-11-19 14:28–14:48，索引 `fmt-3146-4b`，表上索引 10→11）。

## 结论

1. **42 s 全部在 CALL 之内、构建结束之后**，拆成“构建结束→发布提交 8.0 s”和“发布提交→CALL 返回 33.9 s”。同一段在 13:12 的三次同表运行里分别只有 1.0–1.8 s 和 1.3–1.9 s（合计 2.6–4.1 s，与卡上的 2.6 s 对得上）。
2. **可优化的是发布收尾的串行对象操作，不是“读全表索引元数据”**。今天的实测把这段拆开：构建后段 7.23 s = `publishSnapshot` 4.07 s（其中 registry 读写 + 表提交 2.04 s、台账清理 0.89 s）+ 构建后 `counts()` 0.03 s + `SparkIndexBuilder` 打点后的收尾约 2.7 s。构建前的 `checkCanBuild` 只有 4.99 s（含一次读 10 个索引 metadata 的 `listIndexes`）。
3. **没有任何证据支持“随索引数增长”**。同一次运行里，构建前的 `createIndex` 用 75.9 s、构建后的 `updateIndex` 只用 2.04 s（建前 9 个索引、建后 11 个，期间还多出一个别人试建的失败索引），相差 37 倍；五次 `list_indexes` 调用（5 个索引 1.90 s、9 个 1.90 s、9 个 9.74 s、9 个 55.6 s、11 个 2.42 s）也不随索引数单调变化。**摆动来自对象存储在分钟级窗口里每操作延迟从 ~8 ms 到 ~3 s（30–100 倍）的变化**，表现为“某一段突然变慢”。
4. 每个索引给 `list_indexes` 增加的固定成本只有 **3 次 GET**（metadata root + manifest list + manifest），快窗口下 11 个索引合计 2.42 s，没有超线性项。
5. 值得改的是**操作数**：构建收尾的台账清理按标记逐个“探存在 + 删除”，约 114 次串行调用；`list_indexes` 约 2/3 的操作只为算 counts；`createIndex` 为校验 `tableUuid` 逐个复读全部索引 metadata。构建后 `counts()` 只要 28 ms，**明确不值得改**。
6. 构建本身在今天也变慢了（stage 884.5 s 对 672.0 s），但那是集群负载，与这 42 s 无关；两者必须分开报，否则会把环境噪声当成构建收益。

## 口径

| 项 | 值 |
|---|---|
| 表 | `local.smoke.mm_lance`：10,784,907 行 / 586 数据文件 / 83.38 GB，`qwen_3_4b_embedding` 2560 维 |
| 索引定义 | IVF + RaBitQ 4-bit，`nlist=0`（round(sqrt(entries))≈570），`max_entries_per_index_file=340000` → 33 个索引文件 |
| 资源 | 12 executors × 4 core × 24 g heap + 12 g overhead，driver 4c/12 g，`task_cpus=4` |
| 历史运行 | `oss32-ab-kd-1119-1312`（before 镜像）、`oss32-ab-km/kt-1119-1312`、`oss32-p4-default-1119-1351`（after 镜像） |
| 今天的分段计时运行 | `oss32-3146-phase-1119-1428`，14:28:50–14:48:48；镜像 `hub.i.basemind.com/wanganyang/sparkann-e2e:oss-bench-20261119-3146-phase@sha256:6ad2dfbb15bd06f801a7b98810e260d95c88cfb4ea80f4964bfa711b504f3ce8`，由 `measure/3146-phase-timing`（`1a92ea349c` + `IDXPHASE` 日志，不改语义）构建，runtime jar sha256 `e27c9296fae4e948c872d8f5f3f0458132cd8d1c94a0e7867c8793e454456092` |
| 计时方式 | 在 build procedure、`publishSnapshot`、`SidecarIndexCatalog` 的 registry/CAS/prune、`TableIndexRegistryPointer`、`VectorIndexes.counts`、`ListIndexesProcedure` 上打 `IDXPHASE <phase> <ms>` 日志；不改产品语义，计时代码不打算合入 |

每操作延迟是“该段耗时 ÷ 代码里该段的 FileIO 调用数”的估算，用来做量级归因，不是精确到单次调用的直方图。

## 数据一：五次运行的 CALL 边界

`CALL` 与 `build_ms` 是 harness 对 `CALL build_vector_index` 的墙钟；`建前段` 是 CALL 开始到构建 Spark stage 提交（历史运行用 event log 的 SQL 执行起点 + driver log 的 stage 提交时间算，今天是程序内计时）；`内部构建` 是产品自己打的 `Built 33 of 33 … in X ms`；`构建后段` = CALL − 建前段 − 内部构建。

| 运行 | 索引数（建前→建后） | CALL | 建前段 | Spark stage 7 | 内部构建 | 构建后段 |
|---|---|---|---|---|---|---|
| `ab-kd-1119-1312`（before） | 5→6 | 740.66 s | 5.03 s | 731.99 s | 733.06 s | **2.57 s** |
| `ab-km-1119-1312` | 7→8 | 506.52 s | 4.59 s | 493.99 s | 497.80 s | **4.12 s** |
| `ab-kt-1119-1312` | 6→7 | 774.17 s | 3.84 s | 762.19 s | 768.06 s | **2.27 s** |
| `p4-default-1119-1351`（after） | 8→9 | 725.56 s | 3.70 s | 672.04 s | 679.72 s | **42.14 s** |
| `3146-phase-1119-1428`（今天） | 9→11¹ | 1,026.80 s | **93.30 s** | 884.51 s | 926.27 s | **7.23 s** |

两点读法：

- 同样的代码，构建后段在 2.27–42.14 s 之间摆动，索引数 5→9→11 与它没有单调关系（9 个索引 42.14 s，11 个索引 7.23 s）。¹ 今天建后多出的两个是本次建的 `fmt-3146-4b` 和同期别人在这张表上试建失败留下的 `fmt-fx-4b`（无 snapshot）。
- 今天这次运行的“慢”发生在**构建前**：建前段 93.30 s（表加载约 3.3 s + `proc.load-and-checks` 4.99 s + `proc.createIndex` 75.91 s + Spark 计划约 9 s）。同一次运行、同样多的对象操作，构建后只花 7.23 s。**“慢窗口”落在哪一段是随机的，与表状态和索引数无因果关系。**

## 数据二：今天的分段计时明细

`proc.return` = 1,026.80 s 是 CALL 全程。它由表加载（`modifyIcebergTable` 里 `loadSparkTable` 读表 metadata，约 3.3 s，在 lambda 之前）+ 下表五个阶段构成，收尾的 `refreshSparkCache`/`closeService` 合计不到 0.2 s。

| 阶段 | 耗时 | 内容 | 估算 FileIO 调用 | 估算每操作 |
|---|---|---|---|---|
| 表加载 | ~3.3 s | `loadSparkTable` 读表 metadata（CALL 内、lambda 之前） | ~2 | — |
| `proc.load-and-checks` | 4.99 s | 解析列、`checkCanBuild`（`listIndexes` 读 10 个索引 metadata） | ~12 | ~420 ms |
| `proc.createIndex` | **75.91 s** | 写 metadata root、注册 registry（含逐个 `loadRegistered` 校验 tableUuid）、CAS 表提交、prune | ~24 | ~3.2 s |
| `proc.build` | 938.51 s | `SparkIndexBuilder.build`（内部计时 926.27 s、stage 884.51 s、stage 结束到 Built 之间 41.8 s 的 driver 侧收尾） | — | — |
| `proc.publish` | 4.07 s | `updateIndex` 2.04 s（写 metadata root 25 ms、写 registry 62 ms、表 refresh 172 ms、表提交 192 ms、prune 153 ms）+ `finishPublication` 0.89 s（台账清理） | ~15 + ~114 | ~136 ms / ~8 ms |
| `proc.counts` | **0.03 s** | 读新索引 manifest list + manifest | 2 | ~14 ms |
| 返回前收尾 | <0.2 s | `refreshSparkCache`、`closeService` | 0 | — |

`createIndex` 与 `publish` 的对比是本次最有价值的一组数：**同一套 registry 写 + 表提交 + prune 协议，构建前 75.91 s、构建后 2.04 s，相差 37 倍**，而索引数只从 10 变成 11。

## 数据三：`list_indexes` 的五次调用

每次调用的读操作是可数的：1 次表 metadata refresh（`pointer.current`）+ 1 次 registry GET + 每索引 3 次 GET（metadata root、manifest list、manifest）。

| 调用 | 索引数 | 窗口 | 总时长 | 估算 GET 数 | 每 GET |
|---|---|---|---|---|---|
| 13:12 kd 建前 | 5 | 快 | 1.90 s | ~17 | ~110 ms |
| 13:52 p4 建前 | 9 | 快 | 1.90 s | ~29 | ~66 ms |
| 13:52 p4 建后 | 9 | 慢 | **9.74 s** | ~29 | ~336 ms |
| 14:30 今天建前 | 9 | 极慢 | **55.65 s** | ~29 | ~1.9 s |
| 14:48 今天建后 | 11¹ | 快 | **2.42 s** | ~35 | ~69 ms |

索引数从 5 到 11 只增加固定 3 次 GET/索引；9 个索引既可以是 1.90 s，也可以是 55.65 s。卡上“list_indexes 从 0.9 s 涨到 9.7 s”因此不是索引数变多的结果，而是那次调用正好落在慢窗口。

¹ 11 个里有 1 个是同期别人试建的 `fmt-fx-4b`（无 snapshot，读 metadata 后 counts 为 0）。

今天这次运行内部还有一个直白的对照：`list.metadata` 第一次花 26.88 s（9 个索引，含客户端/连接的冷启动），9 秒后构建前检查里同样 9 份 metadata 只花约 3.2 s；构建后 11 份 metadata 花 0.50 s。同一进程内冷热差可达 50 倍。

## 数据四：42 s 的构成（p4 运行）

p4 运行没有程序内计时，只能从 CALL 与提交时间推出两段：

- **构建结束→发布提交 8.0 s**：写 metadata root、写 registry 版本、表 metadata refresh + 表提交（`TableIndexRegistryPointer.compareAndSet`）、prune 旧 registry 版本。
- **发布提交→CALL 返回 33.9 s**：`finishPublication`（台账清理）+ 构建后 `counts()` + `refreshSparkCache`/返回。台账按标记逐个删除：33 个 index file 标记 + manifest/manifest list/statistics/complete/started ≈ 38 个标记，每个标记“探存在 + 探存在 + 删除”最多 3 次调用，约 **114 次串行对象操作**；33.9 s ÷ 约 120 次 ≈ 280 ms/op。
- 今天的构建后段合计 7.23 s（`publishSnapshot` 4.07 s + `counts()` 0.03 s + `SparkIndexBuilder` 打点后的收尾约 2.7 s + 返回收尾），台账 114 次只花 0.89 s（8 ms/op）。**结构与调用数一致，绝对值随窗口延迟变化 5–8 倍。**

## 对卡面四个问题的回答

**1. 42 s 里各段占多少？** 建后段合计 42.14 s：发布相关约 8.0 s（registry 写 + 表提交 + prune），发布提交后的收尾约 33.9 s，其中可归到台账清理的约 114 次串行对象操作是大头；`counts()` 只读 2 个对象（今天实测 28 ms），表提交只有一次（`pointer.commit`，今天实测 192 ms）。

**2. 索引数 1→9 的增长曲线？** 不是曲线，是噪声。每索引固定 3 次 GET；实测 5/9/9/9/11 个索引分别 1.90/1.90/9.74/55.65/2.42 s。同一次运行内建前 75.91 s 对建后 2.04 s（索引 9→11）已经排除了“索引数”作为解释变量。两个 585 文件的旧索引确实让 counts 读的 manifest 更大（469 KB，对 33 文件索引的不超过 108 KB），但 manifest 读取今天在 12–66 ms 之间，远小于窗口抖动。

**3. 有没有低风险优化？** 有，按节省的串行操作数排序（详见下节）：台账清理 114→**42** 次（落地值，见下）；`list_indexes` 的 counts 占 2/3 的 GET；`createIndex` 的逐索引 `tableUuid` 复读；prune 从提交路径上挪走。构建后 `counts()` 只有 2 次调用、28 ms，明确不值得改。

**4. 区分“表状态副作用”与“构建本身变慢”？** 是表状态/环境副作用，不是构建变慢。构建 stage 今天 884.51 s（p4 672.04 s）是集群负载造成的，与发布段无关；发布段慢的那次（p4）构建 stage 反而是最快的一次。跨 run 比较构建收益应使用 stage 或产品内部 “Built N of N … in X ms”，不要用 CALL 级 `build_ms`。

## 优化项（按节省的对象操作数排序，实现另开卡）

1. **台账清理（`IndexSnapshotWriter.deleteLedger` / `SidecarIndexCatalog.deleteIfExists`）**：现状每个标记两次探测再删除；`FileIO.deleteFile` 对缺失文件本身是 no-op（S3 删除幂等、HDFS `delete` 返回 false、本地 `deleteIfExists`），去掉探测可把 114 次调用降到约 38 次；再把“一文件一标记”换成“一个 ledger 文件 + 有界并行删除”可降到约 10 次。按今天 8 ms/op 与 p4 280 ms/op 两端估算：p4 那次 33.9 s → 约 11 s → 约 3 s；今天的 0.89 s → 约 0.3 s。风险：需覆盖 `abortAttempt`（失败清理）与三种 FileIO 的删缺失语义，用现有 publication lifecycle 测试扩用例。

   **落地（#3152，`04aa7e298e`）**：没有做成“一个 ledger 文件”，而是让完成标记写下本次写了多少个 file/manifest 标记，清理时读一次、逐个删、只在序列末端补探测：33 文件的一次尝试从约 114 次对象操作降到 **42 次**（1 读 + 37 删 + 4 探测；`TestIndexSnapshotWriterAttempts` 钉住“1 次读、≤7 次探测、总操作 ≤45”这三个上界）。实测墙钟在快窗口基本持平（0.89 s 量级，本来就不是瓶颈），收益按每操作延迟线性放大，慢窗口才看得出来。
2. **`list_indexes` 的 counts**：每次调用 2/3 的 GET（11 个索引 35 次里的 22 次）只为算 `index_files`/`entries`。原来的两条路（把 counts 变成可选的新参数；或让 manifest list 携带 totals）都不需要：manifest list 从 `56d40a3ad1` 起就为每个 manifest 写了 `indexFileCount`/`entryCount`，只是没人用。**已落地（`4d0cb2b537`）**：counts 直接读 manifest list 汇总，每索引 3 读降为 1 读（35 次 → 13 次），无格式变更。快窗口收益 2.42 s → 约 0.9 s；极慢窗口 55.65 s → 约 20 s。
3. **`createIndex`/`register` 的 tableUuid 校验**：为了判断“这些索引是不是本表实例的”，逐个 `loadRegistered` 读全部索引 metadata（N 次 GET）。**已落地（`6f1d21769f`）**：v3 registry 增加可选的 `table-uuids` 映射，注册时先看 registry 记的表实例、只在缺记录时回读 metadata，不升版本、旧 reader 忽略该字段。
4. **`pruneOldVersions` 内联在提交路径上**：慢窗口里 18.71 s、约 10 次调用，纯 housekeeping；失败即放弃或延后不影响正确性。
5. **不推荐改**：构建后的 `counts()`（28 ms、2 次 GET）；表加载（3.3 s，CALL 内 procedure 之外，属于 catalog 读表）与 `refreshSparkCache`/`closeService`（<0.2 s）。

## 局限

- p4 运行的 8.0 s/33.9 s 是从 CALL 边界和表提交时间推出来的**边界值**，不是同一次运行的 `IDXPHASE` 实测；33.9 s 内各子段的拆分依赖代码里的调用数估算。
- 今天这次实测的“快/慢窗口”只覆盖一次运行的 20 分钟，每操作延迟的绝对值依赖集群与对象存储当时状态，不能当 SLA；结论是“这类阶段是操作数 × 每操作延迟”，不是“某段固定 42 s”。
- 今天的构建 stage 884.51 s（p4 672.04 s）说明当天集群更忙，**构建本身的比较不可用**；本文只使用 CALL 内非构建段和同一次运行内的前后对照。
- 今天的 `list_indexes` 建前那次含 26.88 s 冷启动，未单独剥离；表二的“每 GET”是量级估算。

## 复现

- 计时代码：分支 `measure/3146-phase-timing`（基于 `index@1a92ea349c`，只有 `IDXPHASE` 日志、不改语义；`measure/3152-counts` 是它加上本页两项优化的测量分支），镜像 `hub.i.basemind.com/wanganyang/sparkann-e2e:oss-bench-20261119-3146-phase@sha256:6ad2dfbb15bd06f801a7b98810e260d95c88cfb4ea80f4964bfa711b504f3ce8`，runtime jar sha256 `e27c9296fae4e948c872d8f5f3f0458132cd8d1c94a0e7867c8793e454456092`。
- 运行：`~/probe3142/launch-3146.sh` → `vb/run-format-benchmark.sh --params params/oss32-4b-3146-phase.yaml --format lance-iceberg --out runs/oss32-3146-phase-1119-1428 --index-only`；产物在 `~/probe3142/vb/runs/oss32-3146-phase-1119-1428/`（driver log、events jsonl、plan、provenance）。
- 历史四次的证据：`~/probe3142/vb/runs/oss32-{ab-kd,ab-km,ab-kt}-1119-1312/`、`oss32-p4-default-1119-1351/`（driver log + Spark event log）。
- 本次新建的索引：`fmt-3146-4b`（uuid `beb6d01e-0b91-48fc-9805-3431b9113777`，33 个索引文件、10,784,401 条目），留在同一张基准表上，与之前各卡留下的 `fmt-*` 索引一致。同期表上出现的 `fmt-fx-4b`（无 snapshot）不是本次运行建的，未处理。
