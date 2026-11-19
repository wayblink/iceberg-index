# 索引构建内核方案：Java 能力项与 native 构建内核

本文是 [索引构建成本归因](index-build-cost-attribution-2026-11-19.md) 的后续方案。归因结论是：每索引单元 646 s = k-means 训练 292 s（45%）+ 归属 164 s（25%）+ 读取/物化/编码/写 190 s（30%），对纯 Lance 的 26.4 s 差 24×。本文给出两阶段方案：**阶段一只改 Java 侧的内核与并行结构，不动语义；阶段二把整段构建搬进自有 native 内核**。需要拿质量换时间的项一律做成索引参数，默认值全部保持现状。

## 结论

- **阶段一（Java，3–4 周）**：可观测性、并行编码、分块归属、训练预算参数化、读路径并行。默认参数下构建结果与今天逐字节一致；显式打开参数才改变产物。预期每单元 646 s → **420–500 s**，构建 4 h 45 m → **3 h–3 h 35 m**；若同时显式使用推荐训练参数，**210–230 s / 1 h 30 m–1 h 40 m**。
- **阶段二（自有 native 内核，6–9 周）**：Rust 内核按索引单元读数据、聚类、量化、写我们的 payload，Spark 只做调度。预期 **40–60 s/单元、整次构建 20–30 分钟**，落到纯 Lance 的 1.5–2.5×。
- **不采用“复用 lance 的 Rust 仓库”**：从 Java 侧能调到的入口只有 `Dataset.createIndex`，它只产出 Lance 格式的索引文件；只借计算则拿不到 I/O 收益，还要背 native 平台矩阵。理由见 §7。
- 新增 4 个索引参数（训练样本/轮数/收敛阈值、归属内核）与 1 个阶段二才引入的构建内核开关。默认值保持现状；参数随索引定义序列化，重建可复现。
- 每项改动都必须同时给出 `build_ms`、`recall@10`、查询延迟三项证据，缺一项不算完成。

## 1. 现状与目标

每单元（326k 行 × 2560 维、570 个质心、4-bit RaBitQ，4 线程）的实测构成：

| 块 | 现在 | 关键事实 |
|---|---|---|
| k-means 训练 | 292 s（45%） | 真实数据跑满 25 轮上限（探针实测），8.1e11 次距离 |
| 归属 assign | 164 s（25%） | 4.74e11 次距离，标量 double + 每质心一次 `sqrt`，2.19e9 次/s |
| 读/物化/编码/写 | 190 s（30%） | 编码 ~70 s（214 µs/向量、单线程）、读解析 CPU ~27 s、装箱 ~8 s、S3 等待 ~86 s |

对照：纯 Lance 每单元 26.4 s（`num_partitions≈571`）、17.4 s（`num_partitions=1`）。差距 = 训练工作量 121× × 归属实现 ~25× × 读路径 ~11×。

阶段目标：

| 里程碑 | 内容 | 每单元 | 构建（317 单元、12 并发） |
|---|---|---|---|
| M1 | 可观测性 + 并行编码 + 分块归属（精确路径） | 500–515 s | 3 h 35 m–3 h 40 m |
| M2 | SIMD 归属（显式开启）+ 读路径并行 | 420–440 s | 3 h–3 h 10 m |
| M3 | 训练预算参数化（显式使用推荐值） | 210–230 s | 1 h 30 m–1 h 40 m |
| M4 | native 内核 | 40–60 s | 20–30 min |

不含“加 executor”这条资源路线（已明确排除，本文不设计，只在 §7 记录）。

## 2. 设计原则

1. **输出格式不变**：payload 的 `BLOCK_VERSION` / `FORMAT_VERSION` / `QUANTIZED_FORMAT_VERSION` 一律不动，查询侧不需要任何改动就能读新构建的文件。
2. **默认行为不变**：不显式设置新参数的索引，构建产物与今天逐字节一致；这一点在 P1、P2a、P3 的默认路径上由测试保证。新算法（SIMD 归属、native 内核）只能通过显式参数启用。
3. **参数进索引定义**：新参数走 `CALL system.build_vector_index(..., options => map(...))` 的 `options`，落在 `VectorIndexDefinition.properties()` 里，随定义序列化；重建同一索引只要照抄定义就复现。
4. **每项改动三项证据**：`build_ms`、`recall@10`（对精确扫描）、查询延迟；任何一项掉出验收线就回滚。
5. **可观测**：每个索引文件记录分阶段耗时与实际生效的算法参数，运维不用再靠探针。

## 3. 索引参数

参数键与现有 `nlist`、`rabitq-bits`、`ivf-list-blocks` 同域，写在 `options` map 里：

```sql
CALL local.system.build_vector_index(
  table => 'smoke.ex103m_lance', column => 'qwen_3_4b_embedding',
  dimension => 2560, metric => 'l2', structure => 'IVF', quantizer => 'RABITQ',
  name => 'fmt-ivf-4b',
  options => map('nlist', '0', 'rabitq-bits', '4',
                 'ivf-train-samples-per-list', '8',
                 'ivf-train-max-iterations', '3',
                 'ivf-train-convergence-tolerance', '0.001'),
  max_entries_per_index_file => 340000)
```

| 键 | 类型 | 默认 | 取值 | 语义 | 质量影响 |
|---|---|---|---|---|---|
| `ivf-train-samples-per-list` | int | 39 | 1..entries | 每个质心的训练样本数；样本总数 = `nlist × 该值`（受 entries 上限约束） | 越小质心越粗，recall 可能下降 |
| `ivf-train-max-iterations` | int | 25 | 1..100 | Lloyd 轮数上限 | 越小质心越粗；实测默认真实数据用满 25 |
| `ivf-train-convergence-tolerance` | double | 0.0 | 0.0..0.1 | 连续两轮质心相对位移小于该值即停止；0 表示保持现在的“逐位相等”判据 | 越大越早停，通常无损或接近无损 |
| `ivf-assign-kernel` | string | `exact` | `exact` \| `simd` | `exact` 是今天的标量 double 路径（逐字节一致）；`simd` 是分块 + float32 + Vector API 的快速路径 | `simd` 在等距向量上可能改变归属，recall 需验证 |
| `ivf-build-kernel` | string | `java` | `java` \| `native` | 阶段二引入：整段构建用 Java 实现还是 native 内核 | native 与 Java 数值路径不同，recall 需验证 |

校验与兼容：

- 解析、范围校验放在 `IvfOptions`（和 `nlist` 一起），非法值在构建时直接失败并点名字段。
- 查询侧同样通过 `IvfOptions.from(properties, quantizer)` 读定义：实现是**同版本接受并写入**，没有“先接受并忽略”的过渡版本。因此带新键的索引需要**同版本或更新版本**的节点读取；旧版本节点遇到带新键的索引会在打开时抛 “Unknown IVF option”。滚动升级窗口内不要用新参数构建，这一条写进升级说明（用户指南 §4.2 同口径）。
- `ivf-build-kernel` 属阶段二，引入时另行确认（本文只占位，不实现）。

运行事实的记录：`IndexFileMemory.structure` 里增加 `train-samples`、`train-iterations`、`train-tolerance-ppm`、`assign-kernel-simd`、`build-kernel-native` 五个键（该 map 已有 `entry-count`、`list-count` 等，读者按需取键，新增不影响内存估算）。`assign-kernel-simd` 记录归属内核实际跑成了哪一个（JVM 没开 incubator 模块时为 0），`train-tolerance-ppm` 是生效的收敛阈值（百万分之一），`build-kernel-native` 在阶段二之前恒为 0。`list_indexes` 的输出没有 `structure` 列，这些实际值目前只能从构建日志读到（每写一个索引文件一条 `Encoded an IVF payload … over N rounds and M samples`）。

## 4. 阶段一：Java 能力项

### P0 可观测性（0.5–1 天）

`IvfKernel.buildWithMemory` 里已有分段计时的位置（探针用过），把 decode/train/assign/encode 四个耗时、实际轮数、样本数、list 数打进一行 INFO 日志，随“每写一个索引文件”那条日志一起输出；同时写进 `IndexFileMemory.structure`（§3）。不改算法、不改格式。

验收：一次 33 文件构建能在 driver 日志里看到 33 行分阶段数据，且和探针在 120k 真实向量上量到的比例一致。

### P1 并行编码（2–3 天）

现状：`buildWithMemory` 在 4 线程做完 assign 之后，回到 task 线程逐 list 调 `writeQuantizedList`（每个 list 内部再训一个 RaBitQ 中心）。326k 行 ≈ 70 s，只用 1 个核。

设计：把每个 list 的字节序列算成独立 `byte[]`（每个 list 的 RaBitQ 训练、旋转、量化、码写入互相独立），用 `threads = min(parallelism, nlist)` 的线程池并行计算，最后按 list 顺序拼进 payload。数据文件字典只读共享，偏移量在拼接时统一回填。线程数沿用 `spark.task.cpus`，**不新增参数**。

不变量：同一输入、同一线程数下产物与串行实现逐字节相同；线程数 1/2/4 结果相同。

预期：70 s → 20–25 s（每单元省 45–50 s）。

风险：峰值内存增加（同时持有约 425 MB 的 list 字节），在 24 g 堆、4 线程下可控；线程池生命周期要跟任务走，失败要能把异常带回并 abort 构建。

### P2 归属内核（5–7 天，拆两档）

**P2a 分块精确路径（`exact` 的加速版，2–3 天）**：把“每个向量外层、570 个质心内层”改成“向量块（如 16 个）× 全质心”，质心矩阵转置成连续布局；每个质心的距离仍用 double 累加、并按原顺序比较，因此得分与归属**与今天完全一致**（转置只改遍历顺序，不改每个 pair 的算术）。四个独立累加器打断依赖链。实测转置布局在 Xeon 上 2.19e9 → 4.26e9 次/s（1.95×）。

预期：164 s → 70–90 s（省 75–95 s）。默认走这条，无参数、无质量变化。

**P2b SIMD 快速路径（`ivf-assign-kernel=simd`，3–4 天）**：float32 + Vector API（AVX-512/NEON 由 `SPECIES_PREFERRED` 决定），按质心分块、lane 即质心；实测 10.18e9 次/s（4.65×）。JDK 17/21 的 Vector API 是 incubator：用反射加载，类不在或模块未开时自动回退到 P2a，不新增参数控制回退。

预期：164 s → 35–55 s（省 110–130 s）。

风险：float32 与 double 在近等距处会选出不同质心（实测转置+Vector API 版本 checksum 与精确路径不同）；用 recall@10 门限把关（相对 `exact` 下降 ≤0.5 个百分点），超过就只保留 P2a。tie-break 与遍历顺序写进注释和测试。

### P3 训练预算参数化 + 收敛判据（2–3 天）

现状：`IvfOptions.samplesFor` 固定 39/list、`IvfKernel.MAX_ITERATIONS` 固定 25、退出条件是“分配不变且质心逐位相等”。真实数据 25 轮跑满，从不提前退出。

设计：三个参数（§3）落到 `samplesFor`、`train` 的循环上限和退出条件。收敛判据实现为每轮算质心相对位移 `max_d ‖c_new - c_old‖ / (‖c_old‖ + ε)`，当 `tolerance > 0` 且位移 < tolerance 时停止；`tolerance = 0` 完全走现有逻辑，逐字节不变。`structure` 里记录实际轮数与样本数。

预期：推荐值（`samples-per-list=8`、`max-iterations=3`、`tolerance=0.001`）下训练 292 s → 15–30 s（省 260–280 s）；只开 tolerance 不开小样本时，按实测位移曲线预计省 5–10 轮（70–150 s）。

验证：先在 oss32（10.78M 行）上做三组 A/B（默认 / tolerance / 8×3），每组给 `build_ms`、`recall@10`、热态查询 p50；只在 recall 掉点 ≤0.5 个百分点时把推荐值写进用户文档，**默认值不改**。

风险：小样本 + 少轮数在高维 embedding 上可能把 recall 拉低超过门限；那就只保留 tolerance 这一项（质量无损、收益 11–23%）。

### P4 读路径并行（spike 2 天 + 实现 3–5 天）

现状：一个索引单元 13–14 个数据文件，task 内顺序读 3.3 GB（实测约 43 MB/s，等待 ~86 s），再物化行、装箱、编码、写。

设计分两步：

1. **Spike（先量再做）**：在同一个 task 里用线程池并发读该组的 13 个数据文件（每个文件一个 `FileScanTask`，各自 Open/read），量是否能到 100 MB/s 以上；同时确认 delete file 语义、schema 对齐不会被打破。若并发读拿不到 2×，这项就降级为“预取”或不做。
2. **实现**：把 `IndexEntries.entryRows` 的“逐文件顺序 union + coalesce(1)”改成“并行读、按组归并”，行序仍按数据文件顺序稳定；读取与聚类/编码的重叠只做“读在前、算在后”的流水，不改构建语义。

预期：190 s → 130–150 s（省 40–60 s），并为阶段二的 native 读路径探路。

风险：Spark 扫描指标、delete 处理、任务重试语义都要保持一致；并发读会抬高单 executor 的网络与内存峰值（13 个文件同时打开），需要在 24 g 堆下验证。

## 5. 阶段二：native 构建内核

目标：把“读 → 聚类 → 量化 → 写 payload”整段搬进 Rust，Spark task 只做调度与元数据回写。这是把每单元从 ~200 s 压到 40–60 s 的唯一路径。

### 5.1 边界与 ABI

- 入口一个 JNI 函数：`buildUnit(BuildRequest) -> BuildResult`。`BuildRequest` 只带：索引单元 id、数据文件描述符列表（路径 + 行数 + delete 文件引用）、向量列与维度、生效参数（§3）、目标 payload 版本。`BuildResult` 返回 payload 字节、结构计数（entries/lists/codes）、以及各阶段耗时与实际轮数。
- **读路径用 FileIO 桥**：Rust 侧需要数据时通过 JNI 回调到 Java 的 `FileIO` 做 ranged read，由 Rust 的线程池并发发起、自己控制预取。这样 S3/OSS/HDFS 各后端一个都不用改，存储凭证与多后端兼容性留在 Iceberg 侧；代价是每次范围读多一次 JNI 上跳（13 个文件、几百次读，开销可忽略）。
- 备选的“native 直连 S3”只在 OSS/S3 单一后端上做，作为性能优化项后置；正确性先走桥。

### 5.2 格式与数值

- 在 Rust 里实现我们的 payload 布局（header、质心、list 目录、4-bit 码、factors），与 Java 的 `IvfKernel` 共享同一份格式说明。
- 数值路径不做“逐字节相等”的承诺（SIMD 归约顺序不同，k-means 求和顺序也不同）：验收改为**格式合法 + 质心/码结构一致（容差内）+ recall@10 门限**。golden 测试比较两者的 list 大小、码字节数、质心距离矩阵，而不是字节。
- `ivf-build-kernel` 记录到 `structure`，并写入 `IndexFileMemory.readerId`/`modelVersion` 一类标识，查询侧据此可解释数值差异。

### 5.3 平台、构建与灰度

- 产物：linux x86_64（AVX2 基线，AVX-512 运行时分发）、linux aarch64（NEON）；macOS arm64 仅供开发测试。作为可选组件打进现有镜像（镜像里已经有 lance 的 native 库，机制相同）。
- CI：Rust 单元测试 + 与 Java 的 golden 对比 + 跨平台构建；版本号与 Java 侧格式版本绑定。
- 灰度：`ivf-build-kernel` 默认 `java`；先在一张表上 `native` 跑全量构建，比对 recall 与文件大小，再逐表切换；native 库缺失或平台不支持时自动回退 Java 并告警。
- 验收：每单元 40–60 s、整次构建 20–30 分钟；recall 相对 Java 路径 ±0.5 个百分点；连续 3 次 33 文件构建无崩溃、无内存泄漏。

### 5.4 分步交付

| 步骤 | 内容 | 出口 |
|---|---|---|
| N0 | 单文件、无 Spark 的 native 内核 + Java 桥（读一个数据文件、建一个 payload、比对） | 格式与数值对齐方案被验证 |
| N1 | 单索引单元（13 文件、326k 行） | 与 Java 产物 recall 一致、耗时 ≤60 s |
| N2 | 集群全量（317 单元） | build_ms、recall、稳定性达标 |
| N3 | 切换默认（另行确认） | 默认 native，Java 作为回退 |

## 6. 测试计划

只测关键不变量，套件保持秒级：

- 格式与确定性：默认参数下 P1/P2a/P3 的产物与改动前逐字节一致（golden 文件）；线程数 1/2/4 产物一致。
- 参数：非法值报错并点名字段；默认值等价于不写；旧定义（无新键）仍能解析；新键在旧版本上被拒绝的行为写进兼容性测试。
- 训练：`max-iterations=1` 只跑一轮；`tolerance>0` 在位移足够小时提前退出；`tolerance=0` 与现状等价；`samples-per-list` 生效且不越界。
- 归属：`exact` 路径与旧实现逐字节一致；`simd` 路径在同一输入上确定性可复现，与 `exact` 的归属差异率在随机数据上 ≤0.1%、在 recall 门限内。
- 并发：并行编码在 list 数为 1/2/许多、空 list、单质心等边界下产物正确；失败时构建 abort、不留半成品。
- 性能探针：沿用归因报告里的内核探针（120k 真实向量 + 4 线程）作为每次改动的回归基线，记录 decode/train/assign/encode。
- 集群验收：oss32（33 文件、10.78M 行）与 ex103m（317 文件、103M 行）各一次构建，记录 `build_ms`、`recall@10`、热态查询 p50。

## 7. 被否方案

**复用 lance 的 Rust 仓库。** 从 Java 侧能碰到的只有 `Dataset.createIndex`：输入必须是一个 Lance dataset 的 fragment 范围，输出是 Lance 自己的索引文件。三种落法都不成立——只借计算，则 3.3 GB 仍要从 JVM 喂进去、425 MB 拿回来写，190 s 的流水线原样保留，只赚算术那 456 s，还要背 native 平台矩阵；采用 Lance 的索引格式，等于把索引容器、按数据文件刷新、索引目录、查询路径全换成 lance 的，是战略分叉；让 Lance 建完再读回来重打包，则要逆向它不对外暴露的索引内部布局。而且 Lance 之所以快，是它的 Spark task 只当调度器、整段 I/O 与计算都在 Rust 多线程里；只借计算借不到这一点。

**采用 Lance 的索引格式**（同上，战略分叉，单独列为被否）。

**加 executor 换墙钟时间**：有效且零开发，但用户已明确排除（不是内核问题），本文不设计。

## 8. 里程碑与工作量

| 阶段 | 内容 | 预计 | 出口标准 |
|---|---|---|---|
| M0 | P0 可观测性 | 0.5–1 天 | 33 文件构建能看到分阶段数据 |
| M1 | P1 并行编码 + P2a 分块精确归属 | 4–6 天 | 每单元 500–515 s，产物与今天逐字节一致 |
| M2 | P2b SIMD 归属 + P4 读路径 | 6–9 天 | 每单元 420–440 s；`simd` 的 recall 在门限内 |
| M3 | P3 参数化 + 文档/参数参考 | 2–3 天 | 推荐参数下 210–230 s，recall 门限内 |
| M4 | 阶段二 N0–N2 | 5–7 周 | 每单元 40–60 s，recall ±0.5pp |
| M5 | N3 默认切换 | 1–2 周（含灰度） | 默认 native，Java 回退可用 |

阶段一合计约 3–4 周（含集群验收与返工）；阶段二约 6–9 周。

## 9. 风险与未决

1. **参数的前向兼容**：带新键的索引旧版本读不了。实现是同版本接受并写入，没有“接受并忽略”的过渡版本，所以滚动升级窗口内禁用新参数，等所有节点都升到同一版本后再使用。
2. **P2b 的质量与可移植性**：float32 归属在近等距处的差异要靠 recall 门限兜底；Vector API 是 incubator，JDK 版本变化可能改代码生成，需要按 JDK 版本记录基准。
3. **P4 的收益**：spike 与真机 A/B 已完成，实现保留（本机 4 流 122.7 MiB/s 对串行 54.5 MiB/s；集群按 build stage 口径验收，见 #3143 卡面与 §10）；并发读把秒级长尾重叠掉，delete 语义由新增测试覆盖。
4. **阶段二的格式双实现**：Java 与 Rust 两套 writer 是长期腐烂点，必须用 golden + recall 双层测试钉住；`ivf-build-kernel` 的灰度与回退要真的可用。
5. **训练参数的推荐值需要真实召回数据**：本方案只给方法（A/B + 门限）和候选值（8/3/0.001），最终值由 oss32 与 ex103m 的实测决定，且默认值不改。
6. **构建耗时不等于查询性能**：所有优化都只对构建生效；查询侧的 4.2× 差距（#3131 的 nlist=1 对照）是另一个课题，不在本文范围。

## 10. 实现状态（2026-11-19）

阶段一的六项均已实现并合入 `index` 分支：

| 项 | 内容 | 卡 | 提交 | 状态 |
|---|---|---|---|---|
| P0 | IVF 构建分阶段计时，参数与实际轮数写进 `structure` | #3138 | `905759df71` | 已合入 |
| P1 | 一个 payload 的 list 并行编码 | #3139 | `2aca6aba8d` | 已合入 |
| P2a | 向量块 × 质心的精确归属内核（默认路径） | #3140 | `e1c033ff4e` | 已合入 |
| P2b | Vector API 归属内核（`ivf-assign-kernel=simd`） | #3141 | `37a7d76646` | 已合入，默认休眠 |
| P3 | 训练预算三参数 + 收敛判据 | #3142 | `9fc8dfe841` | 已合入 |
| P4 | 索引组的数据文件四流并发读 | #3143 | `ace91a824b` | 已合入 |
| 评审修复 | P2a 恢复顺序求和、并行异常传播、SIMD 测试接线、P4 取消清理与测试 | #3147 | `fed62ab77f`、`db336d853f` | 已合入 |
| N0 | 单文件 native IVF 内核原型 + Java JNI 桥，golden 逐字节一致 | #3148 | `5a6fe35aa2` | 已合入 |
| N1 首版 | native 接入构建路径，新增 `ivf-build-kernel`（默认 java），双路径逐字节一致 | #3149 | `fae5292459` | 已合入；A 线优化（tiled 质心、encode 向量化）进行中 |
| 阶段二 | native 构建内核（N0–N3） | — | — | 未开始 |

- P2b 默认休眠：只有定义里显式写 `ivf-assign-kernel=simd` 才走 SIMD 内核，且执行器 JVM 要带 `--add-modules jdk.incubator.vector`；没开模块时自动回退 P2a 并留一条 WARN，`structure` 的 `assign-kernel-simd` 为 0。
- P0/P1/P2a/P4 不动默认产物；P3 三个键不写时产物与旧版本逐字节一致。
- 阶段二（`ivf-build-kernel=native`）尚未开始，§5 仍是方案而非实现。
