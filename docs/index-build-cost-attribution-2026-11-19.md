# 索引构建成本归因：iceberg-lance 4 h 45 m 对 pure-Lance 8.5–11.6 min

本文回答一个问题：同一份 103,398,233 行 / 2560 维 / 4-bit IVF+RaBitQ 的数据，为什么 iceberg-lance 建索引要 4 h 45 m，而纯 Lance 只要 8.5–11.6 min（每索引单元 24–33×）。结论按“一个索引单元”（约 326k 行，覆盖 13–14 个数据文件）展开，因为两侧的构建都是按这个粒度切分并并行执行的。

构建耗时不是查询性能结论。两侧的索引语义也不同：iceberg 每文件训练 570 个质心、查询只探 nprobe 个；纯 Lance 的 `num_partitions=1` 时每段只有一个均值质心、查询整段扫描。本文只算构建账。

## 结论

每单元 646 s（ex103m 集群口径）由三块构成：**k-means 训练约 292 s（45%）、归属 assign 约 164 s（25%）、读取/行物化/写出（含 4-bit 编码）约 190 s（30%）**。

差距不在“Rust 比 JVM 快”这一句话里，而在三处不同的地方：

1. **训练工作量差 121×**：iceberg 每单元用 22,230 个样本训练 570 个质心、跑满 **25 轮**（真实数据实测不会提前收敛），共 8.1e11 次距离计算；纯 Lance 每段用 4,568 个样本、只跑 **1 轮**，共 6.7e9 次。这是算法选择，不是实现效率。
2. **同样的归属计算，实现效率差约 25×**：两侧都要把 326k 个向量归到约 570 个质心上，约 4.74e11 次距离。iceberg 是 JVM 标量 double（每个质心还多算一次 `sqrt`），实测 2.19e9 次/s；纯 Lance 的归属在 Rust native 里跑，约 5 s 完成，折合约 54e9 次/s。
3. **读路径差约 11×**：iceberg 每单元 191 s（Lance 格式读 3.3 GB → Spark Row → 装箱 → 编码 → S3 写 425 MB），纯 Lance 整个构建只要 17.4 s（Arrow 零拷贝 + native 量化）。

三项对 620 s 差距的贡献约为：训练 45%、归属 25%、读路径 30%。

## 口径

| 项 | 值 |
|---|---|
| 数据 | ex103m：103,398,233 行 / 4,286 数据文件 / 317 索引单元，每单元 326,177 行；oss32（同表对照）：10,784,901 行 / 33 单元，每单元 326,815 行 |
| 索引 | 4-bit IVF + RaBitQ；`nlist = round(sqrt(entries)) = 570`；训练样本 `39 × nlist = 22,230`；`MAX_ITERATIONS = 25`；`max_entries_per_index_file = 340000` |
| 资源 | 12 executors × 4 core × 24 g heap + 12 g overhead，driver 4c/12 g，`task_cpus = 4`（因此 12 个任务并发） |
| 版本 | 镜像 `oss-bench-20261118-a23f72b9da`（产品 index@a23f72b9da）；纯 Lance 侧用 `977dc9e` 的 lance-spark |
| 内核基准与真实数据探针 | Xeon Gold 6330 @2.0 GHz（AVX-512）、JDK 17、4 线程；产品代码版本 `369f1ccf66`（与镜像同 commit）只加计时，不改语义 |

每单元吞吐的换算：`build_ms × 并发(12) / 单元数`。317 个任务在 12 个槽位上的实际并发均值 11.7，与 12 的偏差 <3%。

## 实测 1：集群构建的 Spark event log

`ex103m-build-4b-1118-1745` 的 event log（43.9 MB）里只有一个构建 stage：317 个 `mapPartitions` 任务。

| 指标 | 值 |
|---|---|
| build_ms | 17,136,283（4 h 45 m 36 s） |
| stage 跨度 | 17,065 s |
| task 墙钟 | 中位 **629.7 s**（min 54.3，max 827.3） |
| task 线程 CPU | 合计 33,892 s（**107 s/task**） |
| GC | 369.6 s，占 run time 的 **0.2%** |
| shuffle / spill | 0 / 0 |
| 12 台 executor 的中位墙钟 | 479.6 / 481.8 / 483.3 / 626.1 / 627.4 / 629.7 / 631.8 / 756.4 / 759.6 / 762.6 / 769.7 / 770.0 s |

节点快慢差 **1.6×**，而 `corr(墙钟, 行数) = 0.27`、行数 CV 只有 5.4%——耗时差异来自节点/争用，不是数据量。如果 12 台都像最快的 3 台，构建约 3 h 30 m；现在多背了约 35%。

## 实测 2：同表 nlist=1 对照（把聚类从单元耗时里减出来）

`oss32` 上两次构建，同一张表、同一镜像、同一资源，32–33 个单元：

| 构建 | nlist | build_ms | 每单元 | 说明 |
|---|---|---|---|---|
| `fmt-nl1-4b` | 1 | 526,263 | **191 s** | 33 tasks，墙钟中位 150.8 s，task 线程 CPU 105.3 s，GC 1.0% |
| `fmt-ivf-4b` | ≈571 | 1,484,952 | **540 s** | 33 个索引文件 |
| 差值 | | 958,689 | **349 s** | 即聚类（训练 + 归属）的成本 |

nl1 构建的 task 线程 CPU（105 s）与 nlist≈571 构建的 107 s 几乎相同，说明聚类跑在 4 个 worker 线程上、不占 task 线程；单元耗时里能减出来的那 349 s 就是聚类的净增量。

## 实测 3：真实数据上的内核分阶段

从 `lance.smoke.ex103m` 的 Lance 数据里导出 12 万行真实向量（`qwen_3_4b_embedding`，fixed-size-list 2560，mean −0.0001 / std 0.0198），用产品内核直接构建一个单元（`nlist=570` 固定，`samples=22,230` 与真实单元相同）：

| 阶段 | 耗时 | 备注 |
|---|---|---|
| decode | 0.8 s | 12 万行 → float[][] |
| train | **369.3 s** | **25 轮，跑满上限**；每轮 14.8 s（32.4e9 次距离 → 2.19e9 次/s） |
| assign | 76.3 s | 1.75e11 次距离 → 2.30e9 次/s |
| encode | 25.7 s | 4-bit 量化 + 4096 点 FHT，214 µs/向量 |
| 合计 | 472 s | 堆峰值 2.9 GB，payload 248 MB |

折算到真实的 326k 行单元（训练与行数无关，归属和编码按行数线性放大）：**train 369 s、assign 207 s、encode 70 s、decode 2 s**。

关键事实：**真实数据上 k-means 跑满了 25 轮上限**。代码只在“连续两轮分配完全不变、质心逐位相等”时才提前退出；在真实 embedding 上 25 轮都没稳定。合成数据（同一个内核、同样的 k/n 比例）9 轮就收敛了，所以“合成数据的轮数”不能拿来估真实构建。

## 实测 4：集群机器上的标量/SIMD 上限

同一台 Xeon Gold 6330，20,000 个向量 × 570 个质心 × 2560 维、4 线程：

| 实现 | 次距离/s | 相对现值 |
|---|---|---|
| 产品当前写法（double L2 + 每质心一次 `sqrt`） | **2.19e9** | 1.00× |
| float32 + 平方距离（去 `sqrt`） | 2.54e9 | 1.16× |
| 质心矩阵转置（内层沿质心连续） | 4.26e9 | 1.95× |
| 转置 + JDK Vector API（AVX-512，16 lane） | **10.18e9** | 4.65× |

这个 2.19e9 和真实数据探针、和集群反推都吻合。也就是说：聚类慢不是被内存或 GC 拖住，就是这条标量循环的原生速率。

## 实测 5：纯 Lance 侧做了什么

| 构建 | build_ms | 每单元 | 聚类 |
|---|---|---|---|
| `num_partitions=1` | 510,402 | 17.4 s | 一个均值质心（流式一趟） |
| `num_partitions≈571` | 696,617 | 26.4 s | **+7.1 s/单元** |

`num_partitions>1` 时每段的质心由 lance-spark 新增的 `SegmentIvfCentroids` 在 **JVM/Scala** 里训练：样本 `8/分区`（4,568，上限 8192），`WorkBudget = 8e9` 使 `iterationsFor(4568, 571, 2560) = 1`，即 **1 轮**；随后把质心交给 Rust native，`IvfBuildParams.maxIters=1`，归属和量化都在 native 里完成。

所以纯 Lance 的 +7.1 s 覆盖了：JVM 1 轮 k-means（6.7e9 次距离）+ native 归属（4.77e11 次距离）+ 多写 5.8 MB 质心表。同样量级的归属，iceberg 要 164–207 s。

## 差距拆解

| 维度 | iceberg-lance | pure-Lance | 倍数 | 性质 |
|---|---|---|---|---|
| 训练距离计算 | 8.10e11 | 6.7e9 | **121×** | 工作量（25 轮 + 39 样本/list） |
| 归属距离计算 | 4.74e11 | 4.77e11 | 1.0× | 相同工作 |
| 聚类吞吐 | 2.2e9 次/s（JVM 标量 double） | ≈54e9 次/s（native SIMD） | **≈25×** | 实现效率 |
| 单元流水线 | 191 s | 17.4 s | **11×** | JVM 读路径 vs Arrow 零拷贝 |
| 每单元合计 | 646 s | 26.4 s | **24×** | |

每单元 646 s 的构成（集群口径，训练/归属按真实数据探针的 369 : 207 比例分摊；流水线按 nl1 实测）：

| 阶段 | 每单元 | 占比 |
|---|---|---|
| k-means 训练（25 轮 × 22,230 样本 × 570 质心） | ~292 s | 45% |
| 归属（326k × 570，4.74e11 次距离） | ~164 s | 25% |
| 读 Lance 3.3 GB + Spark 行物化/装箱 + 4-bit 编码 + S3 写 425 MB | ~190 s | 30% |

## 优化方向

按“实测倍数 × 占比”排序，收益都是每单元口径。

### 1. 训练预算：改默认值，收益最大

现状 25 轮 × 39 样本/list，实测 25 轮全跑满。对齐纯 Lance 的预算（8 样本/list、1–3 轮）后训练距离计算从 8.1e11 降到 6.7e9–2e10，**预期 292 s → 10–45 s，省 245–280 s/单元（38–43%）**，构建 4 h 45 m 降到约 2 h 45 m。

风险与验证：这是拿索引质量换时间，必须同时给 recall@10 与查询延迟；建议先在 oss32 上做 A/B（同表同查询，25 轮 vs 3 轮 vs 1 轮），确认 recall 掉点可接受再改默认值。改动面只有 `IvfOptions.samplesFor` 与 `IvfKernel.MAX_ITERATIONS`，不新增用户可调参数。

若不想动算法质量，可以只加**收敛判据**（“质心相对位移 < ε”而不是“逐位相等”），实测真实数据每轮位移曲线后选择合适的 ε；保守估计能省 5–10 轮，即 70–150 s（11–23%）。

### 2. 归属内核：固定工作量，靠 SIMD/布局

- 质心转置 + 内层沿质心连续：**1.95×**（164 s → 84 s，省 80 s / 12%），纯标量、无新依赖；
- 去 `sqrt`（比较平方距离）：1.16×（省 23 s / 4%），可与上面叠加；
- JDK Vector API（AVX-512 16 lane）：**4.65×**（164 s → 35 s，省 129 s / 20%），但 JDK 17/21 里还是 incubator，产品要用需要反射 + 标量回退，或抬 JDK 底线。

风险：float32 累加与转置后的比较顺序会改变极少数“等距”向量的归属，需验证 recall 不变；Vector API 的反射回退要保证无 native 时行为一致。

### 3. 4-bit 编码：向量化 FHT

实测 214 µs/向量（2560 维补零到 4096 做 Walsh-Hadamard，全程标量），326k 行约 70 s。用 Vector API 批量做 FHT 与量化预期 2–3×，省 35–45 s（5–7%）。收益不如前两项，但改动局限在 `RaBitQQuantizer`。

### 4. 读路径：结构性改造，短期收益小

191 s 里 task 线程 CPU 105 s，其余是 S3 读等待。逐行拆开：装箱成两份 `List<Float>`（Spark Row 一次、`encodeEntryValue` 一次）、`byte[]` 与 `IndexEntry` 的三次拷贝，本地实测每行 20 µs、326k 行约 6.5 s——不是大头。大头是 Spark SQL 扫描与 425 MB 的 S3 写。去掉装箱/拷贝只能拿 10–20%（20–40 s），要拿更多需要 native/Arrow 直读，属结构性改造。

### 5. 集群环境：不改代码

12 台 executor 的墙钟差 1.6×，当前 4 h 45 m 比“全用快节点”慢约 35%。调度上避免混部、把构建放进独占窗口，能省约 1 h。

### 组合预期

| 组合 | 每单元 | 构建 | 相对纯 Lance |
|---|---|---|---|
| 现状 | 646 s | 4 h 45 m | 24× |
| 1 + 2（标量转置） | ~290–380 s | 2 h 10 m–2 h 50 m | 11–14× |
| 1 + 2（含 Vector API）+ 3 | ~240–300 s | 1 h 50 m–2 h 15 m | 9–11× |

剩下的 8–11× 是 native SIMD 与 Arrow 零拷贝的差距；纯 Java 路线到不了纯 Lance 的量级，要再往下必须把聚类/归属搬进 native。

## 未决

1. **集群上真实的迭代轮数**：真实数据切片跑满 25 轮，但用集群总时长按 shai 的 2.19e9 次/s 反推只有 14–16 轮，说明集群节点单核比 shai 的 2.0 GHz Xeon 快 1.4–1.5×（turbo 或 CPU 代差），或真实单元收敛更快。要钉死需要在 executor 里埋点（本文的探针只能在单机上跑）。
2. **流水线的内部拆分**没有单独测：读、行物化、写各占多少，只能给出合计 191 s。
3. 编码的 214 µs/向量依赖 CPU：同一段代码在 M4 上是 40 µs，跨机器比较要打折扣。

## 复现

- 集群 event log：`hdfs://…/user/wanganyang/sparkann/bench/format/ex103m-build-4b-1118-1745/spark-events-lance-iceberg-build/`、`oss32-nl1-build-1118-2004/`（已取回本地解析）。
- 真实数据探针：数据 `shai:/data/wanganyang/kprobe/unit140k.f32`（12 万行 × 2560 float32，来自 `lance.smoke.ex103m`），日志 `probe120k-570.log`；命令
  `java -Xmx5g -cp "classes:<iceberg-spark-runtime.jar>:/opt/spark/jars/*" org.apache.iceberg.index.vector.ProbeFile <file> 120000 570 4`。
- 探针做法：取与镜像同 commit（`369f1ccf66`）的 `index-vector` 源码，只在 `IvfKernel.buildWithMemory` 里加 `System.nanoTime` 分段计时、在 `train()` 里记轮数，并加 `-Dprobe.maxIterations` / `-Dprobe.samplesPerList` 两个开关；其余代码不变。
- 内核微基准（`AssignBench`/`AssignT`/`AssignTV`）与探针源码：`shai:/data/wanganyang/kprobe/`（本地 `/tmp/kprobe` 亦有一份）。
- 构建参数：`iceberg-tools/vector-benchmark/params/ex103m-build-4b.yaml`；同表对照 `params/oss32-nl1-4b-build.yaml`。
