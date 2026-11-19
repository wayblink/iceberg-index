# 索引构建 native 内核 N1：可配置双路径、逐字节一致与单元级性能对比（2026-11-19）

> 本文性能数字是 A1/A2/A3 优化之前的。优化后的最新数据见 [index-build-native-kernel-a-2026-11-19.md](index-build-native-kernel-a-2026-11-19.md)（40k 真实数据 6.53×，326k 单元外推 ≈55 s）。

## 结论

- **双路径可用且产物一致**：新增定义参数 `ivf-build-kernel`（`java` 默认 / `native`）。同一份定义与数据下，两条路写出的 payload 逐字节相同；`structure` 只有 `build-kernel-native`（0/1）不同。native 缺库、非 L2、或运行时异常时自动回退 Java 并告警，构建不会因此失败。
- **性能（真实数据，同一台 x86 机器、同资源、背靠背）**：40,000 行 × 2560 维、nlist=570、4-bit RaBitQ、4 线程、默认训练预算（20 轮收敛、22,230 样本）下，整单元构建 **263,806 ms → 110,527 ms，2.39×**；其中 train 2.29×、assign 4.35×、encode 2.10×，payload 长度与 SHA-256 完全相同（`66d622…`）。
- 本机 M4（20,000 × 2560、nlist=100、4-bit、5 轮）对照为 **1.37×**（1,757 ms → 1,284 ms），payload 也逐字节相同。
- N1 的"每单元 ≤60 s"目标**尚未达到**：按实测吞吐外推到 326k 行的真实单元（native ≈180 s、Java ≈505 s，2.8×）。差距的构成与下一步见"与 N1 出口的差距"。

## 口径（资源对齐）

| 项 | 值 |
|---|---|
| 机器（x86 对比） | `shai` 单机，Xeon Gold 6330，9 vCPU（4 线程），33 GB 内存，Linux x86_64，JDK 17.0.18，Rust 1.94.1 release |
| 机器（本机对照） | Apple M4，10 核，macOS arm64，JDK 17.0.20.1，Rust release |
| 数据 | `shai:/data/wanganyang/kprobe/unit140k.f32`：120,000 行 × 2560 维 float32，来自 `lance.smoke.ex103m` 的真实 embedding；本轮取前 40,000 行 |
| 索引定义 | IVF、L2、RaBitQ 4-bit、`nlist=570`、`ivf-list-blocks=true`、默认训练预算（39 样本/质心、25 轮上限；实际 20 轮收敛、22,230 样本） |
| 线程/内存 | 两个内核都在同一个 JVM 内、`parallelism=4`、`-Xmx3g`；串行先后执行，同一 entries 列表 |
| 计时口径 | 单位毫秒，`System.nanoTime()` 包住整次 `encodeAuxiliaryPayloadWithMemory`；构建 entries 与读文件不计入 |
| 负载说明 | 对比期间机器是共享的，load average 19–21（9 核），JVM 实际拿到约 2.9–3.1 核（286–312% CPU）。**绝对吞吐低于空闲机器的水平，但两个内核在同一时段、同一资源下背靠背测量，比值仍可比**；N2 的集群 A/B 会在独占窗口重测 |

## 实测（shai，40k × 2560，nlist=570，4-bit）

| 阶段 | Java 内核 | native 内核 | 加速 |
|---|---|---|---|
| decode（Java 解码 entries） | 242 ms | 242 ms（同一条 Java 解码路径） | 1.00× |
| read（把解码向量经 JNI 分块交给 native） | — | 895 ms | — |
| train（22,230 样本 × 570 质心 × 2560 维 × 20 轮） | 229,102 ms | 100,134 ms | **2.29×** |
| assign（40,000 行 × 570 质心 × 2560 维） | 30,134 ms | 6,925 ms | **4.35×** |
| encode（RaBitQ 4-bit，40,000 行） | 3,615 ms | 1,725 ms | **2.10×** |
| **合计（wall）** | **263,806 ms** | **110,527 ms** | **2.39×** |

换算成可跨行数比较的吞吐（每次"维度更新"= 一次 f64 减、乘、加）：

| 阶段 | 工作量 | Java | native |
|---|---|---|---|
| train | 6.49e11 维度更新 | 2.83e9 /s | 6.48e9 /s |
| assign | 5.84e10 维度更新 | 1.94e9 /s | 8.43e9 /s |
| encode | 40,000 向量 | 11,065 行/s（90 µs/行） | 23,188 行/s（43 µs/行） |

产物：两边长度都是 94,560,556 B，SHA-256 都是 `66d622234cb045047c983f15e4aa733b3952d0deae98b40a3a139682dd3466c8`；`structure` 除 `build-kernel-native` 外逐项相同（`train-samples=22230`、`train-iterations=20`、`list-count=570`、`payload-bytes=94560556` 等）。

本机 M4（20,000 × 2560、nlist=100、5 轮、3,900 样本）：Java 1,757 ms（decode 125 / train 636 / assign 520 / encode 455），native 1,284 ms（read 171 / train 292 / assign 288 / encode 273），**1.37×**；两边 SHA-256 同为 `a39769…`。M4 上 native 的维度吞吐是 train 17.1e9/s、assign 17.8e9/s。

## 外推到真实索引单元（326k 行、nlist≈570、13 个数据文件）

train 的样本数被 `nlist × 39` 封顶，与行数无关；assign/encode/read/decode 随行数线性放大（×8.15）：

| 阶段 | Java | native |
|---|---|---|
| train（同工作量） | 229 s | 100 s |
| assign | 245 s | 57 s |
| encode | 29 s | 14 s |
| read/copy + Java decode | 2 s | 9 s |
| **每单元合计（外推）** | **≈505 s** | **≈180 s（2.8×）** |

这是外推，不是 326k 实测；N2 的集群 A/B 会给出真实 `build_ms`。

## 双路径与配置

- `ivf-build-kernel=java`（默认）：与 N1 之前完全一致，任何旧定义不写该键都不受影响。
- `ivf-build-kernel=native`：构建时选择 native 内核；以下情况自动回退 Java 并打 WARN，`structure` 的 `build-kernel-native=0` 即为回退发生的证据：
  - 非 L2 metric（native 目前只实现 L2）；
  - native 库不可加载（`System.loadLibrary("iceberg_ivf_native")` 与 jar 内 `<platform>/` 打包资源都失败）；
  - native 构建抛运行时异常（OOM、JNI 错误等）。
- 库查找顺序：`java.library.path` → jar 资源 `org/apache/iceberg/index/vector/native/<os>-<arch>/<lib>` 抽临时文件加载。没有新增除 `ivf-build-kernel` 以外的任何配置项。
- 产物一致性不是承诺而是测试：`TestIvfBuildKernel` 对同一 entries 用两份只差该属性的定义各构建一次，逐字节比 payload，并比 structure（除标志位）；`TestNativeIvfBuild` 的 85 个用例覆盖维度 8–2560、bits 1–5、list-blocks、训练预算、NUL/emoji 路径、空 list、产品形态。

## 实现要点

**Rust 侧（`index-vector/native`）**：N0 的标量与逐位对齐不变；本轮把最近质心计算改成"跨质心 lane + 8 行分块"的精确 SIMD：

- lane 放在质心轴上：一次广播一个维度，与 4/8 个质心同时比较，每个 lane 自己的累加器按维度顺序求和——与 #3147 的标量顺序和**逐位一致**；乘、加分开写，不用 FMA（FMA 只舍入一次，会改末位）。
- 行分块 8 行：一组质心载入后服务 8 行，避免 N0 初版"每行重读全部质心"的缓存浪费（M4 上初版为 0.76×，改成分块后 1.37×）。
- 运行时分发：x86_64 AVX-512F（8 lane）→ AVX2（4 lane）→ aarch64 NEON（2 lane）→ 标量；`is_x86_feature_detected!`/`is_aarch64_feature_detected!` 选择。
- 精确性验证：`cargo test` 在 shai（AVX-512）与 M4（NEON）上比较每个 lane 的距离与标量内核**按位相等**，并在含重复质心/等距行的数据上比较归属。

**Java 侧**：`IvfOptions` 解析与校验、`IvfKernel` 在 `decodeAll` 后按选项分流、两条路共用 `buildResult(...)` 组装 `IndexBuildResult` 与 `structure`（避免两份统计漂移）、`NativeIvfBuild.ensureLoaded()` 与 `buildFromVectors(...)`（复用 N0 的帧与回调，只换向量来源）。benchmark 是 `NativeBuildBenchmark`（test 源集里的 `main`，不属于常规测试）。

## 测试

| 套件 | 结果 |
|---|---|
| `cargo test`（M4） | 18/18，含 SIMD 逐位相等、块边界、线程不变性、端到端 |
| `cargo test`（shai x86_64） | 18/18，AVX2 与 AVX-512F 路径都实际执行并逐位相等 |
| `:iceberg-index-vector:test` | 351 tests / 0 失败 / 1 预期跳过（`withoutTheModuleASimdBuildFallsBackToTheExactKernel`，JVM 已加载模块时跳过） |
| golden 逐字节 | `TestNativeIvfBuild` 85/85；`TestIvfBuildKernel` 14/14（含 COSINE 回退、大小写、非法值、structure 一致） |
| spotless | `spotlessCheck` 通过 |

## 与 N1 出口的差距

- 出口写的是"每单元 ≤60 s"。按当前实测量级，326k 行的单元 native ≈180 s（默认预算），**未达到**。
- 构成按大小：train 100 s、assign 57 s、encode 14 s、read+decode 9 s。train 与 570 个质心、22,230 样本、20 轮和维度成正比，是最大项；`ivf-train-samples-per-list=8`/`ivf-train-max-iterations=3`（#3142 已支持、默认值不变）可把 train 压到约 3 s，单元降到约 83 s——仍差 23 s。
- 要进 60 s 内，下一步优先级：assign 再快 ~2×（AVX-512 分块调优/预取；当前 8.43e9 维度更新/s，约 2.2e9/core，低于该核的算术上限）、encode 再快 ~2×（FHT/量化向量化，当前 43 µs/向量）、以及考虑让 native 直接读数据文件以省掉 Java 解码与 JNI 拷贝（本轮的 read+decode 9 s）。
- 这些不改变"产物一致"的约束：所有优化都必须保持逐位一致，靠 `TestNativeIvfBuild` 的字节比对守门。

## 风险与未决

1. **测量时段机器争用**（load 19–21）使绝对数字偏低；比值是同条件背靠背得到的，但 native 运行在后，若负载在两次之间变化会带来偏差。N2 需要在独占窗口重测，并至少两轮交替顺序。
2. **native 内存**：native 路径除 Java 侧 entries 与解码后的 `float[][]` 外，还持有等价的一份 native 向量（3.3 GB/326k×2560），比纯 Java 多一份。缓解方向是让 Rust 直接从 entry payload 解码（省掉 Java 的 `float[][]` 与 JNI 拷贝），这与"native 直读数据文件"是同一路线上的两步。
3. **仅 L2**：cosine/dot 目前回退 Java；若产品表需要，需要在 Rust 补 `VectorScoring` 的两种打分并重新验证逐位一致。
4. **平台**：x86_64 AVX-512/AVX2 与 aarch64 NEON 都已实测；镜像与 aarch64 Linux 的打包、glibc 基线留到 N2。
5. **参数前向兼容**：`ivf-build-kernel` 已进入 `IvfOptions.KEYS`，旧版本节点读到带该键的定义会拒绝（与方案 §9 的"先接受并忽略"策略不同）；灰度前必须让读写两侧同版本。

## 复现

```bash
# 单元级对比（本机；native 库路径可省略，省略时走 ensureLoaded 的查找）
JAVA_HOME=/opt/homebrew/opt/openjdk@17 java -Xmx5g -cp "<classes>:<test-classes>:<jars>" \
  org.apache.iceberg.index.vector.NativeBuildBenchmark \
  /tmp/unit20k.f32 20000 2560 100 4 1 4 <libiceberg_ivf_native.dylib>

# shai 上的真实数据对比（本轮）
# 代码与 jar 包：/data/wanganyang/native-bench/{classes-main,classes-test,lib}
# native crate：/data/wanganyang/native-ivec（cargo build --release）
# 日志：/data/wanganyang/native-bench/bench-40k.log
java -Xmx3g -cp "classes-main:classes-test:lib/*" \
  org.apache.iceberg.index.vector.NativeBuildBenchmark \
  /data/wanganyang/kprobe/unit140k.f32 40000 2560 570 4 1 4 \
  /data/wanganyang/native-ivec/target/release/libiceberg_ivf_native.so
```
