# 索引构建 native 内核 N0：单文件原型与逐字节对齐（2026-11-19）

## 结论

- 阶段二的 native 内核（#3144 / N0 #3148）在 Rust 里重建了 IVF 构建的整段：采样、k-means、归属、RaBitQ 量化、payload 写入（含 list-blocks 布局），通过 JNI 从 Java 调用。同一输入下，native 产物与 Java `IvfKernel` 的产物**逐字节相同**，不是容差内接近。
- 证据是 golden 测试：49 个用例，覆盖维度 8/17/32/100/256/2560、1 与 2 bit 码、list-blocks 开关、默认与自定义训练预算（samples/max-iterations/tolerance）、含 NUL 与非 ASCII 的数据文件路径、全同向量造成的空 list、5000×256 与 300×2560 的产品形态；payload 的 SHA-256 全部与 Java 一致，0 失败、0 跳过。模块全套 301 个测试通过（唯一的 skip 是 #3147 新增的"模块已在模块图上时跳过 fallback 用例"，属预期）。
- 方案 §5.4 的 N0 出口是"格式与数值对齐方案被验证"。结论：**可以按字节对齐**，因此"两套 writer 长期腐烂"这个风险不必靠容差门限兜底，可以用 golden 字节比对钉住。后续 native 化换实现（SIMD、流式）时，只要 golden 仍逐字节通过，格式与数值就没有漂移。

## 落地

| 项 | 值 |
|---|---|
| worktree | `/Users/jyxc-dz-0101035/workspace/iceberg-wt/3144` |
| 分支 | `card/3144`（基线 `index@1a92ea349c`，未合并、未推送） |
| 提交 | 分支 `card/3144` 的 N0 提交（`Index: Build the IVF payload with a native kernel (#3148)`），未合并、未推送 |
| Rust crate | `index-vector/native`（cdylib + rlib，`jni 0.21.1` 一个依赖，其余纯 std） |
| Java 桥 | `index-vector/src/main/java/org/apache/iceberg/index/vector/NativeIvfBuild.java`（包内可见） |
| golden 测试 | `index-vector/src/test/java/org/apache/iceberg/index/vector/TestNativeIvfBuild.java` |
| 构建接线 | `build.gradle` 的 `:iceberg-index-vector` 块：`cargoBuildNativeIvf` + `test.dependsOn`，native 源作为 test 输入 |

本轮**不新增任何用户可见参数**，没有接 `ivf-build-kernel`，没有把 native 内核接进 `VectorKernel`；`NativeIvfBuild` 只有包内调用，`available()/load(Path)` 只服务测试。`IvfKernel` 只加了四个包内访问器（`dimension()/options()/scoring()/quantizer()`），没有行为改动。

`cargo` 不在 PATH 或缺 `native/Cargo.toml` 时，Gradle 跳过 cargo 任务，golden 测试因库不存在而 skip（跳过而非失败）；cargo 存在时测试不得跳过。

## ABI

一帧参数、一次分块回调、一帧结果，全部大端（`DataOutputStream` 语义，字符串是 Java `writeUTF` 的 modified UTF-8）。

### 参数帧（Java → Rust）

```
int  magic = 0x4E495650（"NIVP"）
int  version = 1
utf  unitId
int  dimension
int  entryCount
int  dataFileCount
utf  dataFile × dataFileCount
int  listCount            // 已解析的有效值 = IvfOptions.nlistFor(entryCount)
int  trainSampleCount     // 已解析的有效值 = IvfOptions.samplesFor(entryCount, listCount)
int  trainMaxIterations
long trainToleranceBits   // Double.doubleToRawLongBits(tolerance)
int  bits                 // RaBitQ bits，0 表示不量化
long quantizerSeed        // 不量化时 0
int  metric               // 0=L2；N0 只接受 0
bool listBlocks
int  threads              // 有效值 = max(1, min(parallelism, max(1, entryCount)))
```

### 行分块回调（Java ← Rust 调用）

Rust 按块（512 行）调用 Java 的 `VectorChunkSource.fetch(int start, int count, ByteBuffer out)`；`out` 是 Rust 持有的 direct buffer，Java 从 0 起写入、不改变 position、不保留引用。每条记录：

```
int   dataFileIndex
long  rowPosition
float dimension × 4
```

一次 JNI 上跳只发生在块边界，一行的向量不会变成一个 Java 数组。

### 结果帧（Rust → Java）

```
int  magic = 0x4E495652（"NIVR"）
int  version = 1
int  status               // 0=成功；非 0 时 message 是原因
utf  message
int  listCount
int  sampleCount
int  iterations
long entryCount
long nanosRead / nanosTrain / nanosAssign / nanosEncode
int  payloadBytes
byte payload[payloadBytes]
```

`status != 0` 时 Java 抛 `IllegalStateException(message)`；参数帧非法时 `nativeOpen` 抛 `IllegalArgumentException`；Java 回调抛出的异常由 native 侧取出 `toString()` 并并入 `message`，不会静默变成"Java exception was thrown"。

## 逐字节对齐的做法

数值路径不做"差不多"的对齐，而是复刻 JVM 的每一步与求和顺序：

| 位置 | 复刻的规则 |
|---|---|
| 采样 | `stride = max(1, entries / samples)`，`sample[i] = vectors[min(n-1, i*stride)]` |
| 初始质心 | `sample[min(sample-1, (i*sample)/lists)]`，整数运算用 long 语义 |
| 训练赋值 | f32 坐标转 f64，按维度顺序单累加器求和（#3147 把四累加器改回顺序和，否则末位舍入会移动近等距行的归属、重建不复现），每对质心一次 sqrt，距离严格更小才换 list（等距保留更早的 list） |
| 质心均值 | f32 按样本顺序累加，`sums[list][d] / sizes[list]` 为 f32 除法 |
| 停止判据 | 均值与旧质心按位相等（`floatToIntBits`，+0.0/-0.0 不同）才算未变；tolerance>0 时用 `‖new-old‖/(‖old‖+1e-12)` 的最大值比较 |
| 旋转 | `java.util.Random(seed).nextBoolean()` 复现 48 位 LCG；`(v[d]-center[d])*signs[d]` 用 f32，补零到 2 的幂 |
| Hadamard | 原地 f32 蝶形，`scale=(float)(1/sqrt(n))` 后逐元素乘 |
| 量化 | `norm` 为 f64 顺序求和，`largest` 为 f32 绝对值的 f64 最大值，`level = floor(clamp((r+1)*(L-1)/2, 0, L-1) + 0.5)`，`correction` 与 factors 的 f32 转换点与 JVM 一致 |
| 码位 | 按坐标 LSB-first 写入 `bits` 位/坐标，跨字节时读改写两个字节 |
| 字符串 | 数据文件路径用 modified UTF-8：`NUL` 写 `0xC0 0x80`，补充平面字符按两个 UTF-16 code unit 各写 3 字节 |
| payload | 头（3/`ivf`/1 或 2）、维度、（量化时 bits+long seed）、质心、文件字典、entryCount、可选目录、list 依次写出；list-blocks 的目录在写完 list 后回填偏移与成员数，`metadataEnd` 回填到前缀第 8 字节 |

空 list 也必须写出：普通形只写成员数 0；量化形先写全零质心（空 list 的均值）再写 0，Java 与 Rust 在这一处一致。

## 测试与复现

```bash
# Rust 单测：Java 随机序列、writeUTF 字节、Hadamard、码长、端到端 build、线程不变性
cd index-vector/native && cargo test

# golden：同一输入两边构建，payload 逐字节比较
JAVA_HOME=/opt/homebrew/opt/openjdk@17 ./gradlew \
  :iceberg-index-vector:test --tests org.apache.iceberg.index.vector.TestNativeIvfBuild

# 模块全套 + 格式检查
JAVA_HOME=/opt/homebrew/opt/openjdk@17 ./gradlew \
  :iceberg-index-vector:test :iceberg-index-vector:spotlessCheck
```

结果：golden 49/49 通过（1.3 s）；cargo 单测 14 个全过；模块 301 个测试 0 失败、1 跳过（#3147 的 fallback 用例在模块已加载时跳过）；spotlessCheck 通过。

## 与方案 §5.4 N0 的偏差

方案原文的 N0 出口含"读一个数据文件"。本轮输入是"一条数据文件已解码的向量"：Java 侧的 `IndexEntry` 列表经分块回调交给 Rust，Rust 没有解析 Lance/Parquet 文件格式，也没有 FileIO 回调。

理由：读路径是 N1 的决策点（Rust 直读 Lance/Parquet 与 delete 语义，还是 Java 解码后经共享内存流入 Rust），三种方案的实测代价与风险要单独量化；N0 的不可替代风险是格式/数值与 JNI 契约，先把 I/O 押上去会把两个风险耦合，一旦数值对不上就分不清是 I/O 还是算术的问题。分块回调正是 N1 换 I/O 实现的接缝：换成 Rust 发起的 ranged read 或共享内存流，只需要换 `EntrySource` 的实现，`build` 与 payload writer 不动。

## 风险与交给 N1 的未决项

1. **内存**：N0 把一单元的全部向量放在 native 堆上（326k × 2560 × 4 B ≈ 3.3 GB），与 Java `decodeAll` 的持有量同量级。N1 要决定是流式两遍（训练一遍、归属/编码一遍）还是分块 + 压缩存放，否则 24 g 堆的 executor 会被 native 侧挤爆。
2. **I/O 路径**：Rust 直读需要 Lance/Parquet 解码与 delete 语义（位置删除、等值删除）；Java 解码则 I/O 仍受 Spark/JVM 读路径限制。要用"同 3.3 GB 数据的墙钟 + 实现成本 + 多后端影响"做决策，不能只按理论上限选。
3. **metric**：N0 只支持 L2；cosine/dot 的归属走 `VectorScoring` 的另一条路径，需要补实现或在桥上层回退 Java。
4. **线程与确定性**：assign/encode 已按行/按 list 并行且与线程数无关；训练赋值也并行。N1 需要把线程数接到 executor 的核数并量内存峰值。
5. **构建覆盖**：CI 需要 Rust 工具链才有 native 覆盖；没有 cargo 时测试是 skip 而非失败。N2 的镜像构建要把 Rust 版本、目标三元组与产物校验纳入流水线。
6. **两份 writer 的长期腐烂点**：现在靠 golden 字节比对钉住。任何一侧改格式（例如 payload 版本升级）都必须先改 golden，且两侧同步；不能只改一边。
