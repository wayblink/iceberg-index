# 索引构建 native 内核 A 项：tiled 布局、编码向量化与直解码（2026-11-19）

## 结论

- **A1（tiled 质心布局 + f64 行暂存）**：nearest 计算从 6.5e9/8.4e9 提升到 **22.7e9/26.6e9** 维度更新/s（train/assign，约 3.4×）；shai 真实数据 40k 行时 train 6.65×、assign 7.33×。微基准里同算术的 tiled 变体是 shipped 的 3.27–3.57×，与此吻合。
- **A2（RaBitQ encode 向量化）**：encode 从 107.3 µs/向量降到 **22.2 µs**（shai AVX-512，4.8×；M4 21.0→9.4 µs，2.2×；1 bit 默认高到 6.6–7.5×）；三合一实测里 encode 阶段 2,948 ms → **270 ms（10.9×）**（40k 行、4 线程）。
- **A3（native 直解码 entry payload，ABI v2）**：native 路径不再在 Java 侧 `decodeAll`，分配探针显示 132 MB vs 旧路径 173.6 MB；ABI v1/v2 双向不匹配都会 WARN 并回退 Java，payload 始终与 Java 内核逐字节一致。
- **三合一（shai，40k 真实行 × 2560 维、nlist=570、4-bit、4 线程、load≈4.7）**：Java **209,616 ms** vs native **32,082 ms**，**6.53×**；两边 payload 都是 94,560,556 B、SHA-256 均 `66d622…`，`structure` 除 `build-kernel-native` 外逐项相同。
- **外推到 326k 行的真实索引单元**（nlist≈570、13 文件、默认训练预算）：Java ≈348 s vs native **≈55 s**；用 #3142 的 8 样本/3 轮预算则 ≈27 s。**这是外推，集群实测没有达到它**：#3150 的 10M 构建每单元中位 **74.1 s**（见下节），高于 60 s/单元的目标——外推少算了写对象存储与发布段，瓶颈已经转到那里。
- 附带结论：A1 之后，#3142 的"8 样本/3 轮"对构建时间的贡献从约 64 s 缩到约 1 s，**"拿质量换时间"的权衡基本消失**；文档口径应随之更新（默认值仍不改）。

## 口径

与 N1 文档一致：shai 单机 Xeon Gold 6330（9 vCPU，本轮 4 线程）、JDK 17.0.18、Rust 1.94.1 release、数据 `/data/wanganyang/kprobe/unit140k.f32` 前 40,000 行、IVF/L2/RaBitQ 4-bit/nlist=570/list-blocks=true/默认训练预算（20 轮收敛、22,230 样本）、`-Xmx3g`、两个内核同 JVM 背靠背、`System.nanoTime()` 包住整次构建。**本轮终测 load≈4.75（9 核共享机）**，绝对值随负载波动，比值同条件取得。终测日志：`shai:/data/wanganyang/a1a3-bench/bench-a1a2a3.log`。

## A1：tiled 质心布局 + f64 行块暂存

问题（`objdump` + 变体实测）：原布局是维度主序 `[d * 570 + list]`，内层每维度跳 4560 B，每 4 KiB 页只碰 1/64 条缓存线，硬件预取器失效、TLB 抖动；同时每 (行,维度) 的 `vcvtss2sd`+`vbroadcastsd` 占 FP 端口，内层一轮 20 拍而端口下限 12 拍。

改法（`index-vector/native/src/simd.rs`，提交 `c01f706d02`）：

- 布局改成 `[dim/32][group of 8 lists][dim%32][lane]`：固定 group 的一个 tile 是 2 KB 连续块，整块按 tile 顺序流完；lane 方向补到 8 的倍数（570→576），junk lane 填 0 且归约时跳过。
- 缓冲区起点对齐到 64 B（分配时按指针偏移），8-lane 的 `_mm512_load_pd` 是单条对齐 load。
- 每 8 行一个 block，先把 f32 转成 f64 暂存（160 KB/块），内层从暂存广播（单条 load），把转换+广播移出 FP 端口；转换是精确的，顺序不变。
- AVX-512 走一个 8-lane 组；AVX2 拆两个 4-lane 半组；NEON 拆四个 2-lane 四分之一；都按 lane 升序比较，保持标量顺序与"等距取更早 list"。

逐位保证：每个 (行, list) 仍按维度顺序单 f64 累加器、乘加分开、不用 FMA；cargo 测试直接调用每个架构的 kernel 与标量参考比较（含重复/等距质心），在本机 M4 与 shai x86 上都通过。

| 证据 | 结果 |
|---|---|
| 微基准（40k 行 × 2560 维 × 570 list，4 线程） | shipped 4.225 s（7.68e9 pairs/s）→ tiled 1.291 s（25.12e9，**3.27×**）；第二次独立运行 **3.57×** |
| 40k 真实数据、A1 单独（load 6.1） | Java 203,452 ms vs native 36,599 ms（**5.56×**；train 6.09×、assign 5.68×、encode 1.25× 未优化） |
| 三合一终测（load 4.7） | train 190,297→28,627 ms（**6.65×**）、assign 16,046→2,191 ms（**7.33×**）、payload SHA 相同 |

## A2：RaBitQ encode 向量化

基线以产品 `Quantizer::encode` 为准（A1 调查里的 `enc.rs` 因编译期常量被折叠，52.6 µs 不可当基线；真实基线 M4 21.0 µs、shai 107.3 µs）。改动（`quantizer.rs`，提交 `1cf1879830`）：

- FHT 按 padded 尺寸 const-generic 特化（1024/2048/4096/8192/16384），让 LLVM 看到常量 trip count；x86 长度 <16 的 stage 用 `vpermps`+`vpermt2ps`。蝶形仍是同一对元素的加/减，顺序不变。
- 手写 AVX-512（`avx512f`+`avx512vl` 运行时检测）覆盖 center/sign、measure（平方/绝对值/中心点积）、level+correction；逐 lane 的 sub/mul/add/min/max 与标量 IEEE 一致，不用 FMA、不做归约。
- `decode_level` 查表（表项同一表达式生成）；`/2.0`→`*0.5`；`floor(x+0.5)` 在 clamp 后用 `cvttpd`；`norm/largest/center-dot` 三条独立 f64 链合一趟（各自仍逐坐标升序）。
- 位打包替代逐坐标字节读改写（1/2/4 bits 一次乘法、8 bits 直拷、3/5/6/7 通用累加器），与旧 `setLevel` 逐字节一致（有独立 bit-by-bit 对照测试）。
- 线程本地 scratch 复用、code 不再预清零；原实现原样保留为 `encode_reference`（非有限值 fallback 与 property test 基准）。

| 证据 | 结果 |
|---|---|
| 单线程 encode（2000×2560，bits=4） | M4 21.0→9.4 µs（2.2×）；shai **107.3→22.2 µs（4.8×）**；bits=1 默认 shai 138.9→18.6（7.5×） |
| 三合一实测 encode 阶段（40k 行、4 线程） | 2,948→270 ms（**10.9×**） |
| 字节门禁 | x86 上 5,000 与 40,000 行真实数据 Java/native payload SHA 相同；bits 1–8 新旧 encode 校验和两机一致 |

串行地板（shai 实测）：一条 4096 项 f64 加法链 6.35 µs，两条交错仍 6.35 µs；norm 与 correction 依赖 `root` 不能重叠，**约 12.7 µs 是 JVM 求和顺序的硬底**，占 22.2 µs 的 57%。继续压 encode 的杠杆是"一次交 2–4 个向量"让多条链并行（需改 `build.rs`/JNI 调用形态，建议单开卡）；NEON 手写 FHT 还能拿约 15%。

## A3：native 直解码 entry payload（ABI v2）

改动（提交 `29a4fe4244`，合并 `c64ab79fe5`）：

- `ABI_VERSION` 1→2；params 帧在 `listBlocks` 与 `threads` 之间加 `raw_entry_payloads` 布尔；raw 行的记录是 `int fileIndex, long rowPos, byte[8+4*dim]`（entry payload 原样），Rust 侧校验 version==1、length==dimension、坐标 finite 后解码；旧 float 布局保留给既有 golden 测试的桥。
- Java 新增 `buildFromEntries(...)`；`IvfKernel.buildWithMemory` 把 native 尝试提到 `decodeAll` 之前，成功即返回、不再分配 `float[][]`；非 L2、库不可用、native 异常、ABI 不符都 WARN+回退 Java（回退后照常 `decodeAll`）。
- `ensureLoaded()` 装载后校验 `nativeVersion()==ABI_VERSION`，不符视为不可用；`load(Path)` 同校验。

| 证据 | 结果 |
|---|---|
| Java 分配（8000×1024 探针，未提交） | 生产 native 132.2 MB（raw 桥 99.3 + payload clone 32.9）vs 旧路径 173.6 MB（decodeAll 74.1 + 再构建 66.6） |
| ABI 兼容 | 旧库+新 Java：`load` 抛清晰版本错误 → 构建 WARN 回退、flag=0、payload 正常；新库+旧 Java：`nativeOpen` 报 version 1 vs 2 → 旧 Java WARN 回退、两条路 SHA 相同 |
| 字节门禁 | 40k 真实数据 SHA `66d622…` 与非 raw 路径一致 |

## 三合一终测（40k、shai、load≈4.7）

| 阶段 | Java | native | 加速 |
|---|---|---|---|
| decode（Java 侧） | 289 ms | — | — |
| read（raw payload 经 JNI 分块） | — | 772 ms | — |
| train（20 轮 × 22,230 样本 × 570 list） | 190,297 ms | 28,627 ms | 6.65× |
| assign（40k × 570 × 2560） | 16,046 ms | 2,191 ms | 7.33× |
| encode（RaBitQ 4-bit） | 2,948 ms | 270 ms | 10.9× |
| **合计** | **209,616 ms** | **32,082 ms** | **6.53×** |

payload：两边 94,560,556 B、SHA-256 `66d622234cb045047c983f15e4aa733b3952d0deae98b40a3a139682dd3466c8`；structure 除 `build-kernel-native`（0/1）外逐项相同（train-samples=22230、train-iterations=20、list-count=570、payload-bytes=94560556）。

吞吐：train 22.7e9、assign 26.6e9 维度更新/s；encode 约 22 µs/向量（单线程当量）。

## 外推到 326k 行单元

train 的样本数被 `nlist × 39` 封顶（22,230），与行数无关；assign/encode/read 随行数 ×8.15：

| 阶段 | Java | native |
|---|---|---|
| train（同工作量） | 190 s | 29 s |
| assign | 131 s | 18 s |
| encode | 24 s | 2.2 s |
| read / Java decode | 2.4 s | 6.3 s |
| **每单元合计（外推）** | **≈348 s** | **≈55 s（6.3×）** |

用 #3142 预算（8 样本/质心、3 轮）：native 单元 ≈ **27 s**。

## 集群实测（10M，33 个 340k 行单元）

上面的外推在集群上验证过了：镜像 `sparkann-e2e:oss-bench-20261119-071617711d-native`（digest
`sha256:72b33532…1eb227`，`index@071617711d`，native 库由该提交源码编译进 runtime jar），
`local.smoke.mm_lance` 的 10,784,907 行 × 2560 维、33 个 340k 行单元、12 executor × 4c × 24g + 12g、
task_cpus 4、definition 带 `ivf-build-kernel: native`：`build_ms` **236,687 ms**，`Built 33 of 33` 用时
232,953 ms，0 failed；33 个索引文件的 structure 全是 `build-kernel-native=1`（无回退），索引共
17,890,137,090 B。

| 版本（同表、同 33 文件口径、同资源） | build_ms | 相对本轮 |
|---|---|---|
| `071617711d` native（本轮） | **236,687** | — |
| `97dbce8eec` native（已含 A 线） | 246,084 | 慢 3.8% |
| `b3a081bb1e` native（A 线之前） | 420,483 | 慢 1.78× |
| `97dbce8eec` 镜像的 java 臂 | 1,097,833 | 慢 4.64× |
| `13397de974` java（历史） | 740,691 | 慢 3.13× |

每单元中位 **74.1 s**，高于 60 s/单元的目标。按同一形态的分阶段（train 18.1、assign 9.7、encode 1.6、
read 5.1 s，合计约 34.5 s）推算，剩下约 40 s 是每个单元约 542 MB 索引文件写 S3 与发布段——**瓶颈
已经从算术挪到写路径**，继续优化内核对这个形态的收益有限。

## 测试

| 套件 | 结果 |
|---|---|
| `cargo test`（M4） | 29/29 |
| `cargo test`（shai x86_64，AVX-512） | 29/29（AVX-512/AVX2 kernel 直接逐位测试也真跑） |
| `TestNativeIvfBuild`（85 个 golden，raw=false 旧桥） | 85/85 逐字节 |
| `TestIvfBuildKernel`（生产 native 路径） | 18/18（含 raw 入口、ABI 版本、2560×4-bit 产品形态） |
| `:iceberg-index-vector:test` + `spotlessCheck` | 355 tests / 0 失败 / 1 预期跳过；spotless 通过 |
| x86 真实数据字节门禁 | 5,000 与 40,000 行 Java/native SHA 相同 |

## 风险与未决

1. **工具链**：A2 的 AVX-512 intrinsics 需要 Rust ≥1.89；镜像/CI 要钉版本（当前 shai 上 1.94.1）。没有 AVX-512 的主机会退回自动向量化的标量 encode（实测 63.7 µs，仍有 1.7×），但会比 AVX-512 路径慢。
2. **encode 的串行地板**：约 12.7 µs/向量（norm+correction 两条串行链，JVM 顺序和的要求）；批量化是剩余最大杠杆，需改 `build.rs`/JNI 调用形态（新卡）。
3. **A3 的容忍性**：`buildFromEntries` 要求 entry payload 长度恰好 `8+4dim`，否则 native 异常→回退 Java（产物不变，但该次记 `build-kernel-native=0`）；`IndexEntry.payload()` 仍返回 clone，raw 桥每条仍有瞬时复制（32.9 MB 的一部分），彻底消除需 core 加包内访问器。
4. **跨 run 的 payload 字节不同**是读取顺序造成的既有现象（Java 自比也会如此），不是内核差异；本卡用"同 entries 下逐字节相同"与集群 size/recall 判定。
5. **集群实测已完成构建段，召回未测**：10M 上 native 全量构建 236,687 ms、33/33 无回退（见上一节）；recall@10 的两臂对照与 317 单元 ex103m 形态没有跑（集群节点 `gpu-ixv150s-0156` 曾因 ephemeral-storage 反复驱逐驱动 pod，java 臂两次失败、第三次才成功）。切默认内核（#3151）之前应先补这两项。
6. #3142 的"8 样本/3 轮"提速作用在 A1 后基本消失（64 s→1 s），文档里"主要提速手段"的口径需要改。

## 复现

```bash
# Rust 单测与逐位 kernel 测试（M4 与 shai x86 各一次）
cd index-vector/native && cargo test

# 三合一单元级对比（shai）
# crate: /data/wanganyang/a1-tiled（rsync 本提交的 native 源码后 cargo build --release）
# classes/jars: /data/wanganyang/a1a3-bench；数据 /data/wanganyang/kprobe/unit140k.f32
java -Xmx3g -cp "classes-main:classes-test:lib/*" \
  org.apache.iceberg.index.vector.NativeBuildBenchmark \
  /data/wanganyang/kprobe/unit140k.f32 40000 2560 570 4 1 4 \
  /data/wanganyang/a1-tiled/target/release/libiceberg_ivf_native.so
# 日志：/data/wanganyang/a1a3-bench/bench-a1a2a3.log
```

提交：分支 `card/3149-a` 上的 A1 `c01f706d02`、A3 `29a4fe4244`、A2 `1cf1879830`，合并 `c64ab79fe5`、`97dbce8eec`；这些在合入时被 rebase 成 `index` 上的 `f524d24001`（A1）、`ac1bf1692f`（A3）、`543deb87af`（A2），复现请用后者。
