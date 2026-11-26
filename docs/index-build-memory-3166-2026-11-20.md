# 索引构建内存峰值与热路径优化（#3166）

日期：2026-11-20 起，2026-11-23 收口
分支：`card/3166`（基于 `index`@`d62ff69774`）
提交：`487b391b48`、`2a30e23fbf`

## 结论

**ex103m（103,398,233 行 × 2560 维，317 个索引单元）的构建配置定为：每个 executor「16g 堆 + 12g
overhead」（28 GiB 容器），并用 `spark.executorEnv.MALLOC_ARENA_MAX=2` 限制 glibc 的分配 arena。**
该组合实测 **317/317 全部建完、0 次 OOMKilled、0 次 Lost executor，`Job 5` 30 分 40 秒**；同一张表
的旧基线是 4 小时 45 分、且在 24g 堆的旧配置下被 cgroup 反复 kill。

四条实测结论：

1. **吃满容器的是常驻堆，不是构建的临时对象。** `-Xms = -Xmx = 24g` 让堆区 RSS 恒为 24.5 GB
   （`jcmd GC.heap_info` 显示活跃对象只有 11.2 GB），36 GiB 容器只剩 11.5 GB 给全部非堆。
2. **非堆增长来自 glibc 的 arena，不是 jemalloc。** 镜像里没有 jemalloc（`libjemalloc*` 不存在、
   `LD_PRELOAD` 未设，glibc 2.35），`MALLOC_CONF` 是空操作。真正有效的是 `MALLOC_ARENA_MAX=2`：
   28 GiB 下不给它时 206/317 就被 46 次 OOM 打挂，给了它则全程零 OOM。
3. **两个阈值旋钮（`MALLOC_MMAP_THRESHOLD_`、`MALLOC_TRIM_THRESHOLD_`）没有贡献。** 32 GiB 下单独
   开它们反而最慢（47 分钟 vs 33 分钟）；28 GiB 下与只开 `ARENA_MAX` 相比，耗时差 0.5 秒。
4. **这个设置只能放在环境变量里，不能内置进代码。** 试过在 native 库的 `.init_array` 构造函数里调
   `mallopt(M_ARENA_MAX, 2)`（提交 `5f37606b10`，已回退 `cbab9cb554`）：28 GiB 下 258/317 时仍有
   42 次 OOM，和不设置几乎一样。原因：`M_ARENA_MAX` 只在新建 arena 时检查，而 JVM 启动时已经按
   线程建好一批 arena，库被 dlopen 时再设置收不回来。

**代码侧的去拷贝改动是正向但非必要**：payload 字节完全不变、本地构建快 16–18%、每单元少 3.3 GB
堆内垃圾；集群上把“无旋钮”时的 OOM 从 10 次压到 4 次，但单靠它救不了 OOM。

## 现象与证据

`#3164` 在 ex103m 上实测：

- 旧规格 12×(4c, 24g+12g) 下新构建内核的 executor 被 cgroup OOMKilled（JVM 轮 12/12 全灭、native 轮
  20 分钟 48 次）；
- 同规格旧版本（`a23f72b9da`）0 次 OOM；
- 采样：堆常驻 24.5 GB，非堆随存活时间单调爬升（JVM 内核约 0.15 GB/分钟、native 约 0.9–1.0 GB/分钟）。

## 根因

| 位置 | 内容 | 量级 |
|---|---|---|
| Java 堆 | `groupEntries` 的 326k 个 payload 字节数组 | ≈3.3 GB |
| native | `Vec<Vec<f32>>`：每行一个 10 KB 分配 | ≈3.3 GB（326k 次分配） |
| native | 570 个 list body + 整包 payload + 结果帧 | 各 ≈424 MB |
| Java 堆 | JNI 返回的 frame 再复制一份 payload | ≈424 MB |

①`-Xms=-Xmx=24g` 把堆钉在 24.5 GB，只剩 11.5 GB 给非堆；②每单元 3.3 GB 向量以 326k 个小块分配，
释放后由 glibc 各 arena 留着；③payload 同时存在 4 份。②③是代码能改的，①只能改配置。

## 改动清单（代码）

| # | 改动 | 位置 |
|---|---|---|
| A | 向量改单块连续 arena，行是它的切片 | `native/src/build.rs`、`kmeans.rs` |
| B | payload 直接追加进结果帧并回填长度，只保留一份 | `native/src/payload.rs`（`write_into`）、`jni_bridge.rs` |
| C | payload 与 570 个 list body 按精确大小一次预留 | `payload.rs`（`size`）、`build.rs`（`body_bytes`） |
| D | Java 侧从帧里 slice 取 payload，不再复制 | `NativeIvfBuild.decode` |
| E | 热路径不再每行 clone payload | `IndexEntry.payloadView()`、`EntryPayloadSource` 等 |
| F | 构建后立即释放 groupEntries | `SparkIndexBuilder` |

## 证据

### 容器规格阶梯（ex103m，317 单元）

| 容器 | 堆/overhead | 配置 | 结果 | `Job 5` |
|---|---|---|---|---|
| 36 GiB | 16g/20g | **零旋钮** | 完成但 10 次 OOM（优化前）/ 4 次（优化后） | 31–33 分 |
| 32 GiB | 16g/16g | 三项旋钮 | 0 OOM | 35 分 09 秒 |
| 32 GiB | 16g/16g | 只开两个阈值 | 0 OOM | 47 分 19 秒 |
| 32 GiB | 16g/16g | 只开 `ARENA_MAX` | 0 OOM | 33 分 26 秒 |
| **28 GiB** | **16g/12g** | **三项旋钮** | **0 OOM** | **30 分 40 秒** |
| **28 GiB** | **16g/12g** | **只开 `ARENA_MAX`** | **0 OOM** | **30 分 40 秒** |
| 28 GiB | 16g/12g | **零旋钮** | **失败**：206/317，46 次 OOM、25 次 lost，EXIT=2 | — |
| 28 GiB | 16g/12g | 内置 `mallopt`（零环境变量） | **失败**：258/317，42 次 OOM、22 次 lost | — |

### 代码改动本地证据（100k×2560，`-Xmx4g`）

| 指标 | 改动前 | 改动后 |
|---|---|---|
| payload sha256 | `d2fe686d…6232` | 同左（逐字节相同） |
| native 单轮 | 2144 / 1963 ms | 1759 / 1646 ms |
| 同 JVM 三轮 `rssAfter` | 3.15 → 3.55 → 3.70 GB | 3.49 → 3.49 → 3.41 GB |

`index-vector` 全模块 355 个测试、`core` 39 个、`index-spark` 15 个，0 失败。

## 未做项与边界

- `-Xms` 不能用 `spark.executor.extraJavaOptions` 覆盖（实测该选项排在 `-Xms24576m` 之前），要降堆
  只能改 `executor_heap`/`executor_overhead`。
- `MALLOC_*` 只能用 `spark.executorEnv.*` 传；`spark.kubernetes.executorEnv.*` 会被 Spark 静默丢弃。
- S3A 在写索引快照 marker 时会偶发挂起（栈停在 `S3AFileSystem.listObjects ← innerCreateFile`），与
  内存无关；`fs.s3a.create.performance=true` 未绕过。构建耗时应以 `Job 5 ... took` 为准，`build_ms`
  会被这类挂起污染。
- 未测 24 GiB 以下的规格。

## 复现

```bash
# 镜像：product HEAD 2a30e23fbf 的 native 库
IMAGE=hub.i.basemind.com/wanganyang/sparkann-e2e:oss-bench-20261120-2a30e23fbf-native
# 参数：executor_heap 16g / executor_overhead 12g
#       spark.executorEnv.MALLOC_ARENA_MAX=2
POD_PREFIX=fmt-1612 ./run-format-benchmark.sh --params params/ex103m-1612.yaml \
  --format lance-iceberg --out runs/ex103m-1612 --index-only
```
