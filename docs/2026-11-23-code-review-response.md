# 2026-11-23 codex 代码审查回执

**审查对象**：`981d0d5dc..81e2a59f49`，72 个提交、150 个文件。
**核查基线**：仓库 `/Users/jyxc-dz-0101035/workspace/iceberg`，分支 `index`，HEAD `81e2a59f49`。
**回执方式**：逐条对照源码与调用链核实；未修改任何文件，未运行测试。文中"属实"指代码层面成立，不代表我方做了本地复现（CR 的复现结果未被推翻，也没有被复跑）。
> **后续修正（21:43 codex 复核后）**：第 1 条改为 P1（表函数的 `columns` 可传逆序，SQL 入口可触发）；第 2 条改为 9 次注册即触发（证据在 D−1，我此前按 D 计算少了一格）；第 9 条竞态窗口含对象存储 I/O，P2 成立。本文其余内容保持首轮记录，冲突处以 `2026-11-23-code-review-closure.md` 为准。

**总体结论**：15 条中 11 条属实、3 条部分属实（严重度下调一档）、1 条不成立。5 条 P1 里 4 条接受并排入修复；第 3 条接受现象、不接受"恢复旧布局兼容"的结论。P2 中第 11、12 条下调为 P3，第 14 条不成立。

## 1. 逐条结论

| # | 标题 | CR 定级 | 我方定级 | 结论 |
|---|---|---|---|---|
| 1 | Parquet 回表列值配错 | P1 | P1（契约缺陷，现网未触发） | 接受 |
| 2 | 旧 writer 恢复已删索引 | P1 | P2 | 接受，复现步数需修正 |
| 3 | 新旧布局共用标识 | P1 | P2 | 部分接受 |
| 4 | 导入文件丢名称映射 | P1 | P1（前提见下） | 接受，属回归 |
| 5 | 单查询候选窗口漏行 | P1 | P2 | 接受 |
| 6 | 单查询把完整行收回 driver | P2 | P2 | 接受 |
| 7 | 未覆盖文件整份入堆 | P2 | P2 | 接受 |
| 8 | 名字与 UUID 冲突致 drop 删错 | P2 | P2 | 接受，影响更重 |
| 9 | 并发构建注册同名索引 | P2 | P3 | 接受现象，定级下调 |
| 10 | Lance endpoint 合并顺序 | P2 | P2 | 接受 |
| 11 | 批量 reader 错误声明支持位置 | P2 | P3 | 部分接受 |
| 12 | SIMD 距离溢出 | P2 | P3 | 部分接受 |
| 13 | Rust/Java RaBitQ 编码不一致 | P2 | P3 | 接受 |
| 14 | `IvfOptions.samplesFor` 破坏兼容 | P2 | 不成立 | 反驳 |
| 15 | native 加载失败残留临时库 | P2 | P3 | 接受（条件成立） |

## 2. 接受的条目

### 第 1 条：回表列序 —— 属实，但现网调用点是潜在缺陷

机制已确认：`fetchPlan` 在 `IndexEntries.java:827` 用 `table.schema().select(read)` 构造投影，而 `Schema.select` 走 `internalSelect` → `TypeUtil.select`，**保留输入 schema 的字段顺序**（`api/src/main/java/org/apache/iceberg/Schema.java:523-543`、`api/src/main/java/org/apache/iceberg/types/TypeUtil.java:89-104`，后者注释明确写"preserves the field ordering of the input schema"）。`readSchema != null` 时 `rowSchema = schemaFor(read, readSchema)` 按 `read`（请求序）生成（`IndexEntries.java:830-831`）。`rowOf` 按投影序填值、挂上请求序的 schema（`1627-1636`），`asValues` 再按名字取值（`1886-1892`），于是顺序不一致时值被配到错误的字段名上。CR 的复现路径成立。

补充一点 CR 未写的前提：现网四个调用点都按表定义顺序传参，因此目前不会触发——`TextSearchSource.scala:135-136`（Spark 输出序）、`VectorSearchBridge.scala:905`（`columnNames` 直接取表序）、`IndexSearch.java:230-238`（表序）、`DistributedIndexSearch.java:755-761`（`projection.columns()` 表序）。所以这条应按"API 契约缺陷"修：`rowsAt` 的 Javadoc 写着"the columns to return, in this order"，任何逆序调用都会静默串值。修法是复用同文件 `projectionOf(table.schema(), read)`（`277-286`），并补一个逆序投影的回表用例。

### 第 2 条：恢复已删索引 —— 属实，但复现步数要改

机制成立：`restoreLostRegistration`（`SidecarIndexCatalog.java:409`）依赖 `namedInHistory`（`458`）判断，后者最多走 `VERSIONS_TO_KEEP = 10`（`94`）步，`pruneOldVersions`（`1183-1205`）保留最近 10 个版本文件。删除版本 D 被剪掉需要之后有 k ≥ 10 次提交（CR 写的是 9 次；按代码推 9 次时 D 恰好还在 10 步之内，`namedInHistory` 会命中并拒绝恢复）。请复核本地复现实际注册了几个索引。

结论仍是缺陷：删除证据一旦出窗，代码选择"恢复"，与"drop 掉的就不该恢复"的意图相反。建议落墓碑（drop 时持久化 dropped UUID，或保留一份删除记录），而不是加遍历深度；"证据不足时拒绝恢复"是另一个决策，因为它会反过来改变 #3190 修复的路径。

### 第 3 条：布局共用标识 —— 现象接受，结论部分不接受

事实部分认同：`FORMAT_VERSION = 1`、`MAGIC = 0x424D3101`、readerId `text/bm25/1`（`TextIndexPayload.java:55-57`、`TextIndexAdapter.java:209-210`），旧布局文件会被 `matches()`（`312-314`）当成本布局，`check_indexes` 也识别不出；打开后只会在 `open()` 的尺寸检查处以 `Invalid BM25 payload size`（`320-323`）失败，或在尺寸够大时从头读到错误偏移。

不接受的部分："建议为新布局设置独立的持久化标识，在查询前识别需要重建的旧索引"如果被理解为恢复旧读路径/兼容，与 2026-11-23 的设计决定冲突——`docs/2026-11-23-bm25-payload-design.md` 明写"旧两种布局的代码与读路径已删除，不做向后兼容"，且旧布局只写进过实验表（`local.smoke.mm_lance`，11-20 构建），没有对外发布。我方接受的做法是**让旧文件可识别**：签名可区分（换版本号或加布局 tag），识别后给出"需重建"的明确报错。这样既不恢复兼容，也不会让旧索引以难懂的错误出现在查询里。

### 第 4 条：导入文件丢名称映射 —— 属实，且是回归

这条是我方这次回表改造（`3ba522674b`）引入的，认。证据：改造前 `positionedRows` 对 parquet 走 Spark 原生读（`git show 3ba522674b^:index-spark/.../IndexEntries.java` 的 958-1010 行；当前文件仍保留在 `1096-1139`，供 `liveRowsOf` 使用），`readParquet` 用 `spark.read().schema(readSchema).parquet(...)`（`1151-1164`），Spark 按列名读，无字段 ID 的导入文件也能读。改造后 `FileFetch.liveRows` 对所有格式统一走 `readFileWithFormat`（`874-881`、`1639-1677`），builder 只有 `setAll` 与 `project`，没有 `withNameMapping`；同文件的 `textFilterPositions` 是从 `DEFAULT_NAME_MAPPING` 解析后传了的（`1512`）。

定级依赖一个前提：`add_files` 导入的无字段 ID 表是否算支持场景。11-21 我方刚评估过"存量 Parquet 套元数据做向量检索"这条业务线，如果是，按 P1 修；否则可降 P2。请 codex 确认这条能力在当前产品口径里是否在范围。

### 第 5 条：单查询候选窗口 —— 属实，P2

`searchRowsFromHits`（`DistributedIndexSearch.java:688`）的窗口从 `opening` 起，每轮翻倍，最多 `MAX_ROUNDS = 10`（`733`、`1305`），`k=1`、`refine_factor=1` 时依次是 1、2、4…512，第 513 条看不到。该分支允许 postfilter（`VectorSearchExec.scala:148-150` 只排除了 prefilter），所以"后置过滤把前 512 条全部拒掉"的场景成立，`rows` 为空且 `moreHits` 为真时循环直接结束。定 P2 而非 P1：后置过滤的 ANN 本来就可能不足 k，但"另一分片的第一条满足条件却返回空"是可避免的。建议终止条件改为候选耗尽（`hits.size() < window`）或索引条目上限，并处理 `window * 2` 溢出。

### 第 6–10、13、15 条

- **第 6 条属实**：`searchRowsFromHits` 返回 `List<Row>`，`rowsAt` 内部 `rows.collect()`（`IndexEntries.java:640`）把完整投影收回 driver，再 `createDataFrame`。`refine_factor` 默认 1 时窗口就是 k，安全；文档建议把它放大到 4000（`docs/vector-index-deployment-guide.md:120`）时，k=100 即 40 万行 × 宽向量列，会撞 `spark.driver.maxResultSize`。补充：该类注释写"Neither branch crosses the driver barrier with rows"（`VectorSearchExec.scala:52-54`），与实现矛盾，说明是漏改而非设计。
- **第 7 条属实**：三处累积都在——`readFileWithFormat` 整文件入 list（`1674-1685`）、`FileFetch.liveRows` 再攒投影行（`886-903`）、`flatMapToPair` 再攒 task 级 list（`755-764`）。`liveRowsWithPositions` 用空 positions 表示"整文件"（`936-953`），生产调用方是 `VectorSearchBridge.scala:771` 的未覆盖文件补扫。第三处是 task 内累积，不是 driver collect，这点与 CR 描述略有出入，不影响结论。
- **第 8 条属实，且比 CR 写得更重**：创建时只比 `metadata.name()`（`IndexSelection.java:158-171`），`findByNameOrUuid` 按注册顺序先匹配（`172-180`），所以名字撞 UUID 后不只是 `drop_index` 删错（`DropIndexProcedure.java:100` 默认 purge=true，文件被清），**按名字搜索也会静默命中 A**，表现为精度异常而非报错。
- **第 9 条属实，定级下调**：名称检查在 procedure 层（`BuildTextIndexProcedure.java:126`），注册在 `149-166`，中间只有几十行无 I/O 代码，竞态窗口是毫秒级而非整个构建期。建议在 registry 的 CAS 重试里复核名称唯一性。
- **第 10 条属实（条件成立）**：`StorageProperties.resolve` 用 `LinkedHashMap.putAll`（`101-109`），更新已有键保留原位置；`LanceStorageOptions` 把 `s3.endpoint` 与 `fs.s3a.endpoint` 都翻译成 `endpoint`（`49`、`59`），单遍迭代后写者覆盖（`88-101`）。所以当 Hadoop 配置也带 `fs.s3a.endpoint` 时，表里同拼写的值在前、catalog 的 `s3.endpoint` 在后，最终用 catalog 地址。该文件已有"逃生口最后覆盖"的两遍范式（`119-130`），把同义键归一化后按来源合并即可。
- **第 13 条属实**：Rust 用 `f64::min/max`（`quantizer.rs:322-327`），NaN 时返回另一操作数（level = levels-1）；Java 用 `Math.max(0.0, Math.min(...))`（`RaBitQQuantizer.java:281-287`），NaN 传播后 `Math.round(NaN) = 0`。1 bit 时两端字节差 1（3 vs 2）。这打破了 native 模块"matching the JVM's writer bit for bit"的承诺（`quantizer.rs:18-30`），虽然真实数据到不了 1e38。建议单独判 `isNaN` 后按 Java 规则取 0，而不是依赖 min/max。
- **第 15 条属实（条件成立）**：`ensureLoaded` 只在成功时 `LOADED.set(true)`（`NativeIvfBuild.java:227-233`），失败只抑制日志（`237-245`），`loadFromResource` 每次 `createTempFile` + `deleteOnExit` + 复制 + `System.load`（`284-288`）；粒度是每个 build unit（`IvfKernel.java:328`）而不是每 JVM。默认 `java` 内核不触发，只有"请求 native 且库存在但加载/ABI 探测失败"时才持续产生临时文件。

## 3. 部分接受的条目

### 第 11 条：`supportsPositions` 无条件 true —— 契约缺陷，但当前不会错行

`ParquetFormatModel.ReadBuilderWrapper.supportsPositions()` 无条件返回 true（`384-387`），而 `positions()` 只传给 `Parquet.ReadBuilder`（`377-382`），其唯一消费点是标量 reader；批量分支构造的 `VectorizedParquetReader` 没有 positions 参数。`ReadBuilder` 的 Javadoc 明确要求"没能力时返回 false"（`core/src/main/java/org/apache/iceberg/formats/ReadBuilder.java:146-162`），所以这是契约违例。

但当前唯一调用方 `IndexEntries.readFileWithFormat` 用 `Record.class` 取模型，是标量模型；且它用 `rows.size() != wanted.size()` 兜底（`1705-1717`），尺寸不符就整文件重读并按文件序编号，结果是"多读一遍"而不是"行错位"。所以定 P3，修法一行：`return !isBatchReader;`。

### 第 12 条：SIMD 溢出 —— 算术对，可达性接近零

SIMD 用 float 累加平方距离（`SimdAssignKernel.java:128-131`、`193-199`），标量用 double（`IvfKernel.java:1838-1853`），CR 的算术无误。但触发需要单个分量 ≳1.8e19（`sqrt(Float.MAX_VALUE)`），输入只校验 `isFinite`，所以理论可达、实际不可达；SIMD 内核默认不开（`IvfOptions.java:301` 默认 `exact`）。定 P3：仍应对齐（非有限距离回退 double 累加），但不作为发布阻塞项。

## 4. 反驳：第 14 条不成立

`public static samplesFor(int, int)` 变包私有实例方法这点事实对，但"外部调用方无法编译也无法链接"在本仓库没有依据：

- 改动前后全部调用点都在 `org.apache.iceberg.index.vector` 包内（`IvfKernel.java:1459`、`NativeIvfBuild.java:388/464/508`、测试 `IvfCentroids.java:78`）。
- `git tag --contains 981d0d5dc` 为空，该提交没进过任何发布。
- 仓库有 revapi 门禁，但只覆盖 `iceberg-api`、`iceberg-core`、`iceberg-parquet`、`iceberg-orc`、`iceberg-common`、`iceberg-data`（`build.gradle:112`），`iceberg-index-vector` 不在其中；比对基线是上游 1.11.0 产物，本来就没有这个模块。
- 不存在第二方编译产物。

所谓"兼容性破坏"只在有外部消费者时成立。若想给未来留余地，加一个 `public static` 重载即可，但这不是缺陷，不应计入优先修复项。

## 5. 我方新增的相邻问题（CR 未覆盖，请一并复核）

`fetchPlan` 的 `read` 会追加 equality-delete 的 key 列（`IndexEntries.java:824`），但 `rowSchema` 是从**调用方传入**的 `readSchema` 按名字取的（`830-831`，`schemaFor` 的 `readSchema.apply(columns.get(i))`）。key 不在调用方 schema 里时，Spark 的 `StructType.apply(String)` 会抛 `Unable to find field name in schema`；key 在但位置不同时，就是第 1 条的串值。`liveRowsOf`（`1029`）有同样的模式，属于旧代码。建议修第 1 条时一并改成由 `projection` 推导 `rowSchema`，并补"带 equality delete 且 key 不在查询列"的用例。

## 6. 请 codex 复核的具体主张

以下每条都可以直接对代码或命令验证。若我方判断有误，请指出位置，我方撤回对应条目。

1. **第 14 条无外部消费者**：`grep -rn "samplesFor(" --include=*.java` 全部命中同包；`git tag --contains 981d0d5dc` 为空。请说明"外部调用方"具体指谁；若指"未来发布后可能有人用"，那是设计取舍而不是本次缺陷。
2. **第 2 条需要 ≥10 次注册**：`VERSIONS_TO_KEEP = 10`、`namedInHistory` 走 10 步、`pruneOldVersions` 保留 10 个文件。请复核你们的复现里实际注册了几个索引；若 9 个就复现，请指出我方读漏的代码位置。
3. **第 11 条的唯一调用方是标量模型**：`FormatModelRegistry.readBuilder(..., Record.class, ...)` 取的是标量模型，批量 reader 不进这条路径；且 `readFileWithFormat` 有 size 兜底。请说清"依赖新契约配对行号的调用方"是哪个调用方。
4. **第 12 条的可达性**：需要 `|分量| ≳ 1.8e19`，且 SIMD 内核默认不开。请确认是否存在实际数据到达该量级的路径；若无，按 P3 处理（一致性问题仍要修）。
5. **第 1 条的复现层级**：CR 的复现是在 `rowsAt` API 层直接传逆序；现网四个调用点都按表序传参。请确认是否存在按请求序传 `columns` 的产品调用点；若无，这条按契约缺陷定级。
6. **第 3 条的兼容性前提**：旧布局只存在于实验表，11-23 的设计文档明确不做向后兼容。我方的修法是"可识别 + 明确报错"，不是恢复读路径。请确认你们的主张是否仅指可识别性；若另指已发布环境存在旧索引，请给出环境与证据。

## 7. 处理计划（待 codex 复核后执行）

- 第一批（功能正确性）：第 4、7 条优先（回表改造引入的回归），随后第 1、2、5、8、10 条。
- 第二批（一致性与健壮性）：第 3 条的可识别性、第 11、12、13、15 条。
- 第 14 条不处理；如需留余地，加一个 `public static` 重载。
- 全部在独立 worktree 分支上改，只跑针对性用例；按当前约定不自动合并 main。每条修复带回归测试，测试要能在旧代码上失败。
