# 索引构建的部分提交与补覆盖（设计）

日期：2026-11-20　分支：`card/3170`（基底 `index`）　状态：已实现

## 1. 要解决的问题

一次向量索引构建按小时计。旧行为是二值的：要么整次成功并发布，要么任何一个组失败就让整个 Spark job 失败、本次 attempt 写出的索引文件全部作废、`build_vector_index` 连注册一起回滚。一个数据文件撑不住（维度不符、OOM）就足以让几小时的构建白跑，重跑仍从零开始。

用户的要求是：建到 80% 也可以先提交，没索引的数据先由查询补扫，之后增量补齐。

## 2. 为什么这件事本来就该成立

索引从设计上就允许"部分覆盖"：`IndexFile.coveredDataFiles` 精确记录每个索引文件覆盖了哪些数据文件，`IndexReader.uncoveredDataFiles` 报缺口，向量改写 `coverage => 'table'`（默认）答索引能答的部分再扫未覆盖文件合并排序，`check_indexes` 有 `uncovered_data_files` 与补齐建议。缺口不是异常状态，是已有语义。

真正的阻碍只有三处：构建失败即整体作废、同一表快照没有补覆盖入口、文本索引要求完整覆盖。

## 3. 关键选择：补覆盖用"替换"而不是"追加"

`IndexMetadata` 有一条不变式：每份源表快照最多一版索引（`snapshotForSourceTableSnapshot` 返回唯一匹配，发布侧"已覆盖则保留"一直在维护它）。

如果补覆盖走"追加"，这条不变式就破了，随之要重新审：EXACT 一致性取哪一版、`cleanup_index_files` 的 `liveSnapshots`/`expiredSnapshots` 怎么算、`list_indexes` 显示哪一版、时间旅行语义、以及将来任何按源快照找索引的代码。

替换的语义是：**同一份表快照永远只有一版索引**。补覆盖 = 用覆盖更全的一版替换覆盖不全的那一版，新一版继承旧一版的父链位置。于是读取路径、清理、`list_indexes` 全都不用改，历史也不会因为反复补覆盖而堆积。

实现落在 `IndexMetadata.replaceSnapshot`：同源位置替换；若新版的 `parentIndexSnapshotId` 指向被替换者（补覆盖正是从它派生的），改指被替换者的父；后续若有以被替换者为父的版本，也一并改指。这样 `IndexMetadata` 构造里的"父必须在本索引内"仍然成立。

## 4. 部分构建：失败隔离到组

构建的最小单位是"组"（一个索引文件覆盖的一组数据文件），默认 `max_entries_per_index_file` 会把多个小数据文件并成一组。因此失败的影响面天然是组：

- 只有**数据本身建不出来**的失败（`IllegalArgumentException`：维度不符、payload 超限）被隔离成一行记录（该组的数据文件 + 失败原因），driver 收集。其余异常（瞬时 IO、task 被抢占的 `TaskKilledException`、引擎自身 bug）一律重新抛出，好让 Spark 按 `spark.task.maxFailures` 重试——把这些也降级成"数据缺口"会把一次瞬时故障写进索引，而且永久不重试。
- driver 把成功组的索引文件记进 attempt 账本并发布，报告状态 `PARTIAL`（新枚举值）。`files_failed` 是**失败的组数**（与 `files_total` 同单位），`error` 保留缺口说明 + 若干数据文件路径 + 第一个失败原因；真正漏掉多少数据文件由 `check_indexes.uncovered_data_files` 回答。
- 成功组覆盖的数据文件照常进 manifest；失败组的数据文件**不**进"空覆盖文件"，否则会声称索引持有它们的行。
- 一个都没建成时不发布：`abortAttempt` + 抛错，procedure 侧回滚注册，报告 `FAILED`。空索引比没有索引更难解释。

严格路径（`refresh_vector_index`、`refresh_text_index`、`SparkIndexBuilder.build` 原签名）保持旧契约：组失败照旧抛原始异常。原因是一个没要求"部分索引"的调用方不该静默拿到一个残缺结果，既有诊断测试也依赖该异常类型。

## 5. 部分构建不替换已发布版本

`publishSnapshot(..., complete=false)` 在同源已有一版时直接保留现状。已发布那版覆盖至少一样多，替换会立刻拿掉查询的覆盖。完整构建（`complete=true`）可以替换部分版，这是重试修复的路径。

## 6. 文本索引不参与

BM25 改写要求索引完整覆盖查询快照，部分覆盖的倒排绑不上、全局语料统计也会偏。文本保持"整次成功才发布"，失败时既不发布也不回滚（已知缺口，见用户手册 5.5）。

## 7. 代价与边界

- **缺口期间查询变慢**：默认 `coverage => 'table'` 每次扫未覆盖文件；`indexed` / 地址类表函数语义不变，仍只答索引覆盖的部分。
- **失败以组为单位，而组可能很大**：`max_entries_per_index_file` 默认 100 万条目，小文件会被大量并组，一个坏文件可能带走整组。实测 4 文件、预算 2 时 1 组失败让 2 个数据文件同时失去索引。设小预算可把影响面缩到单文件，代价是索引文件更多、图更碎。
- **部分提交不修复脏数据**：没进索引的行在查询的残差扫描里照样会被读到，`vec_distance` 遇到维度不符仍报错。这个能力买的是"不用重跑几小时"，不是"脏数据可用"。
- **补缺口的前提是原因消失**：`refresh_vector_index` 是严格路径，坏文件还在那份快照里时它会再次失败。真实流程是"修数据 → refresh"。两类原因要分开：维度不符靠修数据，而 payload 超限是相对于索引预算说的大小，`refresh` 继承 `currentMetadata.maxEntriesPerIndexFile()`，同样的预算会再次超限——那类缺口要用更小的 `max_entries_per_index_file` 重建，而不是简单重跑。
- **并发替换会让父快照消失**：`updateIndex` 的 CAS 会在新 current 上重放 change，所以并发发布可能让一个构建引用的父快照被替换掉。发布侧现在检测并改指最新快照，而不是在写完所有文件之后被 metadata 不变式拒绝。
- **分批构建没有做**：原计划按批切分 Spark job，实现时发现组级隔离已经给出同样的"保住大部分成果"效果，且不需要多 job 的 barrier 与额外 scan planning，因此没有引入。若将来单组规模过大（大文件 + 大预算），再考虑按组切批。
- **并发**：同一表快照的并发补覆盖以"同源唯一一版 + 发布时取 latest 之后的新 id"收口；两版都在 latest 之外的替换请求会被丢弃。跨源快照的并发由"父快照若已不在 metadata 则改指 latest"兜住（见上）。

## 8. 验证

- `TestVectorIndexRefresh#refreshFillsTheGapsOfTheSameSourceSnapshot`：手工把已发布索引改成只覆盖一半，表不动，刷新后覆盖恢复、同源仍只有一版、`index_snapshot_id` 前进一格。
- `TestPartialIndexBuild`：两组一好一坏 → 发布 1 个文件、覆盖 1/2、`PARTIAL`、`files_failed=1`、`check_indexes` 报 1 个缺口、表仍可读、坏文件还在时严格刷新仍失败、删除坏文件后刷新把缺口补零；全部失败 → 不注册、`FAILED`；两组各含多文件时 `files_failed` 报组数而 `uncovered_data_files` 报文件数。
- `TestIndexPublicationLifecycle`：部分构建不替换已发布构建；部分构建在无已发布版本时仍可发布；父快照已被替换的构建仍可发布（改指 latest）。
- `TestIndexMetadata`：替换位置、从被替换者派生时的 lineage 修复、后续版本改指 replacement、无同源时追加。
- `TestPartialBuildSmoke`：答案等价性为主轴——缺口下 `coverage => 'table'` 的 id 与距离都等于扫描基线、未覆盖文件的行确实由扫描补上、`coverage => 'indexed'` 只答索引覆盖的 live 行、SQL 改写与批量入口同样等价、缺口与表追加并存、清理不误删部分构建已发布的文件、失败文件绝不被记成 covered。
- `TestPartialBuildAtScale`：10 个索引文件（跨多个 manifest）时答案要合并索引与扫描两路，缺口占九成仍答对。
- `TestPartialBuildLance`：同样的部分提交与补覆盖在 Lance 表上成立——Lance 的位置过滤是区间列表而不是 Parquet 的页查找，回表路径不同；含 2 文件 × 10 万行 × 32 维的形状。
- `TestPartialBuildConcurrency`：替换与追加并发提交后元数据不变式仍成立；同一表快照的两个构建不会都留下。
- `TestDataFailurePolicy`：只有 `IllegalArgumentException`（数据本身不可索引）才能变成缺口，读失败、executor 被杀、引擎 bug 必须让构建失败，否则瞬时错误会被固化成永久缺口。
- 关联回归：`TestIndexBuildStatusProcedure`（严格路径仍抛 SparkException）、`TestTextIndexProcedures`、`TestCheckIndexesProcedure`、`TestIndexBuildNullValues`、`TestVectorIndexRefresh`、`TestSparkIndexBuilder`、`TestIndexBuildShuffle`、`TestVectorIndexQuickstart`、`TestDistributedIndexSearch` 通过。未跑全量 core 与跨 Spark 版本回归（按项目规则后置到整项功能完成的节点）。

## 9. 评审后修正（2026-11-20 首轮评审）

外部评审提出两条 P1、若干 P2，均已处理：

1. **catch 收窄**：原来 `catch (Exception)` 会把瞬时 IO、`TaskKilledException`（speculative execution 或被抢占时抛）和引擎 bug 一起降级成"这组数据建不出来"，而且**丢掉了 Spark 的 task 重试**——改动前这类失败会被 `spark.task.maxFailures` 重试吃掉。现在只捕 `IllegalArgumentException`，其余重新抛出。
2. **父快照可能已消失**：`SidecarIndexCatalog.updateIndex` 是 CAS 重放，并发替换会让"构建时读到的父快照"在发布时不存在，追加路径会用构造器的不变式炸掉，且发生在所有文件写完之后。发布侧现在检测并改指 latest。
3. **`files_failed` 口径**：文档原写"数据文件数"，实现是"组数"，已按实现改文档并补了多文件一组的用例。
4. `validateSourceLineage` 的位置恢复原样（"已覆盖就 no-op"优先），避免改变回滚后刷新的语义。
5. `IndexBuildStatus.partial()` 是死代码（reporter 走 builder），删掉并把 500 字符截断移到 builder，让 PARTIAL 的 reason 也受同样限制。
6. 删除未使用的 `dataFileCount` 与其上方残留的 javadoc；`summary` 现在列出前 5 个缺口文件路径，driver 日志可直接用于定位。
