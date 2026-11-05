# 在厂内测试 Lance 数据格式（索引线）

这份文档回答一件事：**要在这条线上用 Lance 数据格式跑 10M / 100M 的向量检索，需要设什么、会看到什么、哪里还不行。**

结论先说：功能是通的。Lance 表建向量索引、按位置回表、`vector_search` 单查与批查、position delete 存进 Lance 文件本身，这几条路径在仓库里都有测试覆盖（`TestLanceVectorSearch`、`TestLanceFetchBack`，以及 `iceberg-lance` 模块的 35 项 round-trip/schema 测试）。索引层不是只认 Parquet：`IndexEntries` 里 Parquet 走原生 Spark 读，其余格式一律走 `FormatModelRegistry`，Lance 由自己的 format model 读。

真正需要注意的是规模：**Lance 文件不可 split**（`FileFormat.LANCE("lance", false)`，`LanceFileAppender.splitOffsets()` 返回 null），所以并行度只来自**文件数**——一个文件就是一个 task、一个核。默认 `write.target-file-size-bytes` 是 512MB，10M 的表可能只有十来个文件，看起来就像"索引构建和回表都只有单核在跑"。

## 1. 建表时控制文件数（不改任何默认值）

```sql
ALTER TABLE hms.sparkann.sift10m_lance
  SET TBLPROPERTIES ('write.target-file-size-bytes' = '67108864');   -- 64MB
```

参考值（SIFT 128 维 float32，每行向量约 520 字节）：

| 数据集 | 数据量（估算） | 目标文件大小 | 期望文件数 | 16 个 task 槽位下的并行度 |
|---|---|---|---|---|
| sift10m | ~5.2 GB | 64 MB | ~80 | 5 轮 |
| sift100m | ~52 GB | 128 MB | ~400 | 25 轮 |

判断依据不是"文件越多越好"：文件数远小于槽位数时读不满，远大于时元数据与调度开销上来。经验上**每轮 2–4 个 task 的余量**就够，所以 80 个文件配 16 槽位是合适的。

`LanceFileAppender.length()` 有实现（已刷出字节 + 当前批缓冲估算），所以滚动是按目标大小生效的，不需要改代码。

## 2. 删除文件仍用 Parquet

Lance 的 format model 只提供**数据文件**与**position delete**；没有 equality delete 的 writer。默认 `write.delete.format.default` 就是 parquet，正常路径不受影响。如果把它改成 lance，现在会得到一条点名式的拒绝：

```
Format model is not registered for format LANCE and type <类型>; LANCE is registered for <已有类型>.
Register a model for this type, or write this content in a format that provides one
```

这条报文是为此改的（见 `FormatModelRegistry`）：原来只说"没注册"，读的人不知道是自己配错了格式，还是这个 build 缺一个模型。

`position delete` 存进 Lance 本身是支持的，`TestLanceVectorSearch.searchAppliesPositionDeletesStoredInTheTablesOwnFormat` 就是按这个形状写的——它验证"被删行不会从索引结果里回来"。

## 3. 还缺的（属 Lance Java SDK，不在本仓）

这三条在 `lance/LANCE_SDK_GAPS.md` 里有完整讨论，只影响性能与裁剪，不影响正确性：

- **无列统计**：`LanceFileAppender.metrics()` 只给 `recordCount`，`nullValueCounts`/`bounds` 都是 null，所以没有文件级裁剪。索引构建不受影响（跳空向量走残余谓词），但会多读。
- **filter 下推是 no-op**：`ReadBuilderWrapper.filter()` 接受表达式但不应用，过滤仍在 Iceberg 的残余求值里做，正确但慢。
- **不可 split**：要真正在单个大文件内并行，需要 SDK 暴露 `getBytesWritten()` 并把字节偏移映射到行区间（Gap #3 的方案），成本高、收益不确定，建议先用第 1 节的文件数控制。

## 4. 一次最小验证

```sql
CREATE TABLE hms.sparkann.lance_smoke (id bigint, embedding array<double>) USING iceberg
  TBLPROPERTIES ('write.format.default' = 'lance',
                 'write.target-file-size-bytes' = '67108864');
INSERT INTO hms.sparkann.lance_smoke SELECT id, array(CAST(id AS double), CAST(100 - id AS double))
  FROM range(0, 2000);

CALL hms.system.build_vector_index(table => 'sparkann.lance_smoke', column => 'embedding',
                                   dimension => 2, metric => 'euclidean',
                                   structure => 'IVF', quantizer => 'RABITQ', name => 'smoke_ivf1',
                                   options => map('nprobe','32','rabitq-bits','1'));

CALL hms.system.check_indexes(table => 'sparkann.lance_smoke');   -- readable=true, stale=false
CALL hms.system.vector_search(table => 'sparkann.lance_smoke', column => 'embedding',
                              query => array(0.0d, 100.0d), k => 3);
```

最后一条的答案应当与 `ORDER BY vec_distance(embedding, array(0.0d,100.0d)) + 0.0d LIMIT 3`（加零让它不被改写）一致——这正是 `TestLanceVectorSearch` 断言的内容。
