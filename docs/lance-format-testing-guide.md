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

## 3. 存储配置：非 AWS 的 S3 兼容存储

Lance 的读写都自己开对象存储，它认的是 `object_store` 的**裸键**（`endpoint`、`access_key_id`、`secret_access_key`、`region`、`allow_http`、`virtual_hosted_style_request`）；Iceberg 和 Hadoop 用的是带前缀的键（`s3.endpoint`、`fs.s3a.access.key`）。把带前缀的键原样交给 Lance，它不会报错，而是**当一个没见过的键忽略掉**，然后回落到进程环境变量。所以只配表属性/catalog、不注入 `AWS_*` 时，读会静默走到公网 AWS 上（或者报 `Bucket ... not found`，看不出是配置没被接住）。

现在写侧会把这些键翻译成 Lance 认的名字，三种写法等价，按需选一种：

| 想配的 | Iceberg 写法 | Hadoop 写法 | Lance 逃逸口 |
|---|---|---|---|
| endpoint | `s3.endpoint` | `fs.s3a.endpoint` | `lance.storage.endpoint` |
| access key | `s3.access-key-id` | `fs.s3a.access.key` | `lance.storage.access_key_id` |
| secret key | `s3.secret-access-key` | `fs.s3a.secret.key` | `lance.storage.secret_access_key` |
| session token | `s3.session-token` | `fs.s3a.session.token` | `lance.storage.session_token` |
| region | `s3.region` | `fs.s3a.endpoint.region` | `lance.storage.region` |
| 用 path-style 寻址 | `s3.path-style-access=true` | `fs.s3a.path.style.access=true` | `lance.storage.virtual_hosted_style_request=false` |
| 走明文 http | — | `fs.s3a.connection.ssl.enabled=false` | `lance.storage.allow_http=true` |

三条容易踩的。一是 path-style 与 `allow_http` 在 Lance 那边是**反向开关**（`virtual_hosted_style_request`、`allow_http`），MinIO / 本厂 OSS 这类自建端点通常两个都要给，否则第一次 PUT 就报 `HTTP error: builder error`。二是 **region 有两个名字**：Iceberg 自己的 `S3FileIO` 认 `client.region`，Lance 认 `region`（由 `s3.region` 翻译而来），少给 `client.region` 会在建 writer 时就报 `Unable to load region from any of the providers`。三是 `lance.storage.*` 是逃逸口，Lance 新增任何键都可以从这里透传，它最后应用、会覆盖同名的翻译结果；`s3.*` 里暂时没有对应项的键（比如 `s3.some-future-knob`）会被忽略并打一条 WARN，不会让已有的表读不出来。

```sql
ALTER TABLE hms.sparkann.sift10m_lance SET TBLPROPERTIES (
  's3.endpoint' = 'http://oss.i.shaipower.com',
  's3.region' = 'us-east-1',
  's3.path-style-access' = 'true',
  'fs.s3a.connection.ssl.enabled' = 'false');
```

**凭据不要写进表属性**：表属性会进元数据和每一个快照的历史，等于把 AK/SK 永久留档。凭据走 catalog 配置、instance profile 或环境变量；表属性里只放 endpoint / region / path-style / 是否明文这几项。

**来源不对称，这点要记住**：写侧由 `ModelWriteBuilder.set` 从**表属性**拿到配置，它没有 `FileIO` 通道，所以写一张非 AWS 上的 Lance 表时，endpoint / path-style / 是否明文都只能写在表属性里（凭据则可以留给 instance profile 或环境变量，免得进元数据与快照历史）。

**读侧的来源顺序**（`#3108` 起固定为这条链，实现在 `org.apache.iceberg.io.StorageProperties`）：

1. 表属性（最具体）；
2. catalog 的 `FileIO.properties()`；
3. 引擎的 Hadoop 配置（`spark.hadoop.fs.s3a.*` / core-site.xml 里的 `fs.s3a.*`）；
4. 进程环境：**不由 Iceberg 读取**，它仍然是 Lance/object_store 自己的最后兜底；所以第 1–3 条都没给值时，客户端会去打公网 AWS。

Spark 3.5/4.0/4.1 的批式、行式读取器（`BaseBatchReader` / `BaseRowReader`）与 `data` 的 `GenericReader` 都会把上面第 1–3 条合并后交给 `ReadBuilder`。其中第 3 条是这样接上的：Spark 在 driver 侧把 `sparkContext.hadoopConfiguration()` 里的存储键挑出来（只挑 `s3.*` / `fs.s3a.*` / `gs.*` / `azure.*` / `lance.storage.*`，不会把整个 Hadoop 配置塞进广播），随 executor 的 `FileIO` 一起带过去——因为 `HadoopFileIO.properties()` 只返回 catalog 初始化时给它的那几项，**不含**它实际读取用的 Hadoop 配置。`index-spark` 的检索与回表也在 driver 侧解析一次，把同一份合并结果带进任务。

**优先级是按"应用顺序"实现的**：`ReadBuilder.setAll` 按 map 的迭代顺序逐项应用，Lance 模型内部用 `LinkedHashMap` 保存，因此同一个设置的不同写法（比如 `s3.endpoint` 与 `fs.s3a.endpoint`）后出现的赢。合并方必须用保序的 map（`LinkedHashMap`/`ImmutableMap`），**不要用 `HashMap`**——它会把这个优先级变成随机的。同理，`spark.hadoop.fs.s3a.endpoint` 这类 Hadoop 默认值可能已经在配置里（值指向公网 AWS），只要表或 catalog 给了更具体的值，保序后就能正确覆盖。

**失败时的信息**：第 1–3 条都没解析出 endpoint 时，`LanceFileReader.open` 的失败信息会点名"没有解析到 endpoint"，并列出 `s3.endpoint` / `fs.s3a.endpoint` / `lance.storage.endpoint` 三种写法，而不是只留下 object_store 的 `Bucket ... not found`。

证据分四层：`TestLanceS3StorageOptions` 在 MinIO 上验证了两套写法都能写能读，且把 endpoint 换成不存在的主机名时错误信息里会点名该主机，说明它真的被用了；`TestLanceStorageOptions` 钉住 `s3.*` / `fs.s3a.*` / `lance.storage.*` 的翻译；`TestStorageProperties` 钉住三类来源的优先顺序与 Hadoop 配置的过滤；`TestGenericReaderStorageProperties` 用一个记录配置的替身 format model 验证读取器把表属性交给了 `ReadBuilder`；`TestLanceSparkS3StorageSmoke` 走完整链路——在 Spark 里建一张数据文件落在 MinIO 的 Lance 表，写入后把表上的存储属性全部撤掉，再靠 catalog 的 `FileIO` 配置读回 `max(id)`；`TestLanceSparkHadoopStorageConfSmoke` 是 `#3108` 的验收形状：catalog 的 `FileIO` 不暴露存储设置（与 `HadoopFileIO` 相同），存储只配在 `spark.hadoop.fs.s3a.*` 里，撤掉表属性后仍能读回 `max(id)`。后两个在没有 MinIO 时跳过。

还**没接**的入口：Flink 的 `RowDataFileScanTaskReader`（构造函数里没有 `FileIO`，要改签名波及上层）、MR 的 `IcebergInputFormat`、`data` 的 `BaseDeleteLoader`（只有 `Function<DeleteFile, InputFile>`，没有配置来源）——这些入口上的 Lance 文件仍然只认环境变量；`index-spark` 里按文件序/位置回表的那两处已经带上了表属性 + `FileIO` + Hadoop 配置，而 delete 文件那一处仍只有 `FileIO.properties()`。

## 4. 还缺的（属 Lance Java SDK，不在本仓）

这三条在 `lance/LANCE_SDK_GAPS.md` 里有完整讨论，只影响性能与裁剪，不影响正确性：

- **无列统计**：`LanceFileAppender.metrics()` 只给 `recordCount`，`nullValueCounts`/`bounds` 都是 null，所以没有文件级裁剪。索引构建不受影响（跳空向量走残余谓词），但会多读。
- **filter 下推是 no-op**：`ReadBuilderWrapper.filter()` 接受表达式但不应用，过滤仍在 Iceberg 的残余求值里做，正确但慢。
- **不可 split**：要真正在单个大文件内并行，需要 SDK 暴露 `getBytesWritten()` 并把字节偏移映射到行区间（Gap #3 的方案），成本高、收益不确定，建议先用第 1 节的文件数控制。

## 5. 一次最小验证

```sql
CREATE TABLE hms.sparkann.lance_smoke (id bigint, embedding array<double>) USING iceberg
  TBLPROPERTIES ('write.format.default' = 'lance',
                 'write.target-file-size-bytes' = '67108864');
INSERT INTO hms.sparkann.lance_smoke SELECT id, array(CAST(id AS double), CAST(100 - id AS double))
  FROM range(0, 2000);

CALL hms.system.build_vector_index(table => 'sparkann.lance_smoke', column => 'embedding',
                                   dimension => 2, metric => 'euclidean',
                                   structure => 'IVF', quantizer => 'RABITQ', name => 'smoke_ivf1',
                                   options => map('rabitq-bits','1'));
SET `spark.iceberg.index.vector.ivf.nprobe` = 32;

CALL hms.system.check_indexes(table => 'sparkann.lance_smoke');   -- readable=true, stale=false
CALL hms.system.vector_search(table => 'sparkann.lance_smoke', column => 'embedding',
                              query => array(0.0d, 100.0d), k => 3);
```

最后一条的答案应当与 `ORDER BY vec_distance(embedding, array(0.0d,100.0d)) + 0.0d LIMIT 3`（加零让它不被改写）一致——这正是 `TestLanceVectorSearch` 断言的内容。
