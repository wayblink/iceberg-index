<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Lance-Spark SQL 用法与 Iceberg 索引语法对比

继续阅读：[向 Lance-Spark 对齐 SQL 的可行性、工作量与收益](2026-10-28-lance-spark-sql-alignment-feasibility.md)。这是研究建议，尚未批准实施或新增接口参数。

## 1. 总览：先找任务，再比较写法

Lance-Spark 最值得学习的不是函数更短，而是命名索引的动作统一、检索返回有类型的业务关系、文本条件放在 WHERE、分数作为 metadata 读取。我方 CALL 沿用 Iceberg procedures，并非错误设计；已有普通距离 SQL 的有限改写和 batch 入口，也不应被比较表抹掉。真正的短板是入口与结果合同分裂、JSON 回表、conf 与 metric 隐式、错误校验和组合边界较弱。对方也要付 namespace 接入、aliases 优先级、options 字符串及部分未知键静默忽略的代价，不能说所有语法都更好。

我方冻结 `83b1af39fa4200c926f23834fd6edd62ee35bd8d`，connector 冻结 `ed2f49f94dfdb4ad81a2153cb658443864cd4d54`，Lance 12 冻结 `cbeec97cb893a66ad6b3db87f25a57ab0093f358`。函数名与参数已按冻结源码核对，既有索引体系报告仅用于语义背景。以下 SQL 全部是未执行示例，不表示实测、制品可部署或性能承诺；我方能力是定制实现，不是上游标准索引语法，#2798 未合入此冻结点。

| 想做的事 | Lance-Spark | 我方 | 主要差别 |
|---|---|---|---|
| 首建索引 | ALTER TABLE CREATE INDEX 创建 scalar/FTS；ANN 首建走 SDK | CALL build_vector_index/build_text_index | 对方 SQL 不覆盖所有原生类型 |
| 找近邻 | VECTOR_SEARCH | CALL vector_search；vector_index_search；有限 vec_distance 改写 | typed 行与 JSON hit 合同不同 |
| 搜 ERROR 1064 | lance_match 或 SEARCH | CALL text_search；text_index_search；match/text_score | 我方普通文本 SQL 正确性未闭环 |
| 多查询 | 未找到对应 SQL batch 入口 | vector_search_batch、vector_index_search_batch、text_search_batch | 我方不是只有单查 |
| 混合排名 | HYBRID_SEARCH 做 RRF | 无统一 hybrid 入口 | 融合不是原向量精排 |
| 更新、诊断、驻留 | REFRESH/OPTIMIZE/DROP INDEX、SHOW INDEXES | 多个 system procedures | 名称、参数和报告域要逐项看 |

三类差异必须分开：**句法**是 CALL 与 ALTER 的可读性；**API 缺口**是 typed 输出、显式参数及错误合同；**执行语义**是过滤前 TopK、活行补足、driver 物化、评分语料。改名不会带来预过滤，也不会让查询离开 driver。

## 2. 部署前提与同一张假设表

例子统一使用 `items(id BIGINT, tenant INT, body STRING, embedding 三维向量)`。Lance 的 embedding 为 ARRAY<FLOAT>，固定维度依靠已有表属性；我方单向量 SQL 输入需要 DOUBLE 数组，例中用 D 后缀。先有表和数据，再建索引；这不是空表训练成功的演示。

Lance 要匹配 Spark/Scala bundle。命名参数 `=>` 需要 Spark 3.5；3.4 的 VECTOR/HYBRID 有位置形式，SEARCH 虽有注册，却没有位置槽传必需的 search_columns，不能当有效兼容入口。extensions 注册 TVF 和规则，FTS scalar 则由 catalog.loadFunction 提供，是两条通道；裸 lance_match 应令 defaultCatalog 指向 Lance，函数只接受空 namespace，不能照表路径拼成 lance.default.lance_match。[1][2]

```text
# 已有配置键的接入示意，不是新增配置或推荐默认值
spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions
spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog
spark.sql.catalog.lance.impl=dir
spark.sql.catalog.lance.root=/path/to/lance/database
spark.sql.defaultCatalog=lance
# 我方会话使用已有 org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
# 及已部署的 Iceberg catalog；两套示例不假定共用一个会话。
```

```sql
CREATE TABLE lance.default.items
(id BIGINT, tenant INT, body STRING, embedding ARRAY<FLOAT>) USING lance
TBLPROPERTIES ('embedding.arrow.fixed-size-list.size'='3');
```

dir 对应目录 namespace，rest 还需服务与查询协议；raw path 的 DataFrame load 并不自动登记 namespace。TVF 先解析 catalog identifier，再要求 namespaceImpl/tableId，把路径放进 table 参数不能绕过这层。catalog 能建表，也不等于 backend 已能执行每种检索。已有 namespace 跳测只能说明验收缺口，不能否定固定 Lance 12 目录实现已有 structured FTS；Java 制品实际调用闭包和端到端结果仍未证。[3][18]

## 3. 生命周期：动作统一，但 ANN 首建有例外

```sql
ALTER TABLE lance.default.items CREATE INDEX tenant_idx USING bitmap (tenant);
ALTER TABLE lance.default.items CREATE INDEX body_fts USING fts (body)
WITH (base_tokenizer='simple', lower_case=true, with_position=true);
SHOW INDEXES FROM lance.default.items;
-- REFRESH 复用与 CREATE 一致的分析器/位置选项，避免段配置不一致
ALTER TABLE lance.default.items REFRESH INDEX body_fts
WITH (base_tokenizer='simple', lower_case=true, with_position=true, num_segments=2);
-- embedding_idx 假设已由 SDK 创建；并非上面的 SQL 创建了 ANN
ALTER TABLE lance.default.items OPTIMIZE INDEX embedding_idx
WITH (num_indices_to_merge=0);
-- DROP 是最后的清理动作，不要在查询示例之前执行
ALTER TABLE lance.default.items DROP INDEX tenant_idx;
```

CREATE 白名单包括 btree、zonemap、bitmap、label_list、ngram、bloomfilter、rtree、fts/inverted；后者统一到 FTS，执行只支持一个字段。不能据底层有 IVF 就编出 USING ivf 的 DDL。ANN 首建是 SDK 能力，已有 ANN 可由 OPTIMIZE INDEX 在 driver 调 native optimizeIndices，增量补覆盖/合段，但 Spark 入口不接受 retrain。REFRESH 反查 scalar 类型，ANN 会拒绝；FTS REFRESH 补未覆盖 fragment，不是全表重排，参数还需满足段配置一致性。[4][5][6]

统一动作让用户记住“表、动作、索引名”即可，但 SHOW 独立，索引 DDL 没有 IF EXISTS/IF NOT EXISTS，CREATE 同名替换也要谨慎。CREATE 返回 fragments_indexed/index_name；REFRESH 报新增覆盖与 segment；OPTIMIZE 报名称、覆盖和前后段数，均不是业务行。OPTIMIZE INDEX 与表级数据压实不是同一个操作。

```sql
CALL spark_catalog.system.build_vector_index(
 table=>'db.items', column=>'embedding', dimension=>3,
 metric=>'COSINE', structure=>'FLAT', name=>'emb_idx');
CALL spark_catalog.system.build_text_index(table=>'db.items', column=>'body');
CALL spark_catalog.system.refresh_vector_index(
 table=>'db.items', column=>'embedding', index=>'emb_idx');
CALL spark_catalog.system.refresh_text_index(table=>'db.items', column=>'body');
```

我方 build/refresh 返回 index_uuid、source_snapshot_id、index_files、entries。文本没有 name，刷新仅 table/column；向量同列多索引需明确名称。刷新复用定义、重建受影响文件组，不是 Lance native 原地维护的同义词。CALL 与 Iceberg 既有过程体系一致，保留既有表治理是合理优点；问题是类型不同就要记多套名字、column/index/name 也不一致。[7]

## 4. 查询：先明确候选资格，再选入口

### 4.1 向量单查、批查

```sql
SELECT id, tenant, _distance FROM VECTOR_SEARCH(
 table=>'lance.default.items', query_vector=>array(1.0,0.0,0.0),
 vector_column=>'embedding', num_results=>10, distance_type=>'cosine',
 filter=>'tenant = 42', prefilter=>true, columns=>array('id','tenant'),
 nprobes=>8, refine_factor=>2) ORDER BY _distance;
-- 位置形式不提供 vector_column 槽；本例列叫 embedding，因此使用命名形式。

CALL spark_catalog.system.vector_search(
 table=>'db.items', column=>'embedding', query=>array(1.0D,0.0D,0.0D),
 k=>10, index=>'emb_idx');
SELECT * FROM vector_index_search(
 'spark_catalog.db.items','embedding',array(1.0D,0.0D,0.0D),10);
CALL spark_catalog.system.vector_search_batch(
 table=>'db.items', column=>'embedding', queries=>array('1,0,0','0,1,0'),
 k=>10, index=>'emb_idx');
SELECT * FROM vector_index_search_batch(
 'spark_catalog.db.items','embedding',array('1,0,0','0,1,0'),10);
```

位置形式是 `VECTOR_SEARCH(table, query_vector, k)`，不能与命名参数混用；它依赖默认向量列解析，分布式路径默认列名为 `vector`，不能直接用于上述仅有 `embedding` 的表。Lance 的 num_results、limit、k 按此前后优先，不是同时传时报冲突；columns 控制业务投影，距离自动加入。query_vector 必须常量，不能把另一表的运行时列直接塞进去做逐行检索。nprobes 是探桶预算，refine_factor 扩候选后精排，两者不是一个旋钮；distance_type 是显式查询合同，但 metric 和索引的兼容性仍要核。默认 VECTOR 通过 namespace，一个 Spark partition；已有 distributed 开关才进入 segment/fragment tasks 加全局 merge，带 filter 时要求显式 prefilter=true，且不支持距离 bounds。typed relation 不等于默认全分布式。[3]

我方向量 TVF 严格四参，**没有第五个 index**；CALL 可选 index，TVF 只能借已有 conf 选择。batch inline 是 ARRAY<STRING>，每个字符串为逗号数字，不是 JSON 方括号，也不是嵌套 ARRAY。CALL 还能用 query_table：指定它时优先于 inline queries，vector_column 默认 vector，query_id_column 只控制排序；返回 query_id 是排序后从零开始的 ordinal，不是原始业务 ID。没有唯一排序键就不能承诺序号跨次稳定。[8][9]

### 4.2 文本与混合检索

```sql
SELECT id, body, _score FROM lance.default.items
WHERE lance_match(body,'ERROR 1064','operator=AND,fuzziness=0') AND tenant=42
ORDER BY _score DESC LIMIT 10;
SELECT id FROM lance.default.items
WHERE lance_match_phrase(body,'ERROR 1064',0);
-- 仅说明多列形式：另假设 title 存在且 title、body 各建 FTS
SELECT id FROM lance.default.items
WHERE lance_multi_match('ERROR 1064','operator=OR',body,title);
SELECT id, _score FROM SEARCH(
 table=>'lance.default.items', query=>'ERROR 1064',
 search_columns=>array('body'), columns=>array('id'), filter=>'tenant=42',limit=>10);

CALL spark_catalog.system.text_search(
 table=>'db.items',column=>'body',query=>'ERROR 1064',k=>10);
CALL spark_catalog.system.text_search_batch(
 table=>'db.items',column=>'body',queries=>array('ERROR 1064','crash'),k=>10);
SELECT * FROM text_index_search('spark_catalog.db.items','body','ERROR 1064',10);
```

我方实际函数是 **match、text_score，不是 text_match**。当前文本查询是 token OR，只出现 ERROR 或 1064 也可入榜。Lance phrase 需要索引存位置；multi_match 至少两列且相应索引存在。match 的 operator 管词项，multi_match 管列组合，不能拿它代替任意 WHERE OR。每个 Filter scope 至多一个 FTS，须为顶层 AND conjunct，OR、NOT、CASE 包装会拒绝；query/options 是常量，派生列与 alias 也不能当然绑定原字段。[10][11]

_score 是隐藏 metadata，SELECT * 不自动带，非 FTS 查询读它会报错。存储列同名会让 FTS（包括 SEARCH/HYBRID）拒绝该表，不是简单别名就能解决。SEARCH 必须命名传 search_columns，走 canonical FTS 加普通 Filter/Project/Sort/Limit；可下推条件先筛，不支持的条件保留 residual，并关闭相应 TopK 下推。只有符合条件的 namespace 路径才能下推 score 降序 TopN，本地 fragment 仍由 Spark 排序，不能泛称所有 WHERE 都在底层完成。[11]

```sql
SELECT id, _distance, _score, _relevance_score FROM HYBRID_SEARCH(
 table=>'lance.default.items',query_vector=>array(1.0,0.0,0.0),query=>'ERROR 1064',
 vector_column=>'embedding',search_columns=>array('body'),columns=>array('id'),
 num_results=>10,candidates=>50,filter=>'tenant=42',prefilter=>true,rrf_k=>60.0)
ORDER BY _relevance_score DESC;
```

HYBRID 分别检索向量和文本候选，按 rowid 合并，以从零开始的 rank 计算 RRF；这不是拿原向量重新精排，也不是把 BM25 和 distance 直接相加。单侧命中另一指标为 null，融合分数非空。两侧顺序 queryTable、单任务收集融合，distributed 开关不改这条路径。我方没有统一 hybrid；手工 UNION/JOIN 不会自动建立相同召回与融合合同。[12]

## 5. 返回结果、普通 SQL 与分数含义

| 入口 | 返回形态 | 结果域 |
|---|---|---|
| Lance VECTOR/SEARCH | typed 业务列＋FLOAT _distance/_score，可取 LONG _rowid | 所选版本及对应过滤/覆盖合同 |
| Lance HYBRID | typed 业务列＋三种指标 | 两侧候选并集后融合截断 |
| 我方向量单查 CALL/TVF | data_file、pos、score、row，四列 | 索引 hit，不补未覆盖文件 |
| 我方向量批查 CALL/TVF、文本 batch CALL | 上述加 INT query_id，五列 | 每查询 hit，query_id 为 ordinal |
| 我方文本单查 CALL/TVF | 四列，与单查 schema 一致 | 规划时文本搜索，JSON 回表 |
| 我方普通向量 SQL | typed 业务行与距离 | 有限改写可补扫残差、活行精排 |

**单查 schema 要以最终返回为准：**`text_index_search` 的 payload 是 ByteBuffer，选中 `IndexSearch` 单查重载；它先走五列 batch 中间结果，再删除 `query_id`。因此内部单元素 batch 虽有 `query_id=0`，最终文本 TVF 与文本 CALL 单查都是四列。不能把中间结果当成用户 API，也不能因调用了 batch 内核就宣称单查接口返回批次列。[9][13]

我方 hit 的 row 是 nullable JSON STRING，不是有类型的业务记录；删除或失效命中可保留地址和 raw score、row=null。解码后再 JOIN/GROUP BY 可用，但作用于已截取命中集合，不是全表匹配或 filter-before。Lance typed 输出少了手写 schema 的负担，仍不意味着 JOIN/聚合下推、逐行 LATERAL 或完整表统计。

```sql
WITH h AS (
 SELECT score, from_json(row,'id BIGINT, tenant INT, body STRING') AS r
 FROM text_index_search('spark_catalog.db.items','body','ERROR 1064',10)
)
SELECT r.tenant, count(*) AS hits FROM h
WHERE r IS NOT NULL GROUP BY r.tenant;
```

这只统计 Top10 命中中的非空行，不是各 tenant 的全集，也不是各 tenant 各自 Top10。向量 TVF 已延期到物理 doExecute，computeStats 用 k×查询数及类型默认大小估上界，帮助 join 规划，不是准确基数，更不是去 driver。文本 TVF 在解析/规划时直接调用查询，不能把向量的延期执行优势泛化到文本。[9][14]

```sql
SELECT id, vec_distance(embedding,array(1.0D,0.0D,0.0D)) AS d
FROM spark_catalog.db.items WHERE embedding IS NOT NULL ORDER BY d ASC LIMIT 10;
-- 以下仅展示文本规则识别形状，不作为可靠上线查询
SELECT id FROM spark_catalog.db.items
ORDER BY text_score(body,'ERROR 1064') DESC LIMIT 10;
SELECT id FROM spark_catalog.db.items WHERE match(body,'ERROR 1064');
```

向量规则只接受单表、单全局升序距离、常量向量、正 LIMIT，可有投影/alias 和 embedding IS NOT NULL；业务 WHERE、join、aggregate、window、offset 会阻止改写。额外 tenant 条件保留 Spark 扫描过滤算距离，不能靠移到 TVF 外变成同义加速。文本要求精确源快照和全覆盖，当前 projection/排序 alias 与多处绑定未闭环，#2798 不能借来补此冻结；match 的 AND 组合也不承诺改写，未绑定 text_score 会报错，没有完整 BM25 fallback。[10][15]

vec_distance 两参默认 cosine，成功改写会绑定所选 index metric，**索引选择可能改变距离含义，不只是速度**，这是重要 API/SQL 正确性缺口。不存在受支持的第三个 metric 参数；注册器直接取 args(0/1)，可能忽略 extra，不能把宽松注册当功能。cosine SQL distance 为 1−相似度，dot 为负点积，距离越小越近；hit cosine/dot score 却越大越近。EUCLIDEAN 我方开平方，native L2 是平方和，差为 3 时分别返回 3 与 9，排名可能相同，阈值不能照搬。量化 score 还可能是估分。[10][16][17]

BM25 双方都有语料统计汇总，不是每个文件局部分数直接拼。这里的“全局”是相关索引语料，不自动等于当前全表活行；我方 hit 不补未索引数据，删除回表安全不等于持久 N/df 自动扣减。Lance partial coverage、残差和 fast 路径也影响评分域，匹配集合正确不等于实时统一精确排名。例中参数仅展示合同，不把默认或示意预算作为推荐。[13]

## 6. 运维与精确参数速查

CALL 以下均挂在 `spark_catalog.system`；table 为该 catalog 的表标识。不要把查询 column、构建 name、选择 index 当同一个参数，尤其缓存报告根本不按 table 过滤。load ACK 也不是所有 executor 永久暖命中的证明。[7]

| procedure | 参数 | 返回合同 |
|---|---|---|
| list_indexes | table | UUID、column、type、源快照、status、files/entries、definition JSON、index_name |
| check_indexes | table 必需 | writer/reader version、readable、stale、suggestion；非召回认证 |
| index_build_status | table | state、时间/age、文件进度、rows、stale/error；旧记录可能 UNKNOWN |
| index_cache_status | refresh 可选，默认 false | executor/host、epoch/mode、bytes、命中/覆盖、报告时间；不是重建 |
| load_index | table,index；retain_for 可选 | summary 与逐文件 status、plan_id、snapshot、保护期、executor/bytes/reason |
| offload_index | table,index | 同驻留报告体系；非 DROP |
| refresh_vector_index | table,column；index 可选 | UUID、源快照、文件数、entries |
| refresh_text_index | table,column | 同四列构建结果；无 index 参数 |
| cleanup_index_files | table | expired_snapshots、deleted_files/bytes、abandoned_attempts；不能与 build/refresh 并行 |
| drop_vector_index | table,column；index、purge 可选，purge 默认 true | index_uuid、purged；不是通用 DROP 所有类型 |

```sql
CALL spark_catalog.system.list_indexes('db.items');
CALL spark_catalog.system.check_indexes(table=>'db.items');
CALL spark_catalog.system.index_build_status(table=>'db.items');
CALL spark_catalog.system.load_index(table=>'db.items',index=>'emb_idx');
CALL spark_catalog.system.index_cache_status(refresh=>true);
CALL spark_catalog.system.offload_index(table=>'db.items',index=>'emb_idx');
CALL spark_catalog.system.cleanup_index_files(table=>'db.items');
-- 最后的破坏性操作，仅示意
CALL spark_catalog.system.drop_vector_index(
 table=>'db.items',column=>'embedding',index=>'emb_idx',purge=>false);
```

cache refresh 更新报告，index refresh 更新索引覆盖，offload 改驻留，cleanup 回收文件，不能因为都叫“刷新/清理”就混用。表中 schema 已逐个核 procedure 的 PARAMETERS/OUTPUT_TYPE：相关定义位于同 procedures 目录的 ListIndexes:46–61、CheckIndexes:72–91、IndexBuildStatus:51–85、IndexCacheStatus:44–75、IndexResidency:38–51、LoadIndex:33–41、OffloadIndex:33–39、CleanupIndexFiles:86–97、DropVectorIndex:51–67 行。

| 参数面 | 精确合同与来源（默认不等于推荐） |
|---|---|
| Lance VECTOR 位置 | table,query_vector,k；命名/位置不可混 |
| Lance VECTOR 命名 | table/query_vector 必需；num_results > limit > k，缺省 10；vector_column、distance_type、filter、prefilter、columns、refine_factor、nprobes、ef |
| Lance 其他向量项 | offset、version、with_row_id、lower_bound、upper_bound、bypass_vector_index、fast_search；分布式不接受 bounds 与 postfilter |
| Lance SEARCH | table、query（别名 search_query）、search_columns 必需；排名数量同三别名；columns/filter/version/with_row_id；不能靠位置形式传 search_columns |
| Lance HYBRID | table/query_vector/query；候选 candidates > num_candidates > candidate_count，下限 k+offset；rrf_k 默认 60；search_columns 可由底层推定，例中明确写出 |
| Lance match options | STRING key=value，以逗号分隔；fuzziness/operator/boost/prefix_length/max_expansions；phrase 的 slop 默认 0；multi options 只接受 operator |
| 我方 vector 单查 CALL | table,column,query ARRAY<DOUBLE>,k,index 可选；TVF 严格前四个位置参数 |
| 我方 vector batch CALL 位置 | **table,column,k,queries,query_table,query_id_column,vector_column,index**；不要照字段声明顺序猜；batch TVF 仍 table,column,queries,k |
| 我方 text 单/批 CALL | table,column,query STRING / queries ARRAY<STRING>,k；无 index；只注册 text_index_search，没有 text_index_search_batch |
| 我方向量 build 位置 | table,column,dimension,metric,structure,quantizer,mode,options,max_m,ef_construction,ef_search,name,max_entries_per_index_file |
| 我方文本 build 位置 | table,column,k1,length_norm,min_token_length,lowercase,**stopwords,max_entries_per_index_file** |
| 我方 metric/结构 | COSINE/DOT_PRODUCT/EUCLIDEAN 大小写不敏感，不接受 cos/l2/dot；FLAT 默认、HNSW/IVF；quantizer 默认 NONE；mode 与 structure 互斥 |
| 我方参数来源 | CALL index > session conf > submit conf；已有 spark.iceberg.index.vector.name；over-sample 同 session > submit，默认 1 且为正整数 |
| 我方搜索预算 | nprobe/ef_search 来自索引 definition，不是查询 conf；options 最后应用，可覆盖同名结构属性；over-sample 不扩大探桶 |

## 7. 常见坑：修正真实写法，而不是创造新接口

| 容易写错 | 应怎样改 | 仍须知道的限制 |
|---|---|---|
| Lance VECTOR 外 WHERE tenant=42 | 将资格移到函数内 filter，并显式 prefilter=true | 不是 ANN 精确/足额保证；外 WHERE 本来只筛 TopK |
| 我方 TVF 外 WHERE 当过滤前 TopK | 不把它解释为租户检索；需要该语义时保留普通 Spark 过滤与距离计算 | 当前无 TVF filter 参数，不能编一个 |
| 单向量 array(1.0,0.0,0.0) | 我方输入明确 array(1.0D,0.0D,0.0D) | DOUBLE 与 FLOAT/DECIMAL 不能随意互换 |
| batch array('[1,0,0]') | array('1,0,0') | 是 CSV 数字字符串，不是 JSON |
| lance_match 第三参 MAP | 'operator=AND,fuzziness=0' | options string 白名单会校验，不是所有入口都宽松 |
| VECTOR 拼错 nprobes 或传 index_name | 用真实 nprobes；不要假定有查询 index_name | TVF 未知命名键可能被忽略，重复键会覆盖 |
| 给 vec_distance 加第三参 metric | 使用真实两参并审查选定索引的 metric | 多余参数被忽略不代表三参受支持 |

```sql
-- 错在问题含义：只过滤全表先取出的十个向量命中
SELECT id FROM VECTOR_SEARCH(
 table=>'lance.default.items',query_vector=>array(1.0,0.0,0.0),
 vector_column=>'embedding',k=>10) WHERE tenant=42;
-- 改为“tenant=42 的候选域内检索”
SELECT id FROM VECTOR_SEARCH(
 table=>'lance.default.items',query_vector=>array(1.0,0.0,0.0),
 vector_column=>'embedding',k=>10,filter=>'tenant=42',prefilter=>true);
-- 我方不造 filter 参数：业务条件使规则放弃改写，按 Spark 扫描计算
SELECT id FROM spark_catalog.db.items
WHERE tenant=42 AND embedding IS NOT NULL
ORDER BY vec_distance(embedding,array(1.0D,0.0D,0.0D)) ASC LIMIT 10;
```

大小写不敏感不等于所有名字都等价。Lance TVF 键先转小写，未知键没有统一白名单，重复键后值覆盖；多个排名 alias 也是静默优先。FTS options 则规范化 key、验证未知 key，operator 值按大小写归一；OPTIMIZE 明确拒绝未知和重复项。索引名在部分 strategy 下沉时小写，REFRESH 忽略大小写匹配又拒绝歧义，SDK 混合大小写名不能概括为全链一致。我方 CALL 参数有数量/命名校验，但 TVF 的 cast/null/数组转换与 scalar 注册仍偏弱，不应把运行时转换错误包装成清楚的分析期类型合同。

## 8. 推荐：先统一可理解、可依赖的合同

第一优先是结果与参数来源文档：明确 typed 行或 INDEX_HITS、JSON/null、单批 schema、过滤在何时、metric 从哪里来；同一个业务问题不要因入口不同悄悄换结果域。随后补错误校验，避免拼错或多余参数“成功但没生效”。这比先换名字更直接减少误用，也保留 CALL 与既有运维体系的价值。

第二优先补 SQL 正确性和组合：文本 score 投影/alias/绑定、业务过滤与 fallback，向量默认 metric 和 index 改写的意义变化，以及规划期搜索和 driver 屏障。否则一个更像标准 SQL 的表面会让用户更放心地得到错误答案。标准 DDL、显式查询 metric、新 typed TVF 值得作为高层讨论方向，但本轮不设计未来 SQL、不命名新增参数、不批准实现或依赖。Lance 的 namespace 接入和单任务 hybrid 成本也要保留，不能把它当无代价模板。

延伸阅读：[Lance 索引内核复用](2026-10-28-lance-index-kernel-reuse.md)；[既有索引体系对比](2026-10-28-lance-spark-ann-comparison.md)。

## 证据索引

以下 18 项以已核源码行范围为准；我方 `../../` 路径相对本文件，行号只适用于文首冻结 SHA。外链全锁 SHA，无网络访问，源码存在不等于测试通过。

1. [catalog 函数通道与空 namespace](https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/BaseLanceNamespaceSparkCatalog.java#L289-L326)。
2. [已有部署与分布式配置](https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/docs/src/config.md#L5-L109)。
3. [TVF 名称、参数、优先级、解析和 namespace 解析](https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/lance/spark/search/LanceSearchTableFunctions.scala#L51-L156)：hybrid 183–276、distributed 300–381、parseArgs 391–483、schema 515–652、resolve 667–709。
4. [索引 DDL grammar](https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/antlr4/org/apache/spark/sql/catalyst/parser/extensions/LanceSqlExtensions.g4#L21-L46)。
5. [CREATE 类型映射，不含 ANN](https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/AddIndexExec.scala#L582-L646)。
6. [OPTIMIZE 参数与 driver native 调用](https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/execution/datasources/v2/OptimizeIndexExec.scala#L54-L87)：run 143–186；同目录 RefreshIndexExec.scala:118–187 核 scalar 反查和覆盖。
7. [我方 procedure 注册名](../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/SparkProcedures.java#L37-L84)：同目录 BuildVectorIndexProcedure:58–118、140–154；BuildTextIndexProcedure:47–81 核 build 参数。
8. [向量 batch PARAMETERS、query_table 与 ordinal](../../spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/VectorSearchBatchProcedure.java#L54-L90)：118–123 输入优先，157–208 排序和 CSV。
9. [三种 TVF 四参、文本 ByteBuffer 调用](../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/IndexTableFunctions.scala#L47-L120)：150–195 参数转换。
10. [scalar 实际名、默认 cosine、extra 参数边界](../../spark/v3.5/spark-extensions/src/main/scala/org/apache/iceberg/spark/extensions/IcebergSparkSessionExtensions.scala#L38-L103)。
11. [Lance FTS 规则与 options 校验](https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/scala/org/apache/spark/sql/catalyst/optimizer/AbstractLanceFtsPredicateRule.scala#L65-L116)：182–257 match，276–391 phrase/multi；LanceScanBuilder.java:528–587、602–711 核 residual/TopN；utils/Utils.java:56–61 核 stored _score 拒绝。
12. [HYBRID 顺序检索、融合和 RRF](https://github.com/lance-format/lance-spark/blob/ed2f49f94dfdb4ad81a2153cb658443864cd4d54/lance-spark-base_2.12/src/main/java/org/lance/spark/search/LanceHybridSearchRowPartitionReader.java#L59-L104)：181–183 RRF。
13. [单批 schema、JSON/null、单查 drop query_id](../../index-spark/src/main/java/org/apache/iceberg/index/spark/IndexSearch.java#L71-L89)：134–177 回表、175–186 单查重载；TextIndexAdapter.java:100–103 返回 ByteBuffer、207–284 全语料统计。
14. [向量 hit 与 computeStats](../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/VectorSearchBridge.scala#L91-L126)：230–242 stats；同目录 VectorSearchStrategy.scala:45–63 doExecute。
15. [向量 eligibility 与 metric 绑定](../../spark/v3.5/spark/src/main/scala/org/apache/iceberg/spark/index/RewriteVectorSearch.scala#L82-L158)：同目录 RewriteTextSearch.scala:64–87、144–164、198–221、243–259 核文本限制；VectorQueryParameters.scala:44–114 核来源。
16. [我方 score/distance 与开平方](../../index-vector/src/main/java/org/apache/iceberg/index/vector/VectorDistance.java#L45-L88)：107–118 L2。
17. [固定 native 平方 L2](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-linalg/src/distance/l2.rs#L106-L140)。
18. [固定目录 namespace structured FTS 已实现](https://github.com/lance-format/lance/blob/cbeec97cb893a66ad6b3db87f25a57ab0093f358/rust/lance-namespace-impls/src/dir.rs#L5500-L5509)：不能被 connector 跳测理由推翻，Java 制品闭包仍未证。
