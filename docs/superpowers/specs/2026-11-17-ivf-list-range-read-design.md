<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# IVF 单文件分块与按范围读取

本文记录原型设计。性能背景见[向量查询性能诊断](../../2026-11-17-vector-query-performance-analysis.md)。原型评估两项变化的收益，一是增大单个索引覆盖的数据量，二是冷查询只读取被探测的 IVF 列表。

## 配置

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `max_entries_per_index_file` | Spark 建索引过程为 `1000000` | 已有参数，以源数据文件的记录数为依据，为每个索引组设定目标行数。`0` 表示每个源文件独立建索引。 |
| `options['ivf-list-blocks']` | `'false'` | 构建时设为 `'true'`，以可独立读取的列表块写入 IVF 索引。已有索引需要重建才能采用该格式。 |
| `spark.iceberg.index.vector.ivf.range-read.enabled` | `false` | Spark 查询时设为 `true`，在整索引缓存未命中且文件支持选读时，按字节范围读取所需列表。 |

`range-read` 描述的是一次 I/O 的范围。目录把 list ID 转为文件中的偏移和长度。构建选项决定文件有没有这种布局，查询开关决定如何读取。同一份分块索引可以切换两种读法。

## 索引分组与文件布局

沿用 `IndexGrouping`，按文件路径排序后对快照里的源文件分组。超过目标行数的单个源文件自己成组。每组独立训练 IVF，每组写一个 Puffin 索引文件。刷新涉及某个组时，按现有覆盖关系重建受影响的组。

```text
一个 Puffin 索引文件
├── IVF 描述区
│   ├── 格式版本、维度、量化参数和 IVF 聚类中心
│   ├── 列表目录（list ID → 字节偏移、长度、条目数）
│   └── 源文件字典（file ID → Iceberg 数据文件）
└── nlist 个列表块
    └── 每块包含向量或量化编码、行定位信息及所需的列表级量化数据
```

从 Puffin footer 找到 IVF blob，再用 blob 开头的格式标记、版本和长度定位描述区。目录中的偏移以 IVF blob 的起点为基准。原型关闭该 blob 的外层压缩，使文件中的字节范围能直接读取和解码。关闭压缩可能增大物理文件，需同时测量这一代价。

沿用现有 Puffin 发布和清理机制、旧格式整读路径，以及 `IndexPayloadLimits.MAX_PAYLOAD_BYTES` 的整读大小约束。分块索引也要支持关闭选读和整索引预热，因此构建时须确保产物能整读。分组参数只约束目标行数，构建时还须检查实际索引大小。

## 查询与回表

1. Spark 沿用每个索引文件一个检索任务。任务先查 Executor 的整索引缓存，命中时复用已准备的查找结构。显式驻留的条目沿用现有租约。
2. 缓存未命中时，分块格式且开关开启的文件走选读。其余情况走现有整读路径。选读先加载描述区，根据 IVF 聚类中心选出初始 `nprobe` 个 list，再读取对应字节范围。已有过滤逻辑需要扩大探测时，继续读取新选中的块。可以把相邻的已选范围合成一次读取。
3. 沿用现有评分、候选合并和回表语义。用候选的 `file ID + 行位置` 按源数据文件组织回表，应用快照与删除过滤。打开索引时验证目录范围都位于 blob 内，读取列表时校验块内容。描述区、目录或已读块损坏时报告读取错误。

I/O 通过 Iceberg `FileIO`。流实现 `RangeReadable` 时可用定点读，通用路径在同一查询流上 `seek` 到偏移后读足长度。HDFS 支持这种读法，IVF 块的边界可以跨 HDFS block。S3 的 Range GET 一次只能返回一个连续范围，零散的块会增加请求次数。

原型沿用现有的整索引预热、容量管理、LRU、卸载和状态查询。选读期间加载的块由当次搜索持有，因此反复查询冷索引仍会重复读；可用 `load_index` 预热整索引。使用现有批量入口时，按每条查询选出各自的列表和 top K，用到同一块时可在该任务内复用读取结果。

## 验证

- 正确性：固定索引和查询输入，对比分块索引的整读和选读结果。覆盖原始向量、RaBitQ、过滤导致的探测扩大、多源文件回表、旧格式整读、整索引缓存命中、目录越界和被探测的块截断。记录底层读取范围，确认选读路径只读取必要元数据及被探测的列表。
- 选读收益：固定数据快照、分组、IVF 参数、查询与过滤条件和 Spark 资源，比较现有压缩格式整读、分块格式整读、同一分块格式选读。后两组对比读取策略，第一组反映相对当前实现的整体收益。冷查询关闭 Executor 整索引缓存并确认没有驻留命中。热查询显式预热并验证缓存命中。分别测 HDFS 和目标对象存储，记录实际读取字节数、读取次数、冷查询和热查询 p50/p95，以及文件大小。
- 分组收益：单独改变 `max_entries_per_index_file`，比较索引文件数、候选数、构建峰值内存和延迟。比较不同分组的查询性能时同时报告 recall@K，因为每组训练的聚类中心会随分组改变。若设为 `nlist=0`，同时记录每组实际的列表数。
