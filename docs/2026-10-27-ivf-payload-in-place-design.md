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

# 索引通用内存与加载优化：IVF 原地布局、向量公共 Top-K

状态：**设计已获开发确认，A/B已收口；用户于2026-10-28确认C/D分工修订：C按当前实际可达状态记账并释放无用容器，D再引入独占U的原地布局。不是实现完成或性能验证报告。** 本稿替代已打回的初稿，对应独立伞卡 #2773、设计卡 #2779，处置评审备注 #2952、#3006。原设计源码核对基线为 `002552ffe`；C实施基底为 `ede87bcd8`。本次仅修订阶段分工，不调整收益验收门槛。

## 1. 目标、分工与范围

保留一份具有明确所有权的解压 payload，IVF 用 list 目录定位记录，不再为每条记录建立对象；查询边打分边维护有界 Top-K，最终才创建结果对象。目标是降低常驻内存、加载/prepare 分配和排序成本，**收益须分别实测，不能称为仅容量优化或保证固定倍数**。

| 层次 | 负责内容与位置 | 实际调用方及边界 |
|---|---|---|
| 通用索引基础设施 | `core`/`index-spark` 的加载所有权、真实记账、估算模型与 placement | 按 adapter 实际依赖处理，不硬编码 IVF；公共 Puffin 读取覆盖适用索引 |
| 向量公共工具 | `index-vector` 包内字节读取、完整结果比较、有界 Top-K | IVF 首先接入；FLAT 由 #2782 接入，分别验收，不扩大公开 API |
| IVF 专属表示 | `IvfKernel` 的目录、量化 code/factors 和未量化记录原地读取 | 不复制到 HNSW 图或文本倒排结构 |

不改已有 payload 编码版本，不要求重建旧索引，不新增配置、不改变默认值。保留 `IndexBlob`/`IndexEntry` 的公开防御性复制语义。文本 BM25 当前跨文件合并语料统计，不宣称它自动进入向量 executor 缓存/工作集估算路径。HNSW 先由图选择有限候选，其查询收益不能套用 IVF 全扫描候选的数字。

## 2. 现状与收益口径

读取链路为 `DistributedIndexSearch.readPayload` → `InMemoryInputFile` → `PuffinReader.readAll`/解压 → `PuffinIndexFileContainer.read`/`IndexBlob` → `prepareFile` → kernel。整容器、内存输入、blob 包装和 kernel 边界存在多处复制；`InMemoryInputFile` 构造也会 clone。默认 ZSTD 的压缩输入与解压输出是独立数组，不能把压缩容器当成可打分的原地字节。

IVF 当前 `readList` 为记录分配 `IvfEntry`、code/vector/factors 数组，查询为所有探测候选创建结果后全部排序。FLAT 当前逐条取得 payload、解码向量、创建结果并排序。因此减少对象和有界选择既可能节省内存，也可能降低查询耗时；不能用假设的单次分配时间替代测量全量排序。

定义 C 为保留的压缩容器，U 为解压 payload，S 为旧对象图，M 为新目录、字典、中心、量化器等元数据。下式均按实际对象图、数组头、对齐和共享引用去重，字节数不是磁盘文件长度：

| 状态 | 常驻组成 |
|---|---|
| 当前典型缓存 | C + S |
| 新结构但仍保留无用容器 | C + U + M |
| 核心目标 | U + M |

节省比例按 `(旧常驻 - 新常驻) / 旧常驻` 实测；既有“2.7 倍容量”“固定每条目降幅”“500 万次分配只花 50ms”“一份 payload 峰值”的承诺全部撤回。目录以外还有 O(listCount × dimension/padded) 元数据，稀疏索引不能把它当零。加载时压缩输入、解压输出、边界复制和准备结构可能同时存在；峰值另测，不能由常驻公式推导。

候选数为探测 list 的真实 count 之和。均衡等长文件下 `nprobe × sqrt(总条目数 × 文件数)` 仅是粗略估计，list 偏斜、显式 nlist 或 nprobe 截断时不适用。coverage 按集群至少一台 executor 持有的文件并集计算，不能用单机文件槽位比例代替；已 coverage=1 的表仍可观察预算余量、多索引容量和尾延迟。

## 3. 所有权、接口与真实记账（#2780）

### 3.1 核心实现先采用可证明的独占路径

保持公开 `prepare(ByteBuffer)` 签名，并对调用方的 `[position, limit)` 做防御性快照。快照不修改输入 position/limit，不受调用方后来写入原数组影响。只读 ByteBuffer 不能证明 backing array 不被其他引用修改，不能凭 `isReadOnly()` 借用。read-only、direct、非零 arrayOffset 和 slice 均有回归样本。

C按当前表示的实际引用处理所有权：IVF目前把快照解码成独立对象图S，prepare结束后无需保留原编码U，因此不为阶段分工人为增加无用U或虚加U费用。C先保证独立状态、完整记账及无用C释放。D切换到原地布局时才保留独占快照U，以U+M替代S，并完整计入U及数组头。该阶段调整不改变最终原地布局目标。

初始实现允许 `blob.payload()` 与 prepare 间仍有暂态复制，不以公共“零复制”改造为前置。prepared 不保留整个 Puffin 文件或外部 InputFile。加载完成后无用 C 不进入缓存根对象；其他 adapter 若保留解压输入，须按其真实引用计费，不一刀切释放。缓存释放/eviction 不破坏仍被 lease 使用的 searcher。

公共拷贝链后置到 #2774。若要转移独占解压数组，必须有受控所有权证据；不能修改公开 `IndexBlob.payload()` 的复制合同，也不把现有 `toBytes` 偷换成返回任意完整 backing array 的方法。

### 3.2 新内存模型与旧记录迁移

采用新的内存 **modelVersion=2**，不改变 payload 的 v1/v2、reader 编码版本或 manifest 存储格式。`payloadBytes` 仍为 **blob 压缩前的编码 payload 长度 U**，`fileBytes` 仍为物理容器 C，二者保留诊断用途，不能自动相加作为常驻。

新模型的 `preparedBytes`/运行时 `estimatedBytes()` 描述 searcher 实际可达的独占状态 P：C阶段IVF为实际独立S，D原地布局为U+M；HNSW为实际图/映射，FLAT为实际保留的entries/字节。排除已经释放的调用方输入和query scratch；只要内部持有快照U，就必须完整计入，不能借本次分工调整漏记。澄清相关javadoc：排除外部输入缓冲，不是排除searcher自己拥有的数组。模型仍是明确JVM布局下的结构估计，不是精确retained heap实测。

新缓存 resident 以 P 为主，再加确实被缓存额外保留且不属于 P 的状态 E。共享数组按身份只计一次；slice 若保留大 backing array，计完整数组。核心独占路径 E 中没有 C。`IndexCacheMemory` 的新模型转换不再无条件加 `fileBytes`；build、runtime、manifest→placement 使用同一估算函数，不能一边报 U+M、一边仍加 C。

model=1、未知模型、缺失 memory、reader/runtimeProfile 不匹配的旧估算不能直接用于精确容量决策：由统一 resolver 加载并按当前模型重估，或返回显式 unknown，不能返回0、不能继续声称严格预算已获保证。保留既有 legacy fallback 的明确“不准确估算”标识；managed 规划不得把 unknown 当已知可容纳。新估算可按不可变文件身份、reader、model、runtime 缓存；升级使旧缓存失效。无需重建或重写旧 payload，也不强制更新旧 manifest。跨 kernel 的 model 标记与公式一起升级，不能统一换版本号却保留不一致语义。

验收同时核对可达数组与收费模型；仅 build 数字等于 prepare 数字不足以证明没有双方共同漏账。

### 3.3 已确认的共享元数据与内部消息记账

用户已确认将缓存额外状态E及driver/executor内部消息调整纳入C。固定的P加包装常数不能覆盖完整key、owner、map/set backing和会增长的ownership关联。缓存按可达组件的集合记账：同一物理文件、owner及共享backing在同一executor只计一次；跨executor的独立副本各自计费。文本身份保留完整值，不靠截短或换成不透明ID消除费用。

区分索引驻留R、必要控制元数据Q及尚未占用的预留增量H。R包含prepared P及仍被驻留/lease持有的附加状态，最后引用释放后对应索引resident归零；Q包含仍需保留的epoch、paused策略等控制状态及其容器，单独计入总预算，不能以“控制面”为名漏掉增长。实际已用为R+Q，有效占用为R+Q+H；releasing是已用中的待释放子集，不再次相加。固定空管理器、锁和计数器为明确基底，不将扩大后的容器backing冒充空基底。

owner/group信息显式沿内部消息传递，不能从缺席值推断零费用。原缺owner的三参数workingSet helper不能继续报告完整resident为known；现有查询转为包内owner-aware入口。规划、预留、接纳、ACK及状态上报消费同一个集合投影；ACK仍按现有计划/命令身份确认文件coverage，但预算不再通过逐file数字盲目累加。较旧或重复收费快照不回滚新状态，失败接纳不留下未收费的元数据，释放不因没有空闲预算而失败。

仅调整index-spark包内状态和现有通信通道的消息内容，不新增网络服务、公开Java接口、SQL/CALL参数或运维开关，不改payload/manifest格式、调度/LRU/保护/删除语义。算法、模型、预算字段及测试必须一起迁移；当前E缓存根回归仍RED，本文不代表完成。

实现与验证细则见`docs/2026-10-28-cache-owned-metadata-and-internal-protocol.md`。

## 4. IVF 编码与目录（#2776）

### 4.1 格式不变：大端、modified UTF-8、padded 步长

继续复用 `VectorPayloads.readHeader` 的 legacy header 和当前 kernel 版本检查。数值字段由 `DataOutputStream` 写出，读取固定 **BIG_ENDIAN**；Puffin 外层部分小端字段不影响 IVF。字符串字典使用 `DataInputStream.readUTF`，匹配 `writeUTF` 的 unsigned-short 字节长度和 **modified UTF-8**，不是普通 UTF-8。

设 d 为逻辑维度，p 为 RaBitQ 使用的下一个2次幂 padded 维度，b 为 bits；计算中间值均使用 long 并检查 representability：

```text
codeBytes       = ceil((long)p * b / 8)
quantizedStride = 4 + 8 + codeBytes + 3 * 4
plainStride     = 4 + 8 + (long)d * 4
quantizedListHeader = (long)d * 4 + 4
plainListHeader     = 4
```

例如 d=129、b=2，p=256，codeBytes=64，量化记录步长=88，不能按逻辑维度得到33字节 code。padded 求值、乘法、加法和 long→int 转换均校验，不能复用可能溢出的 int 公式。

目录保存每个 list 的记录起点、count，以及量化中心位置；保留用于选择 list 的 trained centroids，不能用 listQuantizer 的中心替代它。中心与量化器初期允许解码为小数组，均计入 M；共享 signs、优化 centroid 选择不是核心前置，也不能改变 seed/算术。

### 4.2 prepare 验证所有记录，选择正确性而不是虚假的 O(header)

公开输入先快照；在快照内解析头、字典、centroids 和 list header，记录字节边界。每个 list 用 checked long 计算 `recordStart + count * stride`，先验证不超过有效 payload 区间，再访问/跳转。分配任何按文件计数的数组前，用剩余字节及最小编码长度约束计数，防止畸形 nlist/字典数量直接触发巨大分配。

**初期 prepare 保留全记录校验**：扫描每条记录的 dataFileIndex，含查询不会探测的 list，验证 `0 <= index < dictionary.length`；验证 list count 非负、累计 count 与 entryCount 相等、中心和记录不截断。按新格式规范加强计数一致性只拒绝畸形文件，不改变合法旧文件的可读性；不借本次改动添加新的向量数值规范。已有 reader 对记录后的尾字节不强制EOF，本实现也不无故改变该行为。

验证扫描不 clone code/factors、不创建逐条对象，但仍是 O(N+L) 次记录索引访问；快照本身也是 O(U)。因此撤回“只读 header 就完成 prepare”“微秒/50ms”等承诺。失败发生在 prepare 返回之前，以具体字段/list/record 的可定位异常报告，不能等碰巧探测该 list 才发现错误。

### 4.3 code 定位不对绝对地址做位乘法

```text
localBit = (long)coordinate * bits + bit
byteAddress = (long)codeBase + (localBit >>> 3)
bitInByte = localBit & 7
```

byteAddress 通过合法 code 区间和 U 边界检查后才转换成 int；不能用 `int bitOffset = absoluteOffset * 8`。因素读取从 `codeBase + codeBytes` 读取3个大端 float。初期可以用单次 search 私有的 `float[3]` scratch 适配现有量化器方法，也可增加读取标量的包内重载；不得使用 per-prepared 可变 scratch。无论选哪条，都有逐分数参考测试，保持现有计算顺序。

## 5. 向量公共 Top-K 与数值合同

公共工具留在 `index-vector` 包内，接收显式记录定位/字段读取方式，不依赖 IVF list 布局。比较依次是 metric 的分数方向、文件路径的 Java `String.compareTo`、`Long.compare(rowPosition)`。若建立字典排序 rank，必须按路径值排序、重复路径得到相同 rank，不能用字典首次出现编号。只有以上都相同的完全等价记录，才用遍历序作为稳定性末级规则，不能用 visitRank 替代文件/位置排序。

分数使用现有 `VectorScoring.compareScores` 的 `Double.compare` 语义：欧氏升序，dot/cosine降序；保留 NaN、±0、±Infinity 的顺序，不用减法或普通大小比较代替。保持旧 float/double 运算次序、类型和累加顺序。例如量化查询的 `double difference = query[d] - center[d]` 先执行 float 减法，而未量化欧氏已有显式 double 转换；不能为了原地读取顺手统一精度。不能把 Math.sqrt、省略校正或FMA当作本次优化。

search 在选择 list 后由已验证目录计算候选总数 V，long累计且不超过 entryCount。目标槽位 T=`min((long)topK,V)`；topK<0报错，topK=0或V=0返回空。不能按外部 topK 直接 `new array[topK]`。堆从小的内部容量开始，按实际收到的候选增长，checked 算术保证不超过 T；这是内部实现常量，不增加用户开关。

堆根为最差候选；保存 primitive score、记录 offset 和必要的稳定性序号，不给每个候选创建结果对象。路径rank/位置仅在比较需要时读取；幸存者按同一总序输出并创建 `IndexSearchResult`。大topK且V很小不应有大额预分配；大topK且V确实巨大时返回那么多行本来需要O(T)内存，不能承诺无限输出不OOM，也不能静默减小用户topK。可表示性/长度问题明确报错，内存峰值随T报告。

FLAT 的 #2782 复用字节读取与堆，但仍保持精确打分、entry-bearing encode/decode 和 legacy 可读性；处理 `IndexEntry.payload()` 的复制边界时不得削弱不可变合同。准备期是否保存紧凑字节或已解码向量，须计入该卡的prepare成本与常驻，不能把分配搬到prepare后称为消失。

## 6. 后续公共优化，不阻塞核心

#2774 逐点量容器、`InMemoryInputFile`、Puffin解压、blob边界及kernel复制，优先消除有所有权证明的冗余复制。共享的只能是合适的解压数组，完整ZSTD解压不等于单数组峰值；各kernel分别回归，文本仅覆盖实际公共读取，不重写BM25统计。

#2777 优先利用与当前model/reader/runtime匹配的可信 memory/structure。旧估算统一重估或unknown。当前压缩blob不能按物理offset直接Range出逻辑header；仅读前缀需要实际codec支持。字典、中心及逐list count分布在正文中，目录与验证也未必只靠前缀足够。若现有codec不能做到小量读取，就保留明确的全量加载回退并报告读字节，不能声称已实现“免全量”。无新依赖或公开API扩大未经讨论加入。G的读取优化不跳过第4节首次加载校验。

## 7. 基线、TDD与验收

### 7.1 正确性与模型测试先定义

| 测试组 | 必须覆盖 |
|---|---|
| 旧格式 | 旧写者生成legacy/v1/v2 payload；大端编号/long/float；writeUTF/readUTF路径含NUL、中文、重音、补充字符 |
| 步长/定位 | d=129、2bit及各1–8bit；非2次幂维度；256MiB与512MiB上下偏移；checked乘加和code边界 |
| 完整验证 | 未探测list非法文件编号、负count、总数不符、截断中心/code/factors、过大头部计数、允许的尾字节 |
| 输入与并发 | 非零position/limit、slice、read-only/direct、外部修改；并发search的私有scratch；lease释放 |
| Top-K与数值 | 同分边界/乱序及重复路径字典、各metric；NaN/±0/±Infinity比较器；大topK小V、空索引；逐score原始double位值/逐结果与旧参考一致 |
| 内存与迁移 | 可达数组身份去重、U完整计入、C释放、build/runtime/placement相等且下界正确；旧model、未知/profile不匹配；IVF/HNSW/FLAT不同状态 |
| 集成 | 全部索引回归、recall、实际拥有searchBatch的模块、FLAT精确排序；文本读取和统计合同不变 |

大型offset的算法测试先用不需要巨堆的地址计算样本，独立资源探针再实测跨256/512MiB的真实payload。极端比较器输入不代表放宽已有写入端finite约束。新目录/评分参考不得与待测代码复用错误的解析算法；使用旧写者fixture和旧全量排序作为独立参照。

### 7.2 测量边界与通过标准

B在改产品前固定commit、镜像、JVM/GC/堆、压缩codec、真实文件数与条目数、d/padded/bits/nlist/nprobe/topK和查询集合。分开量：容器load含读取/解压/边界复制，kernel prepare含快照/完整验证/目录，driver估算，kernel search及端到端SQL。记录各阶段峰值、分配字节、prepare时间、常驻对象图；search给暖态p50/p99与冷态数据，原始证据可复跑。工具进并列 `iceberg-tools` 仓，产品测试进拥有实现的模块。

初期IVF门槛保留：同口径常驻下降≥30%、query分配降低至少一个数量级、暖态p50不劣于改前（5%容忍度），回归/recall/searchBatch通过。5%是验收门槛不是测量精度；A/B配对交替、反转先后、充分暖机并多轮重复，样本不足或波动跨门槛时标不确定，不用“两轮符号一致”替代统计证据。p99、峰值及prepare成本如实报告，不能把明显劣化藏在内存收益后；失败先定位/回退，不能迁就设计。

至少覆盖128维1bit/4bit/未量化及129维2bit正确性，按小/大topK、nprobe和文件布局报告。10M单文件是独立实验形态，不能把现有10M多文件索引总磁盘字节写成某单文件大小。100M按真实文件并集coverage、副本和每executor预算报告；baseline coverage=1仍测试余量，不强制要求提高coverage。

FLAT独立测分配、排序和精确结果，不套用IVF内存门槛。所有性能数字在B/E/H实际测量前均为未验证。资源测试用受控子进程、预制fixture和明确堆/GC记录，不在普通单测写固定750MB或固定50ms断言；`getThreadAllocatedBytes`若不支持须报告，不能把缺测当0。测试部署用明确新jar/镜像重启测试服务后真实SQL smoke，清理junk数据/索引并保留正式证据。

## 8. 子卡、交付与复审门

| 子卡 | 交付 |
|---|---|
| A #2779 | 本修订稿、四项确定错误和新增边界逐条处置，双份逐字节保存并独立复审 |
| B #2775 | 固定改前基线与独立探针，不确认尚未测量的收益 |
| C #2780 | 当前表示的独立状态、model=2真实记账与旧估算resolver、公共adapter快照合同；IVF不人为长期保留已解码输入U |
| D #2776 | 包内向量工具+IVF独占U/目录/完整校验/原地打分/有界堆，以U+M替代S并完整计U |
| E #2778 | IVF核心回归、配对测量、集群smoke及容量证据 |
| F #2774 | 后续公共拷贝链减少，逐kernel验证 |
| G #2777 | 后续driver估算读取优化，真实codec路径和回退 |
| H #2782 | FLAT第二真实调用方及独立验收 |

依赖保持 A→B→C→D→E，E→F/G/H；F/G/H可分别推进，不是D/E前置。不使用初稿P0/P1/P2顺序，不预设工期和未验证的收益。首次落地保持中心/量化器算术和公开复制合同，centroid选择、signs共享等可另行测量，不强制捎带。

原稿已完成独立只读技术复审并获开发确认，A/B已收口。C/D分工修订已获用户确认；C、D及后续交付仍须分别实施和复审，不意味着整个#2773完成。plain的≥30%适用范围仍待确认，本次没有调整§7.2正式门槛。仓库`docs/`为权威版本，umem保存同格式同字节副本并校验sha256；模型、布局或验收发生变化，两份一起更新。

## 9. 评审处置索引

| 意见 | 落实位置 |
|---|---|
| 小端错误、modified UTF-8、padded维度 | §4.1与旧格式/步长测试 |
| visitRank错误、NaN/±0/Infinity、浮点精度 | §5完整比较和逐score参考 |
| 256MiB绝对位地址溢出 | §4.3及大offset测试 |
| C/U共享、只读不等于不可变、漏记U | §2、§3.1、§3.2所有权/计费 |
| 未探测list校验、总数、截断和prepare复杂度 | §4.2明确全记录验证，撤回header-only承诺 |
| 超大topK预分配 | §5 T=min(topK,V)，按真实候选增长 |
| 旧model迁移及driver压缩header | §3.2、§6统一重估/unknown与实际读路径 |
| 固定收益、单机比例冒充coverage、阶段峰值 | §2、§7.2基线及独立测量 |
| 分层、FLAT调用方、公共优化后置 | §1、§5、§6、§8 |
