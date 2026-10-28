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

# #2762 接手后的第一阶段实测

## 执行状态

用户已授权强制接手。旧#2762/#2763认领释放后正常重新认领：`pi-191-coordinator`负责伞卡，`pi-optimizer-design`执行首卡，每个角色仅一张doing。后续仍按2763→2764→2765…2771顺序，前一子卡未验收完成不进入后一张。

- 协调worktree：`/Users/jyxc-dz-0101035/workspace/iceberg-wt/2762` / `card/2762`
- 新实验worktree：`/Users/jyxc-dz-0101035/workspace/iceberg-wt/2763` / `card/2763`
- 目标源码：`apache-iceberg-1.9.1`，commit `f40208ae6fb2f33e578c2637d3dea1db18739f31`
- 此次试移植源：`6b4cf446b`
- 原`191-base`的四个未提交实验文件保持原样。

本阶段没有移植后的产品交付、镜像发布或集群性能结论。

## 实测一：厂内runtime的内部依赖

在shai直接读取 `/data/workspace/vendor-jars/iceberg-spark-runtime-3.5_2.12-1.9.1-STEP-rc1.jar`：

- jar SHA256：`1d57b6914a46b6153d40c44cca297088a6d29860b5f976973a61979800986d32`
- 内置Maven属性：Avro **1.12.0**，各Parquet模块 **1.15.2**。
- 确实包含`org.apache.iceberg.shaded.org.apache.avro.LogicalTypes$TimestampNanos`和shaded `PlainParquetConfiguration`；没有未重定位的Avro TimestampNanos类。
- vendor `TableMetadata`的`SUPPORTED_TABLE_FORMAT_VERSION`常量为 **3**（javap实读），与上游tag一致。这不意味着任何1.12新增功能都可读，但旧论证“1.9.1一概读不了v3”不成立。

**需要校正的前提**：外部Spark Avro1.11.4/Parquet1.13.1与runtime内部shaded依赖是两层。保持Spark外部jar不替换，不要求把Iceberg内部依赖也强制降到那些版本。此前“厂内编译1.15.2、实际运行1.13.1”的推断不能仅凭容器外部jar目录成立。

## 实测二：新实验树的编译与测试

| 组合 | 操作 | 结果 |
| --- | --- | --- |
| 原始1.9.1源码、Avro1.12.0/Parquet1.15.2 | core、parquet编译，Java17、显式Spark3.5/Scala2.12 | exit0，34秒 |
| 同一源码、强制Avro1.11.4/Parquet1.13.1 | 同样编译命令 | exit1，4个TimestampNanos/timestampNanos缺失引用，9秒；Parquet被core失败阻塞，不能据此推断其结果 |
| 恢复原始依赖 | 完整Parquet test | 516项，0失败/错误，13跳过，57秒 |

没有为了编译成功删除纳秒类型、variant路径或升级源码基线。

测试首次被旧Git-properties插件挡住（它不识别linked worktree的.git文件）。临时提供一个共享对象的bare Git元数据镜像，HEAD固定为实验源码`f40208ae6`，不再沿用“指向主树.git而误标主树SHA”的旧绕行。该适配仅在实验init脚本，未改产品构建。

生成的`iceberg-build.properties`实际记录`git.commit.id=f40208ae6…`、branch `card/2763`。默认build.version为`1.10.0-SNAPSHOT`，这是原构建脚本根据最近tag自动递增minor的标签，不是源码升级。正式发布必须显式按已批准的`1.9.1-shb.N`命名，不能使用该默认标签。

## 实测三：cherry-pick采样与其局限

独立临时树只试了源版本core/index生产目录的历史。这个选择包含36个提交，**不是完整索引迁移集合**：

1. 首个MVP提交`56d40a3ad`直接应用成功。
2. `d457bdc1b`触发Git目录重命名推断，将本应属于core/index的测试建议移动到Kafka目录。核对其package后恢复到源意图的core路径，并仅在试验命令关闭目录重命名推断；不是软件API冲突。
3. `fa67a83e4`应用成功。
4. `ab02da1c2`出现6个未解决路径：两条core测试、版本目录、文档、Spark builder与quickstart测试。缺失的测试/Spark文件有独立前置提交，未被“只选core生产目录”纳入，版本目录也存在上下文差异。

因此：不能用这个4/36采样断言“整个cherry-pick不可行”，也不能继续重复“只会冲突3个文件”。下一步需生成包含测试、Spark入口和format-model基础层的迁移依赖集合，再评估整批提交或按最终快照移植的实际成本。

## 仍待收口

- #2763的完整迁移集合及冲突面未完成，不能标done。
- #2764仍负责源码/补丁风险、内部shading策略、集成分支及发布命名的最终收口。
- 1.9.1产品分支不能rebase到当前1.12主线，否则又把新基线带回去；#2764需明确专用回溯集成分支，不能机械套用“每卡rebase main”。
- baseline516项通过不等于新增索引模块或shaded runtime的运行验收通过。真正的集群与10M回归仍在后续验证卡。

证据：`/Users/jyxc-dz-0101035/workspace/iceberg-reviews/2762-start/`中的vendor-runtime.log、两个编译日志、Parquet测试日志与两份cherry-pick JSON记录。
