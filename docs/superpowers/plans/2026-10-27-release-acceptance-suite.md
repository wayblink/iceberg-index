<!-- Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See NOTICE. Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0 -->
# 索引发布验收套件 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement task-by-task, with independent review before accepting each deliverable.

**Goal:** 实现#2787全部验收工具；今天新镜像/新10M表跑通standard，100M执行延期。
**Architecture:** Python标准库定义manifest/results及门禁，shell负责边界清晰的构建/提交/日志采集，Java Spark drivers实现真实case。先L0协议，再L1/L2/L3，最后G闭环；已有工具仅复用可验证的行为。
**Tech Stack:** Python3标准库unittest、Java17、Spark3.5/Scala2.12、Bash、Docker/Kubernetes/HDFS；无新依赖。
**Spec:** `docs/2026-10-27-release-acceptance-suite-design.md`（用户2026-10-27授权实施）。

## Global Constraints
- 工具只在独立iceberg-tools仓库；产品构建在独立Iceberg worktree。具体路径add，review后ff-only合入。
- 冻结产品f33843cd3与工具6a008ef起步，运行不得随main后来变化而混装；新提交下一次release验收。
- 新镜像标签含产品SHA、digest和逐jar哈希入档。今天不做1.9.1回溯或依赖降级（#2762另项），不把测试镜像视为已适配厂内发行版。
- 新建run命名空间10M Parquet/Lance与小fixture，来源资产只读；不覆盖旧SIFT表。100M只实现入口，不运行，结果not_requested而非pass。
- 两合同分别断言；metric/查询ID/source snapshot固定；缺query/缺case/非法数值/截断拒绝，--only不可晋升完整baseline。
- 生命周期与迟到ACK保持真实断言，现有产品缺陷保持fail并定位，不为套件绿灯调默认值。

## Review Focus
1. 错镜像/旧jar与新manifest不能pass，digest匹配当前发布而非上一版。
2. 多臂混并、缺query及done但少样本不能掩盖recall回归。
3. INDEX_HITS deleted row=null不能误按SQL完整表答案比较。
4. 假空闲/Completed旧pod/监控失联及cleanup越界。
5. 子集、首版、竞争污染性能与并发baseline晋升资格。

## 文件与接口（工具仓相对路径）
- `release_acceptance/schema.py`：`load_json(path)`拒绝重复键/nonfinite；`instance_key(identity)->str`规范复合身份；`validate_manifest(data)`、`validate_results(manifest,results)`抛ValidationError；`expected_instances(manifest,only=None)`。
- `release_acceptance/environment.py`：`classify_pods(pod_list,run_id)->dict`、`verify_identity(expected,actual)->list`、快照/资产与时间戳规范；不隐藏kubectl非零。
- `release_acceptance/gate.py`：`evaluate(manifest,results,baseline=None)->dict`返回fail/blocked/eligible/metrics，`promote(...)`原子expected-prev-hash更新。
- `release_acceptance/report.py`：`render(manifest,results,decision)->str`固定生成时间/排序、无外链。
- `release_acceptance/cli.py` + `release-e2e.sh`：manifest输入、--only/--profile、输出run目录、执行case与验证、exit0/1/2/3。
- `release_acceptance/drivers/`：Java `ReleaseFunctionalChecks`（L1）、`ReleaseAssets`（新10M资产/快照）、`ReleaseResidencyChecks`（L3）；依赖发布镜像/Spark，不把功能判断藏进主类日志grep。
- `cluster/build-release-image.sh`：使用冻结产品构建产物、独立staging、digest/sha采集；不改共享image-jars。
- `tests/test_release_schema.py`、`test_release_environment.py`、`test_release_gate.py`、`test_release_report.py`、`test_release_runner.py`：离线RED/GREEN；真实Spark输出fixture作为parser输入，不伪造集群通过。

## Task L0 / #2788：协议与环境
- [ ] RED：测试复合case键隔离臂/metric，缺字段/重复键/NaN/缺case/query/repeat拒绝；new digest与当前manifest匹配通过；old Completed不占用、自身不阻塞、失联blocked。
- [ ] GREEN：实现schema与environment，先用离线unittest跑绿，结果是每case-instance原始断言/样本/证据。
- [ ] 使用现有构建流程编译冻结源码新镜像；provenance与实际启动的driver/executor身份核对。
- [ ] review/提交/L0交付给后续任务；环境未取得只标blocked，不done。

## Task L1 / #2789：功能driver
- [ ] RED：合同映射/JSON值比对/deleted hits/零分母/tie与数值oracle单测；已有功能回归先作参考。
- [ ] GREEN：小表全kernel writer及BM25，三入口分合同、删除/残差/边界和显式错误；新数据fixture与只读旧格式样本区分。
- [ ] 本地Spark和新镜像集群先小规模迭代，失败逐case回填；不因#2784跳过值断言。

## Task L2 / #2790：资产与曲线
- [ ] RED：BENCHRESULT的index/arm规范化、样本与完成标记、pk/真值/metric、重复query与预期格子覆盖。
- [ ] GREEN：复用并修正VectorBenchmark/collect工具，按冻结新10M表+六查询执行；记录计划/索引名/快照，FLAT单点合同独立。
- [ ] 100M配置/入口完成，但今天不启动；报告明确未请求，不填旧数字冒充本版。

## Task L3 / #2791：驻留与故障
- [ ] RED：已有smoke结果adapter、complete/partial/ACK/coverage与预算断言，错误缺口及旧epoch被拒。
- [ ] GREEN：复用IndexResidencySmoke并补保护期/快照/offload/故障/双owner断言，真实输出规范样本。
- [ ] 用新镜像小fixture及10M测；现存RED定位另卡修复，不弱化门槛。

## Task G / #2792：整合与发布门
- [ ] RED：单臂0.8→0.4其余不变、缺臂/query/done截断/NaN、跨metric拒、子集与首版/并发promotion不可错误通过。
- [ ] GREEN：按expected全集及逐查询规则判定，报告重生确定性，功能/性能晋升分别控制。
- [ ] standard真实闭环：运行→原始证据→门禁→报告→人工批准首版baseline。没有授权不得自动覆盖认可基线。
- [ ] 最后独立审查整套工具，修复重要问题后重跑；文档与umem同字节保存。

## 今天的执行边界
用户已要求开始实施并用10M迭代，因此以上任务连续推进；首次镜像/资产环境验证即启动，不等待100M窗口。100M明天再执行，不设置自动定时或偷偷运行。
