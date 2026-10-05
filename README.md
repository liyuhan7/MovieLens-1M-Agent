# MovieLens 1M 数据治理 Agent

用**真实的 Hadoop MapReduce 管线**清洗 MovieLens 1M，用 **LLM Agent** 把“清洗 + 五维质量评估”
串成一句自然语言就能触发的流程，并在其上加一层**数据治理平台**：可恢复任务、结构化产物与
证据、内容校验、“只正式发布一次”的结果，以及正式结果/证据查询、历史与版本比较、Hive 只读
分析、性能基线和备份恢复。

- **清洗与评分**：单次完整管线发起 12 次 MapReduce 作业调用（9 种作业：提取参照 ID ×2、
  评分 ×6、清洗 ×4），在容器内的 Hadoop 3.3.6 上以真实 MapReduce 执行（local 模式，或治理
  形态下的单节点 HDFS/YARN）；Accurate / Complete / Unique / Up-to-date / Consistent 五维评分
  并登记版本。
- **Agent**：OpenAI Agents SDK + DeepSeek `deepseek-chat` 驱动；只读工具取数 + 触发词门控的
  写工具，回复中的分数经报告白名单校验，trace 落盘可回溯。
- **治理平台**：MySQL 元数据与 Run/Attempt/Job 模型、Parquet 产物与证据索引、内容校验门禁与
  唯一发布协议、发布中断恢复与只读存储对账、Run 绑定的 Agent 结论引用校验、历史列表与版本
  比较、Hive 只读外部表与统计对账、Job/Task 性能基线与优化语义等价校验、保留策略预览、
  备份与空库恢复、维护排空与镜像切换回退。

系统设计、清洗规则与运行说明见 [docs/README.md](docs/README.md)（文档导航）。

## 目录结构

```
ml-1m/
├── data/                                        # 原始 .dat（不入库）+ 探查脚本
├── docs/                                        # 设计、规则、运行说明与参考资料（见 docs/README.md）
├── hadoop/
│   ├── src/main/java/mliter1/IterationOne.java  # 全部 MR 作业（单个 jar）
│   ├── deps.manifest.txt                        # 编译所需 Hadoop 3.3.6 jar 清单（125 个）
│   ├── build.ps1                                # 编译脚本 → build/iter1.jar
│   └── build-reproducible.py                    # 在固定 Linux worker 中的可复现构建
├── pipeline/
│   ├── run_pipeline.py                          # 编排：MR 作业串联 + 输入指纹 + 版本登记 + report.json
│   ├── execution.py                             # local 执行适配
│   └── yarn.py                                  # YARN 执行适配、作业标识与恢复
├── governance/                                  # 治理核心
│   ├── service.py                               # Run 提交、契约登记、状态迁移
│   ├── worker.py                                # 持久队列 worker、租约与执行令牌
│   ├── artifacts.py                             # Parquet / 证据 / 质量明细 / Manifest 导出
│   ├── indexing.py                              # Evidence Index 写入与校验
│   ├── validation.py                            # 内容与报告门禁
│   ├── manifest.py                              # 产物清单预检
│   ├── provenance.py                            # 来源协议与动作链校验
│   ├── publication.py                           # 唯一发布协议与中断恢复
│   ├── reconciliation.py                        # 只读存储对账
│   ├── history.py                               # Hive 外部表登记与历史/版本比较查询
│   ├── performance.py / task_metrics.py         # Job/Task 性能采集与基线
│   ├── equivalence.py                           # 优化前后业务语义等价校验
│   ├── maintenance.py                           # 保留策略候选预览
│   ├── backup.py                                # 备份包生成、校验与空库恢复
│   ├── build_identity.py                        # 执行材料指纹
│   ├── raw.py                                   # 原始输入清单与版本
│   └── cli.py                                   # init / import-data / submit / status / reconcile-storage / backup 等
├── metadata/                                    # MySQL 数据访问与迁移
│   ├── store.py                                 # Run/Attempt/Job/Publish/current/Evidence 访问
│   ├── connection.py
│   ├── 001_governance.sql
│   └── 003_legacy_archive.sql
├── storage/hdfs.py                              # HDFS 读写与原始输入导入
├── schemas/                                     # metrics-v2.0 / metrics-v2.1 指标契约
├── agent/
│   ├── loop.py                                  # LLM 循环、工具、会话记忆、防编造、trace
│   ├── server.py                                # FastAPI HTTP 壳（前后端契约、正式报告与历史报告入口）
│   ├── published.py                             # 按 Publish 读取并校验正式报告缓存
│   ├── results.py                               # 正式结果/证据查询、历史列表与版本比较
│   ├── conclusions.py                           # Agent 结论提案的报告/证据引用校验
│   ├── legacy_archive.py                        # 历史报告档案与显式别名
│   └── .env.example                             # DeepSeek key 模板
├── web/
│   ├── index.html                               # 清洗演示前端单页
│   └── governance.html / governance.js          # 治理控制台（状态、证据、历史、性能）
├── runtime/                                     # 容器环境与运维脚本（见 runtime/README.md、DEPLOYMENT.md）
│   ├── Dockerfile                               # local 运行镜像
│   ├── Dockerfile.governance / Dockerfile.worker / Dockerfile.api
│   ├── compose.governance.yaml / compose.history.yaml   # 治理与 Hive 历史服务
│   ├── hadoop-conf/                             # 单节点 HDFS/YARN 覆盖配置
│   └── prepare-*.ps1 / prepare-build-sdk.py / export-runtime-images.ps1 / start-hadoop.sh / switch-api.ps1
├── .github/workflows/business-contracts.yml     # 无模型业务模块 CI
├── tests/                                       # 快速回归（test_*.py）+ 集成验收（integration_*.py）
├── tests/verify_published_storage.py            # 正式文件全字节只读复核
├── tests/verify_runtime_archive.py              # 镜像归档校验
├── profiling/                                   # 数据探查脚本与报告
├── outputs/                                     # 运行产物（不入库）
├── .dockerignore / .gitignore                   # 构建与 Git 忽略规则
├── requirements.txt                             # Agent / HTTP 服务依赖
├── requirements-worker.txt（见 runtime/）        # worker 容器依赖
└── run_iteration1.ps1                           # 一键脚本：build / pipeline / serve
```

## 环境要求

| 组件 | 要求 |
| --- | --- |
| 操作系统 | Windows（脚本为 PowerShell） |
| Docker | Docker Desktop，能跑 Linux 容器 |
| JDK | JDK 8，仅用于编译 jar；安装目录通过环境变量 `ML_JAVA_HOME` 指定（未设置时自动搜索常见 Adoptium/Java 8 安装位置） |
| Python | 3.10+（实测 3.14.5），依赖见 `requirements.txt` |
| 数据 | MovieLens 1M 的 `users.dat` / `movies.dat` / `ratings.dat`（ISO-8859-1） |
| MySQL | 由 `runtime/compose.governance.yaml` 提供（固定镜像摘要），仅治理平台需要 |

## 快速开始（local 清洗 + Agent 演示）

以下命令都在**仓库根目录**（即 `ml-1m/`）执行。

### 1. 获取原始数据

仓库不包含原始数据（许可禁止再分发，见文末）。从 <https://grouplens.org/datasets/movielens/1m/>
下载 `ml-1m.zip`，把其中的三个文件解压到 `data/`：

```
data/users.dat  data/movies.dat  data/ratings.dat
```

### 2. 构建 Hadoop 运行镜像

镜像基于已有的 `wurstmeister/kafka:latest`（含 OpenJDK 11）+ Apache Hadoop 3.3.6 二进制包。
把 Hadoop 发行包下载到 `runtime/` 下（约 697MB，**不要入库**）：

```powershell
# 发行包:https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
curl.exe -L -o runtime\hadoop-3.3.6.tar.gz https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
docker build -t movielens-hadoop:3.3.6 -f runtime/Dockerfile runtime
```

### 3. 准备 Hadoop 依赖 jar

`javac` 需要 Hadoop 3.3.6 的 jar（约 44MB，不入库）。镜像建好后直接从镜像里复制：

```powershell
docker run --rm -v "$($PWD.Path)/hadoop:/work" --entrypoint sh movielens-hadoop:3.3.6 `
  -c 'mkdir -p /work/build/deps && cp $HADOOP_HOME/share/hadoop/common/*.jar $HADOOP_HOME/share/hadoop/common/lib/*.jar $HADOOP_HOME/share/hadoop/mapreduce/*.jar $HADOOP_HOME/share/hadoop/mapreduce/lib/*.jar /work/build/deps/'
```

结果落在 `hadoop/build/deps/`，多带几个同目录下的 jar 不影响 `javac`（`-cp` 允许冗余）。
`hadoop/deps.manifest.txt` 记录了原构建实际用到的 125 个 jar，可用来核对。

### 4. 编译 MapReduce 作业

```powershell
powershell -ExecutionPolicy Bypass -File run_iteration1.ps1 build
```

产物 `hadoop/build/iter1.jar`（等价于直接跑 `hadoop\build.ps1`）。

### 5. 运行管线

```powershell
powershell -ExecutionPolicy Bypass -File run_iteration1.ps1 pipeline
```

等价于 `python pipeline\run_pipeline.py --tag iteration1-run`（只跑原始评分用 `--only score-raw`）。
管线会在容器内跑完 12 次真实 MR 作业调用，产物写入 `outputs/<tag>/report.json`，并追加登记到
`outputs/registry.json`。

### 6. 启动 Agent 与前端

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
copy agent\.env.example agent\.env      # 填入 DEEPSEEK_API_KEY
powershell -ExecutionPolicy Bypass -File run_iteration1.ps1 serve
```

浏览器打开 <http://127.0.0.1:8000/>。未配置 key 时 `POST /api/tasks` 返回 503 并说明原因，
只读端点（报告 / 登记 / 前端）不受影响。

## 治理平台（HDFS/YARN + MySQL）

在清洗与评分之上，建立**任务可恢复、结果只正式发布一次、版本可复用、解释有证据、
历史可查询**的治理链路。核心机制：

- **Run / Attempt**：一次治理请求是稳定业务身份 `run_id`；每次实际执行是新的 `attempt_id`。重试保留 Run、新建 Attempt。
- **持久执行**：MySQL 持久队列 + 单 worker，租约与执行令牌保证过期执行者不能覆盖新状态；提交外部作业前先保存阶段与关联标识。
- **结构化产物与证据**：Parquet 导出、来源协议（源表/文件/偏移 + 动作链）、质量明细与摘要、Manifest、Evidence Index。
- **校验与唯一发布**：计算完成先进入 `VALIDATING`，核对来源、质量明细、证据与执行身份后建立 `PUBLISHING` 意图，全部正式 HDFS 文件通过校验才事务确认 `PUBLISHED`；`UNIQUE(run_id)` 保证同一 Run 最多一个正式发布，current 独立更新并按顺序保护较新版本。
- **正式结果入口**：`GET /api/runs/{run_id}/report`（含 `/report/download`）按 Publish 选择胜出 Attempt 并校验 Manifest 与报告字节；未知 Run 返回 404，未发布返回 409，缓存缺失/损坏返回 503，**不回退到其它 Run**。历史报告走 `/api/legacy/reports/{归档ID或显式别名}`，明确标注 `LEGACY_INCOMPLETE`。
- **结论引用校验**：Agent 提交 `ConclusionProposal`，业务服务按精确报告字段与证据事实校验；只有通过校验的事实进入正式答复，推测单列且 `hypotheses_verified=false`，材料不足时明确说明。
- **历史与版本比较**：MySQL 持久列表分页；固定两个正式任务比较评分、版本与行数变化（不同指标版本不算差值）；Hive 只读外部表按同一数据集的正式版本查询三表数量与评分分布。
- **性能与等价**：Job/Task 耗时与计数器基线；优化前后以磁盘 SQLite 比较 Parquet 与报告字段的业务行多重集合，保证规则结果与证据不变。
- **保留、备份与切换**：保留策略仅列候选；备份包含元数据与 Raw/Published/历史字节并支持空库事务恢复；维护排空与 API 镜像切换/回退见 [runtime/DEPLOYMENT.md](runtime/DEPLOYMENT.md)。

准备与运行（详细命令、端口、验证分层见 [runtime/README.md](runtime/README.md)）：

```powershell
.\runtime\prepare-governance.ps1   # 生成 runtime/.env，启动 MySQL 与单节点 HDFS/YARN
.\runtime\prepare-hadoop.ps1       # 构建 Hadoop 守护进程镜像
.\runtime\prepare-worker.ps1       # 构建原生 Hadoop worker 镜像
.\hadoop\build.ps1                 # 编译 MapReduce jar

# 初始化元数据并导入原始数据到 HDFS（在 worker 容器内执行）
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --entrypoint python worker -m governance.cli init
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --entrypoint python worker -m governance.cli import-data

# 提交任务并启动 worker
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --entrypoint python worker -m governance.cli submit --key demo-request-001
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker up -d worker
```

## 测试分层

`tests/test_*.py`（26 个）为不依赖外部服务的快速回归（含无模型业务模块，CI 由
`.github/workflows/business-contracts.yml` 执行 `runtime/check-business.py`）；
`tests/integration_*.py`（24 个）为真实 MySQL / HDFS / YARN / Hive 验收，
需按 [runtime/README.md](runtime/README.md) 的分层验证说明依次运行。

```powershell
.\runtime\prepare-test-db.ps1
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_*.py' -v
```

## 实测结果（tag=iteration1-final）

| 表 | 原始行数 | 清洗后 | 处置量 |
| --- | --- | --- | --- |
| users | 6,946 | 6,112 | 834 |
| movies | 4,465 | 3,858 | 607 |
| ratings | 1,150,241 | 987,270 | 162,971 |

五维综合分（ratings 0.6 / users 0.2 / movies 0.2 加权）：**93.33 → 99.98**。
分维明细、T1/T2 切分与处置计数见 [docs/运行说明.md](docs/运行说明.md) §5 与 `report.json`。

## 文档

完整导航见 [docs/README.md](docs/README.md)。核心文档：

| 文档 | 内容 |
| --- | --- |
| [docs/系统设计.md](docs/系统设计.md) | 分层、数据区与版本身份、元数据模型、运行生命周期、产物与证据、校验与发布、查询与解释 |
| [docs/清洗规则与五维评分设计.md](docs/清洗规则与五维评分设计.md) | 清洗规则（U/M/R 系列）与五维评分口径 |
| [docs/规则与指标契约.md](docs/规则与指标契约.md) | 规则与指标口径、来源与证据协议 |
| [docs/Hadoop与Agent实现.md](docs/Hadoop与Agent实现.md) | Hadoop 作业、Agent 循环、HTTP 与前端契约 |
| [docs/运行说明.md](docs/运行说明.md) | 实测运行说明：步骤、API、实测数字、局限与边界 |
| [docs/演示文档.md](docs/演示文档.md) / [.pdf](docs/演示文档.pdf) | 演示材料 |
| [runtime/README.md](runtime/README.md) | 容器环境、准备脚本与分层验证 |
| [runtime/DEPLOYMENT.md](runtime/DEPLOYMENT.md) | 部署、备份恢复与维护切换操作说明 |
| [docs/参考资料/](docs/参考资料) | 课程要求与选型参考材料 |

## 数据来源与许可

本仓库**不包含** MovieLens 数据集，仅包含代码与文档。数据来自 GroupLens Research：

> F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context.
> ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4, Article 19.
> DOI: <http://dx.doi.org/10.1145/2827872>

使用须知（数据集自带说明，原文见 `data/README-MovieLens.txt`）：不得暗示明尼苏达大学或
GroupLens 的背书；发表成果需引用上述文献；**未经许可不得再分发数据**；未经许可不得用于
商业用途。

## 已知边界

1. 执行模式是容器内的 Hadoop **local MapReduce** 或**单节点 HDFS/YARN**
   （治理平台，`dfs.replication=1`），不是多节点分布式集群，不证明跨物理机器容错或高可用。
2. 五维中的 "Accurate" 评的是值域合规与引用可解析，无法验证真实世界准确性；
   `users`/`movies` 无时间字段，Up-to-date 记为"不适用"，不参与综合分。
3. 置 NULL 与登记仍保留记录（登记 ≠ 修复）；隔离会缩小分母，分数上升部分来自剔除劣质记录。
4. `registry.json` 为追加写、不去重；`outputs/_tmp_*` 中间产物可安全删除。
5. Agent 的写工具只在用户原话命中"重新/重跑/清洗"类触发词时注入，其余场景只读复用最新报告。
6. 治理平台的全量规模、性能基线与复杂恢复的集中验收尚未完成；历史全量运行的产物与数据库
   已按要求清理，需要重建环境并重新导入数据后再验证。模块级用例通过不代表真实
   MySQL/HDFS/YARN/Hive、实际大模型调用与跨机器恢复已验收。详见
   [docs/系统设计.md](docs/系统设计.md) §15。
