# MovieLens 1M 数据治理 Agent（迭代一）

用**真实的 Hadoop MapReduce 管线**清洗 MovieLens 1M，用**LLM Agent** 把"清洗 + 五维质量评估"
串成一句自然语言就能触发的流程：Agent 决定调哪个工具、如实转述实测数字，前端三栏实时展示
任务状态、Agent 对话与评估报告。

- 管线：12 次 MapReduce 作业调用（9 种作业：提取参照 ID ×2、评分 ×6（原始/清洗后 × 三表）、清洗 ×4），
  跑在容器内的 Hadoop 3.3.6 **local MapReduce** 上。
- Agent：OpenAI Agents SDK + DeepSeek `deepseek-chat`，只读工具取数 + 触发词门控的写工具，
  回复中的分数经白名单校验（防编造），trace 落盘可回溯。
- 评分：Accurate / Complete / Unique / Up-to-date / Consistent 五维，登记版本（`outputs/registry.json`）。

## 目录结构

```
ml-1m/
├── data/                                        # 原始 .dat（不入库，见「获取原始数据」）+ 探查脚本
├── docs/                                        # 设计文档、运行说明、演示材料
├── hadoop/
│   ├── src/main/java/mliter1/IterationOne.java  # 全部 MR 作业（单个 jar）
│   ├── deps.manifest.txt                        # 编译所需 Hadoop 3.3.6 jar 清单（125 个）
│   └── build.ps1                                # 编译脚本 → build/iter1.jar
├── pipeline/run_pipeline.py                     # 编排：MR 作业串联 + 版本登记 + report.json
├── agent/
│   ├── loop.py                                  # LLM 循环、工具、会话记忆、防编造、trace、并发锁
│   ├── server.py                                # FastAPI HTTP 壳（前后端契约）
│   └── .env.example                             # DeepSeek key 模板
├── web/index.html                               # 前端单页（由 FastAPI 在 / 提供）
├── runtime/                                     # Hadoop 运行镜像的 Dockerfile
├── profiling/                                   # 数据探查脚本与报告（交叉验证用）
├── outputs/                                     # 运行产物（不入库）
├── requirements.txt
└── run_iteration1.ps1                           # 一键脚本：build / pipeline / serve
```

## 环境要求

| 组件 | 要求 |
| --- | --- |
| 操作系统 | Windows（脚本为 PowerShell） |
| Docker | Docker Desktop，能跑 Linux 容器 |
| JDK | JDK 8，仅用于编译；`hadoop/build.ps1` 默认路径 `C:\Program Files\Eclipse Adoptium\jdk-8.0.452.9-hotspot` |
| Python | 3.10+（实测 3.14.5），依赖见 `requirements.txt` |
| 数据 | MovieLens 1M 的 `users.dat` / `movies.dat` / `ratings.dat`（ISO-8859-1） |

## 快速开始

以下命令都在**仓库根目录**（即 `ml-1m/`）执行。

### 0. 获取原始数据

仓库不包含原始数据（许可禁止再分发，见文末）。从 <https://grouplens.org/datasets/movielens/1m/>
下载 `ml-1m.zip`，把其中的三个文件解压到 `data/`：

```
data/users.dat  data/movies.dat  data/ratings.dat
```

### 1. 构建 Hadoop 运行镜像

镜像基于已有的 `wurstmeister/kafka:latest`（含 OpenJDK 11）+ Apache Hadoop 3.3.6 二进制包。
把 Hadoop 发行包下载到 `runtime/` 下（约 697MB，**不要入库**）：

```powershell
# 发行包:https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
curl.exe -L -o runtime\hadoop-3.3.6.tar.gz https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
docker build -t movielens-hadoop:3.3.6 -f runtime/Dockerfile runtime
```

### 2. 准备 Hadoop 依赖 jar

`javac` 需要 Hadoop 3.3.6 的 jar（约 44MB，不入库）。镜像建好后直接从镜像里复制：

```powershell
docker run --rm -v "$($PWD.Path)/hadoop:/work" --entrypoint sh movielens-hadoop:3.3.6 `
  -c 'mkdir -p /work/build/deps && cp $HADOOP_HOME/share/hadoop/common/*.jar $HADOOP_HOME/share/hadoop/common/lib/*.jar $HADOOP_HOME/share/hadoop/mapreduce/*.jar $HADOOP_HOME/share/hadoop/mapreduce/lib/*.jar /work/build/deps/'
```

结果落在 `hadoop/build/deps/`，多带几个同目录下的 jar 不影响 `javac`（`-cp` 允许冗余）。
`hadoop/deps.manifest.txt` 记录了原构建实际用到的 125 个 jar，可用来核对；也可以直接从
已编译过的机器复制整个 `deps/` 目录。

### 3. 编译 MapReduce 作业

```powershell
powershell -ExecutionPolicy Bypass -File run_iteration1.ps1 build
```

产物 `hadoop/build/iter1.jar`（等价于直接跑 `hadoop\build.ps1`）。

### 4. 运行管线

```powershell
powershell -ExecutionPolicy Bypass -File run_iteration1.ps1 pipeline
```

等价于 `python pipeline\run_pipeline.py --tag iteration1-run`（只跑原始评分用 `--only score-raw`）。
管线会在容器内跑完 12 次真实 MR 作业调用，产物写入 `outputs/<tag>/report.json`，并追加登记到
`outputs/registry.json`。

### 5. 启动 Agent 与前端

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
copy agent\.env.example agent\.env      # 填入 DEEPSEEK_API_KEY
powershell -ExecutionPolicy Bypass -File run_iteration1.ps1 serve
```

浏览器打开 <http://127.0.0.1:8000/>。未配置 key 时 `POST /api/tasks` 返回 503 并说明原因，
只读端点（报告 / 登记 / 前端）不受影响。

## 实测结果（tag=iteration1-final）

| 表 | 原始行数 | 清洗后 | 处置量 |
| --- | --- | --- | --- |
| users | 6,946 | 6,112 | 834 |
| movies | 4,465 | 3,858 | 607 |
| ratings | 1,150,241 | 987,270 | 162,971 |

五维综合分（ratings 0.6 / users 0.2 / movies 0.2 加权）：**93.33 → 99.98**。
分维明细、T1/T2 切分与处置计数见 `docs/迭代一_运行说明.md` §5 与 `report.json`。

## 文档索引

| 文档 | 内容 |
| --- | --- |
| `docs/迭代一_正式设计文档.md` | 设计文档：分层、契约、规则与版本 |
| `docs/迭代一_运行说明.md` | 实测运行说明：步骤、API、实测数字、局限与边界 |
| `docs/清洗规则与五维评分设计.md` | 清洗规则（U/M/R 系列）与五维评分口径 |
| `docs/迭代一_Hadoop数据清洗与Agent基础.md` | Hadoop 与 Agent 基础机制 |
| `docs/数据探查报告.txt` | 原始数据探查结果 |
| `docs/迭代一_演示文档.md` / `.pdf` | 演示材料 |
| `runtime/README.md` | Hadoop 容器环境说明 |

## 数据来源与许可

本仓库**不包含** MovieLens 数据集，仅包含代码与文档。数据来自 GroupLens Research：

> F. Maxwell Harper and Joseph A. Konstan. 2015. The MovieLens Datasets: History and Context.
> ACM Transactions on Interactive Intelligent Systems (TiiS) 5, 4, Article 19.
> DOI: <http://dx.doi.org/10.1145/2827872>

使用须知（数据集自带说明，原文见 `data/README-MovieLens.txt`）：不得暗示明尼苏达大学或
GroupLens 的背书；发表成果需引用上述文献；**未经许可不得再分发数据**；未经许可不得用于
商业用途。

## 已知边界

1. 执行模式是容器内的 Hadoop **local MapReduce**，不是 HDFS/YARN 多节点集群，不能称作分布式集群。
2. 五维中的 "Accurate" 评的是值域合规与引用可解析，无法验证真实世界准确性；
   `users`/`movies` 无时间字段，Up-to-date 记为"不适用"，不参与综合分。
3. 置 NULL 与登记仍保留记录（登记 ≠ 修复）；隔离会缩小分母，分数上升部分来自剔除劣质记录。
4. `registry.json` 为追加写、不去重；`outputs/_tmp_*` 中间产物保留供人工抽验，可安全删除。
5. Agent 的写工具只在用户原话命中"重新/重跑/清洗"类触发词时注入，其余场景只读复用最新报告。
