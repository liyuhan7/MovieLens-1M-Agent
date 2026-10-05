# Hadoop 容器运行环境

此目录只用于准备环境；**不含数据清洗、评分或报告的实现**。

业务模块入口与验收边界见 [系统设计.md](../docs/系统设计.md) 第 14 节。`check-business.py` 执行不依赖大模型和计算任务的模块回归，保存当次结果及源码指纹；备份、离线验证与独立空数据库恢复通过 `python -m governance.cli backup|verify-backup|restore-backup` 调用。

- `Dockerfile` 使用本机已有 `wurstmeister/kafka:latest`（OpenJDK 11）及 Apache Hadoop 3.3.6 官方二进制包构建镜像 `movielens-hadoop:3.3.6`。
- `hadoop-3.3.6.tar.gz` 约 697 MiB，按需从 Apache 镜像下载；不建议纳入代码仓库或提交成果。
- 默认 `file:///` 文件系统与 `local` MapReduce 框架，在容器中执行真实 Hadoop MapReduce 作业，而不是 Docker 多节点 HDFS/YARN 集群。该执行模式必须如实声明，不能称为分布式集群。

检查环境：

```powershell
# 在项目根目录（ml-1m 所在目录）运行；使用工作区完整路径即可支持中文目录
$root = (Get-Location).Path
docker build -t movielens-hadoop:3.3.6 -f ml-1m/runtime/Dockerfile ml-1m/runtime
docker run --rm -v "${root}/ml-1m:/project:ro" movielens-hadoop:3.3.6 'hadoop version && hadoop jar "$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar"'
```

**不要**在构建完成前将数据检查结果称作 Hadoop 作业结果：环境验证只验证 Hadoop 本身可启动。

## 治理开发环境（MySQL + 单节点 HDFS/YARN）

新增 `compose.governance.yaml` 与准备脚本用于实际 MySQL、单节点 HDFS/YARN 和持久 worker。原 `Dockerfile` 保留作 local 回归入口。开发环境只使用 `ml-governance-*` 容器和 `ml-governance_*` 数据卷，不复用其他项目的数据库。

从仓库根目录执行：

```powershell
.\runtime\prepare-governance.ps1
.\runtime\prepare-hadoop.ps1
.\runtime\prepare-worker.ps1
.\hadoop\build.ps1
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --entrypoint python worker -m governance.cli init
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --entrypoint python worker -m governance.cli import-data
```

MySQL 镜像使用固定摘要；Hadoop 和 worker 准备脚本把构建后的镜像 ID 固定在本地 `runtime/.env`。此文件被 Git 忽略，包含随机生成的项目数据库凭据；脚本不会输出凭据。原 Hadoop 基础镜像依赖本机已有构建，跨机器还需按原运行说明准备基础镜像；这部分尚不能称为完全可重建的发行环境。

`prepare-hadoop.ps1` 补齐原镜像缺失的 `ps` 工具，使用 HTTPS Debian 软件源。基础包仓库仍可能变化；正式环境交付前还需固定包清单和构建输入。NameNode 仅在空目录且没有 VERSION 时格式化；非空存储缺少 VERSION 时拒绝启动并要求核查，不能自动重建。

实际拓扑是一个 Hadoop 容器内的 NameNode、DataNode、ResourceManager、NodeManager 和 JobHistoryServer，另设 MySQL 和 Linux worker 容器。`dfs.replication=1`，不证明跨物理机器容错或高可用。配置采用 [Hadoop 3.3.6 单节点文档](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/SingleCluster.html) 的 HDFS/YARN 模式；worker 通过原生 Hadoop 客户端访问服务，不依赖宿主 Docker API。

本机入口：MySQL `127.0.0.1:13306`，NameNode 页面 `http://127.0.0.1:19870`，ResourceManager 页面 `http://127.0.0.1:18088`，JobHistory 页面 `http://127.0.0.1:19888`。全部映射端口仅监听本机；Hadoop 容器上限 6 GiB，NodeManager 分配 4 GiB，worker 上限 2 GiB。实际性能和容量应通过运行验证，不由此配置推断。

提交与查看任务：

```powershell
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --entrypoint python worker -m governance.cli submit --key demo-request-001
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker up -d worker
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --entrypoint python worker -m governance.cli status --run <返回的run_id>
```

相同键和相同参数复用同一任务；不同参数使用同一键会被拒绝。明确重跑使用新键，确认失败后的重试通过 `/api/runs/{run_id}/retry` 创建新 Attempt。worker 恢复先核对已经提交的 YARN Application；提交状态不确定时保持待核对，不盲目重交。

**当前实现边界：** worker 已接入 Parquet 导出、证据索引、内容门禁和唯一发布协议。计算完成先进入 `VALIDATING`，核对来源、质量明细、证据和执行身份后建立 `PUBLISHING` 意图，全部正式 HDFS 文件通过校验后才事务确认 `PUBLISHED`。current 更新独立执行，并按请求顺序保护较新版本。完整 MovieLens 管线、多文件与进程退出/重启仍需验收。

提交请求同时固定 jar、执行代码、运行配置和镜像身份。任务执行期间不要修改这些材料；旧候选缺少完整执行身份时拒绝直接发布，应保留历史并创建符合新契约的新任务。

## 分层验证

本机快速回归与真实 MySQL 验证：

```powershell
.\runtime\prepare-test-db.ps1
.\.venv\Scripts\python.exe -m unittest discover -s tests -p 'test_*.py' -v
.\.venv\Scripts\python.exe -m unittest discover -s tests -p integration_mysql.py -v
```

真实 HDFS/YARN 的小样本完整管线：

```powershell
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python -e ML_MYSQL_DATABASE=ml_governance_test worker -m unittest discover -s tests -p integration_yarn.py -v
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python -e ML_MYSQL_DATABASE=ml_governance_test worker -m unittest discover -s tests -p integration_yarn_recovery.py -v

docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python -e ML_MYSQL_DATABASE=ml_governance_test worker -m unittest discover -s tests -p integration_artifacts.py -v

docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python -e ML_MYSQL_DATABASE=ml_governance_test worker -m unittest discover -s tests -p integration_indexing.py -v

docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python -e ML_MYSQL_DATABASE=ml_governance_test worker -m unittest discover -s tests -p integration_validation.py -v

docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python -e ML_MYSQL_DATABASE=ml_governance_test worker -m unittest discover -s tests -p integration_publication.py -v

docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python -e ML_MYSQL_DATABASE=ml_governance_test worker -m unittest discover -s tests -p integration_current.py -v

docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python -e ML_MYSQL_DATABASE=ml_governance_test worker -m unittest discover -s tests -p integration_reconciliation.py -v

docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python -e ML_MYSQL_DATABASE=ml_governance_test worker -m unittest discover -s tests -p integration_multifile.py -v
```

MySQL 与 YARN 集成测试应依次运行，共用专用 `ml_governance_test` 库及其单 worker 槽位。测试不删库、不清空历史；计算和索引测试标记为合成测试结束，发布测试通过时保留真实的测试库 `PUBLISHED` 记录和独立正式文件路径。它们不代表生产库原始 MovieLens 已发布。合成数据、作业日志和验收结果保留在被忽略的 `outputs/`。首次建库失败后，可重复执行增量迁移；迁移不包含 DROP/TRUNCATE。

`integration_current.py` 使用 `current-fixture-*` 数据集和 `fixture_only` 元数据，只验证并发指针策略；它不证明那些样本执行过计算或形成了 HDFS 正式版本。

## 只读存储对账

```powershell
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python worker -m governance.cli reconcile-storage --include-test-metadata
```

结果保存在 `outputs/storage-reconciliation.json`。共享 HDFS 应同时纳入项目库和测试库，避免把另一个已知元数据范围的文件误认为未登记对象。对账区分正式版本、待发布文件、Raw、活跃/待恢复 Attempt、历史暂存及未登记待核对对象；检查路径、清单存在性和字节数，不把这些检查称作完整 SHA-256 验证。命令不删除文件，也不据未登记标签自动授权清理。

原始输入可以是规范命名的 `users.dat` / `movies.dat` / `ratings.dat`，或三张表各自的目录。目录中的可见文件按相对路径排序，递归保留路径；以 `.` / `_` 开头的文件和目录不参与版本和计算，符号链接拒绝登记。文件内容或相对名称变化会产生新版本，迁移整个目录不会因根目录/日期变化产生新版本；原单文件版本算法保留。

## 补充验收

独立原始评分使用新的输入和 Attempt，在隔离测试库实际执行五个 YARN 作业，不依赖旧参照文件：

```powershell
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python -e ML_MYSQL_DATABASE=ml_governance_test worker -m unittest discover -s tests -p integration_raw_scoring.py -v
```

`tests/integration_http_worker.py` 使用宿主 HTTP 依赖与隔离测试库，按 `ML_HTTP_PHASE=prepare/submit/verify` 分步运行。prepare 输出新的验收目录；后续必须固定 `ML_HTTP_FIXTURE` 为该目录。准备后通过直接容器调用读取该目录 `inputs.json`，用 `storage.hdfs.import_input` 导入各 version/paths；再执行 submit、独立测试库 worker `--once`、verify。宿主 Python 子进程访问 Docker 管道在本机受限，因此容器操作由直接调用执行。

只有 verify 通过并产生 `acceptance.json` 才表示此 HTTP→独立 worker→正式发布用例通过。prepare 和 submit 的单独成功不能代表完整验收。HTTP 使用 TestClient 和真实 MySQL，只替换测试输入选择；重新创建客户端生命周期不等同于实际 API 进程故障注入。尚未验收的范围见 [系统设计.md](../docs/系统设计.md) §15。

## 正式报告读取

`GET /api/runs/{run_id}/report` 与 `/report/download` 根据 MySQL PUBLISHED 记录选择胜出 Attempt，校验发布 Manifest 自身、报告字节 SHA-256、大小和完整版本绑定，再返回原始报告字节及 `X-Publish-ID`。未知 Run 返回 404；未正式发布返回 409；损坏缓存或正式存储不可用返回 503。HDFS 是正式数据权威，缺失缓存已支持按该 Publish 的正式路径取回、校验完整字节并原子创建；不选择其他 Run 或覆盖损坏缓存。旧身份查询按显式历史档案映射解析，不回退最新结果。

正式文件的独立全字节复核使用 `tests/verify_published_storage.py --run <run_id> --output outputs/<audit>.json`，必须在有原生 Hadoop 客户端的 worker 环境执行；默认数据库来自运行配置，测试验收明确指定隔离测试库。它核对完整文件集合、每个文件的大小与 SHA-256 以及 SQL Manifest 指纹，不修改 HDFS。

执行材料（提交时使用的代码、配置与镜像身份）由 `governance/build_identity.py` 的 `capture_execution` 固定，归档到 `outputs/execution-materials/<sha256>/`。镜像身份登记不等于镜像字节已导出，跨机器环境重建仍需单独验收。

旧 Run 晚发布验收使用 `tests/integration_late_current.py`：指定 `ML_LATE_OLD_RUN`、`ML_LATE_NEW_RUN`，submit 通过正常 HTTP 重试旧 Run 并输出 `ML_LATE_FIXTURE` 路径；独立测试库 worker 完成后，用正式存储复核工具检查两个 Run 并写入该目录 `hdfs-acceptance.json`，最后以同一目录运行 verify。旧 Run 必须是本测试产生的未发布、确认结束的独立原始评分 Run；不得用于改写用户正式任务。verify 检查真实 Publish、current、旧文件指纹、失效执行者返回及完整 HDFS 字节。

## 指标边界与坏产物门禁验收

```powershell
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python -e ML_MYSQL_DATABASE=ml_governance_test worker -m unittest discover -s tests -p integration_metric_boundaries.py -v
```

`integration_report_integrity.py` 检查重新计算文件校验值后的报告派生字段篡改（综合分、增量、切分数量/边界、移除总数等）。内容门禁拒绝此类报告，即使文件 SHA-256 重新计算正确；相应负例纳入小样本回归，见 [系统设计.md](../docs/系统设计.md) §8.1。

## 固定镜像恢复材料

执行文件归档之外，可保存实际使用的 Hadoop、worker 和固定 MySQL 镜像字节：

```powershell
runtime/export-runtime-images.ps1 -ExecutionManifest outputs/execution-materials/<execution_sha256>/manifest.json
python tests/verify_runtime_archive.py outputs/runtime-images/<execution_sha256>/manifest.json
```

导出只读取现有镜像，不改容器或数据卷；不读取运行密钥。相同目标重入只复核，冲突不覆盖。验证器核对整个归档、固定 OCI 索引、实际 linux/amd64 子清单和全部配置/文件层；其他平台未验证。恢复时先核对 `verification.json` 和完整归档校验值，再在目标机器执行 `docker image load -i <归档路径>/images.tar`，按记录中的固定镜像身份准备配置。数据卷和数据库须单独备份/恢复，这份镜像归档不能替代业务数据备份。当前已完成归档与字节核验，尚未完成跨机器执行验收。

## 内容校验与 JobHistory 恢复

内容校验与 JobHistory 恢复在正式模块中实现（`governance/validation.py`、`pipeline/yarn.py`），不依赖候选副本。历史恢复在真实历史服务和存储上恢复已完成作业，模拟 RM 记录不可用，禁止新计算提交；历史服务列表的截短名称只作候选筛选，完整 Job 名称必须精确匹配，接口依据 [Hadoop 3.3.6 JobHistory REST 文档](https://hadoop.apache.org/docs/r3.3.6/hadoop-mapreduce-client/hadoop-mapreduce-client-hs/HistoryServerRest.html)。内容测试绑定保留的合成 Run，可用 `ML_CONTENT_RUN` 显式指定相同规模且含去重记录的测试产物；不根据故障重放会改变的 error 标签猜测。

```powershell
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps -e ML_MYSQL_DATABASE=ml_governance_test --entrypoint python worker -m unittest discover -s tests -p integration_jobhistory_recovery.py -v
```

## 历史报告登记与显式身份

历史档案表由 `metadata/003_legacy_archive.sql` 和 `agent/legacy_archive.py` 提供：核对 `docs/历史报告基线清单.json` 的完整报告字节后登记路径/校验值及源 tag、登记 task_id、报告内部 task_id 的显式映射。导入键由源路径和完整 SHA-256 派生，重复执行不增加记录；旧 success 不创建 Run/Attempt/Publish 或正式 Evidence。登记与读取的集成验收见 `tests/integration_legacy_archive.py` 与 `tests/integration_legacy_http.py`。

`GET /api/legacy/reports/{归档ID或显式别名}` 返回历史状态和原报告，明确 `LEGACY_INCOMPLETE` / `formal_publication_verified=false`。同名冲突返回 409，未知身份/空身份不回退 latest，材料缺失/变化或数据库不可用返回 503。旧 `/api/tasks/{id}/report` 兼容没有同名目录的已登记历史 ID；正式结果仍走 `/api/runs/{run_id}/report`。历史接口只提供显式身份查询，不替代正式结果页面与 Agent 接入。

## 发布转移入口

发布转移的元数据与存储约束在正式模块中实现（`metadata/store.py`、`governance/reconciliation.py`）。操作者入口 `runtime/request-publication-replacement.py` 是显式 CAS：要求原 Publish、Attempt、Manifest 校验值和令牌的完整预期及原因，仅在未正式发布、无有效租约且外部作业均确认终态时排队替代 Attempt；不能用于改写已 PUBLISHED 的记录。真实 MySQL 与存储对账约束见 `tests/integration_publication_transfer.py` 与 `tests/integration_publication_transfer_metadata.py`。

## 业务模块回归与运维入口

不依赖大模型与计算任务的模块回归（CI 同款入口，见 `.github/workflows/business-contracts.yml`）：

```powershell
.\.venv\Scripts\python.exe runtime/check-business.py
node tests/test_governance_ui.cjs          # 治理控制台渲染用例（需 Node）
```

结果连同源码指纹写入 `outputs/business-validation/<时间戳>/verification.json`，CI 以工件上传。

其它运维子命令（`governance.cli`）：`retention-preview`；`hive-projection` / `hive-register`；
`history-analysis` / `history-series`；`performance-capture` / `performance-compare`；
`compare-semantics` / `compare-optimization`；`maintenance-status` / `maintenance-begin` /
`maintenance-end` / `maintenance-switch-begin` / `maintenance-switch-end`；
`backup` / `verify-backup` / `restore-backup`。备份要求 Run 与 Attempt 均已终态、执行槽位空闲、
无未完成发布意图。完整参数、Hive 权限、排空与 API 切换/回退步骤见
[业务服务部署、切换和回退](DEPLOYMENT.md)。

## 独立可重复构建工具（未替换部署 JAR）

当前 Worker 只有 Java 运行环境，缺少 javac。构建工具使用单独的 [Temurin 11.0.28+6 官方发行包](https://github.com/adoptium/temurin11-binaries/releases/tag/jdk-11.0.28%2B6)，Linux x64 JDK 归档 SHA-256 固定为 `7dfd551795a8884b26cbb02e0301da95db40160bb194f48271dc2ef9367f50c2`，保存于 `outputs/build-sdk/`。`runtime/prepare-build-sdk.py` 核对归档后展开，并登记工具链每个文件的完整校验值。已展开时不覆盖原目录。构建只读取现有固定 Worker 的 Hadoop 依赖，不使用 Windows JDK 或 `hadoop/build/deps`。

```powershell
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker run --rm --no-deps --entrypoint python worker -m unittest discover -s tests -p integration_reproducible_build.py -v
```

`hadoop/build-reproducible.py --output outputs/<新路径>.jar` 要求全新路径，复核完整工具链，目标 Java 8 字节码，并记录源文件、编译器、工具链、332 个依赖 JAR、class 和最终 JAR 的校验值。归档条目顺序、时间戳和权限固定。新 JAR 自带 Main-Class，因此运行形式为 `hadoop jar <新JAR> extractUsers <输入> <输出>`，不重复传入主类名；尚未替换当前 pipeline 使用的部署 JAR 或验证其部署兼容。两次本机构建一致不代表已在物理第二台机器上验证，也不代表业务数据卷已备份。

## 已知边界

API/Hive 客户端部署配置、维护排空、切换及回退命令见 [业务服务部署、切换和回退](DEPLOYMENT.md)。这些入口已实现并完成模块及配置解析检查，本轮未执行真实部署或切换。

- 执行模式为容器内单节点 HDFS/YARN（`dfs.replication=1`）或 local MapReduce，不是多节点分布式集群，也不证明跨物理机器容错或高可用。
- 开发环境依赖本机已有的 `wurstmeister/kafka:latest` 基础镜像；MySQL/Python 基础镜像按固定摘要拉取。跨机器完整重建与可复现发行尚未验收。
- 全量 MovieLens 规模、性能基线与复杂恢复的集中验收尚未完成。历史全量运行的产物与数据库已按要求清理，需要重建环境并重新导入数据后再验证。
- 发布转移、内容校验与 JobHistory 恢复在正式模块中实现；一次性操作脚本在完成后已移除。
