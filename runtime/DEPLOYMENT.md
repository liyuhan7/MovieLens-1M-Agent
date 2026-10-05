# 业务服务部署、切换和回退

本文件交付操作入口，不记录已执行部署。当前工作将前端操作和真实环境业务闭环独立验收；模块回归与配置解析不能证明镜像构建、Hive/JVM 兼容、权限、部署或切换成功。

## 一、准备与首次启动

前置条件：已准备 MySQL、固定 Hadoop/Worker 镜像、当前应用 JAR，依赖安装与镜像文件按现有运行说明核验。原始数据放入 `data/`，不包含在 API 镜像中。目录 `outputs/` 保存读取缓存及诊断材料。

```powershell
# 初始化增量业务 Schema、维护开关、历史档案 Schema 和规则/指标定义
.venv\Scripts\python.exe -m governance.cli init
# 从 runtime/.env 中固定的 Worker image ID 构建 API；不会启动容器
.\runtime\prepare-api.ps1
# 首次启动，不会自动发起清洗任务
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile api up -d --wait api
```

API 镜像包含固定业务源码、配置和 JAR；`data/` 只读挂载，`outputs/` 为缓存/诊断挂载，凭据不进入构建上下文。API 使用单个 Uvicorn 进程，现有 Agent 叙事状态仍是进程内状态；正式 Run/Attempt/Publish 以 MySQL 为准。原始数据路径与执行材料在提交时固定，Worker 的材料不同会拒绝执行，不应通过修改请求或替换文件绕过检查。

Agent 会话及固定报告关系保存在 `outputs/agent_memory.db`；`agent_report_binding` 将只读会话固定到首次已校验的正式 Run，重启后的追问仍使用该 Run。该 SQLite 文件应随会话材料保留和备份，不能仅把它当成可任意删除的报告缓存。正式业务备份包覆盖 MySQL 与 Raw/Published/历史档案材料，不自动覆盖本次新增的在线会话库；在线会话存储的备份、丢失和真实重启恢复单独验收。

`ML_API_IMAGE` 保存精确 image ID；API 读取的 `ML_HADOOP_IMAGE`、`ML_WORKER_IMAGE` 与 Worker 使用的固定身份一致。设置 `DEEPSEEK_API_KEY`、`DEEPSEEK_MODEL` 时只在运行环境传入；非模型业务接口不要求模型可用。端口默认只监听 `127.0.0.1:8787`。

`GET /api/health` 仅表示 HTTP 服务进程可响应；`GET /api/readiness` 验证 MySQL Schema 5 和维护控制记录，返回是否开放接入及修订号。维护窗口关闭接入时仍可就绪、读取正式结果。它不证明 Hive/HDFS/YARN 或模型调用可用。

## 二、Hive 客户端与权限

`compose.history.yaml` 是可选覆盖配置，连接操作者指定的已有 HiveServer2；不创建另一份 Raw/Published 数据存储或将 Hive 改成业务状态权威。操作者提供 `ML_HIVE_CLIENT_HOME`，其中包含与服务端、当前 JVM/Hadoop 兼容并已核验来源的完整 Beeline 客户端。客户端只读挂载；JDBC 必须使用容器内可达地址，不能将容器 localhost 当作宿主服务。

查询配置：`ML_HIVE_JDBC_URL`、`ML_HIVE_USER`；登记配置：`ML_HIVE_ADMIN_JDBC_URL`、`ML_HIVE_ADMIN_USER`。API 只收到查询配置，管理配置仅传给手工 `history-admin` 命令。权限由 HiveServer2 授权控制，两个变量名不能代替只读账号与权限验收。凭据和认证方式按目标 Hive 环境配置，当前客户端入口使用 Beeline `-u/-n`，没有自动配置 Kerberos/密码文件。

```powershell
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml -f runtime/compose.history.yaml --profile api up -d --wait api
# 仅在正式版本存在后，以管理配置登记；不会发起计算
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml -f runtime/compose.history.yaml --profile history-admin run --rm --no-deps history-admin hive-register --run <正式RunID>
```

当前只交付外部 Hive 接入配置；真实 Hive 部署、JVM/Parquet 兼容、账号只读权限和删表保护单独验收。历史读取不执行 DDL；已有错误分区或表拒绝使用，不自动改写或删除正式文件。

## 三、暂停与排空

先读取 `maintenance-status` 的修订号，再以明确维护者、原因关闭新接入。维护者是审计身份，不是认证凭据；本机管理员命令及数据库权限应由部署环境控制。

```powershell
.venv\Scripts\python.exe -m governance.cli maintenance-status
.venv\Scripts\python.exe -m governance.cli maintenance-begin --owner release-operator --reason "upgrade API" --expected-revision <当前修订号>
.venv\Scripts\python.exe -m governance.cli maintenance-status
```

关闭后：新 Run、失败重试和排队任务的首次领取被阻止；相同已登记请求可复用原 Run。已有在途 Attempt 仍可租约接管、恢复、校验和发布，应保持原 Worker 运行至终态。排队任务保留，不自动删除或改写。`ready_for_switch=true` 要求没有在途 Run/Attempt、未完成 Publish、占用执行槽位或其他切换预留。

有未完成发布、失效槽位或恢复异常时，先使用既有恢复/对账流程处理；切换入口不会强行结束任务、清空槽位或删除文件。备份检查点仍要求所有 Run 终态；保留的 QUEUED 任务不符合该备份门槛，不能用排空就绪替代备份就绪。

## 四、API 切换及回退

目标镜像必须是精确 ID，支持治理 Schema 5 与持久接入控制；脚本拒绝不带相应标签的旧执行入口。维护期间不变更数据库 Schema 为旧版，不回退 MySQL/HDFS 正式记录，也不开放旧 JSON 登记或直接执行管线的写入口。

```powershell
# 要求既有 API 部署；ExpectedRevision 使用排空后最新值
.\runtime\switch-api.ps1 -ImageId <目标image-ID> -Owner release-operator -ExpectedRevision <修订号> -Reason "upgrade API"
# 使用 Hive 覆盖配置的部署，应加 -WithHistory，保留客户端挂载与查询配置
```

脚本先核对维护者、修订号和排空状态，再在 MySQL 预留 switch ID；预留期间拒绝开放接入。随后停止已排空 Worker、保存原/目标镜像身份，更新 API 镜像并等待健康状态，核对实际运行 image ID。成功后释放预留，仍保持接入关闭和 Worker 停止；操作者根据记录中的 `final_revision` 检查就绪并显式恢复。

记录保存在 `outputs/service-switches/<switch-ID>/switch.json`，不保存 `.env` 内容或凭据。失败或中断保留维护状态和预留，不能盲目开放接入。运行 `maintenance-status` 检查 `switch_id` 与修订号，对照记录和实际容器；确认该次操作停止后，可显式释放预留：

```powershell
.venv\Scripts\python.exe -m governance.cli maintenance-switch-end --owner release-operator --switch-id <记录中的switch-ID> --expected-revision <当前修订号>
```

回退使用同一 `switch-api.ps1`，目标为记录的 `previous_image`，仍需匹配当前维护修订号、排空条件及治理兼容标签。不存在原镜像、权限/挂载变化或新旧 Schema/代码不兼容时不能宣称可回退；提前保存兼容镜像和环境材料。回退后已发布版本和当前指针保持原状，正式历史不因 API 回退被覆盖。

```powershell
.venv\Scripts\python.exe -m governance.cli maintenance-end --owner release-operator --reason "readiness reviewed" --expected-revision <最终修订号>
docker compose --env-file runtime/.env -f runtime/compose.governance.yaml --profile compute --profile worker up -d worker
```

恢复前核对保留 QUEUED 请求的固定执行材料是否与当前 Worker 一致。材料不一致的请求不能通过重写已登记请求继续执行，应按业务意图创建新 Run；失败重试仍沿用同一固定请求。不要在已有执行期间重建或修改绑定的代码、配置、镜像或 JAR。

## 五、验证边界

单元/模块覆盖接入关闭、幂等复用、恢复领取、修订号/维护者、切换预留、在途/发布阻止、HTTP 503 和读取就绪，以及备份恢复保留维护状态。PowerShell 解析检查两个脚本；Docker Compose 只进行配置解析，历史覆盖配置采用占位客户端/镜像参数。未构建 API 镜像、启动新容器、运行真实 MySQL 门槛或执行切换/回退，因此这些入口的真实环境验收仍未完成。
