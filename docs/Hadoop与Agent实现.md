# Hadoop 与 Agent 实现

本文档描述计算与 Agent 的实现细节。系统设计见 [系统设计.md](系统设计.md)，
清洗规则与评分公式见 [清洗规则与五维评分设计.md](清洗规则与五维评分设计.md)，
实测步骤与数字见 [运行说明.md](运行说明.md)。

## 1. Hadoop 作业架构

运行环境为 Docker 容器 `movielens-hadoop:3.3.6`（OpenJDK 11 + Hadoop 3.3.6），
`mapreduce.framework.name=local`，输入输出经卷挂载。治理形态下同一套作业由单节点
HDFS/YARN 调度。**如实口径：真实 MapReduce 管线（map/shuffle/reduce），
local 模式或单节点集群，不是多节点分布式集群。**

### 1.1 作业清单（单 jar `iter1.jar`，按作业名分发）

| 作业 | 输入 | 输出 | 实现规则 |
| --- | --- | --- | --- |
| extractUsers / extractMovies | 原始或清洗后 users/movies | `ids.txt` | 参照 ID 集合 |
| scoreRatings | ratings + cache（参照 ID） | 指标计数（N/ACC/COMP/UD/CONS/TRAIN/VALID/TEST/UNIQ_EXCESS） | ratings 五维公式 |
| scoreUsers | users | 同上（无 UD） | users 五维公式 |
| scoreMovies | movies | 同上（无 UD） | movies 五维公式 |
| cleanUsers | users | clean / isolate / log 流 | U1（含 U1c）–U6 |
| cleanMovies | movies | 同上 | M7、M8、M1–M4、M5a |
| cleanMoviesTitle | cleanMovies 的 clean 行 | 同上 | M5b（标题级二次去重） |
| cleanRatings | ratings + cache（清洗后 users/movies ID） | 同上 + TRAIN/VALID/TEST 计数 | R1–R9 |

单次完整管线对上述作业共发起 12 次调用（9 种作业：提取参照 ID ×2、评分 ×6、清洗 ×4）。

### 1.2 关键实现决策

- **输入编码**：Mapper 内以 ISO-8859-1 解码原始行，输出统一 UTF-8（编码转换在管线内完成，
  而非预处理）。
- **行序稳定性**：以文件字节偏移为 seq，去重规则的“并列取文件序靠前”有确定性依据。
- **行级判定在 Mapper、去重在 Reducer**：R7 按 (uid, mid)、U6 按 UserID、M5a 按 MovieID、
  M5b 按 (titleNorm, year) 分组；无法可靠还原的记录直通隔离区。
- **处置输出**：每行输出 `seq|action|rule|payload` 统一构型，编排层聚合为各规则计数与样例。
- **参照广播**：清洗后 users/movies ID 列表经 `-files` + DistributedCache 广播给
  cleanRatings / scoreRatings（判定与对照引用）。
- **同一 jar 评两侧**：score 类作业对原始与清洗后数据的差别仅在输入路径与 cache 参照文件。
- **消息透传**：Java 中间记录携带原始文件、字节偏移、动作链与去重目标，
  电影第二阶段继承原始身份（见 [系统设计.md](系统设计.md) §7.2）。

### 1.3 输出目录契约（local 形态）

```text
outputs/
├── <tag>/report.json        # 每次任务的评估报告（报告是唯一对外数据面）
├── registry.json            # 版本登记（追加式，兼容读取）
├── agent_memory.db          # Agent 会话（SQLiteSession）
├── traces/*.json            # Agent 推理链（本地 trace）
└── _tmp_*                   # 中间产物（clean/score 作业输出，可人工抽验，可删）
```

治理形态的输出目录见 [系统设计.md](系统设计.md) §3。

### 1.4 可重复构建

`hadoop/build-reproducible.py` 在固定的 Linux worker 中用独立工具链重新构建 JAR，
要求全新输出路径，目标 Java 8 字节码，并记录源文件、编译器、工具链、332 个依赖 JAR、
class 与最终 JAR 的校验值；归档条目顺序、时间戳与权限固定。新 JAR 自带 Main-Class，
运行形式为 `hadoop jar <新JAR> extractUsers <输入> <输出>`，不重复传入主类名。
构建工具链准备见 [runtime/README.md](../runtime/README.md)。

## 2. 版本登记与任务标识

local 形态下 `registry.json` 每次任务追加一条：

```json
{
  "task_id": "task-<uuid4>",
  "created_at": "<ISO8601>",
  "tag": "<任务标签>",
  "input_data_version": "raw-<yyyymmdd>-<sha256前8位>",
  "output_data_version": "clean-v1.0-<task_id截取>",
  "rule_version": "rules-v2.0",
  "t1": "2002-01-01T00:00:00Z",
  "t2": "2002-07-01T00:00:00Z",
  "split_counts": {"train": 960233, "valid": 14391, "test": 12646},
  "status": "success",
  "report": "<tag>/report.json"
}
```

引用产物必须同时匹配 `output_data_version` + `rule_version` + T1/T2，版本不一致时拒绝执行。
治理形态下的业务标识统一为 Run/Attempt，`registry.json` 仅作兼容导出，不再是权威状态源
（见 [系统设计.md](系统设计.md) §5、§13）。

## 3. Agent 架构

### 3.1 设计定位

Agent = LLM 循环（OpenAI Agents SDK + DeepSeek）+ 工具调用 + 证据约束下的解释生成。
**全部数字必须来自工具返回的报告实测字段；禁止编造；查询对象尚未生成时如实说明，不得占位。**

```text
用户自然语言
  → LLM（deepseek）循环，自主决定调用工具
    ├─ run_clean_data_pipeline(rule_pack) → 持久队列 → hadoop jar（真实 MR）
    └─ read_latest_report / read_report_field / read_registry / read_samples（纯读，零 Hadoop）
  → 组织回答：任务状态、五维对比、数据量变化、处置区分（修复/去重/隔离）、
     清洗后数据版本、规则版本、T1/T2、评估报告路径与局限
```

### 3.2 模型与鲁棒性

- 模型：`deepseek-chat`，经 SDK 内置路径接入
  （`AsyncOpenAI(base_url="https://api.deepseek.com")` + `OpenAIChatCompletionsModel`），
  可经环境变量切换 DeepSeek 其他型号。
- 每次 Agent 构建新建模型客户端（httpx 连接池绑定 asyncio loop，不跨线程复用全局客户端）。
- 模型调用超时 120s/次；429/5xx 经指数退避自动重试 2 次；**工具执行失败不重试**，错误如实上报。
- 未配置 key 时服务启动正常，`POST /api/tasks` 返回 503 并明示“模型未配置”，不降级到模板问答。

### 3.3 工具表

| 工具 | 输入 | 输出 | 副作用 |
| --- | --- | --- | --- |
| `run_clean_data_pipeline` | rule_pack（默认 `default`） | 立即返回 task_id/排队提示；最终结果经第二阶段总结落状态 | 提交全管线（约 3 分钟） |
| `read_latest_report` | 无 | 最新成功报告全文 | 无 |
| `read_report_field` | tag, section | 报告单段：scores/dataset_composite/row_change/disposition/split/limitations/metrics_* | 无 |
| `read_registry` | 无 | 版本登记（输入/输出数据版本、规则版本、T1/T2、切分计数） | 无 |
| `read_samples` | tag, kind（clean/isolate/log） | 三表样例 | 无 |

### 3.4 结构强制与防编造

- **重跑门槛（结构强制）**：常驻工具仅只读组；`run_clean_data_pipeline` 仅当用户原话命中
  “重新/重跑/清洗/清理/再跑”类触发词时注入本轮，其余场景模型无权发起管线。
- **只读优先**：数据未变（输入指纹一致）时复用最新报告，Agent 先调 `read_registry` /
  `read_latest_report` 比对。
- **Prompt 三条硬约束**：① 一切分数/计数/版本必须来自工具返回字段；② 措辞区分
  修复（repair）/去重（dedup）/隔离（isolate），删除不得说成修复，并必引局限；
  ③ 所求数据未生成时如实说明，不得占位。
- **回复校验**：回复中的分数类数值须经报告实测值白名单校验（±0.01），越界值在 API 响应
  `flagged` 中列出并附“该数字未出自本次报告”标记。
- **结论引用校验**：治理形态下改为 `ConclusionProposal` 校验，见
  [系统设计.md](系统设计.md) §9.4。
- **失败语义**：任何阶段失败 → 任务 failed + 失败环节与原因；管线失败时由 Agent 如实转述，
  不生成部分占位报告。

### 3.5 任务生命周期与会话

- **长任务两阶段**：Agent 触发清洗后立即回执（task_id）；写工具把请求提交到持久队列，
  状态经 `ACCEPTED → QUEUED → RUNNING → VALIDATING → PUBLISHING → PUBLISHED` 推进。
  Agent 的叙事与阶段展示仅用于 UI 呈现，业务状态与恢复以 MySQL 为准。
- **会话记忆**：SQLiteSession 落盘 `outputs/agent_memory.db`，按 task_id；服务重启后同一任务
  的追问仍携带历史上下文。
- **并发控制**：多任务由持久队列与 worker 的租约、执行令牌串行调度；过期执行者不能覆盖新状态。
- **可观测**：本地 trace 落盘 `outputs/traces/*.json`（Agent→Turn→LLM→Function span 链），
  默认的云端上传处理器关闭。

## 4. HTTP 契约

`agent/server.py` 是纯适配层：解析参数、调用业务服务、映射错误码。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/tasks` | body `{message}` → 返回 task_id（异步）；模型未配置时 503 + 原因 |
| GET | `/api/tasks/{id}` | status / stage / narrative（LLM 叙事）/ error |
| GET | `/api/tasks/{id}/report` | 完整评估报告（`report.json` 原样） |
| GET | `/api/tasks/{id}/report/download` | 报告下载 |
| GET | `/api/tasks/{id}/sample` | 三表 clean/isolate/log 样例 |
| POST | `/api/tasks/{id}/ask` | 追问（同步）：回答 + `flagged` 列表 |
| GET | `/api/registry` | 版本登记列表 |

治理形态的 HTTP 入口清单见 [系统设计.md](系统设计.md) §14.2。

## 5. 前端契约

`web/index.html` 为单页静态页面，轮询任务状态，契约与 §4 一致：自然语言输入 →
执行阶段展示（含 Agent 叙事与 MR 阶段）→ 五维前后对比（含“不适用”标注）→
处置统计（修复/去重/隔离/登记 + 代表性记录）→ 数据量变化 → T1/T2 与版本 → 局限说明 →
样例查看/报告下载 → 追问（展示回答与越界标记）。

`web/governance.html` + `web/governance.js` 提供治理控制台：任务列表与状态、Attempt 与
Job 明细、正式报告与五维对比、证据查看、版本比较、性能与历史分析入口。处置分类由服务端
结构化结果提供，不按规则名称猜测“正常保留/修复”。

## 6. 已知限制

1. 执行模式为容器内 local MapReduce 或单节点 HDFS/YARN，非多节点集群。
2. LLM 为外部服务依赖，可用性受 DeepSeek 服务质量影响；意图理解依赖模型能力，
   触发词注入门按中文关键词设计，英文等价表达需扩展词表。
3. “登记”类问题保留在数据中，清洗后仍有未解决字段（如实计数展示）。
4. 评分类指标无法证明数据“真实性”，只能证明“值域/格式/引用”层面的合规。
5. 前端为静态页面 + 轮询，不承担大规模并发；治理控制台以查询与证据展示为主。
