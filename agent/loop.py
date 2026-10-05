# -*- coding: utf-8 -*-
"""真实 LLM Agent 循环层(OpenAI Agents SDK + DeepSeek)。

分层:
    loop.py(本文件)
      ├─ Agent: deepseek-chat 驱动,Instructions 只含三条硬约束
      ├─ 只读工具(read_latest_report / read_report_field / read_registry / read_samples)
      ├─ 写工具(run_clean_data_pipeline)—— 仅当用户原话命中触发词时注入本轮
      ├─ 防编造校验:回复中的分数必须出自 report 实测值集合
      ├─ 会话:SQLiteSession(outputs/agent_memory.db),按 task_id 持久化
      ├─ Tracing:本地 TraceProcessor 落盘 outputs/traces/
      └─ 任务状态:内存只保留叙事与阶段展示,业务状态与恢复以 MySQL 为准
"""
import asyncio
from contextlib import closing
import dataclasses
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import threading
import traceback
import uuid
from pathlib import Path

from agent.published import ReportUnavailable, read_published_report
from agent.legacy_archive import LegacyUnavailable

from agents import (
    AsyncOpenAI,
    Agent,
    ModelRetrySettings,
    ModelSettings,
    OpenAIChatCompletionsModel,
    Runner,
    RunContextWrapper,
    SQLiteSession,
    set_tracing_disabled,
    function_tool,
    retry_policies,
)

BASE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(BASE)
OUT_ROOT = os.path.join(PROJECT, "outputs")
TRACE_DIR = os.path.join(OUT_ROOT, "traces")
AGENT_DB = os.path.join(OUT_ROOT, "agent_memory.db")
sys.path.insert(0, os.path.join(PROJECT, "pipeline"))

# ---------------- .env 加载(轻量,无额外依赖) ----------------
def _load_env():
    p = os.path.join(BASE, ".env")
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip())

_load_env()
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")

# ---------------- 全局任务状态 ----------------
TASKS: dict[str, dict] = {}  # 仅保存 Agent 叙事与阶段展示,业务状态以 MySQL 为准

RUN_TRIGGERS = re.compile(r"重新|重跑|再跑|清洗|清理|从头|重新评估|重新清洗再来|干净")


def run_allowed(user_message: str) -> bool:
    """结构强制:仅当用户原话含清洗/重跑类触发词时,才把写工具注入本轮。"""
    return bool(RUN_TRIGGERS.search(user_message))


# ---------------- 本地 Trace 落盘 ----------------
class LocalTraceProcessor:
    """把每个 trace 的 span 链写成 outputs/traces/<trace_id>.json(推理链答辩用)。
    通过 on_span_start/on_span_end 累积 span,trace 结束时统一落盘。"""

    def __init__(self):
        os.makedirs(TRACE_DIR, exist_ok=True)
        self._lock = threading.Lock()
        self._spans: dict[str, list] = {}   # trace_id -> [span dict]
        self._trace_meta: dict[str, dict] = {}

    def _dump(self, trace_id: str):
        doc = {"trace_id": trace_id,
               "name": self._trace_meta.get(trace_id, {}).get("name", ""),
               "events": self._spans.get(trace_id, [])}
        tid = re.sub(r"[^a-zA-Z0-9_-]", "_", str(trace_id) or "trace")
        with self._lock, open(os.path.join(TRACE_DIR, tid + ".json"), "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

    def on_trace_start(self, trace_obj):
        tid = getattr(trace_obj, "trace_id", "")
        self._trace_meta[tid] = {"name": getattr(trace_obj, "name", "")}
        self._spans.setdefault(tid, [])

    def on_trace_end(self, trace_obj):
        tid = getattr(trace_obj, "trace_id", "")
        self._dump(tid)
        self._spans.pop(tid, None)
        self._trace_meta.pop(tid, None)

    @staticmethod
    def _span_record(span) -> dict:
        data = getattr(span, "span_data", None)
        rec = {"type": getattr(span, "span_data_type", None) or type(data).__name__,
               "name": getattr(span, "name", "") or getattr(data, "name", ""),
               "started_at": getattr(span, "started_at", ""),
               "ended_at": getattr(span, "ended_at", "")}
        usage = getattr(data, "usage", None)
        if usage is not None:
            for k in ("input_tokens", "output_tokens", "total_tokens"):
                v = getattr(usage, k, None)
                if v is not None:
                    rec[k] = v
        if "LLM" in rec["type"] or type(data).__name__ == "GenerationSpanData":
            # 模型名与 token 用量是答辩关心的字段
            rec["model"] = getattr(data, "model", "")
        return rec

    def on_span_start(self, span):
        tid = getattr(span, "trace_id", "")
        if tid in self._spans:
            self._spans[tid].append(self._span_record(span))

    def on_span_end(self, span):
        tid = getattr(span, "trace_id", "")
        if tid in self._spans:
            # span_end 时补齐 ended_at / usage(生成后才可用)
            for rec in reversed(self._spans[tid]):
                if rec["name"] == (getattr(span, "name", "") or ""):
                    rec["ended_at"] = getattr(span, "ended_at", rec.get("ended_at", ""))
                    data = getattr(span, "span_data", None)
                    usage = getattr(data, "usage", None)
                    if usage is not None:
                        for k in ("input_tokens", "output_tokens", "total_tokens"):
                            v = getattr(usage, k, None)
                            if v is not None:
                                rec[k] = v
                    rec["model"] = getattr(data, "model", rec.get("model", ""))
                    break

    def shutdown(self):
        pass

    def force_flush(self):
        pass


# 保留本地 trace 生成,但清掉默认的 OpenAI 上传处理器(DeepSeek 场景无 OpenAI key,
# 上传必然 401);自定义 LocalTraceProcessor 负责落盘
from agents.tracing import set_trace_processors
set_tracing_disabled(False)
set_trace_processors([LocalTraceProcessor()])


# ---------------- 工具实现(纯读 + 一写) ----------------
def _report_path(tag: str | None) -> str:
    root = Path(OUT_ROOT).resolve()
    if tag:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", tag):
            raise FileNotFoundError("Invalid report identity")
        if root == (Path(PROJECT) / "outputs").resolve():
            from metadata.store import Store
            store = Store()
            if store.get_run(tag):
                _, _, publication = read_published_report(tag, store=store)
                return str(root / tag / "attempts" / publication["attempt_id"] / "artifacts" / "report.json")
        p = root / tag / "report.json"
        if root != (Path(PROJECT) / "outputs").resolve() and p.is_file():
            return str(p)
        if root == (Path(PROJECT) / "outputs").resolve():
            from agent.legacy_archive import LegacyUnavailable, checked_path, read_legacy_report, resolve_legacy_report
            try:
                archive = resolve_legacy_report(tag)
                read_legacy_report(tag)
                return str(checked_path(PROJECT, archive["source_path"]))
            except LegacyUnavailable as error:
                if error.status_code != 404:
                    raise
        raise FileNotFoundError(f"任务 {tag} 的报告尚未生成")
    # 默认取 registry 中最新成功任务
    reg_p = os.path.join(OUT_ROOT, "registry.json")
    if os.path.exists(reg_p):
        with open(reg_p, encoding="utf-8") as f:
            reg = [e for e in json.load(f) if e.get("status") == "success"]
        for entry in reversed(reg):
            p = (root / entry.get("report", "")).resolve()
            if p.is_relative_to(root) and p.is_file():
                return str(p)
    raise FileNotFoundError("尚无成功的评估报告;需先执行清洗管线")


def _load_report(tag: str | None) -> dict:
    if tag and Path(OUT_ROOT).resolve() == (Path(PROJECT) / "outputs").resolve():
        from metadata.store import Store
        store = Store()
        if store.get_run(tag):
            return read_published_report(tag, store=store)[1]
    with open(_report_path(tag), encoding="utf-8") as f:
        return json.load(f)


@function_tool
def read_latest_report(ctx: RunContextWrapper) -> str:
    """读取最近一次成功清洗评估任务的完整报告 JSON。"""
    run_id = _bound_report(ctx, None)
    _, report, publication = read_published_report(run_id)
    return json.dumps({**report, "publication_identity": {
        "run_id": run_id, "publish_id": publication["publish_id"],
        "input_version": report["input_data_version"], "rule_version": report["rule_version"],
        "metric_version": report["metric_version"]}}, ensure_ascii=False)


@function_tool
def read_report_field(ctx: RunContextWrapper, tag: str, section: str) -> str:
    """读取报告的某一段。section 可选: scores(五维分数) | dataset_composite(综合分)
    | row_change(数据量变化) | disposition(处置明细与样例) | split(T1/T2与切分)
    | versions(rule/input/output数据版本) | limitations(评价局限) | metrics_raw | metrics_clean。
    tag 传 'latest' 取最新报告。"""
    rep = _load_report(_bound_report(ctx, tag))
    if section not in rep:
        return f"报告不含字段 {section};可用字段:{', '.join(rep.keys())}"
    return json.dumps({section: rep[section]}, ensure_ascii=False)


@function_tool
def read_registry() -> str:
    """读取版本登记表:输入/输出数据版本、规则版本、T1/T2、切分计数。"""
    from metadata.store import Store
    return json.dumps(Store().list_runs(status="PUBLISHED", limit=50), ensure_ascii=False, default=str)


@function_tool
def read_samples(ctx: RunContextWrapper, tag: str, kind: str) -> str:
    """读取处置样例。kind 可选 clean(清洗后)/isolate(隔离)/log(处置日志),各至多3条。"""
    rep = _load_report(_bound_report(ctx, tag))
    disp = rep.get("disposition", {})
    out = {}
    for tbl, d in disp.items():
        out[tbl] = d.get("samples", {}).get(kind, [])
    return json.dumps(out, ensure_ascii=False)


@function_tool
def read_processing_evidence(ctx: RunContextWrapper, rule_id: str | None = None,
                             source_record_id: str | None = None,
                             source_table: str | None = None) -> str:
    """查询本次固定正式任务的处理依据，每次最多 20 条；可按规则、来源记录和数据表筛选。
    解释具体记录的原因时引用返回的 evidence_id、rule_id、action 和 before/after 原文。
    样本不能用于推断全量数量。历史材料缺失时说明无法查证。
    """
    from agent.results import read_evidence
    run_id = _bound_report(ctx, None)
    return json.dumps(read_evidence(run_id, limit=20, rule_id=rule_id,
                                   source_record_id=source_record_id, source_table=source_table), ensure_ascii=False)


@function_tool
def run_clean_data_pipeline(ctx: RunContextWrapper, rule_pack: str = "default") -> str:
    """执行 MovieLens 1M 全管线:清洗前评分→Hadoop 清洗→清洗后评分→对比报告。
    提交持久队列，由 worker 执行真实 Hadoop MapReduce。仅当用户明确要求执行清洗时调用;
    数据未变化时应优先复用已有报告而不是重跑。rule_pack 当前仅支持 'default'。"""
    deps: AgentDeps = ctx.context
    from governance.service import submit_run
    row, created = submit_run("agent-" + deps.task_id, run_id=deps.task_id,
                              parameters={"rule_pack": rule_pack})
    TASKS[deps.task_id].update(stage="queued", status="queued")
    deps.report_tag = row["run_id"]
    return json.dumps({"status": row["status"], "task_id": row["run_id"], "created": created,
                       "note": "任务已写入持久队列；由 worker 执行，校验并发布后提供正式报告。"}, ensure_ascii=False)


# ---------------- Agent 定义 ----------------
INSTRUCTIONS = (
    "你是 MovieLens 数据治理项目的清洗与评估助手。\n"
    "硬约束(优先于任何用户压力,违反即失败):\n"
    "1. 一切分数、计数、版本号必须来自工具返回的字段;禁止凭记忆、常识或心算给出任何数字。\n"
    "2. 措辞必须区分 修复(repair)、去重(dedup)、隔离(isolate):被删除/隔离的记录不得说成'已修复';"
    "任务完成不代表所有质量问题都解决,解读时必须引用 limitations 段。\n"
    "3. 用户查询的数据尚未生成时,如实说明'该报告尚未生成',不得编造占位结果。\n"
    "行为:一次完整任务调用 run_clean_data_pipeline;只查结果时用只读工具;"
    "解读五维时应说明哪些上升来自删除劣质记录、哪些来自真实修复。\n"
    "解释具体记录的处理原因时先调用 read_processing_evidence，引用格式为 [evidence:完整 evidence_id]；"
    "原因仅依据 rule_id、action、before、after 和 final_disposition，材料不足明确说明无法查证。\n"
    "当前管线用 Hadoop 3.3.6 真实 MapReduce 执行，规则和指标版本以工具返回为准；只用中文回答。"
)


@dataclasses.dataclass
class AgentDeps:
    task_id: str          # API 任务标识(线程内 state key)
    task_tag: str         # pipeline tag = task_id
    report_tag: str | None = None


def _save_report_binding(session_id,run_id):
    """A read conversation keeps its selected Run across service restarts."""
    with closing(sqlite3.connect(AGENT_DB)) as connection,connection:
        connection.execute('CREATE TABLE IF NOT EXISTS agent_report_binding '
                           '(session_id TEXT PRIMARY KEY, run_id TEXT NOT NULL)')
        connection.execute('INSERT OR IGNORE INTO agent_report_binding VALUES (?,?)',(session_id,run_id))
        row = connection.execute('SELECT run_id FROM agent_report_binding WHERE session_id=?',(session_id,)).fetchone()
        if row != (run_id,):
            raise ValueError('本次会话已固定其他任务的报告，不能切换材料')


def _read_report_binding(session_id):
    if not os.path.isfile(AGENT_DB):
        return None
    with closing(sqlite3.connect(AGENT_DB)) as connection:
        exists = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_report_binding'").fetchone()
        if not exists:
            return None
        row = connection.execute('SELECT run_id FROM agent_report_binding WHERE session_id=?',(session_id,)).fetchone()
        return row[0] if row else None


def _bound_report(ctx, requested):
    """Pin the first read; historical follow-up cannot drift when latest changes."""
    deps = ctx.context
    if deps.report_tag:
        if requested and requested not in {"latest", deps.report_tag}:
            raise ValueError("本次查询已固定报告范围，不能混用其他任务的材料")
        return deps.report_tag
    if requested and requested != "latest":
        _report_path(requested)
        selected = requested
    else:
        from metadata.store import Store
        selected = Store().current_run()
        if not selected:
            raise FileNotFoundError("尚无当前正式报告，请指定已有正式任务。")
        _report_path(selected)
    deps.report_tag = selected
    return selected


def _build_agent(allow_run: bool, *, output_type=None) -> Agent:
    tools = [read_latest_report, read_report_field, read_registry, read_samples, read_processing_evidence]
    if allow_run:
        tools.append(run_clean_data_pipeline)
    return Agent(
        name="MovieLens数据治理Agent",
        instructions=INSTRUCTIONS + ("\n本次只提交结构化结论提案：report_claims 必须定位报告标量字段并照抄值；"
                                     "evidence_claims 必须照抄处理依据的全部事实字段。"
                                     "读取正式报告和处理依据以填入 run_id、publish_id 和版本；"
                                     "材料不足时返回空事实列表，推测仅放 hypotheses。" if output_type else ""),
        model=_model(),
        model_settings=ModelSettings(
            timeout=120.0,
            retry=ModelRetrySettings(
                max_retries=2,
                backoff={"initial_delay": 0.5, "max_delay": 5.0, "multiplier": 2.0, "jitter": True},
                policy=retry_policies.any(
                    retry_policies.network_error(),
                    retry_policies.http_status([429, 500, 502, 503, 504]),
                    retry_policies.retry_after(),
                ),
            ),
        ),
        tools=tools,
        output_type=output_type,
    )


def _model():
    """每次构建 Agent 时新建客户端:AsyncOpenAI 的 httpx 连接池绑定创建时的
    asyncio loop,跨线程/跨 loop 复用全局客户端会触发 'Event loop is closed'。"""
    key = os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        raise RuntimeError("DEEPSEEK_API_KEY 未配置(.env 或环境变量)")
    client = AsyncOpenAI(api_key=key, base_url="https://api.deepseek.com")
    return OpenAIChatCompletionsModel(model=DEEPSEEK_MODEL, openai_client=client)


def model_configured() -> bool:
    return bool(os.environ.get("DEEPSEEK_API_KEY"))


# ---------------- 历史会话回放 ----------------
def read_transcript(task_id: str) -> list:
    """从 SQLiteSession 数据库回放某任务的历史对话(服务重启后仍可用)。
    只提取用户消息与助手回复;工具调用折叠为一条提示气泡,推理摘要与
    工具原始输出不入 transcript。"""
    if not os.path.exists(AGENT_DB):
        return []
    import sqlite3
    out = []
    con = sqlite3.connect(AGENT_DB)
    try:
        rows = con.execute(
            "SELECT message_data FROM agent_messages WHERE session_id=? ORDER BY id",
            (task_id,)).fetchall()
    finally:
        con.close()
    for (data,) in rows:
        try:
            j = json.loads(data)
        except Exception:
            continue
        role = j.get("role")
        if role == "user" and isinstance(j.get("content"), str):
            out.append({"role": "user", "text": j["content"]})
        elif role == "assistant" and j.get("type") == "message":
            texts = [p.get("text", "") for p in j.get("content", [])
                     if isinstance(p, dict) and p.get("type") == "output_text"]
            text = "\n".join(t for t in texts if t)
            if text:
                try:
                    proposal = json.loads(text)
                    if isinstance(proposal, dict) and {"report_claims", "evidence_claims"}.issubset(proposal):
                        text = "本轮模型提交了结构化提案；正式结论需经过报告与证据校验，以查询响应中的校验结果为准。"
                except ValueError:
                    pass
                out.append({"role": "agent", "text": text})
        elif j.get("type") == "function_call":
            name = j.get("name", "")
            if out and out[-1].get("tool") == name:
                out[-1]["tool_count"] = out[-1].get("tool_count", 1) + 1
            else:
                out.append({"role": "tool", "text": name})
    return out


# ---------------- 入口:提交任务 / 状态 / 追问 ----------------
def _agent_loop(task_id: str, user_message: str, allow_run: bool):
    state = TASKS[task_id]
    try:
        session = SQLiteSession(session_id=task_id, db_path=AGENT_DB)
        deps = AgentDeps(task_id=task_id, task_tag=task_id)
        state["stage"] = "agent-thinking"
        from agent.conclusions import ConclusionProposal, validate_conclusion
        result = asyncio.run(Runner.run(
            _build_agent(allow_run, output_type=None if allow_run else ConclusionProposal), user_message,
            session=session, context=deps, max_turns=12))
        if allow_run:
            state["narrative"] = str(result.final_output)
            state["narrative_kind"] = "REQUEST_ACKNOWLEDGEMENT"
        else:
            if not deps.report_tag:
                raise ValueError("本轮查询未绑定正式报告")
            verified = validate_conclusion(deps.report_tag, result.final_output)
            _save_report_binding(task_id,deps.report_tag)
            state["narrative"] = verified["answer"]
            state["conclusion"] = verified
            state["report_run_id"] = deps.report_tag
        state.update(status="success", stage="done")
    except Exception as e:  # noqa: BLE001
        state.update(status="failed", stage="failed", error=f"{type(e).__name__}: {e}")
        traceback.print_exc()


def submit_task(user_message: str) -> dict:
    if not model_configured():
        return {"status": "rejected", "reason": "Agent 模型未配置:请在 ml-1m/agent/.env 设置 DEEPSEEK_API_KEY"}
    task_id = f"task-{uuid.uuid4().hex}"
    TASKS[task_id] = {"status": "running", "stage": "starting", "error": None,
                      "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                      "message": user_message, "narrative": "",
                      "allow_run": run_allowed(user_message)}
    threading.Thread(target=_agent_loop, args=(task_id, user_message, TASKS[task_id]["allow_run"]),
                     daemon=True).start()
    return {"status": "accepted", "task_id": task_id, "run_tool_injected": TASKS[task_id]["allow_run"]}


def get_status(task_id: str) -> dict | None:
    from metadata.store import Store
    run = Store().get_run(task_id)
    if run:
        display = "success" if run["status"] == "PUBLISHED" else ("failed" if run["status"] == "FAILED" else "running")
        task = TASKS.get(task_id, {})
        return {"status": display, "run_status": run["status"], "stage": run["stage"],
                "error": run["error"], "narrative": task.get("narrative", ""),
                "created_at": run["created_at"].isoformat(), "attempt_id": run["active_attempt"]}
    t = TASKS.get(task_id)
    if not t:
        return None
    return {k: t.get(k) for k in ("status", "stage", "error", "narrative",
                                  "created_at", "allow_run", "report_run_id", "conclusion", "narrative_kind")}


def _ask_loop(task_id: str, question: str, *, report_run_id=None):
    from agent.conclusions import ConclusionProposal
    session = SQLiteSession(session_id=task_id, db_path=AGENT_DB)
    deps = AgentDeps(task_id=task_id, task_tag=task_id, report_tag=report_run_id or task_id)
    result = asyncio.run(Runner.run(
        _build_agent(False, output_type=ConclusionProposal), question, session=session, context=deps, max_turns=8))
    return result.final_output


def ask_followup(task_id: str, question: str) -> dict:
    try:
        report_run_id = _read_report_binding(task_id) or task_id
    except sqlite3.Error:
        return {'answer':None,'error':'会话报告绑定不可读取，请检查会话存储。','flagged':[],'status_code':503}
    try:
        _report_path(report_run_id)
    except FileNotFoundError:
        return {"answer": None, "error": "该任务的报告尚未生成", "flagged": [], "status_code":404}
    except (ReportUnavailable,LegacyUnavailable) as error:
        return {"answer": None, "error": str(error), "flagged": [], "status_code":error.status_code}
    if not model_configured():
        return {"answer": None, "error": "DEEPSEEK_API_KEY 未配置", "flagged": []}
    try:
        answer = (_ask_loop(task_id,question,report_run_id=report_run_id) if report_run_id != task_id
                  else _ask_loop(task_id, question))
        if not isinstance(answer, str):
            from agent.conclusions import validate_conclusion
            return validate_conclusion(report_run_id, answer)
    except Exception as e:  # noqa: BLE001
        return {"answer": None, "error": f"{type(e).__name__}: {e}", "flagged": []}
    flagged = verify_numbers(answer, report_run_id)
    return {"answer": answer, "flagged": flagged, "validation_status": "UNVERIFIED",
            "note": "旧文本响应只做数字兼容检查，不构成已验证的正式结论。"}


# ---------------- 防编造:分数白名单校验 ----------------
# 分数类数字:小数(93.33)或带计量后缀的整数(40509 条/96%);排除 task-<id> 等 token 内的数字
_NUM_RE = re.compile(r"(?<![\w-])(\d{1,3}\.\d{1,4}|\d{2,})(?=%|分|条)|(?<![\w-])(\d{1,3}\.\d{1,4})(?![\w.-])")


def _collect_numbers(obj, acc):
    if isinstance(obj, dict):
        for v in obj.values():
            _collect_numbers(v, acc)
    elif isinstance(obj, list):
        for v in obj:
            _collect_numbers(v, acc)
    elif isinstance(obj, (int, float)) and not isinstance(obj, bool):
        acc.add(round(float(obj), 2))


def verify_numbers(answer: str, tag: str) -> list[str]:
    """答案中出现的分数类数字(形如 93.33)必须在报告实测值集合内(±0.01)。"""
    try:
        rep = _load_report(tag)
    except FileNotFoundError:
        return [m.group(1) or m.group(2) for m in _NUM_RE.finditer(answer)]
    pool: set[float] = set()
    _collect_numbers(rep, pool)
    flagged = []
    for m in _NUM_RE.finditer(answer):
        v = float(m.group(1) or m.group(2))
        s = m.group(1) or m.group(2)
        if v in pool or any(abs(v - p) < 0.011 for p in pool):
            continue
        flagged.append(s)
    return flagged
