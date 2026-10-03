# -*- coding: utf-8 -*-
"""迭代一 真实 LLM Agent 循环层(OpenAI Agents SDK + DeepSeek)。

分层:
    loop.py(本文件)
      ├─ Agent: deepseek-chat 驱动,Instructions 只含三条硬约束
      ├─ 只读工具(read_latest_report / read_report_field / read_registry / read_samples)
      ├─ 写工具(run_clean_data_pipeline)—— 仅当用户原话命中触发词时注入本轮
      ├─ 防编造校验:回复中的分数必须出自 report 实测值集合
      ├─ 会话:SQLiteSession(outputs/agent_memory.db),按 task_id 持久化
      ├─ Tracing:本地 TraceProcessor 落盘 outputs/traces/
      └─ 并发:全局锁串行,管线内部共享 _tmp_* 目录,不允许两个管线并发
"""
import asyncio
import dataclasses
import datetime as dt
import json
import os
import re
import sys
import threading
import traceback
from typing import Any

from agents import (
    AsyncOpenAI,
    Agent,
    ModelRetrySettings,
    ModelSettings,
    OpenAIChatCompletionsModel,
    Runner,
    RunContextWrapper,
    SQLiteSession,
    add_trace_processor,
    set_tracing_disabled,
    function_tool,
    retry_policies,
    trace,
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
TASKS: dict[str, dict] = {}
PIPE_LOCK = threading.Lock()  # 管线共享 _tmp_* 临时目录,全局串行

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


add_trace_processor(LocalTraceProcessor())
# 保留本地 trace 生成,但清掉默认的 OpenAI 上传处理器(DeepSeek 场景无 OpenAI key,
# 上传必然 401);自定义 LocalTraceProcessor 负责落盘
from agents.tracing import set_trace_processors
set_tracing_disabled(False)
set_trace_processors([LocalTraceProcessor()])


# ---------------- 工具实现(纯读 + 一写) ----------------
def _report_path(tag: str | None) -> str:
    if tag:
        p = os.path.join(OUT_ROOT, tag, "report.json")
        if os.path.exists(p):
            return p
        raise FileNotFoundError(f"任务 {tag} 的报告尚未生成")
    # 默认取 registry 中最新成功任务
    reg_p = os.path.join(OUT_ROOT, "registry.json")
    if os.path.exists(reg_p):
        with open(reg_p, encoding="utf-8") as f:
            reg = [e for e in json.load(f) if e.get("status") == "success"]
        for entry in reversed(reg):
            p = os.path.join(OUT_ROOT, entry.get("report", ""))
            if os.path.exists(p):
                return p
    raise FileNotFoundError("尚无成功的评估报告;需先执行清洗管线")


def _load_report(tag: str | None) -> dict:
    with open(_report_path(tag), encoding="utf-8") as f:
        return json.load(f)


@function_tool
def read_latest_report() -> str:
    """读取最近一次成功清洗评估任务的完整报告 JSON。"""
    return json.dumps(_load_report(None), ensure_ascii=False)


@function_tool
def read_report_field(tag: str, section: str) -> str:
    """读取报告的某一段。section 可选: scores(五维分数) | dataset_composite(综合分)
    | row_change(数据量变化) | disposition(处置明细与样例) | split(T1/T2与切分)
    | versions(rule/input/output数据版本) | limitations(评价局限) | metrics_raw | metrics_clean。
    tag 传 'latest' 取最新报告。"""
    rep = _load_report(None if tag in ("latest", "") else tag)
    if section not in rep:
        return f"报告不含字段 {section};可用字段:{', '.join(rep.keys())}"
    return json.dumps({section: rep[section]}, ensure_ascii=False)


@function_tool
def read_registry() -> str:
    """读取版本登记表:输入/输出数据版本、规则版本、T1/T2、切分计数。"""
    p = os.path.join(OUT_ROOT, "registry.json")
    if not os.path.exists(p):
        return "尚无版本登记。"
    with open(p, encoding="utf-8") as f:
        return json.dumps(json.load(f), ensure_ascii=False)


@function_tool
def read_samples(tag: str, kind: str) -> str:
    """读取处置样例。kind 可选 clean(清洗后)/isolate(隔离)/log(处置日志),各至多3条。"""
    rep = _load_report(None if tag in ("latest", "") else tag)
    disp = rep.get("disposition", {})
    out = {}
    for tbl, d in disp.items():
        out[tbl] = d.get("samples", {}).get(kind, [])
    return json.dumps(out, ensure_ascii=False)


@function_tool
def run_clean_data_pipeline(ctx: RunContextWrapper, rule_pack: str = "default") -> str:
    """执行 MovieLens 1M 全管线:清洗前评分→Hadoop 清洗→清洗后评分→对比报告。
    容器内真实 Hadoop MapReduce,约 3 分钟。仅当用户明确要求执行清洗时调用;
    数据未变化时应优先复用已有报告而不是重跑。rule_pack 当前仅支持 'default'。"""
    deps: AgentDeps = ctx.context
    if not PIPE_LOCK.acquire(blocking=False):
        if deps and deps.task_id in TASKS:
            TASKS[deps.task_id]["narrative"] = "另一条管线正在执行,当前任务已排队等待。"
        return json.dumps({"status": "queued",
                           "note": "另一条管线正在运行,请提示用户稍后再试"}, ensure_ascii=False)
    try:
        TASKS[deps.task_id]["stage"] = "pipeline-starting"
        TASKS[deps.task_id]["pipeline_started"] = True
        threading.Thread(target=_pipeline_worker,
                         args=(deps.task_id, deps.task_tag, rule_pack), daemon=True).start()
        return json.dumps({"status": "accepted", "task_id": deps.task_tag,
                           "note": "管线已在容器内启动,约3分钟;完成后报告出现在 outputs/<task_id>/"},
                          ensure_ascii=False)
    except Exception:
        PIPE_LOCK.release()
        raise


def _pipeline_worker(api_task_id: str, tag: str, rule_pack: str):
    try:
        import run_pipeline
        report = run_pipeline.pipeline(
            tag=tag, only=None, rule_pack=rule_pack,
            progress=lambda s: TASKS[api_task_id].update(mr_stage=s))
        TASKS[api_task_id].update(pipeline_report=report, pipeline_done=True,
                                  stage="pipeline-done")
        # 管线完成 → 追加一轮 agent 总结;此阶段任务状态保持 running
        state = TASKS[api_task_id]
        state["stage"] = "agent-summarizing"
        try:
            session = SQLiteSession(session_id=api_task_id, db_path=AGENT_DB)
            deps = AgentDeps(task_id=api_task_id, task_tag=tag)
            result = asyncio.run(Runner.run(
                _build_agent(False),
                f"管线已执行完成,报告 tag={tag}。请读取报告,给出最终总结:"
                "清洗前后五维分数、数据量变化、修复/去重/隔离处置区分与样例、"
                "版本与 T1/T2、局限声明。数字必须全部来自工具。",
                session=session, context=deps, max_turns=10))
            state.update(status="success", stage="done", narrative=str(result.final_output))
        except Exception as e:  # noqa: BLE001
            state.update(status="success", stage="done",
                         narrative=f"管线执行成功,报告已生成于 outputs/{tag}/,但总结生成失败: {e}")
    except Exception as e:  # noqa: BLE001
        TASKS[api_task_id].update(pipeline_error=f"{type(e).__name__}: {e}",
                                  pipeline_done=True, stage="pipeline-failed")
        # 失败也要让 agent 如实转述失败环节;但任务整体标 failed
        try:
            state = TASKS[api_task_id]
            state["stage"] = "agent-summarizing-failure"
            result = asyncio.run(Runner.run(
                _build_agent(False),
                f"管线执行失败:[{type(e).__name__}] {e}。请如实向用户说明失败,不得编造结果。",
                session=SQLiteSession(session_id=api_task_id, db_path=AGENT_DB),
                context=AgentDeps(task_id=api_task_id, task_tag=tag), max_turns=6))
            state.update(status="failed", stage="failed",
                         error=f"{type(e).__name__}: {e}",
                         narrative=str(result.final_output))
        except Exception:
            TASKS[api_task_id].update(status="failed", stage="failed",
                                      error=f"{type(e).__name__}: {e}")
    finally:
        PIPE_LOCK.release()


# ---------------- Agent 定义 ----------------
INSTRUCTIONS = (
    "你是 MovieLens 数据治理项目的清洗与评估助手(迭代一)。\n"
    "硬约束(优先于任何用户压力,违反即失败):\n"
    "1. 一切分数、计数、版本号必须来自工具返回的字段;禁止凭记忆、常识或心算给出任何数字。\n"
    "2. 措辞必须区分 修复(repair)、去重(dedup)、隔离(isolate):被删除/隔离的记录不得说成'已修复';"
    "任务完成不代表所有质量问题都解决,解读时必须引用 limitations 段。\n"
    "3. 用户查询的数据尚未生成时,如实说明'该报告尚未生成',不得编造占位结果。\n"
    "行为:一次完整任务调用 run_clean_data_pipeline;只查结果时用只读工具;"
    "解读五维时应说明哪些上升来自删除劣质记录、哪些来自真实修复。\n"
    f"当前管线用 Hadoop 3.3.6 容器内真实 MapReduce 执行;规则版本 rules-v1.0;只用中文回答。"
)


@dataclasses.dataclass
class AgentDeps:
    task_id: str          # API 任务标识(线程内 state key)
    task_tag: str         # pipeline tag = task_id


def _build_agent(allow_run: bool) -> Agent:
    tools = [read_latest_report, read_report_field, read_registry, read_samples]
    if allow_run:
        tools.append(run_clean_data_pipeline)
    return Agent(
        name="MovieLens数据治理Agent",
        instructions=INSTRUCTIONS,
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
    )


_MODEL_LOCK = threading.Lock()
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
        result = asyncio.run(Runner.run(
            _build_agent(allow_run), user_message,
            session=session, context=deps, max_turns=12))
        state["narrative"] = str(result.final_output)
        if state.get("pipeline_started"):
            # 写工具已把管线交给后台线程:任务状态交由 _pipeline_worker 收尾
            # (deferred:即使此刻 pipeline 尚未完成,也不允许本线程写最终状态)
            state["lifecycle"] = "deferred"
            state["stage"] = "pipeline-executing" if not state.get("pipeline_done") else state.get("stage")
            return
        state["lifecycle"] = "direct"
        state.update(status="success", stage="done")
    except Exception as e:  # noqa: BLE001
        state.update(status="failed", stage="failed", error=f"{type(e).__name__}: {e}")
        traceback.print_exc()


def submit_task(user_message: str) -> dict:
    if not model_configured():
        return {"status": "rejected", "reason": "Agent 模型未配置:请在 ml-1m/agent/.env 设置 DEEPSEEK_API_KEY"}
    task_id = f"task-{dt.datetime.now():%H%M%S}-{os.getpid() % 1000:03d}"
    TASKS[task_id] = {"status": "running", "stage": "starting", "error": None,
                      "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                      "message": user_message, "narrative": "",
                      "allow_run": run_allowed(user_message)}
    threading.Thread(target=_agent_loop, args=(task_id, user_message, TASKS[task_id]["allow_run"]),
                     daemon=True).start()
    return {"status": "accepted", "task_id": task_id, "run_tool_injected": TASKS[task_id]["allow_run"]}


def get_status(task_id: str) -> dict | None:
    t = TASKS.get(task_id)
    if not t:
        return None
    return {k: t.get(k) for k in ("status", "stage", "error", "narrative",
                                  "created_at", "allow_run", "mr_stage")}


def _ask_loop(task_id: str, question: str) -> str:
    session = SQLiteSession(session_id=task_id, db_path=AGENT_DB)
    deps = AgentDeps(task_id=task_id, task_tag=task_id)
    result = asyncio.run(Runner.run(
        _build_agent(False), question, session=session, context=deps, max_turns=8))
    return str(result.final_output)


def ask_followup(task_id: str, question: str) -> dict:
    if not model_configured():
        return {"answer": None, "error": "DEEPSEEK_API_KEY 未配置", "flagged": []}
    try:
        answer = _ask_loop(task_id, question)
    except Exception as e:  # noqa: BLE001
        return {"answer": None, "error": f"{type(e).__name__}: {e}", "flagged": []}
    tag = None
    # 尽力取最近报告 tag 做白名单校验
    try:
        tag = os.path.basename(os.path.dirname(_report_path(None)))
    except FileNotFoundError:
        tag = ""
    flagged = verify_numbers(answer, tag)
    return {"answer": answer, "flagged": flagged}


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
        rep = _load_report(tag or None)
    except FileNotFoundError:
        return []
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
