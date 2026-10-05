# -*- coding: utf-8 -*-
"""Agent 服务(HTTP 壳)。

    loop.py     —— 全部智能:LLM 循环、工具、会话记忆、防编造、trace、并发锁
    server.py   —— 仅做 HTTP 适配,五端点契约与前端(index.html)完全兼容:
      POST /api/tasks                自然语言发起任务(异步 task_id)
      GET  /api/tasks/{id}           状态 + agent 叙事(轮询)
      GET  /api/tasks/{id}/report    评估报告(原 report.json)
      GET  /api/tasks/{id}/report/download
      GET  /api/tasks/{id}/sample
      POST /api/tasks/{id}/ask       追问(同步,防编造白名单校验)
      GET  /api/registry             版本登记
"""
import json
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pymysql import MySQLError

from agent import loop
from agent.published import ReportUnavailable, read_published_report
from agent.legacy_archive import LegacyUnavailable, read_legacy_report
from agent.results import read_evidence, compare_runs
from governance.history import history_analysis, history_series, HistoryUnavailable
from governance.performance import performance_baseline
from governance.service import submit_run
from metadata.store import Conflict, Store, AdmissionPaused
from metadata.connection import MetadataUnavailable

BASE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(BASE)
OUT_ROOT = os.path.join(PROJECT, "outputs")
WEB_DIR = os.path.join(PROJECT, "web")

app = FastAPI(title="MovieLens 数据治理 Agent")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


class MsgBody(BaseModel):
    message: str


class RunBody(BaseModel):
    rule_pack: str = "default"


@app.exception_handler(MySQLError)
@app.exception_handler(MetadataUnavailable)
async def metadata_unavailable(request, error):
    return JSONResponse({"detail": "任务元数据服务不可用，请检查 MySQL 连接。"}, status_code=503)


@app.get('/api/health')
def health():
    return {'status':'up','schema_required':5}


@app.get('/api/readiness')
def readiness():
    store = Store()
    with store.transaction() as cursor:
        cursor.execute('SELECT version FROM schema_migration WHERE version=5')
        if not cursor.fetchone():
            raise HTTPException(503,'Governance schema version 5 migration is required')
        cursor.execute('SELECT admission_open,revision FROM runtime_control WHERE control_id=1')
        row = cursor.fetchone()
        if not row:
            raise HTTPException(503,'Runtime admission control is missing')
    return {'status':'ready','schema_version':5,'admission_open':bool(row['admission_open']),
            'control_revision':row['revision']}


@app.get("/api/runs")
def list_runs(before: int | None = Query(None, ge=1), limit: int = Query(50, ge=1, le=100),
              status: str | None = Query(None, min_length=1,max_length=24),
              dataset_id: str | None = Query(None,min_length=1,max_length=80),
              input_version: str | None = Query(None,min_length=1,max_length=100),
              rule_version: str | None = Query(None,min_length=1,max_length=100),
              metric_version: str | None = Query(None,min_length=1,max_length=100)):
    return Store().list_runs(before=before, limit=limit, status=status,dataset_id=dataset_id,
                            input_version=input_version,rule_version=rule_version,metric_version=metric_version)


@app.get('/api/datasets/{dataset_id}/current')
def current_result(dataset_id: str):
    try:
        row = Store().current_result(dataset_id)
    except ValueError as error:
        raise HTTPException(400,str(error)) from error
    if row is None:
        raise HTTPException(404,'Dataset has no current formal publication')
    return row


@app.get("/api/comparisons")
def compare_results(left: str, right: str):
    try:
        return compare_runs(left, right)
    except ReportUnavailable as error:
        raise HTTPException(error.status_code, str(error)) from error


@app.get("/api/runs/{run_id}/history-analysis")
def get_history_analysis(run_id: str):
    try:
        return history_analysis(run_id)
    except ReportUnavailable as error:
        raise HTTPException(error.status_code, str(error)) from error
    except HistoryUnavailable as error:
        raise HTTPException(503, str(error)) from error


@app.get("/api/history-series")
def get_history_series(runs: list[str] = Query(..., min_length=2, max_length=10)):
    try:
        return history_series(runs)
    except ReportUnavailable as error:
        raise HTTPException(error.status_code,str(error)) from error
    except ValueError as error:
        raise HTTPException(400,str(error)) from error
    except HistoryUnavailable as error:
        raise HTTPException(503,str(error)) from error


@app.get("/api/runs/{run_id}/performance")
def get_performance(run_id: str):
    try:
        return performance_baseline(run_id)
    except ReportUnavailable as error:
        raise HTTPException(error.status_code, str(error)) from error


@app.get("/api/runs/{run_id}/evidence")
def get_evidence(run_id: str, limit: int = Query(50, ge=1, le=100),
                 after: str = Query("", max_length=64),
                 source_table: str | None = Query(None, max_length=24),
                 rule_id: str | None = Query(None, max_length=80),
                 source_record_id: str | None = Query(None, max_length=64),
                 metric: str | None = Query(None, max_length=40),
                 evidence_id: str | None = Query(None, max_length=64)):
    try:
        return read_evidence(run_id, limit=limit, after=after, source_table=source_table,
                             rule_id=rule_id, source_record_id=source_record_id,
                             metric=metric, evidence_id=evidence_id)
    except ReportUnavailable as error:
        raise HTTPException(error.status_code, str(error)) from error


@app.post("/api/runs", status_code=202)
def create_run(body: RunBody, idempotency_key: str = Header(..., alias="Idempotency-Key")):
    try:
        row, created = submit_run(idempotency_key, parameters={"rule_pack": body.rule_pack})
    except AdmissionPaused as error:
        raise HTTPException(503,str(error)) from error
    except Conflict as error:
        raise HTTPException(409, str(error)) from error
    except ValueError as error:
        raise HTTPException(400, str(error)) from error
    return {"run_id": row["run_id"], "status": row["status"], "created": created,
            "execution_mode": row["request"]["execution_mode"]}


@app.get("/api/runs/{run_id}")
def get_run(run_id: str):
    store = Store()
    row = store.get_run(run_id)
    if row is None:
        raise HTTPException(404, "run not found")
    row["attempts"] = store.list_attempts(run_id)
    for attempt in row["attempts"]:
        attempt["jobs"] = store.list_jobs(attempt["attempt_id"])
    return row


@app.post("/api/runs/{run_id}/retry", status_code=202)
def retry_run(run_id: str):
    try:
        Store().retry(run_id)
    except AdmissionPaused as error:
        raise HTTPException(503,str(error)) from error
    except Conflict as error:
        raise HTTPException(409, str(error)) from error
    return {"run_id": run_id, "status": "QUEUED"}


def _published_report(run_id):
    try:
        return read_published_report(run_id)
    except ReportUnavailable as error:
        raise HTTPException(error.status_code, str(error)) from error


@app.get("/api/runs/{run_id}/report")
def get_published_report(run_id: str):
    content, _, publication = _published_report(run_id)
    return Response(content, media_type="application/json",
                    headers={"X-Publish-ID": publication["publish_id"]})


@app.get("/api/runs/{run_id}/report/download")
def download_published_report(run_id: str):
    content, _, publication = _published_report(run_id)
    return Response(content, media_type="application/json",
                    headers={"X-Publish-ID": publication["publish_id"],
                             "Content-Disposition": f'attachment; filename="{publication["publish_id"]}_report.json"'})


@app.get("/api/legacy/reports/{identity}")
def get_legacy_report(identity: str):
    try:
        return read_legacy_report(identity)
    except LegacyUnavailable as error:
        raise HTTPException(error.status_code, str(error)) from error
    except MySQLError as error:
        raise HTTPException(503, "legacy metadata is unavailable") from error


@app.post("/api/tasks")
def create_task(body: MsgBody):
    r = loop.submit_task(body.message)
    if r["status"] != "accepted":
        # 模型未配置等情况:明确拒绝并说明,不生成占位任务
        return JSONResponse(r, status_code=503)
    return {"task_id": r["task_id"], "status": "accepted"}


@app.get("/api/tasks/{task_id}")
def get_status(task_id: str):
    t = loop.get_status(task_id)
    if t is None:
        raise HTTPException(404, "task not found")
    return {"task_id": task_id, **t}


def _report_path(task_id: str) -> str:
    try:
        return loop._report_path(task_id)
    except FileNotFoundError:
        raise HTTPException(404, "report not generated yet")
    except ReportUnavailable as error:
        raise HTTPException(error.status_code, str(error)) from error
    except LegacyUnavailable as error:
        raise HTTPException(error.status_code, str(error)) from error
    except MySQLError as error:
        raise HTTPException(503, "legacy metadata is unavailable") from error


@app.get("/api/tasks/{task_id}/report")
def get_report(task_id: str):
    return JSONResponse(_report_body(task_id))


def _report_body(task_id: str):
    _report_path(task_id)
    try:
        return loop._load_report(task_id)
    except (ReportUnavailable, LegacyUnavailable) as error:
        raise HTTPException(error.status_code, str(error)) from error
    except MySQLError as error:
        raise HTTPException(503, "report metadata is unavailable") from error


@app.get("/api/tasks/{task_id}/report/download")
def download_report(task_id: str):
    path = Path(_report_path(task_id))
    if path.parent.name == "artifacts" and path.parent.parent.parent.name == "attempts":
        return download_published_report(task_id)
    return FileResponse(path, filename=f"{task_id}_report.json",
                        media_type="application/json")


@app.get("/api/tasks/{task_id}/sample")
def get_sample(task_id: str, n: int = 5):
    rep = _report_body(task_id)
    return rep.get("disposition", {})


@app.get("/api/registry")
def get_registry():
    reg = os.path.join(OUT_ROOT, "registry.json")
    if not os.path.exists(reg):
        return []
    with open(reg, encoding="utf-8") as f:
        return JSONResponse(json.load(f))


@app.get("/api/tasks/{task_id}/transcript")
def get_transcript(task_id: str):
    """回放历史任务的对话(用户/助手/工具提示气泡),服务重启后仍可用。"""
    return JSONResponse(loop.read_transcript(task_id))


@app.post("/api/tasks/{task_id}/ask")
def ask(task_id: str, body: MsgBody):
    r = loop.ask_followup(task_id, body.message)
    if r.get("error"):
        raise HTTPException(r.get('status_code',503), r["error"])
    return r


@app.get("/", include_in_schema=False)
def business_console():
    return FileResponse(Path(WEB_DIR) / "governance.html")


if os.path.isdir(WEB_DIR):
    app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
