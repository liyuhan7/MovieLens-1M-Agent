# -*- coding: utf-8 -*-
"""迭代一 Agent 服务(HTTP 壳)。

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

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

import loop

BASE = os.path.dirname(os.path.abspath(__file__))
PROJECT = os.path.dirname(BASE)
OUT_ROOT = os.path.join(PROJECT, "outputs")
WEB_DIR = os.path.join(PROJECT, "web")

app = FastAPI(title="MovieLens 数据治理 Agent(迭代一 · LLM Agent)")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


class MsgBody(BaseModel):
    message: str


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
    # 已完成任务直接用其报告;运行中/未知 id 则按登记表回溯最新成功报告
    known = os.path.exists(os.path.join(OUT_ROOT, task_id, "report.json")) if task_id else False
    try:
        return loop._report_path(task_id if known else None)
    except FileNotFoundError:
        raise HTTPException(404, "report not generated yet")


@app.get("/api/tasks/{task_id}/report")
def get_report(task_id: str):
    with open(_report_path(task_id), encoding="utf-8") as f:
        return JSONResponse(json.load(f))


@app.get("/api/tasks/{task_id}/report/download")
def download_report(task_id: str):
    return FileResponse(_report_path(task_id), filename=f"{task_id}_report.json",
                        media_type="application/json")


@app.get("/api/tasks/{task_id}/sample")
def get_sample(task_id: str, n: int = 5):
    with open(_report_path(task_id), encoding="utf-8") as f:
        rep = json.load(f)
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
        raise HTTPException(503, r["error"])
    return r


if os.path.isdir(WEB_DIR):
    app.mount("/", StaticFiles(directory=WEB_DIR, html=True), name="web")
