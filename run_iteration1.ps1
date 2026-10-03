# -*- coding: utf-8 -*-
# 迭代一 一键启动/验证脚本(Windows PowerShell)。
#
# 用法:
#   powershell -ExecutionPolicy Bypass -File run_iteration1.ps1 build    # 编译 Hadoop jar(本机 JDK8)
#   powershell -ExecutionPolicy Bypass -File run_iteration1.ps1 pipeline # 全管线(Hadoop 清洗 + 双轮评分)
#   powershell -ExecutionPolicy Bypass -File run_iteration1.ps1 serve    # 启动 Agent + 前端(http://127.0.0.1:8000)
param(
    [Parameter(Position = 0)]
    [ValidateSet("build", "pipeline", "serve")]
    [string]$Action = "pipeline"
)
$ErrorActionPreference = "Stop"
$root = $PSScriptRoot                                      # 仓库根(ml-1m)
Set-Location $root

switch ($Action) {
    "build" {
        & "$root\hadoop\build.ps1"
        if ($LASTEXITCODE -ne 0) { throw "build failed" }
        Write-Host "`nHadoop jar 构建完成: $root\hadoop\build\iter1.jar"
    }
    "pipeline" {
        python "$root\pipeline\run_pipeline.py" --tag iteration1-run
    }
    "serve" {
        $env:PYTHONIOENCODING = "utf-8"
        python -m uvicorn --app-dir "$root\agent" server:app --host 127.0.0.1 --port 8000
    }
}
