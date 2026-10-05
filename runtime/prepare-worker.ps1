$ErrorActionPreference = 'Stop'
$runtimePath = $PSScriptRoot
$credentialFile = Join-Path $runtimePath '.env'
if (-not (Test-Path -LiteralPath $credentialFile)) { throw 'Prepare MySQL and Hadoop first' }
$baseImageId = (& docker image inspect movielens-governance-hadoop:3.3.6 --format '{{.Id}}').Trim()
if ($LASTEXITCODE -ne 0) { throw 'Run prepare-hadoop.ps1 first' }
$basePinTag = 'movielens-governance-hadoop-base:' + $baseImageId.Substring(7)
& docker tag $baseImageId $basePinTag
if ($LASTEXITCODE -ne 0) { throw 'Cannot fix worker Hadoop base' }
$pythonBase = 'python:3.12-slim@sha256:dddfd7e07f9d15aeeca61529320492139d21cac7f0070c00609243e51e4e0016'
& docker build --build-arg "HADOOP_BASE=$basePinTag" --build-arg "PYTHON_BASE=$pythonBase" -t movielens-governance-worker:3.3.6 -f (Join-Path $runtimePath 'Dockerfile.worker') $runtimePath
if ($LASTEXITCODE -ne 0) { throw 'Worker image build failed' }
$imageId = (& docker image inspect movielens-governance-worker:3.3.6 --format '{{.Id}}').Trim()
$lines = @(Get-Content -LiteralPath $credentialFile | Where-Object { $_ -notmatch '^ML_WORKER_IMAGE=' })
$lines += "ML_WORKER_IMAGE=$imageId"
[System.IO.File]::WriteAllLines($credentialFile, $lines, (New-Object System.Text.UTF8Encoding($false)))
Write-Host 'Worker image ready; image identity fixed. Initialize/import before starting the queue worker.'
