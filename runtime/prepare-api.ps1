$ErrorActionPreference = 'Stop'
$runtimePath = $PSScriptRoot
$projectPath = Split-Path -Parent $runtimePath
$credentialFile = Join-Path $runtimePath '.env'
if (-not (Test-Path -LiteralPath $credentialFile)) { throw 'Prepare MySQL and worker first' }
if (-not (Test-Path -LiteralPath (Join-Path $projectPath 'hadoop/build/iter1.jar'))) { throw 'Build the application JAR first' }
$workerImage = @(Get-Content -LiteralPath $credentialFile | Where-Object { $_ -match '^ML_WORKER_IMAGE=' })
if ($workerImage.Count -ne 1) { throw 'Worker image identity is required in runtime/.env' }
$workerId = $workerImage[0].Substring('ML_WORKER_IMAGE='.Length)
if ($workerId -notmatch '^sha256:[a-f0-9]{64}$') { throw 'Worker base must use an exact image ID' }
& docker image inspect $workerId --format '{{.Id}}' | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Pinned worker base is unavailable' }
& docker build --build-arg "WORKER_BASE=$workerId" -t movielens-governance-api:5 -f (Join-Path $runtimePath 'Dockerfile.api') $projectPath
if ($LASTEXITCODE -ne 0) { throw 'API image build failed' }
$apiId = (& docker image inspect movielens-governance-api:5 --format '{{.Id}}').Trim()
if ($LASTEXITCODE -ne 0 -or $apiId -notmatch '^sha256:[a-f0-9]{64}$') { throw 'API image identity unavailable' }
$lines = @(Get-Content -LiteralPath $credentialFile | Where-Object { $_ -notmatch '^ML_API_IMAGE=' })
$lines += "ML_API_IMAGE=$apiId"
[System.IO.File]::WriteAllLines($credentialFile,$lines,(New-Object System.Text.UTF8Encoding($false)))
Write-Host 'API image prepared. This command does not start or switch services.'
