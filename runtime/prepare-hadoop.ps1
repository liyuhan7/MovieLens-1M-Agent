$ErrorActionPreference = 'Stop'
$runtimePath = $PSScriptRoot
$credentialFile = Join-Path $runtimePath '.env'
if (-not (Test-Path -LiteralPath $credentialFile)) { throw 'Run prepare-governance.ps1 first' }
$baseImageId = (& docker image inspect movielens-hadoop:3.3.6 --format '{{.Id}}').Trim()
if ($LASTEXITCODE -ne 0) { throw 'Missing Hadoop base image; see runtime/README.md' }
$basePinTag = 'movielens-hadoop-base:' + $baseImageId.Substring(7)
& docker tag $baseImageId $basePinTag
if ($LASTEXITCODE -ne 0) { throw 'Cannot fix Hadoop base image tag' }
& docker build --build-arg "HADOOP_BASE=$basePinTag" -t movielens-governance-hadoop:3.3.6 -f (Join-Path $runtimePath 'Dockerfile.governance') $runtimePath
if ($LASTEXITCODE -ne 0) { throw 'Hadoop daemon image build failed' }
$imageId = (& docker image inspect movielens-governance-hadoop:3.3.6 --format '{{.Id}}').Trim()
$lines = @(Get-Content -LiteralPath $credentialFile | Where-Object { $_ -notmatch '^ML_HADOOP_IMAGE=' })
$lines += "ML_HADOOP_IMAGE=$imageId"
[System.IO.File]::WriteAllLines($credentialFile, $lines, (New-Object System.Text.UTF8Encoding($false)))
& docker compose --env-file $credentialFile -f (Join-Path $runtimePath 'compose.governance.yaml') --profile compute up -d --wait hadoop
if ($LASTEXITCODE -ne 0) { throw 'HDFS/YARN startup failed; inspect project container logs' }
Write-Host 'MovieLens single-node HDFS/YARN ready. Image identity fixed in runtime/.env.'
