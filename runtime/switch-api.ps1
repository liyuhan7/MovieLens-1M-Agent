param(
    [Parameter(Mandatory=$true)][ValidatePattern('^sha256:[a-f0-9]{64}$')][string]$ImageId,
    [Parameter(Mandatory=$true)][ValidatePattern('^[A-Za-z0-9_-]{1,80}$')][string]$Owner,
    [Parameter(Mandatory=$true)][long]$ExpectedRevision,
    [Parameter(Mandatory=$true)][ValidateNotNullOrEmpty()][string]$Reason,
    [switch]$WithHistory
)
$ErrorActionPreference = 'Stop'
$runtimePath = $PSScriptRoot
$projectPath = Split-Path -Parent $runtimePath
$credentialFile = Join-Path $runtimePath '.env'
$pythonPath = Join-Path $projectPath '.venv/Scripts/python.exe'
$composePath = Join-Path $runtimePath 'compose.governance.yaml'
$composeArguments = @('compose','--env-file',$credentialFile,'-f',$composePath)
if ($WithHistory) { $composeArguments += @('-f',(Join-Path $runtimePath 'compose.history.yaml')) }
if ($env:ML_API_IMAGE -and $env:ML_API_IMAGE -ne $ImageId) { throw 'Environment ML_API_IMAGE overrides the requested image; remove the override first' }
if (-not (Test-Path -LiteralPath $credentialFile) -or -not (Test-Path -LiteralPath $pythonPath)) { throw 'Runtime credentials and local Python are required' }
$metadata = & docker image inspect $ImageId --format '{{json .Config.Labels}}'
if ($LASTEXITCODE -ne 0) { throw 'Target API image is unavailable locally' }
$labels = $metadata | ConvertFrom-Json
if ($labels.'ml.governance.schema' -ne '5' -or $labels.'ml.governance.admission-control' -ne '1') { throw 'Target image must support governance schema 5 and durable admission control' }
Push-Location -LiteralPath $projectPath
try {
    $snapshotText = & $pythonPath -m governance.cli maintenance-status
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect maintenance state' }
    $snapshot = $snapshotText | ConvertFrom-Json
    if ($snapshot.control.revision -ne $ExpectedRevision -or $snapshot.control.admission_open -or $snapshot.control.owner -ne $Owner) { throw 'Begin and drain a maintenance window with this owner/revision first' }
    if (-not $snapshot.ready_for_switch) { throw 'In-flight Attempts, occupied slot or unfinished publication prevents switching' }
    $currentText = & docker inspect ml-governance-api --format '{{.Image}}'
    if ($LASTEXITCODE -ne 0) { throw 'Switch requires an existing API deployment; use compose up for first deployment' }
    $previousImage = $currentText.Trim()
    $lines = @(Get-Content -LiteralPath $credentialFile | Where-Object { $_ -notmatch '^ML_API_IMAGE=' })
    $operationId = [guid]::NewGuid().ToString('N')
    $recordDirectory = Join-Path $projectPath "outputs/service-switches/$operationId"
    New-Item -ItemType Directory -Path $recordDirectory | Out-Null
    $record = @{previous_image=$previousImage;target_image=$ImageId;owner=$Owner;revision=$ExpectedRevision;reason=$Reason;status='PREPARED';admission_remains_closed=$true}
    $recordPath = Join-Path $recordDirectory 'switch.json'
    $record | ConvertTo-Json | Set-Content -LiteralPath $recordPath -Encoding utf8
    $reservationText = & $pythonPath -m governance.cli maintenance-switch-begin --owner $Owner --switch-id $operationId --expected-revision $ExpectedRevision
    if ($LASTEXITCODE -ne 0) { throw 'Cannot reserve the drained maintenance window' }
    $reservation = $reservationText | ConvertFrom-Json
    $record.reserved_revision = $reservation.revision
    $record.switch_id = $operationId
    $record.status = 'RESERVED_ADMISSION_CLOSED'
    $record | ConvertTo-Json | Set-Content -LiteralPath $recordPath -Encoding utf8
    & docker @composeArguments --profile compute --profile worker stop worker
    if ($LASTEXITCODE -ne 0) { throw 'Cannot stop the drained worker' }
    $lines += "ML_API_IMAGE=$ImageId"
    [System.IO.File]::WriteAllLines($credentialFile,$lines,(New-Object System.Text.UTF8Encoding($false)))
    & docker @composeArguments --profile api up -d --no-deps --wait api
    if ($LASTEXITCODE -ne 0) {
        $record.status = 'FAILED_ADMISSION_CLOSED'
        $record | ConvertTo-Json | Set-Content -LiteralPath $recordPath -Encoding utf8
        throw "API switch failed. Admission remains closed; previous image is recorded at $recordPath"
    }
    $actual = (& docker inspect ml-governance-api --format '{{.Image}}').Trim()
    if ($LASTEXITCODE -ne 0 -or $actual -ne $ImageId) { throw 'Running API image differs from requested identity' }
    $finishedText = & $pythonPath -m governance.cli maintenance-switch-end --owner $Owner --switch-id $operationId --expected-revision $reservation.revision
    if ($LASTEXITCODE -ne 0) { throw 'API switched but maintenance reservation requires reconciliation' }
    $finished = $finishedText | ConvertFrom-Json
    $record.final_revision = $finished.revision
    $record.status = 'SWITCHED_ADMISSION_CLOSED'
    $record | ConvertTo-Json | Set-Content -LiteralPath $recordPath -Encoding utf8
    Write-Host "API switched; worker stopped and admission closed. Inspect readiness, then explicitly resume. Record: $recordPath"
} finally { Pop-Location }
