param([Parameter(Mandatory = $true)][string]$ExecutionManifest)
$ErrorActionPreference = 'Stop'
$workspacePath = Split-Path -Parent $PSScriptRoot
$executionPath = (Resolve-Path -LiteralPath $ExecutionManifest).Path
$executionRecord = Get-Content -LiteralPath $executionPath -Raw -Encoding UTF8 | ConvertFrom-Json
if ($executionRecord.schema_version -ne 'execution-archive-v1') { throw 'Unsupported execution archive' }
$archiveIdentity = $executionRecord.execution_sha256
if ($archiveIdentity -notmatch '^[0-9a-f]{64}$') { throw 'Invalid execution archive identity' }
$imageIds = @($executionRecord.execution.images.ML_HADOOP_IMAGE, $executionRecord.execution.images.ML_WORKER_IMAGE)
$mysqlReference = 'mysql@sha256:6ea90827b1100f8f2ae306a539f86d2c264a26ed435a2a9f75551dd5c3aeb242'
$mysqlImageId = (& docker image inspect $mysqlReference --format '{{.Id}}').Trim()
if ($LASTEXITCODE -ne 0) { throw 'The pinned MySQL runtime image is unavailable' }
$imageIds += $mysqlImageId
foreach ($imageId in $imageIds) {
    if ($imageId -notmatch '^sha256:[0-9a-f]{64}$') { throw 'Image identity must be fixed' }
    $actualImageId = (& docker image inspect $imageId --format '{{.Id}}').Trim()
    if ($LASTEXITCODE -ne 0 -or $actualImageId -ne $imageId) { throw 'A fixed runtime image is unavailable' }
}
$archivePath = [System.IO.Path]::GetFullPath((Join-Path $workspacePath ('outputs/runtime-images/' + $archiveIdentity)))
$outputRoot = [System.IO.Path]::GetFullPath((Join-Path $workspacePath 'outputs'))
if (-not $archivePath.StartsWith($outputRoot + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw 'Image archive must stay within this workspace outputs directory'
}
New-Item -ItemType Directory -Force -Path $archivePath | Out-Null
$finalArchive = Join-Path $archivePath 'images.tar'
$metadataPath = Join-Path $archivePath 'manifest.json'
if (Test-Path -LiteralPath $finalArchive) {
    if (-not (Test-Path -LiteralPath $metadataPath)) { throw 'Existing archive lacks verification metadata; retained for diagnosis' }
    $existing = Get-Content -LiteralPath $metadataPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($existing.execution_sha256 -ne $archiveIdentity -or
        $existing.image_ids.Count -ne $imageIds.Count -or
        @($imageIds | Where-Object { $_ -notin $existing.image_ids }).Count -ne 0 -or
        (Get-FileHash -LiteralPath $finalArchive -Algorithm SHA256).Hash.ToLowerInvariant() -ne $existing.archive_sha256) {
        throw 'Existing image archive conflicts with fixed execution; retained without overwrite'
    }
    Write-Host ('Verified existing runtime archive: ' + $finalArchive)
    exit 0
}
$temporaryArchive = Join-Path $archivePath ('images.' + [guid]::NewGuid().ToString('N') + '.tar')
Write-Host 'Saving the fixed Hadoop, worker and MySQL images; no containers or volumes are changed.'
& docker image save --output $temporaryArchive @imageIds
if ($LASTEXITCODE -ne 0) { throw 'Image export failed; partial archive retained for diagnosis' }
$archiveHash = (Get-FileHash -LiteralPath $temporaryArchive -Algorithm SHA256).Hash.ToLowerInvariant()
$archiveBytes = (Get-Item -LiteralPath $temporaryArchive).Length
Move-Item -LiteralPath $temporaryArchive -Destination $finalArchive
$metadata = [ordered]@{
    schema_version = 'runtime-image-archive-v1'
    execution_sha256 = $archiveIdentity
    image_ids = $imageIds
    mysql_reference = $mysqlReference
    archive = 'images.tar'
    archive_sha256 = $archiveHash
    archive_bytes = $archiveBytes
    data_volumes_exported = $false
    credentials_exported = $false
}
[System.IO.File]::WriteAllText($metadataPath, ($metadata | ConvertTo-Json -Depth 8), [System.Text.UTF8Encoding]::new($false))
Write-Host ('Runtime images saved: ' + $metadataPath)
