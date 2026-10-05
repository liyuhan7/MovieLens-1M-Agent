$ErrorActionPreference = "Stop"
$runtimePath = $PSScriptRoot
$credentialFile = Join-Path $runtimePath '.env'
if (-not (Test-Path -LiteralPath $credentialFile)) {
    function New-ProjectPassword {
        $randomBytes = New-Object byte[] 32
        $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
        try { $generator.GetBytes($randomBytes) } finally { $generator.Dispose() }
        return [System.BitConverter]::ToString($randomBytes).Replace('-', '').ToLowerInvariant()
    }
    $lines = @(
        'ML_MYSQL_HOST=127.0.0.1'
        'ML_MYSQL_PORT=13306'
        'ML_MYSQL_DATABASE=ml_governance'
        'ML_MYSQL_USER=ml_governance'
        "ML_MYSQL_PASSWORD=$(New-ProjectPassword)"
        "ML_MYSQL_ROOT_PASSWORD=$(New-ProjectPassword)"
    )
    [System.IO.File]::WriteAllLines($credentialFile, $lines, (New-Object System.Text.UTF8Encoding($false)))
}
& docker compose --env-file $credentialFile -f (Join-Path $runtimePath 'compose.governance.yaml') up -d --wait mysql
if ($LASTEXITCODE -ne 0) { throw 'MovieLens MySQL startup failed' }
Write-Host 'MovieLens MySQL ready. Credentials are stored in ignored runtime/.env.'
