$ErrorActionPreference = 'Stop'
& docker exec ml-governance-mysql sh -c 'MYSQL_PWD=$MYSQL_ROOT_PASSWORD mysql --user=root -e "CREATE DATABASE IF NOT EXISTS ml_governance_test CHARACTER SET utf8mb4"'
if ($LASTEXITCODE -ne 0) { throw 'Test database creation failed' }
& docker exec ml-governance-mysql sh -c 'MYSQL_PWD=$MYSQL_ROOT_PASSWORD mysql --user=root -e "GRANT ALL ON ml_governance_test.* TO ml_governance"'
if ($LASTEXITCODE -ne 0) { throw 'Test database permission setup failed' }
Write-Host 'Isolated ml_governance_test database ready; no databases removed.'
