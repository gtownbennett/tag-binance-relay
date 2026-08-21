$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Security
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$workspaceRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$credentialPath = Join-Path $workspaceRoot 'local_credentials\tagnext-render-api-key.dpapi'
$outputDirectory = Join-Path (Split-Path $PSScriptRoot -Parent) 'outputs\rc4'
$outputPath = Join-Path $outputDirectory 'render-inventory-sanitized-latest.json'

function Read-RenderApiKey {
    $encoded = [IO.File]::ReadAllText($credentialPath)
    if (-not $encoded.StartsWith('dpapi-v1:')) { throw 'Unsupported credential envelope.' }
    $protectedBytes = [Convert]::FromBase64String($encoded.Substring(9))
    $entropy = [Text.Encoding]::UTF8.GetBytes('TAGneXt Render API key v1')
    $plainBytes = $null
    try {
        $plainBytes = [Security.Cryptography.ProtectedData]::Unprotect(
            $protectedBytes,
            $entropy,
            [Security.Cryptography.DataProtectionScope]::CurrentUser
        )
        return [Text.Encoding]::UTF8.GetString($plainBytes)
    }
    finally {
        if ($plainBytes) { [Array]::Clear($plainBytes, 0, $plainBytes.Length) }
        if ($entropy) { [Array]::Clear($entropy, 0, $entropy.Length) }
        if ($protectedBytes) { [Array]::Clear($protectedBytes, 0, $protectedBytes.Length) }
    }
}

function Invoke-RenderGet {
    param([string]$Path, [string]$ApiKey)
    Invoke-RestMethod -Uri ("https://api.render.com$Path") `
        -Headers @{ Authorization = "Bearer $ApiKey"; Accept = 'application/json' } `
        -Method Get -TimeoutSec 30
}

$apiKey = Read-RenderApiKey
try {
    $ownersRaw = Invoke-RenderGet -Path '/v1/owners?limit=100' -ApiKey $apiKey
    $servicesRaw = Invoke-RenderGet -Path '/v1/services?limit=100&includePreviews=false' -ApiKey $apiKey
    $postgresRaw = Invoke-RenderGet -Path '/v1/postgres?limit=100&includeReplicas=false' -ApiKey $apiKey

    $owners = @(foreach ($ownerEntry in $ownersRaw) {
        $owner = if ($ownerEntry.owner) { $ownerEntry.owner } else { $ownerEntry }
        [ordered]@{ id = $owner.id; name = $owner.name; type = $owner.type }
    })

    $services = @(foreach ($serviceEntry in $servicesRaw) {
        $service = if ($serviceEntry.service) { $serviceEntry.service } else { $serviceEntry }
        $envKeys = @()
        if ($service.id -and $service.name -match '(?i)tag|terminal|challenger') {
            try {
                $envRaw = Invoke-RenderGet -Path ("/v1/services/{0}/env-vars?limit=100" -f $service.id) -ApiKey $apiKey
                $envKeys = @(foreach ($envEntry in $envRaw) {
                    $envVar = if ($envEntry.envVar) { $envEntry.envVar } else { $envEntry }
                    if ($envVar.key) { $envVar.key }
                }) | Sort-Object -Unique
            }
            catch {
                $envKeys = @('[unavailable]')
            }
        }
        [ordered]@{
            id = $service.id
            name = $service.name
            type = $service.type
            ownerId = $service.ownerId
            repo = $service.repo
            branch = $service.branch
            rootDir = $service.rootDir
            autoDeploy = $service.autoDeploy
            suspended = $service.suspended
            createdAt = $service.createdAt
            updatedAt = $service.updatedAt
            url = $service.serviceDetails.url
            runtime = $service.serviceDetails.runtime
            region = $service.serviceDetails.region
            plan = $service.serviceDetails.plan
            envVarKeys = $envKeys
        }
    })

    $postgresInstances = @(foreach ($postgresEntry in $postgresRaw) {
        $postgres = if ($postgresEntry.postgres) { $postgresEntry.postgres } else { $postgresEntry }
        [ordered]@{
            id = $postgres.id
            name = $postgres.name
            ownerId = $postgres.ownerId
            status = $postgres.status
            suspended = $postgres.suspended
            createdAt = $postgres.createdAt
            updatedAt = $postgres.updatedAt
            region = $postgres.region
            plan = $postgres.plan
            version = $postgres.version
        }
    })

    $document = [ordered]@{
        schemaVersion = 'tagnext-render-inventory-sanitized-v1'
        checkedAt = [DateTime]::UtcNow.ToString('o')
        apiKeyValidated = $true
        secretValuesIncluded = $false
        owners = $owners
        services = $services
        postgresInstances = $postgresInstances
    }
    New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
    [IO.File]::WriteAllText($outputPath, ($document | ConvertTo-Json -Depth 8))
    [pscustomobject]@{
        apiKeyValidated = $true
        ownerCount = $owners.Count
        serviceCount = $services.Count
        postgresCount = $postgresInstances.Count
        relevantServices = @($services | Where-Object { $_.name -match '(?i)tag|terminal|challenger' } | ForEach-Object { $_.name })
        output = $outputPath
    } | ConvertTo-Json -Depth 4 -Compress
}
finally {
    $apiKey = $null
}
