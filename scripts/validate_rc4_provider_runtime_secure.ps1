$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Security

$backendRoot = Split-Path -Parent $PSScriptRoot
$credentialRoot = [IO.Path]::GetFullPath((Join-Path $backendRoot '..\local_credentials'))
$nodeRealPath = Join-Path $credentialRoot 'tagnext-nodereal-bnb-rpc-url.dpapi'
$coinalyzePath = Join-Path $credentialRoot 'tagnext-coinalyze-api-key.dpapi'
$outputPath = Join-Path $backendRoot 'outputs\rc4\provider-runtime-live-final-20260821.json'
$integrationOutputPath = Join-Path $backendRoot 'outputs\rc4\provider-runtime-integration-final-20260821.json'
$pythonPath = Join-Path $backendRoot '.venv\Scripts\python.exe'
$entropyText = 'TAGneXt provider credentials v1'

function Read-ProtectedValue {
    param([Parameter(Mandatory = $true)][string]$Path)

    $stored = [IO.File]::ReadAllText($Path).Trim()
    if (-not $stored.StartsWith('dpapi-v1:')) {
        throw "Unsupported protected credential format: $([IO.Path]::GetFileName($Path))"
    }
    $protectedBytes = [Convert]::FromBase64String($stored.Substring(9))
    $entropy = [Text.Encoding]::UTF8.GetBytes($entropyText)
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
        if ($protectedBytes) { [Array]::Clear($protectedBytes, 0, $protectedBytes.Length) }
        if ($entropy) { [Array]::Clear($entropy, 0, $entropy.Length) }
        $stored = $null
    }
}

if (-not (Test-Path -LiteralPath $nodeRealPath)) { throw 'Protected NodeReal credential is missing.' }
if (-not (Test-Path -LiteralPath $coinalyzePath)) { throw 'Protected Coinalyze credential is missing.' }
if (-not (Test-Path -LiteralPath $pythonPath)) { throw 'Backend virtual-environment Python is missing.' }

try {
    $env:NODEREAL_BNB_RPC_URL = Read-ProtectedValue -Path $nodeRealPath
    $env:COINALYZE_API_KEY = Read-ProtectedValue -Path $coinalyzePath
    $env:TAGNEXT_RUNTIME_MODE = 'unit_test'
    & $pythonPath (Join-Path $backendRoot 'scripts\validate_rc4_provider_credentials.py') `
        --output $outputPath --provider all
    if ($LASTEXITCODE -ne 0) { throw "Provider validation exited with code $LASTEXITCODE." }
    & $pythonPath (Join-Path $backendRoot 'scripts\validate_rc4_provider_runtime_integration.py') `
        --output $integrationOutputPath
    if ($LASTEXITCODE -ne 0) { throw "Provider runtime integration exited with code $LASTEXITCODE." }
}
finally {
    Remove-Item Env:NODEREAL_BNB_RPC_URL -ErrorAction SilentlyContinue
    Remove-Item Env:COINALYZE_API_KEY -ErrorAction SilentlyContinue
    Remove-Item Env:TAGNEXT_RUNTIME_MODE -ErrorAction SilentlyContinue
}
