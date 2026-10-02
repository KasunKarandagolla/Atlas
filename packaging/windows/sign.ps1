param([Parameter(Mandatory = $true)][string]$Path)
$ErrorActionPreference = 'Stop'
if (-not $env:ATLAS_SIGNING_PFX_BASE64 -or -not $env:ATLAS_SIGNING_PASSWORD) {
    throw 'Protected build signing credentials unavailable'
}
$bytes = [Convert]::FromBase64String($env:ATLAS_SIGNING_PFX_BASE64)
$certificate = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2
try {
    $flags = [System.Security.Cryptography.X509Certificates.X509KeyStorageFlags]::EphemeralKeySet
    $certificate.Import($bytes, $env:ATLAS_SIGNING_PASSWORD, $flags)
    if (-not $certificate.HasPrivateKey) { throw 'Signing certificate has no private key' }
    $result = Set-AuthenticodeSignature -LiteralPath $Path -Certificate $certificate -HashAlgorithm SHA256 `
        -TimestampServer 'https://timestamp.digicert.com'
    if ($result.Status -ne 'Valid') { throw 'Authenticode signing failed' }
} finally {
    $certificate.Dispose()
    [Array]::Clear($bytes, 0, $bytes.Length)
}
