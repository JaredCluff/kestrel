<#
.SYNOPSIS
    Build kestrel release binaries and assemble a distributable .zip.

.DESCRIPTION
    Runs on Windows. macOS/Linux users: use packaging/build-release.sh instead.
    Produces:
      dist\kestrel-<version>-<target>\         (staging dir)
      dist\kestrel-<version>-<target>.zip      (final archive)

.PARAMETER Target
    Rust target triple to build for. Defaults to the host triple as reported
    by rustc (typically x86_64-pc-windows-msvc on stock Windows).

.EXAMPLE
    .\packaging\build-release.ps1
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
    [string]$Target = ''
)

$ErrorActionPreference = 'Stop'

# PS 5.1 doesn't have $IsWindows; $env:OS is set to "Windows_NT" on every
# supported Windows. macOS/Linux pwsh users should run the .sh sibling.
if ($env:OS -ne 'Windows_NT') {
    throw "This script is for Windows. On macOS/Linux run packaging/build-release.sh."
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot '..')
Set-Location $repoRoot

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    throw "cargo not on PATH. Install via https://rustup.rs."
}

$hostTriple = ((& rustc -vV) | Where-Object { $_ -match '^host:' } |
    ForEach-Object { ($_ -split '\s+')[1] }) | Select-Object -First 1
if ([string]::IsNullOrEmpty($Target)) { $Target = $hostTriple }

# Read version straight from the hub crate's Cargo.toml; avoids needing
# python/jq for a stable single-line `version = "x.y.z"`.
$hubManifest = Join-Path $repoRoot 'crates\kestrel-hub\Cargo.toml'
$versionMatch = Select-String -Path $hubManifest -Pattern '^version\s*=' |
    Select-Object -First 1
if (-not $versionMatch) {
    throw "could not parse version from $hubManifest"
}
$version = ($versionMatch.Line -replace '.*"([^"]+)".*', '$1')

# Build openh264 from source rather than downloading Cisco's binary blob.
# nasm must be on PATH (see README "Building from source").
$env:OPENH264_FROM_SOURCE = '1'

Write-Host ">> building kestrel v$version for $Target"

$buildArgs = @('build', '--release', '-p', 'kestrel-hub', '-p', 'kestrel-agent')
if ($Target -ne $hostTriple) {
    $buildArgs += @('--target', $Target)
    $artifactDir = Join-Path $repoRoot "target\$Target\release"
} else {
    $artifactDir = Join-Path $repoRoot 'target\release'
}

& cargo @buildArgs
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$stageName = "kestrel-$version-$Target"
$stage = Join-Path $repoRoot "dist\$stageName"
if (Test-Path $stage) { Remove-Item -Recurse -Force $stage }
$null = New-Item -ItemType Directory -Path (Join-Path $stage 'bin') -Force

Copy-Item -Force (Join-Path $artifactDir 'kestrel-hub.exe')   (Join-Path $stage 'bin\kestrel-hub.exe')
Copy-Item -Force (Join-Path $artifactDir 'kestrel-agent.exe') (Join-Path $stage 'bin\kestrel-agent.exe')

Copy-Item -Force (Join-Path $PSScriptRoot 'install.ps1')   $stage
Copy-Item -Force (Join-Path $PSScriptRoot 'uninstall.ps1') $stage
Copy-Item -Force (Join-Path $repoRoot 'README.md')         $stage
Copy-Item -Force (Join-Path $repoRoot 'LICENSE')           $stage

# PS 5.1 doesn't have `Get-Date -AsUTC`; build the ISO-8601 string ourselves.
$builtAt = [DateTime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
$commit = try {
    (& git rev-parse --short HEAD 2>$null).Trim()
} catch {
    'unknown'
}
@"
kestrel $version
target  $Target
built   $builtAt
commit  $commit
"@ | Set-Content -Path (Join-Path $stage 'VERSION') -Encoding UTF8

# Push-Location dist before compressing so the zip's root entry is the
# staged folder name (matches the .tar.gz layout from build-release.sh).
$zipPath = Join-Path $repoRoot "dist\$stageName.zip"
if (Test-Path $zipPath) { Remove-Item -Force $zipPath }
Push-Location (Join-Path $repoRoot 'dist')
try {
    Compress-Archive -Path $stageName -DestinationPath $zipPath -Force
} finally {
    Pop-Location
}

Write-Host ""
Write-Host ">> done"
Write-Host "   staged:   $stage"
Write-Host "   zip:      $zipPath"
$sizeMB = (Get-Item $zipPath).Length / 1MB
Write-Host ("   size:     {0:N1} MB" -f $sizeMB)
