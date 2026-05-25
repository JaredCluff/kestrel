<#
.SYNOPSIS
    Copy kestrel-hub.exe and kestrel-agent.exe into <Prefix>\bin.

.DESCRIPTION
    Default install location is %LOCALAPPDATA%\Programs\Kestrel — per-user,
    no admin elevation needed. Override with -Prefix to install elsewhere
    (e.g. -Prefix 'C:\Program Files\Kestrel' requires an elevated PowerShell).

.PARAMETER Prefix
    Install root. The binaries land in <Prefix>\bin.

.EXAMPLE
    .\install.ps1
.EXAMPLE
    .\install.ps1 -Prefix 'C:\tools\kestrel'
#>
#Requires -Version 5.1
[CmdletBinding()]
param(
    [string]$Prefix = ''
)

$ErrorActionPreference = 'Stop'

if ([string]::IsNullOrEmpty($Prefix)) {
    $Prefix = Join-Path $env:LOCALAPPDATA 'Programs\Kestrel'
}
$binDir = Join-Path $Prefix 'bin'

$here = $PSScriptRoot
$srcHub   = Join-Path $here 'bin\kestrel-hub.exe'
$srcAgent = Join-Path $here 'bin\kestrel-agent.exe'

if (-not (Test-Path $srcHub) -or -not (Test-Path $srcAgent)) {
    throw "$here\bin is missing the kestrel binaries. Run this script from inside the unpacked zip."
}

$null = New-Item -ItemType Directory -Path $binDir -Force

Copy-Item -Force $srcHub   (Join-Path $binDir 'kestrel-hub.exe')
Copy-Item -Force $srcAgent (Join-Path $binDir 'kestrel-agent.exe')

Write-Host ">> installed:"
Write-Host "   $(Join-Path $binDir 'kestrel-hub.exe')"
Write-Host "   $(Join-Path $binDir 'kestrel-agent.exe')"

# Lowercase-compare against the user PATH so re-runs don't double-warn.
# Modifying PATH would be invasive; we just tell the user how to do it.
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (-not $userPath) { $userPath = '' }
$onPath = $userPath.ToLower().Split(';') -contains $binDir.ToLower()
if (-not $onPath) {
    Write-Host ''
    Write-Host "note: $binDir is not on your user PATH."
    Write-Host '      add it with (then open a new terminal):'
    Write-Host "        [Environment]::SetEnvironmentVariable('Path', '$binDir;' + [Environment]::GetEnvironmentVariable('Path','User'), 'User')"
}
