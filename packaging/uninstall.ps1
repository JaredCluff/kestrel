<#
.SYNOPSIS
    Remove kestrel-hub.exe and kestrel-agent.exe from <Prefix>\bin.

.DESCRIPTION
    Mirrors install.ps1. This script does NOT clear Windows Credential
    Manager entries or delete kestrel.toml. Run `kestrel-hub unenroll` /
    `kestrel-agent unenroll` first if you want a clean wipe.

.PARAMETER Prefix
    Install root used at install time. Defaults to
    %LOCALAPPDATA%\Programs\Kestrel.

.EXAMPLE
    .\uninstall.ps1
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

$removed = 0
foreach ($bin in @('kestrel-hub.exe', 'kestrel-agent.exe')) {
    $target = Join-Path $binDir $bin
    if (Test-Path $target) {
        Remove-Item -Force $target
        Write-Host ">> removed $target"
        $removed++
    }
}
if ($removed -eq 0) {
    Write-Host "nothing to remove from $binDir"
}
