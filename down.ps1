# Supprime le cluster local et ses données : .\down.ps1
$ErrorActionPreference = 'Stop'
$candidats = @(
    "$env:ProgramFiles\Git\bin\bash.exe",
    "${env:ProgramFiles(x86)}\Git\bin\bash.exe",
    "$env:LOCALAPPDATA\Programs\Git\bin\bash.exe"
)
$bash = $candidats | Where-Object { $_ -and (Test-Path $_) } | Select-Object -First 1
if (-not $bash) {
    Write-Host "Git Bash est introuvable. Installez Git for Windows : winget install --id Git.Git -e" -ForegroundColor Red
    exit 1
}
& $bash "$PSScriptRoot/scripts/down.sh" @args
exit $LASTEXITCODE
