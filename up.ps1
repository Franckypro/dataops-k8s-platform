# Lance toute la plateforme depuis PowerShell, en une commande : .\up.ps1
# Le travail est fait par scripts/up.sh (le même script qu'en CI), exécuté avec Git Bash.
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
& $bash "$PSScriptRoot/scripts/up.sh" @args
exit $LASTEXITCODE
