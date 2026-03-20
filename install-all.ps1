$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Test-CommandExists {
    param([string]$Name)
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Get-Python311Path {
    $candidates = @(
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python311\python.exe"),
        (Join-Path $env:ProgramFiles "Python311\python.exe")
    )

    foreach ($candidate in $candidates) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }

    return $null
}

function Ensure-WingetPackage {
    param(
        [Parameter(Mandatory = $true)][string]$Id,
        [switch]$UserScope
    )

    if (-not (Test-CommandExists "winget")) {
        throw "winget is required but was not found."
    }

    $args = @(
        "install",
        "--id", $Id,
        "-e",
        "--source", "winget",
        "--accept-package-agreements",
        "--accept-source-agreements",
        "--disable-interactivity"
    )

    if ($UserScope) {
        $args += @("--scope", "user")
    }

    & winget @args
}

function Get-RepoRoot {
    $path = $PSScriptRoot
    if (-not $path) {
        $path = (Get-Location).Path
    }
    return $path
}

$repoRoot = Get-RepoRoot
Set-Location $repoRoot

Write-Step "Ensuring Python 3.11 is installed"
$pythonExe = Get-Python311Path
if (-not $pythonExe) {
    Ensure-WingetPackage -Id "Python.Python.3.11" -UserScope
    $pythonExe = Get-Python311Path
}

if (-not $pythonExe) {
    throw "Python 3.11 installation did not produce a usable python.exe."
}

Write-Host "Using Python: $pythonExe"

Write-Step "Creating virtual environment"
if (-not (Test-Path ".venv\Scripts\python.exe")) {
    & $pythonExe -m venv .venv
}

$venvPython = Join-Path $repoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    throw "Virtual environment was not created successfully."
}

Write-Step "Upgrading pip tooling"
& $venvPython -m pip install --upgrade pip setuptools wheel

Write-Step "Installing Python dependencies"
& $venvPython -m pip install -r requirements.txt -r requirements.optional.txt

Write-Step "Installing FFmpeg"
Ensure-WingetPackage -Id "Gyan.FFmpeg.Essentials"

Write-Step "Installing Tesseract OCR"
Ensure-WingetPackage -Id "tesseract-ocr.tesseract"

Write-Step "Warming sentence-transformers model cache"
$env:PYTHONIOENCODING = "utf-8"
& $venvPython -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2'); print('model-ready')"

Write-Step "Install completed"
Write-Host "Run the bot with:" -ForegroundColor Green
Write-Host "  `$env:PYTHONIOENCODING='utf-8'; .\.venv\Scripts\python.exe main.py"
Write-Host ""
Write-Host "The bot still needs runtime config in .env, including TELEGRAM_API_ID, TELEGRAM_API_HASH, and DESTINATION."
