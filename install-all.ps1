$ErrorActionPreference = "Stop"
$MinimumPythonVersion = [version]"3.11"

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Test-CommandExists {
    param([string]$Name)
    return $null -ne (Get-Command $Name -ErrorAction SilentlyContinue)
}

function Get-PythonCommandInfo {
    param(
        [Parameter(Mandatory = $true)][string]$Command,
        [string[]]$Args = @()
    )

    try {
        $details = & $Command @Args -c "import sys; print(sys.executable); print('.'.join(map(str, sys.version_info[:3])))"
        if ($LASTEXITCODE -ne 0 -or -not $details -or $details.Count -lt 2) {
            return $null
        }

        $path = [string]$details[0]
        $versionText = [string]$details[1]
        $version = [version]$versionText.Trim()

        return [pscustomobject]@{
            Path = $path.Trim()
            Version = $version
        }
    }
    catch {
        return $null
    }
}

function Get-BestInstalledPython {
    $candidates = @()

    if (Test-CommandExists "py") {
        $info = Get-PythonCommandInfo -Command "py" -Args @("-3")
        if ($info) {
            $candidates += $info
        }
    }

    foreach ($name in @("python", "python3")) {
        if (-not (Test-CommandExists $name)) {
            continue
        }

        $info = Get-PythonCommandInfo -Command $name
        if ($info) {
            $candidates += $info
        }
    }

    if (-not $candidates) {
        return $null
    }

    $deduped = $candidates |
        Where-Object { $_ -and $_.Path } |
        Group-Object Path |
        ForEach-Object { $_.Group | Sort-Object Version -Descending | Select-Object -First 1 }

    return $deduped |
        Where-Object { $_.Version -ge $MinimumPythonVersion } |
        Sort-Object Version -Descending |
        Select-Object -First 1
}

function Get-LatestWingetPythonPackage {
    if (-not (Test-CommandExists "winget")) {
        return $null
    }

    try {
        $output = & winget search --id Python.Python --source winget --accept-source-agreements 2>$null
    }
    catch {
        return $null
    }

    $candidates = foreach ($line in $output) {
        if ($line -match "(Python\.Python\.3\.(\d+))") {
            $minor = [int]$Matches[2]
            if ($minor -ge 11) {
                [pscustomobject]@{
                    Id = $Matches[1]
                    Version = [version]"3.$minor"
                }
            }
        }
    }

    if (-not $candidates) {
        return $null
    }

    return $candidates |
        Group-Object Id |
        ForEach-Object { $_.Group | Select-Object -First 1 } |
        Sort-Object Version -Descending |
        Select-Object -First 1
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

Write-Step "Ensuring Python 3.11+ is available"
$pythonInfo = Get-BestInstalledPython
if (-not $pythonInfo) {
    $latestPackage = Get-LatestWingetPythonPackage
    if (-not $latestPackage) {
        throw "No installed Python 3.11+ was found, and winget could not resolve a current Python 3 package."
    }

    Ensure-WingetPackage -Id $latestPackage.Id -UserScope
    $pythonInfo = Get-BestInstalledPython
}

if (-not $pythonInfo) {
    throw "Python 3.11+ installation did not produce a usable python.exe."
}

$pythonExe = $pythonInfo.Path
Write-Host "Using Python: $pythonExe ($($pythonInfo.Version))"

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
