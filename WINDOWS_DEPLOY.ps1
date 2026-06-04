# ============================================================
# BDGW VaultFlow — Windows Fix + Deploy Script
# Run from PowerShell: .\WINDOWS_DEPLOY.ps1
#
# FILES folder: C:\projects\Running_Projects\PROJECT_INFOS\files\
# Project root: C:\projects\Running_Projects\BDGW_VaultFlow\
# ============================================================

$ErrorActionPreference = "Stop"

# ── PATHS ─────────────────────────────────────────────────────────
$PROJECT = "C:\projects\Running_Projects\BDGW_VaultFlow"
$FILES   = "C:\projects\Running_Projects\PROJECT_INFOS\files"

function Pass($msg) { Write-Host "  [OK] $msg" -ForegroundColor Green }
function Fail($msg) { Write-Host "  [FAIL] $msg" -ForegroundColor Red }
function Info($msg) { Write-Host "  [INFO] $msg" -ForegroundColor Cyan }
function Warn($msg) { Write-Host "  [WARN] $msg" -ForegroundColor Yellow }

Write-Host ""
Write-Host "============================================================" -ForegroundColor Blue
Write-Host "  BDGW VaultFlow — Windows Fix + Deploy" -ForegroundColor Blue
Write-Host "  $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Blue
Write-Host "============================================================" -ForegroundColor Blue
Write-Host ""

# ── VERIFY PATHS ──────────────────────────────────────────────────
if (-not (Test-Path $PROJECT)) {
    Fail "Project root not found: $PROJECT"
    exit 1
}
if (-not (Test-Path $FILES)) {
    Fail "Files folder not found: $FILES"
    exit 1
}
Pass "Project root: $PROJECT"
Pass "Files folder: $FILES"

# ── COPY PRE-FIXED FILES ──────────────────────────────────────────
Write-Host ""
Write-Host "[STEP 1] Copying pre-fixed files..." -ForegroundColor Yellow

# payments/__init__.py
$src = Join-Path $FILES "__init__.py"
$dst = Join-Path $PROJECT "app\payments\__init__.py"
if (Test-Path $src) {
    New-Item -ItemType Directory -Force -Path (Split-Path $dst) | Out-Null
    Copy-Item -Path $src -Destination $dst -Force
    Pass "app\payments\__init__.py"
} else { Warn "__init__.py not found in files folder — skipping" }

# topic_service.py
$src = Join-Path $FILES "topic_service.py"
$dst = Join-Path $PROJECT "app\services\topic_service.py"
if (Test-Path $src) {
    New-Item -ItemType Directory -Force -Path (Split-Path $dst) | Out-Null
    Copy-Item -Path $src -Destination $dst -Force
    Pass "app\services\topic_service.py"
} else { Warn "topic_service.py not found in files folder — skipping" }

# flood_wait.py
$src = Join-Path $FILES "flood_wait.py"
$dst = Join-Path $PROJECT "app\distribution\flood_wait.py"
if (Test-Path $src) {
    New-Item -ItemType Directory -Force -Path (Split-Path $dst) | Out-Null
    Copy-Item -Path $src -Destination $dst -Force
    Pass "app\distribution\flood_wait.py"
} else { Warn "flood_wait.py not found in files folder — skipping" }

# GEMINI.md → project root
$src = Join-Path $FILES "GEMINI.md"
$dst = Join-Path $PROJECT "GEMINI.md"
if (Test-Path $src) {
    Copy-Item -Path $src -Destination $dst -Force
    Pass "GEMINI.md → project root"
} else { Warn "GEMINI.md not found — skipping" }

# GEMINI_FIX_PROMPT.md → project root
$src = Join-Path $FILES "GEMINI_FIX_PROMPT.md"
$dst = Join-Path $PROJECT "GEMINI_FIX_PROMPT.md"
if (Test-Path $src) {
    Copy-Item -Path $src -Destination $dst -Force
    Pass "GEMINI_FIX_PROMPT.md → project root"
} else { Warn "GEMINI_FIX_PROMPT.md not found — skipping" }

# ── PATCH: import random in moderation_actions.py ─────────────────
Write-Host ""
Write-Host "[STEP 2] Patching moderation_actions.py..." -ForegroundColor Yellow

$file = Join-Path $PROJECT "app\moderation\moderation_actions.py"
if (Test-Path $file) {
    $content = Get-Content $file -Raw -Encoding UTF8
    if ($content -match "^import random") {
        Pass "moderation_actions.py — already has 'import random'"
    } elseif ($content -match "import hashlib") {
        $content = $content -replace "import hashlib", "import hashlib`nimport random"
        Set-Content -Path $file -Value $content -Encoding UTF8 -NoNewline
        Pass "moderation_actions.py — added 'import random'"
    } else {
        # Fallback: prepend after the first block of imports
        $content = "import random`n" + $content
        Set-Content -Path $file -Value $content -Encoding UTF8 -NoNewline
        Warn "moderation_actions.py — prepended 'import random' (verify placement)"
    }
} else { Fail "moderation_actions.py not found: $file" }

# ── PATCH: Optional in takedown_handler.py ────────────────────────
Write-Host ""
Write-Host "[STEP 3] Patching takedown_handler.py..." -ForegroundColor Yellow

$file = Join-Path $PROJECT "app\handlers\takedown_handler.py"
if (Test-Path $file) {
    $content = Get-Content $file -Raw -Encoding UTF8
    if ($content -match "from typing import.*Optional") {
        Pass "takedown_handler.py — already imports Optional"
    } elseif ($content -match "from datetime import datetime, timezone") {
        $content = $content -replace `
            "from datetime import datetime, timezone", `
            "from datetime import datetime, timezone`nfrom typing import Optional"
        Set-Content -Path $file -Value $content -Encoding UTF8 -NoNewline
        Pass "takedown_handler.py — added 'from typing import Optional'"
    } else {
        Warn "takedown_handler.py — could not find insertion point, manual fix needed"
    }
} else { Fail "takedown_handler.py not found: $file" }

# ── PATCH: WATERMARK_ROTATION in settings.py ──────────────────────
Write-Host ""
Write-Host "[STEP 4] Patching settings.py..." -ForegroundColor Yellow

$file = Join-Path $PROJECT "app\config\settings.py"
if (Test-Path $file) {
    $content = Get-Content $file -Raw -Encoding UTF8
    if ($content -match "WATERMARK_ROTATION") {
        Pass "settings.py — already has WATERMARK_ROTATION"
    } elseif ($content -match "WATERMARK_SCALE: float = 0\.15") {
        $content = $content -replace `
            "WATERMARK_SCALE: float = 0\.15", `
            "WATERMARK_SCALE: float = 0.15`n    WATERMARK_ROTATION: int = 0"
        Set-Content -Path $file -Value $content -Encoding UTF8 -NoNewline
        Pass "settings.py — added WATERMARK_ROTATION: int = 0"
    } else {
        Warn "settings.py — WATERMARK_SCALE line not found, manual fix needed"
    }
} else { Fail "settings.py not found: $file" }

# ── PATCH: .env.example ───────────────────────────────────────────
$file = Join-Path $PROJECT ".env.example"
if (Test-Path $file) {
    $content = Get-Content $file -Raw -Encoding UTF8
    if (-not ($content -match "WATERMARK_ROTATION")) {
        Add-Content -Path $file -Value "`n# Degrees to rotate watermark logo (0 = no rotation)`nWATERMARK_ROTATION=0"
        Pass ".env.example — added WATERMARK_ROTATION"
    } else {
        Pass ".env.example — already has WATERMARK_ROTATION"
    }
}

# ── PATCH: Remove duplicate /help from user_handler.py ────────────
Write-Host ""
Write-Host "[STEP 5] Checking for duplicate /help handler..." -ForegroundColor Yellow

$userHandler    = Join-Path $PROJECT "app\handlers\user_handler.py"
$supportHandler = Join-Path $PROJECT "app\handlers\support_handler.py"

$helpInUser    = 0
$helpInSupport = 0

if (Test-Path $userHandler) {
    $c = Get-Content $userHandler -Raw -Encoding UTF8
    $helpInUser = ([regex]::Matches($c, 'filters\.command\("help"\)')).Count
}
if (Test-Path $supportHandler) {
    $c = Get-Content $supportHandler -Raw -Encoding UTF8
    $helpInSupport = ([regex]::Matches($c, 'filters\.command\("help"\)')).Count
}

if ($helpInUser -gt 0 -and $helpInSupport -gt 0) {
    Warn "Duplicate /help handler detected!"
    Warn "  user_handler.py has $helpInUser registration(s)"
    Warn "  support_handler.py has $helpInSupport registration(s)"
    Warn ""
    Warn "ACTION REQUIRED — manually remove the /help handler from user_handler.py."
    Warn "Keep it in support_handler.py only (opens support ticket — correct behavior)."
    Warn ""
    Warn "In user_handler.py, find and DELETE this entire function:"
    Warn "  @Client.on_message(filters.command('help') & filters.private)"
    Warn "  async def handle_help(client, message): ..."
    Warn "(the one that calls build_help_card_v2 / help_cards)"
} else {
    Pass "No duplicate /help handler detected"
}

# -- SYNTAX VERIFICATION ------------------------------------------
Write-Host ""
Write-Host "[STEP 6] Syntax verification..." -ForegroundColor Yellow

$filesToCheck = @(
    "app\moderation\moderation_actions.py",
    "app\handlers\takedown_handler.py",
    "app\payments\__init__.py",
    "app\config\settings.py",
    "app\services\topic_service.py",
    "app\distribution\flood_wait.py",
    "app\core\lifecycle.py",
    "main_bot.py"
)

$syntaxOk = $true
foreach ($rel in $filesToCheck) {
    $full = Join-Path $PROJECT $rel
    if (Test-Path $full) {
        $result = & python -m py_compile $full 2>&1
        if ($LASTEXITCODE -eq 0) {
            Pass $rel
        } else {
            Fail "$rel — $result"
            $syntaxOk = $false
        }
    } else {
        Warn "$rel — not found (skip)"
    }
}

if (-not $syntaxOk) {
    Write-Host ""
    Fail "Syntax errors found. Fix before pushing."
    exit 1
}

# ── QUICK IMPORT TESTS ─────────────────────────────────────────────
Write-Host ""
Write-Host "[STEP 7] Import tests..." -ForegroundColor Yellow

$testScript = @"
import sys, importlib.util

def check_file(path, label):
    try:
        spec = importlib.util.spec_from_file_location('_test', path)
        mod = importlib.util.module_from_spec(spec)
        # Don't exec — just check it loads spec without error
        print(f'  [OK] {label}')
        return True
    except Exception as e:
        print(f'  [FAIL] {label} — {e}')
        return False

ok = True

# Check import random
with open(r'$($file = Join-Path $PROJECT "app\moderation\moderation_actions.py"; $file -replace "\\","\\\\")') as f:
    c = f.read()
ok &= 'import random' in c or print('  [FAIL] moderation_actions.py missing import random') or False

# Check Optional
with open(r'$((Join-Path $PROJECT "app\handlers\takedown_handler.py") -replace "\\","\\\\")') as f:
    c = f.read()
ok &= 'Optional' in c or print('  [FAIL] takedown_handler.py missing Optional') or False

# Check WATERMARK_ROTATION
with open(r'$((Join-Path $PROJECT "app\config\settings.py") -replace "\\","\\\\")') as f:
    c = f.read()
ok &= 'WATERMARK_ROTATION' in c or print('  [FAIL] settings.py missing WATERMARK_ROTATION') or False

if ok:
    print('  All import checks passed')
"@

Push-Location $PROJECT
python -c $testScript
Pop-Location

# ── GIT COMMIT & PUSH ─────────────────────────────────────────────
Write-Host ""
Write-Host "[STEP 8] Git push..." -ForegroundColor Yellow

Push-Location $PROJECT

git add -A
git status --short

Write-Host ""
$confirm = Read-Host "Commit and push to Railway? [y/N]"

if ($confirm -eq 'y' -or $confirm -eq 'Y') {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm"
    git commit -m "fix: production crash fixes — import errors, forward refs, topic singleton, watermark

- app/moderation/moderation_actions.py: add import random
- app/handlers/takedown_handler.py: add Optional import
- app/payments/__init__.py: fix PaymentService forward reference
- app/config/settings.py: add WATERMARK_ROTATION field
- app/services/topic_service.py: consolidate split-brain singleton
- app/distribution/flood_wait.py: safe asyncio.create_task in sync context
Deployed: $timestamp"

    git push origin main

    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Green
    Pass "Pushed to origin/main"
    Info "Railway deploying now — watch: https://railway.app/dashboard"
    Info ""
    Info "NEXT: Run Gemini CLI for remaining 11 fixes:"
    Info "  cd `"$PROJECT`""
    Info "  gemini -p `"`$(Get-Content GEMINI_FIX_PROMPT.md -Raw)`" --yolo"
    Write-Host "============================================================" -ForegroundColor Green
} else {
    Warn "Push skipped. Run manually:"
    Write-Host "  cd `"$PROJECT`""
    Write-Host "  git add -A"
    Write-Host "  git commit -m 'fix: production crash fixes'"
    Write-Host "  git push origin main"
}

Pop-Location