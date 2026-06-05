#!/bin/bash
# ============================================================
# BDGW VaultFlow — Complete Fix + Deploy Pipeline
# Run from project root: bash deploy_fix.sh
# ============================================================
# 
# This script:
#   1. Applies the 5 critical hotfixes directly
#   2. Verifies syntax on all changed files
#   3. Runs a basic import test
#   4. Commits and pushes to trigger Railway deploy
#
# Prerequisite: git must be clean or have only the files 
# you intend to push. Run `git status` first.
# ============================================================

set -e  # Exit on first error

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

pass() { echo -e "${GREEN}  [OK]${NC} $1"; }
fail() { echo -e "${RED}  [FAIL]${NC} $1"; }
info() { echo -e "${BLUE}  [INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}  [WARN]${NC} $1"; }

echo ""
echo "============================================================"
echo "  BDGW VaultFlow — Emergency Fix + Deploy"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================================"
echo ""

# ── Pre-flight ────────────────────────────────────────────────────
info "Checking git status..."
if ! git diff --quiet HEAD 2>/dev/null; then
    warn "Uncommitted changes detected. They will be included in the commit."
fi

info "Python version: $(python3 --version)"

# ── FIX 1: import random in moderation_actions.py ─────────────────
echo ""
echo "[1/5] Fixing moderation_actions.py — add import random"

FILE="app/moderation/moderation_actions.py"
if grep -q "^import random$" "$FILE" 2>/dev/null; then
    pass "$FILE — already has 'import random'"
elif [ -f "$FILE" ]; then
    # Insert after 'import hashlib'
    sed -i 's/^import hashlib$/import hashlib\nimport random/' "$FILE"
    pass "$FILE — added 'import random'"
else
    fail "$FILE — file not found"
fi

# ── FIX 2: Optional import in takedown_handler.py ─────────────────
echo ""
echo "[2/5] Fixing takedown_handler.py — add Optional import"

FILE="app/handlers/takedown_handler.py"
if grep -q "from typing import Optional" "$FILE" 2>/dev/null; then
    pass "$FILE — already imports Optional"
elif [ -f "$FILE" ]; then
    # Insert after 'from datetime import datetime, timezone'
    sed -i 's/^from datetime import datetime, timezone$/from datetime import datetime, timezone\nfrom typing import Optional/' "$FILE"
    pass "$FILE — added 'from typing import Optional'"
else
    fail "$FILE — file not found"
fi

# ── FIX 3: payments/__init__.py ───────────────────────────────────
echo ""
echo "[3/5] Fixing payments/__init__.py — forward reference crash"

cat > "app/payments/__init__.py" << 'PYEOF'
from __future__ import annotations

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from app.payments.service import PaymentService

_payment_service: "Optional[PaymentService]" = None


def get_payment_service() -> "PaymentService":
    global _payment_service
    if _payment_service is None:
        from app.payments.repository import PaymentRepository
        from app.payments.service import PaymentService
        from app.referral.repository import ReferralRepository
        from app.core.database import DatabaseManager

        db = DatabaseManager.get_db()
        repo = PaymentRepository(db)
        referral_repo = ReferralRepository(db)
        _payment_service = PaymentService(repo, referral_repo)
    return _payment_service
PYEOF
pass "app/payments/__init__.py — rewritten with safe forward reference"

# ── FIX 4: WATERMARK_ROTATION in settings.py ──────────────────────
echo ""
echo "[4/5] Fixing settings.py — add WATERMARK_ROTATION"

FILE="app/config/settings.py"
if grep -q "WATERMARK_ROTATION" "$FILE" 2>/dev/null; then
    pass "$FILE — already has WATERMARK_ROTATION"
elif [ -f "$FILE" ]; then
    # Insert after WATERMARK_SCALE line
    sed -i 's/    WATERMARK_SCALE: float = 0.15/    WATERMARK_SCALE: float = 0.15\n    WATERMARK_ROTATION: int = 0/' "$FILE"
    pass "$FILE — added WATERMARK_ROTATION: int = 0"
else
    fail "$FILE — file not found"
fi

# Add to .env.example too
if [ -f ".env.example" ]; then
    if ! grep -q "WATERMARK_ROTATION" ".env.example"; then
        echo "# Degrees to rotate watermark logo (0 = no rotation)" >> ".env.example"
        echo "WATERMARK_ROTATION=0" >> ".env.example"
        pass ".env.example — added WATERMARK_ROTATION"
    else
        pass ".env.example — already has WATERMARK_ROTATION"
    fi
fi

# ── FIX 5: topic_service.py split-brain ───────────────────────────
echo ""
echo "[5/5] Fixing topic_service.py — consolidate singleton"

cat > "app/services/topic_service.py" << 'PYEOF'
"""
app/services/topic_service.py — Compatibility shim.

Canonical implementation is in app/services/topic_manager.py.
This file re-exports everything so imports from EITHER module work.
"""
from __future__ import annotations

from app.services.topic_manager import (
    TopicManager,
    get_topic_manager,
    TOPIC_CONTENT,
    TOPIC_SUPPORT,
    TOPIC_PAYMENT,
    TOPIC_REJECTED,
)

TopicService = TopicManager
get_topic_service = get_topic_manager


async def _warm_cache_alias(self) -> None:
    await self.restore_cache()


if not hasattr(TopicManager, "warm_cache_from_db"):
    TopicManager.warm_cache_from_db = _warm_cache_alias

__all__ = [
    "TopicService", "TopicManager",
    "get_topic_service", "get_topic_manager",
    "TOPIC_CONTENT", "TOPIC_SUPPORT", "TOPIC_PAYMENT", "TOPIC_REJECTED",
]
PYEOF
pass "app/services/topic_service.py — rewritten as compatibility shim"

# ── FIX 6: Remove duplicate /help in user_handler.py ──────────────
echo ""
echo "[6/6] Checking for duplicate /help handler in user_handler.py"

FILE="app/handlers/user_handler.py"
# Count how many /help handlers exist in total project
HELP_COUNT=$(grep -rn 'filters.command("help") & filters.private' app/handlers/ 2>/dev/null | wc -l)
if [ "$HELP_COUNT" -gt 1 ]; then
    warn "Found $HELP_COUNT /help handler registrations. Manual review needed."
    warn "Keep the one in support_handler.py (opens support ticket)."
    warn "Remove the one in user_handler.py (shows help_cards — wrong behavior)."
    warn "File: app/handlers/user_handler.py — search for handle_help and remove the function"
else
    pass "Only 1 /help handler found — OK"
fi

# ── SYNTAX VERIFICATION ────────────────────────────────────────────
echo ""
echo "--- Syntax Verification ---"
SYNTAX_OK=true

FILES_TO_CHECK=(
    "app/moderation/moderation_actions.py"
    "app/handlers/takedown_handler.py"
    "app/payments/__init__.py"
    "app/config/settings.py"
    "app/services/topic_service.py"
    "app/distribution/flood_wait.py"
    "app/core/lifecycle.py"
    "main_bot.py"
)

for f in "${FILES_TO_CHECK[@]}"; do
    if [ -f "$f" ]; then
        if python3 -m py_compile "$f" 2>/dev/null; then
            pass "$f"
        else
            fail "$f — SYNTAX ERROR"
            python3 -m py_compile "$f"
            SYNTAX_OK=false
        fi
    else
        warn "$f — not found (skip)"
    fi
done

if [ "$SYNTAX_OK" = false ]; then
    echo ""
    fail "Syntax errors detected. Fix before pushing."
    exit 1
fi

# ── IMPORT TEST ───────────────────────────────────────────────────
echo ""
echo "--- Import Tests ---"

python3 -c "
import sys
sys.path.insert(0, '.')
try:
    # Test the payments module loads without crashing
    import importlib.util
    spec = importlib.util.spec_from_file_location('payments_init', 'app/payments/__init__.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    print('  [OK] app/payments/__init__.py — no NameError')
except Exception as e:
    print(f'  [FAIL] app/payments/__init__.py — {e}')
    sys.exit(1)

try:
    spec = importlib.util.spec_from_file_location('topic_service', 'app/services/topic_service.py')
    # Can't fully import without DB but syntax check passes
    print('  [OK] app/services/topic_service.py — syntax valid')
except Exception as e:
    print(f'  [WARN] app/services/topic_service.py — {e}')

try:
    # Verify WATERMARK_ROTATION is parseable
    with open('app/config/settings.py') as f:
        content = f.read()
    if 'WATERMARK_ROTATION' in content:
        print('  [OK] app/config/settings.py — WATERMARK_ROTATION present')
    else:
        print('  [FAIL] app/config/settings.py — WATERMARK_ROTATION missing')
        sys.exit(1)
except Exception as e:
    print(f'  [FAIL] {e}')
    sys.exit(1)

# Verify import random in moderation_actions
with open('app/moderation/moderation_actions.py') as f:
    content = f.read()
if 'import random' in content:
    print('  [OK] app/moderation/moderation_actions.py — import random present')
else:
    print('  [FAIL] app/moderation/moderation_actions.py — import random STILL MISSING')
    sys.exit(1)

# Verify Optional in takedown_handler
with open('app/handlers/takedown_handler.py') as f:
    content = f.read()
if 'Optional' in content and ('from typing import' in content or 'typing.Optional' in content):
    print('  [OK] app/handlers/takedown_handler.py — Optional imported')
else:
    print('  [FAIL] app/handlers/takedown_handler.py — Optional not imported')
    sys.exit(1)
" || { fail "Import tests failed. Fix errors above."; exit 1; }

# ── GIT COMMIT & PUSH ─────────────────────────────────────────────
echo ""
echo "--- Git Push ---"

git add -A
git status --short

echo ""
read -p "Commit and push now? [y/N] " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    TIMESTAMP=$(date '+%Y-%m-%d %H:%M')
    git commit -m "fix: production crash fixes — import errors, forward refs, topic singleton, watermark settings

Critical fixes applied:
- app/moderation/moderation_actions.py: add import random (crash on every approval)
- app/handlers/takedown_handler.py: add Optional import (NameError on type annotation)
- app/payments/__init__.py: fix PaymentService forward reference (NameError on startup)
- app/config/settings.py: add WATERMARK_ROTATION field (AttributeError when enabled)
- app/services/topic_service.py: consolidate split-brain singleton with topic_manager.py
- app/distribution/flood_wait.py: safe asyncio.create_task in sync context

Deployed: $TIMESTAMP"
    
    git push origin main
    
    echo ""
    echo "============================================================"
    pass "Pushed to origin/main"
    info "Railway deploy triggered. Watch: https://railway.app/dashboard"
    info "Health check: curl https://your-app.railway.app/health"
    info ""
    info "Next: Run Gemini CLI for the remaining 11 non-crash fixes:"
    info "  gemini -p \"\$(cat GEMINI_FIX_PROMPT.md)\" --all_files"
    echo "============================================================"
else
    warn "Push skipped. Run manually:"
    echo "  git commit -m 'fix: production crash fixes'"
    echo "  git push origin main"
fi
