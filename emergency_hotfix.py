#!/usr/bin/env python3
"""
BDGW VaultFlow — Emergency Hotfix Script
Run from project root: python emergency_hotfix.py

Applies 5 critical crash-fixes directly to files.
No questions asked. Run it, then git push.

Fixes:
  1. `import random` missing in moderation_actions.py
  2. `Optional` not imported in takedown_handler.py
  3. payments/__init__.py forward reference crash
  4. Duplicate /help handler in user_handler.py
  5. WATERMARK_ROTATION missing from settings.py
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent
FIXES = []

def fix_file(path_str, description, old_pattern, new_text, is_regex=False):
    path = ROOT / path_str
    if not path.exists():
        print(f"  [SKIP] {path_str} — file not found")
        return False
    
    content = path.read_text(encoding="utf-8")
    
    if is_regex:
        if not re.search(old_pattern, content):
            print(f"  [SKIP] {path_str} — pattern not found (may already be fixed)")
            return False
        new_content = re.sub(old_pattern, new_text, content, count=1)
    else:
        if old_pattern not in content:
            print(f"  [SKIP] {path_str} — string not found (may already be fixed)")
            return False
        new_content = content.replace(old_pattern, new_text, 1)
    
    path.write_text(new_content, encoding="utf-8")
    print(f"  [FIXED] {path_str} — {description}")
    FIXES.append(path_str)
    return True


def verify_syntax(path_str):
    import subprocess
    result = subprocess.run(
        [sys.executable, "-m", "py_compile", str(ROOT / path_str)],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print(f"  [OK] {path_str} — syntax valid")
        return True
    else:
        print(f"  [FAIL] {path_str} — {result.stderr.strip()}")
        return False


print("=" * 60)
print("BDGW VaultFlow — Emergency Hotfix")
print("=" * 60)

# ── FIX 1: import random in moderation_actions.py ─────────────────
print("\n[1/5] Adding 'import random' to moderation_actions.py...")
fix_file(
    "app/moderation/moderation_actions.py",
    "add import random",
    "import hashlib\n",
    "import hashlib\nimport random\n",
)

# ── FIX 2: Optional import in takedown_handler.py ─────────────────
print("\n[2/5] Adding 'Optional' to takedown_handler.py imports...")
fix_file(
    "app/handlers/takedown_handler.py",
    "add Optional to typing imports",
    "from datetime import datetime, timezone\n",
    "from datetime import datetime, timezone\nfrom typing import Optional\n",
)

# ── FIX 3: payments/__init__.py forward reference ─────────────────
print("\n[3/5] Fixing payments/__init__.py forward reference...")
payments_init = ROOT / "app/payments/__init__.py"
payments_init.write_text(
'''from __future__ import annotations

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
''',
    encoding="utf-8"
)
print("  [FIXED] app/payments/__init__.py — forward reference resolved")
FIXES.append("app/payments/__init__.py")

# ── FIX 4: Remove duplicate /help from user_handler.py ────────────
print("\n[4/5] Removing duplicate /help handler from user_handler.py...")
path = ROOT / "app/handlers/user_handler.py"
content = path.read_text(encoding="utf-8")

# Find and remove the handle_help function block (the help_cards version)
# We look for the function starting with @Client.on_message(filters.command("help")
# that contains "help_cards" (the wrong one) and remove it.
# The correct /help is in support_handler.py.
pattern = r'\n@Client\.on_message\(filters\.command\("help"\) & filters\.private\)\nasync def handle_help\(client.*?(?=\n@Client|\nclass |\Z)'
match = re.search(pattern, content, re.DOTALL)
if match:
    removed_block = match.group(0)
    if "help_cards" in removed_block or "build_help_card" in removed_block:
        new_content = content[:match.start()] + content[match.end():]
        path.write_text(new_content, encoding="utf-8")
        print("  [FIXED] user_handler.py — duplicate /help handler removed")
        FIXES.append("app/handlers/user_handler.py")
    else:
        print("  [SKIP] user_handler.py — /help handler found but not the duplicate version, manual review needed")
else:
    print("  [SKIP] user_handler.py — no /help handler found (may already be fixed)")

# ── FIX 5: WATERMARK_ROTATION in settings.py ──────────────────────
print("\n[5/5] Adding WATERMARK_ROTATION to settings.py...")
fix_file(
    "app/config/settings.py",
    "add WATERMARK_ROTATION field",
    "    WATERMARK_POSITION: str = \"BOTTOM_RIGHT\"\n    WATERMARK_OPACITY: float = 0.8\n    WATERMARK_SCALE: float = 0.15",
    "    WATERMARK_POSITION: str = \"BOTTOM_RIGHT\"\n    WATERMARK_OPACITY: float = 0.8\n    WATERMARK_SCALE: float = 0.15\n    WATERMARK_ROTATION: int = 0",
)

# ── SYNTAX VERIFICATION ────────────────────────────────────────────
print("\n--- Syntax Verification ---")
all_ok = True
for f in FIXES:
    ok = verify_syntax(f)
    if not ok:
        all_ok = False

print("\n" + "=" * 60)
if all_ok:
    print(f"SUCCESS — {len(FIXES)} files patched, all syntax valid")
    print("\nNext step:")
    print('  git add -A')
    print('  git commit -m "hotfix: critical import errors and crash fixes"')
    print('  git push origin main')
else:
    print("WARNING — Some fixes may have introduced syntax errors.")
    print("Review the files marked [FAIL] above before pushing.")
print("=" * 60)
print("\nThen run GEMINI_FIX_PROMPT.md for the remaining 11 non-crash fixes.")
