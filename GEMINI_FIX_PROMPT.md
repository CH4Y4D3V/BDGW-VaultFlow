# BDGW VAULTFLOW — GEMINI CLI MASTER FIX PROMPT
# ============================================================
# HOW TO USE:
#   gemini -p "$(cat GEMINI_FIX_PROMPT.md)"
#   (Run from project root. Gemini sees all files via context.)
# ============================================================

You are a senior Python production engineer. The deadline was June 1.
It is now June 3. You have TWO HOURS to fix this entire codebase and
push to production. You do not ask questions. You fix and ship.

## YOUR CONTEXT

This is BDGW VaultFlow — a Pyrogram (pyrofork) + MongoDB async
Telegram bot. The ground truth architecture is in
`BDGW_FLOW_BY_SHADOW_MASTER.txt`. The known production errors are in
`Final_Check.txt`. Every fix must align with the architecture.

## YOUR RULES

1. Read EVERY file before changing ANYTHING
2. Read `BDGW_FLOW_BY_SHADOW_MASTER.txt` fully — it is the spec
3. Read `Final_Check.txt` fully — it is the known error log
4. Output EVERY changed file IN FULL — no truncation, no `# ...rest`
5. After outputting files, run `python -m py_compile <file>` mentally
   to verify no syntax errors
6. End with: `git add -A && git commit -m "fix: production fixes" && git push`
7. DO NOT change the stack. Stay on Pyrogram, Motor, MongoDB, Redis.
8. DO NOT leave any `pass`, `# TODO`, `# FIXME`, or placeholder

---

## CONFIRMED BUGS — FIX ALL OF THESE

### FIX 1: `random` not imported in moderation_actions.py

**File**: `app/moderation/moderation_actions.py`
**Problem**: `random.choice(...)` is called on line ~145 inside
`_get_watermark_config()` but `import random` is missing from imports.
This crashes EVERY moderation approval silently.
**Fix**: Add `import random` to the import block at the top of the file.

### FIX 2: `Optional` not imported in takedown_handler.py

**File**: `app/handlers/takedown_handler.py`
**Problem**: `_resolve_content_id_or_link` has return type `Optional[str]`
but `Optional` is not imported. Python will raise `NameError` when the
type annotation is evaluated.
**Fix**: Add `from typing import Optional` to imports.

### FIX 3: `PaymentService` forward reference crash in payments/__init__.py

**File**: `app/payments/__init__.py`
**Problem**: The module-level `_payment_service: Optional[PaymentService]`
annotation references `PaymentService` which is only imported INSIDE
the `get_payment_service()` function. This causes `NameError` on import.
**Fix**: Change the annotation to `Optional["PaymentService"]` (string
forward reference) OR move the type annotation to use `Any`:

```python
from __future__ import annotations
from typing import Optional, TYPE_CHECKING
from app.core.database import DatabaseManager

if TYPE_CHECKING:
    from app.payments.service import PaymentService

_payment_service: Optional["PaymentService"] = None

def get_payment_service() -> "PaymentService":
    global _payment_service
    if _payment_service is None:
        from app.payments.repository import PaymentRepository
        from app.payments.service import PaymentService
        from app.referral.repository import ReferralRepository
        db = DatabaseManager.get_db()
        repo = PaymentRepository(db)
        referral_repo = ReferralRepository(db)
        _payment_service = PaymentService(repo, referral_repo)
    return _payment_service
```

### FIX 4: Duplicate `/help` command handler

**File**: `app/handlers/user_handler.py`
**Problem**: `user_handler.py` defines a `/help` handler AND
`app/handlers/support_handler.py` also defines a `/help` handler.
Pyrogram loads them alphabetically — `support_handler` wins, but both
register, causing undefined behavior.
**Fix**: Remove the `handle_help` function from `user_handler.py`
entirely (the one that shows help_cards). The correct `/help` behavior
per architecture Section 4.2 is to open a support ticket, which is
correctly implemented in `support_handler.py`.

### FIX 5: `WATERMARK_ROTATION` AttributeError

**File**: `app/config/settings.py` AND `app/watermark/ffmpeg_processor.py`
**Problem**: `ffmpeg_processor.py` accesses `settings.WATERMARK_ROTATION`
which does not exist in `Settings` class → `AttributeError` at runtime
when watermarking is enabled.
**Fix A** (settings.py): Add field:
```python
WATERMARK_ROTATION: int = 0
```
**Fix B** (.env.example): Add line:
```
# Degrees to rotate watermark logo (0 = no rotation)
WATERMARK_ROTATION=0
```

### FIX 6: `get_payment_service` wrong import path

**File**: `app/handlers/admin_handler.py` and any other file that does:
```python
from app.payments.service import get_payment_service
```
**Problem**: `get_payment_service()` is defined in
`app/payments/__init__.py`, NOT in `app/payments/service.py`.
This import raises `ImportError` at handler load time.
**Fix**: Change import to:
```python
from app.payments import get_payment_service
```
Search ALL files for this wrong import and fix every occurrence.

### FIX 7: `PaymentService.create_session` signature mismatch

**File**: `app/payments/service.py` vs `app/payments/handlers.py`
**Problem**: `create_session` in service.py is defined as:
```python
async def create_session(self, user_id: int, plan_id: str) -> PaymentSession:
```
But in handlers.py it is called as:
```python
session = await service.create_session(user_id, plan_id, method)
```
Three arguments vs two defined. This raises `TypeError`.
**Fix**: Update `service.py` signature to:
```python
async def create_session(
    self, user_id: int, plan_id: str, method: Optional[str] = None
) -> PaymentSession:
```
And inside the function, use `method` when setting `session.payment_method`.

### FIX 8: `flood_wait.py` — sync method calling `asyncio.create_task`

**File**: `app/distribution/flood_wait.py`
**Problem**: `register_flood_wait()` is a SYNC method but calls
`asyncio.create_task(_save())`. If called from a sync context (which
is possible during startup), this raises `RuntimeError: no running event loop`.
**Fix**: Use `asyncio.get_event_loop().create_task(...)` with a try/except,
or make it safe:

```python
def register_flood_wait(self, target_id: str, wait_seconds: int) -> None:
    total_wait = wait_seconds + settings.FLOODWAIT_EXTRA_BUFFER
    capped_wait = min(total_wait, settings.FLOODWAIT_MAX_WAIT)
    self._blocked_until[target_id] = time.monotonic() + capped_wait
    blocked_until_wall = time.time() + capped_wait

    async def _save():
        try:
            await self._redis.setex(
                f"fw:{target_id}",
                int(capped_wait) + 1,
                str(blocked_until_wall),
            )
        except Exception as e:
            logger.warning(
                "FloodWait: Redis persist failed",
                extra={"ctx_target": target_id, "ctx_error": str(e)},
            )

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_save())
    except RuntimeError:
        # No running loop — skip Redis persist, in-memory state is still valid
        pass

    logger.warning(
        "FloodWait registered",
        extra={
            "ctx_target": target_id,
            "ctx_wait_seconds": capped_wait,
            "ctx_original_seconds": wait_seconds,
        },
    )
```

### FIX 9: `PaymentTimeoutMonitor.check_timeouts` scheduler registration

**File**: `app/core/lifecycle.py`
**Problem**: The scheduler registers:
```python
raw_scheduler.add_job(
    timeout_monitor.check_timeouts,
    "interval",
    minutes=1,
    kwargs={"client": bot_ref},
    ...
)
```
But `check_timeouts(self, client: Client)` — `client` is a positional
parameter. APScheduler with `kwargs={"client": bot_ref}` should work
IF `client` is accepted as keyword. Verify the function signature uses
`client` as its parameter name exactly (it does in timeouts.py).
This is actually correct. But add a safety check:
```python
# In lifecycle.py, wrap the add_job in explicit try/except:
try:
    raw_scheduler.add_job(
        timeout_monitor.check_timeouts,
        "interval",
        minutes=1,
        kwargs={"client": bot_ref},
        id="payment_timeout_monitor",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    logger.info("lifecycle_payment_monitor_registered")
except Exception as e:
    logger.error(
        "lifecycle_payment_monitor_failed",
        extra={"ctx_error": str(e)},
        exc_info=True,
    )
```

### FIX 10: Missing `WATERMARK_POSITION` validation in worker_pool.py

**File**: `app/watermark/worker_pool.py`
**Problem**: 
```python
position = WatermarkPosition(position_str) if position_str in WatermarkPosition.__members__ else WatermarkPosition.BOTTOM_RIGHT
```
`WatermarkPosition.__members__` checks by NAME (e.g. "BOTTOM_RIGHT"),
but `position_str` from config could be "BOTTOM_RIGHT" (correct) or
an actual value. This is currently safe since both name and value are
"BOTTOM_RIGHT". But add explicit fallback logging:
```python
try:
    position = WatermarkPosition(position_str)
except ValueError:
    logger.warning(
        "Invalid WATERMARK_POSITION value, defaulting to BOTTOM_RIGHT",
        extra={"ctx_value": position_str}
    )
    position = WatermarkPosition.BOTTOM_RIGHT
```

### FIX 11: 3-day expiry reminder only sends ONCE (should be twice)

**File**: `app/workers/subscription_worker.py`
**Problem**: Architecture Section 7.7 says 3-day reminder sends TWICE
(24h apart). Current code uses `reminder_3d_sent: True` flag which
blocks the second send.
**Fix**: Add a `reminder_3d_sent_2` flag for the second send, with a
window offset. Change the 3-day window to:

First 3-day reminder: window 2.5d → 3.5d → set `reminder_3d_sent: True`
Second 3-day reminder: window 1.5d → 2.5d → set `reminder_3d_sent_2: True`

Add this second block to `_sweep_reminders()`:
```python
# ── Second 3-day reminder (window: 1.5d → 2.5d) ─────────────────────
min_3d_2 = now + timedelta(days=1, hours=12)
max_3d_2 = now + timedelta(days=2, hours=12)

subs_3d_2 = await col.find({
    "status": "active",
    "expires_at": {"$gte": min_3d_2, "$lte": max_3d_2},
    "plan": {"$nin": ["free", "owner", "sudo"]},
    "reminder_3d_sent": True,      # First was sent
    "reminder_3d_sent_2": {"$ne": True},  # Second not yet sent
}).to_list(length=None)

for sub_doc in subs_3d_2:
    user_id = sub_doc["user_id"]
    expires_at = sub_doc.get("expires_at")
    try:
        await self._notify(
            user_id,
            f"🚨 <b>FINAL REMINDER: Subscription expires tomorrow!</b>\n\n"
            f"Your premium access expires on "
            f"<b>{expires_at.strftime('%Y-%m-%d') if expires_at else 'soon'}</b>.\n\n"
            "Renew NOW or lose access. Contact an admin immediately.",
        )
        await col.update_one(
            {"user_id": user_id},
            {"$set": {"reminder_3d_sent_2": True}},
        )
    except Exception as e:
        logger.error("Failed to send second 3-day reminder",
                     extra={"ctx_user_id": user_id, "ctx_error": str(e)})
```

### FIX 12: `./prefix` auto-delete missing from GROUPS

**File**: `app/handlers/group_handler.py` (existing handler) AND
`app/handlers/update_logger.py`
**Problem**: Architecture Section 4.3 says `./` prefix messages in
groups are deleted after 10 seconds. `update_logger.py` only handles
PRIVATE chats (`filters.private`). `group_handler.py` has a handler
but it deletes IMMEDIATELY (no 10-second delay).
**Fix** in `group_handler.py`:
```python
@Client.on_message(filters.regex(r"^\./") & filters.group, group=-1)
async def handle_prefix_auto_delete(client: Client, message: Message) -> None:
    async def _delayed():
        await asyncio.sleep(10)
        try:
            await message.delete()
        except Exception:
            pass
    asyncio.create_task(_delayed())
    # DO NOT call stop_propagation — other handlers may need to process
```

### FIX 13: `user_handler.py` — `find_one` called wrong

**File**: `app/handlers/user_handler.py`
**Problem**:
```python
user_doc = await user_repo.find_one({"_id": user_id})
```
`UserRepository` inherits from `BaseRepository`. `BaseRepository.find_one`
takes `(self, filter: dict, projection=None)`. This call IS correct.
No fix needed. Verify only.

### FIX 14: `handle_ban_guard` stops propagation on banned user

**File**: `app/handlers/update_logger.py`
**Problem**: `update.stop_propagation()` is called on a Pyrogram
`Message` or `CallbackQuery`. In Pyrogram, the correct method is
`update.stop_propagation()` which raises `StopPropagation` exception
(it's a class, not a method). The correct Pyrogram pattern is:
```python
from pyrogram.handlers.handler import StopPropagation
raise StopPropagation
```
OR simply `update.stop_propagation()` — in newer pyrofork this may work.
Verify this works in pyrofork. If it raises an unhandled exception,
wrap handler in try/except StopPropagation.

### FIX 15: `app/services/topic_manager.py` has `warm_cache_from_db` 
but lifecycle.py calls `topic_mgr.warm_cache_from_db()` on the WRONG class

**File**: `app/core/lifecycle.py`
**Problem**:
```python
from app.services.topic_service import get_topic_manager
topic_mgr = get_topic_manager()
if hasattr(topic_mgr, 'warm_cache_from_db'):
    await topic_mgr.warm_cache_from_db()
```
`get_topic_manager` in `topic_service.py` is an alias for
`get_topic_service()` which returns a `TopicService` instance.
But `app/services/topic_manager.py` has a DIFFERENT `TopicManager`
class with `restore_cache()` method (not `warm_cache_from_db`).

There are TWO topic manager implementations:
- `app/services/topic_service.py` — `TopicService` with `warm_cache_from_db`
- `app/services/topic_manager.py` — `TopicManager` with `restore_cache`

Both are used in different places! This is a split-brain problem.
**Fix**: 
1. `support_handler.py`, `callback_handler.py`, `submission_handler.py`,
   `takedown_handler.py` all import from `app.services.topic_manager`
2. `lifecycle.py` imports from `app.services.topic_service`
3. Both must refer to the SAME singleton

The correct fix: make `topic_service.py`'s `get_topic_service` an alias
that returns `TopicManager.get_instance()` from `topic_manager.py`,
OR consolidate both files into one. Since `topic_manager.py` is more
complete (has the raw MTProto topic creation with preflight checks),
**keep `topic_manager.py` as canonical** and update `topic_service.py`:

```python
# app/services/topic_service.py — replace entire content
from app.services.topic_manager import TopicManager, get_topic_manager, TOPIC_CONTENT, TOPIC_SUPPORT, TOPIC_PAYMENT, TOPIC_REJECTED

# Aliases for backward compat
TopicService = TopicManager
get_topic_service = get_topic_manager

# Add warm_cache_from_db as alias for restore_cache
TopicManager.warm_cache_from_db = TopicManager.restore_cache
```

### FIX 16: Membership reconciliation worker is MISSING

**Architecture**: Section 26 and launch checklist require a background
worker that checks active subscriptions vs actual group membership.

**Fix**: Add to `app/core/lifecycle.py` startup (step 10):
```python
# 10. Membership reconciliation (Section 26)
try:
    if self._engine and self._engine.scheduler:
        raw_scheduler = self._engine.scheduler._scheduler
        from app.services.membership_service import MembershipService
        membership_service = MembershipService()
        bot_ref = self._bot

        async def reconcile_memberships():
            """Check active subscriptions vs actual Telegram membership."""
            try:
                from app.repositories.subscription_repository import SubscriptionRepository
                sub_repo = SubscriptionRepository()
                active_subs = await sub_repo.get_all_active()
                target_chats = membership_service.get_managed_chat_ids()
                
                for sub in active_subs:
                    for chat_id in target_chats:
                        try:
                            is_member = await membership_service.verify_membership(
                                bot_ref, sub.user_id, chat_id
                            )
                            if not is_member:
                                logger.info(
                                    "reconciliation_mismatch",
                                    extra={
                                        "ctx_user_id": sub.user_id,
                                        "ctx_chat_id": chat_id,
                                        "ctx_action": "sub_active_not_member"
                                    }
                                )
                                # Do NOT auto-add: that requires invite link
                                # Just log the discrepancy for admin review
                        except Exception as e:
                            logger.debug(
                                "reconciliation_check_failed",
                                extra={"ctx_user_id": sub.user_id, "ctx_error": str(e)}
                            )
            except Exception as e:
                logger.error("reconciliation_job_failed", extra={"ctx_error": str(e)})

        raw_scheduler.add_job(
            reconcile_memberships,
            "interval",
            hours=6,
            id="membership_reconciliation",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
        logger.info("lifecycle_reconciliation_job_registered")
except Exception as e:
    logger.warning("lifecycle_reconciliation_failed", extra={"ctx_error": str(e)})
```

---

## FILES TO OUTPUT IN FULL

After applying all fixes above, output these complete files:

1. `app/payments/__init__.py` — FIX 3
2. `app/config/settings.py` — FIX 5 (add WATERMARK_ROTATION)
3. `app/moderation/moderation_actions.py` — FIX 1 (add import random)
4. `app/handlers/takedown_handler.py` — FIX 2 (add Optional import)
5. `app/handlers/user_handler.py` — FIX 4 (remove duplicate /help)
6. `app/distribution/flood_wait.py` — FIX 8 (safe asyncio.create_task)
7. `app/watermark/ffmpeg_processor.py` — FIX 10 (safe position parsing)
8. `app/workers/subscription_worker.py` — FIX 11 (second 3d reminder)
9. `app/handlers/group_handler.py` — FIX 12 (10-second delay)
10. `app/services/topic_service.py` — FIX 15 (consolidate managers)
11. `app/core/lifecycle.py` — FIX 9, FIX 15, FIX 16 (monitor + reconcile)
12. `.env.example` — FIX 5 (add WATERMARK_ROTATION)
13. `app/payments/handlers.py` — FIX 6 (fix import path for get_payment_service)
14. `app/handlers/admin_handler.py` — FIX 6 (fix import path)
15. `app/payments/service.py` — FIX 7 (add method param to create_session)

For each file output:
```
=== FILE: app/path/to/file.py ===
[COMPLETE FILE CONTENT — NO TRUNCATION]
=== END FILE ===
```

---

## VERIFICATION STEPS

After outputting all files, output these commands:

```bash
# 1. Syntax check every changed file
python -m py_compile app/payments/__init__.py
python -m py_compile app/config/settings.py
python -m py_compile app/moderation/moderation_actions.py
python -m py_compile app/handlers/takedown_handler.py
python -m py_compile app/handlers/user_handler.py
python -m py_compile app/distribution/flood_wait.py
python -m py_compile app/watermark/ffmpeg_processor.py
python -m py_compile app/workers/subscription_worker.py
python -m py_compile app/handlers/group_handler.py
python -m py_compile app/services/topic_service.py
python -m py_compile app/core/lifecycle.py
python -m py_compile app/payments/handlers.py
python -m py_compile app/handlers/admin_handler.py
python -m py_compile app/payments/service.py

# 2. Check for import issues
python -c "from app.payments import get_payment_service; print('OK')"
python -c "from app.config import settings; print(settings.WATERMARK_ROTATION)"
python -c "import app.moderation.moderation_actions; print('OK')"
python -c "import app.handlers.takedown_handler; print('OK')"

# 3. Git push
git add -A
git status
git commit -m "fix: production crash fixes — import errors, signature mismatches, duplicate handlers, watermark, reminders, reconciliation"
git push origin main
```

---

## FINAL CHECK

Before pushing, verify:
- [ ] `import random` exists in moderation_actions.py
- [ ] `from typing import Optional` exists in takedown_handler.py
- [ ] `get_payment_service` imported from `app.payments` not `app.payments.service`
- [ ] Only ONE `/help` handler exists (in support_handler.py)
- [ ] `WATERMARK_ROTATION` exists in Settings class with default 0
- [ ] `create_session` accepts `method` parameter
- [ ] `./prefix` in groups deletes after 10 seconds not immediately
- [ ] 3-day expiry reminder sends twice (24h apart)
- [ ] Membership reconciliation job is registered
- [ ] `topic_service.py` and `topic_manager.py` point to same singleton
- [ ] `flood_wait.py` safe for sync contexts

SHIP IT. Deadline was June 1. It is June 3. Every minute costs revenue.
