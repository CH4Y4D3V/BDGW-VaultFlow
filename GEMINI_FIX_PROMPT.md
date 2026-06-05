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

railway log: 
2026-06-05T00:00:29.912826778Z [inf]  Starting Container
2026-06-05T00:00:31.719882711Z [inf]  Initializing BDGW VaultFlow main process...
2026-06-05T00:00:31.719891410Z [inf]  lifecycle_bootstrapping_start
2026-06-05T00:00:31.719899265Z [inf]  Health server started
2026-06-05T00:00:31.719906358Z [inf]  mongodb_connection_start
2026-06-05T00:00:31.719913580Z [inf]  mongodb_connection_established
2026-06-05T00:00:31.719920223Z [inf]  mongodb_replica_set_detected
2026-06-05T00:00:31.719927190Z [inf]  migration_queue_stabilization_start
2026-06-05T00:00:31.721585724Z [inf]  100.64.0.2 [05/Jun/2026:00:00:31 +0000] "GET /health HTTP/1.1" 200 193 "-" "RailwayHealthCheck/1.0"
2026-06-05T00:00:31.721658352Z [inf]  migration_queue_stabilization_complete
2026-06-05T00:00:31.721670782Z [inf]  migration_vault_stabilization_start
2026-06-05T00:00:31.721676896Z [inf]  migration_vault_stabilization_complete
2026-06-05T00:00:31.721682757Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:31.721688494Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:31.721694004Z [inf]  referral_indexes_verified
2026-06-05T00:00:31.724356052Z [err]  Index creation error for payments
2026-06-05T00:00:31.726424440Z [inf]  Payment repository indexes created
2026-06-05T00:00:31.726434859Z [inf]  payment_indexes_verified
2026-06-05T00:00:31.726442223Z [inf]  txid_indexes_verified
2026-06-05T00:00:31.726451743Z [inf]  mongodb_initialization_complete
2026-06-05T00:00:31.726457300Z [inf]  NSFW channel seeded
2026-06-05T00:00:31.726464440Z [inf]  PREMIUM channel seeded
2026-06-05T00:00:31.726470815Z [inf]  Distribution channels seeded successfully
2026-06-05T00:00:31.728194930Z [inf]  lifecycle_bot_start
2026-06-05T00:00:35.733439435Z [err]  lifecycle_bot_start_failed
2026-06-05T00:00:35.733444810Z [inf]  lifecycle_shutdown_start
2026-06-05T00:00:35.734879000Z [err]  lifecycle_shutdown_bot_failed
2026-06-05T00:00:35.737100146Z [inf]  mongodb_connection_closed
2026-06-05T00:00:35.737107247Z [inf]  lifecycle_shutdown_complete
2026-06-05T00:00:35.737111455Z [err]      return runner.run(main)
2026-06-05T00:00:35.737111714Z [inf]  Main process exit complete
2026-06-05T00:00:35.737116763Z [err]  Traceback (most recent call last):
2026-06-05T00:00:35.737121753Z [err]             ^^^^^^^^^^^^^^^^
2026-06-05T00:00:35.737122770Z [err]    File "/app/main_bot.py", line 52, in <module>
2026-06-05T00:00:35.737128994Z [err]      main()
2026-06-05T00:00:35.737129708Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 118, in run
2026-06-05T00:00:35.737134960Z [err]    File "/app/main_bot.py", line 48, in main
2026-06-05T00:00:35.737136875Z [err]      return self._loop.run_until_complete(task)
2026-06-05T00:00:35.737141087Z [err]      asyncio.run(async_main())
2026-06-05T00:00:35.737142721Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:35.737148775Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 195, in run
2026-06-05T00:00:35.737149528Z [err]    File "/usr/local/lib/python3.12/asyncio/base_events.py", line 691, in run_until_complete
2026-06-05T00:00:35.737154739Z [err]      return future.result()
2026-06-05T00:00:35.737159156Z [err]             ^^^^^^^^^^^^^^^
2026-06-05T00:00:35.741708547Z [err]    File "<frozen importlib._bootstrap>", line 1360, in _find_and_load
2026-06-05T00:00:35.741714321Z [err]    File "/app/main_bot.py", line 33, in async_main
2026-06-05T00:00:35.741720925Z [err]    File "<frozen importlib._bootstrap>", line 1331, in _find_and_load_unlocked
2026-06-05T00:00:35.741721790Z [err]      await lifecycle.start()
2026-06-05T00:00:35.741727538Z [err]    File "<frozen importlib._bootstrap>", line 935, in _load_unlocked
2026-06-05T00:00:35.741730004Z [err]    File "/app/app/core/lifecycle.py", line 73, in start
2026-06-05T00:00:35.741735527Z [err]    File "<frozen importlib._bootstrap_external>", line 999, in exec_module
2026-06-05T00:00:35.741735841Z [err]      await self._bot.start()
2026-06-05T00:00:35.741741233Z [err]    File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
2026-06-05T00:00:35.741744839Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/utilities/start.py", line 75, in start
2026-06-05T00:00:35.741751046Z [err]      await self.initialize()
2026-06-05T00:00:35.741757037Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/auth/initialize.py", line 48, in initialize
2026-06-05T00:00:35.741762850Z [err]      self.load_plugins()
2026-06-05T00:00:35.741768370Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/client.py", line 927, in load_plugins
2026-06-05T00:00:35.741774330Z [err]      module = import_module(module_path)
2026-06-05T00:00:35.741780096Z [err]               ^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:35.741786424Z [err]    File "/usr/local/lib/python3.12/importlib/__init__.py", line 90, in import_module
2026-06-05T00:00:35.741791680Z [err]      return _bootstrap._gcd_import(name[level:], package, level)
2026-06-05T00:00:35.741797617Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:35.741803143Z [err]    File "<frozen importlib._bootstrap>", line 1387, in _gcd_import
2026-06-05T00:00:35.743584606Z [err]    File "/app/app/handlers/support_handler.py", line 12, in <module>
2026-06-05T00:00:35.743592230Z [err]      from aiogram import Bot, F, Router
2026-06-05T00:00:35.743598805Z [err]  ModuleNotFoundError: No module named 'aiogram'
2026-06-05T00:00:38.233932385Z [inf]  Initializing BDGW VaultFlow main process...
2026-06-05T00:00:38.233936349Z [inf]  lifecycle_bootstrapping_start
2026-06-05T00:00:38.233940454Z [inf]  Health server started
2026-06-05T00:00:38.233944830Z [inf]  mongodb_connection_start
2026-06-05T00:00:38.808197130Z [inf]  mongodb_connection_established
2026-06-05T00:00:38.808201068Z [inf]  mongodb_replica_set_detected
2026-06-05T00:00:38.808205640Z [inf]  migration_queue_stabilization_start
2026-06-05T00:00:38.808209978Z [inf]  migration_queue_stabilization_complete
2026-06-05T00:00:38.808213747Z [inf]  migration_vault_stabilization_start
2026-06-05T00:00:38.808217353Z [inf]  migration_vault_stabilization_complete
2026-06-05T00:00:38.808229402Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:38.810449417Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:38.810454115Z [inf]  referral_indexes_verified
2026-06-05T00:00:38.873766275Z [err]  Index creation error for payments
2026-06-05T00:00:38.918579210Z [inf]  Payment repository indexes created
2026-06-05T00:00:38.918586765Z [inf]  payment_indexes_verified
2026-06-05T00:00:38.963850034Z [inf]  txid_indexes_verified
2026-06-05T00:00:38.963856255Z [inf]  mongodb_initialization_complete
2026-06-05T00:00:38.970087829Z [inf]  NSFW channel seeded
2026-06-05T00:00:38.978418594Z [inf]  PREMIUM channel seeded
2026-06-05T00:00:38.978427904Z [inf]  Distribution channels seeded successfully
2026-06-05T00:00:38.978434138Z [inf]  lifecycle_bot_start
2026-06-05T00:00:39.773175764Z [err]  lifecycle_bot_start_failed
2026-06-05T00:00:39.773187492Z [inf]  lifecycle_shutdown_start
2026-06-05T00:00:39.774637723Z [err]  lifecycle_shutdown_bot_failed
2026-06-05T00:00:39.778876274Z [inf]  mongodb_connection_closed
2026-06-05T00:00:39.778883044Z [inf]  lifecycle_shutdown_complete
2026-06-05T00:00:39.778889029Z [inf]  Main process exit complete
2026-06-05T00:00:39.778893495Z [err]  Traceback (most recent call last):
2026-06-05T00:00:39.778897815Z [err]    File "/app/main_bot.py", line 52, in <module>
2026-06-05T00:00:39.778902326Z [err]      main()
2026-06-05T00:00:39.778906269Z [err]    File "/app/main_bot.py", line 48, in main
2026-06-05T00:00:39.778910235Z [err]      asyncio.run(async_main())
2026-06-05T00:00:39.778914357Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 195, in run
2026-06-05T00:00:39.778918680Z [err]      return runner.run(main)
2026-06-05T00:00:39.778923196Z [err]             ^^^^^^^^^^^^^^^^
2026-06-05T00:00:39.778927216Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 118, in run
2026-06-05T00:00:39.778931000Z [err]      return self._loop.run_until_complete(task)
2026-06-05T00:00:39.778935002Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:39.778939524Z [err]    File "/usr/local/lib/python3.12/asyncio/base_events.py", line 691, in run_until_complete
2026-06-05T00:00:39.778943491Z [err]      return future.result()
2026-06-05T00:00:39.778947511Z [err]             ^^^^^^^^^^^^^^^
2026-06-05T00:00:39.782735235Z [err]    File "/app/main_bot.py", line 33, in async_main
2026-06-05T00:00:39.782741436Z [err]               ^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:39.782742202Z [err]      await lifecycle.start()
2026-06-05T00:00:39.782748107Z [err]    File "/app/app/core/lifecycle.py", line 73, in start
2026-06-05T00:00:39.782756579Z [err]      await self._bot.start()
2026-06-05T00:00:39.782757827Z [err]    File "/usr/local/lib/python3.12/importlib/__init__.py", line 90, in import_module
2026-06-05T00:00:39.782766300Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/utilities/start.py", line 75, in start
2026-06-05T00:00:39.782771576Z [err]      return _bootstrap._gcd_import(name[level:], package, level)
2026-06-05T00:00:39.782837495Z [err]      await self.initialize()
2026-06-05T00:00:39.782849668Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/auth/initialize.py", line 48, in initialize
2026-06-05T00:00:39.782852082Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:39.782862426Z [err]      self.load_plugins()
2026-06-05T00:00:39.782862588Z [err]    File "<frozen importlib._bootstrap>", line 1387, in _gcd_import
2026-06-05T00:00:39.782875943Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/client.py", line 927, in load_plugins
2026-06-05T00:00:39.782877645Z [err]    File "<frozen importlib._bootstrap>", line 1360, in _find_and_load
2026-06-05T00:00:39.782886008Z [err]      module = import_module(module_path)
2026-06-05T00:00:39.782891318Z [err]    File "<frozen importlib._bootstrap>", line 1331, in _find_and_load_unlocked
2026-06-05T00:00:39.782895967Z [err]    File "<frozen importlib._bootstrap>", line 935, in _load_unlocked
2026-06-05T00:00:39.782901484Z [err]    File "<frozen importlib._bootstrap_external>", line 999, in exec_module
2026-06-05T00:00:39.782906684Z [err]    File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
2026-06-05T00:00:39.785021847Z [err]    File "/app/app/handlers/support_handler.py", line 12, in <module>
2026-06-05T00:00:39.785028300Z [err]      from aiogram import Bot, F, Router
2026-06-05T00:00:39.785033530Z [err]  ModuleNotFoundError: No module named 'aiogram'
2026-06-05T00:00:42.846241312Z [inf]  mongodb_replica_set_detected
2026-06-05T00:00:42.846247599Z [inf]  mongodb_connection_start
2026-06-05T00:00:42.846250616Z [inf]  migration_queue_stabilization_start
2026-06-05T00:00:42.846258178Z [inf]  mongodb_connection_established
2026-06-05T00:00:42.846276525Z [inf]  Initializing BDGW VaultFlow main process...
2026-06-05T00:00:42.846285041Z [inf]  lifecycle_bootstrapping_start
2026-06-05T00:00:42.846292025Z [inf]  Health server started
2026-06-05T00:00:42.847062406Z [inf]  migration_queue_stabilization_complete
2026-06-05T00:00:42.847069488Z [inf]  migration_vault_stabilization_start
2026-06-05T00:00:42.847076599Z [inf]  migration_vault_stabilization_complete
2026-06-05T00:00:42.956235655Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:42.956254326Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:43.038088402Z [inf]  referral_indexes_verified
2026-06-05T00:00:43.170452272Z [err]  Index creation error for payments
2026-06-05T00:00:43.229695417Z [inf]  Payment repository indexes created
2026-06-05T00:00:43.229702128Z [inf]  payment_indexes_verified
2026-06-05T00:00:43.267720942Z [inf]  txid_indexes_verified
2026-06-05T00:00:43.267726922Z [inf]  mongodb_initialization_complete
2026-06-05T00:00:43.281707846Z [inf]  NSFW channel seeded
2026-06-05T00:00:43.283755541Z [inf]  PREMIUM channel seeded
2026-06-05T00:00:43.283764685Z [inf]  Distribution channels seeded successfully
2026-06-05T00:00:43.283774209Z [inf]  lifecycle_bot_start
2026-06-05T00:00:43.864930398Z [err]  lifecycle_bot_start_failed
2026-06-05T00:00:43.864936277Z [inf]  lifecycle_shutdown_start
2026-06-05T00:00:43.866259809Z [err]  lifecycle_shutdown_bot_failed
2026-06-05T00:00:43.870349217Z [inf]  mongodb_connection_closed
2026-06-05T00:00:43.870362676Z [inf]  lifecycle_shutdown_complete
2026-06-05T00:00:43.870374720Z [inf]  Main process exit complete
2026-06-05T00:00:43.870383023Z [err]  Traceback (most recent call last):
2026-06-05T00:00:43.870390875Z [err]    File "/app/main_bot.py", line 52, in <module>
2026-06-05T00:00:43.870405712Z [err]      main()
2026-06-05T00:00:43.870413744Z [err]    File "/app/main_bot.py", line 48, in main
2026-06-05T00:00:43.870420881Z [err]      asyncio.run(async_main())
2026-06-05T00:00:43.870428842Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 195, in run
2026-06-05T00:00:43.870437244Z [err]      return runner.run(main)
2026-06-05T00:00:43.870444499Z [err]             ^^^^^^^^^^^^^^^^
2026-06-05T00:00:43.870452191Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 118, in run
2026-06-05T00:00:43.870465985Z [err]      return self._loop.run_until_complete(task)
2026-06-05T00:00:43.870477868Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:43.870484234Z [err]    File "/usr/local/lib/python3.12/asyncio/base_events.py", line 691, in run_until_complete
2026-06-05T00:00:43.870490509Z [err]      return future.result()
2026-06-05T00:00:43.870496330Z [err]             ^^^^^^^^^^^^^^^
2026-06-05T00:00:43.873222724Z [err]    File "/app/main_bot.py", line 33, in async_main
2026-06-05T00:00:43.873223535Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/client.py", line 927, in load_plugins
2026-06-05T00:00:43.873233068Z [err]      await lifecycle.start()
2026-06-05T00:00:43.873239043Z [err]      module = import_module(module_path)
2026-06-05T00:00:43.873240415Z [err]    File "/app/app/core/lifecycle.py", line 73, in start
2026-06-05T00:00:43.873247779Z [err]      await self._bot.start()
2026-06-05T00:00:43.873249030Z [err]               ^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:43.873255617Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/utilities/start.py", line 75, in start
2026-06-05T00:00:43.873257737Z [err]    File "/usr/local/lib/python3.12/importlib/__init__.py", line 90, in import_module
2026-06-05T00:00:43.873263198Z [err]      await self.initialize()
2026-06-05T00:00:43.873265448Z [err]      return _bootstrap._gcd_import(name[level:], package, level)
2026-06-05T00:00:43.873270130Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/auth/initialize.py", line 48, in initialize
2026-06-05T00:00:43.873274298Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:43.873279063Z [err]      self.load_plugins()
2026-06-05T00:00:43.873282131Z [err]    File "<frozen importlib._bootstrap>", line 1387, in _gcd_import
2026-06-05T00:00:43.873288336Z [err]    File "<frozen importlib._bootstrap>", line 1360, in _find_and_load
2026-06-05T00:00:43.873294226Z [err]    File "<frozen importlib._bootstrap>", line 1331, in _find_and_load_unlocked
2026-06-05T00:00:43.873300090Z [err]    File "<frozen importlib._bootstrap>", line 935, in _load_unlocked
2026-06-05T00:00:43.873305913Z [err]    File "<frozen importlib._bootstrap_external>", line 999, in exec_module
2026-06-05T00:00:43.873311572Z [err]    File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
2026-06-05T00:00:43.874714276Z [err]    File "/app/app/handlers/support_handler.py", line 12, in <module>
2026-06-05T00:00:43.874720854Z [err]      from aiogram import Bot, F, Router
2026-06-05T00:00:43.874727162Z [err]  ModuleNotFoundError: No module named 'aiogram'
2026-06-05T00:00:46.903507165Z [inf]  Initializing BDGW VaultFlow main process...
2026-06-05T00:00:46.903513554Z [inf]  lifecycle_bootstrapping_start
2026-06-05T00:00:46.903520255Z [inf]  Health server started
2026-06-05T00:00:46.903526285Z [inf]  mongodb_connection_start
2026-06-05T00:00:46.983107717Z [inf]  mongodb_connection_established
2026-06-05T00:00:46.983116246Z [inf]  mongodb_replica_set_detected
2026-06-05T00:00:46.983121546Z [inf]  migration_queue_stabilization_start
2026-06-05T00:00:46.983127801Z [inf]  migration_queue_stabilization_complete
2026-06-05T00:00:46.983132944Z [inf]  migration_vault_stabilization_start
2026-06-05T00:00:46.983138027Z [inf]  migration_vault_stabilization_complete
2026-06-05T00:00:47.234884905Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:47.234889256Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:47.334326264Z [inf]  referral_indexes_verified
2026-06-05T00:00:47.843392924Z [err]  Index creation error for payments
2026-06-05T00:00:47.844228110Z [inf]  Payment repository indexes created
2026-06-05T00:00:47.844237604Z [inf]  payment_indexes_verified
2026-06-05T00:00:47.844243402Z [inf]  txid_indexes_verified
2026-06-05T00:00:47.844249405Z [inf]  mongodb_initialization_complete
2026-06-05T00:00:47.844255670Z [inf]  NSFW channel seeded
2026-06-05T00:00:47.844263919Z [inf]  PREMIUM channel seeded
2026-06-05T00:00:47.844270519Z [inf]  Distribution channels seeded successfully
2026-06-05T00:00:47.845452707Z [inf]  lifecycle_bot_start
2026-06-05T00:00:48.130312710Z [err]  lifecycle_bot_start_failed
2026-06-05T00:00:48.130319591Z [inf]  lifecycle_shutdown_start
2026-06-05T00:00:48.131902152Z [err]  lifecycle_shutdown_bot_failed
2026-06-05T00:00:48.133117350Z [inf]  mongodb_connection_closed
2026-06-05T00:00:48.133123902Z [inf]  lifecycle_shutdown_complete
2026-06-05T00:00:48.133128522Z [inf]  Main process exit complete
2026-06-05T00:00:48.135323899Z [err]  Traceback (most recent call last):
2026-06-05T00:00:48.135330850Z [err]    File "/app/main_bot.py", line 52, in <module>
2026-06-05T00:00:48.135335504Z [err]      main()
2026-06-05T00:00:48.135339900Z [err]    File "/app/main_bot.py", line 48, in main
2026-06-05T00:00:48.135344033Z [err]      asyncio.run(async_main())
2026-06-05T00:00:48.135348729Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 195, in run
2026-06-05T00:00:48.135357363Z [err]      return runner.run(main)
2026-06-05T00:00:48.135361454Z [err]             ^^^^^^^^^^^^^^^^
2026-06-05T00:00:48.135387923Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 118, in run
2026-06-05T00:00:48.135394884Z [err]      return self._loop.run_until_complete(task)
2026-06-05T00:00:48.135399272Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:48.135403952Z [err]    File "/usr/local/lib/python3.12/asyncio/base_events.py", line 691, in run_until_complete
2026-06-05T00:00:48.135408561Z [err]      return future.result()
2026-06-05T00:00:48.135412614Z [err]             ^^^^^^^^^^^^^^^
2026-06-05T00:00:48.135416801Z [err]    File "/app/main_bot.py", line 33, in async_main
2026-06-05T00:00:48.135421509Z [err]      await lifecycle.start()
2026-06-05T00:00:48.135427024Z [err]    File "/app/app/core/lifecycle.py", line 73, in start
2026-06-05T00:00:48.135431331Z [err]      await self._bot.start()
2026-06-05T00:00:48.135435478Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/utilities/start.py", line 75, in start
2026-06-05T00:00:48.135439739Z [err]      await self.initialize()
2026-06-05T00:00:48.135444350Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/auth/initialize.py", line 48, in initialize
2026-06-05T00:00:48.135448676Z [err]      self.load_plugins()
2026-06-05T00:00:48.135452663Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/client.py", line 927, in load_plugins
2026-06-05T00:00:48.138636233Z [err]      module = import_module(module_path)
2026-06-05T00:00:48.138645066Z [err]               ^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:48.138651639Z [err]    File "/usr/local/lib/python3.12/importlib/__init__.py", line 90, in import_module
2026-06-05T00:00:48.138657970Z [err]      return _bootstrap._gcd_import(name[level:], package, level)
2026-06-05T00:00:48.138663768Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:48.138669393Z [err]    File "<frozen importlib._bootstrap>", line 1387, in _gcd_import
2026-06-05T00:00:48.138673723Z [err]    File "<frozen importlib._bootstrap>", line 1360, in _find_and_load
2026-06-05T00:00:48.138678117Z [err]    File "<frozen importlib._bootstrap>", line 1331, in _find_and_load_unlocked
2026-06-05T00:00:48.138682009Z [err]    File "<frozen importlib._bootstrap>", line 935, in _load_unlocked
2026-06-05T00:00:48.138685879Z [err]    File "<frozen importlib._bootstrap_external>", line 999, in exec_module
2026-06-05T00:00:48.138690365Z [err]    File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
2026-06-05T00:00:48.138694588Z [err]    File "/app/app/handlers/support_handler.py", line 12, in <module>
2026-06-05T00:00:48.138698558Z [err]      from aiogram import Bot, F, Router
2026-06-05T00:00:48.138703031Z [err]  ModuleNotFoundError: No module named 'aiogram'
2026-06-05T00:00:51.071525041Z [inf]  Initializing BDGW VaultFlow main process...
2026-06-05T00:00:51.071531458Z [inf]  lifecycle_bootstrapping_start
2026-06-05T00:00:51.071536924Z [inf]  Health server started
2026-06-05T00:00:51.071543819Z [inf]  mongodb_connection_start
2026-06-05T00:00:51.175432388Z [inf]  mongodb_connection_established
2026-06-05T00:00:51.175445424Z [inf]  mongodb_replica_set_detected
2026-06-05T00:00:51.175452937Z [inf]  migration_queue_stabilization_start
2026-06-05T00:00:51.176388655Z [inf]  migration_queue_stabilization_complete
2026-06-05T00:00:51.176396801Z [inf]  migration_vault_stabilization_start
2026-06-05T00:00:51.183214277Z [inf]  migration_vault_stabilization_complete
2026-06-05T00:00:51.440837129Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:51.440848899Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:51.995192819Z [inf]  referral_indexes_verified
2026-06-05T00:00:51.995199824Z [err]  Index creation error for payments
2026-06-05T00:00:51.996248492Z [inf]  payment_indexes_verified
2026-06-05T00:00:51.996255246Z [inf]  txid_indexes_verified
2026-06-05T00:00:51.996260404Z [inf]  mongodb_initialization_complete
2026-06-05T00:00:51.996265421Z [inf]  NSFW channel seeded
2026-06-05T00:00:51.996270123Z [inf]  PREMIUM channel seeded
2026-06-05T00:00:51.996275377Z [inf]  Distribution channels seeded successfully
2026-06-05T00:00:51.996302516Z [inf]  Payment repository indexes created
2026-06-05T00:00:51.997888476Z [inf]  lifecycle_bot_start
2026-06-05T00:00:52.078442014Z [err]  lifecycle_bot_start_failed
2026-06-05T00:00:52.078451376Z [inf]  lifecycle_shutdown_start
2026-06-05T00:00:52.080532663Z [err]  lifecycle_shutdown_bot_failed
2026-06-05T00:00:52.082192928Z [inf]  mongodb_connection_closed
2026-06-05T00:00:52.082208147Z [inf]  lifecycle_shutdown_complete
2026-06-05T00:00:52.082217339Z [inf]  Main process exit complete
2026-06-05T00:00:52.082224624Z [err]  Traceback (most recent call last):
2026-06-05T00:00:52.082231233Z [err]    File "/app/main_bot.py", line 52, in <module>
2026-06-05T00:00:52.082238811Z [err]      main()
2026-06-05T00:00:52.082248168Z [err]    File "/app/main_bot.py", line 48, in main
2026-06-05T00:00:52.082254671Z [err]      asyncio.run(async_main())
2026-06-05T00:00:52.082260934Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 195, in run
2026-06-05T00:00:52.082267799Z [err]      return runner.run(main)
2026-06-05T00:00:52.082274614Z [err]             ^^^^^^^^^^^^^^^^
2026-06-05T00:00:52.082280629Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 118, in run
2026-06-05T00:00:52.082286378Z [err]      return self._loop.run_until_complete(task)
2026-06-05T00:00:52.082435007Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:52.082440438Z [err]    File "/usr/local/lib/python3.12/asyncio/base_events.py", line 691, in run_until_complete
2026-06-05T00:00:52.082445026Z [err]      return future.result()
2026-06-05T00:00:52.082449934Z [err]             ^^^^^^^^^^^^^^^
2026-06-05T00:00:52.083839049Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:52.083848852Z [err]    File "/app/main_bot.py", line 33, in async_main
2026-06-05T00:00:52.083861147Z [err]    File "<frozen importlib._bootstrap>", line 1387, in _gcd_import
2026-06-05T00:00:52.083866926Z [err]    File "<frozen importlib._bootstrap>", line 1360, in _find_and_load
2026-06-05T00:00:52.083871628Z [err]    File "<frozen importlib._bootstrap>", line 1331, in _find_and_load_unlocked
2026-06-05T00:00:52.083875938Z [err]    File "<frozen importlib._bootstrap>", line 935, in _load_unlocked
2026-06-05T00:00:52.083880297Z [err]    File "<frozen importlib._bootstrap_external>", line 999, in exec_module
2026-06-05T00:00:52.083884127Z [err]    File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
2026-06-05T00:00:52.083908946Z [err]      await lifecycle.start()
2026-06-05T00:00:52.083915108Z [err]    File "/app/app/core/lifecycle.py", line 73, in start
2026-06-05T00:00:52.083921909Z [err]      await self._bot.start()
2026-06-05T00:00:52.083927523Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/utilities/start.py", line 75, in start
2026-06-05T00:00:52.083932914Z [err]      await self.initialize()
2026-06-05T00:00:52.083938093Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/auth/initialize.py", line 48, in initialize
2026-06-05T00:00:52.083943456Z [err]      self.load_plugins()
2026-06-05T00:00:52.083949348Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/client.py", line 927, in load_plugins
2026-06-05T00:00:52.083954917Z [err]      module = import_module(module_path)
2026-06-05T00:00:52.083961056Z [err]               ^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:52.083966894Z [err]    File "/usr/local/lib/python3.12/importlib/__init__.py", line 90, in import_module
2026-06-05T00:00:52.083972187Z [err]      return _bootstrap._gcd_import(name[level:], package, level)
2026-06-05T00:00:52.084696272Z [err]    File "/app/app/handlers/support_handler.py", line 12, in <module>
2026-06-05T00:00:52.084702532Z [err]      from aiogram import Bot, F, Router
2026-06-05T00:00:52.084708047Z [err]  ModuleNotFoundError: No module named 'aiogram'
2026-06-05T00:00:55.027468582Z [inf]  Initializing BDGW VaultFlow main process...
2026-06-05T00:00:55.027474631Z [inf]  lifecycle_bootstrapping_start
2026-06-05T00:00:55.027481180Z [inf]  Health server started
2026-06-05T00:00:55.027486317Z [inf]  mongodb_connection_start
2026-06-05T00:00:55.054702788Z [inf]  mongodb_connection_established
2026-06-05T00:00:55.054714059Z [inf]  mongodb_replica_set_detected
2026-06-05T00:00:55.054721190Z [inf]  migration_queue_stabilization_start
2026-06-05T00:00:55.060442750Z [inf]  migration_queue_stabilization_complete
2026-06-05T00:00:55.060453019Z [inf]  migration_vault_stabilization_start
2026-06-05T00:00:55.065884340Z [inf]  migration_vault_stabilization_complete
2026-06-05T00:00:55.340938251Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:55.340942910Z [inf]  mongodb_indexes_verified
2026-06-05T00:00:55.414259873Z [inf]  referral_indexes_verified
2026-06-05T00:00:56.127844566Z [err]  Index creation error for payments
2026-06-05T00:00:56.128647407Z [inf]  Payment repository indexes created
2026-06-05T00:00:56.128655127Z [inf]  payment_indexes_verified
2026-06-05T00:00:56.128663206Z [inf]  txid_indexes_verified
2026-06-05T00:00:56.128670033Z [inf]  mongodb_initialization_complete
2026-06-05T00:00:56.128676905Z [inf]  NSFW channel seeded
2026-06-05T00:00:56.128703063Z [inf]  PREMIUM channel seeded
2026-06-05T00:00:56.128709377Z [inf]  Distribution channels seeded successfully
2026-06-05T00:00:56.129724230Z [inf]  lifecycle_bot_start
2026-06-05T00:00:56.129728785Z [err]  lifecycle_bot_start_failed
2026-06-05T00:00:56.131309248Z [inf]  lifecycle_shutdown_start
2026-06-05T00:00:56.131316957Z [err]  lifecycle_shutdown_bot_failed
2026-06-05T00:00:56.133656337Z [inf]  mongodb_connection_closed
2026-06-05T00:00:56.133666308Z [inf]  lifecycle_shutdown_complete
2026-06-05T00:00:56.133668943Z [err]      return future.result()
2026-06-05T00:00:56.133669382Z [err]      main()
2026-06-05T00:00:56.133674435Z [inf]  Main process exit complete
2026-06-05T00:00:56.133678816Z [err]             ^^^^^^^^^^^^^^^
2026-06-05T00:00:56.133680651Z [err]    File "/app/main_bot.py", line 48, in main
2026-06-05T00:00:56.133682850Z [err]  Traceback (most recent call last):
2026-06-05T00:00:56.133687798Z [err]      asyncio.run(async_main())
2026-06-05T00:00:56.133690541Z [err]    File "/app/main_bot.py", line 52, in <module>
2026-06-05T00:00:56.133694342Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 195, in run
2026-06-05T00:00:56.133698884Z [err]      return runner.run(main)
2026-06-05T00:00:56.133703963Z [err]             ^^^^^^^^^^^^^^^^
2026-06-05T00:00:56.133708162Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 118, in run
2026-06-05T00:00:56.133713081Z [err]      return self._loop.run_until_complete(task)
2026-06-05T00:00:56.133718069Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:56.133723891Z [err]    File "/usr/local/lib/python3.12/asyncio/base_events.py", line 691, in run_until_complete
2026-06-05T00:00:56.136120634Z [err]    File "<frozen importlib._bootstrap>", line 1331, in _find_and_load_unlocked
2026-06-05T00:00:56.136128831Z [err]    File "<frozen importlib._bootstrap>", line 935, in _load_unlocked
2026-06-05T00:00:56.136134368Z [err]    File "<frozen importlib._bootstrap_external>", line 999, in exec_module
2026-06-05T00:00:56.136139967Z [err]    File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
2026-06-05T00:00:56.136151896Z [err]    File "/app/main_bot.py", line 33, in async_main
2026-06-05T00:00:56.136160051Z [err]      await lifecycle.start()
2026-06-05T00:00:56.136164649Z [err]    File "/app/app/core/lifecycle.py", line 73, in start
2026-06-05T00:00:56.136168717Z [err]      await self._bot.start()
2026-06-05T00:00:56.136172842Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/utilities/start.py", line 75, in start
2026-06-05T00:00:56.136177464Z [err]      await self.initialize()
2026-06-05T00:00:56.136182334Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/auth/initialize.py", line 48, in initialize
2026-06-05T00:00:56.136186346Z [err]      self.load_plugins()
2026-06-05T00:00:56.136191234Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/client.py", line 927, in load_plugins
2026-06-05T00:00:56.136198174Z [err]      module = import_module(module_path)
2026-06-05T00:00:56.136216491Z [err]               ^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:56.136221890Z [err]    File "/usr/local/lib/python3.12/importlib/__init__.py", line 90, in import_module
2026-06-05T00:00:56.136227279Z [err]      return _bootstrap._gcd_import(name[level:], package, level)
2026-06-05T00:00:56.136231800Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:00:56.136236552Z [err]    File "<frozen importlib._bootstrap>", line 1387, in _gcd_import
2026-06-05T00:00:56.136240861Z [err]    File "<frozen importlib._bootstrap>", line 1360, in _find_and_load
2026-06-05T00:00:56.137369274Z [err]    File "/app/app/handlers/support_handler.py", line 12, in <module>
2026-06-05T00:00:56.137375996Z [err]      from aiogram import Bot, F, Router
2026-06-05T00:00:56.137380367Z [err]  ModuleNotFoundError: No module named 'aiogram'
2026-06-05T00:00:59.371034564Z [inf]  Initializing BDGW VaultFlow main process...
2026-06-05T00:00:59.371039346Z [inf]  lifecycle_bootstrapping_start
2026-06-05T00:00:59.371043617Z [inf]  Health server started
2026-06-05T00:00:59.371047940Z [inf]  mongodb_connection_start
2026-06-05T00:00:59.484106275Z [inf]  mongodb_connection_established
2026-06-05T00:00:59.484114462Z [inf]  mongodb_replica_set_detected
2026-06-05T00:00:59.484120484Z [inf]  migration_queue_stabilization_start
2026-06-05T00:00:59.492643045Z [inf]  migration_queue_stabilization_complete
2026-06-05T00:00:59.492649908Z [inf]  migration_vault_stabilization_start
2026-06-05T00:00:59.492654550Z [inf]  migration_vault_stabilization_complete
2026-06-05T00:01:00.139045931Z [err]  Index creation error for payments
2026-06-05T00:01:00.139130997Z [inf]  mongodb_indexes_verified
2026-06-05T00:01:00.139135065Z [inf]  mongodb_indexes_verified
2026-06-05T00:01:00.139139222Z [inf]  referral_indexes_verified
2026-06-05T00:01:00.139673683Z [inf]  Payment repository indexes created
2026-06-05T00:01:00.139685092Z [inf]  payment_indexes_verified
2026-06-05T00:01:00.139691761Z [inf]  txid_indexes_verified
2026-06-05T00:01:00.139699102Z [inf]  mongodb_initialization_complete
2026-06-05T00:01:00.139705161Z [inf]  NSFW channel seeded
2026-06-05T00:01:00.139711868Z [inf]  PREMIUM channel seeded
2026-06-05T00:01:00.139718774Z [inf]  Distribution channels seeded successfully
2026-06-05T00:01:00.141191110Z [inf]  lifecycle_bot_start
2026-06-05T00:01:00.489154803Z [err]  lifecycle_bot_start_failed
2026-06-05T00:01:00.489169103Z [inf]  lifecycle_shutdown_start
2026-06-05T00:01:00.491668796Z [err]  lifecycle_shutdown_bot_failed
2026-06-05T00:01:00.494075675Z [err]             ^^^^^^^^^^^^^^^^
2026-06-05T00:01:00.494090232Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 118, in run
2026-06-05T00:01:00.494093097Z [inf]  mongodb_connection_closed
2026-06-05T00:01:00.494104075Z [err]      return self._loop.run_until_complete(task)
2026-06-05T00:01:00.494104089Z [inf]  lifecycle_shutdown_complete
2026-06-05T00:01:00.494112247Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:00.494114483Z [inf]  Main process exit complete
2026-06-05T00:01:00.494119110Z [err]    File "/usr/local/lib/python3.12/asyncio/base_events.py", line 691, in run_until_complete
2026-06-05T00:01:00.494122691Z [err]  Traceback (most recent call last):
2026-06-05T00:01:00.494127485Z [err]      return future.result()
2026-06-05T00:01:00.494130261Z [err]    File "/app/main_bot.py", line 52, in <module>
2026-06-05T00:01:00.494135719Z [err]             ^^^^^^^^^^^^^^^
2026-06-05T00:01:00.494139127Z [err]      main()
2026-06-05T00:01:00.494145904Z [err]    File "/app/main_bot.py", line 48, in main
2026-06-05T00:01:00.494153669Z [err]      asyncio.run(async_main())
2026-06-05T00:01:00.494159463Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 195, in run
2026-06-05T00:01:00.494165327Z [err]      return runner.run(main)
2026-06-05T00:01:00.496615410Z [err]    File "/app/main_bot.py", line 33, in async_main
2026-06-05T00:01:00.496617015Z [err]    File "<frozen importlib._bootstrap>", line 1360, in _find_and_load
2026-06-05T00:01:00.496625590Z [err]    File "<frozen importlib._bootstrap>", line 1331, in _find_and_load_unlocked
2026-06-05T00:01:00.496627117Z [err]      await lifecycle.start()
2026-06-05T00:01:00.496631551Z [err]    File "<frozen importlib._bootstrap>", line 935, in _load_unlocked
2026-06-05T00:01:00.496636933Z [err]    File "<frozen importlib._bootstrap_external>", line 999, in exec_module
2026-06-05T00:01:00.496639356Z [err]    File "/app/app/core/lifecycle.py", line 73, in start
2026-06-05T00:01:00.496642817Z [err]    File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
2026-06-05T00:01:00.496649215Z [err]      await self._bot.start()
2026-06-05T00:01:00.496655899Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/utilities/start.py", line 75, in start
2026-06-05T00:01:00.496663075Z [err]      await self.initialize()
2026-06-05T00:01:00.496670139Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/auth/initialize.py", line 48, in initialize
2026-06-05T00:01:00.496677334Z [err]      self.load_plugins()
2026-06-05T00:01:00.496683801Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/client.py", line 927, in load_plugins
2026-06-05T00:01:00.496710071Z [err]      module = import_module(module_path)
2026-06-05T00:01:00.496716853Z [err]               ^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:00.496722814Z [err]    File "/usr/local/lib/python3.12/importlib/__init__.py", line 90, in import_module
2026-06-05T00:01:00.496729778Z [err]      return _bootstrap._gcd_import(name[level:], package, level)
2026-06-05T00:01:00.496737267Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:00.496744977Z [err]    File "<frozen importlib._bootstrap>", line 1387, in _gcd_import
2026-06-05T00:01:00.498459655Z [err]    File "/app/app/handlers/support_handler.py", line 12, in <module>
2026-06-05T00:01:00.498533851Z [err]      from aiogram import Bot, F, Router
2026-06-05T00:01:00.498550814Z [err]  ModuleNotFoundError: No module named 'aiogram'
2026-06-05T00:01:04.202766538Z [inf]  Initializing BDGW VaultFlow main process...
2026-06-05T00:01:04.202772169Z [inf]  lifecycle_bootstrapping_start
2026-06-05T00:01:04.202798014Z [inf]  Health server started
2026-06-05T00:01:04.202804917Z [inf]  mongodb_connection_start
2026-06-05T00:01:04.202811457Z [inf]  mongodb_connection_established
2026-06-05T00:01:04.202817569Z [inf]  mongodb_replica_set_detected
2026-06-05T00:01:04.202824893Z [inf]  migration_queue_stabilization_start
2026-06-05T00:01:04.203309798Z [inf]  migration_queue_stabilization_complete
2026-06-05T00:01:04.203314258Z [inf]  migration_vault_stabilization_start
2026-06-05T00:01:04.203317957Z [inf]  migration_vault_stabilization_complete
2026-06-05T00:01:04.350136941Z [inf]  mongodb_indexes_verified
2026-06-05T00:01:04.350143899Z [inf]  mongodb_indexes_verified
2026-06-05T00:01:04.421781664Z [inf]  referral_indexes_verified
2026-06-05T00:01:04.580814724Z [err]  Index creation error for payments
2026-06-05T00:01:04.641912580Z [inf]  Payment repository indexes created
2026-06-05T00:01:04.641917089Z [inf]  payment_indexes_verified
2026-06-05T00:01:05.411543194Z [inf]  lifecycle_shutdown_start
2026-06-05T00:01:05.411550434Z [err]  lifecycle_shutdown_bot_failed
2026-06-05T00:01:05.411954285Z [inf]  txid_indexes_verified
2026-06-05T00:01:05.411959926Z [inf]  mongodb_initialization_complete
2026-06-05T00:01:05.411965535Z [inf]  NSFW channel seeded
2026-06-05T00:01:05.411970659Z [inf]  PREMIUM channel seeded
2026-06-05T00:01:05.411978610Z [inf]  Distribution channels seeded successfully
2026-06-05T00:01:05.412000046Z [inf]  lifecycle_bot_start
2026-06-05T00:01:05.412005449Z [err]  lifecycle_bot_start_failed
2026-06-05T00:01:05.413282244Z [inf]  Main process exit complete
2026-06-05T00:01:05.413282663Z [err]             ^^^^^^^^^^^^^^^
2026-06-05T00:01:05.413290957Z [err]  Traceback (most recent call last):
2026-06-05T00:01:05.413296373Z [err]    File "/app/main_bot.py", line 52, in <module>
2026-06-05T00:01:05.413302809Z [err]      main()
2026-06-05T00:01:05.413308716Z [err]    File "/app/main_bot.py", line 48, in main
2026-06-05T00:01:05.413311192Z [inf]  mongodb_connection_closed
2026-06-05T00:01:05.413317158Z [err]      asyncio.run(async_main())
2026-06-05T00:01:05.413317251Z [inf]  lifecycle_shutdown_complete
2026-06-05T00:01:05.413323858Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 195, in run
2026-06-05T00:01:05.413328066Z [err]      return runner.run(main)
2026-06-05T00:01:05.413332629Z [err]             ^^^^^^^^^^^^^^^^
2026-06-05T00:01:05.413347506Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 118, in run
2026-06-05T00:01:05.413353028Z [err]      return self._loop.run_until_complete(task)
2026-06-05T00:01:05.413357129Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:05.413361220Z [err]    File "/usr/local/lib/python3.12/asyncio/base_events.py", line 691, in run_until_complete
2026-06-05T00:01:05.413365462Z [err]      return future.result()
2026-06-05T00:01:05.416888384Z [err]    File "<frozen importlib._bootstrap_external>", line 999, in exec_module
2026-06-05T00:01:05.416898653Z [err]    File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
2026-06-05T00:01:05.416906168Z [err]      await self._bot.start()
2026-06-05T00:01:05.416915844Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/utilities/start.py", line 75, in start
2026-06-05T00:01:05.416919943Z [err]      return _bootstrap._gcd_import(name[level:], package, level)
2026-06-05T00:01:05.416922408Z [err]      await self.initialize()
2026-06-05T00:01:05.416927245Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/auth/initialize.py", line 48, in initialize
2026-06-05T00:01:05.416932856Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:05.416933422Z [err]      self.load_plugins()
2026-06-05T00:01:05.416938630Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/client.py", line 927, in load_plugins
2026-06-05T00:01:05.416942416Z [err]    File "<frozen importlib._bootstrap>", line 1387, in _gcd_import
2026-06-05T00:01:05.416944159Z [err]      module = import_module(module_path)
2026-06-05T00:01:05.416949511Z [err]               ^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:05.416951891Z [err]    File "<frozen importlib._bootstrap>", line 1360, in _find_and_load
2026-06-05T00:01:05.416955125Z [err]    File "/usr/local/lib/python3.12/importlib/__init__.py", line 90, in import_module
2026-06-05T00:01:05.416959880Z [err]    File "<frozen importlib._bootstrap>", line 1331, in _find_and_load_unlocked
2026-06-05T00:01:05.416966845Z [err]    File "<frozen importlib._bootstrap>", line 935, in _load_unlocked
2026-06-05T00:01:05.417043537Z [err]    File "/app/main_bot.py", line 33, in async_main
2026-06-05T00:01:05.417049011Z [err]      await lifecycle.start()
2026-06-05T00:01:05.417053763Z [err]    File "/app/app/core/lifecycle.py", line 73, in start
2026-06-05T00:01:05.418839621Z [err]    File "/app/app/handlers/support_handler.py", line 12, in <module>
2026-06-05T00:01:05.418844843Z [err]      from aiogram import Bot, F, Router
2026-06-05T00:01:05.418893957Z [err]  ModuleNotFoundError: No module named 'aiogram'
2026-06-05T00:01:09.352156886Z [inf]  Initializing BDGW VaultFlow main process...
2026-06-05T00:01:09.352160691Z [inf]  lifecycle_bootstrapping_start
2026-06-05T00:01:09.352165040Z [inf]  Health server started
2026-06-05T00:01:09.352169118Z [inf]  mongodb_connection_start
2026-06-05T00:01:09.352172997Z [inf]  mongodb_connection_established
2026-06-05T00:01:09.352176928Z [inf]  mongodb_replica_set_detected
2026-06-05T00:01:09.352181160Z [inf]  migration_queue_stabilization_start
2026-06-05T00:01:09.354182373Z [inf]  referral_indexes_verified
2026-06-05T00:01:09.354189879Z [err]  Index creation error for payments
2026-06-05T00:01:09.354200542Z [inf]  migration_vault_stabilization_start
2026-06-05T00:01:09.354211529Z [inf]  migration_vault_stabilization_complete
2026-06-05T00:01:09.354211958Z [inf]  migration_queue_stabilization_complete
2026-06-05T00:01:09.354220233Z [inf]  mongodb_indexes_verified
2026-06-05T00:01:09.354225024Z [inf]  mongodb_indexes_verified
2026-06-05T00:01:09.414655719Z [inf]  Payment repository indexes created
2026-06-05T00:01:09.414665359Z [inf]  payment_indexes_verified
2026-06-05T00:01:09.445088673Z [inf]  txid_indexes_verified
2026-06-05T00:01:09.445094202Z [inf]  mongodb_initialization_complete
2026-06-05T00:01:09.461023517Z [inf]  NSFW channel seeded
2026-06-05T00:01:09.461032786Z [inf]  PREMIUM channel seeded
2026-06-05T00:01:09.461040918Z [inf]  Distribution channels seeded successfully
2026-06-05T00:01:09.461046911Z [inf]  lifecycle_bot_start
2026-06-05T00:01:10.427545884Z [err]  lifecycle_bot_start_failed
2026-06-05T00:01:10.427551804Z [inf]  lifecycle_shutdown_start
2026-06-05T00:01:10.429681159Z [err]  lifecycle_shutdown_bot_failed
2026-06-05T00:01:10.432676208Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:10.432684787Z [err]    File "/usr/local/lib/python3.12/asyncio/base_events.py", line 691, in run_until_complete
2026-06-05T00:01:10.432691211Z [err]      return future.result()
2026-06-05T00:01:10.432697820Z [err]             ^^^^^^^^^^^^^^^
2026-06-05T00:01:10.432714032Z [inf]  mongodb_connection_closed
2026-06-05T00:01:10.432719505Z [inf]  lifecycle_shutdown_complete
2026-06-05T00:01:10.432725397Z [inf]  Main process exit complete
2026-06-05T00:01:10.432731989Z [err]  Traceback (most recent call last):
2026-06-05T00:01:10.432738122Z [err]    File "/app/main_bot.py", line 52, in <module>
2026-06-05T00:01:10.432754570Z [err]      main()
2026-06-05T00:01:10.432760986Z [err]    File "/app/main_bot.py", line 48, in main
2026-06-05T00:01:10.432766951Z [err]      asyncio.run(async_main())
2026-06-05T00:01:10.432773177Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 195, in run
2026-06-05T00:01:10.432779558Z [err]      return runner.run(main)
2026-06-05T00:01:10.432785771Z [err]             ^^^^^^^^^^^^^^^^
2026-06-05T00:01:10.432792008Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 118, in run
2026-06-05T00:01:10.432797727Z [err]      return self._loop.run_until_complete(task)
2026-06-05T00:01:10.435323481Z [err]    File "/app/main_bot.py", line 33, in async_main
2026-06-05T00:01:10.435329227Z [err]      await lifecycle.start()
2026-06-05T00:01:10.435333558Z [err]      module = import_module(module_path)
2026-06-05T00:01:10.435335930Z [err]    File "/app/app/core/lifecycle.py", line 73, in start
2026-06-05T00:01:10.435343163Z [err]      await self._bot.start()
2026-06-05T00:01:10.435345996Z [err]               ^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:10.435350429Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/utilities/start.py", line 75, in start
2026-06-05T00:01:10.435355645Z [err]    File "/usr/local/lib/python3.12/importlib/__init__.py", line 90, in import_module
2026-06-05T00:01:10.435379030Z [err]      return _bootstrap._gcd_import(name[level:], package, level)
2026-06-05T00:01:10.435383064Z [err]      await self.initialize()
2026-06-05T00:01:10.435388326Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/auth/initialize.py", line 48, in initialize
2026-06-05T00:01:10.435394310Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:10.435394443Z [err]      self.load_plugins()
2026-06-05T00:01:10.435399317Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/client.py", line 927, in load_plugins
2026-06-05T00:01:10.435403138Z [err]    File "<frozen importlib._bootstrap>", line 1387, in _gcd_import
2026-06-05T00:01:10.435409634Z [err]    File "<frozen importlib._bootstrap>", line 1360, in _find_and_load
2026-06-05T00:01:10.435415548Z [err]    File "<frozen importlib._bootstrap>", line 1331, in _find_and_load_unlocked
2026-06-05T00:01:10.435421229Z [err]    File "<frozen importlib._bootstrap>", line 935, in _load_unlocked
2026-06-05T00:01:10.435427332Z [err]    File "<frozen importlib._bootstrap_external>", line 999, in exec_module
2026-06-05T00:01:10.435433410Z [err]    File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
2026-06-05T00:01:10.438433661Z [err]    File "/app/app/handlers/support_handler.py", line 12, in <module>
2026-06-05T00:01:10.438440065Z [err]      from aiogram import Bot, F, Router
2026-06-05T00:01:10.438446495Z [err]  ModuleNotFoundError: No module named 'aiogram'
2026-06-05T00:01:13.588796594Z [inf]  Initializing BDGW VaultFlow main process...
2026-06-05T00:01:13.598283583Z [inf]  lifecycle_bootstrapping_start
2026-06-05T00:01:13.598293530Z [inf]  Health server started
2026-06-05T00:01:13.598300272Z [inf]  mongodb_connection_start
2026-06-05T00:01:14.271472959Z [inf]  mongodb_connection_established
2026-06-05T00:01:14.271486065Z [inf]  mongodb_replica_set_detected
2026-06-05T00:01:14.271493109Z [inf]  migration_queue_stabilization_start
2026-06-05T00:01:14.271499328Z [inf]  migration_queue_stabilization_complete
2026-06-05T00:01:14.271521793Z [inf]  migration_vault_stabilization_start
2026-06-05T00:01:14.271528702Z [inf]  migration_vault_stabilization_complete
2026-06-05T00:01:14.271535639Z [inf]  mongodb_indexes_verified
2026-06-05T00:01:14.272082041Z [inf]  mongodb_indexes_verified
2026-06-05T00:01:14.272087875Z [inf]  referral_indexes_verified
2026-06-05T00:01:14.373597752Z [err]  Index creation error for payments
2026-06-05T00:01:14.434959453Z [inf]  Payment repository indexes created
2026-06-05T00:01:14.434963910Z [inf]  payment_indexes_verified
2026-06-05T00:01:14.477770517Z [inf]  txid_indexes_verified
2026-06-05T00:01:14.477774669Z [inf]  mongodb_initialization_complete
2026-06-05T00:01:14.485784535Z [inf]  NSFW channel seeded
2026-06-05T00:01:14.487825941Z [inf]  PREMIUM channel seeded
2026-06-05T00:01:14.487832478Z [inf]  Distribution channels seeded successfully
2026-06-05T00:01:14.487838191Z [inf]  lifecycle_bot_start
2026-06-05T00:01:15.365040965Z [err]  lifecycle_bot_start_failed
2026-06-05T00:01:15.365050254Z [inf]  lifecycle_shutdown_start
2026-06-05T00:01:15.366062544Z [err]  lifecycle_shutdown_bot_failed
2026-06-05T00:01:15.367905199Z [err]    File "/app/main_bot.py", line 52, in <module>
2026-06-05T00:01:15.367905943Z [err]      return future.result()
2026-06-05T00:01:15.367914839Z [err]      main()
2026-06-05T00:01:15.367917776Z [err]             ^^^^^^^^^^^^^^^
2026-06-05T00:01:15.367921379Z [err]    File "/app/main_bot.py", line 48, in main
2026-06-05T00:01:15.367928306Z [err]      asyncio.run(async_main())
2026-06-05T00:01:15.367932506Z [inf]  mongodb_connection_closed
2026-06-05T00:01:15.367935852Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 195, in run
2026-06-05T00:01:15.367938936Z [inf]  lifecycle_shutdown_complete
2026-06-05T00:01:15.367944135Z [err]      return runner.run(main)
2026-06-05T00:01:15.367944320Z [inf]  Main process exit complete
2026-06-05T00:01:15.367952280Z [err]  Traceback (most recent call last):
2026-06-05T00:01:15.367952726Z [err]             ^^^^^^^^^^^^^^^^
2026-06-05T00:01:15.367958835Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 118, in run
2026-06-05T00:01:15.367964716Z [err]      return self._loop.run_until_complete(task)
2026-06-05T00:01:15.367970499Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:15.367975404Z [err]    File "/usr/local/lib/python3.12/asyncio/base_events.py", line 691, in run_until_complete
2026-06-05T00:01:15.370690889Z [err]    File "/app/main_bot.py", line 33, in async_main
2026-06-05T00:01:15.370700409Z [err]      await lifecycle.start()
2026-06-05T00:01:15.370707796Z [err]    File "/app/app/core/lifecycle.py", line 73, in start
2026-06-05T00:01:15.370711477Z [err]      return _bootstrap._gcd_import(name[level:], package, level)
2026-06-05T00:01:15.370714887Z [err]      await self._bot.start()
2026-06-05T00:01:15.370721357Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/utilities/start.py", line 75, in start
2026-06-05T00:01:15.370726269Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:15.370728643Z [err]      await self.initialize()
2026-06-05T00:01:15.370735778Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/auth/initialize.py", line 48, in initialize
2026-06-05T00:01:15.370738251Z [err]    File "<frozen importlib._bootstrap>", line 1387, in _gcd_import
2026-06-05T00:01:15.370742840Z [err]      self.load_plugins()
2026-06-05T00:01:15.370749096Z [err]    File "<frozen importlib._bootstrap>", line 1360, in _find_and_load
2026-06-05T00:01:15.370751427Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/client.py", line 927, in load_plugins
2026-06-05T00:01:15.370759724Z [err]      module = import_module(module_path)
2026-06-05T00:01:15.370760325Z [err]    File "<frozen importlib._bootstrap>", line 1331, in _find_and_load_unlocked
2026-06-05T00:01:15.370767134Z [err]               ^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:15.370772395Z [err]    File "<frozen importlib._bootstrap>", line 935, in _load_unlocked
2026-06-05T00:01:15.370773707Z [err]    File "/usr/local/lib/python3.12/importlib/__init__.py", line 90, in import_module
2026-06-05T00:01:15.370794364Z [err]    File "<frozen importlib._bootstrap_external>", line 999, in exec_module
2026-06-05T00:01:15.370802792Z [err]    File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
2026-06-05T00:01:15.375480681Z [err]    File "/app/app/handlers/support_handler.py", line 12, in <module>
2026-06-05T00:01:15.375485527Z [err]      from aiogram import Bot, F, Router
2026-06-05T00:01:15.375490322Z [err]  ModuleNotFoundError: No module named 'aiogram'
2026-06-05T00:01:18.287562711Z [inf]  mongodb_replica_set_detected
2026-06-05T00:01:18.287570240Z [inf]  migration_queue_stabilization_start
2026-06-05T00:01:18.287683328Z [inf]  Initializing BDGW VaultFlow main process...
2026-06-05T00:01:18.287689741Z [inf]  lifecycle_bootstrapping_start
2026-06-05T00:01:18.287696603Z [inf]  Health server started
2026-06-05T00:01:18.287703510Z [inf]  mongodb_connection_start
2026-06-05T00:01:18.287710441Z [inf]  mongodb_connection_established
2026-06-05T00:01:18.288618091Z [inf]  migration_queue_stabilization_complete
2026-06-05T00:01:18.288623385Z [inf]  migration_vault_stabilization_start
2026-06-05T00:01:18.288627836Z [inf]  migration_vault_stabilization_complete
2026-06-05T00:01:18.525426302Z [inf]  mongodb_indexes_verified
2026-06-05T00:01:18.525526729Z [inf]  mongodb_indexes_verified
2026-06-05T00:01:18.591990038Z [inf]  referral_indexes_verified
2026-06-05T00:01:18.780123011Z [err]  Index creation error for payments
2026-06-05T00:01:19.441573510Z [inf]  PREMIUM channel seeded
2026-06-05T00:01:19.441595097Z [inf]  Distribution channels seeded successfully
2026-06-05T00:01:19.441930566Z [inf]  Payment repository indexes created
2026-06-05T00:01:19.441958099Z [inf]  payment_indexes_verified
2026-06-05T00:01:19.441967245Z [inf]  txid_indexes_verified
2026-06-05T00:01:19.441975352Z [inf]  mongodb_initialization_complete
2026-06-05T00:01:19.441984873Z [inf]  NSFW channel seeded
2026-06-05T00:01:19.444292459Z [inf]  lifecycle_bot_start
2026-06-05T00:01:19.444298555Z [err]  lifecycle_bot_start_failed
2026-06-05T00:01:19.446612514Z [inf]  lifecycle_shutdown_start
2026-06-05T00:01:19.446618925Z [err]  lifecycle_shutdown_bot_failed
2026-06-05T00:01:19.449096570Z [inf]  mongodb_connection_closed
2026-06-05T00:01:19.449105391Z [err]             ^^^^^^^^^^^^^^^
2026-06-05T00:01:19.449106348Z [inf]  lifecycle_shutdown_complete
2026-06-05T00:01:19.449112931Z [inf]  Main process exit complete
2026-06-05T00:01:19.449118142Z [err]  Traceback (most recent call last):
2026-06-05T00:01:19.449123479Z [err]    File "/app/main_bot.py", line 52, in <module>
2026-06-05T00:01:19.449128859Z [err]      main()
2026-06-05T00:01:19.449134905Z [err]    File "/app/main_bot.py", line 48, in main
2026-06-05T00:01:19.449140160Z [err]      asyncio.run(async_main())
2026-06-05T00:01:19.449145612Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 195, in run
2026-06-05T00:01:19.449150716Z [err]      return runner.run(main)
2026-06-05T00:01:19.449155993Z [err]             ^^^^^^^^^^^^^^^^
2026-06-05T00:01:19.449161837Z [err]    File "/usr/local/lib/python3.12/asyncio/runners.py", line 118, in run
2026-06-05T00:01:19.449166886Z [err]      return self._loop.run_until_complete(task)
2026-06-05T00:01:19.449172464Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:19.449177310Z [err]    File "/usr/local/lib/python3.12/asyncio/base_events.py", line 691, in run_until_complete
2026-06-05T00:01:19.449182575Z [err]      return future.result()
2026-06-05T00:01:19.452873012Z [err]    File "/app/main_bot.py", line 33, in async_main
2026-06-05T00:01:19.452875813Z [err]               ^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:19.452883929Z [err]      await lifecycle.start()
2026-06-05T00:01:19.452891631Z [err]    File "/app/app/core/lifecycle.py", line 73, in start
2026-06-05T00:01:19.452897388Z [err]      await self._bot.start()
2026-06-05T00:01:19.452904349Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/utilities/start.py", line 75, in start
2026-06-05T00:01:19.452908396Z [err]    File "/usr/local/lib/python3.12/importlib/__init__.py", line 90, in import_module
2026-06-05T00:01:19.452911616Z [err]      await self.initialize()
2026-06-05T00:01:19.452918413Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/methods/auth/initialize.py", line 48, in initialize
2026-06-05T00:01:19.452921181Z [err]      return _bootstrap._gcd_import(name[level:], package, level)
2026-06-05T00:01:19.452926545Z [err]      self.load_plugins()
2026-06-05T00:01:19.452930449Z [err]             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
2026-06-05T00:01:19.452934207Z [err]    File "/usr/local/lib/python3.12/site-packages/pyrogram/client.py", line 927, in load_plugins
2026-06-05T00:01:19.452940071Z [err]    File "<frozen importlib._bootstrap>", line 1387, in _gcd_import
2026-06-05T00:01:19.452941715Z [err]      module = import_module(module_path)
2026-06-05T00:01:19.452948226Z [err]    File "<frozen importlib._bootstrap>", line 1360, in _find_and_load
2026-06-05T00:01:19.452954738Z [err]    File "<frozen importlib._bootstrap>", line 1331, in _find_and_load_unlocked
2026-06-05T00:01:19.452961204Z [err]    File "<frozen importlib._bootstrap>", line 935, in _load_unlocked
2026-06-05T00:01:19.452967635Z [err]    File "<frozen importlib._bootstrap_external>", line 999, in exec_module
2026-06-05T00:01:19.452974313Z [err]    File "<frozen importlib._bootstrap>", line 488, in _call_with_frames_removed
2026-06-05T00:01:19.454436222Z [err]    File "/app/app/handlers/support_handler.py", line 12, in <module>
2026-06-05T00:01:19.454442965Z [err]      from aiogram import Bot, F, Router
2026-06-05T00:01:19.454447736Z [err]  ModuleNotFoundError: No module named 'aiogram'

