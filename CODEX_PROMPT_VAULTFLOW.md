# CODEX CLI — BDGW VaultFlow Production Hardening Mission

## OPERATOR CONTEXT

You are operating as a **senior backend architect + Telegram infrastructure engineer** on the
**BDGW VaultFlow** project. This is a production Telegram bot built on:

- Python 3.11+
- Pyrogram v2
- MongoDB + Motor (async)
- APScheduler
- Structlog (structured JSON logging only)
- AsyncIO
- FFmpeg (watermark pipeline)
- Redis (async client, already initialised at boot)

The infrastructure runtime is **already working**:
- MongoDB connects and indexes verify
- APScheduler starts cleanly
- Worker pools start (dispatcher, watermark, subscription)
- Pyrogram connects and confirms channel access
- Lifecycle orchestration (start/shutdown) is stable
- Structured logging is operational
- Redis client initialises successfully

**Do NOT rewrite the project. Do NOT collapse the architecture. Do NOT monolithify.**

---

## PRIMARY MISSION

Fix all broken event flow, handler registration, and runtime errors. Then implement the
complete premium payment moderation system as described below. Leave every working
subsystem untouched unless it is directly causing a confirmed bug.

---

## PHASE 1 — RUNTIME TRIAGE (Fix First, Touch Nothing Else)

### 1.1 — `CallbackQuery` has no attribute `chat` (CRITICAL)

**Symptom from logs:**
```
'CallbackQuery' object has no attribute 'chat'
```
Appearing on every callback_query that hits a handler using `callback.chat`.

**Root cause:** Pyrogram's `CallbackQuery` object does not have a `.chat` attribute.
The correct path is `callback.message.chat` (if from an inline message) or
`callback.from_user` for the user context.

**Fix required:**
- Audit every handler file for any usage of `callback.chat`, `query.chat`, `update.chat`
  on a `CallbackQuery` object.
- Replace with the correct attribute:
  - For chat context: `callback.message.chat`
  - For user context: `callback.from_user`
  - For chat_id: `callback.message.chat.id`
- Do NOT change any handler that already correctly uses `.message.chat`.
- After fix, every callback handler must `await callback.answer()` before doing any
  heavy async work to prevent Telegram timeout on the button press.

### 1.2 — Handler Registration Audit

**Symptom from logs:**
```
STARTUP AUDIT: handler group registered   (×2)
STARTUP AUDIT: handler registration complete
```
Only 2 handler groups are registering. Submissions are not routing. Moderation pipeline
is not triggering.

**Fix required:**
- Locate the plugin/handler bootstrap module (likely `plugins/`, `handlers/`, or a
  `loader.py` / `bootstrap.py`).
- List every handler module that exists on disk.
- Verify each module is being imported and its handlers are being registered with the
  Pyrogram client via `app.add_handler(...)` or equivalent plugin loading.
- If plugin auto-loading exists (e.g., `pyrogram.Client` with `plugins=` kwarg), verify
  the `root` path is correct and all handler files follow the required naming pattern.
- Add a structured log line per registered handler group so the audit log shows every
  group, not just 2.
- If any handler file is present on disk but NOT loaded, fix the loader — do not delete
  the handler file.

### 1.3 — `referral_system_initialization_failed` (ERROR at boot)

**Symptom:**
```
referral_system_initialization_failed
```

**Fix required:**
- Locate the referral system initialisation path.
- Identify the exact exception being swallowed.
- Add proper structured error logging: log the exception class, message, and traceback.
- Fix the root cause (likely a missing collection, missing index, or missing config key).
- If referral system is optional at boot, make the failure non-fatal but log a clear
  `[wrn]` warning, not a silent `[err]` with no detail.

### 1.4 — `non_core_index_setup_failed` (ERROR at boot)

**Symptom:**
```
non_core_index_setup_failed
```

**Fix required:**
- Locate the index setup function that raises this.
- Log the specific index name, collection, and exception detail — not just the label.
- Fix the failing index definition (likely a missing field spec or bad index option).
- All indexes must be idempotent (`background=True`, ignore `IndexAlreadyExists`).

### 1.5 — `forum_topic_creation_failed` (×4 on support flow)

**Symptom:**
```
forum_topic_creation_failed   (×4 rapid succession)
```
Triggering when user hits support menu.

**Fix required:**
- Locate the forum topic creation call in the support handler.
- Wrap with proper exception capture and log the Telegram error response.
- Add retry logic with exponential backoff (max 3 retries, 2/4/8 second delays).
- If topic already exists for this user, reuse it — do NOT attempt to create a duplicate.
- Check that the target group has forum topics enabled; if `CHANNEL_INVALID` or
  `CHAT_ADMIN_REQUIRED` is returned, log a clear actionable warning and fall back
  gracefully (do not crash the handler).
- Ensure the support system uses a **separate** group/topic from the payment moderation
  system. These must NEVER share topics.

### 1.6 — `PREMIUM_CHANNEL_ID` Access Failure

**Symptom:**
```
WARNING: Failed to access channel PREMIUM_CHANNEL_ID (-1002857697115).
Error: [400 CHANNEL_INVALID]
```

**Fix required:**
- Verify the `PREMIUM_CHANNEL_ID` value in config/environment. The channel ID may be
  incorrect, or the bot may not be a member.
- Do NOT crash or block startup on this failure — it is already non-fatal.
- Add a clear `[wrn]` log that says exactly: "PREMIUM_CHANNEL_ID is unreachable —
  invite link generation will fail until this is resolved."
- Any code path that calls `create_chat_invite_link` on this channel must check
  channel access status first and return a structured error to the user if unavailable.

### 1.7 — Duplicate `UPDATE_TRACE` Emissions

**Symptom:** Multiple `UPDATE_TRACE: callback_query` lines emitted simultaneously for
the same user interaction (up to ×6 for one button press).

**Fix required:**
- Audit the update middleware or trace logging hook.
- Ensure `UPDATE_TRACE` is emitted exactly once per incoming update, at the entry point,
  before handler dispatch.
- If multiple handler groups are each emitting the trace, centralise it into middleware
  only.

---

## PHASE 2 — PREMIUM PAYMENT MODERATION SYSTEM (Full Implementation)

Implement the complete payment system described below. This must be **fully isolated**
from: support system, verification hub, anonymous moderation, content moderation,
vault distribution, scheduler, and creator workflow.

### 2.1 — Architecture Requirements

**New files to create (do not add code to existing files):**

```
bot/
  handlers/
    premium/
      __init__.py
      plan_selection.py       # User plan selection flow
      payment_initiation.py   # Payment method + request creation
      txid_submission.py      # TXID + screenshot collection
      user_status.py          # /mystatus dashboard
  services/
    payment_service.py        # All payment business logic
    subscription_service.py   # Subscription activation + expiry
    invite_service.py         # Invite link generation
    referral_service.py       # Referral balance + discount logic
  repositories/
    payment_repository.py     # All DB access for payments
    subscription_repository.py
  workers/
    payment_recovery_worker.py  # Restart-safe recovery worker
    subscription_expiry_worker.py
  models/
    payment_models.py
```

**Dependency direction must be preserved:**
`config → database → repositories → services → workers → handlers`
Never import a handler from a service. Never import a service from a repository.

### 2.2 — Database Collections

Create these collections with the following schemas and indexes.

**Collection: `payments`**
```python
{
    "payment_id": int,           # auto-increment counter
    "request_id": int,           # auto-increment counter
    "user_id": int,
    "username": str,
    "display_name": str,
    "plan": str,                 # "1_month" | "3_month" | "6_month"
    "original_amount": int,      # before referral discount
    "referral_discount": int,    # points used (0 if none)
    "amount": int,               # LOCKED final amount — never recalculate
    "payment_method": str,       # "bkash" | "nagad" | "crypto"
    "status": str,               # see state machine below
    "txid": str | None,
    "screenshot_file_id": str | None,
    "proof_message_id": int | None,
    "moderation_message_id": int | None,
    "moderation_topic_id": int | None,
    "admin_id": int | None,
    "approved_by": int | None,
    "rejected_by": int | None,
    "rejection_reason": str | None,
    "invite_link": str | None,
    "payment_details_sent_at": datetime | None,  # delivery-confirmed timestamp
    "expires_at": datetime | None,               # set AFTER delivery confirmed
    "created_at": datetime,
    "approved_at": datetime | None,
    "rejected_at": datetime | None,
}
```

Required indexes on `payments`:
- `user_id` (ascending)
- `status` (ascending)
- `payment_id` (unique, ascending)
- `(user_id, status)` compound — for active payment lookup
- TTL index: `expires_at` with `expireAfterSeconds=0` — Mongo auto-cleans expired docs
  (keep this — just set the field when creating)

**Collection: `payment_topics`**
```python
{
    "user_id": int,
    "topic_id": int,
    "thread_id": int | None,
    "group_id": int,
    "active": bool,
    "created_at": datetime,
}
```

Index: `user_id` (unique) — one active topic per user.

**Collection: `subscriptions`**
```python
{
    "user_id": int,
    "plan": str,
    "started_at": datetime,
    "expires_at": datetime,
    "payment_id": int,
    "invite_link": str,
    "status": str,     # "active" | "expired" | "cancelled"
    "channel_id": int,
}
```

Index: `user_id` (ascending), `expires_at` (ascending for expiry worker).

### 2.3 — Payment State Machine

Valid statuses and allowed transitions:

```
waiting_payment_details  →  waiting_txid         (admin sends details, delivery confirmed)
waiting_txid             →  waiting_screenshot   (user submits TXID)
waiting_screenshot       →  submitted            (user submits screenshot OR skips)
submitted                →  processing           (atomic: admin clicks Approve/Reject)
processing               →  approved             (approval flow completes)
processing               →  rejected             (rejection flow completes)
[any active state]       →  expired              (timeout worker fires)
[any active state]       →  cancelled            (user cancels or admin cancels)
```

**CRITICAL:** The transition `submitted → processing` MUST be atomic.
Use `find_one_and_update` with filter `{"payment_id": X, "status": "submitted"}`.
If the document is not found, another admin already claimed it — return a conflict error.
Never use a read-then-write pattern for this transition.

### 2.4 — Plan Display

When user clicks 💎 Join Premium, show all three plans.
Each plan card must display:
- Duration and price (৳)
- 3 bullet-point benefits
- Whether referral discount is applicable
- Any active promotion (read from config — leave as empty string if none)

Plans:
| Plan     | Price  | Duration  |
|----------|--------|-----------|
| 1 Month  | ৳499   | 30 days   |
| 3 Months | ৳1299  | 90 days   |
| 6 Months | ৳2199  | 180 days  |

Buttons per plan: `🚀 Join Now` | `⏰ Join Later`
`Join Later` dismisses with a message: "No problem! You can join anytime from the menu."

### 2.5 — Referral Discount Logic

Before showing checkout:
1. Query referral balance from `referral_repository` (do not inline DB access in handler).
2. If balance > 0, show: "You have X referral points = ৳X discount available."
3. Ask user to enter discount amount (0 to max balance, integer only).
4. Validate input is integer, ≤ balance, ≤ plan price.
5. Calculate final amount = plan price - discount.
6. **Snapshot and lock this amount into the payment document at creation.**
7. Deduct used points from referral balance atomically at payment creation.
8. If payment is rejected or expired, restore the referral points.

### 2.6 — Payment Request Creation Flow

After method selection:
1. Check for existing active payment for this user (status not in `approved`, `rejected`,
   `expired`, `cancelled`). If found, show: "You have a pending payment. Complete or
   cancel it before starting a new one." with a button to view it.
2. Create payment document with status `waiting_payment_details`.
3. Post request card to admin payment moderation group/topic with button:
   `📩 Send Payment Details`
4. Confirm to user: "Your request has been sent to the admin team. Please wait for
   payment details to be sent to you."

Admin moderation card format:
```
💰 Premium Join Request

👤 User: {display_name} (@{username})
🆔 User ID: {user_id}

📦 Plan: {plan_label}
💰 Amount: ৳{amount}
📱 Method: {method}
🔗 Request ID: #{request_id}

User is waiting for payment details.
```
Button: `📩 Send Payment Details`

### 2.7 — Admin Send Payment Details Flow

When admin clicks `📩 Send Payment Details`:
1. Validate admin permission (check `ADMIN_IDS` from config).
2. Enter admin-scoped FSM state `AdminPaymentDetailEntry` keyed on `(admin_id, payment_id)`.
3. Prompt admin in the group topic: "Send the payment number, QR code, or wallet address."
4. Admin sends message (text or photo).
5. Bot relays payment details to the user's DM.
6. **ONLY after successful Telegram send to user:**
   - Record `payment_details_sent_at = now()`
   - Set `expires_at = now() + 20 minutes`
   - Update status to `waiting_txid`
   - Schedule timeout reminders (see §2.8)
7. Confirm to admin: "Payment details delivered to user. Timer started."

**NEVER set `expires_at` before confirming delivery.**

### 2.8 — Timeout Warning System

After delivery-confirmed activation, schedule three async tasks:
- `+10 minutes`: warn user "⏰ 10 minutes remaining to submit your payment."
- `+15 minutes`: warn user "⚠️ 5 minutes remaining! Submit TXID now."
- `+20 minutes`: expire the payment:
  - Set status to `expired`
  - Restore referral points if any were deducted
  - Notify user: "⌛ Payment session expired. Please start a new request."
  - Notify admin topic: "Session expired for Request #{request_id}."
  - Clear any FSM state for this user

All timeout tasks must:
- Check current payment status before executing (payment may have been submitted before
  timeout fires — abort if status is no longer `waiting_txid` or `waiting_screenshot`).
- Be stored as asyncio tasks and tracked to prevent garbage collection.
- Be cancellable via task reference keyed on `payment_id`.

### 2.9 — TXID Submission Flow

User receives prompt:
```
🔒 Secure Payment Submission

Please send your Transaction ID (TXID).
This can be the last 4–6 digits or full reference number.
```

After TXID received:
1. Validate payment is in `waiting_txid` status.
2. Validate payment belongs to this user.
3. Validate payment is not expired.
4. Store TXID.
5. Update status to `waiting_screenshot`.
6. Prompt: "Screenshot uploaded (optional). Send a screenshot of your payment
   confirmation, or type `skip` to continue without one."

After screenshot or skip:
1. Update status to `submitted`.
2. Store `screenshot_file_id` if provided.
3. Send confirmation to user.
4. Post proof moderation card to admin topic (see §2.10).

### 2.10 — Payment Proof Moderation Card

```
💰 New Payment Proof

👤 User: {display_name} (@{username})
📦 Plan: {plan_label}
🔑 TX ID: {txid}
💰 Amount: ৳{amount}
📱 Method: {method}
🆔 Payment ID: #{payment_id}
🔗 Request ID: #{request_id}
```
(If screenshot provided, send screenshot as media with this caption)

Buttons: `✅ Approve` | `❌ Reject`

### 2.11 — Approval Flow (Atomic)

When admin clicks `✅ Approve`:
1. **Atomic state transition:** `find_one_and_update` with
   `filter={"payment_id": X, "status": "submitted"}`,
   `update={"$set": {"status": "processing", "admin_id": admin_id}}`.
2. If document not returned → reply to admin: "This payment is already being processed
   by another admin." Stop.
3. Validate payment exists, is not expired, is not already approved.
4. Call `subscription_service.activate(user_id, plan, payment_id)`.
5. Call `invite_service.generate(channel_id)` → returns unique invite link.
6. Store invite link in payment document.
7. Create subscription document.
8. Send to user:
```
✅ Payment Approved!

📦 Plan: {plan_label}
💰 Amount: ৳{amount}

🔓 Your premium access is ready.

👇 Join using your private invite link:
[JOIN PREMIUM] → {invite_link}

⚠️ This link is for you only. Do not share it.
```
9. Mark payment `approved`, set `approved_by`, `approved_at`.
10. Cancel active timeout tasks for this payment.
11. Update moderation card in admin topic: "✅ Approved by {admin_name} at {time}."

**Invite links must use `create_chat_invite_link` with:**
- `member_limit=1` (single-use)
- `expire_date = now() + 7 days` (link validity)
- `name = f"payment_{payment_id}"`

### 2.12 — Rejection Flow (Two-Step)

When admin clicks `❌ Reject`:
1. Show rejection reason panel (do NOT process rejection yet):
```
Buttons:
• ❌ Invalid TX ID
• ❌ Wrong Amount
• ❌ Duplicate TX
• ❌ Screenshot Unclear
• ✏️ Custom Reason
• ← Back
```
2. On preset selection → proceed to §2.13.
3. On `✏️ Custom Reason` → enter admin FSM state `AdminRejectionReason`, prompt:
   "Type your rejection reason." → on receipt, proceed to §2.13.

### 2.13 — Rejection Execution

1. **Atomic state transition** same as approval: `submitted → processing`.
2. If conflict → reply: "Already being processed." Stop.
3. Store rejection reason.
4. Mark payment `rejected`, set `rejected_by`, `rejected_at`.
5. Restore referral points atomically.
6. Send to user:
```
❌ Payment Verification Failed

Reason: {reason}

Please verify your payment details and try again.
```
Buttons: `🔄 Retry Payment` | `🆘 Contact Support`
7. Clear processing lock (set status `rejected`).
8. Update admin moderation card: "❌ Rejected by {admin_name}: {reason}"

`🔄 Retry Payment` must start a fresh payment flow (do NOT reuse the rejected payment
document — create a new one). Old document stays in DB for audit.

### 2.14 — `/mystatus` Dashboard

Command or menu button `📊 My Status` shows:

```
📊 Your Account Status

━━━━━━━━━━━━━━
💎 Subscription
━━━━━━━━━━━━━━
Status: Active / Inactive
Plan: {plan_label}
Expires: {date} ({N} days remaining)

━━━━━━━━━━━━━━
🎁 Referral Balance
━━━━━━━━━━━━━━
Points: {N} = ৳{N} discount

━━━━━━━━━━━━━━
💳 Payment History
━━━━━━━━━━━━━━
{last 5 payments, each line:}
• #{payment_id} | {plan} | ৳{amount} | {status} | {date}

━━━━━━━━━━━━━━
⏳ Pending
━━━━━━━━━━━━━━
{if pending payment exists: show status and request_id}
{if none: "No pending payments."}
```

All data from repositories only. No direct collection access in handler.

### 2.15 — Restart-Safe Payment Recovery Worker

On every bot startup (after DB connects, before Pyrogram starts):

1. Query all payments with status in:
   `[waiting_payment_details, waiting_txid, waiting_screenshot, submitted, processing]`

2. For each found payment:

   - **`processing` status:** These are stuck (crashed mid-approval/rejection).
     Reset to `submitted`. Log: `payment_stuck_processing_reset | payment_id={X}`.

   - **`waiting_txid` or `waiting_screenshot`:** Check if `expires_at < now()`.
     If expired: mark `expired`, restore referral points, send expiry message to user
     (best-effort, swallow Telegram errors). Log: `payment_expired_on_recovery`.

   - **`waiting_payment_details`:** These are waiting for admin. No recovery action
     needed — just log count.

   - **`submitted`:** These are waiting for admin review. Re-post moderation card
     if `moderation_message_id` is None (message was lost). Log: `payment_reposted_to_moderation`.

3. For any `waiting_txid`/`waiting_screenshot` that is still valid (not expired):
   Reschedule timeout tasks with remaining duration = `expires_at - now()`.

4. Log summary: `payment_recovery_complete | recovered={N} | expired={N} | reset={N}`

---

## PHASE 3 — SUPPORT SYSTEM ISOLATION

**Current issue:** Support topic creation is failing and firing ×4 per interaction.

**Fix required:**
- Ensure support handler only calls topic creation once per user (check for existing
  topic first using `support_topics` collection or equivalent).
- Support topics MUST use a different group than payment moderation topics.
  They must be configured as separate environment variables:
  `SUPPORT_GROUP_ID` and `PAYMENT_MODERATION_GROUP_ID`.
- Add idempotency: if topic exists, reuse it.
- Support message forwarding must NEVER route into payment moderation topic and vice versa.
- Log which topic_id is being used at the start of every support interaction.

---

## PHASE 4 — OBSERVABILITY HARDENING

### 4.1 — Exception Logging Rule

Every `except` clause in the entire codebase must:
1. Log the exception using structlog with the pattern:
   ```python
   log.error("event_label", error=str(e), exc_info=True)
   ```
2. Never silently pass.
3. Never log only a label without the error detail.

Audit all `except` blocks and fix any that only emit a label string (like
`referral_system_initialization_failed` with no detail).

### 4.2 — Handler Entry Logging

Every handler entry point must log:
```python
log.info("handler_entered", handler="handler_name", user_id=user_id, update_type="...")
```
This must fire before any business logic.

### 4.3 — Structured Log Field Standard

All log calls must include at minimum:
- `event` (the structlog event string — the label)
- Relevant entity IDs (`user_id`, `payment_id`, `topic_id`, etc.)
- No f-string interpolation into the event label — use keyword args.

---

## PHASE 5 — PRODUCTION RULES AUDIT

Run this checklist against the entire codebase and fix every violation:

| Rule | Check |
|------|-------|
| No `subprocess.run` in async paths | Use `asyncio.create_subprocess_exec` |
| No direct collection access in handlers | All DB via repositories |
| No direct collection access in services | All DB via repositories only |
| No global mutable state | Audit module-level variables |
| No wildcard imports (`from x import *`) | Replace with explicit imports |
| No circular imports | Verify dependency direction |
| No unbounded in-memory queues | Use `asyncio.Queue(maxsize=N)` |
| All `asyncio.create_task` calls tracked | Store reference, handle cancellation |
| All FFmpeg calls have timeouts | `asyncio.wait_for(proc, timeout=300)` |
| All FFmpeg temp files cleaned | Use `try/finally` |
| FloodWait handling on all Telegram sends | Catch `FloodWait`, sleep, retry |
| All queue workers have graceful shutdown | Check for stop event in loop |
| All scheduled jobs delegate to services | No business logic in APScheduler jobs |

---

## PHASE 6 — FINAL VALIDATION CHECKLIST

Before declaring the work complete, verify:

- [ ] Bot starts cleanly with 0 `[err]` log lines (excluding PREMIUM_CHANNEL_ID warning
      which is environment-dependent)
- [ ] All handler groups appear in startup audit log (not just 2)
- [ ] Sending `/start` routes correctly and shows main menu
- [ ] Sending a callback does NOT produce `'CallbackQuery' object has no attribute 'chat'`
- [ ] Clicking `💎 Join Premium` shows plan selection
- [ ] Full payment flow completes end-to-end in test
- [ ] Atomic approval lock prevents double-approval
- [ ] Timeout fires correctly after delivery confirmation
- [ ] `/mystatus` returns correct dashboard
- [ ] Bot restart recovers stuck payments
- [ ] Support and payment moderation use separate topics
- [ ] All exceptions log full detail (not just label)
- [ ] No test or debug code left in any handler

---

## CONSTRAINTS — DO NOT VIOLATE

1. **Do NOT rewrite the project.** Fix and extend only.
2. **Do NOT create monolithic files.** Max ~150 lines per handler module.
3. **Do NOT mix moderation systems.** Payment, support, and verification are isolated.
4. **Do NOT start payment timeout before delivery confirmation.**
5. **Do NOT use FSM as source of truth.** DB is the source of truth. FSM assists UI only.
6. **Do NOT use `subprocess.run` in any async context.**
7. **Do NOT access MongoDB collections directly from handlers or services.** Repositories only.
8. **Do NOT add print() statements.** Structlog only.
9. **Do NOT collapse or merge existing working modules.**
10. **Do NOT remove any existing structured logging.** Only add to it.

---

## EXECUTION ORDER

Execute phases in this order. Do not skip phases:

1. **PHASE 1** — Fix all runtime errors (callback.chat, handler registration, boot errors)
2. **PHASE 3** — Fix support system isolation (unblocks testing)
3. **PHASE 4** — Observability hardening (needed before implementing Phase 2)
4. **PHASE 2** — Implement full payment system
5. **PHASE 5** — Production rules audit
6. **PHASE 6** — Final validation

Report progress per phase. After each phase, state:
- What was changed and why
- What files were modified
- What errors were resolved
- What remains for the next phase

Do not proceed to the next phase until the current phase is confirmed working.
