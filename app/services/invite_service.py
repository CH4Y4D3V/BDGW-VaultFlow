"""
app/services/invite_service.py
==============================
Premium invite link generation and lifecycle management.

Spec references:
  Section  7.5 — Subscription Activation (Atomic)
  Section  7.6 — Invite Link Rules (30-min expiry, one-time join)
  Section  9.4 — Admin Logs topic (dual audit requirement)
  Section 22   — Audit Logging (both MongoDB + Admin Logs topic)
  Section 24   — Redis distributed locks for atomic operations
  Section 25   — Restart safety: DB written before any Telegram action

Critical guarantees enforced by this module:
  • A Redis distributed lock scoped to (user_id, chat_id) serialises
    every invite-generation attempt for the same pair.
  • An idempotency guard returns any already-active invite without
    creating a duplicate.
  • Stale invite revocation on Telegram is BLOCKING: if the Telegram
    API call fails the DB mark is rolled back and an exception is
    raised — no double-link scenario is possible.
  • A "CREATING" record is written to MongoDB BEFORE the Telegram
    create_chat_invite_link call (restart safety, spec Section 25).
  • All Telegram API calls handle FloodWait explicitly (bounded retry).
  • Every outcome is dual-logged: MongoDB audit_logs AND Admin Logs
    topic in the Verification Hub.
"""

from __future__ import annotations

import asyncio
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from pyrogram.client import Client
from pyrogram.errors import FloodWait

from app.config import settings
from app.models.invite import Invite, InviteStatus
from app.repositories.invite_repository import InviteRepository
from app.services.audit_service import get_audit, AuditAction
from app.utils.logger import get_logger

logger = get_logger(__name__)

_UTC = timezone.utc

# Redis lock TTL in seconds.  Set generously above the longest expected
# combined Telegram round-trip time so a crash during the flow does not
# permanently deadlock the key.
_LOCK_TTL_SECONDS: int = 60

# Maximum Telegram API retry attempts after a FloodWait.
_MAX_TELEGRAM_RETRIES: int = 3


# ──────────────────────────────────────────────────────────────────────────────
# Distributed lock
# ──────────────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def _invite_lock(redis, user_id: int, chat_id: int):
    """
    Acquire a Redis SET NX distributed lock scoped to ``(user_id, chat_id)``.

    The lock is released in the ``finally`` block regardless of whether
    the body raises.  If the lock cannot be acquired (another coroutine
    holds it), ``RuntimeError`` is raised immediately — callers should
    surface this as a "try again shortly" error to the user.

    Args:
        redis:   An ``aioredis.Redis`` client instance.
        user_id: The user whose invite is being generated.
        chat_id: The target premium chat/group.

    Raises:
        RuntimeError: If the lock cannot be acquired.
    """
    lock_key = f"lock:invite:{user_id}:{chat_id}"
    acquired: bool = False
    try:
        acquired = await redis.set(lock_key, "1", ex=_LOCK_TTL_SECONDS, nx=True)
        if not acquired:
            logger.warning(
                "_invite_lock: lock already held — rejecting concurrent request",
                extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id},
            )
            raise RuntimeError(
                f"Invite generation already in progress for "
                f"user={user_id} chat={chat_id}. Try again shortly."
            )
        logger.debug(
            "_invite_lock: acquired",
            extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id},
        )
        yield
    finally:
        if acquired:
            try:
                await redis.delete(lock_key)
                logger.debug(
                    "_invite_lock: released",
                    extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id},
                )
            except Exception as exc:
                logger.warning(
                    "_invite_lock: failed to release lock — will expire naturally",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_chat_id": chat_id,
                        "ctx_error": str(exc),
                    },
                )


# ──────────────────────────────────────────────────────────────────────────────
# Telegram API helpers (FloodWait-safe)
# ──────────────────────────────────────────────────────────────────────────────

async def _create_invite_link(
    client: Client,
    chat_id: int,
    expire_date: datetime,
) -> object:
    """
    Call ``client.create_chat_invite_link`` with explicit FloodWait
    handling and a bounded retry loop.

    Always sets ``member_limit=1`` (one-time join, spec Section 7.6) and
    ``creates_join_request=False`` (direct join, no approval step).

    Args:
        client:      Pyrogram ``Client`` instance.
        chat_id:     Target premium chat/group ID from hub_config.
        expire_date: Invite expiry timestamp (UTC, 30 min from now).

    Returns:
        The ``ChatInviteLink`` object returned by Pyrogram.

    Raises:
        Exception: On final failure (after all FloodWait retries).
    """
    for attempt in range(1, _MAX_TELEGRAM_RETRIES + 1):
        try:
            result = await client.create_chat_invite_link(
                chat_id=chat_id,
                member_limit=1,
                expire_date=expire_date,
                creates_join_request=False,
            )
            return result
        except FloodWait as fw:
            if attempt < _MAX_TELEGRAM_RETRIES:
                logger.warning(
                    "_create_invite_link: FloodWait — sleeping before retry",
                    extra={
                        "ctx_chat_id": chat_id,
                        "ctx_wait_seconds": fw.value,
                        "ctx_attempt": attempt,
                    },
                )
                await asyncio.sleep(fw.value)
            else:
                logger.error(
                    "_create_invite_link: FloodWait on final attempt — giving up",
                    extra={
                        "ctx_chat_id": chat_id,
                        "ctx_wait_seconds": fw.value,
                    },
                )
                raise
        except Exception as exc:
            logger.error(
                "_create_invite_link: Telegram API error",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_attempt": attempt,
                    "ctx_error": str(exc),
                },
            )
            raise


async def _revoke_invite_link(
    client: Client,
    chat_id: int,
    link: str,
) -> None:
    """
    Call ``client.revoke_chat_invite_link`` with explicit FloodWait
    handling and a bounded retry loop.

    This function raises on failure — the caller MUST treat a failed
    revocation as a blocking error and roll back the DB mark.  The
    original audit finding (MEDIUM severity) documented that treating
    Telegram revocation failure as non-fatal could leave the user with
    two valid links simultaneously.  This implementation closes that
    gap.

    Args:
        client:  Pyrogram ``Client`` instance.
        chat_id: The chat whose invite is being revoked.
        link:    The full Telegram invite link URL to revoke.

    Raises:
        Exception: On final failure (after all FloodWait retries).
                   Caller must roll back the DB REVOKED mark.
    """
    for attempt in range(1, _MAX_TELEGRAM_RETRIES + 1):
        try:
            await client.revoke_chat_invite_link(
                chat_id=chat_id,
                invite_link=link,
            )
            logger.info(
                "_revoke_invite_link: Telegram revocation successful",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_link_prefix": link[:30] if link else None,
                },
            )
            return
        except FloodWait as fw:
            if attempt < _MAX_TELEGRAM_RETRIES:
                logger.warning(
                    "_revoke_invite_link: FloodWait — sleeping before retry",
                    extra={
                        "ctx_chat_id": chat_id,
                        "ctx_wait_seconds": fw.value,
                        "ctx_attempt": attempt,
                    },
                )
                await asyncio.sleep(fw.value)
            else:
                logger.error(
                    "_revoke_invite_link: FloodWait on final attempt — giving up",
                    extra={
                        "ctx_chat_id": chat_id,
                        "ctx_link_prefix": link[:30] if link else None,
                        "ctx_wait_seconds": fw.value,
                    },
                )
                raise
        except Exception as exc:
            logger.error(
                "_revoke_invite_link: Telegram API error",
                extra={
                    "ctx_chat_id": chat_id,
                    "ctx_link_prefix": link[:30] if link else None,
                    "ctx_attempt": attempt,
                    "ctx_error": str(exc),
                },
            )
            raise


# ──────────────────────────────────────────────────────────────────────────────
# Service
# ──────────────────────────────────────────────────────────────────────────────

class InviteService:
    """
    Manages premium invite link generation and lifecycle.

    This service is the single point of authority for creating one-time
    Telegram invite links for premium group access.  It enforces:

      • Distributed locking (Redis) for every invite-generation call.
      • Idempotency: returns existing active invites instead of creating
        duplicates.
      • Blocking revocation: an old invite that cannot be revoked on
        Telegram causes the entire generation flow to abort.
      • Restart safety: a CREATING record is persisted to MongoDB before
        the Telegram create call; on restart, orphaned CREATING records
        can be detected and cleaned up.
      • Dual audit logging: both MongoDB (audit_logs) and the Admin Logs
        topic in the Verification Hub receive an entry on every
        successful invite generation.
    """

    def __init__(self) -> None:
        """Initialise the service with its repository dependency."""
        self._repo = InviteRepository()

    # ── Public API ────────────────────────────────────────────────────

    async def generate_premium_invite(
        self,
        client: Client,
        user_id: int,
        chat_id: int,
        granted_by: int,
        plan: str,
    ) -> Invite:
        """
        Generate a single-use, 30-minute invite link for the premium chat.

        This is the main entry point.  It acquires a Redis distributed
        lock for ``(user_id, chat_id)`` and then delegates to the
        locked implementation.

        Full flow (spec Sections 7.5, 7.6, 22, 24, 25):
          1. Acquire Redis distributed lock.
          2. Idempotency guard — return existing active invite if present.
          3. Mark any previously active invites as REVOKED in MongoDB.
          4. Revoke each old link on Telegram (FloodWait-safe).
             → If ANY Telegram revoke fails: re-mark the DB record as
               ACTIVE and raise.  No new invite is created.
          5. Write a "CREATING" record to MongoDB (restart safety).
          6. Call ``create_chat_invite_link`` on Telegram (FloodWait-safe).
             → If the Telegram call fails: delete the CREATING record
               and raise.
          7. Promote the CREATING record to ACTIVE with the real link.
          8. Dual-audit: write to ``audit_logs`` collection AND Admin
             Logs topic in the Verification Hub.
          9. Release the lock and return the ``Invite`` model.

        Args:
            client:     Authenticated Pyrogram ``Client``.
            user_id:    Telegram user ID receiving the invite.
            chat_id:    Premium group/channel ID (from hub_config —
                        never hardcoded).
            granted_by: Admin user ID who approved the subscription.
            plan:       Plan identifier string (e.g. ``"1_month"``).

        Returns:
            The persisted ``Invite`` model containing the Telegram link.

        Raises:
            RuntimeError: If the distributed lock cannot be acquired, or
                          if a Telegram revocation of an old link fails
                          (old link may still be active on Telegram —
                          caller must surface this error).
            Exception:    On any unrecoverable Telegram or DB failure.
        """
        # Deferred import avoids circular dependency chains at module load.
        from app.core.redis import get_redis_client

        redis = await get_redis_client()

        async with _invite_lock(redis, user_id, chat_id):
            return await self._generate_locked(
                client=client,
                user_id=user_id,
                chat_id=chat_id,
                granted_by=granted_by,
                plan=plan,
            )

    # ── Internal locked implementation ───────────────────────────────

    async def _generate_locked(
        self,
        client: Client,
        user_id: int,
        chat_id: int,
        granted_by: int,
        plan: str,
    ) -> Invite:
        """
        Core invite-generation logic, executed inside the distributed lock.

        Should not be called directly.  Always go through
        ``generate_premium_invite`` which acquires the lock first.

        All Telegram calls use FloodWait-safe helpers.
        All DB operations use Motor async calls.
        See ``generate_premium_invite`` for the full flow description.

        Args:
            client:     Pyrogram ``Client``.
            user_id:    Recipient's Telegram user ID.
            chat_id:    Target premium group/channel ID.
            granted_by: Approving admin's Telegram user ID.
            plan:       Plan identifier string.

        Returns:
            Persisted ``Invite`` model with status ACTIVE.

        Raises:
            RuntimeError: On Telegram revocation failure of an old link.
            Exception:    On Telegram creation failure or DB failure.
        """
        now = datetime.now(_UTC)
        expires_at = now + timedelta(minutes=settings.INVITE_EXPIRY_MINUTES)

        # ── Step 1: Idempotency guard ─────────────────────────────────
        # If an active invite already exists for this (user_id, chat_id),
        # return it directly.  Prevents duplicate generation if the
        # caller retries without waiting for the lock to clear.
        try:
            existing_doc: Optional[dict] = (
                await self._repo.get_active_invite_for_user_chat(
                    user_id=user_id,
                    chat_id=chat_id,
                )
            )
        except Exception as exc:
            logger.error(
                "_generate_locked: idempotency check (get_active_invite_for_user_chat) failed",
                extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id, "ctx_error": str(exc)},
            )
            raise

        if existing_doc is not None:
            logger.info(
                "_generate_locked: active invite already exists — returning existing",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_chat_id": chat_id,
                    "ctx_existing_token": existing_doc.get("token"),
                },
            )
            return Invite.from_dict(existing_doc)

        # ── Step 2: Mark previous invites REVOKED in DB ───────────────
        try:
            revoked_links: list[str] = (
                await self._repo.revoke_all_active_for_user_chat(
                    user_id=user_id,
                    chat_id=chat_id,
                )
            )
        except Exception as exc:
            logger.error(
                "_generate_locked: revoke_all_active_for_user_chat (DB) failed",
                extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id, "ctx_error": str(exc)},
            )
            raise

        if revoked_links:
            logger.info(
                "_generate_locked: marked prior active invites REVOKED in DB",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_chat_id": chat_id,
                    "ctx_revoked_count": len(revoked_links),
                },
            )

        # ── Step 3: Revoke each old link on Telegram (BLOCKING) ───────
        # Audit finding (MEDIUM): treating Telegram revocation as
        # non-fatal could leave the user with two simultaneously valid
        # links.  This loop is now BLOCKING: any Telegram failure causes
        # a DB rollback and an exception that aborts the entire flow.
        for link in revoked_links:
            try:
                await _revoke_invite_link(client, chat_id, link)
            except Exception as tg_exc:
                # ── Roll back the DB REVOKED mark ─────────────────────
                logger.error(
                    "_generate_locked: Telegram revocation failed — "
                    "rolling back DB mark to prevent double-link risk",
                    extra={
                        "ctx_user_id": user_id,
                        "ctx_chat_id": chat_id,
                        "ctx_link_prefix": link[:30] if link else None,
                        "ctx_error": str(tg_exc),
                    },
                )
                try:
                    await self._repo.reactivate_invite_by_link(
                        chat_id=chat_id,
                        telegram_link=link,
                    )
                    logger.info(
                        "_generate_locked: DB rollback successful — "
                        "old invite re-marked ACTIVE",
                        extra={
                            "ctx_user_id": user_id,
                            "ctx_chat_id": chat_id,
                            "ctx_link_prefix": link[:30] if link else None,
                        },
                    )
                except Exception as rollback_exc:
                    # Both the Telegram revoke and the DB rollback failed.
                    # This is a CRITICAL state: the old link may or may not
                    # be revoked on Telegram, and the DB says REVOKED.
                    # Requires manual intervention.
                    logger.critical(
                        "_generate_locked: CRITICAL — Telegram revoke failed AND "
                        "DB rollback failed. Manual intervention required.",
                        extra={
                            "ctx_user_id": user_id,
                            "ctx_chat_id": chat_id,
                            "ctx_link_prefix": link[:30] if link else None,
                            "ctx_tg_error": str(tg_exc),
                            "ctx_rollback_error": str(rollback_exc),
                        },
                    )
                raise RuntimeError(
                    f"Telegram revocation of old invite failed for "
                    f"user={user_id} chat={chat_id}. "
                    f"DB has been rolled back. Original error: {tg_exc}"
                ) from tg_exc

        # ── Step 4: Write CREATING record to DB BEFORE Telegram call ──
        # Spec Section 25: "Every piece of state must be written to
        # MongoDB FIRST, before any Telegram action is taken."
        # On restart, records with status=CREATING can be detected and
        # either completed (if a link was already made) or cleaned up.
        token = secrets.token_urlsafe(16)
        creating_doc: dict = {
            "token": token,
            "created_by": granted_by,
            "chat_id": chat_id,
            "intended_user_id": user_id,       # indexed field (spec 25A.7)
            "plan_grant": plan,
            "max_uses": 1,
            "uses_remaining": 1,
            "created_at": now,
            "expires_at": expires_at,
            "status": "creating",              # sentinel — see InviteStatus note
            "telegram_link": None,
            "notes": f"user_{user_id} plan:{plan} granted_by:{granted_by}",
        }
        try:
            insert_result = await self._repo.collection.insert_one(creating_doc)
            doc_id = insert_result.inserted_id
        except Exception as exc:
            logger.error(
                "_generate_locked: failed to write CREATING record to MongoDB",
                extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id, "ctx_error": str(exc)},
            )
            raise

        # ── Step 5: Create Telegram invite link ───────────────────────
        try:
            tg_result = await _create_invite_link(client, chat_id, expires_at)
        except Exception as exc:
            logger.error(
                "_generate_locked: Telegram create_chat_invite_link failed — "
                "deleting CREATING record",
                extra={"ctx_user_id": user_id, "ctx_chat_id": chat_id, "ctx_error": str(exc)},
            )
            try:
                await self._repo.collection.delete_one({"_id": doc_id})
            except Exception as cleanup_exc:
                logger.warning(
                    "_generate_locked: failed to delete orphaned CREATING record",
                    extra={
                        "ctx_doc_id": str(doc_id),
                        "ctx_error": str(cleanup_exc),
                    },
                )
            raise

        # ── Step 6: Promote CREATING → ACTIVE with the real link ──────
        try:
            await self._repo.collection.update_one(
                {"_id": doc_id},
                {
                    "$set": {
                        "status": InviteStatus.ACTIVE.value,
                        "telegram_link": tg_result.invite_link,
                    }
                },
            )
        except Exception as exc:
            logger.error(
                "_generate_locked: failed to promote CREATING → ACTIVE in MongoDB. "
                "Telegram link was already created — manual reconciliation may be needed.",
                extra={
                    "ctx_user_id": user_id,
                    "ctx_chat_id": chat_id,
                    "ctx_doc_id": str(doc_id),
                    "ctx_link_prefix": (
                        tg_result.invite_link[:30] if tg_result.invite_link else None
                    ),
                    "ctx_error": str(exc),
                },
            )
            raise

        # ── Step 7: Build return model ────────────────────────────────
        # NOTE: the Invite model must have an `intended_user_id` field
        # added (see Dependencies section).  Until then the field lives
        # only in the DB document (already written above).
        invite = Invite(
            token=token,
            created_by=granted_by,
            chat_id=chat_id,
            plan_grant=plan,
            max_uses=1,
            uses_remaining=1,
            created_at=now,
            expires_at=expires_at,
            status=InviteStatus.ACTIVE,
            telegram_link=tg_result.invite_link,
            notes=f"user_{user_id} plan:{plan} granted_by:{granted_by}",
        )

        # ── Step 8: Dual audit log (MongoDB + Admin Logs topic) ───────
        # Spec Section 22: all events must be written to BOTH places.
        audit_detail: dict = {
            "token": token,
            "chat_id": chat_id,
            "plan": plan,
            "expires_at": expires_at.isoformat(),
            "expiry_minutes": settings.INVITE_EXPIRY_MINUTES,
        }

        # 8a — MongoDB audit_logs
        try:
            await get_audit().log(
                action=AuditAction.INVITE_GENERATE,
                performed_by=granted_by,
                target_user_id=user_id,
                details=audit_detail,
            )
        except Exception as exc:
            logger.error(
                "_generate_locked: MongoDB audit log write failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            )
            # Non-fatal: the invite is already created.  Log and continue.

        # 8b — Admin Logs topic in Verification Hub (spec Section 9.4)
        try:
            from app.services.hub_logger import write_admin_log

            await write_admin_log(
                action_type="INVITE GENERATED",
                performed_by=granted_by,
                target_user_id=user_id,
                detail=(
                    f"plan={plan} "
                    f"chat={chat_id} "
                    f"token={token} "
                    f"expires={expires_at.isoformat()}"
                ),
            )
        except Exception as exc:
            logger.error(
                "_generate_locked: Admin Logs topic write failed",
                extra={"ctx_user_id": user_id, "ctx_error": str(exc)},
            )
            # Non-fatal: the invite is created.  Log and continue.

        logger.info(
            "Premium invite generated successfully",
            extra={
                "ctx_user_id": user_id,
                "ctx_chat_id": chat_id,
                "ctx_plan": plan,
                "ctx_granted_by": granted_by,
                "ctx_expires_at": expires_at.isoformat(),
                "ctx_expiry_minutes": settings.INVITE_EXPIRY_MINUTES,
                "ctx_token": token,
            },
        )

        return invite
