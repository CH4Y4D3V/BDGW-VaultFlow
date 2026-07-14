from datetime import datetime, timezone, timedelta
from typing import List, Dict

from app.config import settings
from app.core.database import DatabaseManager
from app.core.models import ModerationState
from app.utils.logger import get_logger

logger = get_logger(__name__)


async def fetch_distribution_content() -> List[Dict]:
    """
    Called by DistributionScheduler to fetch valid vault content.

    Provides the data boundary ensuring only fully ingested, non-duplicate,
    non-locked, non-cooldown content is released to the fairness selector.

    FIX: Previously only queried status=QUEUED. Content approved via
    execute_approve() has status=POSTED and was invisible forever, meaning
    "Approve Immediately" content never re-entered the distribution pool.
    Now also includes status=POSTED items where cooldown_until has expired
    (or is absent). execute_approve() sets cooldown_until on the vault doc
    so these items are suppressed until their cooldown passes.
    """
    db = DatabaseManager.get_db()
    vault = db[getattr(settings, "VAULT_COLLECTION", "vault")]
    queue = db[getattr(settings, "QUEUE_COLLECTION", "queue_jobs")]
    channels = db[getattr(settings, "CHANNEL_CONFIG_COLLECTION", "channel_config")]

    # Case A: no channels at all — seeding failure
    total_active_channels = await channels.count_documents({"is_active": True})
    if total_active_channels == 0:
        logger.error(
            "no_active_channels_configured",
            extra={"ctx_hint": "Check NSFW_GROUP_ID/PREMIUM_GROUP_ID and seeding"}
        )
        return []

    active_configs = []
    now = datetime.now(timezone.utc)

    async for config in channels.find({"is_active": True}):
        dest = config.get("destination")
        source_id = config.get("source_channel_id")

        if not dest or not source_id:
            logger.warning(
                "malformed_channel_config_skipped",
                extra={"ctx_config_id": str(config.get("_id"))},
            )
            continue

        # Eligible statuses: QUEUED (queued for first delivery) and POSTED
        # (already posted once via execute_approve — eligible for vault replay
        # after cooldown expires).
        eligible_statuses = [
            ModerationState.QUEUED.value,
            ModerationState.POSTED.value,
        ]

        # Diagnostic counts so operators can see exactly why content is or isn't flowing
        total_queued = await vault.count_documents({
            "moderation_destination": dest,
            "status": {"$in": eligible_statuses},
        })

        total_locked = await vault.count_documents({
            "moderation_destination": dest,
            "status": {"$in": eligible_statuses},
            "distribution_state": {"$in": ["locked", "removed"]},
        })

        # FIX: total_locked never checked "pending_delivery" specifically, so
        # a warning like "vault_has_content_none_eligible ctx_locked:0" gave
        # no way to tell whether items were correctly waiting out a cooldown
        # (benign, by design — see execute_approve's 24h vault-fill cooldown)
        # or permanently orphaned at distribution_state="pending_delivery"
        # with no job left to ever release them (a real, silent bug). These
        # two counts make the distinction explicit in every cycle's log.
        total_pending_delivery = await vault.count_documents({
            "moderation_destination": dest,
            "status": {"$in": eligible_statuses},
            "distribution_state": "pending_delivery",
        })
        total_future_cooldown = await vault.count_documents({
            "moderation_destination": dest,
            "status": {"$in": eligible_statuses},
            "distribution_state": {"$nin": ["locked", "removed", "pending_delivery"]},
            "cooldown_until": {"$gt": now},
        })

        # ── Self-healing: release orphaned "pending_delivery" vault items ──
        # PROACTIVE COMPLEMENT to the one-time stabilize_vault() startup
        # migration. That migration only runs once per container start, so
        # any item that becomes orphaned AFTER startup (e.g. an edge case
        # not covered by existing recovery paths) would stay silently stuck
        # until the next restart. This check runs every distribution cycle
        # (~2 min) instead: any vault item stuck at distribution_state=
        # "pending_delivery" for longer than ORPHAN_GRACE_PERIOD_MINUTES
        # (default 15 — long enough that a genuinely in-flight job's brief
        # enqueue window can never be mistaken for an orphan) with NO
        # matching active-status queue job is released automatically.
        grace_minutes = getattr(settings, "ORPHAN_GRACE_PERIOD_MINUTES", 15)
        grace_cutoff = now - timedelta(minutes=grace_minutes)
        active_job_statuses = [
            "pending", "processing", "locked",
            "watermarking", "ready", "delivering",
        ]
        released_count = 0
        async for v_doc in vault.find(
            {
                "moderation_destination": dest,
                "status": {"$in": eligible_statuses},
                "distribution_state": "pending_delivery",
                "updated_at": {"$lt": grace_cutoff},
            },
            {"content_id": 1},
        ):
            content_id = v_doc.get("content_id")
            if not content_id:
                continue
            active_job = await queue.find_one({
                "content_id": content_id,
                "status": {"$in": active_job_statuses},
            })
            if active_job is None:
                await vault.update_one(
                    {"_id": v_doc["_id"]},
                    {"$set": {"distribution_state": None, "updated_at": now}},
                )
                released_count += 1

        if released_count:
            logger.warning(
                "provider_self_heal_released_orphaned_vault_items",
                extra={
                    "ctx_dest": dest,
                    "ctx_released": released_count,
                    "ctx_grace_minutes": grace_minutes,
                },
            )

        cursor = vault.find({
            "moderation_destination": dest,
            "status": {"$in": eligible_statuses},
            # FIX: "pending_delivery" was missing from this exclusion list.
            # _enqueue_content() sets distribution_state="pending_delivery"
            # immediately after creating a queue job. Without this exclusion,
            # the scheduler returned the same vault doc every cycle (every 2 min)
            # while the job was active — triggering a DuplicateJobError on every
            # attempt (vault_ref_unique blocks duplicate active jobs). In
            # deployments where vault_ref_unique was not successfully migrated,
            # this caused the scheduler to create a second job for the same
            # content, resulting in a second watermark upload to the vault channel
            # AND a second delivery to the group — exactly the "same content
            # multiple times" symptom. The exclusion is safe: mark_completed()
            # resets distribution_state to None after delivery, so content
            # becomes eligible again (subject to cooldown_until) once the job
            # completes normally.
            "distribution_state": {"$nin": ["locked", "removed", "pending_delivery"]},
            "$or": [
                {"cooldown_until": None},
                {"cooldown_until": {"$exists": False}},
                {"cooldown_until": {"$lte": now}},
            ],
        }).sort("vault_message_id", 1).limit(getattr(settings, "MAX_JOBS_PER_CYCLE", 100))

        content = await cursor.to_list(length=None)

        logger.info(
            "channel_provider_query_result",
            extra={
                "ctx_dest": dest,
                "ctx_total_queued": total_queued,
                "ctx_locked_or_removed": total_locked,
                "ctx_pending_delivery": total_pending_delivery,
                "ctx_future_cooldown": total_future_cooldown,
                "ctx_eligible": len(content),
            },
        )

        if content:
            active_configs.append({
                "source_channel_id": source_id,
                "target_channel_ids": config.get("target_channel_ids", []),
                "content": content,
                "watermark_config": config.get("watermark_config"),
            })
        else:
            if total_queued == 0:
                logger.info(
                    "vault_empty_for_destination",
                    extra={"ctx_dest": dest},
                )
            else:
                logger.warning(
                    "vault_has_content_none_eligible",
                    extra={
                        "ctx_dest": dest,
                        "ctx_total_queued": total_queued,
                        "ctx_locked": total_locked,
                        "ctx_pending_delivery": total_pending_delivery,
                        "ctx_future_cooldown": total_future_cooldown,
                    },
                )

    if not active_configs:
        logger.info(
            "no_eligible_content_this_cycle",
            extra={"ctx_active_channels": total_active_channels},
        )

    return active_configs
