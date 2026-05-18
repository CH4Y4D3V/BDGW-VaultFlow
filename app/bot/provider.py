# app/bot/provider.py  (replace the existing function)
async def fetch_distribution_content() -> list[dict]:
    db = DatabaseManager.get_db()
    vault = db[getattr(settings, "VAULT_COLLECTION", "vault")]
    channels = db[getattr(settings, "CHANNEL_CONFIG_COLLECTION", "channel_config")]

    # Diagnostic: count total active channel configs
    total_configs = await channels.count_documents({"is_active": True})
    if total_configs == 0:
        logger.warning(
            "fetch_distribution_content: channel_config collection has NO active channels. "
            "Seed missing — check NSFW_GROUP_ID / PREMIUM_GROUP_ID env vars."
        )
        return []

    active_configs = []
    now = datetime.now(timezone.utc)

    async for config in channels.find({"is_active": True}):
        dest = config.get("destination")
        source_id = config.get("source_channel_id")

        if not dest or not source_id:
            logger.warning(
                "Skipping malformed channel config",
                extra={"ctx_config_id": str(config.get("_id"))},
            )
            continue

        # Diagnostic: count eligible vault items before filtering
        total_vault = await vault.count_documents(
            {"moderation_destination": dest, "status": ModerationState.QUEUED.value}
        )

        cursor = vault.find({
            "moderation_destination": dest,
            "status": ModerationState.QUEUED.value,
            "distribution_state": {"$nin": ["locked", "removed"]},
            "$or": [
                {"cooldown_until": None},
                {"cooldown_until": {"$exists": False}},
                {"cooldown_until": {"$lte": now}},
            ],
        }).sort("message_id", 1).limit(getattr(settings, "MAX_JOBS_PER_CYCLE", 100))

        content = await cursor.to_list(length=None)

        logger.info(
            "Channel provider query",
            extra={
                "ctx_dest": dest,
                "ctx_total_queued": total_vault,
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

    return active_configs