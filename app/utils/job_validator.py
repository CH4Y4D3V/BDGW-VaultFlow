from app.core.exceptions import InvalidQueueJobError


REQUIRED_FIELDS = [
    "content_id",
    "vault_chat_id",
    "vault_message_id",
    "target_channel_ids",
    "media_type",
]


def validate_queue_job(job: dict) -> None:
    missing = []

    for field in REQUIRED_FIELDS:
        value = job.get(field)

        if value is None:
            missing.append(field)
            continue

        if isinstance(value, str) and not value.strip():
            missing.append(field)

        if isinstance(value, list) and not value:
            missing.append(field)

    if missing:
        raise InvalidQueueJobError(
            f"Queue job missing required fields: {missing}"
        )

    if not isinstance(job["vault_chat_id"], int):
        raise InvalidQueueJobError(
            "vault_chat_id must be int"
        )

    if not isinstance(job["vault_message_id"], int):
        raise InvalidQueueJobError(
            "vault_message_id must be int"
        )