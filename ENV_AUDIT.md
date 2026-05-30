# Environment Variable Audit

## Removed Variables (Obsolete/Duplicate)
- `MEDIA_GROUP_TIMEOUT`: Duplicate of `MEDIA_GROUP_TIMEOUT_SECONDS`.
- `WATERMARK_LOGO_PATH`: Obsolete; destination-specific paths (`NSFW`/`PREMIUM`) are used instead.

## Added to .env.example (Synchronization)
- `QUARANTINE_COLLECTION`: Missing in template.
- `PREMIUM_CHANNEL_ID`: Missing in template.
- `FLOOD_MAX_REQUESTS`: Missing in template.
- `FLOOD_WINDOW_SECONDS`: Missing in template.
- `WATERMARK_ENABLED`: Missing in template.
- `WATERMARK_FONT_PATH`: Missing in template.
- `DAILY_CAP_NSFW`: Missing in template.
- `DAILY_CAP_PREMIUM`: Missing in template.

## Verified Consistency
- `.env.example` now contains every variable defined in `app/config/settings.py` (excluding private internal logic).
- All variables in `settings.py` have been audited for usage in the codebase.
