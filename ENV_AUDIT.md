# ── Pyrogram (REQUIRED) ───────────────────────────────────────────
BOT_TOKEN=8955404453:AAFqU3T7REnfLzloFK6CCaD4zRszzL3ezcY
API_ID=28280135
API_HASH=4034deab44d29750ea7ddd288cd191a5
SESSION_NAME=vaultflow_bot
SESSION_DIR=./sessions
MAX_CONCURRENT_TRANSMISSIONS=10

# ── MongoDB (REQUIRED) ────────────────────────────────────────────
MONGO_URI=mongodb+srv://shaariarxx:Bangla12@bdgwvault.vxrggao.mongodb.net/?appName=bdgwvault
MONGO_DB_NAME=bdgw_vaultflow
MONGO_MAX_POOL_SIZE=20
MONGO_MIN_POOL_SIZE=5

# Collection names
QUEUE_COLLECTION=queue
DEAD_LETTER_COLLECTION=dead_letters
LOCK_COLLECTION=locks
METRICS_COLLECTION=metrics
VAULT_COLLECTION=vault
CHANNEL_CONFIG_COLLECTION=channel_config
PENDING_COLLECTION=pending_submissions
SCHEDULER_JOBS_COLLECTION=scheduler_jobs
QUARANTINE_COLLECTION=quarantine

# ── Redis ─────────────────────────────────────────────────────────
REDIS_URL=${{ Redis.REDIS_URL }}

# ── Telegram Channel / Group IDs (REQUIRED) ───────────────────────
VERIFICATION_GROUP_ID=-1001895403473
VAULT_CHANNEL_ID=-1002048690257
NSFW_GROUP_ID=-1002908207184
PREMIUM_GROUP_ID=-1002505469098
PREMIUM_CHANNEL_ID=-1003758437237
LOG_CHANNEL_ID=-1002231418857
MAIN_CHANNEL_ID=-1002255449546

# ── Destination Display Names ─────────────────────────────────────
NSFW_DISPLAY_NAME=𝐁𝐃 𝐆𝐎𝐍𝐄 𝐖𝐈𝐋𝐃 𝐕𝐈𝐃𝐄𝐎
PREMIUM_DISPLAY_NAME=𝐁𝐃 𝐆𝐎𝐍𝐄 𝐖𝐈𝐋𝐃 ✦ 𝐏𝐑𝐄𝐌𝐈𝐔𝐌

# ── Access Control (REQUIRED — set OWNER_ID) ──────────────────────
OWNER_ID=6247623313
ADMIN_IDS=[6247623313]
SUDO_IDS=[6994887289, 7918120402]
MODERATOR_IDS=[6994887289, 7918120402]
SUPPORT_ADMIN_IDS=[6994887289, 7918120402]
PAYMENT_ADMIN_IDS=[6994887289, 7918120402]
SCHEDULER_ADMIN_IDS=[6994887289, 7918120402]

# ── Worker Pools ──────────────────────────────────────────────────
DISPATCHER_WORKER_COUNT=4
WATERMARK_WORKER_COUNT=2
WORKER_BATCH_SIZE=5
WORKER_POLL_INTERVAL=2.0

# ── Scheduler & Fairness ──────────────────────────────────────────
SCHEDULER_INTERVAL_SECONDS=120
MAX_JOBS_PER_CYCLE=50
RANDOMIZE_POSTING_WINDOW=300
REPOST_PREVENTION_HOURS=168
 
# ── Queue ─────────────────────────────────────────────────────────
QUEUE_DEADLINE_HOURS=24

# ── Retries & Backoff ─────────────────────────────────────────────
MAX_RETRY_ATTEMPTS=3
RETRY_BASE_DELAY=5.0
RETRY_MAX_DELAY=3600.0
RETRY_JITTER_RANGE=2.0

# ── Distributed Locks ─────────────────────────────────────────────
LOCK_TTL_SECONDS=300
LOCK_RETRY_ATTEMPTS=5
LOCK_RETRY_DELAY=1.0
STALE_LOCK_THRESHOLD_SECONDS=600

# ── Rate Limits & Flood Protection ────────────────────────────────
GLOBAL_RATE_LIMIT_PER_MIN=30
PER_TARGET_RATE_LIMIT_PER_MIN=10
FLOODWAIT_EXTRA_BUFFER=2
FLOODWAIT_MAX_WAIT=86400
FLOOD_MAX_REQUESTS=5
FLOOD_WINDOW_SECONDS=60

# ── Media Groups ──────────────────────────────────────────────────
MEDIA_GROUP_TIMEOUT_SECONDS=3.0
MEDIA_GROUP_MAX_SIZE=10

# ── Media Processing ──────────────────────────────────────────────
PROCESSED_MEDIA_DIR=./processed
WATERMARK_CACHE_DIR=./watermark_cache
FFMPEG_TIMEOUT=120.0

# ── Watermark Assets ──────────────────────────────────────────────
WATERMARK_ENABLED=false
WATERMARK_LOGO_PATH_NSFW=./assets/watermarks/nsfw_logo.png
WATERMARK_LOGO_PATH_PREMIUM=./assets/watermarks/premium_logo.png
WATERMARK_FONT_PATH=./assets/fonts/Montserrat-SemiBold.ttf
WATERMARK_TEXT_NSFW=𝐁𝐃 𝐆𝐎𝐍𝐄 𝐖𝐈𝐋𝐃 𝐕𝐈𝐃𝐄𝐎
WATERMARK_TEXT_PREMIUM=𝐁𝐃 𝐆𝐎𝐍𝐄 𝐖𝐈𝐋𝐃 ✦ 𝐏𝐑𝐄𝐌𝐈𝐔𝐌
WATERMARK_ROTATION=-27
WATERMARK_OPACITY=107
WATERMARK_SCALE=0.040

# ── Vault ─────────────────────────────────────────────────────────
VAULT_IMMUTABLE=true

# ── Subscriptions ─────────────────────────────────────────────────
GRACE_PERIOD_DAYS=3

# ── Invite Security ───────────────────────────────────────────────
INVITE_EXPIRY_MINUTES=30

# ── Daily distribution caps ──────────────────────────────────────
DAILY_CAP_PREMIUM=10
DAILY_CAP_NSFW=50

# ── Runtime ───────────────────────────────────────────────────────
DEBUG=false
LOG_LEVEL=INFO
LOG_FORMAT=JSON