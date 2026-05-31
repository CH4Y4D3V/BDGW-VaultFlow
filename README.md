# BDGW VaultFlow

BDGW VaultFlow is a highly scalable, async-first Telegram automation and moderation bot designed to handle content submission, premium subscription gating, support ticketing, and queue-based content distribution.

## Requirements
- Python 3.12+
- MongoDB 6.0+ (Replica Set required for transactions)
- Redis 7.0+
- FFmpeg (for video watermarking)

## Setup Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourorg/bdgw-vaultflow.git
   cd bdgw-vaultflow
   ```

2. **Create and activate a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Configuration:**
   Copy the example config and fill in your details:
   ```bash
   cp .env.example .env
   ```

5. **Ensure MongoDB and Redis are running:**
   ```bash
   docker-compose up -d
   ```

## How to Run
Start the bot using the main entry point:
```bash
python main_bot.py
```

## Environment Variable Reference
Key variables to set in your `.env` file:
- `BOT_TOKEN`: Your Telegram bot token.
- `API_ID` / `API_HASH`: Your Telegram API credentials.
- `MONGO_URI`: MongoDB connection string (e.g., `mongodb://localhost:27017`).
- `REDIS_URL`: Redis connection string (e.g., `redis://localhost:6379/0`).
- `VERIFICATION_GROUP_ID`: The supergroup for moderation.
- `VAULT_CHANNEL_ID`: The channel for permanent archival.
- `OWNER_ID`: Your Telegram User ID.
- `WATERMARK_ENABLED`: Set to `true` to enable watermarking.

For a full list of configuration options, see `.env.example`.