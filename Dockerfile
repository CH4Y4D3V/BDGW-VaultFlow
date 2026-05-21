FROM python:3.12-slim

# System dependencies
# gcc + python3-dev are required to build TgCrypto (C extension)
# ffmpeg is required for watermark processing
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create runtime directories
RUN mkdir -p sessions processed watermark_cache assets/watermarks logs

CMD ["python", "main_bot.py"]
