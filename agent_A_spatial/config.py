import os

FRICTION = 0.0092          # 0.92% total fees + taxes (both sides combined)
TARGET_SPREAD = 1.07       # Minimum spread % to trigger a trade
TRADE_SIZE_IDR = 100_000   # Fixed trade size in IDR per signal
WALLET_IDR = 1_000_000     # Starting wallet for ROE calculation

MIN_VOLUME_IDR = 100_000   # Minimum top-of-book volume on BOTH exchanges

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DB_DSN = os.getenv("DB_DSN", "postgresql://mammon:mammon@localhost:5432/mammon")

POLL_INTERVAL_MS = 5       # Poll Redis every 5ms
