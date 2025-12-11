from pathlib import Path

# API & Discord
API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1448251944413171815/6BVL0bl02ZGKJC7gHvIHYUvyGy8P3w9ULuL2JV27Khyu9raX3G-ADrEjfdaiYLOduKtf"

# Đường dẫn file
BASE_DIR = Path(__file__).resolve().parents[1]

RAW_IDS_FILE = BASE_DIR / "data" / "raw" / "products-0-200000.csv"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR = BASE_DIR / "data" / "logs"
FAILED_FILE = LOGS_DIR / "failed_records.csv"


BATCH_SIZE = 1000
MAX_WORKERS = 10
MAX_RETRIES = 3
BACKOFF = 1.0

# Đảm bảo thư mục tồn tại
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
