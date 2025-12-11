from __future__ import annotations

from pipelines.crawl_pipeline import run_crawl
from src.utils.notifier import send_discord_alert


if __name__ == "__main__":
    try:
        send_discord_alert("Crawl Tiki START.")
        run_crawl()
        send_discord_alert("Crawl Tiki FINISH.")
    except KeyboardInterrupt:
        send_discord_alert("Crawl Tiki has been stopped by user")
        raise
    except Exception as e:
        send_discord_alert(f"Crawl Tiki CRASH: {e}")
        raise
