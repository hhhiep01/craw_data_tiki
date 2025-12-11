from __future__ import annotations

import requests
from config.settings import DISCORD_WEBHOOK_URL


def send_discord_alert(message: str):
    if not DISCORD_WEBHOOK_URL:
        print("DISCORD_WEBHOOK_URL chưa được cấu hình")
        return

    try:
        requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": message},
            timeout=5,
        )
    except Exception as e:
        print("Không gửi được notification Discord:", e)
