from __future__ import annotations

from pathlib import Path
import time
import csv
import requests

from config.settings import API_URL, MAX_RETRIES, BACKOFF


session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://tiki.vn/",
})


def fetch_product(
    product_id: str,
    max_retries: int = MAX_RETRIES,
    backoff: float = BACKOFF,
):
    url = API_URL.format(product_id)

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=10)

            if response.status_code == 200:
                return response.json(), True

            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff * attempt)
                continue

            return None, False

        except requests.RequestException:
            if attempt < max_retries:
                time.sleep(backoff * attempt)
            else:
                break

    return None, False


def read_ids_from_csv(path: Path, max_ids: int | None = None) -> list[str]:
    ids: list[str] = []
    with open(path, "r", encoding="utf-8", newline="") as csvfile:
        reader = csv.reader(csvfile)
        _header = next(reader, None)  # bỏ header

        for row in reader:
            if row and row[0].strip():
                ids.append(row[0].strip())
                if max_ids is not None and len(ids) >= max_ids:
                    break
    return ids
