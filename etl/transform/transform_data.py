from __future__ import annotations

from typing import Any
from bs4 import BeautifulSoup


def clean_description(text: str | None) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text().strip()


def transform_product(raw: dict[str, Any] | None) -> dict[str, Any] | None:
    if not raw:
        return None

    desc = clean_description(raw.get("description"))

    images_raw = raw.get("images") or []
    image_urls: list[str] = []
    for img in images_raw:
        if isinstance(img, dict):
            url = (
                img.get("base_url")
                or img.get("thumbnail_url")
                or img.get("medium_url")
            )
            if url:
                image_urls.append(url)

    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        "url_key": raw.get("url_key"),
        "price": raw.get("price"),
        "description": desc,
        "images": image_urls,
    }
