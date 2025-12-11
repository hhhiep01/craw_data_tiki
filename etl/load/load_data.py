from __future__ import annotations
import json
import csv

from config.settings import PROCESSED_DIR, FAILED_FILE


def save_batch(batch: list[dict], file_index: int):
    file_path = PROCESSED_DIR / f"batch_{file_index:04d}.json"
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(batch, file, ensure_ascii=False, indent=4)
    print(f"Đã lưu {file_path.name} ({len(batch)} records)")


def get_resume_info(batch_size: int) -> tuple[int, int]:
    file_index = 1
    last_existing_index = 0

    while (PROCESSED_DIR / f"batch_{file_index:04d}.json").exists():
        last_existing_index = file_index
        file_index += 1

    if last_existing_index == 0:
        return 0, 1

    last_file = PROCESSED_DIR / f"batch_{last_existing_index:04d}.json"

    try:
        with open(last_file, "r", encoding="utf-8") as file:
            data = json.load(file)
        count_last = len(data) if isinstance(data, list) else 0
    except Exception:
        start_index = (last_existing_index - 1) * batch_size
        return start_index, last_existing_index + 1

    start_index = (last_existing_index - 1) * batch_size + count_last
    next_file_index = last_existing_index + 1

    return start_index, next_file_index


def append_failed_record(record: dict):
    file_exist = FAILED_FILE.exists()

    with open(FAILED_FILE, "a", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["id", "error"])
        if not file_exist:
            writer.writeheader()
        writer.writerow(record)
