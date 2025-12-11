from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

from config.settings import (
    RAW_IDS_FILE,
    BATCH_SIZE,
    MAX_WORKERS,
)
from etl.extract.extract_data import read_ids_from_csv, fetch_product
from etl.transform.transform_data import transform_product
from etl.load.load_data import save_batch, get_resume_info, append_failed_record


def run_crawl(batch_size: int | None = None, max_workers: int | None = None):
    batch_size = batch_size or BATCH_SIZE
    max_workers = max_workers or MAX_WORKERS

    start_time = time.time()
    ids = read_ids_from_csv(RAW_IDS_FILE, max_ids=None)

    batch: list[dict] = []
    success_count = 0
    fail_count = 0
    processed_count = 0

    start_index, file_index = get_resume_info(batch_size)
    ids_to_process = ids[start_index:]

    print("========================\n")
    print(f"Tổng số ID: {len(ids)}")
    print(f"Bắt đầu từ ID thứ: {start_index + 1}")
    print(f"Số ID cần crawl: {len(ids_to_process)}\n")

    if not ids_to_process:
        print("Không còn ID nào để crawl.")
        return

    def process_one(product_id: str):
        raw, success = fetch_product(product_id)

        if not success or not raw:
            return {
                "id": product_id,
                "success": False,
                "product": None,
            }

        slim = transform_product(raw)
        return {
            "id": product_id,
            "success": slim is not None,
            "product": slim,
        }

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            for result in executor.map(process_one, ids_to_process):
                processed_count += 1

                if result["success"]:
                    batch.append(result["product"])
                    success_count += 1
                else:
                    fail_count += 1
                    append_failed_record({
                        "id": result["id"],
                        "error": "fetch_failed",
                    })

                if len(batch) >= batch_size:
                    save_batch(batch, file_index)
                    batch = []
                    file_index += 1

        except KeyboardInterrupt:
            from src.utils.notifier import send_discord_alert

            send_discord_alert("Program has been stopped inside ThreadPool.")
            executor.shutdown(wait=False, cancel_futures=True)
            raise

        except Exception as e:
            from src.utils.notifier import send_discord_alert

            send_discord_alert(f"**ThreadPool Crash**: {e}")
            executor.shutdown(wait=False, cancel_futures=True)
            raise

    if batch:
        save_batch(batch, file_index)

    elapsed = time.time() - start_time
    print("\n==== TỔNG KẾT ====")
    print(f"Thành công: {success_count}")
    print(f"Thất bại: {fail_count}")
    print(f"Thời gian: {elapsed:.1f} giây ({elapsed / 60:.1f} phút)")
