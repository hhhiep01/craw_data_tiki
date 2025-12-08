from pathlib import Path
import requests
import json
import time
from bs4 import BeautifulSoup
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(exist_ok=True)
FAILED_FILE = OUTPUT_DIR / "failed_records.csv"

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


def fetch_product(product_id: str):
    url = API_URL.format(product_id)

    try:
        response = session.get(url, timeout=10)

        if response.status_code == 200:
            return response.json(), True
        else:
            return None, False
    except Exception as e:
        return None, False


def clean_description(text: str) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    text = soup.get_text().strip()
    return text


def read_ids_from_csv(path: str, max_ids: int | None = None):
    ids = []
    with open(path, "r", encoding="utf-8", newline="") as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader, None)

        for row in reader:
            if row and row[0].strip():
                ids.append(row[0].strip())
                if max_ids is not None and len(ids) >= max_ids:
                    break
    return ids


def process_product(product_id: str):
    product_data, success = fetch_product(product_id)
    if success and product_data and "description" in product_data:
        product_data["description"] = clean_description(product_data["description"])

    return {
        "id": product_id,
        "product": product_data,
        "success": success,
    }


def save_batch(batch, file_index: int):
    file_path = OUTPUT_DIR / f"batch_{file_index:04d}.json"
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(batch, file, ensure_ascii=False, indent=4)
    print(f"Đã lưu batch_{file_index:04d}.json ({len(batch)} records)")


def get_get_resume_info(batch_size: int):
    file_index = 1
    last_existing_index = 0

    while(OUTPUT_DIR / f"batch_{file_index:04d}.json").exists():
        last_existing_index = file_index
        file_index += 1

    if last_existing_index == 0:
        return 0 , 1

    last_file = OUTPUT_DIR / f"batch_{last_existing_index:04d}.json"

    try:
        with open(last_file, "r", encoding="utf-8") as file:
            data = json.load(file)
        count_last = len(data) if isinstance(data, list) else 0
    except Exception as e:
        start_index = (last_existing_index - 1) * batch_size
        return start_index, last_existing_index + 1

    start_index = (last_existing_index - 1) * batch_size + count_last
    next_file_index = last_existing_index + 1



    return start_index, next_file_index

def append_failed_record(record: dict):
    file_exist = FAILED_FILE.exists()

    with open(FAILED_FILE, "a", encoding="utf-8",newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["id", "error"])
        if not file_exist:
            writer.writeheader()
        writer.writerow(record)

def run(batch_size=1000):
    start_time = time.time()
    ids = read_ids_from_csv("products-0-200000.csv", max_ids=None)

    batch = []
    failed_records = []
    success_count = 0
    fail_count = 0
    processed_count = 0

    start_index, file_index = get_get_resume_info(batch_size)
    ids_to_process = ids[start_index:]
    print("========================\n")

    print(f"Tổng số ID: {len(ids)}")
    print(f"Bắt đầu từ ID thứ: {start_index + 1}")
    print(f"Số ID cần crawl: {len(ids_to_process)}")


    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {}
        for pid in ids_to_process:
            future = executor.submit(process_product, pid)
            futures[future] = pid

        for future in as_completed(futures):
            pid = futures[future]

            try:
                result = future.result()
                processed_count += 1

                if result["success"]:
                    success_count += 1
                    batch.append(result)
                else:
                    fail_count += 1
                    rec = {
                        "id": pid,
                        "error": "fetch_failed",
                    }
                    failed_records.append(rec)
                    append_failed_record(rec)

                if len(batch) >= batch_size:
                    save_batch(batch, file_index)
                    batch = []
                    file_index += 1

            except Exception as e:
                fail_count += 1
                processed_count += 1

    if batch:
        save_batch(batch, file_index)

    elapsed = time.time() - start_time
    print("\n==== TỔNG KẾT ====")
    print(f"Thành công: {success_count}")
    print(f"Thất bại: {fail_count}")
    print(f"Thời gian: {elapsed:.1f} giây ({elapsed/60:.1f} phút)")


if __name__ == "__main__":
    run()


