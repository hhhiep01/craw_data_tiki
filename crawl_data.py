from pathlib import Path
import requests
import json
import time
from bs4 import BeautifulSoup
import csv
from concurrent.futures import ThreadPoolExecutor

API_URL = "https://api.tiki.vn/product-detail/api/v1/products/{}"
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1448251944413171815/6BVL0bl02ZGKJC7gHvIHYUvyGy8P3w9ULuL2JV27Khyu9raX3G-ADrEjfdaiYLOduKtf"
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

def fetch_product(product_id: str, max_retries: int = 3, backoff: float = 1.0):
    url = API_URL.format(product_id)

    last_exception: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=10)

            if response.status_code == 200:
                return response.json(), True

            if response.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff * attempt)
                continue
            return None, False

        except requests.RequestException as e:
            last_exception = e
            # Retry nếu còn lượt
            if attempt < max_retries:
                time.sleep(backoff * attempt)
            else:
                break

    return None, False

def clean_description(text: str) -> str:
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text().strip()


def read_ids_from_csv(path: str, max_ids: int | None = None):
    ids = []
    with open(path, "r", encoding="utf-8", newline="") as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader, None)  # bỏ header

        for row in reader:
            if row and row[0].strip():
                ids.append(row[0].strip())
                if max_ids is not None and len(ids) >= max_ids:
                    break
    return ids

def transform_product(raw: dict) -> dict | None:
    """
    Nhận full JSON của sản phẩm, trả về object rút gọn gồm:
    id, name, url_key, price, description, images
    """
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


def process_product(product_id: str):
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

def save_batch(batch, file_index: int):
    file_path = OUTPUT_DIR / f"batch_{file_index:04d}.json"
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(batch, file, ensure_ascii=False, indent=4)
    print(f"Đã lưu batch_{file_index:04d}.json ({len(batch)} records)")


# ========== RESUME INFO ==========
def get_get_resume_info(batch_size: int):
    """
    Dựa trên các file batch_* đã tồn tại để tính:
    - start_index: vị trí ID tiếp theo sẽ crawl
    - next_file_index: số thứ tự file batch tiếp theo

    Với điều kiện: các record trong batch được lưu THEO ĐÚNG THỨ TỰ ID.
    (đã đảm bảo nhờ dùng executor.map trong run())
    """
    file_index = 1
    last_existing_index = 0

    while (OUTPUT_DIR / f"batch_{file_index:04d}.json").exists():
        last_existing_index = file_index
        file_index += 1

    if last_existing_index == 0:
        return 0, 1

    last_file = OUTPUT_DIR / f"batch_{last_existing_index:04d}.json"

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


def run(batch_size: int = 1000, max_workers: int = 10):
    start_time = time.time()
    ids = read_ids_from_csv("products-0-200000.csv", max_ids=None)

    batch: list[dict] = []
    success_count = 0
    fail_count = 0
    processed_count = 0

    start_index, file_index = get_get_resume_info(batch_size)
    ids_to_process = ids[start_index:]

    print("========================\n")
    print(f"Tổng số ID: {len(ids)}")
    print(f"Bắt đầu từ ID thứ: {start_index + 1}")
    print(f"Số ID cần crawl: {len(ids_to_process)}\n")

    if not ids_to_process:
        print("Không còn ID nào để crawl.")
        return

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        try:
            for result in executor.map(process_product, ids_to_process):
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
            send_discord_alert("Program have been stopped")
            executor.shutdown(wait=False, cancel_futures=True)
            raise

        except Exception as e:
            send_discord_alert(f"*ThreadPool Crash**: {e}")
            executor.shutdown(wait=False, cancel_futures=True)
            raise

    if batch:
        save_batch(batch, file_index)

    elapsed = time.time() - start_time
    print("\n==== TỔNG KẾT ====")
    print(f"Thành công: {success_count}")
    print(f"Thất bại: {fail_count}")
    print(f"Thời gian: {elapsed:.1f} giây ({elapsed / 60:.1f} phút)")

def send_discord_alert(message : str):
    try:
        requests.post(
            DISCORD_WEBHOOK_URL,
            json={"content": message},
            timeout=5
        )
    except Exception as e:
        print("Can not send notification Discord:", e)

if __name__ == "__main__":
    try:
        send_discord_alert("START.")
        run()
        send_discord_alert("FINISH")
    except KeyboardInterrupt:
        send_discord_alert("Crawl Tiki have been stoped")
        raise
    except Exception as e:
        send_discord_alert(f"Crawl Tiki CRASH: {str(e)}")
        raise
