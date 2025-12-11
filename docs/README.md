# TIKI Product Crawler

Project crawl dữ liệu sản phẩm Tiki 

## 1. Cài đặt môi trường

install requirements:

    pip install -r requirements.txt

## 2. Cấu hình

Thay đổi cấu hình trong file config/settings.py:

    BATCH_SIZE = 1000
    MAX_WORKERS = 10
    RAW_IDS_FILE = RAW_DIR / "products-0-200000.csv"
    DISCORD_WEBHOOK_URL = "your_webhook_url"

## 3. Chạy chương trình

    python main.py

Chương trình sẽ:
- Đọc danh sách product_id
- Crawl dữ liệu từ API Tiki
- Transform và lọc các field cần thiết
- Lưu dữ liệu theo batch (batch_0001.json, batch_0002.json,…)
- Ghi lỗi vào failed_records.csv
- Gửi thông báo Discord khi bắt đầu, kết thúc hoặc gặp lỗi

## 4. Tính năng chính

### Resume
Tự tính vị trí crawl tiếp theo dựa trên batch đã tồn tại:

    start_index, file_index = get_resume_info(batch_size)

### Retry khi API lỗi
Retry với backoff khi gặp mã lỗi 429, 500, 502, 503, 504:

    fetch_product(product_id, max_retries=3, backoff=1.0)

### Đa luồng

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:

### Lưu batch dữ liệu

    processed/batch_0001.json
    processed/batch_0002.json

### Log lỗi

    data/failed_records.csv

### Discord Alert

    send_discord_alert("START")
    send_discord_alert("FINISH")
    send_discord_alert("CRASH: ...")

## 5. Output

- Dữ liệu thành công: thư mục processed
- Lỗi: file failed_records.csv


