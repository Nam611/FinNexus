if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from minio import Minio
import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
import time
from datetime import datetime

@data_exporter
def export_data(data, *args, **kwargs):
    # --- DÒNG QUAN TRỌNG NHẤT VỪA THÊM VÀO ---
    # Chuyển đổi List (danh sách tin) thành DataFrame (Bảng)
    df = pd.DataFrame(data)
    
    # Kiểm tra nếu bảng rỗng thì dừng (để tránh lỗi)
    if df.empty:
        print("⚠️ Không có dữ liệu để lưu!")
        return

    # 1. Cấu hình kết nối MinIO
    minio_client = Minio(
        "fn-minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )
    bucket_name = "finnexus-lake"

    # 2. Chuyển đổi DataFrame sang Parquet
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    
    # 3. Tạo đường dẫn file: raw_news/Năm/Tháng/Ngày/timestamp.parquet
    now = datetime.now()
    file_path = f"raw_news/{now.year}/{now.month:02d}/{now.day:02d}/{int(time.time())}.parquet"
    
    # Reset con trỏ dữ liệu về đầu
    data_stream = io.BytesIO(buf.getvalue())

    # 4. Gửi lên MinIO
    try:
        # Nếu chưa có bucket thì tạo mới
        if not minio_client.bucket_exists(bucket_name):
            minio_client.make_bucket(bucket_name)
            
        minio_client.put_object(
            bucket_name,
            file_path,
            data=data_stream,
            length=len(buf.getvalue()),
            content_type="application/octet-stream"
        )
        print(f"✅ Đã lưu thành công vào: {bucket_name}/{file_path}")
    except Exception as e:
        print(f"❌ Lỗi khi lưu file: {e}")
        raise e