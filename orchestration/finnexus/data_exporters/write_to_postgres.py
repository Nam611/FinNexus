if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.io.postgres import Postgres
from pandas import DataFrame

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs):
    # 1. Kiểm tra dữ liệu rỗng
    if df.empty:
        print("⚠️ Không có dữ liệu để ghi vào DB.")
        return

    # 2. Cấu hình bảng
    schema_name = 'public'
    table_name = 'news_articles'
    
    # 3. KẾT NỐI TRỰC TIẾP (Bỏ qua file io_config.yaml để tránh lỗi)
    # Chúng ta khai báo thẳng thông tin container ở đây
    loader = Postgres(
        dbname='finnexus',
        user='admin',
        password='admin',
        host='fn_postgres', # Tên container database
        port=5432,
        schema=schema_name
    )

    # 4. Xuất dữ liệu
    with loader:
        print(f"🚀 Đang ghi {len(df)} dòng vào bảng '{table_name}'...")
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,
            # ĐỔI THÀNH 'replace' ĐỂ NÓ TỰ TẠO BẢNG MỚI FULL CỘT
            if_exists='replace', 
            allow_reserved_words=True
        )
        print("✅ Ghi vào PostgreSQL thành công!")