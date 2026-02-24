from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs):
    """
    Lưu bảng tương quan (Giá + Tin tức của 5 Mã) vào Postgres
    """
    schema_name = 'public'
    table_name = 'market_correlation'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    print(f"💾 Đang lưu {len(df)} dòng dữ liệu tương quan vào bảng '{table_name}'...")

    # 👇 BÍ KÍP "TRẢM TRƯỚC TẤU SAU": Chủ động xóa hẳn bảng cũ bằng lệnh SQL thuần
    drop_query = f"DROP TABLE IF EXISTS {schema_name}.{table_name} CASCADE;"
    
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as db:
        # Xóa sạch bảng cũ
        print("🧹 Đang dọn dẹp cấu trúc bảng cũ...")
        db.execute(drop_query)
        db.commit() # Chốt lệnh xóa
        
        # Nhét bảng mới tinh vào
        db.export(
            df,
            schema_name,
            table_name,
            index=False,
            if_exists='replace', 
            allow_reserved_words=True
        )
    
    print("✅ Đã tạo cấu trúc bảng mới 5 cột và lưu thành công!")
    print("🚀 Dashboard đã có thể dùng được tính năng Multi-Ticker.")