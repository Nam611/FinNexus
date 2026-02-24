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
    Lưu bảng tương quan (Giá + Tin tức) vào Postgres
    Tên bảng: market_correlation
    """
    schema_name = 'public'
    table_name = 'market_correlation'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    print(f"💾 Đang lưu {len(df)} dòng dữ liệu tương quan vào bảng '{table_name}'...")

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            schema_name,
            table_name,
            index=False,
            if_exists='replace',  # Ghi đè lại bảng cũ mỗi lần chạy để update dữ liệu mới nhất
            allow_reserved_words=True
        )
    
    print("✅ Đã lưu thành công! Dashboard sẵn sàng hiển thị.")