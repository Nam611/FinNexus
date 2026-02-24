from mage_ai.io.postgres import Postgres
from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data(*args, **kwargs):
    query = "DROP TABLE IF EXISTS public.news_articles CASCADE;"
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        # Dùng hàm execute thay vì load/export để chạy lệnh quản trị
        print("🔥 Đang xóa bảng cũ...")
        loader.execute(query) 
        print("✅ Đã xóa bảng news_articles thành công!")
        
    return {}