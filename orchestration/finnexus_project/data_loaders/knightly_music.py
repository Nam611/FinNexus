from mage_ai.io.postgres import Postgres
from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data(*args, **kwargs):
    # Câu lệnh SQL theo thứ tự chuẩn:
    query = """
    -- BƯỚC 1: Dọn dẹp dữ liệu trùng lặp (Giữ lại dòng mới nhất, xóa các dòng cũ trùng URL)
    DELETE FROM public.news_articles a USING (
        SELECT MIN(ctid) as ctid, url
        FROM public.news_articles 
        GROUP BY url HAVING COUNT(*) > 1
    ) b
    WHERE a.url = b.url 
    AND a.ctid <> b.ctid;

    -- BƯỚC 2: Xóa ràng buộc cũ (nếu lỡ tạo sai)
    ALTER TABLE public.news_articles DROP CONSTRAINT IF EXISTS unique_url;

    -- BƯỚC 3: Tạo ràng buộc DUY NHẤT (Unique) cho cột URL
    ALTER TABLE public.news_articles ADD CONSTRAINT unique_url UNIQUE (url);
    """
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        print("🔥 Đang dọn dẹp Database và tạo Unique Constraint...")
        loader.execute(query) # Dùng execute để chạy lệnh quản trị
        print("✅ THÀNH CÔNG! Database đã sạch và an toàn.")
        
    return {}