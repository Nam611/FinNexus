from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data(*args, **kwargs):
    """
    Load data from PostgreSQL: Lấy các bài báo chưa được phân tích AI
    """
    ## Câu lệnh SQL để lấy dữ liệu (LẤY TẤT CẢ 100 TIN MỚI NHẤT)
    query = """
    SELECT * FROM public.news_articles 
    -- Bỏ dòng WHERE đi để ép AI chạy lại từ đầu
    -- WHERE sentiment_label IS NULL OR sentiment_label = '' 
    ORDER BY published_at DESC
    LIMIT 100;
    """
    
    # Kết nối DB và lấy dữ liệu về dưới dạng DataFrame
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'local_db'

    try:
        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            df = loader.load(query)
            print(f"✅ Đã tải được {len(df)} bài báo từ Database.")
            return df
    except Exception as e:
        print(f"❌ Lỗi kết nối DB: {e}")
        return None