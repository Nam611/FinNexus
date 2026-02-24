import pandas as pd
from datetime import datetime
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_dummy_data(*args, **kwargs):
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    # Dữ liệu chuẩn với cột 'url'
    df_moi = pd.DataFrame({
        'title': [
            'Thị trường chứng khoán Việt Nam bùng nổ nhờ AI', 
            'VN-Index lao dốc vì áp lực chốt lời'
        ],
        'url': ['https://cafef.vn/demo1.chn', 'https://cafef.vn/demo2.chn'], 
        'published_at': [datetime.now(), datetime.now()],
        'sentiment_score': [None, None], 
        'sentiment_label': [None, None]
    })
    
    print("🚀 Đang đập bảng cũ đi và xây lại bảng mới với cột 'url'...")
    
    try:
        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as db:
            # 👇 CHIÊU BÍ MẬT: Xóa sạch bảng cũ không để lại dấu vết
            db.execute("DROP TABLE IF EXISTS public.news_articles CASCADE;")
            db.commit()
            
            # Xây bảng mới tinh
            db.export(
                df_moi,
                'public',
                'news_articles',
                index=False,
                if_exists='replace',
                allow_reserved_words=True
            )
        print("✅ ĐÃ XÂY XONG BẢNG TRÊN NEON CLOUD (Chuẩn 100% cột url)!")
    except Exception as e:
        print(f"❌ Có lỗi xảy ra: {e}")