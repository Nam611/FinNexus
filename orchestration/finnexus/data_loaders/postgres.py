from mage_ai.io.postgres import Postgres
from mage_ai.io.config import ConfigFileLoader
from mage_ai.settings.repo import get_repo_path
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data(*args, **kwargs):
    # Câu lệnh SQL tạo View chuẩn
    query = """
    DROP VIEW IF EXISTS public.stg_news_articles;

    CREATE OR REPLACE VIEW public.stg_news_articles AS
    WITH source AS (
        SELECT * FROM public.news_articles
    ),
    deduplicated AS (
        SELECT 
            url as article_id,
            source_name,
            title,
            content,
            tickers, -- ✅ Cột tickers quan trọng
            CAST(NULLIF(collected_at, '') AS TIMESTAMP) as collected_at,
            COALESCE(published_at, CAST(NULLIF(collected_at, '') AS TIMESTAMP)) as published_at,
            sentiment_score,
            sentiment_label,
            ROW_NUMBER() OVER (PARTITION BY url ORDER BY collected_at DESC) as row_num
        FROM source
    )
    SELECT 
        article_id,
        source_name,
        title,
        content,
        tickers,
        published_at,
        collected_at,
        sentiment_score,
        sentiment_label
    FROM deduplicated
    WHERE row_num = 1;
    """
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        print("🔥 Đang tạo View báo cáo...")
        loader.execute(query) # Dùng hàm execute để chạy lệnh tạo View
        print("✅ Đã tạo View 'public.stg_news_articles' thành công!")
        
    return {}