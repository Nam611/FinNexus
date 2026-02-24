-- 1. Xóa view cũ nếu có để tránh lỗi
DROP VIEW IF EXISTS public.stg_news_articles;

-- 2. Tạo View mới (Bao gồm cả cột Tickers)
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
        tickers, -- ✅ Đã lấy cột tickers
        CAST(NULLIF(collected_at, '') AS TIMESTAMP) as collected_at,
        COALESCE(published_at, CAST(NULLIF(collected_at, '') AS TIMESTAMP)) as published_at,
        sentiment_score,
        sentiment_label,
        -- Lọc trùng: Lấy tin mới nhất theo URL
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

-- 3. Select thử vài dòng để Mage báo xanh (Success)
SELECT * FROM public.stg_news_articles LIMIT 10;