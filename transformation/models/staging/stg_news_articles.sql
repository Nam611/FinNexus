with source as (
    
    select * from {{ source('finnexus_source', 'news_articles') }}

),

renamed as (

    select
        md5(url) as article_id,
        title,
        content,
        url,
        
        -- LOGIC QUAN TRỌNG: Xử lý lỗi NULL
        -- Nếu ingested_at có dữ liệu -> Dùng nó.
        -- Nếu ingested_at bị rỗng (NULL) -> Lấy giờ hiện tại (current_timestamp).
        coalesce(
            to_timestamp(ingested_at)::timestamp, 
            current_timestamp
        ) as published_date,
        
        source_name,
        sentiment_score,
        sentiment_label,
        
        to_timestamp(ingested_at) as ingested_at

    from source

)

select * from renamed