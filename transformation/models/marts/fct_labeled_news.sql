with source as (
    
    -- Lấy dữ liệu từ Staging (đã được làm sạch ID và ngày tháng)
    select * from {{ ref('stg_news_articles') }}

),

final as (

    select
        article_id,
        published_date,
        source_name,
        title,
        url,
        sentiment_label,
        sentiment_score,
        
        -- Thêm logic phân loại mạnh/nhẹ (Tùy chọn)
        case 
            when sentiment_score > 0.5 then 'Rất tích cực'
            when sentiment_score < -0.5 then 'Rất tiêu cực'
            else 'Bình thường'
        end as sentiment_intensity

    from source
    -- CHỈ LẤY BÀI ĐÃ CÓ NHÃN (Lọc rác)
    where sentiment_label is not null

)

select * from final