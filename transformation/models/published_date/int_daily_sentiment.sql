with source as (
    
    -- Lấy dữ liệu từ tầng Staging (đã làm sạch)
    select * from {{ ref('stg_news_articles') }}

),

daily_aggregation as (

    select
        -- Gom nhóm theo ngày và nguồn tin
        -- LƯU Ý: Dùng cột published_date (tên đã đổi ở staging) thay vì published_at
        date(published_date) as report_date,
        source_name,
        
        -- Đếm tổng số bài viết
        count(*) as total_articles,
        
        -- Tính điểm cảm xúc trung bình
        avg(sentiment_score) as avg_sentiment_score,
        
        -- Đếm số lượng bài theo từng loại cảm xúc
        count(case when sentiment_label = 'POSITIVE' then 1 end) as positive_count,
        count(case when sentiment_label = 'NEGATIVE' then 1 end) as negative_count,
        count(case when sentiment_label = 'NEUTRAL' then 1 end) as neutral_count

    from source
    group by 1, 2 

)

select * from daily_aggregation
order by report_date desc