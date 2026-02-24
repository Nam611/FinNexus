import pandas as pd
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(df_price, *args, **kwargs):
    """
    Kết hợp dữ liệu Giá (Multi-Ticker) và Tâm lý (Database)
    """
    # 1. LẤY DỮ LIỆU TÂM LÝ TỪ DATABASE
    query = """
    SELECT 
        DATE(published_at) as date_sent,
        -- 👇 ÉP KIỂU TEXT SANG SỐ THỰC (FLOAT) ĐỂ TÍNH TRUNG BÌNH
        AVG(CAST(sentiment_score AS FLOAT)) as avg_sentiment,
        COUNT(*) as news_count
    FROM public.news_articles
    WHERE sentiment_score IS NOT NULL
    GROUP BY DATE(published_at)
    ORDER BY date_sent DESC;
    """
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        df_sentiment = loader.load(query)
    
    print(f"🧠 Dữ liệu Tâm lý: {len(df_sentiment)} ngày.")
    
    # 2. CHUẨN HÓA NGÀY THÁNG VÀ CỘT
    if df_price is None or df_price.empty:
        print("⚠️ Cảnh báo: Không có dữ liệu giá đầu vào!")
        return pd.DataFrame()

    df_sentiment['Date'] = pd.to_datetime(df_sentiment['date_sent']).dt.normalize()
    
    if 'Date' not in df_price.columns and 'time' in df_price.columns:
        df_price['Date'] = pd.to_datetime(df_price['time'])
    elif 'Date' in df_price.columns:
        df_price['Date'] = pd.to_datetime(df_price['Date'])

    df_price['Date'] = df_price['Date'].dt.normalize()

    # 3. TRỘN 2 BẢNG VÀ GIỮ LẠI TICKER
    merged_df = pd.merge(df_price, df_sentiment, on='Date', how='inner')
    
    # Đảm bảo lấy đúng cột Close và Ticker
    price_col = 'Close' if 'Close' in merged_df.columns else 'close'
    
    if price_col in merged_df.columns and 'ticker' in merged_df.columns:
        # 👇 QUAN TRỌNG: Thêm 'ticker' vào danh sách cột cuối cùng
        final_df = merged_df[['Date', 'ticker', price_col, 'avg_sentiment', 'news_count']]
        final_df.columns = ['Date', 'Ticker', 'Stock_Price', 'Sentiment_Score', 'News_Count']
        
        # Sắp xếp theo ngày MỚI NHẤT trước
        final_df = final_df.sort_values(['Date', 'Ticker'], ascending=[False, True])

        print("="*40)
        print("✅ KẾT QUẢ SAU KHI TRỘN (5 NGÀY MỚI NHẤT):")
        if final_df.empty:
            print("⚠️ Bảng rỗng! Ngày của dữ liệu Giả và Tin tức chưa trùng nhau.")
        else:
            print(final_df.head(10)) # In 10 dòng để thấy đủ các mã của ngày mới nhất
            print(f"👉 Tổng cộng: {len(final_df)} điểm dữ liệu chung.")
        print("="*40)
        
        return final_df
    else:
        print("❌ Lỗi cấu trúc: Bảng giá bị thiếu cột 'Close' hoặc 'ticker'.")
        return pd.DataFrame()