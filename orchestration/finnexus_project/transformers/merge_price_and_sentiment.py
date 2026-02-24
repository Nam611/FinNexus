import pandas as pd
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

# 👇 1. FIX LỖI: Đảm bảo Mage hiểu @transformer là gì
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(df_price, *args, **kwargs):
    """
    Kết hợp dữ liệu Giá (Mock/Real) và Tâm lý (Database)
    """
    # 1. LẤY DỮ LIỆU TÂM LÝ TỪ DATABASE
    query = """
    SELECT 
        DATE(published_at) as date_sent,
        AVG(sentiment_score) as avg_sentiment,
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
    
    print(f"🧠 Dữ liệu Tâm lý: {len(df_sentiment)} dòng.")
    
    # 2. CHUẨN HÓA NGÀY THÁNG
    # Bên Sentiment: Chuyển về datetime
    df_sentiment['Date'] = pd.to_datetime(df_sentiment['date_sent'])
    
    # Bên Price: Xử lý linh hoạt bất kể tên cột là gì
    if df_price is None or df_price.empty:
        print("⚠️ Cảnh báo: Không có dữ liệu giá đầu vào!")
        return pd.DataFrame()

    if 'Date' not in df_price.columns and 'time' in df_price.columns:
        df_price['Date'] = pd.to_datetime(df_price['time'])
    elif 'Date' in df_price.columns:
        df_price['Date'] = pd.to_datetime(df_price['Date'])

    # 🔥 BÍ KÍP: Cắt bỏ giờ phút giây (Normalize) để khớp 100%
    # Ví dụ: '2026-01-18 16:18:15' -> '2026-01-18 00:00:00'
    df_sentiment['Date'] = df_sentiment['Date'].dt.normalize()
    if 'Date' in df_price.columns:
        df_price['Date'] = df_price['Date'].dt.normalize()
    else:
        print("❌ Lỗi cấu trúc: Dữ liệu giá thiếu cột ngày tháng!")
        return pd.DataFrame()

    # 3. TRỘN 2 BẢNG (MERGE)
    # Chỉ giữ lại ngày nào có cả Tin tức VÀ Giá
    merged_df = pd.merge(df_price, df_sentiment, on='Date', how='inner')
    
    # Chọn cột cần thiết
    # Kiểm tra xem có cột Close không (vì mock data dùng Close viết hoa hoặc thường)
    price_col = 'Close' if 'Close' in merged_df.columns else 'close'
    
    if price_col in merged_df.columns:
        final_df = merged_df[['Date', price_col, 'avg_sentiment', 'news_count']]
        final_df.columns = ['Date', 'Stock_Price', 'Sentiment_Score', 'News_Count']
        
        # Sắp xếp
        final_df = final_df.sort_values('Date')

        print("="*30)
        print("✅ KẾT QUẢ SAU KHI TRỘN:")
        if final_df.empty:
            print("⚠️ Bảng rỗng! Ngày của dữ liệu Giả và Tin tức chưa trùng nhau.")
            print("👉 Ngày tin gần nhất:", df_sentiment['Date'].max())
            print("👉 Ngày giá gần nhất:", df_price['Date'].max())
        else:
            print(final_df.head())
            print(f"👉 Tổng cộng: {len(final_df)} điểm dữ liệu chung.")
        print("="*30)
        
        return final_df
    else:
        print(f"❌ Không tìm thấy cột giá ({price_col}) trong bảng sau khi merge.")
        return pd.DataFrame()