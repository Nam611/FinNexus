from mage_ai.data_preparation.decorators import transformer
import pandas as pd
from transformers import pipeline

# Khai báo biến toàn cục (nhưng chưa tải gì cả)
if 'sentiment_model' not in globals():
    sentiment_model = None

@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.
    """
    # 1. Kích hoạt Model (Chỉ tải khi hàm này được chạy)
    global sentiment_model
    if sentiment_model is None:
        print("⏳ Đang tải Model AI (Lần đầu sẽ mất 30s)...")
        # Tải model ở đây để không làm đơ Mage lúc khởi động
        sentiment_model = pipeline("sentiment-analysis", model="wonrax/phobert-base-vietnamese-sentiment")
    
    # 2. Kiểm tra dữ liệu
    if df is None or df.empty:
        print("⚠️ Không có dữ liệu!")
        return pd.DataFrame()

    print(f"🚀 Đang phân tích {len(df)} bài tin...")

    # 3. Hàm xử lý từng dòng
    def analyze(text):
        try:
            # Cắt ngắn text
            clean_text = str(text)[:256] 
            
            # AI dự đoán
            result = sentiment_model(clean_text)[0]
            label = result['label']
            score = result['score']

            if label in ['POS', 'POSITIVE']:
                return 'POSITIVE', score
            elif label in ['NEG', 'NEGATIVE']:
                return 'NEGATIVE', -score
            else:
                return 'NEUTRAL', 0.0
        except Exception:
            return 'NEUTRAL', 0.0

    # 4. Chạy AI
    results = [analyze(t) for t in df['title']]

    df['sentiment_label'] = [r[0] for r in results]
    df['sentiment_score'] = [r[1] for r in results]

    print("✅ Xong! Đã có kết quả phân tích.")
    return df