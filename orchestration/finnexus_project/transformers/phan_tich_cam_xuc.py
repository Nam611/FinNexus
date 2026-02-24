from mage_ai.data_preparation.decorators import transformer
import pandas as pd
from transformers import pipeline

# Khai báo biến toàn cục để lưu model (Cache)
if 'sentiment_model_cache' not in globals():
    globals()['sentiment_model_cache'] = None

@transformer
def transform(df: pd.DataFrame, *args, **kwargs):
    """
    Phân tích cảm xúc tài chính sử dụng FinBERT
    """
    # 1. Kiểm tra Model trong Cache
    if globals()['sentiment_model_cache'] is None:
        print("⏳ Đang tải Model FinBERT (Chỉ chạy lần đầu)...")
        try:
            globals()['sentiment_model_cache'] = pipeline(
                "sentiment-analysis",
                model="ProsusAI/finbert",
                tokenizer="ProsusAI/finbert",
                return_all_scores=True,
                device=-1 # CPU
            )
            print("✅ Model đã tải xong!")
        except Exception as e:
            print(f"❌ Lỗi tải Model: {e}")
            return df
    else:
        print("⚡ Sử dụng Model đã có sẵn trong bộ nhớ.")

    # Lấy model từ cache ra dùng
    sentiment_pipeline = globals()['sentiment_model_cache']

    # 2. Xử lý dữ liệu
    if df is None or df.empty:
        return df

    print(f"🤖 Đang phân tích {len(df)} dòng tin...")

    def analyze(text):
        try:
            if not text: return 0.0, "NEUTRAL"
            text = str(text)[:512] # Cắt ngắn
            
            # Dự đoán
            result = sentiment_pipeline(text)[0]
            
            # Map điểm số
            scores = {item['label']: item['score'] for item in result}
            
            # Công thức: Positive - Negative
            final_score = scores.get('positive', 0) - scores.get('negative', 0)
            
            # Gán nhãn
            if final_score > 0.05: label = "POSITIVE"
            elif final_score < -0.05: label = "NEGATIVE"
            else: label = "NEUTRAL"
            
            return final_score, label
        except:
            return 0.0, "NEUTRAL"

    # 3. Áp dụng
    if 'title' in df.columns:
        # Chạy trên cột title
        results = df['title'].apply(analyze)
        
        df['sentiment_score'] = [res[0] for res in results]
        df['sentiment_label'] = [res[1] for res in results]
        
        print("✅ Hoàn tất!")
        print(df[['title', 'sentiment_label', 'sentiment_score']].head(3))
    
    return df