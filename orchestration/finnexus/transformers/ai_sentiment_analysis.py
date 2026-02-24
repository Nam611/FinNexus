import pandas as pd
from transformers import pipeline
import torch

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

@transformer
def transform(df, *args, **kwargs):
    """
    Trái tim AI của FinNexus: Phân tích cảm xúc Tiếng Việt (Deep Learning)
    Model: lxyuan/distilbert-base-multilingual-cased-sentiments-student
    """
    if df is None or df.empty:
        print("⚠️ Cảnh báo: Bảng dữ liệu trống, bỏ qua phân tích AI.")
        return df

    # --- 1. KHỞI TẠO MODEL (SINGLETON PATTERN) ---
    model_name = "lxyuan/distilbert-base-multilingual-cased-sentiments-student"
    model_key = 'finnexus_ai_model'
    
    if model_key not in globals():
        print(f"⏳ Khởi động động cơ AI ({model_name})...")
        try:
            globals()[model_key] = pipeline(
                "sentiment-analysis",
                model=model_name,
                top_k=None, # Lấy tất cả xác suất (Pos, Neg, Neu)
                device=-1   # Bắt buộc chạy CPU trong Docker để tránh lỗi tràn bộ nhớ
            )
            print("✅ Động cơ AI đã sẵn sàng!")
        except Exception as e:
            print(f"❌ KHẨN CẤP: Không thể nạp Model. Lỗi: {e}")
            df['sentiment_score'] = 0.0
            df['sentiment_label'] = 'NEUTRAL'
            return df
            
    analyzer = globals()[model_key]
    
    # 👇 ĐÃ FIX LỖI DẤU NGOẶC KÉP Ở ĐÂY
    print(f"🧠 AI đang đọc và 'chấm điểm' {len(df)} bài báo...")

    # --- 2. HÀM PHÂN TÍCH LÕI (BỌC THÉP CHỐNG CRASH) ---
    def get_sentiment(text):
        if not isinstance(text, str) or not text.strip():
            return 0.0, "NEUTRAL"
            
        safe_text = text[:500]
        
        try:
            raw_output = analyzer(safe_text)
            
            if isinstance(raw_output, list) and len(raw_output) > 0 and isinstance(raw_output[0], list):
                result_list = raw_output[0]
            elif isinstance(raw_output, list) and len(raw_output) > 0 and isinstance(raw_output[0], dict):
                result_list = raw_output
            else:
                return 0.0, "NEUTRAL"

            pos_score, neg_score = 0.0, 0.0
            for item in result_list:
                label = item.get('label', '').lower()
                score = item.get('score', 0.0)
                
                if label == 'positive': pos_score = score
                elif label == 'negative': neg_score = score

            final_score = pos_score - neg_score
            
            if final_score >= 0.15: 
                final_label = "POSITIVE"
            elif final_score <= -0.15: 
                final_label = "NEGATIVE"
            else: 
                final_label = "NEUTRAL"
                
            return final_score, final_label

        except Exception as e:
            print(f"⚠️ AI bỏ qua dòng '{safe_text[:20]}...': {e}")
            return 0.0, "NEUTRAL"

    # --- 3. THỰC THI & GÁN DỮ LIỆU ---
    scores = []
    labels = []
    
    for title in df['title']:
        s, l = get_sentiment(title)
        scores.append(s)
        labels.append(l)
        
    df['sentiment_score'] = scores
    df['sentiment_label'] = labels

    # --- 4. BÁO CÁO NGHIỆM THU ---
    print("="*60)
    print("🎯 BÁO CÁO AI PHÂN TÍCH (5 Mẫu nổi bật):")
    strong_sentiments = df[df['sentiment_label'] != 'NEUTRAL']
    
    if not strong_sentiments.empty:
        print(strong_sentiments[['title', 'sentiment_label', 'sentiment_score']].head(5))
    else:
        print("⚠️ Hôm nay thị trường quá bình yên, 100% tin tức là Trung lập (NEUTRAL).")
        print(df[['title', 'sentiment_label', 'sentiment_score']].head(3))
    print("="*60)

    return df