if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@transformer
def transform(df, *args, **kwargs):
    """
    Phân tích cảm xúc Tiếng Việt (Phiên bản Fix Lỗi 'string indices')
    Model: lxyuan/distilbert-base-multilingual-cased-sentiments-student
    """
    import pandas as pd
    from transformers import pipeline
    import torch

    # 1. KHỞI TẠO MODEL (Dùng tham số top_k=None)
    model_key = 'vn_sentiment_fixed_v2' 
    model_name = "lxyuan/distilbert-base-multilingual-cased-sentiments-student"
    
    if model_key not in globals():
        print(f"⏳ Đang tải Model: {model_name}...")
        try:
            globals()[model_key] = pipeline(
                "sentiment-analysis",
                model=model_name,
                # 👇 QUAN TRỌNG: Dùng top_k=None để lấy điểm của cả 3 nhãn (Pos, Neg, Neu)
                top_k=None, 
                device=-1
            )
            print("✅ Model tải thành công!")
        except Exception as e:
            print(f"❌ Lỗi tải Model: {e}")
            return df
    
    sentiment_pipeline = globals()[model_key]

    if df is None or df.empty:
        return df

    print(f"🤖 Đang phân tích {len(df)} dòng tin...")

    # Hàm phân tích đã được bọc thép
    def analyze(text):
        try:
            if not text: return 0.0, "NEUTRAL"
            text = str(text)[:512]
            
            # Gọi model
            raw_output = sentiment_pipeline(text)
            
            # 👇 LOGIC XỬ LÝ LINH HOẠT (Chống lỗi cấu trúc)
            # Model có thể trả về [[{...}, {...}]] hoặc [{...}, {...}]
            if isinstance(raw_output, list) and len(raw_output) > 0:
                if isinstance(raw_output[0], list):
                    # Trường hợp danh sách lồng nhau (Batch)
                    result_list = raw_output[0]
                else:
                    # Trường hợp danh sách phẳng
                    result_list = raw_output
            else:
                return 0.0, "NEUTRAL"

            # Chuyển đổi thành Dict để dễ lấy điểm: {'positive': 0.9, 'negative': 0.1, ...}
            scores = {}
            for item in result_list:
                if isinstance(item, dict) and 'label' in item and 'score' in item:
                    scores[item['label']] = item['score']
            
            # Tính điểm
            pos = scores.get('positive', 0)
            neg = scores.get('negative', 0)
            final_score = pos - neg
            
            # Ngưỡng (Lớn hơn 0 là tính)
            if final_score > 0.001: label = "POSITIVE"
            elif final_score < -0.001: label = "NEGATIVE"
            else: label = "NEUTRAL"
            
            return final_score, label
            
        except Exception as e:
            print(f"⚠️ Lỗi dòng: '{text[:15]}...': {e}")
            return 0.0, "NEUTRAL"

    # Chạy phân tích
    results = [analyze(x) for x in df['title']]
    
    df['sentiment_score'] = [r[0] for r in results]
    df['sentiment_label'] = [r[1] for r in results]

    print("="*40)
    print("📊 KẾT QUẢ KIỂM TRA (Đã có màu Xanh/Đỏ chưa?):")
    # In ra 5 dòng có điểm số KHÁC 0 để kiểm chứng
    non_neutral = df[df['sentiment_score'] != 0]
    if not non_neutral.empty:
        print(non_neutral[['title', 'sentiment_label', 'sentiment_score']].head(5))
    else:
        print("⚠️ Vẫn toàn NEUTRAL (Có thể do nội dung tin quá trung lập)")
        print(df[['title', 'sentiment_label', 'sentiment_score']].head(3))
    print("="*40)

    return df