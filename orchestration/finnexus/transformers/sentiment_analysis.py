from typing import Dict, List
from textblob import TextBlob

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

def get_sentiment(text):
    """Phân tích cảm xúc: Trả về Score và Label"""
    if not text:
        return 0, "NEUTRAL"
    
    # TextBlob phân tích (Score từ -1.0 đến 1.0)
    analysis = TextBlob(text)
    score = analysis.sentiment.polarity
    
    # Quy đổi ra nhãn
    if score > 0.1:
        label = "POSITIVE"
    elif score < -0.1:
        label = "NEGATIVE"
    else:
        label = "NEUTRAL"
    return score, label

def extract_tags(text):
    """Gắn thẻ từ khóa đơn giản (Rule-based)"""
    text_lower = text.lower()
    tags = []
    keywords = {
        "vàng": "Gold",
        "bất động sản": "Real Estate",
        "chứng khoán": "Stock",
        "cổ phiếu": "Stock",
        "lãi suất": "Interest Rate",
        "ngân hàng": "Banking",
        "vinfast": "VinFast",
        "vn-index": "Stock"
    }
    
    for k, v in keywords.items():
        if k in text_lower:
            tags.append(v)
            
    return list(set(tags)) # Loại bỏ trùng lặp

@transformer
def transform(messages: List[Dict], *args, **kwargs):
    """
    Input: Danh sách tin nhắn từ Kafka
    Output: Danh sách tin nhắn ĐÃ ĐƯỢC BỔ SUNG thông tin AI
    """
    enriched_data = []
    print(f"⚡ Transformer nhận được {len(messages)} tin nhắn!")

    for msg in messages:
        # Lấy nội dung tin bài
        content = msg.get('content', '')
        title = msg.get('title', '')
        full_text = f"{title} {content}"

        # 1. Phân tích cảm xúc
        score, label = get_sentiment(full_text)
        
        # 2. Trích xuất Tags
        tags = extract_tags(full_text)

        # 3. Bổ sung vào dữ liệu gốc
        msg['sentiment_score'] = score
        msg['sentiment_label'] = label
        msg['tags'] = tags
        
        enriched_data.append(msg)

    return enriched_data