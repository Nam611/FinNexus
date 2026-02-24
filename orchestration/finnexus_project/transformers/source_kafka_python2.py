from typing import Dict, List
from textblob import TextBlob

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

def get_sentiment(text):
    if not text: return 0, "NEUTRAL"
    score = TextBlob(text).sentiment.polarity
    if score > 0.1: label = "POSITIVE"
    elif score < -0.1: label = "NEGATIVE"
    else: label = "NEUTRAL"
    return score, label

def extract_tags(text):
    text_lower = text.lower()
    keywords = {"vàng": "Gold", "bất động sản": "Real Estate", "chứng khoán": "Stock", "lãi suất": "Interest Rate", "vinfast": "VinFast"}
    tags = [v for k, v in keywords.items() if k in text_lower]
    return list(set(tags))

@transformer
def transform(messages: List[Dict], *args, **kwargs):
    enriched_data = []
    print(f"⚡ Transformer đang xử lý {len(messages)} tin...")
    
    for msg in messages:
        full_text = f"{msg.get('title', '')} {msg.get('content', '')}"
        score, label = get_sentiment(full_text)
        tags = extract_tags(full_text)
        
        msg['sentiment_score'] = score
        msg['sentiment_label'] = label
        msg['tags'] = tags
        enriched_data.append(msg)
        
    return enriched_data