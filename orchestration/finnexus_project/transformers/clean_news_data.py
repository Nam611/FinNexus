if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
import pandas as pd
import re

@transformer
def transform(data, *args, **kwargs):
    # 1. Chuyển thành DataFrame
    df = pd.DataFrame(data)
    if df.empty:
        return df

    # 2. Hàm làm sạch text (Dùng chung cho Title và Content)
    def clean_text(text):
        if not isinstance(text, str): return ""
        text = re.sub(r'[\n\t\r]', ' ', text) # Thay xuống dòng bằng khoảng trắng
        text = re.sub(r'\s+', ' ', text).strip() # Xóa khoảng trắng thừa
        return text

    # 3. Làm sạch Title
    if 'title' in df.columns:
        df['title'] = df['title'].apply(clean_text)

    # 4. Làm sạch Content (QUAN TRỌNG: Giữ lại một chút xuống dòng để dễ đọc nếu muốn)
    # Nhưng để lưu DB gọn gàng, ta tạm thời clean hết.
    if 'content' in df.columns:
        print(f"Content found! Cleaning {len(df)} articles...")
        # Với content, ta có thể muốn giữ lại \n để phân đoạn, tùy bạn. 
        # Ở đây mình clean sạch để AI vector dễ xử lý sau này.
        df['content'] = df['content'].apply(clean_text)
    
    # 5. Drop trùng lặp
    df = df.drop_duplicates(subset=['url'], keep='last')
    
    return df