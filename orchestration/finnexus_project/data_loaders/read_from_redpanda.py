if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from kafka import KafkaConsumer
import json
import time
import pandas as pd

@data_loader
def load_data(*args, **kwargs):
    # --- 1. CẤU HÌNH CHUẨN TỪ DOCKER PS ---
    topic_name = 'financial_news'
    
    # ĐÂY LÀ CHÌA KHÓA: Tên container là 'fn_redpanda'
    bootstrap_servers = ['fn_redpanda:9092'] 

    # Đổi Group ID một lần nữa để đảm bảo đọc mới tinh
    group_id = 'mage_reader_final_success'

    print(f"🚀 Đang kết nối tới {bootstrap_servers} | Topic: {topic_name}...")

    # --- 2. KẾT NỐI ---
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest', # Bắt buộc đọc từ đầu
            enable_auto_commit=True,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=5000 
        )
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        return pd.DataFrame()

    messages = []
    
    # --- 3. LẤY TIN ---
    print("📥 Đang quét dữ liệu từ fn_redpanda...")
    for message in consumer:
        row = message.value
        row['ingested_at'] = int(time.time())
        messages.append(row)
        
        # Lấy tối đa 1000 tin
        if len(messages) >= 1000:
            break

    print(f"✅ THÀNH CÔNG RỰC RỠ: Lấy được {len(messages)} tin bài.")
    return pd.DataFrame(messages)