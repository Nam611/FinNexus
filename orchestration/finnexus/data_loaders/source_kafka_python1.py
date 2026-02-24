from mage_ai.streaming.sources.base_python import BasePythonSource
from kafka import KafkaConsumer
import json
import time

if 'streaming_source' not in globals():
    from mage_ai.data_preparation.decorators import streaming_source

@streaming_source
class CustomKafkaSource(BasePythonSource):
    def init_client(self):
        print("🚀 Đang khởi tạo kết nối Kafka...")
        self.consumer = KafkaConsumer(
            'financial_news',
            bootstrap_servers=['fn_redpanda:9092'],
            auto_offset_reset='earliest',  # QUAN TRỌNG: Đọc từ tin cũ nhất
            enable_auto_commit=True,
            group_id='finnexus_clean_v2_start', # Tên nhóm mới tinh
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("✅ Kết nối Kafka thành công!")

    def batch_read(self, handler):
        try:
            # Hút 50 tin mỗi lần
            data = self.consumer.poll(timeout_ms=1000, max_records=50)
            batch = []
            for _, messages in data.items():
                for msg in messages:
                    batch.append(msg.value)
            
            if batch:
                print(f"🔥 Hút được {len(batch)} tin! Đang đẩy sang Transformer...")
                handler(batch) # Đẩy dữ liệu đi tiếp
        except Exception as e:
            print(f"💀 Lỗi đọc Kafka: {e}")