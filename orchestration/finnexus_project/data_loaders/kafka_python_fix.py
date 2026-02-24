from mage_ai.streaming.sources.base_python import BasePythonSource
from kafka import KafkaConsumer
import json
import time

if 'streaming_source' not in globals():
    from mage_ai.data_preparation.decorators import streaming_source

@streaming_source
class CustomKafkaSource(BasePythonSource):
    def init_client(self):
        """
        Khởi tạo kết nối - Chạy 1 lần duy nhất khi Pipeline bắt đầu
        """
        print("🚀 Đang khởi tạo kết nối Kafka Python...")
        self.consumer = KafkaConsumer(
            'financial_news',
            bootstrap_servers=['fn_redpanda:9092'],
            # QUAN TRỌNG: Đọc từ đầu (earliest) để lấy 6000 tin cũ
            auto_offset_reset='earliest', 
            enable_auto_commit=True,
            # Đổi tên group để ép Kafka gửi lại dữ liệu từ đầu
            group_id='finnexus_python_worker_final_v100', 
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("✅ Đã kết nối Kafka thành công!")

    def batch_read(self, handler):
        """
        Hàm này chạy vòng lặp liên tục để hút tin
        """
        try:
            # Hút tối đa 50 tin mỗi lần
            data = self.consumer.poll(timeout_ms=1000, max_records=50)
            
            batch = []
            for _, messages in data.items():
                for msg in messages:
                    batch.append(msg.value)
            
            if batch:
                print(f"🔥 Hút được {len(batch)} tin! Đang đẩy sang Transformer...")
                handler(batch)
                
        except Exception as e:
            print(f"💀 Lỗi đọc Kafka: {e}")