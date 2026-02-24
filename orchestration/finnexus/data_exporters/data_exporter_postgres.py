from mage_ai.streaming.sinks.base_python import BasePythonSink
from typing import List, Dict
from sqlalchemy import create_engine
import json

if 'streaming_sink' not in globals():
    from mage_ai.data_preparation.decorators import streaming_sink

# Cấu hình kết nối (Dùng tên container fn_postgres)
DB_CONN = 'postgresql+psycopg2://admin:admin@fn_postgres:5432/finnexus'

@streaming_sink
class PostgresSink(BasePythonSink):
    def init_client(self):
        """
        Hàm này chỉ chạy 1 lần khi Pipeline khởi động.
        Dùng để tạo kết nối Database, giúp tối ưu hiệu suất.
        """
        self.engine = create_engine(DB_CONN)
        # Test kết nối ngay khi khởi động
        with self.engine.connect() as conn:
            pass
        print("✅ Kết nối Postgres thành công!")

    def batch_write(self, messages: List[Dict]):
        """
        Hàm này chạy liên tục mỗi khi có tin mới từ Kafka.
        Input: Một danh sách (List) các bài báo.
        """
        if not messages:
            return

        # Dùng connection từ engine đã khởi tạo
        with self.engine.connect() as connection:
            for row in messages:
                # 1. Chuẩn bị dữ liệu Tags cho Postgres (Dạng chuỗi "{Tag1,Tag2}")
                tags_list = row.get('tags', [])
                # Xử lý trường hợp tags có thể là None hoặc rỗng
                if not tags_list:
                    tags_str = "{}"
                else:
                    # Escape dấu phẩy nếu có trong tag
                    cleaned_tags = [str(t).replace(",", "") for t in tags_list]
                    tags_str = "{" + ",".join(cleaned_tags) + "}"
                
                # 2. Câu lệnh SQL Upsert (Chèn hoặc Cập nhật)
                query = """
                    INSERT INTO news_articles 
                    (source_id, source_name, title, url, content, collected_at, sentiment_score, sentiment_label, tags)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) 
                    DO UPDATE SET 
                        sentiment_score = EXCLUDED.sentiment_score,
                        sentiment_label = EXCLUDED.sentiment_label,
                        tags = EXCLUDED.tags;
                """
                
                try:
                    connection.execute(query, (
                        row.get('source_id'),
                        row.get('source_name'),
                        row.get('title'),
                        row.get('url'),
                        row.get('content'),
                        row.get('collected_at'),
                        row.get('sentiment_score'),
                        row.get('sentiment_label'),
                        tags_str
                    ))
                except Exception as e:
                    print(f"⚠️ Lỗi lưu bài viết {row.get('url')}: {e}")
                    
        print(f"💾 Đã lưu batch {len(messages)} tin.")