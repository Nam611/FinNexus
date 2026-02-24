from mage_ai.streaming.sinks.base_python import BasePythonSink
from typing import List, Dict
from sqlalchemy import create_engine
from sqlalchemy.sql import text
# import json  <-- KHÔNG CẦN IMPORT CÁI NÀY NỮA

if 'streaming_sink' not in globals():
    from mage_ai.data_preparation.decorators import streaming_sink

# Chuỗi kết nối
DB_CONN = 'postgresql+psycopg2://admin:admin@fn_postgres:5432/finnexus'

@streaming_sink
class PostgresSink(BasePythonSink):
    def init_client(self):
        self.engine = create_engine(DB_CONN)
        print("✅ Kết nối Database thành công!")

    def batch_write(self, messages: List[Dict]):
        if not messages: return
        
        print(f"💾 Đang lưu {len(messages)} tin vào Database...")
        
        # Query giữ nguyên
        query = text("""
            INSERT INTO news_articles (source_id, source_name, title, url, content, sentiment_score, sentiment_label, tags)
            VALUES (:source_id, :source_name, :title, :url, :content, :sentiment_score, :sentiment_label, :tags)
            ON CONFLICT (url) DO UPDATE SET
                sentiment_score = EXCLUDED.sentiment_score,
                sentiment_label = EXCLUDED.sentiment_label,
                tags = EXCLUDED.tags;
        """)
        
        with self.engine.begin() as conn:
            for msg in messages:
                conn.execute(query, {
                    'source_id': msg.get('source_id'),
                    'source_name': msg.get('source_name'),
                    'title': msg.get('title'),
                    'url': msg.get('url'),
                    'content': msg.get('content', ''),
                    'sentiment_score': msg.get('sentiment_score', 0),
                    'sentiment_label': msg.get('sentiment_label', 'NEUTRAL'),
                    
                    # --- SỬA Ở ĐÂY: Bỏ json.dumps, truyền list trực tiếp ---
                    'tags': msg.get('tags', []) 
                    # -------------------------------------------------------
                })
        print(f"🎉 Đã lưu thành công {len(messages)} tin!")