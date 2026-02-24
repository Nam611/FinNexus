import streamlit as st
import pandas as pd
import psycopg2
import os 

# --- CẤU HÌNH KẾT NỐI NEON CLOUD (THAY THẾ ĐOẠN CŨ) ---
def get_db_connection():
    """
    Kết nối thông minh:
    Ưu tiên lấy thông tin từ Streamlit Secrets (Khi lên mạng)
    Nếu không thấy thì lấy từ cấu hình Neon cứng (Khi chạy local)
    """
    
    # 1. Thông tin Neon Cloud của Nam (Dùng cho chạy thử ở máy)
    NEON_HOST = "ep-curly-dust-a1k3kq61-pooler.ap-southeast-1.aws.neon.tech"
    NEON_USER = "neondb_owner"
    NEON_PASS = "npg_fby6LhI1giro"
    NEON_DB   = "neondb"

    # 2. Kiểm tra nếu có biến môi trường (Lúc lên Streamlit Cloud sau này)
    db_host = os.getenv('DB_HOST', NEON_HOST)
    db_user = os.getenv('DB_USER', NEON_USER)
    db_pass = os.getenv('DB_PASS', NEON_PASS)
    db_name = os.getenv('DB_NAME', NEON_DB)

    try:
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_pass,
            host=db_host, 
            port="5432",
            sslmode="require" # 👈 QUAN TRỌNG: Neon bắt buộc phải có cái này mới cho vào!
        )
        return conn
    except Exception as e:
        st.error(f"❌ Không thể kết nối tới Neon Cloud ({db_host}): {e}")
        return None