import streamlit as st
import pandas as pd
import psycopg2
import os

# --- 1. CẤU HÌNH KẾT NỐI (BẢO MẬT & LINH HOẠT) ---
def get_db_connection():
    # Ưu tiên lấy từ Streamlit Secrets (Khi đã Deploy lên Cloud)
    if "postgres" in st.secrets:
        creds = st.secrets["postgres"]
        host = creds.get("host")
        user = creds.get("user")
        password = creds.get("password")
        database = creds.get("database")
    else:
        # Fallback về cấu hình cứng (Khi Nam chạy thử ở máy Local)
        host = os.getenv('DB_HOST', "ep-curly-dust-a1k3kq61-pooler.ap-southeast-1.aws.neon.tech")
        user = os.getenv('DB_USER', "neondb_owner")
        password = os.getenv('DB_PASS', "npg_fby6LhI1giro")
        database = os.getenv('DB_NAME', "neondb")

    try:
        conn = psycopg2.connect(
            dbname=database, user=user, password=password,
            host=host, port="5432", sslmode="require"
        )
        return conn
    except Exception as e:
        st.error(f"❌ Lỗi kết nối Database: {e}")
        return None

# --- 2. HÀM LOAD DỮ LIỆU TIN TỨC (ĐÃ FIX LỖI SOURCE_NAME & TYPEERROR) ---
@st.cache_data(ttl=600)
def load_data(ticker=None, days=7):
    conn = get_db_connection()
    if not conn: return pd.DataFrame()
    try:
        # 🛠 Đã loại bỏ source_name vì Database chưa có cột này
        query = f"""
            SELECT published_at, title, sentiment_label, sentiment_score, url 
            FROM public.news_articles 
            WHERE published_at >= NOW() - INTERVAL '{days} days'
        """
        if ticker:
            query += f" AND (title ILIKE '%{ticker}%')"
        query += " ORDER BY published_at DESC"
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if not df.empty:
            # 🚀 QUAN TRỌNG: Ép kiểu số để biểu đồ Charts không bị lỗi
            df['sentiment_score'] = pd.to_numeric(df['sentiment_score'], errors='coerce').fillna(0)
            df['published_at'] = pd.to_datetime(df['published_at'])
            
        return df
    except Exception as e:
        st.error(f"❌ Lỗi truy vấn News: {e}")
        return pd.DataFrame()

# --- 3. HÀM LOAD DỮ LIỆU TƯƠNG QUAN (ĐÃ FIX LỖI CASE-SENSITIVE) ---
@st.cache_data(ttl=300)
def load_correlation_data():
    conn = get_db_connection()
    if not conn: return pd.DataFrame()
    try:
        # 🛠 Dùng "date" (thường) và "Close" (ngoặc kép) để khớp với Neon
        query = 'SELECT "date", "ticker", "Close", "sentiment_score" FROM public.market_correlation ORDER BY "date" ASC'
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if not df.empty:
            # Đồng nhất tên cột để các thành phần Dashboard khác chạy đúng
            df.columns = ['Date', 'Ticker', 'Close', 'Sentiment_Score']
            df['Date'] = pd.to_datetime(df['Date'])
            
            # Ép kiểu dữ liệu số cho an toàn khi tính toán
            df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
            df['Sentiment_Score'] = pd.to_numeric(df['Sentiment_Score'], errors='coerce')
            
        return df
    except Exception as e:
        st.error(f"❌ Lỗi truy vấn Correlation: {e}")
        return pd.DataFrame()