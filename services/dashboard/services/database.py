import streamlit as st
import pandas as pd
import psycopg2
import os

# --- 1. CẤU HÌNH KẾT NỐI (SỬ DỤNG SECRETS KHI DEPLOY) ---
def get_db_connection():
    """
    Kết nối tới Neon Cloud. 
    Ưu tiên dùng st.secrets (Streamlit Cloud) -> Sau đó tới Env -> Cuối cùng là Hardcode.
    """
    # Thử lấy từ Streamlit Secrets trước (Dành cho bản Deploy)
    if "postgres" in st.secrets:
        creds = st.secrets["postgres"]
        host = creds.get("host")
        user = creds.get("user")
        password = creds.get("password")
        database = creds.get("database")
    else:
        # Fallback về cấu hình của Nam (Dành cho chạy Local)
        host = os.getenv('DB_HOST', "ep-curly-dust-a1k3kq61-pooler.ap-southeast-1.aws.neon.tech")
        user = os.getenv('DB_USER', "neondb_owner")
        password = os.getenv('DB_PASS', "npg_fby6LhI1giro")
        database = os.getenv('DB_NAME', "neondb")

    try:
        conn = psycopg2.connect(
            dbname=database,
            user=user,
            password=password,
            host=host,
            port="5432",
            sslmode="require" # Bắt buộc cho Neon Cloud
        )
        return conn
    except Exception as e:
        st.error(f"❌ Lỗi kết nối Database: {e}")
        return None

# --- 2. HÀM LOAD DỮ LIỆU TIN TỨC (PHẢI CÓ HÀM NÀY) ---
@st.cache_data(ttl=600)
def load_data(ticker=None, days=7):
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        # Query lấy tin tức và điểm AI
        query = f"""
            SELECT published_at, title, sentiment_label, sentiment_score, source_name, url 
            FROM public.news_articles 
            WHERE published_at >= NOW() - INTERVAL '{days} days'
        """
        # Nếu có chọn mã chứng khoán thì lọc theo mã đó
        if ticker:
            query += f" AND (title ILIKE '%{ticker}%')"
            
        query += " ORDER BY published_at DESC"
        
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"❌ Lỗi truy vấn News: {e}")
        return pd.DataFrame()

# --- 3. HÀM LOAD DỮ LIỆU TƯƠNG QUAN (PHẢI CÓ HÀM NÀY) ---
@st.cache_data(ttl=300)
def load_correlation_data():
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        # Cột "Date" và "Ticker" phải viết hoa chữ cái đầu theo dữ liệu của Nam
        query = 'SELECT "Date", "Ticker", "Close", "Sentiment_Score" FROM public.market_correlation ORDER BY "Date" ASC'
        df = pd.read_sql(query, conn)
        conn.close()
        
        if not df.empty and 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
        return df
    except Exception as e:
        st.error(f"❌ Lỗi truy vấn Correlation: {e}")
        return pd.DataFrame()