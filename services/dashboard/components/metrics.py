import streamlit as st
import pandas as pd

def show_metrics(df: pd.DataFrame):
    if df.empty:
        st.warning("Chưa có dữ liệu để tính toán.")
        return

    # 1. Tính toán các chỉ số
    total_news = len(df)
    
    # Đếm số lượng theo nhãn
    pos_news = len(df[df['sentiment_label'] == 'POSITIVE'])
    neg_news = len(df[df['sentiment_label'] == 'NEGATIVE'])
    neu_news = len(df[df['sentiment_label'] == 'NEUTRAL'])
    
    # Chỉ số tâm lý thị trường (Market Sentiment Index)
    # Công thức đơn giản: (Pos - Neg) / Total * 100
    sentiment_index = 0
    if total_news > 0:
        sentiment_index = round(((pos_news - neg_news) / total_news) * 100, 2)

    # 2. Hiển thị ra giao diện (4 cột)
    col1, col2, col3, col4 = st.columns(4)
    
    col1.metric("📰 Tổng tin tức", f"{total_news}", "tin mới nhất")
    
    col2.metric("🟢 Tích cực", f"{pos_news}", 
                f"{round(pos_news/total_news*100, 1)}%", 
                delta_color="normal")
    
    col3.metric("🔴 Tiêu cực", f"{neg_news}", 
                f"-{round(neg_news/total_news*100, 1)}%", 
                delta_color="inverse") # Màu đỏ nếu tăng
    
    # Logic hiển thị chỉ số thị trường
    state = "Trung lập"
    if sentiment_index > 5: state = "Hưng phấn (Bullish) 🐮"
    elif sentiment_index < -5: state = "Sợ hãi (Bearish) 🐻"
    
    col4.metric("📊 Tâm lý thị trường", f"{sentiment_index} điểm", state)