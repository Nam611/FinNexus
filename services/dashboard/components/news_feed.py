import streamlit as st
import pandas as pd

def show_news_feed(df):
    st.markdown("### 📰 Dòng chảy Tin tức (Real-time)")
    
    if df.empty:
        st.info("Hiện chưa có tin tức nào được cập nhật.")
        return

    # 1. Tạo bản sao và dọn dẹp dữ liệu (Chỉ làm 1 lần)
    display_df = df.copy()
    display_df['sentiment_label'] = display_df['sentiment_label'].fillna('NEUTRAL')
    display_df['sentiment_score'] = pd.to_numeric(display_df['sentiment_score'], errors='coerce').fillna(0.0)
    
    # 2. CHỈ CHỌN 5 CỘT CÓ THẬT TRÊN NEON DB (Đã loại bỏ hoàn toàn source_name)
    display_df = display_df[['published_at', 'title', 'sentiment_label', 'sentiment_score', 'url']]
    
    # 3. Đổi tên 5 cột tương ứng sang Tiếng Việt
    display_df.columns = ['Thời gian', 'Tiêu đề', 'Cảm xúc', 'Điểm số', 'Link']

    # 4. Hàm tô màu (Highlight)
    def highlight_sentiment(val):
        color = ''
        if val == 'POSITIVE':
            color = 'background-color: #d4edda; color: #155724' # Nền xanh nhạt, chữ xanh
        elif val == 'NEGATIVE':
            color = 'background-color: #f8d7da; color: #721c24' # Nền đỏ nhạt, chữ đỏ
        return color

    # 5. Hiển thị 1 BẢNG DUY NHẤT với đầy đủ Style và Config
    st.dataframe(
        display_df.style.map(highlight_sentiment, subset=['Cảm xúc']),
        height=500,
        hide_index=True,
        width="stretch", # Đã thay thế use_container_width=True để sửa cảnh báo của Streamlit
        column_config={
            "Link": st.column_config.LinkColumn("Đọc ngay"), 
            "Điểm số": st.column_config.ProgressColumn(
                "Mức độ", format="%.2f", min_value=-1.0, max_value=1.0
            ),
            "Thời gian": st.column_config.DatetimeColumn("Đăng lúc", format="DD/MM HH:mm")
        }
    )