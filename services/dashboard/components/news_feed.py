import streamlit as st
import pandas as pd

def show_news_feed(df):
    st.markdown("### 📰 Dòng chảy Tin tức (Real-time)")
    
    # Tạo bản sao để không làm hỏng dữ liệu gốc
    display_df = df.copy()
    
    # 🛠 FIX TẠI ĐÂY: Ép kiểu và xử lý dữ liệu trống
    # Neon lưu là sentiment_label và sentiment_score
    display_df['sentiment_label'] = display_df['sentiment_label'].fillna('NEUTRAL')
    display_df['sentiment_score'] = pd.to_numeric(display_df['sentiment_score'], errors='coerce').fillna(0.0)
    
    # Chọn và đổi tên cột để hiển thị lên Dashboard
    # Đảm bảo các cột này có tồn tại trong bảng news_articles trên Neon
    output_df = display_df[['published_at', 'title', 'sentiment_label', 'sentiment_score', 'url']]
    output_df.columns = ['Đăng lúc', 'Tiêu đề', 'Cảm xúc', 'Mức độ', 'Đọc ngay']
    
    st.dataframe(
        output_df,
        column_config={
            "Đọc ngay": st.column_config.LinkColumn("Đọc ngay"),
            "Mức độ": st.column_config.NumberColumn(format="%.2f")
        },
        use_container_width=True,
        hide_index=True
    )

    # 1. Tạo hàm tô màu (Highlight)
    def highlight_sentiment(val):
        color = ''
        if val == 'POSITIVE':
            color = 'background-color: #d4edda; color: #155724' # Nền xanh nhạt, chữ xanh đậm
        elif val == 'NEGATIVE':
            color = 'background-color: #f8d7da; color: #721c24' # Nền đỏ nhạt, chữ đỏ đậm
        return color

    # 2. Chọn cột cần hiển thị
    display_df = df[['published_at', 'title', 'sentiment_label', 'sentiment_score', 'source_name', 'url']].copy()
    
    # 3. Đổi tên cột cho đẹp
    display_df.columns = ['Thời gian', 'Tiêu đề', 'Cảm xúc', 'Điểm số', 'Nguồn', 'Link']

    # 4. Hiển thị bảng với Style
    # Dùng style.map (hoặc applymap) để tô màu cột 'Cảm xúc'
    st.dataframe(
        display_df.style.map(highlight_sentiment, subset=['Cảm xúc']),
        use_container_width=True,
        height=500,
        column_config={
            "Link": st.column_config.LinkColumn("Đọc ngay"), # Biến URL thành nút bấm
            "Điểm số": st.column_config.ProgressColumn(
                "Mức độ", format="%.2f", min_value=-1, max_value=1
            ),
            "Thời gian": st.column_config.DatetimeColumn("Đăng lúc", format="DD/MM HH:mm")
        }
    )