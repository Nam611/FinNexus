import streamlit as st
import plotly.express as px
import pandas as pd
import altair as alt

# --- HÀM 1: Biểu đồ Phân tích Tin tức (Code cũ của bạn) ---
def show_charts(df: pd.DataFrame):
    if df.empty:
        return

    st.markdown("### 📊 Phân tích Xu hướng & Phân bổ")
    c1, c2 = st.columns([2, 1]) # Cột trái to gấp đôi cột phải

    with c1:
        # Biểu đồ 1: Xu hướng cảm xúc theo thời gian (Line Chart)
        if 'published_at' in df.columns:
            # Gom nhóm theo giờ
            daily_trend = df.set_index('published_at').resample('H')['sentiment_score'].mean().reset_index()
            
            fig_line = px.line(daily_trend, x='published_at', y='sentiment_score',
                               title="Diễn biến Tâm lý (Theo giờ)",
                               labels={'published_at': 'Thời gian', 'sentiment_score': 'Điểm cảm xúc (-1 đến 1)'},
                               markers=True)
            # Thêm đường kẻ ngang mức 0
            fig_line.add_hline(y=0, line_dash="dash", line_color="gray")
            st.plotly_chart(fig_line, use_container_width=True)

    with c2:
        # Biểu đồ 2: Phân bổ Tích cực/Tiêu cực (Pie Chart)
        if 'sentiment_label' in df.columns:
            sentiment_counts = df['sentiment_label'].value_counts().reset_index()
            sentiment_counts.columns = ['Label', 'Count']
            
            fig_pie = px.pie(sentiment_counts, names='Label', values='Count',
                             title="Tỷ lệ Cảm xúc",
                             color='Label',
                             color_discrete_map={
                                 'POSITIVE': '#00CC96', # Xanh lá
                                 'NEGATIVE': '#EF553B', # Đỏ cam
                                 'NEUTRAL':  '#636EFA'  # Xanh dương nhạt
                             },
                             hole=0.4) # Biểu đồ Donut
            st.plotly_chart(fig_pie, use_container_width=True)

# --- HÀM 2: Biểu đồ Tương quan Giá & Tâm lý (MỚI THÊM) ---
def show_correlation_chart(df_corr: pd.DataFrame):
    """
    Vẽ biểu đồ 2 trục: Cột (Sentiment) và Đường (Giá)
    """
    if df_corr.empty:
        # Không hiển thị gì nếu chưa có dữ liệu tương quan
        return

    st.markdown("### 🔗 Tương Quan: Giá Cổ Phiếu vs Tâm Lý")
    
    # Tạo biểu đồ cơ sở bằng Altair
    base = alt.Chart(df_corr).encode(
        x=alt.X('Date:T', axis=alt.Axis(title='Thời gian', format='%d/%m'))
    )

    # 1. Trục trái: Biểu đồ cột (Sentiment) - Màu Cam
    bar_sentiment = base.mark_bar(opacity=0.5, width=20).encode(
        y=alt.Y('Sentiment_Score', axis=alt.Axis(title='Chỉ số Tâm lý (AI)', titleColor='#ff7f0e')),
        color=alt.value('#ff7f0e'),
        tooltip=['Date', 'Sentiment_Score', 'News_Count']
    )

    # 2. Trục phải: Biểu đồ đường (Stock Price) - Màu Xanh
    line_price = base.mark_line(strokeWidth=3, point=True).encode(
        y=alt.Y('Stock_Price', axis=alt.Axis(title='Giá Cổ Phiếu (VNĐ)', titleColor='#1f77b4')),
        color=alt.value('#1f77b4'),
        tooltip=['Date', 'Stock_Price']
    )

    # Ghép 2 biểu đồ (Layer) và cho phép 2 trục Y riêng biệt (independent)
    chart = alt.layer(bar_sentiment, line_price).resolve_scale(
        y='independent'
    ).properties(
        height=400 # Chiều cao biểu đồ
    )

    st.altair_chart(chart, use_container_width=True)
    
    # Expander để xem số liệu thô (Optional)
    with st.expander("🔍 Xem dữ liệu chi tiết bảng Tương Quan"):
        st.dataframe(df_corr, use_container_width=True)