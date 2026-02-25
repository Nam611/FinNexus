import streamlit as st
import plotly.express as px
import pandas as pd
import altair as alt

# --- HÀM 1: Biểu đồ Phân tích Tin tức ---
def show_charts(df: pd.DataFrame):
    if df.empty:
        return

    st.markdown("### 📊 Phân tích Xu hướng & Phân bổ")
    c1, c2 = st.columns([2, 1]) 

    with c1:
        if 'published_at' in df.columns:
            # 🛠 Đổi 'H' thành 'h' để fix cảnh báo FutureWarning của Pandas
            daily_trend = df.set_index('published_at').resample('h')['sentiment_score'].mean().reset_index()
            
            fig_line = px.line(daily_trend, x='published_at', y='sentiment_score',
                               title="Diễn biến Tâm lý (Theo giờ)",
                               labels={'published_at': 'Thời gian', 'sentiment_score': 'Điểm cảm xúc (-1 đến 1)'},
                               markers=True)
            fig_line.add_hline(y=0, line_dash="dash", line_color="gray")
            st.plotly_chart(fig_line, use_container_width=True)

    with c2:
        if 'sentiment_label' in df.columns:
            sentiment_counts = df['sentiment_label'].value_counts().reset_index()
            sentiment_counts.columns = ['Label', 'Count']
            
            fig_pie = px.pie(sentiment_counts, names='Label', values='Count',
                             title="Tỷ lệ Cảm xúc",
                             color='Label',
                             color_discrete_map={
                                 'POSITIVE': '#00CC96', 
                                 'NEGATIVE': '#EF553B', 
                                 'NEUTRAL':  '#636EFA'  
                             },
                             hole=0.4) 
            st.plotly_chart(fig_pie, use_container_width=True)

# --- HÀM 2: Biểu đồ Tương quan Giá & Tâm lý ---
def show_correlation_chart(df_corr: pd.DataFrame):
    if df_corr.empty:
        return

    st.markdown("### 🔗 Tương Quan: Giá Cổ Phiếu vs Tâm Lý")
    
    # 🛠 BẢO VỆ ALTAIR: Xóa các dòng có dữ liệu trống để tránh lỗi ValueError
    df_clean = df_corr.dropna(subset=['Date', 'Close', 'Sentiment_Score']).copy()
    
    if df_clean.empty:
        st.warning("⚠️ Dữ liệu hiện tại chưa đủ để vẽ biểu đồ tương quan.")
        return

    # Tạo biểu đồ cơ sở
    base = alt.Chart(df_clean).encode(
        x=alt.X('Date:T', axis=alt.Axis(title='Thời gian', format='%d/%m %H:%M'))
    )

    # 1. Trục trái: Biểu đồ cột (Sentiment) - Màu Cam
    bar_sentiment = base.mark_bar(opacity=0.5, width=20).encode(
        y=alt.Y('Sentiment_Score:Q', axis=alt.Axis(title='Chỉ số Tâm lý (AI)', titleColor='#ff7f0e')),
        color=alt.value('#ff7f0e'),
        tooltip=['Date:T', 'Ticker:N', 'Sentiment_Score:Q']
    )

    # 2. Trục phải: Biểu đồ đường (Close Price) - Màu Xanh
    # 🛠 Sửa 'Stock_Price' thành 'Close' cho khớp với backend
    line_price = base.mark_line(strokeWidth=3, point=True).encode(
        y=alt.Y('Close:Q', axis=alt.Axis(title='Giá Cổ Phiếu (VNĐ)', titleColor='#1f77b4'), scale=alt.Scale(zero=False)),
        color=alt.value('#1f77b4'),
        tooltip=['Date:T', 'Ticker:N', 'Close:Q']
    )

    chart = alt.layer(bar_sentiment, line_price).resolve_scale(
        y='independent'
    ).properties(
        height=400 
    )

    # 🛠 Đổi use_container_width thành width="stretch" để fix cảnh báo của Streamlit
    st.altair_chart(chart, width="stretch")
    
    with st.expander("🔍 Xem dữ liệu chi tiết bảng Tương Quan"):
        st.dataframe(df_clean, width="stretch")