import streamlit as st
from config.theme import apply_theme
# Import các hàm load dữ liệu
from services.database import load_data, load_correlation_data 
from components.sidebar import show_sidebar
from components.metrics import show_metrics
# Import các hàm vẽ biểu đồ
from components.charts import show_charts, show_correlation_chart 
from components.news_feed import show_news_feed

# 1. Cấu hình trang
st.set_page_config(
    page_title="FinNexus AI Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply Theme (nếu có)
try:
    apply_theme()
except:
    pass # Bỏ qua nếu chưa config theme

# 2. Sidebar (Bộ lọc)
filters = show_sidebar()

# 3. Load dữ liệu
# a) Dữ liệu phân tích tin tức (theo ticker lọc)
with st.spinner('Đang tải dữ liệu tin tức...'):
    df_news = load_data(
        ticker=filters['ticker'], 
        days=filters['days_back']
    )

# b) Dữ liệu tương quan (Lấy toàn cục từ bảng market_correlation)
with st.spinner('Đang tải dữ liệu thị trường...'):
    df_corr_raw = load_correlation_data() 
    
    # 👇 THÊM BỘ LỌC MULTI-TICKER TẠI ĐÂY
    if not df_corr_raw.empty and filters.get('ticker'):
        # Lọc bảng tương quan chỉ giữ lại mã cổ phiếu đang được chọn ở Sidebar
        # (Lưu ý: Chữ 'Ticker' viết hoa chữ T theo đúng cấu trúc ở Mage AI)
        df_corr = df_corr_raw[df_corr_raw['Ticker'] == filters['ticker']]
    else:
        df_corr = df_corr_raw

# 4. Tiêu đề
st.title("📈 FinNexus: AI Financial Intelligence")

if filters.get('ticker'):
    st.markdown(f"### 🎯 Phân tích chuyên sâu mã: **{filters['ticker']}**")
else:
    st.markdown("### 🌏 Hệ thống phân tích tâm lý thị trường toàn cảnh (Real-time)")

st.markdown("---")

# 5. LOGIC HIỂN THỊ

# A. Hiển thị Metrics & Tin tức (Nếu có dữ liệu tin)
if not df_news.empty:
    show_metrics(df_news)
    st.markdown("---")
elif filters.get('ticker'):
    st.warning(f"⚠️ Không tìm thấy tin tức nào cho mã {filters['ticker']} trong {filters['days_back']} ngày qua.")

# B. 🔥 HIỂN THỊ BIỂU ĐỒ TƯƠNG QUAN
# Truyền df_corr (đã được lọc) vào hàm vẽ biểu đồ
if not df_corr.empty:
    show_correlation_chart(df_corr)
    st.markdown("---")
else:
    pass

# C. Các biểu đồ phân tích chi tiết & Bảng tin (Dựa trên News)
if not df_news.empty:
    show_charts(df_news)
    st.markdown("---")
    show_news_feed(df_news)

# D. Cảnh báo chung nếu TRẮNG TRƠN cả 2 nguồn
if df_news.empty and df_corr.empty:
    st.error("⚠️ Hệ thống chưa có dữ liệu. Vui lòng kiểm tra lại Pipeline trong Mage AI hoặc kết nối Database.")