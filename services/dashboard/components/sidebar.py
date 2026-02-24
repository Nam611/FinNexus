import streamlit as st

def show_sidebar():
    """
    Hiển thị Sidebar và trả về các bộ lọc
    """
    st.sidebar.title("🔍 FinNexus Control")
    st.sidebar.markdown("---")
    
    # Bộ lọc thời gian (Ví dụ)
    days_back = st.sidebar.slider("Dữ liệu trong vòng (ngày):", 1, 30, 7)
    
    # Bộ lọc mã chứng khoán (Hiện tại chưa dùng tới nhưng để sẵn)
    ticker = st.sidebar.text_input("Mã chứng khoán (VD: VCB):", "").upper()
    
    st.sidebar.markdown("---")
    st.sidebar.info("💡 **Mẹo:** Dữ liệu được cập nhật tự động từ hệ thống Crawler.")
    
    return {
        "days_back": days_back,
        "ticker": ticker
    }