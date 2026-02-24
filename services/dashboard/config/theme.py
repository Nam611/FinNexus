import streamlit as st

def apply_theme():
    """
    Áp dụng CSS tùy chỉnh cho Dashboard
    """
    st.markdown("""
        <style>
        /* 1. Tùy chỉnh thanh cuộn (Scrollbar) */
        ::-webkit-scrollbar {
            width: 10px;
        }
        ::-webkit-scrollbar-track {
            background: #0e1117;
        }
        ::-webkit-scrollbar-thumb {
            background: #262730; 
            border-radius: 5px;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: #555; 
        }

        /* 2. Tăng chiều rộng container chính */
        .block-container {
            padding-top: 1rem;
            padding-bottom: 1rem;
            max-width: 95rem;
        }

        /* 3. Tùy chỉnh Metrics (Các ô chỉ số) */
        [data-testid="stMetricValue"] {
            font-size: 2rem;
            color: #00CC96; /* Màu xanh mặc định */
        }
        
        /* 4. Tùy chỉnh bảng dữ liệu (Table) */
        [data-testid="stDataFrame"] {
            border: 1px solid #333;
            border-radius: 5px;
        }
        
        /* 5. Ẩn menu mặc định của Streamlit (Hamburger menu) nếu muốn */
        /* #MainMenu {visibility: hidden;} */
        /* footer {visibility: hidden;} */
        
        </style>
    """, unsafe_allow_html=True)