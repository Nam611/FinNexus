import pandas as pd
import numpy as np
from datetime import datetime, timedelta

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

# Import thư viện (giữ nguyên để sau này mạng ổn thì dùng)
try:
    from vnstock3 import Vnstock
except ImportError:
    try:
        from vnstock import Vnstock
    except:
        pass

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Hàm lấy dữ liệu giá (Có chế độ 'Dữ liệu giả lập' khi mất mạng)
    """
    symbol = 'VCB'
    days = 30
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    print(f"🚀 Đang nạp dữ liệu cho mã {symbol}...")
    
    df = pd.DataFrame()
    
    # --- CÁCH 1: THỬ LẤY DỮ LIỆU THẬT ---
    try:
        stock = Vnstock().stock(symbol=symbol, source='VCI') # Thử nguồn VCI
        df = stock.quote.history(start=start_date.strftime('%Y-%m-%d'), 
                               end=end_date.strftime('%Y-%m-%d'), 
                               interval='1D')
    except Exception as e:
        print(f"⚠️ Kết nối API thất bại: {e}")

    # --- CÁCH 2: NẾU CÁCH 1 THẤT BẠI -> TẠO DỮ LIỆU GIẢ ---
    if df is None or df.empty:
        print("💡 Đang kích hoạt chế độ: DỮ LIỆU GIẢ LẬP (MOCK DATA)...")
        
        # Tạo chuỗi ngày
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        # Tạo giá giả (Random walk)
        np.random.seed(42)
        base_price = 90000
        changes = np.random.normal(0, 1000, size=len(date_range))
        prices = base_price + np.cumsum(changes)
        
        df = pd.DataFrame({
            'time': date_range,
            'open': prices,
            'high': prices + 500,
            'low': prices - 500,
            'close': prices,
            'volume': np.random.randint(100000, 500000, size=len(date_range)),
            'ticker': symbol
        })
        print(f"✅ Đã tạo thành công {len(df)} dòng dữ liệu giả để test!")

    # --- CHUẨN HÓA DỮ LIỆU ĐẦU RA ---
    # Đổi tên cột time -> Date, close -> Close để khớp với code vẽ biểu đồ sau này
    if 'time' in df.columns:
        df['Date'] = pd.to_datetime(df['time'])
    if 'close' in df.columns:
        df['Close'] = df['close']
        
    # Sắp xếp theo ngày tăng dần
    df = df.sort_values('Date')
    
    print("📊 Dữ liệu mẫu:")
    print(df[['Date', 'Close']].head())
    
    return df