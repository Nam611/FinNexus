import pandas as pd
import numpy as np
from datetime import datetime, timedelta

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data_from_api(*args, **kwargs):
    # 👇 NÂNG CẤP 1: Danh sách các mã cổ phiếu "quốc dân"
    symbols = ['VCB', 'HPG', 'SSI', 'FPT', 'VNM']
    days = 30
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Tạo một list trống để chứa dữ liệu của tất cả các mã
    all_data = []
    
    try:
        from vnstock3 import Vnstock
        sources = ['TCBS', 'SSI', 'DNSE', 'VCI']
    except ImportError:
        sources = []

    # 👇 NÂNG CẤP 2: Vòng lặp lấy giá cho từng mã
    for symbol in symbols:
        print(f"\n🚀 Đang xử lý mã: {symbol}...")
        df_symbol = pd.DataFrame()
        success = False
        
        # Thử lấy dữ liệu thật
        for src in sources:
            try:
                stock = Vnstock().stock(symbol=symbol, source=src)
                df_temp = stock.quote.history(
                    start=start_date.strftime('%Y-%m-%d'), 
                    end=end_date.strftime('%Y-%m-%d'), 
                    interval='1D'
                )
                if df_temp is not None and not df_temp.empty:
                    df_symbol = df_temp
                    success = True
                    print(f"✅ Lấy Real Data thành công từ {src}")
                    break
            except Exception:
                pass
        
        # Nếu bị chặn, dùng Smart Mock Data riêng biệt cho từng mã
        if not success:
            print(f"💡 Dùng Smart Mock Data cho {symbol}...")
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # Đổi mốc giá dựa trên độ dài tên để mỗi mã có một đường giá khác nhau
            np.random.seed(len(symbol) + sum(ord(c) for c in symbol)) 
            base_price = np.random.randint(20000, 90000)
            changes = np.random.normal(0, 800, size=len(date_range))
            prices = base_price + np.cumsum(changes)
            
            df_symbol = pd.DataFrame({
                'time': date_range,
                'close': prices
            })
        
        # Chuẩn hóa cột
        if 'time' in df_symbol.columns:
            df_symbol['Date'] = pd.to_datetime(df_symbol['time'])
        if 'close' in df_symbol.columns:
            df_symbol['Close'] = df_symbol['close']
        
        if 'Date' in df_symbol.columns and 'Close' in df_symbol.columns:
            df_symbol = df_symbol[['Date', 'Close']].sort_values('Date')
            
            # 👇 NÂNG CẤP 3: Gắn thẻ tên mã vào từng dòng dữ liệu
            df_symbol['ticker'] = symbol 
            all_data.append(df_symbol)
    
    # 👇 NÂNG CẤP 4: Gộp 5 bảng nhỏ thành 1 bảng lớn duy nhất
    final_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    print("\n" + "="*40)
    print(f"📊 ĐÃ LẤY XONG DỮ LIỆU {len(symbols)} MÃ CỔ PHIẾU!")
    print(final_df.sample(5)) # In ngẫu nhiên 5 dòng để kiểm tra
    print("="*40)
    
    return final_df