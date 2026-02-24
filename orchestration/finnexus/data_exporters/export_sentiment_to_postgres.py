from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(df: DataFrame, **kwargs) -> None:
    """
    PHIÊN BẢN UPDATE: Tự động nhận diện cột url hoặc link để update.
    """
    if df is None or df.empty:
        print("⚠️ Không có dữ liệu để lưu.")
        return

    if 'sentiment_score' not in df.columns:
        print("❌ Dataframe thiếu cột điểm số AI. Kiểm tra lại block trước.")
        return

    # 👇 BỘ LỌC THÔNG MINH: Tìm đúng tên cột chứa đường dẫn
    url_col = 'url' if 'url' in df.columns else 'link' if 'link' in df.columns else None
    
    if not url_col:
        print(f"❌ Lỗi: Dataframe không có cột 'url' hay 'link'. Các cột hiện tại: {list(df.columns)}")
        return

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    # Câu lệnh SQL Update
    query_template = """
    UPDATE public.news_articles
    SET 
        sentiment_score = %s,
        sentiment_label = %s
    WHERE url = %s; 
    """

    print(f"💾 Đang cập nhật kết quả AI cho {len(df)} bài báo (Dùng cột: {url_col})...")

    try:
        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            count = 0
            with loader.conn.cursor() as cursor:
                for index, row in df.iterrows():
                    try:
                        cursor.execute(query_template, (
                            float(row['sentiment_score']),
                            str(row['sentiment_label']),
                            str(row[url_col]) # 👈 Tự động dùng đúng tên cột
                        ))
                        count += 1
                    except Exception as e:
                        print(f"⚠️ Lỗi update dòng {index}: {e}")
                
                loader.conn.commit()
                print(f"✅ THÀNH CÔNG! Đã cập nhật điểm số cho {count} bài báo.")
                
    except Exception as e:
        print(f"❌ Lỗi kết nối DB: {e}")