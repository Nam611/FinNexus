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
    PHIÊN BẢN UPDATE: Chỉ cập nhật điểm số AI cho các bài báo dựa trên URL.
    Không xóa dữ liệu cũ!
    """
    if df is None or df.empty:
        print("⚠️ Không có dữ liệu để lưu.")
        return

    # Kiểm tra xem có cột sentiment chưa
    if 'sentiment_score' not in df.columns:
        print("❌ Dataframe thiếu cột điểm số AI. Kiểm tra lại block trước.")
        return

    # Cấu hình kết nối
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    
    # Câu lệnh SQL Update
    # Chúng ta dùng URL làm khóa để tìm đúng bài báo cần sửa
    query_template = """
    UPDATE public.news_articles
    SET 
        sentiment_score = %s,
        sentiment_label = %s
    WHERE url = %s;
    """

    print(f"💾 Đang cập nhật kết quả AI cho {len(df)} bài báo...")

    try:
        with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
            count = 0
            # Mở connection và loop qua từng dòng để update
            with loader.conn.cursor() as cursor:
                for index, row in df.iterrows():
                    try:
                        cursor.execute(query_template, (
                            float(row['sentiment_score']),
                            str(row['sentiment_label']),
                            str(row['url'])
                        ))
                        count += 1
                    except Exception as e:
                        print(f"⚠️ Lỗi update dòng {index}: {e}")
                
                # Lưu thay đổi
                loader.conn.commit()
                print(f"✅ THÀNH CÔNG! Đã cập nhật điểm số cho {count} bài báo.")
                
    except Exception as e:
        print(f"❌ Lỗi kết nối DB: {e}")