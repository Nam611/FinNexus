import docker
import time

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data(*args, **kwargs):
    try:
        client = docker.from_env()
    except Exception as e:
        raise Exception("❌ Lỗi kết nối Docker Socket. Kiểm tra volume /var/run/docker.sock")

    print("🚀 Đang khởi tạo Container Crawler (Debug Mode)...")

    # 1. Dọn dẹp container cũ nếu bị kẹt
    try:
        old_container = client.containers.get("daily_crawler_job_auto")
        old_container.remove(force=True)
        print("🧹 Đã dọn dẹp container cũ bị kẹt.")
    except docker.errors.NotFound:
        pass

    # 2. Chạy Container ở chế độ Detach (Chạy ngầm để kiểm soát)
    try:
        container = client.containers.run(
            image="fn_crawler",
            command=[],
            network="finnexus_finnexus_net",  # Đảm bảo đúng tên mạng
            environment={
                # 👇 TRẢ VỀ CHẠY LOCAL ĐỂ KHÔNG BỊ LỖI SSL
                "DB_HOST": "fn_postgres",     
                "DB_PORT": "5432",
                "DB_USER": "admin",
                "DB_PASS": "admin",
                "DB_NAME": "finnexus",
                "STORAGE_TYPE": "POSTGRES"
            },
            detach=True,                      # Chạy ngầm để Python kiểm soát
            name="daily_crawler_job_auto"
        )
        
        # 3. Chờ container chạy xong và Stream log ra màn hình
        print("⏳ Container đang chạy...")
        output_log = ""
        for line in container.logs(stream=True):
            decoded_line = line.decode('utf-8').strip()
            print(decoded_line) # In ngay lập tức ra màn hình Mage
            output_log += decoded_line + "\n"
        
        # 4. Kiểm tra kết quả cuối cùng (Exit Code)
        result = container.wait()
        exit_code = result.get('StatusCode', 1)
        
        # 5. Dọn dẹp sau khi xong
        container.remove()

        if exit_code == 0:
            print("\n✅ CRAWLER THÀNH CÔNG RỰC RỠ!")
            return "Success"
        else:
            print(f"\n❌ CRAWLER THẤT BẠI (Exit Code: {exit_code})")
            raise Exception(f"Job Failed. Logs:\n{output_log}")

    except docker.errors.ImageNotFound:
        raise Exception("❌ Không tìm thấy Image 'fn_crawler'. Hãy chạy 'docker-compose build fn_crawler' lại.")
    except Exception as e:
        raise Exception(f"❌ Lỗi Python: {str(e)}")