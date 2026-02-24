# 1. Dùng Python 3.12-slim để đồng bộ với máy local của bạn
FROM python:3.12-slim

WORKDIR /app

# 2. Cài đặt thư viện hệ thống
# Thêm 'curl' vào để phục vụ Healthcheck bên dưới (bản slim mặc định không có)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 3. Copy requirements và cài đặt (Tận dụng Cache của Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copy source code
COPY . .

# 5. Mở cổng
EXPOSE 8501

# 6. Kiểm tra sức khỏe ứng dụng (Dùng curl đã cài ở trên)
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# 7. Chạy ứng dụng
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]