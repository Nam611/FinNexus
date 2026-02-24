FROM mageai/mageai:latest

# LẦN 1: Cài Torch bản CPU (Chỉ lấy từ kho này để đảm bảo nhẹ ~200MB)
RUN pip3 install torch --index-url https://download.pytorch.org/whl/cpu

# LẦN 2: Cài các thư viện còn lại từ kho chung (PyPI)
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt