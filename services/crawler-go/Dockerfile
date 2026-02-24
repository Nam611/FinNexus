# ----------------------------
# GIAI ĐOẠN 1: BUILDER (Thợ xây)
# ----------------------------
FROM golang:1.24-alpine AS builder

# Cài đặt git (để tải thư viện) và tzdata (để lấy dữ liệu múi giờ)
RUN apk add --no-cache git tzdata

WORKDIR /app

# Copy file dependency trước để tận dụng cache của Docker
COPY go.mod go.sum ./
RUN go mod download

# Copy toàn bộ source code
COPY . .

# Build binary: Tắt CGO để chạy được trên Alpine, cắt debug info (-w -s)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o crawler cmd/main.go

# ----------------------------
# GIAI ĐOẠN 2: RUNNER (Người vận chuyển)
# ----------------------------
FROM alpine:latest

WORKDIR /app

# Cài chứng chỉ SSL (để gọi HTTPS) và Timezone (để chỉnh giờ VN)
RUN apk add --no-cache ca-certificates tzdata

# Thiết lập múi giờ Việt Nam (Rất tiện khi xem log chạy tự động)
ENV TZ=Asia/Ho_Chi_Minh

# Copy file thực thi từ Builder sang
COPY --from=builder /app/crawler .

# LƯU Ý: Không copy .env nữa. Chúng ta sẽ inject biến môi trường từ bên ngoài (Docker Compose/Mage)
# COPY --from=builder /app/.env . 

# Chạy chương trình
CMD ["./crawler"]