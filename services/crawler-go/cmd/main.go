package main

import (
	"context"
	"crawler-go/internal/config"
	"crawler-go/internal/crawler"
	"crawler-go/internal/storage"
	"os"
	"time"

	"go.uber.org/zap"
)

// JOB_TIMEOUT: Giới hạn thời gian chạy tối đa cho 1 lần cào (Batch).
// Nếu quá 10 phút mà chưa xong, chương trình sẽ tự hủy để tránh treo hệ thống.
const JOB_TIMEOUT = 10 * time.Minute

func main() {
	// 1. Init Logger (Dùng Production để log ra JSON chuẩn)
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// 2. Load Config
	cfg := config.LoadConfig()
	logger.Info("⚙️  Cấu hình đã tải",
		zap.String("storage_mode", cfg.StorageType),
		zap.String("target", "CafeF"),
	)

	// 3. Setup Context (Tạo bộ đếm ngược thời gian)
	ctx, cancel := context.WithTimeout(context.Background(), JOB_TIMEOUT)
	defer cancel()

	// 4. Init Storage (KIẾN TRÚC SENIOR: Switch Implementation)
	// Khai báo biến Interface để chứa KafkaStore hoặc PostgresStore
	var store storage.Storage
	var err error

	switch cfg.StorageType {
	case "KAFKA":
		// Nếu bạn muốn dùng lại Kafka, hãy đảm bảo KafkaStore đã implement đúng Interface
		// store = storage.NewKafkaStore(cfg.KafkaBrokers, cfg.KafkaTopic, logger)
		logger.Fatal("❌ Chế độ Kafka đang tạm tắt để tối ưu RAM cho Phase 1")

	case "POSTGRES":
		// Khởi tạo kết nối Postgres
		store, err = storage.NewPostgresStore(cfg, logger)
		if err != nil {
			logger.Fatal("❌ Không thể kết nối Postgres", zap.Error(err))
		}
		logger.Info("✅ Đã kết nối Postgres thành công (Direct Mode)")

	default:
		// Fallback an toàn
		logger.Fatal("❌ Loại Storage không hợp lệ (Kiểm tra file .env hoặc Config)", zap.String("type", cfg.StorageType))
	}

	// Đảm bảo đóng kết nối khi hàm main kết thúc
	defer store.Close()

	// 5. Init Engine
	// Lúc này Engine nhận vào Interface 'storage.Storage', nên truyền cái gì vào cũng được
	engine := crawler.NewEngine(cfg, store, logger)

	logger.Info("🚀 Bắt đầu Job Crawler (Batch Mode)...")

	// 6. Start với Context
	// Hàm Start sẽ trả về lỗi nếu Timeout hoặc Cào thất bại
	err = engine.Start(ctx)

	if err != nil {
		logger.Error("⚠️ Job thất bại hoặc bị Timeout", zap.Error(err))
		// Trả về Exit Code 1 để Mage/Docker biết là chạy lỗi -> Nó sẽ tự Retry hoặc báo động
		os.Exit(1)
	}

	logger.Info("✅ Job hoàn thành xuất sắc! Dữ liệu đã vào DB.")
	// Trả về Exit Code 0 (Thành công)
	os.Exit(0)
}
