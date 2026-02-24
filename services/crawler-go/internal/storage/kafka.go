package storage

import (
	"context"
	"crawler-go/internal/models" // Import models
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

type KafkaStore struct {
	writer *kafka.Writer
	logger *zap.Logger
}

// Hàm khởi tạo giữ nguyên
func NewKafkaStore(brokers string, topic string, logger *zap.Logger) (*KafkaStore, error) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(brokers),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
	// Trả về thêm error (nil) để khớp với cách gọi bên main (nếu cần đồng bộ)
	// Hoặc bạn có thể giữ nguyên và chỉ sửa main. Nhưng để tiện nhất, mình sửa main cho khớp interface.
	return &KafkaStore{
		writer: writer,
		logger: logger,
	}, nil
}

// --- QUAN TRỌNG: SỬA ĐỂ KHỚP INTERFACE ---

// 1. Đổi tên hàm từ SaveArticle -> Save
// 2. Đổi tham số từ models.NewsArticle -> models.Article
func (k *KafkaStore) Save(article models.Article) error {
	// Chuyển struct thành JSON để bắn lên Kafka
	msgBytes, err := json.Marshal(article)
	if err != nil {
		return err
	}

	// Gửi tin nhắn
	return k.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(article.URL), // Dùng URL làm khóa (Key) để đảm bảo thứ tự
			Value: msgBytes,
			Time:  time.Now(),
		},
	)
}

// 3. Hàm Close phải trả về error
func (k *KafkaStore) Close() error {
	return k.writer.Close()
}
