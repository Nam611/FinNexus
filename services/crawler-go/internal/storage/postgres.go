package storage

import (
	"crawler-go/internal/config"
	"crawler-go/internal/models"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // Driver Postgres
	"go.uber.org/zap"
)

type PostgresStore struct {
	db     *sql.DB
	logger *zap.Logger
}

// Đây là hàm mà lỗi đang báo thiếu
func NewPostgresStore(cfg *config.Config, logger *zap.Logger) (*PostgresStore, error) {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		cfg.DBHost, cfg.DBPort, cfg.DBUser, cfg.DBPass, cfg.DBName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return &PostgresStore{db: db, logger: logger}, nil
}

// Hàm Save để ghi dữ liệu vào DB (Upsert)
func (s *PostgresStore) Save(article models.Article) error {
	query := `
		INSERT INTO news_articles (url, title, content, source_name, published_at, collected_at)
		VALUES ($1, $2, $3, $4, $5, NOW())
		ON CONFLICT (url) DO UPDATE 
		SET content = EXCLUDED.content, 
		    title = EXCLUDED.title,
		    collected_at = NOW();
	`
	// Chuyển đổi published_at nếu cần, ở đây giả sử article.PublishedAt là time.Time
	_, err := s.db.Exec(query, article.URL, article.Title, article.Content, article.Source, time.Now())
	return err
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}
