package storage

import (
	// Dòng này cực quan trọng: Phải đúng tên module trong go.mod
	"crawler-go/internal/models"
)

type Storage interface {
	Save(article models.Article) error
	Close() error
}
