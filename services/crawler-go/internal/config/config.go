package config

import (
	"os"
	"strconv"
)

// SỬA TÊN STRUCT: Từ AppConfig -> Config (để khớp với main.go và engine.go)
type Config struct {
	// Kafka Config
	KafkaBrokers string
	KafkaTopic   string

	// Crawler Config
	TargetDomain  string
	Parallelism   int
	CrawlDelaySec int
	StartURL      string

	// --- DATABASE CONFIG (MỚI) ---
	StorageType string // "KAFKA" hoặc "POSTGRES"
	DBHost      string
	DBPort      string
	DBUser      string
	DBPass      string
	DBName      string
}

func LoadConfig() *Config {
	return &Config{
		// Kafka Defaults
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "financial_news"),

		// Crawler Defaults
		TargetDomain:  getEnv("TARGET_DOMAIN", "cafef.vn"),
		Parallelism:   2,
		CrawlDelaySec: 2,
		StartURL:      getEnv("START_URL", "https://cafef.vn/tai-chinh-ngan-hang.chn"),

		// Database Defaults (QUAN TRỌNG)
		StorageType: getEnv("STORAGE_TYPE", "POSTGRES"), // Mặc định chạy Postgres
		DBHost:      getEnv("DB_HOST", "localhost"),
		DBPort:      getEnv("DB_PORT", "5432"),
		DBUser:      getEnv("DB_USER", "postgres"), // Bạn kiểm tra lại user DB của bạn nhé
		DBPass:      getEnv("DB_PASS", "password"), // Kiểm tra lại password DB
		DBName:      getEnv("DB_NAME", "finnexus"),
	}
}

// Hàm hỗ trợ đọc biến môi trường
func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// Hàm hỗ trợ đọc số nguyên
func getEnvInt(key string, fallback int) int {
	strValue := getEnv(key, "")
	if value, err := strconv.Atoi(strValue); err == nil {
		return value
	}
	return fallback
}
