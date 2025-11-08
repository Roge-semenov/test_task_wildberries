package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config хранит все основные настройки приложения
type Config struct {
	DatabaseURL    string
	CacheCapacity  int
	CacheNumShards int
	HTTPPort       string
	MetricsPort    string
	KafkaBrokers   []string
}

// Load читает конфигурацию из .env файла
// и предоставляет значения по умолчанию
func Load() *Config {
	if err := godotenv.Load(); err != nil {
		slog.Warn("No .env file found, using environment variables")
	}

	dbUser := os.Getenv("POSTGRES_USER")
	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	dbHost := os.Getenv("POSTGRES_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}
	dbPort := os.Getenv("POSTGRES_PORT")
	if dbPort == "" {
		dbPort = "5432"
	}
	dbName := os.Getenv("POSTGRES_DB")

	cacheCapacity := getEnvAsInt("CACHE_CAPACITY", 128)
	cacheNumShards := getEnvAsInt("CACHE_NUM_SHARDS", 64)

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8081"
	}

	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9090"
	}

	kafkaBroker := os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "localhost:9092"
	}

	return &Config{
		DatabaseURL: fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			dbUser, dbPassword, dbHost, dbPort, dbName),
		CacheCapacity:  cacheCapacity,
		CacheNumShards: cacheNumShards,
		HTTPPort:       ":" + httpPort,
		MetricsPort:    ":" + metricsPort,
		KafkaBrokers:   []string{kafkaBroker},
	}
}

func getEnvAsInt(key string, fallback int) int {
	valueStr, exists := os.LookupEnv(key)
	if !exists {
		return fallback
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		slog.Warn(fmt.Sprintf("Invalid value for env var '%s', using default value.", key))
		return fallback
	}

	return value
}
