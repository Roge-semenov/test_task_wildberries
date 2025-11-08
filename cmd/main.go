package main

import (
	"context"
	"log/slog"
	"os"
	"test_task_wb/internal/app"
	"test_task_wb/internal/config"
	"test_task_wb/internal/logger"
)

func main() {
	// 1. Инициализация логгера
	slogLogger := logger.NewSlogLogger()
	slog.SetDefault(slogLogger)

	slog.Info("Starting service...")

	// 2. Загрузка конфигурации
	cfg := config.Load()

	// 3. Создание экземпляра приложения
	application, err := app.New(context.Background(), cfg)
	if err != nil {
		slog.Error("Failed to initialize application", "error", err)
		os.Exit(1)
	}

	// 4. Запуск приложения
	application.Run()
}
