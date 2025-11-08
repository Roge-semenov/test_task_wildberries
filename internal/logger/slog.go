package logger

import (
	"log/slog"
	"os"
)

// NewSlogLogger создает и настраивает новый JSON логгер
func NewSlogLogger() *slog.Logger {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	logger := slog.New(handler)

	return logger
}
