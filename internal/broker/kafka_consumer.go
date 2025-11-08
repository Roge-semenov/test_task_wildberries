package broker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"test_task_wb/internal/cache"
	"test_task_wb/internal/metrics"
	"test_task_wb/internal/model"
	"test_task_wb/internal/storage"

	"github.com/go-playground/validator/v10"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/segmentio/kafka-go"
)

// MessageConsumer содержит зависимости для обработки сообщений
type MessageConsumer struct {
	Reader    *kafka.Reader
	db        *storage.Storage
	cache     cache.OrderCache
	metrics   *metrics.Metrics
	validator *validator.Validate
}

// NewMessageConsumer создает новый экземпляр консьюмера со всеми зависимостями
func NewMessageConsumer(
	brokers []string,
	db *storage.Storage,
	cache cache.OrderCache,
	metrics *metrics.Metrics,
	validator *validator.Validate,
) *MessageConsumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          "orders",
		GroupID:        "order-service-group",
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		CommitInterval: 0,
	})

	return &MessageConsumer{
		Reader:    r,
		db:        db,
		cache:     cache,
		metrics:   metrics,
		validator: validator,
	}
}

// StartConsuming запускает главный цикл чтения и обработки сообщений из Kafka.
// onCriticalError - это колбэк, который вызывается при неустранимой ошибке,
// чтобы инициировать остановку всего сервиса.
func (mc *MessageConsumer) StartConsuming(ctx context.Context, onCriticalError context.CancelFunc) {
	slog.Info("Kafka consumer connected and started consuming messages")
	for {
		msg, err := mc.Reader.ReadMessage(ctx) //ожидаем сообщения из kafka
		if err != nil {
			if errors.Is(err, context.Canceled) {
				slog.Info("Kafka consumer context cancelled, stopping...")
				break
			}
			slog.Error("Error while receiving message from Kafka", "error", err)
			continue
		}

		mc.metrics.MessagesConsumed.Inc()

		//логируем некорректные сообщения и коммитим в kafka что получили сообщение
		var order model.Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			slog.Warn("Failed to unmarshal message. Message ignored.", "error", err)
			if err := mc.Reader.CommitMessages(ctx, msg); err != nil {
				slog.Error("Failed to commit unmarshallable kafka message", "error", err)
			}
			continue
		}

		if err = mc.validator.Struct(order); err != nil {
			mc.metrics.ValidationErrors.Inc()
			slog.Warn("Invalid data received. Message ignored.", "error", err.Error(), "order_uid", order.OrderUID)
			// Сообщение невалидно, коммитим его, чтобы не обрабатывать повторно
			if err := mc.Reader.CommitMessages(ctx, msg); err != nil {
				slog.Error("Failed to commit invalid kafka message", "error", err)
			}
			continue
		}

		if err = mc.db.SaveOrder(ctx, order); err != nil {
			var pgErr *pgconn.PgError
			// Проверяем, является ли ошибка ошибкой PostgreSQL с кодом "unique_violation" (23505)
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				// Это дубликат, логируем как Warn
				slog.Warn("Duplicate order received. Message ignored.", "order_uid", order.OrderUID)
				mc.metrics.ValidationErrors.Inc()

				if err := mc.Reader.CommitMessages(ctx, msg); err != nil {
					slog.Error("Failed to commit duplicate kafka message", "error", err)
					onCriticalError()
					break
				}
				continue
			}

			mc.metrics.DBErrors.Inc()
			slog.Error("CRITICAL: Failed to save order to DB. Shutting down to prevent message loss.", "order_uid", order.OrderUID, "error", err)
			onCriticalError()
			break
		}

		mc.cache.Set(order.OrderUID, order)
		slog.Info("Successfully saved and cached order", "order_uid", order.OrderUID)

		if err := mc.Reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("CRITICAL: Failed to commit kafka message after processing. Shutting down.", "error", err)
			onCriticalError()
			break
		}
	}
}

// Close закрывает соединение с Kafka
func (mc *MessageConsumer) Close() {
	slog.Info("Closing kafka reader...")
	if err := mc.Reader.Close(); err != nil {
		slog.Error("Failed to close kafka reader", "error", err)
	}
}
