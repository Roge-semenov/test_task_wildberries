package app

//импорт пакетов и библиотек
import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"test_task_wb/internal/broker"
	"test_task_wb/internal/cache"
	"test_task_wb/internal/config"
	"test_task_wb/internal/metrics"
	"test_task_wb/internal/server"
	"test_task_wb/internal/storage"
	"time"

	"github.com/go-playground/validator/v10"
)

// основная структура нашего приложения, которая содержит все зависимости
type App struct {
	cfg           *config.Config
	db            *storage.Storage
	cache         cache.OrderCache
	consumer      *broker.MessageConsumer
	httpServer    *http.Server
	metricsServer *http.Server
	mainCtx       context.Context
	mainCancel    context.CancelFunc
}

// Создание и инициализация нового экземпляра App
func New(ctx context.Context, cfg *config.Config) (*App, error) {
	// 1. подключение к базе данных
	dbPool, err := storage.NewDB(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}
	dbStorage := storage.NewStorage(dbPool)

	// 2. инициализация кэша
	shardCapacity := cfg.CacheCapacity / cfg.CacheNumShards
	if shardCapacity < 1 {
		shardCapacity = 1
	}
	orderCache := cache.NewShardedCache(shardCapacity, cfg.CacheNumShards)
	slog.Info("Sharded cache initialized",
		"total_capacity_approx", cfg.CacheCapacity,
		"shards", cfg.CacheNumShards,
		"capacity_per_shard", shardCapacity,
	)

	// 3. загрузка актуальных данных из БД в кэш
	slog.Info("Restoring cache from DB...", "limit", cfg.CacheCapacity)
	restoredOrders, err := dbStorage.GetAllOrders(ctx, cfg.CacheCapacity)
	if err != nil {
		slog.Error("Failed to restore cache from DB, continuing with empty cache", "error", err)
	} else {
		for _, order := range restoredOrders {
			orderCache.Set(order.OrderUID, order)
		}
		slog.Info("Cache restored successfully", "items_loaded", len(restoredOrders))
	}

	// 4. инициализация остальных компонентов
	appMetrics := metrics.NewMetrics()
	validate := validator.New()
	consumer := broker.NewMessageConsumer(
		cfg.KafkaBrokers,
		dbStorage,
		orderCache,
		appMetrics,
		validate,
	)

	// 5. Настройка HTTP сервера
	mainServer := server.NewServer(orderCache, appMetrics, dbStorage)
	fs := http.FileServer(http.Dir("./web"))
	mainServer.Router.Handle("/*", fs)
	srv := &http.Server{
		Addr:    cfg.HTTPPort,
		Handler: mainServer.Router,
	}

	// 6. Настраиваем сервер метрик
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler())
	metricsSrv := &http.Server{
		Addr:    cfg.MetricsPort,
		Handler: metricsMux,
	}

	// 7. Создаем основной контекст приложения
	mainCtx, mainCancel := context.WithCancel(context.Background())

	return &App{
		cfg:           cfg,
		db:            dbStorage,
		cache:         orderCache,
		consumer:      consumer,
		httpServer:    srv,
		metricsServer: metricsSrv,
		mainCtx:       mainCtx,
		mainCancel:    mainCancel,
	}, nil
}

// Запуск все долгоживущих процессов(серверы, консьюмеры)
func (a *App) Run() {

	go a.startMetricsServer()
	go a.startHTTPServer()
	go a.startKafkaConsumer()

	slog.Info("Service is running. Press Ctrl+C to exit.")

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	select {
	case <-shutdownChan:
		slog.Info("Received shutdown signal.")
	case <-a.mainCtx.Done():
		slog.Warn("Shutting down due to critical error (context cancelled).")
	}

	slog.Info("Shutting down gracefully...")
	a.Shutdown()
}

// startMetricsServer запускает HTTP-сервер для эндпоинта /metrics
func (a *App) startMetricsServer() {
	slog.Info("Starting metrics server", "address", a.cfg.MetricsPort)
	if err := a.metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("Failed to start metrics server", "error", err)
		a.mainCancel()
	}
}

// startHTTPServer запускает основной HTTP-сервер приложения
func (a *App) startHTTPServer() {
	slog.Info("Starting HTTP server", "address", a.cfg.HTTPPort)
	if err := a.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("Failed to start HTTP server", "error", err)
		a.mainCancel()
	}
}

// startKafkaConsumer запускает главный цикл консьюмера
func (a *App) startKafkaConsumer() {
	slog.Info("Starting Kafka consumer loop...")
	a.consumer.StartConsuming(a.mainCtx, a.mainCancel)
	slog.Info("Kafka consumer loop stopped.")
}

// Shutdown останавливает все компоненты приложения
func (a *App) Shutdown() {
	a.mainCancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(4)

	go func() {
		defer wg.Done()
		if err := a.httpServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("HTTP server shutdown error", "error", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := a.metricsServer.Shutdown(shutdownCtx); err != nil {
			slog.Error("Metrics server shutdown error", "error", err)
		}
	}()

	go func() {
		defer wg.Done()
		a.consumer.Close()
	}()

	go func() {
		defer wg.Done()
		a.db.Close()
	}()

	wg.Wait()
}
