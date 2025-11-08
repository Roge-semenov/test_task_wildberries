package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"test_task_wb/internal/cache"
	"test_task_wb/internal/metrics"
	"test_task_wb/internal/storage"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Server struct {
	Router  *chi.Mux
	Cache   cache.OrderCache
	Metrics *metrics.Metrics
	DB      *storage.Storage
}

// NewServer создает новый экземпляр сервера с зависимостями
func NewServer(c cache.OrderCache, m *metrics.Metrics, db *storage.Storage) *Server {
	s := &Server{
		Router:  chi.NewRouter(),
		Cache:   c,
		Metrics: m,
		DB:      db,
	}
	s.Router.Use(s.metricsMiddleware)
	s.initRoutes()
	return s
}

func (s *Server) initRoutes() {
	s.Router.Get("/order/{orderUID}", s.handleGetOrder())
}

// metricsMiddleware добавляет метрики к ответам
func (s *Server) metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		s.Metrics.HTTPServerReqs.WithLabelValues(strconv.Itoa(ww.Status()), r.Method).Inc()
	})
}

// handleGetOrder возвращает обработчик для получения заказа по UID (сначала ищет в кэше, затем в БД)
func (s *Server) handleGetOrder() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		orderUID := chi.URLParam(r, "orderUID")
		if orderUID == "" {
			http.Error(w, "Order UID is required", http.StatusBadRequest)
			return
		}

		order, found := s.Cache.Get(orderUID)
		if found {
			slog.Debug("Cache hit", "order_uid", orderUID)
			s.Metrics.CacheHits.Inc()

			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			if err := json.NewEncoder(w).Encode(order); err != nil {
				http.Error(w, "Failed to encode response", http.StatusInternalServerError)
			}
			return
		}

		slog.Debug("Cache miss", "order_uid", orderUID)
		s.Metrics.CacheMisses.Inc()

		order, err := s.DB.GetOrderByUID(r.Context(), orderUID)
		if err != nil {
			if errors.Is(err, storage.ErrOrderNotFound) {
				http.Error(w, "Order not found", http.StatusNotFound)
				return
			}

			slog.Error("Failed to get order from DB", "error", err, "order_uid", orderUID)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		s.Cache.Set(order.OrderUID, order)
		slog.Debug("Order retrieved from DB and cached", "order_uid", orderUID)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(order); err != nil {
			http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		}
	}
}
