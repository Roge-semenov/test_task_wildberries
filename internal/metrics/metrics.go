package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics содержит все метрики сервиса
type Metrics struct {
	MessagesConsumed prometheus.Counter
	CacheHits        prometheus.Counter
	CacheMisses      prometheus.Counter
	DBErrors         prometheus.Counter
	ValidationErrors prometheus.Counter
	HTTPServerReqs   *prometheus.CounterVec
}

// NewMetrics создает и регистрирует новые метрики
func NewMetrics() *Metrics {
	return &Metrics{
		MessagesConsumed: promauto.NewCounter(prometheus.CounterOpts{
			Name: "service_messages_consumed_total",
			Help: "The total number of messages consumed from Kafka.",
		}),
		CacheHits: promauto.NewCounter(prometheus.CounterOpts{
			Name: "service_cache_hits_total",
			Help: "The total number of cache hits.",
		}),
		CacheMisses: promauto.NewCounter(prometheus.CounterOpts{
			Name: "service_cache_misses_total",
			Help: "The total number of cache misses.",
		}),
		DBErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "service_db_errors_total",
			Help: "The total number of database errors.",
		}),
		ValidationErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "service_validation_errors_total",
			Help: "The total number of validation errors on incoming messages.",
		}),
		HTTPServerReqs: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "service_http_requests_total",
			Help: "The total number of HTTP requests.",
		}, []string{"code", "method"}),
	}
}

// Handler возвращает http.Handler для эндпоинта /metrics
func Handler() http.Handler {
	return promhttp.Handler()
}
