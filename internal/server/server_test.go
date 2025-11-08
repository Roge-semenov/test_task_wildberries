package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"test_task_wb/internal/cache"
	"test_task_wb/internal/metrics"
	"test_task_wb/internal/model"
	"test_task_wb/internal/storage"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestServer_handleGetOrder(t *testing.T) {
	orderCache := cache.NewLRUCache(10)
	appMetrics := metrics.NewMetrics()

	const testDSN = "postgres://test_user:test_password@localhost:5433/test_db?sslmode=disable"

	log.SetFlags(0)

	dbPool, err := storage.NewDB(context.Background(), testDSN)
	require.NoError(t, err, "Не удалось подключиться к тестовой БД 'postgres-test' на порту 5433")

	dbStorage := storage.NewStorage(dbPool)
	server := NewServer(orderCache, appMetrics, dbStorage)

	testOrder := model.Order{OrderUID: "order123", TrackNumber: "some_track"}
	orderCache.Set(testOrder.OrderUID, testOrder)

	t.Run("Order Found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/order/order123", nil)
		rr := httptest.NewRecorder()

		server.Router.ServeHTTP(rr, req)

		require.Equal(t, http.StatusOK, rr.Code, "Код ответа должен быть 200 OK")

		var returnedOrder model.Order
		err := json.NewDecoder(rr.Body).Decode(&returnedOrder)
		require.NoError(t, err, "Тело ответа должно быть валидным JSON")
		require.Equal(t, testOrder.OrderUID, returnedOrder.OrderUID, "UID заказа должен совпадать")
	})

	t.Run("Order Not Found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/order/non_existent_order", nil)
		rr := httptest.NewRecorder()

		server.Router.ServeHTTP(rr, req)
		require.Equal(t, http.StatusNotFound, rr.Code, "Код ответа должен быть 404 Not Found")
	})
}
