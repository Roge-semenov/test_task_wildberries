package cache

import (
	"test_task_wb/internal/model"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestLRUCache проверяет основную логику LRU кэша
func TestLRUCache(t *testing.T) {
	order1 := model.Order{OrderUID: "order1", DateCreated: time.Now()}
	order2 := model.Order{OrderUID: "order2", DateCreated: time.Now()}
	order3 := model.Order{OrderUID: "order3", DateCreated: time.Now()}
	t.Run("Set and Get", func(t *testing.T) {
		cache := NewLRUCache(2)
		cache.Set(order1.OrderUID, order1)

		retrievedOrder, found := cache.Get(order1.OrderUID)
		require.True(t, found, "Элемент должен быть найден в кэше")
		require.Equal(t, order1.OrderUID, retrievedOrder.OrderUID, "UID полученного заказа должен совпадать")
	})

	t.Run("Eviction policy", func(t *testing.T) {
		cache := NewLRUCache(2)

		cache.Set(order1.OrderUID, order1)
		cache.Set(order2.OrderUID, order2)

		cache.Set(order3.OrderUID, order3)

		_, found := cache.Get(order1.OrderUID)
		require.False(t, found, "Самый старый элемент (order1) должен был быть вытеснен")

		_, found = cache.Get(order2.OrderUID)
		require.True(t, found, "Элемент order2 должен остаться в кэше")
		_, found = cache.Get(order3.OrderUID)
		require.True(t, found, "Новый элемент order3 должен быть в кэше")
	})

	t.Run("Get updates recentness", func(t *testing.T) {
		cache := NewLRUCache(2)

		cache.Set(order1.OrderUID, order1)
		cache.Set(order2.OrderUID, order2)

		cache.Get(order1.OrderUID)

		cache.Set(order3.OrderUID, order3)

		_, found := cache.Get(order2.OrderUID)
		require.False(t, found, "Элемент order2 должен был быть вытеснен")

		_, found = cache.Get(order1.OrderUID)
		require.True(t, found, "Элемент order1 (к которому недавно обращались) должен остаться")
		_, found = cache.Get(order3.OrderUID)
		require.True(t, found, "Новый элемент order3 должен быть в кэше")
	})
}
