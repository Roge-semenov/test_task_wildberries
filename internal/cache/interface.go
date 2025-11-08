package cache

import "test_task_wb/internal/model"

// OrderCache определяет интерфейс для кэша
type OrderCache interface {
	Set(uid string, order model.Order)
	Get(uid string) (model.Order, bool)
}
