package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"test_task_wb/internal/model"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrOrderNotFound = errors.New("order not found")

// пул соединений с БД
type Storage struct {
	pool *pgxpool.Pool
}

// NewDB создает и возвращает новый пул соединений с базой данных,
// используя технику повторных попыток
func NewDB(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	var pool *pgxpool.Pool
	var lastErr error

	retryCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	b := backoff.NewExponentialBackOff()
	b.MaxInterval = 30 * time.Second

	ticker := backoff.NewTicker(b)
	defer ticker.Stop()

	slog.Info("Connecting to database with retries...")

	for range ticker.C {
		if retryCtx.Err() != nil {
			return nil, fmt.Errorf("retries stopped, context timeout exceeded: %w", lastErr)
		}

		attemptCtx, attemptCancel := context.WithTimeout(retryCtx, 5*time.Second)

		var err error
		pool, err = pgxpool.New(attemptCtx, dsn)
		attemptCancel()

		if err == nil {
			slog.Info("Successfully connected to the database.")
			return pool, nil
		}

		slog.Warn("Failed to connect to database, will retry.", "error", err)
		lastErr = err
	}

	return nil, fmt.Errorf("failed to connect to database after all attempts: %w", lastErr)
}

// Создание нового экземпляра Storage.
func NewStorage(pool *pgxpool.Pool) *Storage {
	return &Storage{pool: pool}
}

// SaveOrder сохраняет заказ в базу данных в рамках одной транзакции.
func (s *Storage) SaveOrder(ctx context.Context, order model.Order) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	orderSQL := `INSERT INTO orders (order_uid, track_number, entry, locale, internal_signature, customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard)
				  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	_, err = tx.Exec(ctx, orderSQL, order.OrderUID, order.TrackNumber, order.Entry, order.Locale, order.InternalSignature, order.CustomerID, order.DeliveryService, order.Shardkey, order.SmID, order.DateCreated, order.OofShard)
	if err != nil {
		return fmt.Errorf("failed to insert order: %w", err)
	}

	deliverySQL := `INSERT INTO deliveries (order_uid, name, phone, zip, city, address, region, email)
					  VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`
	_, err = tx.Exec(ctx, deliverySQL, order.OrderUID, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Zip, order.Delivery.City, order.Delivery.Address, order.Delivery.Region, order.Delivery.Email)
	if err != nil {
		return fmt.Errorf("failed to insert delivery: %w", err)
	}

	paymentSQL := `INSERT INTO payments (order_uid, transaction, request_id, currency, provider, amount, payment_dt, bank, delivery_cost, goods_total, custom_fee)
					   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`
	_, err = tx.Exec(ctx, paymentSQL, order.OrderUID, order.Payment.Transaction, order.Payment.RequestID, order.Payment.Currency, order.Payment.Provider, order.Payment.Amount, order.Payment.PaymentDt, order.Payment.Bank, order.Payment.DeliveryCost, order.Payment.GoodsTotal, order.Payment.CustomFee)
	if err != nil {
		return fmt.Errorf("failed to insert payment: %w", err)
	}

	itemSQL := `INSERT INTO items (order_uid, chrt_id, track_number, price, rid, name, sale, size, total_price, nm_id, brand, status)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`
	for _, item := range order.Items {
		_, err = tx.Exec(ctx, itemSQL, order.OrderUID, item.ChrtID, item.TrackNumber, item.Price, item.Rid, item.Name, item.Sale, item.Size, item.TotalPrice, item.NmID, item.Brand, item.Status)
		if err != nil {
			return fmt.Errorf("failed to insert item with chrt_id %d: %w", item.ChrtID, err)
		}
	}

	return tx.Commit(ctx)
}

// GetAllOrders загружает N заказов из базы данных для восстановления кэша
func (s *Storage) GetAllOrders(ctx context.Context, limit int) ([]model.Order, error) {
	query := `
		WITH recent_orders AS (
			SELECT order_uid, track_number, entry, locale, internal_signature,
				   customer_id, delivery_service, shardkey, sm_id, date_created, oof_shard
			FROM orders
			ORDER BY date_created DESC
			LIMIT $1
		)
		SELECT
			o.*,
			d.name as delivery_name, d.phone, d.zip, d.city, d.address, d.region, d.email,
			p.transaction, p.request_id, p.currency, p.provider, p.amount,
			p.payment_dt, p.bank, p.delivery_cost, p.goods_total, p.custom_fee,
			i.chrt_id, i.track_number as item_track_number, i.price, i.rid, i.name as item_name,
			i.sale, i.size, i.total_price, i.nm_id, i.brand, i.status
		FROM recent_orders AS o
		LEFT JOIN deliveries AS d ON o.order_uid = d.order_uid
		LEFT JOIN payments AS p ON o.order_uid = p.order_uid
		LEFT JOIN items AS i ON o.order_uid = i.order_uid
		ORDER BY o.date_created DESC;`

	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query to get all orders: %w", err)
	}
	defer rows.Close()

	orderMap := make(map[string]*model.Order)

	for rows.Next() {
		var o model.Order
		var d model.Delivery
		var p model.Payment
		var i model.Item

		err := rows.Scan(
			&o.OrderUID, &o.TrackNumber, &o.Entry, &o.Locale, &o.InternalSignature,
			&o.CustomerID, &o.DeliveryService, &o.Shardkey, &o.SmID, &o.DateCreated, &o.OofShard,
			&d.Name, &d.Phone, &d.Zip, &d.City, &d.Address, &d.Region, &d.Email,
			&p.Transaction, &p.RequestID, &p.Currency, &p.Provider, &p.Amount,
			&p.PaymentDt, &p.Bank, &p.DeliveryCost, &p.GoodsTotal, &p.CustomFee,
			&i.ChrtID, &i.TrackNumber, &i.Price, &i.Rid, &i.Name,
			&i.Sale, &i.Size, &i.TotalPrice, &i.NmID, &i.Brand, &i.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("error scanning order row: %w", err)
		}

		if existingOrder, ok := orderMap[o.OrderUID]; !ok {
			o.Delivery = d
			o.Payment = p
			if i.ChrtID > 0 {
				o.Items = []model.Item{i}
			}
			orderMap[o.OrderUID] = &o
		} else {
			if i.ChrtID > 0 {
				existingOrder.Items = append(existingOrder.Items, i)
			}
		}
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("error after iterating through orders: %w", rows.Err())
	}

	orders := make([]model.Order, 0, len(orderMap))
	for _, order := range orderMap {
		orders = append(orders, *order)
	}

	return orders, nil
}

// GetOrderByUID ищет один заказ по его UID и собирает все связанные с ним товары
func (s *Storage) GetOrderByUID(ctx context.Context, uid string) (model.Order, error) {
	query := `
		SELECT
			o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature,o.customer_id, o.delivery_service, o.shardkey, o.sm_id, o.date_created, o.oof_shard, 
			d.name as delivery_name, d.phone, d.zip, d.city, d.address, d.region, d.email,
			p.transaction, p.request_id, p.currency, p.provider, p.amount,p.payment_dt, p.bank, p.delivery_cost, p.goods_total, p.custom_fee,
			i.chrt_id, i.track_number as item_track_number, i.price, i.rid, i.name as item_name,i.sale, i.size, i.total_price, i.nm_id, i.brand, i.status
		FROM orders AS o
		LEFT JOIN deliveries AS d ON o.order_uid = d.order_uid
		LEFT JOIN payments AS p ON o.order_uid = p.order_uid
		LEFT JOIN items AS i ON o.order_uid = i.order_uid
		WHERE o.order_uid = $1;`

	rows, err := s.pool.Query(ctx, query, uid)
	if err != nil {
		return model.Order{}, fmt.Errorf("failed to query order from DB: %w", err)
	}
	defer rows.Close()

	var order model.Order
	itemsMap := make(map[int]struct{})
	found := false

	for rows.Next() {
		found = true
		var i model.Item

		err := rows.Scan(
			&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale, &order.InternalSignature,
			&order.CustomerID, &order.DeliveryService, &order.Shardkey, &order.SmID, &order.DateCreated, &order.OofShard,
			&order.Delivery.Name, &order.Delivery.Phone, &order.Delivery.Zip, &order.Delivery.City, &order.Delivery.Address, &order.Delivery.Region, &order.Delivery.Email,
			&order.Payment.Transaction, &order.Payment.RequestID, &order.Payment.Currency, &order.Payment.Provider, &order.Payment.Amount,
			&order.Payment.PaymentDt, &order.Payment.Bank, &order.Payment.DeliveryCost, &order.Payment.GoodsTotal, &order.Payment.CustomFee,
			&i.ChrtID, &i.TrackNumber, &i.Price, &i.Rid, &i.Name,
			&i.Sale, &i.Size, &i.TotalPrice, &i.NmID, &i.Brand, &i.Status,
		)
		if err != nil {
			return model.Order{}, fmt.Errorf("failed to scan order row: %w", err)
		}

		if i.ChrtID > 0 {
			if _, ok := itemsMap[i.ChrtID]; !ok {
				order.Items = append(order.Items, i)
				itemsMap[i.ChrtID] = struct{}{}
			}
		}
	}

	if !found {
		return model.Order{}, ErrOrderNotFound
	}
	if rows.Err() != nil {
		return model.Order{}, fmt.Errorf("error after iterating order rows: %w", rows.Err())
	}

	return order, nil
}

func (s *Storage) Close() {
	s.pool.Close()
}
