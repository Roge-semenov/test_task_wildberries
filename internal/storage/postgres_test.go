package storage

import (
	"context"
	"log"
	"os"
	"test_task_wb/internal/model"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const testDSN = "postgres://test_user:test_password@localhost:5433/test_db?sslmode=disable"

var testStorage *Storage

func TestMain(m *testing.M) {
	migrator, err := migrate.New("file://../../migrations", testDSN)
	if err != nil {
		log.Fatalf("Не удалось создать экземпляр мигратора: %v", err)
	}

	if err := migrator.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("Не удалось накатить миграции: %v", err)
	}

	pool, err := pgxpool.New(context.Background(), testDSN)
	if err != nil {
		log.Fatalf("Не удалось подключиться к тестовой БД: %v", err)
	}
	testStorage = NewStorage(pool)

	exitCode := m.Run()

	if err := migrator.Down(); err != nil {
		log.Fatalf("Не удалось откатить миграции: %v", err)
	}

	os.Exit(exitCode)
}

func truncateTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	_, err := pool.Exec(ctx, "TRUNCATE TABLE items, payments, deliveries, orders RESTART IDENTITY CASCADE")
	require.NoError(t, err)
}

func TestStorage_SaveAndGetAllOrders(t *testing.T) {
	ctx := context.Background()

	truncateTables(t, ctx, testStorage.pool)

	order := model.Order{
		OrderUID:    "testuid123",
		TrackNumber: "track1",
		Entry:       "WBIL",
		Delivery: model.Delivery{
			Name: "Test Testov", Phone: "+9720000000", Zip: "2639809", City: "Kiryat Mozkin",
			Address: "Ploshad Mira 15", Region: "Kraiot", Email: "test@gmail.com",
		},
		Payment: model.Payment{
			Transaction: "testuid123", RequestID: "", Currency: "USD", Provider: "wbpay",
			Amount: 1817, PaymentDt: 1637907727, Bank: "alpha", DeliveryCost: 1500,
			GoodsTotal: 317, CustomFee: 0,
		},
		Items: []model.Item{
			{ChrtID: 9934930, TrackNumber: "track1", Price: 453, Rid: "ab4219087a764ae0btest",
				Name: "Mascaras", Sale: 30, Size: "0", TotalPrice: 317, NmID: 2389221, Brand: "Vivienne Sabo", Status: 202},
		},
		Locale: "en", InternalSignature: "", CustomerID: "test", DeliveryService: "meest",
		Shardkey: "9", SmID: 99, DateCreated: time.Now().UTC().Truncate(time.Second), OofShard: "1",
	}

	err := testStorage.SaveOrder(ctx, order)
	require.NoError(t, err, "Сохранение заказа не должно вызывать ошибку")

	restoredOrders, err := testStorage.GetAllOrders(ctx, 10)
	require.NoError(t, err, "Получение заказов не должно вызывать ошибку")
	require.Len(t, restoredOrders, 1, "Должен быть возвращен один заказ")

	restored := restoredOrders[0]
	require.Equal(t, order.OrderUID, restored.OrderUID)
	require.Equal(t, order.TrackNumber, restored.TrackNumber)
	require.Equal(t, order.Delivery.Name, restored.Delivery.Name)
	require.Equal(t, order.Payment.Transaction, restored.Payment.Transaction)
	require.Len(t, restored.Items, 1, "У заказа должен быть один товар")
	require.Equal(t, order.Items[0].ChrtID, restored.Items[0].ChrtID)
}
