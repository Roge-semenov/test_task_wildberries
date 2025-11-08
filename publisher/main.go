package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"test_task_wb/internal/model"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "localhost:9092"
	topic       = "orders"
)

func randomRussianPhone() string {
	operatorCode := gofakeit.Number(900, 999)
	numberPart := fmt.Sprintf("%07d", gofakeit.Number(0, 9999999))
	return fmt.Sprintf("+7%d%s", operatorCode, numberPart)
}

// generateRandomOrder создает случайный, но валидный заказ,
// опираясь на правила валидации из структуры model.Order
func generateRandomOrder() (model.Order, error) {
	gofakeit.Seed(time.Now().UnixNano())

	orderUID := gofakeit.Password(true, false, true, false, false, 20)
	trackNumber := "WBILM" + gofakeit.Password(false, true, false, false, false, 10)

	var items []model.Item
	for i := 0; i < gofakeit.Number(1, 5); i++ {
		item := model.Item{
			ChrtID:      gofakeit.Number(1000000, 9999999),
			TrackNumber: trackNumber,
			Price:       gofakeit.Number(100, 5000),
			Rid:         gofakeit.Password(true, false, true, false, false, 21),
			Name:        gofakeit.ProductName(),
			Sale:        gofakeit.Number(0, 70),
			Size:        "0",
			TotalPrice:  gofakeit.Number(100, 5000),
			NmID:        gofakeit.Number(1000000, 9999999),
			Brand:       gofakeit.Company(),
			Status:      202,
		}
		items = append(items, item)
	}

	order := model.Order{
		OrderUID:    orderUID,
		TrackNumber: trackNumber,
		Entry:       "WBIL",
		Delivery: model.Delivery{
			Name:    gofakeit.Name(),
			Phone:   randomRussianPhone(),
			Zip:     gofakeit.Zip(),
			City:    gofakeit.City(),
			Address: gofakeit.StreetName() + " " + gofakeit.StreetNumber(),
			Region:  gofakeit.State(),
			Email:   gofakeit.Email(),
		},
		Payment: model.Payment{
			Transaction:  orderUID,
			RequestID:    "",
			Currency:     gofakeit.CurrencyShort(),
			Provider:     "wbpay",
			Amount:       gofakeit.Number(1000, 10000),
			PaymentDt:    time.Now().Unix(),
			Bank:         gofakeit.BS(),
			DeliveryCost: gofakeit.Number(300, 1500),
			GoodsTotal:   gofakeit.Number(500, 8000),
			CustomFee:    0,
		},
		Items:             items,
		Locale:            "en",
		InternalSignature: "",
		CustomerID:        "test",
		DeliveryService:   "meest",
		Shardkey:          fmt.Sprintf("%d", gofakeit.Number(1, 10)),
		SmID:              gofakeit.Number(1, 100),
		DateCreated:       time.Now(),
		OofShard:          fmt.Sprintf("%d", gofakeit.Number(1, 10)),
	}

	return order, nil
}

// runAutoMode запускает режим автоматической отправки сообщений каждые 10 секунд
func runAutoMode(writer *kafka.Writer) {
	log.Println("Starting auto-generation mode. New message every 10 seconds.")
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		order, err := generateRandomOrder()
		if err != nil {
			log.Printf("Error generating random order: %v", err)
			continue
		}

		messageBytes, err := json.Marshal(order)
		if err != nil {
			log.Printf("Error marshalling order to JSON: %v", err)
			continue
		}

		sendMessage(writer, messageBytes, fmt.Sprintf("random order %s", order.OrderUID))
	}
}

// runFileMode запускает режим отправки сообщения из файла
func runFileMode(writer *kafka.Writer) {
	if len(os.Args) < 2 {
		log.Fatalf("Usage in file mode: go run main.go <json_file_name>")
	}
	filePath := os.Args[1]
	log.Printf("Starting file mode. Reading from: %s", filePath)

	messageBytes, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read file %s: %v", filePath, err)
	}

	sendMessage(writer, messageBytes, fmt.Sprintf("file %s", filePath))
}

// sendMessage — общая функция для отправки сообщения в Kafka
func sendMessage(writer *kafka.Writer, messageValue []byte, messageSource string) {
	msg := kafka.Message{
		Value: messageValue,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("Sending message from %s to topic: %s", messageSource, topic)
	err := writer.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf("Failed to send message to Kafka: %v", err)
		return
	}

	log.Println("Message sent successfully!")
}

func main() {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}
	defer writer.Close()

	fmt.Println("Select publisher mode:")
	fmt.Println("1: Auto-generate and send a random message every 10 seconds")
	fmt.Println("2: Send a message from a specified file")
	fmt.Print("Enter mode (1 or 2): ")

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	mode := strings.TrimSpace(scanner.Text())

	switch mode {
	case "1":
		os.Args = os.Args[:1]
		runAutoMode(writer)
	case "2":
		runFileMode(writer)
	default:
		log.Fatalf("Invalid mode selected. Please enter '1' or '2'.")
	}
}
