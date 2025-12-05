package main

import (
	"context"
	"crm-lead-service/cmd/app"
	"crm-lead-service/pkg/database"
	"crm-lead-service/pkg/rabbitmq"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configRabbit := getConfigRabbitMQ()
	clientRabbit, err := configRabbit.NewConnectionRabbit()
	if err != nil {
		log.Fatal(err)
	}

	db := getConfigDatabase()
	clientDb, err := db.NewConnection()
	if err != nil {
		log.Fatal(err)
	}

	handler := app.NewHandler(clientRabbit, clientDb, configRabbit.RabbitQueue)

	stateCh := make(chan bool, 1)

	// Запускаем обработку в горутине и отслеживаем состояние
	go func() {
		state, err := handler.Run()
		if err != nil {
			log.Fatal(err)
		}
		stateCh <- state
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Ждем либо сигнала, либо изменения состояния системы
	select {
	case <-quit:
		log.Println("Received shutdown signal...")
	case state := <-stateCh:
		if !state {
			log.Println("System state is false, shutting down...")
		}
	}

	_, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = clientRabbit.CloseRabbitMQ()
	if err != nil {
		log.Fatalf("Error closing RabbitMQ connection: %v", err)
	}
}

func getConfigRabbitMQ() *rabbitmq.Config {
	host := os.Getenv("RABBITMQ_HOST")
	port := os.Getenv("RABBITMQ_PORT")
	user := os.Getenv("RABBITMQ_USER")
	password := os.Getenv("RABBITMQ_PASSWORD")
	queue := os.Getenv("RABBITMQ_QUEUE")

	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, password, host, port)
	rabbit, err := rabbitmq.GetConfig(url, user, password, queue)
	if err != nil {
		log.Fatal(err)
	}
	return rabbit
}

func getConfigDatabase() *database.Config {
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	name := os.Getenv("DB_NAME")

	db, err := database.GetConfig(host, port, user, password, name)
	if err != nil {
		log.Fatal(err)
	}
	return db
}
