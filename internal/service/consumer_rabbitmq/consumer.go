package consumer_rabbitmq

import (
	"encoding/json"
	"fmt"
	"log"

	"crm-lead-service/internal/domain"
	storageDb "crm-lead-service/internal/storage/db"
	"crm-lead-service/pkg/rabbitmq"
)

func Listener(c *rabbitmq.Client, storage *storageDb.Storage, queueName string) error {
	msgs, err := c.RabbitmqChannel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack (отключаем автоматическое подтверждение)
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer: %w", err)
	}

	log.Printf("Waiting for messages in queue: %s", queueName)

	for msg := range msgs {
		log.Printf("Received a message from queue: %s", queueName)

		var message domain.Message
		err := json.Unmarshal(msg.Body, &message)
		if err != nil {
			log.Printf("Error unmarshaling message: %v", err)
			// Отклоняем сообщение, но не возвращаем его в очередь
			msg.Nack(false, true)
			continue
		}

		// Проверяем валидность схемы сообщения
		isValid, err := message.ValidateMessage()
		if err != nil || !isValid {
			log.Printf("Invalid message schema: %v", err)
			msg.Nack(false, true)
			continue
		}

		log.Printf("Processing message: EventType=%s, Table=%s, Fields=%d",
			message.EventType, message.Schema.TableName, len(message.Data))

		err = storage.SaveMessage(&message)
		if err != nil {
			log.Printf("Error saving message to database: %v", err)
			// Отклоняем сообщение и возвращаем в очередь для повторной обработки
			msg.Nack(false, true)
			continue
		}

		log.Printf("Successfully processed message: Table=%s, EventType=%s",
			message.Schema.TableName, message.EventType)

		// Подтверждаем успешную обработку сообщения
		err = msg.Ack(false)
		if err != nil {
			log.Printf("Error acknowledging message: %v", err)
		}
	}

	return nil
}
