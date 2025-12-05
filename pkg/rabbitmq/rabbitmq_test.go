package rabbitmq

import (
	"os"
	"testing"
)

// TestInit проверяет инициализацию конфигурации RabbitMQ
func TestInit(t *testing.T) {
	// Сохраняем оригинальные значения переменных окружения
	originalURL := os.Getenv("RABBITMQ_URL")
	originalUser := os.Getenv("RABBITMQ_USER")
	originalPass := os.Getenv("RABBITMQ_PASSWORD")
	originalQueue := os.Getenv("RABBITMQ_QUEUE")

	// Восстанавливаем переменные окружения после теста
	defer func() {
		os.Setenv("RABBITMQ_URL", originalURL)
		os.Setenv("RABBITMQ_USER", originalUser)
		os.Setenv("RABBITMQ_PASSWORD", originalPass)
		os.Setenv("RABBITMQ_QUEUE", originalQueue)
	}()

	t.Run("Default values when env vars are not set", func(t *testing.T) {
		// Очищаем переменные окружения
		os.Unsetenv("RABBITMQ_URL")
		os.Unsetenv("RABBITMQ_USER")
		os.Unsetenv("RABBITMQ_PASSWORD")
		os.Unsetenv("RABBITMQ_QUEUE")

		rabbit, err := GetConfig()

		if err != nil {
			t.Errorf("Init() returned unexpected error: %v", err)
		}

		if rabbit == nil {
			t.Fatal("Init() returned nil RabbitMQ")
		}

		// Проверяем значения по умолчанию
		if rabbit.RabbitURL != "mqp://guest:guest@localhost:5672/" {
			t.Errorf("Expected default RabbitURL, got: %s", rabbit.RabbitURL)
		}

		if rabbit.RabbitUser != "guest" {
			t.Errorf("Expected default RabbitUser 'guest', got: %s", rabbit.RabbitUser)
		}

		if rabbit.RabbitPass != "guest" {
			t.Errorf("Expected default RabbitPass 'guest', got: %s", rabbit.RabbitPass)
		}

		if rabbit.RabbitQueue != "guest" {
			t.Errorf("Expected default RabbitQueue 'guest', got: %s", rabbit.RabbitQueue)
		}
	})

	t.Run("Custom values from env vars", func(t *testing.T) {
		// Устанавливаем кастомные значения
		os.Setenv("RABBITMQ_URL", "amqp://testuser:testpass@testhost:5672/")
		os.Setenv("RABBITMQ_USER", "testuser")
		os.Setenv("RABBITMQ_PASSWORD", "testpass")
		os.Setenv("RABBITMQ_QUEUE", "test_queue")

		rabbit, err := GetConfig()

		if err != nil {
			t.Errorf("Init() returned unexpected error: %v", err)
		}

		if rabbit == nil {
			t.Fatal("Init() returned nil RabbitMQ")
		}

		// Проверяем кастомные значения
		if rabbit.RabbitURL != "amqp://testuser:testpass@testhost:5672/" {
			t.Errorf("Expected custom RabbitURL, got: %s", rabbit.RabbitURL)
		}

		if rabbit.RabbitUser != "testuser" {
			t.Errorf("Expected custom RabbitUser 'testuser', got: %s", rabbit.RabbitUser)
		}

		if rabbit.RabbitPass != "testpass" {
			t.Errorf("Expected custom RabbitPass 'testpass', got: %s", rabbit.RabbitPass)
		}

		if rabbit.RabbitQueue != "test_queue" {
			t.Errorf("Expected custom RabbitQueue 'test_queue', got: %s", rabbit.RabbitQueue)
		}
	})
}

// TestRabbitMQStruct проверяет структуру RabbitMQ
func TestRabbitMQStruct(t *testing.T) {
	rabbit := &Config{
		RabbitURL:   "amqp://localhost:5672/",
		RabbitUser:  "user",
		RabbitPass:  "pass",
		RabbitQueue: "queue",
	}

	if rabbit.RabbitURL != "amqp://localhost:5672/" {
		t.Errorf("Expected RabbitURL to be set correctly")
	}

	if rabbit.RabbitUser != "user" {
		t.Errorf("Expected RabbitUser to be set correctly")
	}

	if rabbit.RabbitPass != "pass" {
		t.Errorf("Expected RabbitPass to be set correctly")
	}

	if rabbit.RabbitQueue != "queue" {
		t.Errorf("Expected RabbitQueue to be set correctly")
	}
}
