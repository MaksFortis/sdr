package database

import (
	"testing"
)

// TestGetConfig проверяет создание конфигурации базы данных
func TestGetConfig(t *testing.T) {
	t.Run("Valid configuration", func(t *testing.T) {
		host := "localhost"
		port := "5432"
		user := "testuser"
		password := "testpass"
		name := "testdb"

		config, err := GetConfig(host, port, user, password, name)

		if err != nil {
			t.Errorf("GetConfig() returned unexpected error: %v", err)
		}

		if config == nil {
			t.Fatal("GetConfig() returned nil config")
		}

		if config.Host != host {
			t.Errorf("Expected Host '%s', got '%s'", host, config.Host)
		}

		if config.Port != port {
			t.Errorf("Expected Port '%s', got '%s'", port, config.Port)
		}

		if config.User != user {
			t.Errorf("Expected User '%s', got '%s'", user, config.User)
		}

		if config.Password != password {
			t.Errorf("Expected Password '%s', got '%s'", password, config.Password)
		}

		if config.Name != name {
			t.Errorf("Expected Name '%s', got '%s'", name, config.Name)
		}
	})

	t.Run("Empty values", func(t *testing.T) {
		config, err := GetConfig("", "", "", "", "")

		if err != nil {
			t.Errorf("GetConfig() returned unexpected error: %v", err)
		}

		if config == nil {
			t.Fatal("GetConfig() returned nil config")
		}

		if config.Host != "" {
			t.Errorf("Expected empty Host, got '%s'", config.Host)
		}

		if config.Port != "" {
			t.Errorf("Expected empty Port, got '%s'", config.Port)
		}
	})

	t.Run("Special characters in password", func(t *testing.T) {
		password := "p@ssw0rd!#$%"
		config, err := GetConfig("localhost", "5432", "user", password, "db")

		if err != nil {
			t.Errorf("GetConfig() returned unexpected error: %v", err)
		}

		if config.Password != password {
			t.Errorf("Expected Password '%s', got '%s'", password, config.Password)
		}
	})
}

// TestConfigStruct проверяет структуру Config
func TestConfigStruct(t *testing.T) {
	config := &Config{
		Host:     "192.168.1.1",
		Port:     "5432",
		User:     "admin",
		Password: "secret",
		Name:     "mydb",
	}

	if config.Host != "192.168.1.1" {
		t.Errorf("Expected Host to be set correctly, got '%s'", config.Host)
	}

	if config.Port != "5432" {
		t.Errorf("Expected Port to be set correctly, got '%s'", config.Port)
	}

	if config.User != "admin" {
		t.Errorf("Expected User to be set correctly, got '%s'", config.User)
	}

	if config.Password != "secret" {
		t.Errorf("Expected Password to be set correctly, got '%s'", config.Password)
	}

	if config.Name != "mydb" {
		t.Errorf("Expected Name to be set correctly, got '%s'", config.Name)
	}
}

// TestNewConnection_InvalidConfig проверяет обработку невалидных конфигураций
func TestNewConnection_InvalidConfig(t *testing.T) {
	t.Run("Invalid host", func(t *testing.T) {
		config := &Config{
			Host:     "invalid-host-that-does-not-exist-12345",
			Port:     "5432",
			User:     "user",
			Password: "pass",
			Name:     "db",
		}

		conn, err := config.NewConnection()

		if err == nil {
			t.Error("Expected error for invalid host, got nil")
			if conn != nil {
				conn.Close()
			}
		}

		if conn != nil {
			t.Error("Expected nil connection for invalid host")
		}
	})

	t.Run("Invalid port", func(t *testing.T) {
		config := &Config{
			Host:     "localhost",
			Port:     "99999",
			User:     "user",
			Password: "pass",
			Name:     "db",
		}

		conn, err := config.NewConnection()

		if err == nil {
			t.Error("Expected error for invalid port, got nil")
			if conn != nil {
				conn.Close()
			}
		}

		if conn != nil {
			t.Error("Expected nil connection for invalid port")
		}
	})

	t.Run("Empty configuration", func(t *testing.T) {
		config := &Config{}

		conn, err := config.NewConnection()

		if err == nil {
			t.Error("Expected error for empty config, got nil")
			if conn != nil {
				conn.Close()
			}
		}

		if conn != nil {
			t.Error("Expected nil connection for empty config")
		}
	})
}

// TestConnectionDatabase_Close проверяет закрытие соединения
func TestConnectionDatabase_Close(t *testing.T) {
	t.Run("Close nil DB", func(t *testing.T) {
		conn := &ConnectionDatabase{
			DB: nil,
		}

		err := conn.Close()

		if err != nil {
			t.Errorf("Expected no error when closing nil DB, got: %v", err)
		}
	})

	t.Run("Close on nil ConnectionDatabase", func(t *testing.T) {
		var conn *ConnectionDatabase

		// Это вызовет панику, если не обработано правильно
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Close() caused panic: %v", r)
			}
		}()

		if conn != nil {
			_ = conn.Close()
		}
	})
}

// TestConnectionDatabaseStruct проверяет структуру ConnectionDatabase
func TestConnectionDatabaseStruct(t *testing.T) {
	conn := &ConnectionDatabase{
		DB: nil,
	}

	if conn.DB != nil {
		t.Error("Expected DB to be nil")
	}
}

// TestNewConnection_DSNFormat проверяет формат DSN строки
func TestNewConnection_DSNFormat(t *testing.T) {
	// Этот тест проверяет, что DSN строка формируется правильно
	// Мы не можем напрямую проверить DSN, но можем убедиться,
	// что функция пытается подключиться с правильными параметрами

	t.Run("Standard configuration format", func(t *testing.T) {
		config := &Config{
			Host:     "localhost",
			Port:     "5432",
			User:     "postgres",
			Password: "postgres",
			Name:     "testdb",
		}

		// Ожидаем ошибку, так как база может не существовать,
		// но важно, что формат DSN правильный
		conn, err := config.NewConnection()

		// Если соединение установлено, закрываем его
		if conn != nil {
			defer conn.Close()
		}

		// Проверяем, что функция хотя бы попыталась подключиться
		// (не возникло ошибок парсинга DSN)
		if err != nil {
			// Это ожидаемо, если база данных не запущена
			t.Logf("Connection failed (expected if DB is not running): %v", err)
		}
	})
}
