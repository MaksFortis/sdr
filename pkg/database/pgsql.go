package database

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
}

type ConnectionDatabase struct {
	DB *sql.DB
}

func GetConfig(host, port, user, password, name string) (*Config, error) {
	return &Config{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Name:     name,
	}, nil
}

func (d Config) NewConnection() (*ConnectionDatabase, error) {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		d.Host, d.Port, d.User, d.Password, d.Name)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}

	return &ConnectionDatabase{
		DB: db,
	}, nil
}

// Close закрывает соединение с базой данных
func (c *ConnectionDatabase) Close() error {
	if c.DB != nil {
		return c.DB.Close()
	}
	return nil
}
