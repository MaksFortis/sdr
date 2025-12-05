package app

import (
	"crm-lead-service/internal/service/consumer_rabbitmq"
	storageDb "crm-lead-service/internal/storage/db"
	"crm-lead-service/pkg/database"
	"crm-lead-service/pkg/rabbitmq"
)

type Handler struct {
	Client    *rabbitmq.Client
	DB        *storageDb.Storage
	QueueName string
}

func NewHandler(rabbit *rabbitmq.Client, db *database.ConnectionDatabase, queueName string) *Handler {
	storage, _ := storageDb.NewStorage(db)
	return &Handler{
		Client:    rabbit,
		DB:        storage,
		QueueName: queueName,
	}
}

func (h *Handler) Run() (bool, error) {
	err := consumer_rabbitmq.Listener(h.Client, h.DB, h.QueueName)
	if err != nil {
		return false, err
	}
	return true, nil
}
