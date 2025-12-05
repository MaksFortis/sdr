package rabbitmq

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Config struct {
	RabbitURL   string
	RabbitUser  string
	RabbitPass  string
	RabbitQueue string
}

type Client struct {
	RabbitmqConn    *amqp.Connection
	RabbitmqChannel *amqp.Channel
}

func GetConfig(url, user, password, queue string) (*Config, error) {
	return &Config{
		RabbitURL:   url,
		RabbitUser:  user,
		RabbitPass:  password,
		RabbitQueue: queue,
	}, nil
}

func (c *Config) NewConnectionRabbit() (*Client, error) {
	clientRabbit := &Client{}
	var err error
	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		clientRabbit.RabbitmqConn, err = amqp.Dial(c.RabbitURL)
		if err == nil {
			break
		}
		waitTime := time.Duration(i+1) * 2 * time.Second
		time.Sleep(waitTime)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ after %d attempts: %w", maxRetries, err)
	}

	clientRabbit.RabbitmqChannel, err = clientRabbit.RabbitmqConn.Channel()

	if err != nil {
		return nil, fmt.Errorf("ошибка создания канала: %w", err)
	}

	_, err = clientRabbit.RabbitmqChannel.QueueDeclare(
		c.RabbitQueue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare queue: %w", err)
	}

	// батч
	err = clientRabbit.RabbitmqChannel.Qos(
		10,
		0,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return clientRabbit, nil
}

func (c *Client) CloseRabbitMQ() error {

	if c.RabbitmqChannel != nil {
		err := c.RabbitmqChannel.Close()
		if err != nil {
			return err
		}
	}
	if c.RabbitmqConn != nil {
		err := c.RabbitmqConn.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
