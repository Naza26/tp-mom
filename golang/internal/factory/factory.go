package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	rabbitMQ, err := InitializeRabbitWQ(queueName, connectionSettings)
	if err != nil {
		return nil, err
	}
	return rabbitMQ, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	rabbitExchange, err := InitializeRabbitExchange(exchange, keys, connectionSettings)
	if err != nil {
		return nil, err
	}
	return rabbitExchange, nil
}
