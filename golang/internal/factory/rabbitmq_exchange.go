package factory

import (
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	"github.com/rabbitmq/amqp091-go"
)

// TODO: Pending to think how to remove some repeated code from both middleware implementations

type RabbitExchangeMiddleware struct {
	Exchange     string
	Connection   *amqp091.Connection
	Channel      *amqp091.Channel
	RoutingKeys  []string
	ConsumerName string
}

func InitializeRabbitExchange(exchange string, keys []string, connectionSettings m.ConnSettings) (*RabbitExchangeMiddleware, error) {
	hostname := connectionSettings.Hostname
	port := connectionSettings.Port
	address := fmt.Sprintf("amqp://guest:guest@%s:%d/", hostname, port)
	connection, err := amqp091.Dial(address)
	if err != nil {
		return nil, middleware.ErrMessageMiddlewareDisconnected
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, middleware.ErrMessageMiddlewareDisconnected
	}

	err = channel.ExchangeDeclare(
		exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, middleware.ErrMessageMiddlewareDisconnected
	}

	const EMPTY_CONSUMER_NAME = ""

	return &RabbitExchangeMiddleware{
		Exchange:     exchange,
		Connection:   connection,
		Channel:      channel,
		RoutingKeys:  keys,
		ConsumerName: EMPTY_CONSUMER_NAME,
	}, nil

}

func (rabbitMiddleware *RabbitExchangeMiddleware) Send(message middleware.Message) error {
	messageToPublish := amqp091.Publishing{
		ContentType: "text/plain",
		Body:        []byte(message.Body),
	}
	for _, routingKey := range rabbitMiddleware.RoutingKeys {
		err := rabbitMiddleware.Channel.Publish(
			rabbitMiddleware.Exchange,
			routingKey,
			false,
			false,
			messageToPublish)
		if err != nil {
			return middleware.ErrMessageMiddlewareDisconnected
		}
	}
	return nil
}

func (rabbitMiddleware *RabbitExchangeMiddleware) Close() error {
	if rabbitMiddleware.Connection.IsClosed() {
		return nil
	}
	if err := rabbitMiddleware.Connection.Close(); err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	return nil
}

func (rabbitMiddleware *RabbitExchangeMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	const EMPTY_QUEUE_NAME = ""
	queue, err := rabbitMiddleware.Channel.QueueDeclare(
		EMPTY_QUEUE_NAME,
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}

	for _, routingKey := range rabbitMiddleware.RoutingKeys {
		err = rabbitMiddleware.Channel.QueueBind(
			queue.Name,
			routingKey,
			rabbitMiddleware.Exchange,
			false,
			nil,
		)
		if err != nil {
			return middleware.ErrMessageMiddlewareMessage
		}
	}

	const CONSUMER_NAME = "data"
	messages, err := rabbitMiddleware.Channel.Consume(
		queue.Name,
		CONSUMER_NAME,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return middleware.ErrMessageMiddlewareMessage
	}

	rabbitMiddleware.ConsumerName = CONSUMER_NAME

	for delivery := range messages {
		receivedMessage := middleware.Message{Body: string(delivery.Body)}
		ack := func() { delivery.Ack(false) }
		nack := func() { delivery.Nack(false, false) }
		callbackFunc(receivedMessage, ack, nack)
	}

	return nil
}

func (rabbitMiddleware *RabbitExchangeMiddleware) StopConsuming() error {
	if rabbitMiddleware.ConsumerName == "" {
		return nil
	}

	err := rabbitMiddleware.Channel.Cancel(rabbitMiddleware.ConsumerName, false)

	if err != nil {
		return middleware.ErrMessageMiddlewareClose
	}

	rabbitMiddleware.ConsumerName = ""

	return nil
}
