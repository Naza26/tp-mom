package factory

import (
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	"github.com/rabbitmq/amqp091-go"
)

type RabbitWorkQueueMiddleware struct {
	Queue      amqp091.Queue
	Connection *amqp091.Connection
	Channel    *amqp091.Channel
}

func InitializeRabbitWQ(queueName string, connectionSettings m.ConnSettings) (RabbitWorkQueueMiddleware, error) {
	hostname := connectionSettings.Hostname
	port := connectionSettings.Port
	address := fmt.Sprintf("amqp://guest:guest@%s:%d/", hostname, port)
	connection, err := amqp091.Dial(address)
	if err != nil {
		return RabbitWorkQueueMiddleware{}, middleware.ErrMessageMiddlewareDisconnected
	}

	channel, err := connection.Channel()
	if err != nil {
		return RabbitWorkQueueMiddleware{}, middleware.ErrMessageMiddlewareDisconnected
	}

	queue, err := channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		amqp091.Table{
			amqp091.QueueTypeArg: amqp091.QueueTypeQuorum,
		},
	)
	if err != nil {
		return RabbitWorkQueueMiddleware{}, middleware.ErrMessageMiddlewareMessage
	}

	return RabbitWorkQueueMiddleware{
		Queue:      queue,
		Connection: connection,
		Channel:    channel,
	}, nil

}

func (rabbitMiddleware RabbitWorkQueueMiddleware) Send(message middleware.Message) error {
	messageToPublish := amqp091.Publishing{
		ContentType: "text/plain",
		Body:        []byte(message.Body),
	}
	const DEFAULT_EXCHANGE = ""
	err := rabbitMiddleware.Channel.Publish(
		DEFAULT_EXCHANGE,
		rabbitMiddleware.Queue.Name,
		false,
		false,
		messageToPublish)
	if err != nil {
		return middleware.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (rabbitMiddleware RabbitWorkQueueMiddleware) Close() error {
	if rabbitMiddleware.Connection.IsClosed() {
		return nil
	}
	// Closing a connection closes associated channels too
	if err := rabbitMiddleware.Connection.Close(); err != nil {
		return middleware.ErrMessageMiddlewareClose
	}
	return nil
}

func (rabbitMiddleware RabbitWorkQueueMiddleware) StartConsuming(callbackFunc func(msg middleware.Message, ack func(), nack func())) error {
	return middleware.ErrMessageMiddlewareDisconnected
}

func (rabbitMiddleware RabbitWorkQueueMiddleware) StopConsuming() error {
	return middleware.ErrMessageMiddlewareClose
}
