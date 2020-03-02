package rmq

type MessageConsumerCallback func(message []byte)
type MessageQueue interface {
	SetupConnection(connectionName string, info *AmqpConnectionInfo)
	Subscribe(connectionName, queueName, routingKey, exchange string)
	Consume(consumerCallback MessageConsumerCallback)
	Close()
	Publish(message interface{}, replyTo, msgXferType, contentType string) error
}
