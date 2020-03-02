package rmq

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"time"
)

type consumerCallback func(amqp.Delivery)
type postConnectCallback func(r *rmqAutoConnection)
type reConnectCallback func(r *rmqAutoConnection)

type AmqpConnectionInfo struct {
	clientCert      string
	clientKey       string
	serverCertChain string
	host            string
	userName        string
	passKey         string
	vHost           string
}

type rmqAutoConnection struct {
	connectionName      string
	connection          *amqp.Connection
	errChannel          chan *amqp.Error
	postConnectCallback postConnectCallback
	reConnectCallbacks  []reConnectCallback
	local               bool
	connParams          *AmqpConnectionInfo
}

func (r *rmqAutoConnection) addReConnectorCallback(callback reConnectCallback) {
	r.reConnectCallbacks = append(r.reConnectCallbacks, callback)
}
func (r *rmqAutoConnection) connect() {
	r.connection = r.getRabbitMQWithRetry(0, 0)
	r.errChannel = make(chan *amqp.Error)
	r.connection.NotifyClose(r.errChannel)

	if r.postConnectCallback != nil {
		log.Printf("Calling <%s> postconnect callback\n", r.connectionName)
		r.postConnectCallback(r)
	}
}

func (r *rmqAutoConnection) connectWithReConnector(postConnectCallback postConnectCallback) {
	r.postConnectCallback = postConnectCallback
	r.connect()
	go r.reconnect()
}

func (r *rmqAutoConnection) reconnect() {
	for {
		err := <-r.errChannel
		if err != nil {
			log.Printf("Reconnecting for RMQConn=<%s>, it was closed w/ E=%s\n",
				r.connectionName, err)
			r.connect()

			if r.reConnectCallbacks != nil {
				for _, reConnectCallback := range r.reConnectCallbacks {
					reConnectCallback(r)
				}
			}
		}
	}
}

func (r *rmqAutoConnection) close() {
	_ = r.connection.Close()
}

var connections map[string]*rmqAutoConnection

func getRmqAutoConn(connName string) *rmqAutoConnection {
	return connections[connName]
}
func setRmqAutoConn(connName string, params *AmqpConnectionInfo) {
	if connections == nil {
		connections = make(map[string]*rmqAutoConnection)
	}
	if connections[connName] == nil {
		r := new(rmqAutoConnection)
		r.connectionName = connName
		r.connParams = params
		connections[connName] = r
	}
}

type rmqQueue struct {
	exchange         string
	queueName        string
	routingKey       string
	channel          *amqp.Channel
	connection       *rmqAutoConnection
	consumerCallback consumerCallback
}

func (r *rmqQueue) close() {
	if r.channel != nil {
		_ = r.channel.Close()
		r.channel = nil
	}
}

func (r *rmqQueue) setUpChannel(exchange string) {
	if exchange != "" {
		waitForExchange(r.connection.connection, exchange, "topic")
	}
	r.channel, _ = r.connection.connection.Channel()
}

func (r *rmqQueue) consume(consumerCallback consumerCallback) {
	r.consumerCallback = consumerCallback
	r.executeMessageConsumer()
}

func (r *rmqQueue) executeMessageConsumer() {
	if r.consumerCallback != nil {
		go func() {
			msgs, _ := r.channel.Consume(
				r.queueName, // queue
				"",          // messageConsumer
				true,        // auto-ack
				false,       // exclusive
				false,       // no-local
				false,       // no-wait
				nil,         // args
			)
			for msg := range msgs {
				r.consumerCallback(msg)
			}
		}()
	}
}

func (r *rmqQueue) setUpQueue(queueName, exchange, rKey string) {
	r.queueName = queueName
	r.routingKey = rKey
	r.exchange = exchange
	_, _ = r.channel.QueueDeclare(
		r.queueName, // name
		false,       // durable
		true,        // delete when unused
		false,       // exclusive
		true,        // no-wait
		nil,         // arguments
	)
	_ = r.channel.QueueBind(r.queueName, r.routingKey, r.exchange, false, nil)
}

// Implement MessageQueue interface and return different queues
func (r *rmqQueue) SetupConnection(connName string, params *AmqpConnectionInfo) {
	setRmqAutoConn(connName, params)
}

func (r *rmqQueue) Subscribe(connectionName string, queueName string, rKey string, exchange string) {
	connection := getRmqAutoConn(connectionName)
	if connection != nil {
		connection.connectWithReConnector(
			func(rmqConn *rmqAutoConnection) {
				r.connection = rmqConn
				r.setUpChannel(exchange)
				r.setUpQueue(queueName, exchange, rKey)
				log.Printf("Set up channel for %s on %s", queueName, exchange)
			})
	}
}

func (r *rmqQueue) Publish(message interface{}, replyTo, msgXferType, contentType string) error {
	//r.conn.channel.Publish()
	body, _ := json.Marshal(message)
	err := r.channel.Publish(
		r.exchange,   // exchange
		r.routingKey, // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ReplyTo:       replyTo,
			Type:          msgXferType,
			ContentType:   contentType,
			Body:          body,
			MessageId:     "0:0",
			CorrelationId: time.Now().Format(time.RFC3339),
		})
	return err
}

func (r *rmqQueue) Consume(consumerCallback MessageConsumerCallback) {
	r.consume(func(delivery amqp.Delivery) {
		log.Printf("Received Message %s", delivery.Body)
		consumerCallback(delivery.Body)
	})
}

func (r *rmqQueue) Close() {
	r.close()
}

func NewMessageQue() MessageQueue {
	return new(rmqQueue)
}
