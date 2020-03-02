package rmq

import (
	"log"
	"time"
	"github.com/spacemonkeygo/openssl"
	"github.com/streadway/amqp"
)

func (r *rmqAutoConnection) getRabbitMQWithRetry(attempts, interval int) (connection *amqp.Connection) {
	if attempts <= 0 {
		attempts = 30
	}
	if interval < 1 || interval > 120 {
		interval = 10
	}
	connection = nil
	var err error
	for ; attempts > 0; attempts-- {
		if r.local {
			connection, err = r.getRMQLocalConnection()
		} else {
			connection, err = r.getAMQPSConnection()
		}
		if err == nil || attempts == 0 {
			break
		}
		log.Print("Will attempt reconnection after interval seconds: ", interval)
		time.Sleep(time.Second * time.Duration(interval))
	}
	if err != nil {
		log.Fatal("Could not open RabbitMQ connection, err=", err)
	} else {
		log.Printf("RabbitMQ connection setup w/ remaining attempts=%d local=%d\n", attempts, r.local)
	}
	return
}

func (r *rmqAutoConnection) getRMQLocalConnection() (connection *amqp.Connection, err error) {
	url := "amqp://" + r.connParams.userName + ": " + r.connParams.passKey + "@localhost:5672" + "/" + r.connParams.vHost
	connection, err = amqp.Dial(url)
	return
}

func (r *rmqAutoConnection) getAMQPSConnection() (connection *amqp.Connection, err error) {
	ctx, err := openssl.NewCtx()
	if err != nil {
		log.Fatal(err)
	}
	clCert, err := openssl.LoadCertificateFromPEM([]byte(r.connParams.clientCert))
	if err != nil {
		log.Fatal(err)
	}
	err = ctx.UseCertificate(clCert)
	if err != nil {
		log.Fatal(err)
	}
	privateKey, err := openssl.LoadPrivateKeyFromPEM([]byte(r.connParams.clientKey))
	if err != nil {
		log.Fatal(err)
	}
	err = ctx.UsePrivateKey(privateKey)
	if err != nil {
		log.Fatal(err)
	}
	certStore := ctx.GetCertificateStore()

	err = certStore.LoadCertificatesFromPEM([]byte(r.connParams.serverCertChain))
	if err != nil {
		log.Fatal(err)
	}

	var amqpConnection *amqp.Connection

	sslConnection, err := openssl.Dial("tcp", r.connParams.host+":5671", ctx, openssl.InsecureSkipHostVerification)

	if err != nil {
		log.Fatal(err)
	} else {
		config := amqp.Config{
			Vhost: r.connParams.vHost,
			SASL:  []amqp.Authentication{&amqp.PlainAuth{Username: r.connParams.userName, Password: r.connParams.passKey}},
		}

		amqpConnection, err = amqp.Open(sslConnection, config)
		log.Println("amqp = ", amqpConnection, ", err= ", err)
	}

	return amqpConnection, err
}

func waitForExchange(conn *amqp.Connection, exchangeName string, exchangeType string) {
	for exchangeExists := false; exchangeExists == false; {
		channel, _ := conn.Channel()
		err := channel.ExchangeDeclarePassive(exchangeName, exchangeType, true, false, false, false, nil)
		if err != nil {
			time.Sleep(5 * time.Second)
			log.Printf("Waiting for %s %s exchange\n", exchangeName, exchangeType)
		} else {
			exchangeExists = true
			_ = channel.Close()
		}
	}
}
