package rabbitmq

import (
	"log"
	"os"

	"github.com/streadway/amqp"
)

var rbtCount int = 0
var rbtErrorCount int = 0

//RegisterENV marca a connection string nas enviroments variables
func RegisterENV(connectionString string) {
	os.Setenv("RABBITMQURL", connectionString)
}

//GetChannel Connecta no RabbitMQ e devolve o Canal
func GetChannel() (*amqp.Connection, *amqp.Channel) {
	conn, err := amqp.Dial(os.Getenv("RABBITMQURL"))
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	return conn, ch
}

//ChannelToExchage lê um canal de string e envia os textos para a exchange
func ChannelToExchage(ch chan string, exchange, routingKey string) {
	_conn, _ch := GetChannel()
	defer _conn.Close()
	defer _ch.Close()

	for {
		body := <-ch
		rbtCount++
		// StringToExch(exchange, routingKey, body)

		err := _ch.Publish(
			exchange,   // exchange
			routingKey, // routing key
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(body),
			})

		if err != nil {
			log.Println(err)
			rbtErrorCount++
			ch <- body
			_conn, _ch = GetChannel()
		}
	}
}

//StringToExch envia uma string para uma exchange do RabbitMQ
//**Obrigatório ter registrado a enviroment variable RABBITMQURL com  string de conexão**
func StringToExch(exchange, routingKey, body string) {
	conn, ch := GetChannel()
	defer conn.Close()
	defer ch.Close()

	err := ch.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})

	if err != nil {
		conn, ch = GetChannel()
	}
}

//BytesToExch envia um array de byte para uma exchange do RabbitMQ
//**Obrigatório ter registrado a enviroment variable RABBITMQURL com  string de conexão**
func BytesToExch(exchange, routingKey string, body []byte) {
	conn, ch := GetChannel()
	defer conn.Close()
	defer ch.Close()

	err := ch.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		})

	failOnError(err, "Failed to publish a message")
}

//ConsumeQueue consume uma fila e aciona uma função com o retorno
func ConsumeQueue(queue string) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery) {
	conn, ch := GetChannel()

	msgs, err := ch.Consume(
		queue, // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	failOnError(err, "Failed to Consume Queue")

	return conn, ch, msgs
}

//ConsumeBroadcast retona um canal de leitura de broadcast de uma exchange\
//**Importante:** A Exchange tem que ser do tipo FANOUT\
//Envio a CONNECTION e o CHANNEL para poder fechar. defer conn.Close()
func ConsumeBroadcast(exchange string) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery) {
	conn, ch := GetChannel()

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to Declare Temp Queue")

	err = ch.QueueBind(
		q.Name,   // queue
		"",       // key
		exchange, // exchange
		false,    // noWait
		nil,      // args Table
	)
	failOnError(err, "Failed to Bind Exchange with Temp Queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register consumer")

	return conn, ch, msgs
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
