package queue

import (
	"log"
	"os"

	"github.com/streadway/amqp"
)

// Configuracoes para utilizacao das filas do rabbitmq
type RabbitMQ struct {
	User              string
	Password          string
	Host              string
	Port              string
	Vhost             string
	ConsumerQueueName string
	ConsumerName      string
	AutoAck           bool
	Args              amqp.Table
	Channel           *amqp.Channel
}

// inserir as configuracoes do rabbitmq
func NewRabbitMQ() *RabbitMQ {

	rabbitMQArgs := amqp.Table{}
	//exchange para tratamento de erros, dead letter queue
	rabbitMQArgs["x-dead-letter-exchange"] = os.Getenv("RABBITMQ_DLX")

	rabbitMQ := RabbitMQ{
		User:              os.Getenv("RABBITMQ_DEFAULT_USER"),
		Password:          os.Getenv("RABBITMQ_DEFAULT_PASS"),
		Host:              os.Getenv("RABBITMQ_DEFAULT_HOST"),
		Port:              os.Getenv("RABBITMQ_DEFAULT_PORT"),
		Vhost:             os.Getenv("RABBITMQ_DEFAULT_VHOST"),
		ConsumerQueueName: os.Getenv("RABBITMQ_CONSUMER_QUEUE_NAME"),
		ConsumerName:      os.Getenv("RABBITMQ_CONSUMER_NAME"),
		AutoAck:           false,
		Args:              rabbitMQArgs,
	}

	return &rabbitMQ
}

// conexao com o rabbitmq, retorna um channel ativo para comunicacao com rabbitmq
func (r *RabbitMQ) Connect() *amqp.Channel {
	dsn := "amqp://" + r.User + ":" + r.Password + "@" + r.Host + ":" + r.Port + r.Vhost
	conn, err := amqp.Dial(dsn)
	failOnError(err, "Failed to connect to RabbitMQ")

	r.Channel, err = conn.Channel()
	failOnError(err, "Failed to open a channel")

	return r.Channel
}

// consumir fila do rabbitmq, deve ser informado o cannal que sera consumido
func (r *RabbitMQ) Consume(messageChannel chan amqp.Delivery) {

	//declarar a fila, se a fila nao existir cria a fila
	q, err := r.Channel.QueueDeclare(
		r.ConsumerQueueName, // name
		true,                // durable
		false,               // delete when usused
		false,               // exclusive
		false,               // no-wait
		r.Args,              // arguments
	)
	failOnError(err, "failed to declare a queue")

	//ficar lendo as menssagem que chegam na fila
	incomingMessage, err := r.Channel.Consume(
		q.Name,         // queue
		r.ConsumerName, // consumer
		r.AutoAck,      // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)
	failOnError(err, "Failed to register a consumer")

	//cada msg chegada manda para o messageChannel
	//processa a msg para iniciar o job de upload de video
	go func() {
		for message := range incomingMessage {
			log.Println("Incoming new message")
			messageChannel <- message
		}
		log.Println("RabbitMQ channel closed")
		close(messageChannel)
	}()
}

// publica msg de finalizacao na exchange, indica que o job foi concluido
func (r *RabbitMQ) Notify(message string, contentType string, exchange string, routingKey string) error {

	err := r.Channel.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: contentType,
			Body:        []byte(message),
		})

	if err != nil {
		return err
	}

	return nil
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
