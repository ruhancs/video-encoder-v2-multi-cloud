package main

import (
	"encoder-service/application/service"
	"encoder-service/framework/database"
	"encoder-service/framework/queue"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
)

var db database.Database

func init() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("error loading .env file")
	}

	fmt.Println("INIT")
	autoMigrateDB,err := strconv.ParseBool(os.Getenv("AUTO_MIGRATE_DB"))
	if err != nil {
		log.Fatalf("Error parsing bool env file")
	}
	debug,err := strconv.ParseBool(os.Getenv("DEBUG"))
	if err != nil {
		log.Fatalf("Error parsing bool env file")
	}

	db.Automigrate = autoMigrateDB
	db.Debug = debug
	db.DSNTest = os.Getenv("DSN_TEST")
	db.DSN = os.Getenv("DSN")
	db.DBTypeTest = os.Getenv("DB_TYPE_TEST")
	db.DBType = os.Getenv("DB_TYPE")
	db.Env = os.Getenv("ENV")
}

//criar no rabbitmq exchange dlx para receber msg rejeitadas, exchange deve ter o type fanout
//criar fila com binding em dlx para pegar os resultados das falhas de processamento

//criar fila para enviar os resultados do processamento de videos
//fazer binding da fila criada para receber os resultados do processamento de videos com amq.direct e inserir a Routing key = jobs

//fila de videos é criada automaticamente, fila para consumir os dados do video para processar

//detectar race condition: go run -race framework/cmd/server/server.go

//paiload de test


func main() {
	fmt.Println("MAIN")
	messageChannel := make(chan amqp.Delivery)
	jobReturnChannel := make(chan service.JobWorkerResult)

	dbConnection,err := db.Connect()
	if err != nil {
		log.Fatalf("error connecting to db")
	}
	defer dbConnection.Close()

	rabbitMQ := queue.NewRabbitMQ()
	rabbitMQChan := rabbitMQ.Connect()
	defer rabbitMQChan.Close()

	rabbitMQ.Consume(messageChannel)

	jobManager := service.NewJobManager(dbConnection,rabbitMQ,jobReturnChannel,messageChannel)
	jobManager.Start(rabbitMQChan)
}