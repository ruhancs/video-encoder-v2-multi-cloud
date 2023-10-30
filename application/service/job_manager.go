package service

import (
	"encoder-service/application/repository"
	"encoder-service/domain"
	"encoder-service/framework/queue"
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/jinzhu/gorm"
	"github.com/streadway/amqp"
)

type JobManager struct {
	DB               *gorm.DB
	Job              domain.Job
	MessageChannel   chan amqp.Delivery
	JobReturnChannel chan JobWorkerResult
	RabbitMQ         *queue.RabbitMQ
}

type JobNotificationerror struct {
	Message string `json:"message"`
	Error   string `json:"error"`
}

func NewJobManager(db *gorm.DB, rabbitMQ *queue.RabbitMQ, jobReturnChannel chan JobWorkerResult, messageChan chan amqp.Delivery) *JobManager {
	return &JobManager{
		DB:               db,
		RabbitMQ:         rabbitMQ,
		JobReturnChannel: jobReturnChannel,
		MessageChannel:   messageChan,
		Job:              domain.Job{},
	}
}

// realiza os processos de tratamento de video,
// ch = canal do rabbitmq que sera consumido as menssagens, que contem os dados do video
func (j *JobManager) Start(ch *amqp.Channel) {
	videoService := NewVideoService()
	videoService.VideoRepository = repository.VideoRepository{DB: j.DB}

	jobService := JobService{
		JobRepository: repository.JobRepository{DB: j.DB},
		VideoService:  videoService,
	}

	//definir quantos workers serao criados, cada worker pode criar muitas goroutines de acordo com CONCURRENCY_UPLOAD
	// define quantos videos seram tratados de cada vez
	concurrency, err := strconv.Atoi(os.Getenv("CONCURRENCY_WORKERS"))
	if err != nil {
		log.Fatalf("error loading var CONCURRENCY_WORKERS")
	}

	for qtdProccess := 0; qtdProccess < concurrency; qtdProccess++ {
		go JobWorker(j.MessageChannel, j.JobReturnChannel, jobService, j.Job, qtdProccess)
	}

	for jobResult := range j.JobReturnChannel {
		if jobResult.Error != nil {
			err = j.checkParseErrors(jobResult)
		} else {
			err = j.notifySuccess(jobResult, ch)
		}

		//em caso de erro nao envia a menssagem novamente para a fila novamente
		if err != nil {
			jobResult.Message.Reject(false)
		}
	}

}

func (j *JobManager) checkParseErrors(jobResult JobWorkerResult) error {
	if jobResult.Job.ID != "" {
		log.Printf("MessageID %v. Error in job: %v", jobResult.Message.DeliveryTag, jobResult.Job.ID)
	} else {
		log.Printf("MessageID %v. Error parsing message: %v", jobResult.Message.DeliveryTag, jobResult.Error)
	}

	errorMessage := JobNotificationerror{
		Message: string(jobResult.Message.Body),
		Error:   jobResult.Error.Error(),
	}

	jobJson, err := json.Marshal(errorMessage)
	if err != nil {
		return err
	}

	//envia a menssagem de erro para uma fila
	err = j.notify(jobJson)
	if err != nil {
		return err
	}

	//rejeita a menssagem e nao reenvia para a fila, sera enviada para uma deadletter queue
	err = jobResult.Message.Reject(false)
	if err != nil {
		return err
	}

	return nil
}

func (j *JobManager) notify(jobJson []byte) error {

	err := j.RabbitMQ.Notify(
		string(jobJson),
		"application/json",
		os.Getenv("RABBITMQ_NOTIFICATION_EX"),
		os.Getenv("RABBITMQ_NOTIFICATION_ROUTING_KEY"),
	)
	if err != nil {
		return err
	}
	return nil
}

func (j *JobManager) notifySuccess(jobResult JobWorkerResult, ch *amqp.Channel) error {
	mutex.Lock()
	jobJson, err := json.Marshal(jobResult)
	mutex.Unlock()
	if err != nil {
		return err
	}

	err = j.notify(jobJson)
	if err != nil {
		return err
	}

	err = jobResult.Message.Ack(false)
	if err != nil {
		return err
	}

	return nil
}
