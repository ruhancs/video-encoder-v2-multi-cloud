package service

import (
	"encoder-service/domain"
	"encoder-service/framework/utils"
	"encoding/json"
	"os"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

// resutado do Worker de JobService
// Job = job que foi processado,
// Message = menssagem recebida no rabbitmq, com os dados do video
type JobWorkerResult struct {
	Job *domain.Job
	Message *amqp.Delivery
	Error error
}

var mutex = &sync.Mutex{}

// pega menssagens do rabbitmq e inicia o processamento dos videos
// messageChan canal que ira receber as menssagens de uma fila do rabbitmq, que contem dados do video para ser processado
// returnChan returna o resultado do worker
// jobService = servico que faz processamento dos videos
// workerID = controle de workers
func JobWorker(messageChan chan amqp.Delivery, returnChan chan JobWorkerResult, jobService JobService,job domain.Job, workerID int) {
	for msg := range messageChan {
		// body da msg do rabbitmq
		// validar json do rabbitmq
		err := utils.IsJson(string(msg.Body))
		if err != nil {
			 returnChan <- returnJobResult(domain.Job{}, msg, err)
			 continue
		}
		
		mutex.Lock()
		//validar video, com os dados recebidos do rabitmq
		err = json.Unmarshal(msg.Body, &jobService.VideoService.Video)
		jobService.VideoService.Video.ID = uuid.NewV4().String()
		mutex.Unlock()
		if err != nil {
			 returnChan <- returnJobResult(domain.Job{}, msg, err)
			 continue
		}
		
		err = jobService.VideoService.Video.Validate()
		if err != nil {
			 returnChan <- returnJobResult(domain.Job{}, msg, err)
			 continue
		}
		
		mutex.Lock()
		// inserir o video no db
		err = jobService.VideoService.InsertVideo()
		mutex.Unlock()
		if err != nil {
			 returnChan <- returnJobResult(domain.Job{}, msg, err)
			 continue
		}
		
		
		//inicia o job de conversao e upload do video
		job.Video = jobService.VideoService.Video
		job.OutputBucketPath = os.Getenv("OUTPUT_BUCKET_NAME")
		job.ID = uuid.NewV4().String()
		job.Status = "STARTING"
		job.CreatedAt = time.Now()
		
		mutex.Lock()
		_,err = jobService.JobRepository.Insert(&job)
		mutex.Unlock()
		if err != nil {
			 returnChan <- returnJobResult(domain.Job{}, msg, err)
			 continue
		}
		
		jobService.Job = &job
		err = jobService.Start()
		if err != nil {
			 returnChan <- returnJobResult(domain.Job{}, msg, err)
			 continue
		}

		returnChan <- returnJobResult(job,msg,nil)
	}
}

func returnJobResult(job domain.Job, message amqp.Delivery, err error) JobWorkerResult {
	result := JobWorkerResult{
		Job: &job,
		Message: &message,
		Error: err,
	}
	return result
}