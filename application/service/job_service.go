package service

import (
	"encoder-service/application/repository"
	"encoder-service/domain"
	"errors"
	"os"
	"strconv"
)

// inicialiaza os processos de download, fragmentacao, transpilacao do video recebido
type JobService struct {
	Job           *domain.Job
	JobRepository repository.JobRepository
	VideoService  VideoService
}

// inicio do processo de tratamento do video
func (j *JobService) Start() error {
	err := j.changeJobStatus("DOWNLOADING")
	if err != nil {
		return j.failJob(err)
	}
	
	err = j.VideoService.Download(os.Getenv("INPUT_BUCKET_NAME"))	
	if err != nil {
		return j.failJob(err)
	}
	
	err = j.changeJobStatus("FRAGMENTING")
	if err != nil {
		return j.failJob(err)
	}
	
	err = j.VideoService.Fragment()
	if err != nil {
		return j.failJob(err)
	}
	
	err = j.changeJobStatus("ENCODING")
	if err != nil {
		return j.failJob(err)
	}
	
	err = j.VideoService.Encode()
	if err != nil {
		return j.failJob(err)
	}
	
	err = j.performUpload()
	if err != nil {
		return j.failJob(err)
	}

	err = j.changeJobStatus("FINISHING")
	if err != nil {
		return j.failJob(err)
	}
	
	err = j.VideoService.Finish()	
	if err != nil {
		return j.failJob(err)
	}

	err = j.changeJobStatus("COMPLETED")
	if err != nil {
		return j.failJob(err)
	}

	return nil
}

// upload dos arquivos para bucket
func (j *JobService) performUpload() error {
	err := j.changeJobStatus("UPLOADING")
	if err != nil {
		return j.failJob(err)
	}

	videoUpload := NewVideoUpload()
	videoUpload.OutputBucket = os.Getenv("OUTPUT_BUCKET_NAME")
	videoUpload.VideoPath = os.Getenv("LOCALSTORAGEPATH") + j.VideoService.Video.ID
	//ajuste de worker
	concurrency,_ := strconv.Atoi(os.Getenv("CONCURRENCY_UPLOAD"))
	donneUpload := make(chan string)

	go videoUpload.ProcessUpload(concurrency,donneUpload)
	
	var uploadResult string
	uploadResult = <-donneUpload

	if uploadResult != "upload completed" {
		return j.failJob(errors.New(uploadResult))
	}

	return nil
}

// muda o status do job
func(j *JobService) changeJobStatus(status string) error {
	var err error
	j.Job.Status = status
	j.Job,err = j.JobRepository.Update(j.Job)
	if err != nil {
		return j.failJob(err)
	}

	return nil
}

// tratamento de erros no job
func(j *JobService) failJob(error error) error {
	j.Job.Status = "FAILED"
	j.Job.Error = error.Error()
	_,err := j.JobRepository.Update(j.Job)
	if err != nil {
		return err
	}
	return error
}

