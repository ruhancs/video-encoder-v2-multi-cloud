package repository

import (
	"encoder-service/domain"
	"errors"

	"github.com/jinzhu/gorm"
)

type JobRepositoryInterface interface {
	Insert(job *domain.Job) (*domain.Job, error)
	Find(id string) (*domain.Job, error)
	Update(job *domain.Job) (*domain.Job,error)
}

//opercoes na tabela de job
type JobRepository struct {
	DB *gorm.DB
}

func NewJobRepository(db *gorm.DB) *JobRepository {
	return &JobRepository{
		DB: db,
	}
}

func (repo *JobRepository) Insert(job *domain.Job) (*domain.Job, error) {
	err := repo.DB.Create(job).Error
	if err != nil {
		return nil,err
	}
	return job,nil
}

func (repo *JobRepository) Find(id string) (*domain.Job, error) {
	var job domain.Job
	repo.DB.Preload("Video").First(&job, "id=?", id)
	if job.ID == ""{
		return nil,errors.New("job not found")
	}
	
	return &job,nil
}

func (repo *JobRepository) Update(job *domain.Job) (*domain.Job,error) {
	err := repo.DB.Save(job).Error
	if err != nil {
		return nil,err
	}
	return job,nil
}