package repository

import (
	"encoder-service/domain"
	"errors"

	"github.com/jinzhu/gorm"
	uuid "github.com/satori/go.uuid"
)

type VideoRepositoryInterface interface {
	Insert(video *domain.Video) (*domain.Video, error)
	Find(id string) (*domain.Video, error)
}

type VideoRepository struct {
	DB *gorm.DB
}

func NewVideoRepository(db *gorm.DB) *VideoRepository {
	return &VideoRepository{
		DB: db,
	}
}

func (repo *VideoRepository) Insert(video *domain.Video) (*domain.Video, error) {
	if video.ID == "" {
		video.ID = uuid.NewV4().String()
	}

	err := repo.DB.Create(video).Error
	if err != nil {
		return nil,err
	}
	return video,nil
}

func (repo *VideoRepository) Find(id string) (*domain.Video, error) {
	var video domain.Video
	repo.DB.Preload("Jobs").First(&video, "id=?", id)
	if video.ID == ""{
		return nil,errors.New("video not found")
	}

	return &video,nil
}
