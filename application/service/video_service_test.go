package service_test

import (
	"encoder-service/application/repository"
	"encoder-service/application/service"
	"encoder-service/domain"
	"encoder-service/framework/database"
	"log"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func init() {
	err := godotenv.Load("../../.env")
	if err != nil {
		log.Fatalf("error to load env file")
	}
}

func prepare() (*domain.Video, repository.VideoRepository) {
	db := database.NewDBTest()
	defer db.Close()

	video := domain.NewVideo()
	video.ID = uuid.NewV4().String()
	video.FilePath = "convite.MP4"
	video.CreatedAt = time.Now()

	repo := repository.VideoRepository{DB: db}
	repo.Insert(video)

	return video,repo
}

func TestVideoServiceDownload(t *testing.T) {
	video,repo := prepare()
	videoService := service.NewVideoService()
	videoService.VideoRepository = repo
	videoService.Video = video

	err := videoService.Download(os.Getenv("INPUT_BUCKET_NAME"))
	require.Nil(t,err)
	
	err = videoService.Fragment()
	require.Nil(t,err)
	
	err = videoService.Encode()
	require.Nil(t,err)
	
	err = videoService.Finish()
	require.Nil(t,err)
}