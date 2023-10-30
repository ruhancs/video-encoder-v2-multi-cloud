package service_test

import (
	"encoder-service/application/service"
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/require"
)

func init() {
	err := godotenv.Load("../../.env")
	if err != nil {
		log.Fatalf("error to load env file")
	}
}

func TestUploadManager(t *testing.T) {
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
	
	videoUpload := service.NewVideoUpload()
	videoUpload.OutputBucket = (os.Getenv("INPUT_BUCKET_NAME"))
	videoUpload.VideoPath = os.Getenv("LOCALSTORAGEPATH") + video.ID

	doneUpload := make(chan string)
	go videoUpload.ProcessUpload(50, doneUpload)
	
	result := <-doneUpload
	require.Equal(t,result,"upload completed")

	err = videoService.Finish()
	require.Nil(t,err)
}