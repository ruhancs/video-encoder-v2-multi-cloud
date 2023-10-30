package repository_test

import (
	"encoder-service/application/repository"
	"encoder-service/domain"
	"encoder-service/framework/database"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func TestJobRepositoryInsert(t *testing.T) {
	db := database.NewDBTest()
	defer db.Close()

	video := domain.NewVideo()
	video.ID = uuid.NewV4().String()
	video.FilePath = "path"
	video.CreatedAt = time.Now()

	videoRepo := repository.VideoRepository{DB: db}
	videoRepo.Insert(video)

	job,_ := domain.NewJob("bucket_path", "init",video)

	jobRepo := repository.NewJobRepository(db)
	jobRepo.Insert(job)

	jobFounded,err := jobRepo.Find(job.ID)

	require.Nil(t,err)
	require.NotNil(t,jobFounded)
	require.Equal(t,jobFounded.ID,job.ID)
	require.Equal(t,jobFounded.OutputBucketPath,job.OutputBucketPath)
	require.Equal(t,jobFounded.Status,job.Status)
	require.Equal(t,jobFounded.VideoID,video.ID)
	require.Equal(t,jobFounded.Video.FilePath,video.FilePath)
}

func TestJobRepositoryUpdate(t *testing.T) {
	db := database.NewDBTest()
	defer db.Close()

	video := domain.NewVideo()
	video.ID = uuid.NewV4().String()
	video.FilePath = "path"
	video.CreatedAt = time.Now()

	videoRepo := repository.VideoRepository{DB: db}
	videoRepo.Insert(video)

	job,_ := domain.NewJob("bucket_path", "init",video)

	jobRepo := repository.NewJobRepository(db)
	jobRepo.Insert(job)

	job.Status = "processing"
	jobUpdated,err := jobRepo.Update(job)

	require.Nil(t,err)
	require.NotNil(t,jobUpdated)
	require.Equal(t,jobUpdated.Status,"processing")
}