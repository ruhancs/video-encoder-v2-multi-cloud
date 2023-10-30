package domain_test

import (
	"encoder-service/domain"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func TestNewJob(t *testing.T){
	video := domain.NewVideo()
	video.ID = uuid.NewV4().String()
	video.FilePath = "path"
	video.ResourceID = "resourceID"
	video.CreatedAt = time.Now()
	job,err := domain.NewJob("output/bucket/path","init", video)

	require.Nil(t,err)
	require.NotNil(t,job.ID)
	require.Equal(t,job.OutputBucketPath,"output/bucket/path")
	require.Equal(t,job.Status,"init")
}