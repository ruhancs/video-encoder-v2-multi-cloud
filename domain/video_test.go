package domain_test

import (
	"encoder-service/domain"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func TestValidateIfVideoIsEmpty(t *testing.T) {
	video := domain.NewVideo()
	err := video.Validate()

	require.Error(t,err)
}

func TestIDIsNotUUID(t *testing.T) {
	video := domain.NewVideo()
	video.ID = "uyasgd"
	video.ResourceID = "a"
	video.FilePath = "a/a"
	video.CreatedAt = time.Now()

	err := video.Validate()

	require.Error(t,err)
}

func TestValidation(t *testing.T) {
	video := domain.NewVideo()
	video.ID = uuid.NewV4().String()
	video.ResourceID = "a"
	video.FilePath = "a/a"
	video.CreatedAt = time.Now()

	err := video.Validate()

	require.Nil(t,err)
}