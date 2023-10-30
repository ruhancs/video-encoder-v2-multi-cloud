package domain

import (
	"time"

	"github.com/asaskevich/govalidator"
	uuid "github.com/satori/go.uuid"
)

func init() {
	govalidator.SetFieldsRequiredByDefault(true)
}

// OutputBucketPath = caminho de saida do video convertido, caminho para a bucket.
// Status = status do processamento do video.
// Video = video para converter.
type Job struct {
	ID               string    `json:"job_id" valid:"uuid" gorm:"type:uuid;primary_key"`
	OutputBucketPath string    `json:"output_bucket_path" valid:"notnull"`
	Status           string    `json:"status" valid:"notnull"`
	Video            *Video    `json:"video" valid:"-"`
	VideoID          string    `json:"-" valid:"-" gorm:"column:video_id;type:uuid;notnull"`
	Error            string    `valid:"-"`
	CreatedAt        time.Time `json:"created_at" valid:"-"`
	UpdateddAt       time.Time `json:"updated_at" valid:"-"`
}

func (j *Job) prepare() {
	j.ID = uuid.NewV4().String()
	j.CreatedAt = time.Now()
	j.UpdateddAt = time.Now()
}

// recebe ouptut= caminho para bucket que o video convertido sera armazenado
// status= status do processamento
// video= video a ser processado
func NewJob(output, status string, video *Video) (*Job, error) {
	job := &Job{
		OutputBucketPath: output,
		Status:           status,
		Video:            video,
	}
	job.prepare()

	err := job.Validate()
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (j *Job) Validate() error {
	_, err := govalidator.ValidateStruct(j)
	if err != nil {
		return err
	}
	return nil
}
