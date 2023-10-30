package service

import (
	"context"
	"encoder-service/application/repository"
	"encoder-service/domain"
	"io"
	"log"
	"os"
	"os/exec"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

//services que pode ser executado com a entidade video,
//metodo download de video, baixa o video inserido na bucket,
//metodo fragment de video, converte o video de mp4 para frag,
//metodo encoder de video, converte o video fragmentado para mpegDash, divide o video em pedacos,
//metodo finish, apos finnalizado apaga os arquivos gerados, video.mp4,video.frag, pasta com id do video,
type VideoService struct {
	Video           *domain.Video
	VideoRepository repository.VideoRepository
}

func NewVideoService() VideoService{
	return VideoService{}
}

//recebe bucketName e faz download do video, conforme o caminho na entidade de Video do servico
func (vs *VideoService) Download(bucketName string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	bkt := client.Bucket(bucketName)
	bktObject := bkt.Object(vs.Video.FilePath)
	readerObj,err := bktObject.NewReader(ctx)
	if err != nil {
		return err
	}
	defer readerObj.Close()
	
	bodyObj,err := io.ReadAll(readerObj)
	if err != nil {
		return err
	}
	
	//criar arquivo com id do video como nome e extencao .mp4 no caminho LOCALSTORAGEPATH
	file,err := os.Create(os.Getenv("LOCALSTORAGEPATH") + vs.Video.ID + ".mp4")
	if err != nil {
		return err
	}
	
	//copiar o arquivo do video baixado para o arquivo criado
	_,err = file.Write(bodyObj)
	if err != nil {
		return err
	}
	defer file.Close()

	log.Printf("video %v has been storage", vs.Video.ID)

	return nil
}

//recebe bucketName e faz download do video, conforme o caminho na entidade de Video do servico
func (vs *VideoService) DownloadFromS3(bucketName string) error {
    file, err := os.Create(os.Getenv("LOCALSTORAGEPATH") + vs.Video.ID + ".mp4")
    if err != nil {
        return err
    }

    defer file.Close()

    // Initialize a session in us-west-2 that the SDK will use to load
    // credentials from the shared credentials file ~/.aws/credentials.
    sess,err := session.NewSession(
		&aws.Config{
			Region: aws.String(os.Getenv("AWS_REGION")),
			Credentials: credentials.NewStaticCredentials(
				os.Getenv("AWS_PK"),
				os.Getenv("AWS_SK"),
				"",
			),
		},
	)
	if err != nil {
		return err
	}

    downloader := s3manager.NewDownloader(sess)

    _, err = downloader.Download(file,
        &s3.GetObjectInput{
            Bucket: &bucketName,
            Key:    aws.String(vs.Video.FilePath),
        })
    if err != nil {
        return err
    }

    return nil
}

//executa comando no sistema chamando o bento4 para fragmentar o video
func (vs *VideoService) Fragment() error {
	//criar pasta para enviar video fragmentado
	err := os.Mkdir(os.Getenv("LOCALSTORAGEPATH") + vs.Video.ID, os.ModePerm)
	if err != nil {
		return err
	} 
	
	//local onde o video baixado esta armazenado, e onde o video fragmentado sera inserido
	source := os.Getenv("LOCALSTORAGEPATH") + vs.Video.ID + ".mp4"
	target := os.Getenv("LOCALSTORAGEPATH") + vs.Video.ID + ".frag"
	
	//comando bento4
	cmd := exec.Command("mp4fragment", source,target)
	output,err := cmd.CombinedOutput()
	if err != nil {
		return err
	} 

	printOutput(output)

	return nil
}

//converte video para formato mpegDash
func (vs *VideoService) Encode() error {
	cmdArgs := []string{}
	cmdArgs = append(cmdArgs, os.Getenv("LOCALSTORAGEPATH") + vs.Video.ID + ".frag")
	cmdArgs = append(cmdArgs, "--use-segment-timeline")
	cmdArgs = append(cmdArgs, "-o")
	cmdArgs = append(cmdArgs, os.Getenv("LOCALSTORAGEPATH") + vs.Video.ID)
	cmdArgs = append(cmdArgs, "-f")
	cmdArgs = append(cmdArgs, "--exec-dir")
	cmdArgs = append(cmdArgs, "/opt/bento4/bin/")
	cmd := exec.Command("mp4dash", cmdArgs...)

	output,err := cmd.CombinedOutput()
	if err != nil {
		return err
	}

	printOutput(output)
	return nil
}

func (vs *VideoService) Finish() error {
	err := os.Remove(os.Getenv("LOCALSTORAGEPATH")+vs.Video.ID+".mp4")
	if err != nil {
		log.Println("error removing mp4 file")
		return err
	}
	
	err = os.Remove(os.Getenv("LOCALSTORAGEPATH")+vs.Video.ID+".frag")
	if err != nil {
		log.Println("error removing frag file")
		return err
	}
	
	err = os.RemoveAll(os.Getenv("LOCALSTORAGEPATH")+vs.Video.ID)
	if err != nil {
		log.Println("error removing frag file")
		return err
	}
	
	log.Println("all files removed successfuly")
	return nil
}

func (vs *VideoService) InsertVideo() error {
	_,err := vs.VideoRepository.Insert(vs.Video)
	if err != nil {
		return err
	}
	return nil
}

func printOutput(out []byte) {
	if len(out) > 0 {
		log.Printf("======>Output: %s\n", string(out))
	}
}
