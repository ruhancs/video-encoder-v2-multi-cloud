package service

import (
	"context"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Paths caminho dos videos,
// VideoPath caminho que o video sera enviado,
// OutputBucket bucket que os videos serao enviados
type VideoUpload struct {
	Paths        []string
	VideoPath    string
	OutputBucket string
	Errors       []string
}

func NewVideoUpload() *VideoUpload{
	return &VideoUpload{}
}

// realiza upload de um arquivo para bucket,
// objPath caminho completo do video armazenado,
// client para acesso na bucket
func (upload *VideoUpload) UploadObject(objPath string, client *storage.Client, ctx context.Context) error{
	//separar arquivo e caminho do arquivo, path[0] = caminho, path[1] arquivo de video
	path := strings.Split(objPath, os.Getenv("LOCALSTORAGEPATH"))

	//abrir o arquivo
	f,err := os.Open(objPath)
	if err != nil {
		return err
	}
	defer f.Close()

	//client do gcp para gravar o arquivo, gravar video no gcp
	writerClient := client.Bucket(upload.OutputBucket).Object(path[1]).NewWriter(ctx)
	//permissao para todos usuario que tem permissao de leitura, poder ler o arquivo
	writerClient.ACL = []storage.ACLRule{{Entity: storage.AllUsers, Role: storage.RoleReader}}

	//copia o arquivo para o writerClient
	if _,err := io.Copy(writerClient, f); err != nil {
		return err
	}

	//fechar conexao
	if err := writerClient.Close(); err != nil {
		return err
	}

	return nil
}

// gerencia processos de upload para a bucket,
// concurency = numero maximo de threads para fazer upload,
// doneUpload = canal com o resultado dos uploads
func (upload *VideoUpload) ProcessUpload(concurency int, doneUpload chan string) error{
	//ponto de entrada dos uploads, contem a posicao dos caminhos dos arquivos carregados em paths
	inputChan := make(chan int, runtime.NumCPU())
	//canal de retorno de erro o processo finalizado
	returnChan := make(chan string)

	err := upload.loadPaths()
	if err != nil {
		return err
	}
	
	//uploadClient,ctx,err := getClientUpload()
	uploadClient,err := getUploadClientS3()
	if err != nil {
		return err
	}

	//inicio dos processos de upload, determinado por concurency
	for proccess := 0; proccess < concurency; proccess++{
		//go upload.uploadWorker(inputChan, returnChan, uploadClient, ctx)
		go upload.uploadWorker(inputChan, returnChan, uploadClient)
	}

	//envio das posicoes de path para o uploadworker processar 
	go func() {
		for x := 0; x < len(upload.Paths); x++ {
			inputChan <- x
		}
		close(inputChan)
	}()

	//ler canal de retorno
	for r := range returnChan {
		//em caso de erro ou operacao completa
		if r != "" {
			//qualquer valor em doneUpload cancela o processo de upload, seja erro ou processo de upload finalizado
			doneUpload <- r
			break
		}
	}

	return nil
}

// worker que verifica o caminho do arquivo para upload, e invoca metodo para upload.
// inputChan contem a posicao dao arquivo para upload,
// returnChan canal de resposta do processo de upload, indica erro ou operacao completada
//func (upload *VideoUpload) uploadWorker(inputChan chan int, returnChan chan string, uploadClient *storage.Client, ctx context.Context) {
func (upload *VideoUpload) uploadWorker(inputChan chan int, returnChan chan string, uploadClient *s3.S3) {
	
	for x := range inputChan {
		//err := upload.UploadObject(upload.Paths[x],uploadClient,ctx)
		err := upload.UploadToS3(upload.Paths[x],uploadClient)
		
		if err != nil {
			//inserir o caminho do arquivo que deu erro
			upload.Errors = append(upload.Errors, upload.Paths[x])
			log.Printf("Error to upload file: %v. Error: %v", upload.Paths[x], err)
			returnChan <- err.Error()
		}

		//indica que nao houve erro no upload
		returnChan <- ""
	}

	returnChan <- "upload completed"
}

// carrega o caminho dos arquivos para fazer upload
func (upload *VideoUpload) loadPaths() error {
	//para cada arquivo do repositorio executa a funcao
	err := filepath.Walk(upload.VideoPath, func(path string, info fs.FileInfo, err error) error {
		//se for um arquivo salva em paths
		if !info.IsDir() {
			upload.Paths = append(upload.Paths, path)
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

// acesso ao client para utilizar a bucket
func getClientUpload() (*storage.Client, context.Context,error) {
	ctx := context.Background()

	client,err := storage.NewClient(ctx)
	if err != nil {
		return nil,nil,err
	}

	return client, ctx,nil
}

func getUploadClientS3() (*s3.S3,error) {
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
		return nil,err
	}
	s3Client := s3.New(sess)
	return s3Client,nil
}

func (upload *VideoUpload) UploadToS3(objPath string, s3Client *s3.S3,) error {
	//separar arquivo e caminho do arquivo, path[0] = caminho, path[1] arquivo de video
	path := strings.Split(objPath, os.Getenv("LOCALSTORAGEPATH"))

	//abrir o arquivo
	f,err := os.Open(objPath)
	if err != nil {
		return err
	}
	defer f.Close()

	_,err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(os.Getenv("BUCKET_NAME_AWS")),
		Key: aws.String(path[1]),
		Body: f,
	})
	if err != nil {
		return err
	}

	return nil
}
