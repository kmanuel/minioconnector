package minioconnector

import (
	"github.com/google/uuid"
	"github.com/minio/minio-go"
	log "github.com/sirupsen/logrus"
	"io"
)

const NO_SSL = false

type MinioService interface {
	DownloadFile(objectName string) (string, error)
	GetObject(objectName string) (*minio.Object, error)
	UploadFileStream(reader io.Reader, uploadName string) error
	UploadFileWithName(filePath string, uploadName string) (string, error)
}

type minioServiceImpl struct {
	minioHost    string
	accessKey    string
	secretKey    string
	inputBucket  string
	outputBucket string
}

func NewMinioService(
	minioHostArg string,
	accessKeyArg string,
	secretKeyArg string,
	inputBucketArg string,
	outputBucketArg string) *MinioService {

	var service MinioService
	service = minioServiceImpl{
		minioHost:    minioHostArg,
		accessKey:    accessKeyArg,
		secretKey:    secretKeyArg,
		inputBucket:  inputBucketArg,
		outputBucket: outputBucketArg,
	}
	return &service
}

func (m minioServiceImpl) DownloadFile(objectName string) (string, error) {
	outputFilePath := "/tmp/downloaded" + uuid.New().String() + ".jpg"

	client, err := minio.New(
		m.minioHost,
		m.accessKey,
		m.secretKey,
		NO_SSL)
	if err != nil {
		return "", err
	}

	err = m.createBucketIfNotExists(m.inputBucket, client)
	if err != nil {
		return "", err
	}

	err = client.FGetObject(m.inputBucket, objectName, outputFilePath, minio.GetObjectOptions{})

	if err != nil {
		return "", err
	}

	return outputFilePath, nil
}

func (m minioServiceImpl) GetObject(objectName string) (*minio.Object, error) {
	client, err := m.getClient()
	if err != nil {
		return nil, err
	}

	err = m.createBucketIfNotExists(m.inputBucket, client)
	if err != nil {
		return nil, err
	}

	object, err := client.GetObject(m.inputBucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	return object, nil
}

func (m minioServiceImpl) UploadFileStream(reader io.Reader, uploadName string) error {
	client, err := m.getClient()
	if err != nil {
		return err
	}

	err = m.createBucketIfNotExists(m.outputBucket, client)
	if err != nil {
		return err
	}

	_, err = client.PutObject(m.outputBucket, uploadName, reader, -1, minio.PutObjectOptions{ContentType: "img/jpeg"})
	return err
}

func (m minioServiceImpl) UploadFileWithName(filePath string, fileName string) (string, error) {
	client, err := m.getClient()
	if err != nil {
		return "", err
	}

	err = m.createBucketIfNotExists(m.outputBucket, client)
	if err != nil {
		return "", nil
	}

	n, err := client.FPutObject(m.outputBucket, fileName, filePath, minio.PutObjectOptions{ContentType: "img/jpeg"})
	if err != nil {
		return "", err
	}

	log.WithField("fileName", fileName).Info("Successfully uploaded %s of size %d\n", fileName, n)

	return fileName, nil
}

func (m minioServiceImpl) createBucketIfNotExists(bucketName string, client *minio.Client) error {
	bucketExists, err := client.BucketExists(bucketName)
	if err != nil {
		return err
	}

	if !bucketExists {
		err := createBucket(bucketName, client)
		if err != nil {
			return err
		}
		log.WithField("bucketname", bucketName).Info("successfully created bucket")
	}

	return nil
}

func createBucket(bucketName string, client *minio.Client) error {
	return client.MakeBucket(bucketName, "us-east-1")
}

func (m minioServiceImpl) getClient() (*minio.Client, error) {
	return minio.New(
		m.minioHost,
		m.accessKey,
		m.secretKey,
		NO_SSL)
}
