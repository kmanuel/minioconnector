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
	UploadFile(filePath string) (string, error)
	UploadFileWithName(filePath string, uploadName string) (string, error)
}

type minioServiceImpl struct {
	minioHost  string
	accessKey  string
	secretKey  string
	bucketName string
}

func NewMinioService(
	minioHostArg string,
	accessKeyArg string,
	secretKeyArg string,
	bucketNameArg string) *MinioService {

	var service MinioService
	service = minioServiceImpl{
		minioHost:  minioHostArg,
		accessKey:  accessKeyArg,
		secretKey:  secretKeyArg,
		bucketName: bucketNameArg,
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

	err = m.createBucketIfNotExists(client)
	if err != nil {
		return "", err
	}

	err = client.FGetObject(m.bucketName, objectName, outputFilePath, minio.GetObjectOptions{})

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

	err = m.createBucketIfNotExists(client)
	if err != nil {
		return nil, err
	}

	object, err := client.GetObject(m.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	return object, nil
}

func (m minioServiceImpl) UploadFile(filePath string) (string, error) {
	fileName := uuid.New().String()
	return m.UploadFileWithName(filePath, fileName)
}

func (m minioServiceImpl) UploadFileStream(reader io.Reader, uploadName string) error {
	client, err := m.getClient()
	if err != nil {
		return err
	}

	err = m.createBucketIfNotExists(client)
	if err != nil {
		return err
	}

	_, err = client.PutObject(m.bucketName, uploadName, reader, -1, minio.PutObjectOptions{ContentType: "img/jpeg"})
	return err
}

func (m minioServiceImpl) UploadFileWithName(filePath string, uploadName string) (string, error) {
	client, err := m.getClient()
	if err != nil {
		return "", err
	}

	err = m.createBucketIfNotExists(client)
	if err != nil {
		return "", nil
	}

	n, err := client.FPutObject(m.bucketName, uploadName, filePath, minio.PutObjectOptions{ContentType: "img/jpeg"})
	if err != nil {
		return "", err
	}

	log.WithField("uploadName", uploadName).Info("Successfully uploaded %s of size %d\n", uploadName, n)

	return uploadName, nil
}

func (m minioServiceImpl) createBucketIfNotExists(client *minio.Client) error {
	bucketExists, err := client.BucketExists(m.bucketName)
	if err != nil {
		return err
	}

	if !bucketExists {
		err := createBucket(client, m.bucketName)
		if err != nil {
			return err
		}
		log.WithField("bucketname", m.bucketName).Info("successfully created bucket")
	}

	return nil
}

func createBucket(client *minio.Client, bucketName string) error {
	return client.MakeBucket(bucketName, "us-east-1")
}

func (m minioServiceImpl) getClient() (*minio.Client, error) {
	return minio.New(
		m.minioHost,
		m.accessKey,
		m.secretKey,
		NO_SSL)
}
