package minioconnector

import (
	"github.com/google/uuid"
	"github.com/minio/minio-go"
	log "github.com/sirupsen/logrus"
	"io"
)

const NO_SSL = false

var minioHost string
var accessKey string
var secretKey string
var bucketName string

func Init(
	minioHostArg string,
	accessKeyArg string,
	secretKeyArg string,
	bucketNameArg string) {

	minioHost = minioHostArg
	accessKey = accessKeyArg
	secretKey = secretKeyArg
	bucketName = bucketNameArg
}

func DownloadFile(objectName string) (string, error) {
	outputFilePath := "/tmp/downloaded" + uuid.New().String() + ".jpg"

	client, err := minio.New(
		minioHost,
		accessKey,
		secretKey,
		NO_SSL)
	if err != nil {
		return "", err
	}

	err = client.FGetObject(bucketName, objectName, outputFilePath, minio.GetObjectOptions{})

	if err != nil {
		return "", err
	}

	return outputFilePath, nil
}

func GetObject(objectName string) (*minio.Object, error) {
	client, err := getClient()
	if err != nil {
		return nil, err
	}

	object, err := client.GetObject(bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	return object, nil
}

func UploadFileStream(reader io.Reader, uploadName string) error {
	client, err := getClient()
	if err != nil {
		return err
	}
	_, err = client.PutObject(bucketName, uploadName, reader, -1, minio.PutObjectOptions{ContentType: "img/jpeg"})
	return err
}

func UploadFile(filePath string) (string, error) {
	fileName := uuid.New().String()
	return UploadFileWithName(filePath, fileName)
}

func UploadFileWithName(filePath string, uploadName string) (string, error) {
	client, err := getClient()
	if err != nil {
		return "", err
	}

	log.Printf("%#v\n", client)

	err = createBucketIfNotExists(client)
	if err != nil {
		return "", nil
	}

	n, err := client.FPutObject(bucketName, uploadName, filePath, minio.PutObjectOptions{ContentType: "img/jpeg"})
	if err != nil {
		return "", err
	}

	log.WithField("uploadName", uploadName).Info("Successfully uploaded %s of size %d\n", uploadName, n)

	return uploadName, nil
}

func createBucketIfNotExists(client *minio.Client) error {
	bucketExists, err := client.BucketExists(bucketName)
	if err != nil {
		return err
	}

	if !bucketExists {
		err := createBucket(client, bucketName)
		if err != nil {
			return err
		}
		log.WithField("bucketname", bucketName).Info("successfully created bucket")
	} else {
		log.WithField("bucketname", bucketName).Debug("bucket already exists")
	}

	return nil
}

func createBucket(client *minio.Client, bucketName string) error {
	return client.MakeBucket(bucketName, "us-east-1")
}

func getClient() (*minio.Client, error) {
	return minio.New(
		minioHost,
		accessKey,
		secretKey,
		NO_SSL)
}
