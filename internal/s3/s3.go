package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"

	"es-to-s3-dumper/internal/config"
	"es-to-s3-dumper/internal/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3Config "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type FileUploadInput struct {
	Data     []byte
	FileName string
	S3Bucket string
	S3Path   string
	S3Client *s3.Client
}

func NewClient(cfg *config.Config) (*s3.Client, error) {
	log := logger.Logger
	var s3Cfg aws.Config
	var err error

	// are we using credential file or args
	if cfg.S3AccessKey != "" && cfg.S3SecretKey != "" {
		creds := credentials.NewStaticCredentialsProvider(cfg.S3AccessKey, cfg.S3SecretKey, "")
		s3Cfg, err = s3Config.LoadDefaultConfig(context.TODO(), s3Config.WithCredentialsProvider(creds), s3Config.WithRegion(cfg.S3Region))
	} else {
		log.Debugln("Using aws credentials file as no access or secret key specified")
		s3Cfg, err = s3Config.LoadDefaultConfig(context.TODO(), s3Config.WithRegion(cfg.S3Region))
	}

	if err != nil {
		return nil, err
	}

	s3Client := s3.NewFromConfig(s3Cfg)
	return s3Client, nil
}

func gzipData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func UploadFile(input FileUploadInput) error {
	log := logger.Logger
	gzippedData, err := gzipData(input.Data)
	if err != nil {
		log.Errorf("Error compressing data: %s", err)
		return err
	}

	key := fmt.Sprintf("%s%s", input.S3Path, input.FileName)
	_, err = input.S3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(input.S3Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(gzippedData),
	})
	if err != nil {
		return err
	}

	return nil
}
