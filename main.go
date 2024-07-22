package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"es-to-s3-dumper/internal/config"
	"es-to-s3-dumper/internal/elasticsearch"
	"es-to-s3-dumper/internal/logger"
	dumperS3 "es-to-s3-dumper/internal/s3"

	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
)

type Document struct {
	Index   string          `json:"_index"`
	Id      string          `json:"_id"`
	Routing *string         `json:"_routing,omitempty"`
	Source  json.RawMessage `json:"_source"`
}

func worker(id int, wg *sync.WaitGroup, uploadJobs chan dumperS3.FileUploadInput, uploadErrors chan error) {
	log := logger.Logger.WithFields(
		logrus.Fields{
			"worker": id,
		},
	)
	defer wg.Done()
	for item := range uploadJobs {
		log.Debugf("Started job for worker %d on file %s", id, item.FileName)
		err := upload(item)
		if err != nil {
			uploadErrors <- err
		}
	}
}

func main() {
	log := logger.Logger

	// setup
	cfg := config.LoadConfig()
	if cfg.Debug {
		log.SetLevel(logrus.DebugLevel)
	}
	esClient, err := elasticsearch.NewESClient(cfg)
	if err != nil {
		log.Fatalf("Error creating elasticsearch client: %s\n", err)
	}
	s3Client, err := dumperS3.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating s3 client: %s\n", err)
	}

	// prep for scroll and prepare channel for queuing uploads
	ctx := context.Background()
	query := elastic.NewMatchAllQuery()
	scroll := esClient.Scroll(cfg.IndexName).Query(query)
	scroll.Scroll(cfg.ScrollTimeout)
	scroll.Size(cfg.ScrollSize)
	var buffer bytes.Buffer
	fileIndex := 0
	docCount := 0
	totalDocCount := 0

	// upload buffer
	uploadChan := make(chan dumperS3.FileUploadInput, cfg.MaxUploads)
	uploadErrorChan := make(chan error)
	var wg sync.WaitGroup
	go func() {
		for err := range uploadErrorChan {
			log.Infof("Error uploading file to S3: %s", err)
		}
	}()

	log.Debugf("Starting %d upload workers", cfg.MaxUploads)
	for w := 1; w <= cfg.MaxUploads; w++ {
		wg.Add(1)
		go worker(w, &wg, uploadChan, uploadErrorChan)
	}

	for {
		res, err := scroll.Do(ctx)
		if err == io.EOF {

			// finish up the remainder
			if buffer.Len() > 0 {
				log.Infof("Shipping final payload with %d docs for final split of %d", docCount, fileIndex)
				fileName := fmt.Sprintf("%s_s_%d.json.gz", cfg.IndexName, fileIndex)
				jobBuffer := make([]byte, buffer.Len())
				copy(jobBuffer, buffer.Bytes())
				uploadFileInput := dumperS3.FileUploadInput{
					Data:     jobBuffer,
					FileName: fileName,
					S3Bucket: cfg.S3Bucket,
					S3Path:   cfg.S3Path,
					S3Client: s3Client,
				}
				uploadChan <- uploadFileInput
			}
			log.Infof("Met end of scroll/index, finished at %d total docs across %d files", totalDocCount, fileIndex)
			break
		}
		if err != nil {
			log.Errorf("Error scrolling through elasticsearch: %s\n", err)
		}
		for _, searchHit := range res.Hits.Hits {

			// map doc, marshal to json, add to buffer
			doc := Document{
				Index:  searchHit.Index,
				Id:     searchHit.Id,
				Source: searchHit.Source,
			}
			if searchHit.Routing != "" {
				doc.Routing = &searchHit.Routing
			}
			jsonData, err := json.Marshal(doc)
			if err != nil {
				log.Warnln("Could not marshal to JSON, skipping document")
				continue
			}
			buffer.WriteString(string(jsonData) + "\n")

			// increment counts
			docCount++
			totalDocCount++

			// check limits pass to uploads
			if buffer.Len() >= cfg.MaxFileSize || docCount >= cfg.MaxDocs {
				fileName := fmt.Sprintf("%s_s_%d.json.gz", cfg.IndexName, fileIndex)
				jobBuffer := make([]byte, buffer.Len())
				copy(jobBuffer, buffer.Bytes())
				uploadFileInput := dumperS3.FileUploadInput{
					Data:     jobBuffer,
					FileName: fileName,
					S3Bucket: cfg.S3Bucket,
					S3Path:   cfg.S3Path,
					S3Client: s3Client,
				}
				if buffer.Len() >= cfg.MaxFileSize {
					log.Infof("Met max file size limit of %d, uploading file split %d", cfg.MaxFileSize, fileIndex)
				} else {
					log.Infof("Met max doc count limit of %d, uploading file split %d", cfg.MaxDocs, fileIndex)
				}
				uploadChan <- uploadFileInput
				buffer.Reset()
				fileIndex++
				docCount = 0
			}

		}
	}

	// wait for completion
	close(uploadChan)
	wg.Wait()

	// should be done with errors
	close(uploadErrorChan)

	log.Infoln("Uploads complete")
}

func upload(fileInput dumperS3.FileUploadInput) error {
	log := logger.Logger
	err := dumperS3.UploadFile(fileInput)
	if err != nil {
		return err
	}
	log.Infof("Uploaded file %s to S3 bucket %s", fileInput.FileName, fileInput.S3Bucket)
	return nil
}
