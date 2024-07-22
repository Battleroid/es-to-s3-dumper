package elasticsearch

import (
	"time"

	"es-to-s3-dumper/internal/config"
	"es-to-s3-dumper/internal/logger"

	"github.com/olivere/elastic/v7"
)

func NewESClient(cfg *config.Config) (*elastic.Client, error) {
	log := logger.Logger
	options := []elastic.ClientOptionFunc{
		elastic.SetURL(cfg.EsUrl),
		elastic.SetSniff(false),
		elastic.SetHealthcheckTimeout(time.Duration(cfg.MaxTimeout) * time.Second),
	}
	log.Debugf("Using elasticsearch health check timeout of %d", cfg.MaxTimeout)

	if cfg.EsUsername != "" && cfg.EsPassword != "" {
		log.Debugf("Setting elasticsearch basic auth to %s:xxx", cfg.EsUsername)
		options = append(options, elastic.SetBasicAuth(cfg.EsUsername, cfg.EsPassword))
	} else {
		log.Warnln("No basic authentication specified for elasticsearch!")
	}

	client, err := elastic.NewClient(options...)
	if err != nil {
		return nil, err
	}

	return client, nil
}
