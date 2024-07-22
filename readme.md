# es-to-s3-dumper

For dumping an Elasticsearch index contents to S3. Pretty straight forward.

## why

We were using elasticdump, which works well, but for long running dumps it has an occasionally memory leak and requires us to slowly up the limits in kubernetes to avoid OOM kills. This is simpler and pretty stable on memory usage AFAICT.

## usage

```
$ ./es-to-s3-dumper --help
Usage of ./es-to-s3-dumper:
  -debug
    	Enable debug logging
  -es-password string
    	Basic auth password
  -es-url string
    	Elasticsearch URL (default "https://localhost:9200")
  -es-username string
    	Basic auth username
  -index-name string
    	Index name to extract
  -max-docs int
    	Maximum docs before splitting (default 1000000)
  -max-file-size int
    	Maximum file size before splitting (default 33554432)
  -max-timeout int
    	Timeout for http requests to Elasticsearch (default 60)
  -max-uploads int
    	Max background uploads to perform (default 2)
  -s3-access-key string
    	S3 access key
  -s3-bucket string
    	S3 bucket to dump objects into
  -s3-path string
    	S3 path to dump objects into
  -s3-region string
    	Region S3 bucket resides in (default "us-west-2")
  -s3-secret-key string
    	S3 secret key
  -scroll-size int
    	Size of scroll (default 10000)
  -scroll-timeout string
    	Scroll timeout (default "5m")
```
