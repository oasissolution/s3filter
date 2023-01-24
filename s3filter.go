package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/exp/slices"
)

type Record struct {
	Id    int64     `json:"id"`
	Time  time.Time `json:"time"`
	Words []string  `json:"words"`
}

// Arguments variables
var (
	S3URI    *string
	WithID   *int64
	FromTime time.Time
	ToTime   time.Time
	WithWord *string
)

/*
| Name | Required | Description |
| ---- | -------- | ----------- |
| `-input` | Yes | An S3 URI (`s3://{bucket}/{key}`) that refers to the source object to be filtered. |
| `-with-id` | No | An integer that contains the `id` of a JSON object to be selected. |
| `-from-time` | No | An RFC3339 timestamp that represents the earliest `time` of a JSON object to be selected. |
| `-to-time` | No | An RFC3339 timestamp that represents the latest `time` of JSON object to be selected. |
| `-with-word` | No | A string containing a word that must be contained in `words` of a JSON objec to be selected. |
*/
func processArgs() {
	S3URI = flag.String("input", "", "An S3 URI (`s3://{bucket}/{key}`) that refers to the source object to be filtered.")
	WithID = flag.Int64("with-id", 0, "An integer that contains the `id` of a JSON object to be selected.")
	WithWord = flag.String("with-word", "", "A string containing a word that must be contained in `words` of a JSON objec to be selected.")
	fromTime := flag.String("from-time", "", "An RFC3339 timestamp that represents the earliest `time` of a JSON object to be selected.")
	toTime := flag.String("to-time", "", "An RFC3339 timestamp that represents the latest `time` of JSON object to be selected.")
	flag.Parse()

	//`-input` flag is missing then print usage message
	if *S3URI == "" {
		fmt.Println("| Name | Required | Description |")
		fmt.Println("| ---- | -------- | ----------- |")
		fmt.Println("| `-input` | Yes | An S3 URI (`s3://{bucket}/{key}`) that refers to the source object to be filtered. |")
		fmt.Println("| `-with-id` | No | An integer that contains the `id` of a JSON object to be selected. |")
		fmt.Println("| `-from-time` | No | An RFC3339 timestamp that represents the earliest `time` of a JSON object to be selected. |")
		fmt.Println("| `-to-time` | No | An RFC3339 timestamp that represents the latest `time` of JSON object to be selected. |")
		fmt.Println("| `-with-word` | No | A string containing a word that must be contained in `words` of a JSON objec to be selected. |")
		fmt.Println("Docker Command:")
		fmt.Println("docker run --rm -e AWS_REGION -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY s3filter -input s3://maf-sample-data/1k.ndjson.gz -from-time=2000-01-01T00:00:00Z -to-time=2001-01-01T00:00:00Z")
		os.Exit(1)
	}

	var err error
	if *fromTime != "" {
		FromTime, err = time.Parse(time.RFC3339, *fromTime)
		if err != nil {
			fmt.Println("Error while parsing the time :", err)
		}
	}

	if *toTime != "" {
		ToTime, err = time.Parse(time.RFC3339, *toTime)
		if err != nil {
			fmt.Println("Error while parsing the time :", err)
		}
	}
}

// parse bytes array to ndJson and filter based on criteria
func filter(src []byte) error {
	decorder := json.NewDecoder(bytes.NewReader(src))
	for {
		// Decode one JSON document.
		var record Record
		err := decorder.Decode(&record)

		if err != nil {
			// io.EOF is expected at end of stream.
			if err != io.EOF {
				return err
			}
			break
		}

		// Filter
		if *WithID != 0 && *WithID != record.Id {
			continue
		}

		if !FromTime.IsZero() && record.Time.Before(FromTime) {
			continue
		}

		if !ToTime.IsZero() && record.Time.After(ToTime) {
			continue
		}

		if *WithWord != "" && !slices.Contains(record.Words, *WithWord) {
			continue
		}

		//print struct as json string
		s, err := json.Marshal(record)
		if err == nil {
			fmt.Println(string(s))
		}
	}
	return nil
}

// Extract *.gz file in the same directory
func gzUnzip(gzBytes []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(gzBytes))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	buf := new(bytes.Buffer)
	if _, err = io.Copy(buf, reader); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Print error messages and exit application
func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func main() {

	//parse arguments
	processArgs()

	//parse s3URI for Bucket and Key
	s3Info := strings.Split((*S3URI)[5:len(*S3URI)], "/")
	if len(s3Info) != 2 {
		exitErrorf("Failed to parse S3 URI %q \n", *S3URI)
	}

	s3_bucket := s3Info[0]
	s3_key := s3Info[1]

	// Create Session
	sess, err := session.NewSession()
	if err != nil {
		exitErrorf("Failed to create new session. %v\n", err)
		return
	}

	//Create a downloader with the session and custom options
	downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 //64MB per part
		d.Concurrency = 6
	})

	//download file from AWS S3 to memory
	buff := &aws.WriteAtBuffer{}
	_, err = downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(s3_bucket),
		Key:    aws.String(s3_key),
	})

	if err != nil {
		exitErrorf("Unable to download file %v", err)
	}

	//Extract *.gz
	ndJsonBytes, err := gzUnzip(buff.Bytes())
	if err != nil {
		exitErrorf("Unable to unzip file %v", err)
	}

	//Decode ndjson from bytes and print record that matches with criteria
	err = filter(ndJsonBytes)
	if err != nil {
		exitErrorf("Unable to decode ndJson file %v", err)
	}
}
