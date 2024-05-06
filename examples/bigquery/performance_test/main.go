// https://github.com/myzhan/boomer/blob/master/_examples/http/client.go
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"flag"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	// locust 2.0 support is not tagged yet, so `go get -u github.com/myzhan/boomer@master`` is used
	"github.com/myzhan/boomer"
)

// This is a tool like Apache Benchmark a.k.a "ab".
// It doesn't implement all the features supported by ab.

//nolint:gochecknoglobals
var (
	client   *http.Client
	postBody []byte
)

//nolint:gochecknoglobals
var verbose bool

//nolint:gochecknoglobals
var (
	method      string
	url         string
	timeout     int
	postFile    string
	contentType string
)

//nolint:gochecknoglobals
var (
	disableCompression bool
	disableKeepalive   bool
)

const size = 1_000_000

func worker() {
	key := strconv.Itoa(rand.Intn(size)) //nolint:gosec
	url := url + key

	request, err := http.NewRequestWithContext(context.Background(), method, url, bytes.NewBuffer(postBody))
	if err != nil {
		log.Fatalf("%v\n", err)
	}

	request.Header.Set("Content-Type", contentType)

	startTime := time.Now()
	response, err := client.Do(request)
	elapsed := time.Since(startTime)

	if err != nil { //nolint:nestif
		if verbose {
			log.Printf("%v\n", err)
		}

		boomer.RecordFailure("http", "error", 0.0, err.Error())
	} else {
		boomer.RecordSuccess("http", strconv.Itoa(response.StatusCode),
			elapsed.Nanoseconds()/int64(time.Millisecond), response.ContentLength)

		if verbose {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				log.Printf("%v\n", err)
			} else {
				log.Printf("Status Code: %d\n", response.StatusCode)
				log.Println(string(body))
			}
		} else {
			_, err = io.Copy(io.Discard, response.Body)
			if err != nil {
				log.Fatalf("%v\n", err)
			}
		}

		response.Body.Close()
	}
}

//nolint:funlen
func main() {
	flag.StringVar(&method, "method", "GET", "HTTP method, one of GET, POST")
	flag.StringVar(&url, "url", "", "URL")
	flag.IntVar(&timeout, "timeout", 10, "Seconds to max. wait for each response") //nolint:gomnd
	flag.StringVar(&postFile, "post-file", "", "File containing data to POST. Remember also to set --content-type")
	flag.StringVar(&contentType, "content-type", "text/plain", "Content-type header")

	flag.BoolVar(&disableCompression, "disable-compression", false, "Disable compression")
	flag.BoolVar(&disableKeepalive, "disable-keepalive", false, "Disable keepalive")

	flag.BoolVar(&verbose, "verbose", false, "Print debug log")

	flag.Parse()

	log.Printf(`HTTP benchmark is running with these args:
method: %s
url: %s
timeout: %d
post-file: %s
content-type: %s
disable-compression: %t
disable-keepalive: %t
verbose: %t`, method, url, timeout, postFile, contentType, disableCompression, disableKeepalive, verbose)

	if url == "" {
		log.Fatalln("--url can't be empty string, please specify a URL that you want to test.")
	}

	if method != "GET" && method != "POST" {
		log.Fatalln("HTTP method must be one of GET, POST.")
	}

	if method == "POST" {
		if postFile == "" {
			log.Fatalln("--post-file can't be empty string when method is POST")
		}

		tmp, err := os.ReadFile(postFile)
		if err != nil {
			log.Fatalf("%v\n", err)
		}

		postBody = tmp
	}

	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 2000 //nolint:forcetypeassert
	tr := &http.Transport{                                             //nolint:varnamelen
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, //nolint:gosec
		},
		MaxIdleConnsPerHost: 2000, //nolint:gomnd
		DisableCompression:  disableCompression,
		DisableKeepAlives:   disableKeepalive,
	}
	client = &http.Client{
		Transport: tr,
		Timeout:   time.Duration(timeout) * time.Second,
	}

	task := &boomer.Task{
		Name:   "worker",
		Weight: 10, //nolint:gomnd
		Fn:     worker,
	}

	boomer.Run(task)
}
