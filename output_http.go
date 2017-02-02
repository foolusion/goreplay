package main

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/buger/gor/proto"
	"github.com/prometheus/client_golang/prometheus"
)

const initialDynamicWorkers = 10

type response struct {
	payload       []byte
	uuid          []byte
	roundTripTime int64
	startedAt     int64
}

// HTTPOutputConfig struct for holding http output configuration
type HTTPOutputConfig struct {
	redirectLimit int

	stats   bool
	workers int

	elasticSearch string

	Timeout      time.Duration
	OriginalHost bool
	BufferSize   int

	Debug bool

	TrackResponses bool
}

// HTTPOutput plugin manage pool of workers which send request to replayed server
// By default workers pool is dynamic and starts with 10 workers
// You can specify fixed number of workers using `--output-http-workers`
type HTTPOutput struct {
	// Keep this as first element of struct because it guarantees 64bit
	// alignment. atomic.* functions crash on 32bit machines if operand is not
	// aligned at 64bit. See https://github.com/golang/go/issues/599
	activeWorkers int64

	address string
	limit   int
	queue   chan []byte

	responses chan response

	needWorker chan int

	config *HTTPOutputConfig

	queueStats *GorStat

	elasticSearch *ESPlugin
}

// NewHTTPOutput constructor for HTTPOutput
// Initialize workers
func NewHTTPOutput(address string, config *HTTPOutputConfig) io.Writer {
	o := new(HTTPOutput)

	o.address = address
	o.config = config

	if o.config.stats {
		o.queueStats = NewGorStat("output_http")
	}

	o.queue = make(chan []byte, 1000)
	o.responses = make(chan response, 1000)
	o.needWorker = make(chan int, 1)

	// Initial workers count
	if o.config.workers == 0 {
		o.needWorker <- initialDynamicWorkers
	} else {
		o.needWorker <- o.config.workers
	}

	if o.config.elasticSearch != "" {
		o.elasticSearch = new(ESPlugin)
		o.elasticSearch.Init(o.config.elasticSearch)
	}

	go o.workerMaster()

	return o
}

func (o *HTTPOutput) workerMaster() {
	for {
		newWorkers := <-o.needWorker
		for i := 0; i < newWorkers; i++ {
			go o.startWorker()
		}

		// Disable dynamic scaling if workers poll fixed size
		if o.config.workers != 0 {
			return
		}
	}
}

func (o *HTTPOutput) startWorker() {
	client := NewHTTPClient(o.address, &HTTPClientConfig{
		FollowRedirects:    o.config.redirectLimit,
		Debug:              o.config.Debug,
		OriginalHost:       o.config.OriginalHost,
		Timeout:            o.config.Timeout,
		ResponseBufferSize: o.config.BufferSize,
	})

	deathCount := 0

	atomic.AddInt64(&o.activeWorkers, 1)

	for {
		select {
		case data := <-o.queue:
			o.sendRequest(client, data)
			deathCount = 0
		case <-time.After(time.Millisecond * 100):
			// When dynamic scaling enabled workers die after 2s of inactivity
			if o.config.workers == 0 {
				deathCount++
			} else {
				continue
			}

			if deathCount > 20 {
				workersCount := atomic.LoadInt64(&o.activeWorkers)

				// At least 1 startWorker should be alive
				if workersCount != 1 {
					atomic.AddInt64(&o.activeWorkers, -1)
					return
				}
			}
		}
	}
}

func (o *HTTPOutput) Write(data []byte) (n int, err error) {
	if !isRequestPayload(data) {
		return len(data), nil
	}

	buf := make([]byte, len(data))
	copy(buf, data)

	o.queue <- buf

	if o.config.stats {
		o.queueStats.Write(len(o.queue))
	}

	if o.config.workers == 0 {
		workersCount := atomic.LoadInt64(&o.activeWorkers)

		if len(o.queue) > int(workersCount) {
			o.needWorker <- len(o.queue)
		}
	}

	return len(data), nil
}

func (o *HTTPOutput) Read(data []byte) (int, error) {
	resp := <-o.responses

	Debug("[OUTPUT-HTTP] Received response:", string(resp.payload))

	header := payloadHeader(ReplayedResponsePayload, resp.uuid, resp.roundTripTime, resp.startedAt)
	copy(data[0:len(header)], header)
	copy(data[len(header):], resp.payload)

	return len(resp.payload) + len(header), nil
}

var responseTimes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "nordstrom",
	Subsystem: "gor",
	Name:      "response_time_nanoseconds",
	Help:      "The response time in nanoseconds of duplicated request made by gor.",
	Buckets:   prometheus.LinearBuckets(10000000, 10000000, 6),
}, []string{"status"})

var responseFailures = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "nordstrom",
	Subsystem: "gor",
	Name:      "request_errors_count",
	Help:      "The number of errors that occur when making the duplicated request.",
})

func (o *HTTPOutput) sendRequest(client *HTTPClient, request []byte) {
	meta := payloadMeta(request)
	if len(meta) < 2 {
		return
	}

	body := payloadBody(request)
	if !proto.IsHTTPPayload(body) {
		return
	}

	// build the request
	req, err := http.ReadRequest(bufio.NewReader(bytes.NewBuffer(body)))
	if err != nil {
		log.Printf("could not read request: %s", err.Error())
		return
	}
	reqURL, err := url.ParseRequestURI(req.RequestURI)
	if err != nil {
		log.Printf("could not parse request uri: %s", err.Error())
		return
	}
	req.RequestURI = ""
	reqURL.Host = Settings.outputHTTP[0]
	reqURL.Scheme = "http"
	req.URL = reqURL
	if err != nil {
		log.Printf("could not parse request url: %s", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	rDuration := time.Since(start)
	if err != nil {
		responseFailures.Inc()
		log.Printf("could not complete do request: %s", err.Error())
		return
	}
	defer resp.Body.Close()
	_, err = io.Copy(ioutil.Discard, resp.Body)
	if err != nil {
		log.Printf("error reading resp body: %s", err.Error())
	}

	if req != nil && req.URL.Path == "/" {
		responseTimes.WithLabelValues(strconv.Itoa(resp.StatusCode)).Observe(float64(rDuration))
	}
}

func (o *HTTPOutput) String() string {
	return "HTTP output: " + o.address
}
