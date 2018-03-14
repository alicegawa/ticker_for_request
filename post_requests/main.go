package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//"github.com/influxdata/influxdb/client/v2"
	"github.com/influxdata/influxdb-comparisons/util/report"
	//"github.com/pkg/profile"
	"github.com/valyala/fasthttp"
)


// data scanner from os.Stdin
// TODO: initialize bufPool (sync.Pool) anywhere else
func scan(itemsPerBatch int) (int64, int64) {
	buf := bufPool.Get().(*bytes.Buffer)
	var n int
	var itemsRead, bytesRead int64
	newline := []byte("\n")
	var deadline time.Time
	// TODO: timeLimit may be the global var. define it
	if timeLimit >= 0 {
		deadline = time.Now().Add(timeLimit)
	}
	var batchItemCount uint64

	scanner := bufio.NewScanner(bufio.NewReaderSize(os.Stdin, 4 * 1024 * 1024))
outer:
	for scanner.Scan() {
		// TODO: itemLimit may be the global var. define it.
		if itemsRead == itemLimit {
			break
		}
		itemsRead++
		batchItemCount++
		buf.Write(scanner.Bytes())
		buf.Write(newline)

		n++
		// write buffers to batchChan and reset counter
		if n >= itemsPerBatch {
			// TODO: unknown implementation
			atomic.AddUint64(&progressIntervalItems, batchItemCount)
			batchItemCount = 0

			bytesRead += int64(buf.Len())
			batchChan <- buf
			buf = bufPool.Get().(*bytes.Buffer)
			n = 0

			if timeLimit >= 0 && time.Now().After(deadline) {
				break outer
			}
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading input: %s", err.Error())
	}

	// finished reading input, make sure last batch goes out
	if n > 0 {
		batchChan <- buf
	}
	// TODO: inputDone has not yet be implemented. define it in main func 
	close(inputDone)

	return itemsRead, bytesRead
}

func processBatches(w *HTTPWriter, backoffSrc chan bool, backoffDst chan struct{}, telemetrySink chan *report.Point, telemetryWorkerLabel string) {
	var batchesSeen int64
	ticker := time.NewTicker(1 * time.Second)

	for batch := range batchChan {
		batchesSeen++

		var bodySize int
		ts := time.Now().UnixNano()

		if doLoad {
			var err error
			for {
				if useGzip {
					compressedBatch := bufPool.Get().(*bytes.Buffer)
					fasthttp.WriteGzip(compressedBatch, batch.Bytes())
					bodySize = len(compressedBatch.Bytes())
					_, err = w.WriteLineProtocol(compressedBatch.Bytes(), true)
					compressedBatch.Reset()
					bufPool.Put(compressedBatch)
				} else {
					bodySize = len(batch.Bytes())
					_, err = w.WriteLineProtocol(batch.Bytes(), false)
				}

				if err == BackoffError {
					backoffSrc <- true
					time.Sleep(backoff)
				} else {
					backoffSrc <- false
					break
				}
			}
			if err != nil {
				log.Fatalf("Error writing: %s\n", err.Error())
			}
		}

		lagMillis := float64(time.Now().UnixNano() - ts ) / 1e6

		batch.Reset()
		bufPool.Put(batch)
		
		// change from the original definition in influxdb-comparison
		if telemetrySink != nil {
			p := report.GetPointFromGlobalPool()
			p.Init("benchmark_write", time.Now().UnixNano())
			p.AddTag("worker_id", telemetryWorkerLabel)
			p.AddInt64Field("worker_req_num", batchesSeen)
			p.AddFloat64Field("rtt_ms_total", lagMillis)
			p.AddInt64Field("body_bytes", int64(bodySize))
		}			
	}
	// TODO: define workerGroup out ouf this function (maybe in main function)
	workersGroup.Done()
}

func processPeriodic(w *HTTPWriter, backoffSrc chan bool, backoffDst chan struct, stop chan int) {
	ticker := time.NewTicker(1 * time.Second)
LOOP:
	for {
		select {
		case <- ticker.C:
			//write
		case <- stop:
			ticker.stop()
			break LOOP
		}
	}
}

// backoff operations are implemented following to the messages defined in http_writer.go
// backoff occurs when the StatusCode of response message is 500,
// and decide whether backoff or not by reading the body
func processBackoffMessages(workerId int, src chan bool, dst chan struct{}) {
	var totalBackoffSecs float64
	var start time.Time
	last := false
	for this := range src {
		if this && !last {
			start = time.Now()
			last = true
		} else if !this && last {
			took := time.Now().Sub(start)
			fmt.Printf("[worker %d] backoff took %.02fsec\n", workerId, took.Seconds())
			totalBackoffSecs += took.Seconds()
			last = false
			start = time.Now()
		}
	}
	fmt.Printf("[worker %d] backoffs took a total of %fsec of runtime\n", workerId, totalBackoffSecs)
	dst <- struct{}{}
}
	
var (
	bufPool               sync.Pool
	batchChan             chan *bytes.Buffer
	inputDone             chan struct{}
	workersGroup          sync.WaitGroup
	backingOffChans       []chan bool
	backingOffDones       []chan struct{}
	telemetryChanPoints   chan *report.Point
	telemetryChanDone     chan struct{}
	telemetrySrcAddr      string
	progressIntervalItems uint64
	reportTags            [][2]string
)

// get from stdin on behalf of "flag" lib
var (
	batchSize int
	workers int
	itemLimit int64
	timeLimit time.Duration
	backoff            time.Duration
	useGzip bool
	doLoad bool
)

func init() {
	flag.IntVar(&batchSize,  "batch-size", 100, "Batch size (1 line of input = 1 item).")
	flag.IntVar(&workers, "workers", 1, "Number of parallel requests to make.")
	flag.Int64Var(&itemLimit, "item-limit", -1, "Number of items to read from stdin before quitting. (1 item per 1 line of input.)")
	flag.DurationVar(&timeLimit, "time-limit", -1, "Maximum duration to run (-1 is the default: no limit).")
	flag.DurationVar(&backoff, "backoff", time.Second, "Time to sleep between requests when server indicates backpressure is needed.")
	flag.BoolVar(&useGzip, "gzip", true, "Whether to gzip encode requests (default true).")
	flag.BoolVar(&doLoad, "do-load", true, "Whether to write data. Set this flag to false to check input read speed.")
}

func main() {
	dbHost := "http://localhost:8086"
	dbName := "benchmark_manufacuture"
	replicationFactor := 1
	consistency := "all"
	
	existingDatabases, err := listDatabases(dbHost)
	if err != nil {
		log.Fatal(err)
	}
	if len(existingDatabases) > 0 {
		log.Printf("Info: there are databases already in the data store.\n Info: the result may not be adequate")
	}
	if len(existingDatabases) == 0 {
		err = createDb(dbHost, dbName, replicationFactor)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(1000 * time.Millisecond)
	}
	
	bufPool = sync.Pool {
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4 * 1024 * 1024))
		},
	}
	// TODO: workers should be given from "stdin"
	batchChan = make(chan *bytes.Buffer, workers)
	inputDone = make(chan struct{})

	backingOffChans = make([]chan bool, workers)
	backingOffDones = make([]chan struct{}, workers)

	for i := 0; i < workers; i++ {
		backingOffChans[i] = make(chan bool, 100)
		backingOffDones[i] = make(chan struct{})
		workersGroup.Add(1)
		cfg := HTTPWriterConfig{
			DebugInfo:      fmt.Sprintf("worker #%d, dest url: %s", i, dbHost),
			Host:           dbHost,
			Database:       dbName,
			BackingOffChan: backingOffChans[i],
			BackingOffDone: backingOffDones[i],
		}

		// TODO: go funcは、timerfuncのところみたいに呼ぶかも？
		go processBatches(NewHTTPWriter(cfg, consistency), backingOffChans[i], backingOffDones[i], telemetryChanPoints, fmt.Sprintf("%d", i))
		go processBackoffMessages(i, backingOffChans[i], backingOffDones[i])
	}

	start := time.Now()
	itemsRead, bytesRead := scan(batchSize)

	// tick実行でも、この感じでいける
	// つまり、bufへの入力が終わりを<-inputDoneが帰ってくるので判定し、
	// それが帰ってきたら、batchChanを閉じる。閉じても取り出しはまだできるからbatch処理は続く。
	<-inputDone
	close(batchChan)

	workersGroup.Wait()
	
	for i := range backingOffChans {
		close(backingOffChans[i])
		<-backingOffDones[i]
	}

	end := time.Now()
	took := end.Sub(start)
	itemsRate := float64(itemsRead) / float64(took.Seconds())
	bytesRate := float64(bytesRead) / float64(took.Seconds())


	fmt.Printf("loaded %d items in %fsec with %d workers (mean point rate %f/sec, %.2fMB/sec from stdin)\n", itemsRead, took.Seconds(), workers, itemsRate, bytesRate/(1<<20))

	
	// ticker samples
	chStop := make(chan int, 1)
	timerfunc(chStop)

	// time.Sleep(...) -> workersGroup.Wait()に対応させれば良さげ。
	time.Sleep(time.Second * 3)
	chStop <- 0

	close(chStop)

	time.Sleep(time.Second * 1)
	fmt.Println("Application End.")
}

func timerfunc(stopTimer chan int) {
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
	LOOP:
		for {
			select {
			case <- ticker.C:
				fmt.Println("ここにpost requestを書く")
				//fmt.Println("now -> %v\n", time.Now())
			case <- stopTimer:
				fmt.Println("Timer stop.")
				ticker.Stop()
				break LOOP
			}
		}
		fmt.Println("timerfunc end.")
	}()
}

func createDb(daemon_url, dbname string, replicationFactor int) error {
	u, err := url.Parse(daemon_url)
	if err != nil {
		return err
	}

	// serialize params the right way:
	u.Path = "query"
	v := u.Query()
	v.Set("consistency", "all")
	v.Set("q", fmt.Sprintf("CREATE DATABASE %s WITH REPLICATION %d", dbname, replicationFactor))
	u.RawQuery = v.Encode()

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// does the body need to be read into the void?

	if resp.StatusCode != 200 {
		return fmt.Errorf("bad db create")
	}
	return nil
}

// listDatabases lists the existing databases in InfluxDB.
func listDatabases(daemonUrl string) ([]string, error) {
	u := fmt.Sprintf("%s/query?q=show%%20databases", daemonUrl)
	resp, err := http.Get(u)
	if err != nil {
		return nil, fmt.Errorf("listDatabases error: %s", err.Error())
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Do ad-hoc parsing to find existing database names:
	// {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"],["benchmark_db"]]}]}]}%
	type listingType struct {
		Results []struct {
			Series []struct {
				Values [][]string
			}
		}
	}
	var listing listingType
	err = json.Unmarshal(body, &listing)
	if err != nil {
		return nil, err
	}

	ret := []string{}
	for _, nestedName := range listing.Results[0].Series[0].Values {
		name := nestedName[0]
		// the _internal database is skipped:
		if name == "_internal" {
			continue
		}
		ret = append(ret, name)
	}
	return ret, nil
}
