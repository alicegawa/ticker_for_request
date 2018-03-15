package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

var (
	bufPool sync.Pool
	batchChan []chan *bytes.Buffer
	inputDone []chan struct{}
	wg sync.WaitGroup
)

var (
	workers int
)

func scanFromFile(itemsPerBatch int, classname string, myFileId int) (int64, int64) {
	//fmt.Printf("At the beginning of scanFromFile. myFileId = %d\n", myFileId)
	buf := bufPool.Get().(*bytes.Buffer)
	var n int
	var itemsRead, bytesRead int64
	newline := []byte("\n")
	var batchItemCount uint64

	var fp *os.File
	var err error
	filename := fmt.Sprintf("data_%d.txt", myFileId)
	fp, err = os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(fp)

	var progressIntervalItems uint64
	for scanner.Scan() {
		itemsRead++
		batchItemCount++
		buf.Write(scanner.Bytes())
		buf.Write(newline)

		n++

		if n >= itemsPerBatch {
			atomic.AddUint64(&progressIntervalItems, batchItemCount)
			batchItemCount = 0

			bytesRead += int64(buf.Len())
			//fmt.Printf("insert to batchChan[%d]\n", myFileId)
			batchChan[myFileId] <- buf
			buf = bufPool.Get().(*bytes.Buffer)
			n = 0
		}
	}
	if err = scanner.Err(); err != nil {
		log.Fatal("Error reading input: %s", err.Error())
	}

	if n > 0 {
		batchChan[myFileId] <- buf
	}
	
	close(inputDone[myFileId])
	return itemsRead, bytesRead
}

func readFromChan(myFileId int) {
	//fmt.Printf("At the beginning of readFromChan: myFileId is %d\n", myFileId)
	counter := 0
	for elem := range batchChan[myFileId] {
		counter++
		fmt.Println(elem)
	}
	fmt.Printf("I read %d items!!\n", counter)
	wg.Done()
}

func main() {
	bufPool = sync.Pool {
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, 4 * 1024 * 1024))
		},
	}
	fileNum := 4
	workersPerFile := 2
	workers = fileNum * workersPerFile
	batchChan = make([]chan *bytes.Buffer, fileNum)
	inputDone = make([]chan struct{}, fileNum)

	// define batchChan
	fmt.Println("start allocate batchChan")
	for i := 0; i < fileNum; i++ {
		batchChan[i] = make(chan *bytes.Buffer, workersPerFile)
		inputDone[i] = make(chan struct{})
	}

	fmt.Println("start readFromChan")
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go readFromChan(i%fileNum)
	}

	fmt.Println("start scanFromChan")
	for i := 0; i < fileNum; i++ {
		fileId := i
		go func() {
			_, _ = scanFromFile(1, "h2o_feet", fileId)
		}()
	}

	fmt.Println("wait for close(inputDone)")
	for i := 0; i < fileNum; i++ {
		<-inputDone[i]
	}
	fmt.Println("finish to close(inputDone)")
	for i := 0; i < fileNum; i++ {
		close(batchChan[i])
	}
	fmt.Println("input finishes. close batchChans")

	wg.Wait()
	fmt.Println("all operation has finished")
}
