package main

import (
	"fmt"
	"sync"
	"time"
)

var wg sync.WaitGroup

func main() {
	bufChan := make(chan int, 5)
	for i := 0; i < 5; i++ {
		bufChan <- i
	}
	wg.Add(1)
	allElem(bufChan)
	time.Sleep(time.Second)
	for i := 0; i < 2; i++ {
		bufChan <- (5 + i)
	}
	//time.Sleep(time.Second * 8)
	close(bufChan)
	fmt.Println("bufChan has been closed")
	wg.Wait()
	//time.Sleep(time.Second * 8)

	fmt.Println("Application End.")
}

func allElem(bufChan chan int) {
	go func() {
		limiter := time.Tick(1 * time.Second)
		for elem := range bufChan {
			fmt.Println("before call limiter. still Read from bufChan!!")
			<-limiter
			fmt.Println(elem)
		}
		wg.Done()
	}()
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
