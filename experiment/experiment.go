package main

import (
	"fmt"
	"time"
)

var globalInt *int

func main() {
	a := 1
	globalInt = &a
	go goroutineA()
	go goroutineB()

	for {
		time.Sleep(1)
	}
}

func goroutineA() {
	for {
		if globalInt == nil {
			continue
		}
		fmt.Println("Go A: globalInt is ", *globalInt)
		time.Sleep(time.Millisecond * 300)
	}
}

func goroutineB() {
	time.Sleep(4 * time.Second)
	globalInt = nil
}
