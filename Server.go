package main

// packages net and net/http
import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

var (
	mutex sync.Mutex
)

var numGoroutines = 0

func main() {
	// fmt.Fprintln("Hello there!")
	fmt.Println("Hej")

	// Start go routine.

	//  establish a socket connection that it can use to listen for incoming connections.

	server, error := net.Listen("tcp", "localhost:9988")

	if error != nil {
		fmt.Println("Failed to listen to connection ", error.Error())
		os.Exit(1)
	}

	defer server.Close()

	for {
		connection, error := server.Accept()

		if error != nil {
			fmt.Println("Error accepting: ", error.Error())
			os.Exit(1)
		}

		for numGoroutines >= 1 {
			// fmt.Println("Is locked!")
		}

		mutex.Lock()
		{
			numGoroutines += 1
		}
		mutex.Unlock()

		go handleClient(connection)

	}

}

func handleClient(connection net.Conn) {
	defer func() {
		connection.Close()
		mutex.Lock()
		{
			numGoroutines -= 1
		}
		mutex.Unlock()
	}()

	buffer := make([]byte, 1024)
	mLen, err := connection.Read(buffer)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
	}
	fmt.Println("Num requests ", numGoroutines)
	fmt.Println("Received: ", string(buffer[:mLen]))

	reqbuf := bufio.NewReader(strings.NewReader(string(buffer)))

	parsed, err := http.ReadRequest(reqbuf)
	if err != nil {
		respondWithError(400, "Bad Request", connection)
		println("Failed to parse request ", err)
		return
	}
	println("Trying to access ", parsed.URL.Path)
	res := filepath.Ext(parsed.URL.Path)
	file := parsed.URL.Path

	if !isValidExtension(res) {
		respondWithError(400, "Bad Request", connection)
		println("Request with bad extension!", res)
		return
	}

	if parsed.Method == "GET" {
		handleGetRequest(file, connection)
	} else if parsed.Method == "POST" {
		handlePostRequest(file, parsed.Body, connection)
	} else {
		respondWithError(501, "Not Implemented", connection)
		println("Not Implemented method ", parsed.Method)
	}
}

func isValidExtension(a string) bool {
	return a == ".html" || a == ".txt" || a == ".gif" || a == ".jpeg" || a == ".jpg" || a == ".css"
}

func respondWithError(errorCode int, errorMsg string, connection net.Conn) {
	response := http.Response{
		Status:     errorMsg,
		StatusCode: errorCode,
	}
	response.Write(connection)
}

func handleGetRequest(file string, connection net.Conn) {
	root, err := os.Getwd()
	if err != nil {
		println("This is error to get base dir")
		respondWithError(500, "Internal Server Error", connection)
		return
	}
	relativeFileName := path.Join(root, file)

	f, err := os.Open(relativeFileName)

	if err != nil {
		println("This is error find fail")
		respondWithError(404, "Not found", connection)
		return
	}

	response := http.Response{
		StatusCode: 200,

		Body: f,
	}

	println("Responding a file")
	response.Write(connection)
}

func handlePostRequest(file string, body io.ReadCloser, connection net.Conn) {
	root, err := os.Getwd()

	if err != nil {
		println("This is error to get base dir")
		respondWithError(500, "Internal Server Error", connection)
		return
	}
	relativeFileName := path.Join(root, file)

	outFile, err := os.Create(relativeFileName)
	defer outFile.Close()
	_, err = io.Copy(outFile, body)

	if err != nil {
		println("This is error copying file content")
		respondWithError(400, "Bad Input", connection)
		return
	}
	// Response
	// response.Write(connection)
}
