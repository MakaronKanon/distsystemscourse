package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
)

const MAX_GOROUTINES = 10

var semaphore = make(chan struct{}, MAX_GOROUTINES)

func main() {
	port := getPort()
	fmt.Printf("Starting server listening on %s.", port)

	server, error := net.Listen("tcp", "0.0.0.0:"+port)

	if error != nil {
		fmt.Println("Failed to listen to connection ", error.Error())
		os.Exit(1)
	}

	defer server.Close()

	http.HandleFunc("/", handleClient)
	http.Serve(server, nil)
}

func handleClient(w http.ResponseWriter, req *http.Request) {
	defer func() {
		<-semaphore
	}()
	semaphore <- struct{}{}

	println("Trying to access ", req.URL.Path)
	res := filepath.Ext(req.URL.Path)
	file := req.URL.Path

	if !isValidExtension(res) {
		http.Error(w, "Bad Request", 400)
		return
	}

	switch req.Method {
	case "GET":
		handleGetRequest(file, w, req)
	case "POST":
		handlePostRequest(file, req.Body, w)
	default:
		http.Error(w, "Not Implemented", 501)
	}
}

func isValidExtension(a string) bool {
	return a == ".html" || a == ".txt" || a == ".gif" || a == ".jpeg" || a == ".jpg" || a == ".css"
}

func handleGetRequest(file string, w http.ResponseWriter, req *http.Request) {
	root, err := os.Getwd()
	if err != nil {
		println("This is error to get base dir")
		http.Error(w, "Internal Server Error", 500)
		return
	}
	relativeFileName := path.Join(root, file)
	http.ServeFile(w, req, relativeFileName)
}

func handlePostRequest(file string, body io.ReadCloser, w http.ResponseWriter) {
	root, err := os.Getwd()

	if err != nil {
		println("This is error to get base dir")
		http.Error(w, "Internal Server Error", 500)
		return
	}
	relativeFileName := path.Join(root, file)

	outFile, err := os.Create(relativeFileName)
	defer outFile.Close()
	_, err = io.Copy(outFile, body)

	if err != nil {
		println("This is error copying file content")
		http.Error(w, "Bad Input", 400)
		return
	}

	w.WriteHeader(200)
}

func getPort() string {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("You must specify port to listen to!")
		os.Exit(1)
	}
	port := args[0]
	return port
}
