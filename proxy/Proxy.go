package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
)

var origin string = "http://localhost:9988"

func main() {
	port := getPort()
	fmt.Printf("Starting proxy listening on %s, to origin %s.", port, origin)

	server, error := net.Listen("tcp", "0.0.0.0:"+port)

	if error != nil {
		fmt.Println("Failed to listen on x ", error.Error())
		os.Exit(1)
	}
	defer server.Close()

	http.HandleFunc("/", handleClient)
	http.Serve(server, nil)
}

func handleClient(w http.ResponseWriter, req *http.Request) {
	println("Trying to access ", req.URL.Path)

	if req.Method == "GET" {
		handleGetRequest(req.URL.Path, w)
	} else {
		http.Error(w, "Not Implemented", 501)
		println("Not Implemented method ", req.Method)
	}
}

func handleGetRequest(file string, w http.ResponseWriter) {
	resp, err := http.Get(origin + file)

	if err != nil {
		println("This is error to get response ", err.Error())
		http.Error(w, "Internal Server Error", 500)
		return
	}

	io.Copy(w, resp.Body)
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
