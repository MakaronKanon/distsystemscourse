package main

import (
	"bufio"
	"crypto/sha1"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {

	ipHost := flag.String("a", "required", "The IP address that the Chord client will bind to, as well as advertise to other nodes. Represented as an ASCII string (e.g., 128.8.126.63). Must be specified.")
	portHost := flag.Int("p", -1, "The port that the Chord client will bind to and listen on. Represented as a base-10 integer. Must be specified.")

	otherHost := flag.String("ja", "required", "The IP address of the machine running a Chord node. The Chord client will join this node’s ring. Represented as an ASCII string (e.g., 128.8.126.63). Must be specified if --jp is specified.")
	otherPort := flag.Int("jp", -1, " The port that an existing Chord node is bound to and listening on. The Chord client will join this node’s ring. Represented as a base-10 integer. Must be specified if --ja is specified.")

	timeBetweenStabilize := flag.Int("ts", -1, "The time in milliseconds between invocations of ‘stabilize’. Represented as a base-10 integer. Must be specified, with a value in the range of [1,60000].")
	timeBetweenFixFingers := flag.Int("tff", -1, "The time in milliseconds between invocations of ‘fix fingers’. Represented as a base-10 integer. Must be specified, with a value in the range of [1,60000]")
	timeBetweenCheckPredecessors := flag.Int("tcp", -1, "The time in milliseconds between invocations of ‘check predecessor’. Represented as a base-10 integer. Must be specified, with a value in the range of [1,60000]")
	numSuccessors := flag.Int("r", -1, "The number of successors maintained by the Chord client. Represented as a base-10 integer. Must be specified, with a value in the range of [1,32]")
	// overrideIdentifier := flag.String("i", "required", "The identifier (ID) assigned to the Chord client which will override the ID computed by the SHA1 sum of the client’s IP address and port number. Represented as a string of 40 characters matching [0-9a-fA-F]. Optional parameter.")

	flag.Parse()

	if *ipHost == "required" {
		fmt.Println("Ip is required!")
		os.Exit(1)
	}
	if *portHost == -1 {
		fmt.Println("Port is required!")
		os.Exit(1)
	}
	if *otherHost != "required" && *otherPort == -1 {
		fmt.Println("Must set jp if ja is set.")
		os.Exit(1)
	}
	if *otherHost == "required" && *otherPort != -1 {
		fmt.Println("Must set ja if jp is set.")
		os.Exit(1)
	}
	if *timeBetweenStabilize < 1 || *timeBetweenStabilize > 60000 {
		fmt.Println("Time between stabilize '-ts' must be specified, in range [1,60000].")
		os.Exit(1)
	}
	if *timeBetweenFixFingers < 1 || *timeBetweenFixFingers > 60000 {
		fmt.Println("Time between fix fingers '-tff' must be specified, in range [1,60000].")
		os.Exit(1)
	}
	if *timeBetweenCheckPredecessors < 1 || *timeBetweenCheckPredecessors > 60000 {
		fmt.Println("Time between check predecessors '-tcp' must be specified, in range [1,60000].")
		os.Exit(1)
	}
	if *numSuccessors < 1 || *numSuccessors > 32 {
		fmt.Println("Number of successors '-r' must be specified, in range [1,32].")
		os.Exit(1)
	}

	currentNode = createOurNode(*ipHost, *portHost)
	shouldJoin := *otherPort != -1
	if shouldJoin {
		joinRing(*otherHost, *otherPort)
	} else {
		createRing()
	}

	go listenForConnections(*ipHost, *portHost)

	go runInterval(*timeBetweenStabilize, stabilize)
	go runInterval(*timeBetweenCheckPredecessors, checkPredecessor)

	// go fixFingers()

	readCommands()
}

type node struct {
	address string
	id      id
}

func createNode(address string) node {
	return node{
		address: address,
		id:      getId(address),
	}
}

var successor node
var currentNode node
var predecessor *node

func createOurNode(ip string, port int) node {
	return createNode(fmt.Sprintf("%s:%d", ip, port))
}

func createRing() {
	successor = currentNode
}

func joinRing(ip string, port int) {
	otherNode := createNode(fmt.Sprintf("%s:%d", ip, port))

	// TODO: Can we assume this will succeed?
	successor = findSuccessorIteratively(currentNode.id, otherNode)
}

func sleepMilliSeconds(seconds int) {
	time.Sleep(time.Duration(seconds) * time.Millisecond)
}

// Other node notify us of their existance.
func notify(potentialPredecessor node) {
	if predecessor == nil {
		// We don't have anything better as predecessor so use it.
		predecessor = &potentialPredecessor
		return
	}

	if isBetween(predecessor.id, potentialPredecessor.id, currentNode.id) {
		predecessor = &potentialPredecessor
	}
}

// Whether middle is in range (y, z)
func isBetween(x id, middle id, z id) bool {
	if x.Cmp(z) == -1 {
		// x ---- middle ---- z
		// x < middle && middle < z
		return (x.Cmp(middle) == -1) && middle.Cmp(z) == -1
	} else {
		// middle ---- z ---- x ---- middle
		// middle < z || x < middle
		return (middle.Cmp(z) == -1 || x.Cmp(middle) == -1)
	}
}

// func isBetween(x node, y node, z node) bool {
// 	 if y.id.Cmp(z.id) == -1 {
// 	 	return (x.id.Cmp(y.id) == 1 || x.id.Cmp(y.id) == 0) && x.id.Cmp(z.id) == -1
// 	 } else { // Other case.
// 	 	return (x.id.Cmp(y.id) == 1 || x.id.Cmp(y.id) == 0) || x.id.Cmp(z.id) == -1
// 	 }
// 	return false
// }

func fixFingers() {
	for {
		fmt.Println("Fixing fingers")
		// text, _ := reader.ReadString('\n');

		//

		time.Sleep(time.Second)
	}

}

func runInterval(interval int, function func()) {
	for {
		sleepMilliSeconds(interval)
		function()
	}
}

func checkPredecessor() {
	fmt.Println("Checking predecessor")

	if predecessor == nil {
		return
	}

	isAlive := nodeInterface.Ping(*predecessor)
	if !isAlive {
		predecessor = nil
	}
}

func stabilize() {
	fmt.Println("Running stabilize")

	potentialNewSuccessor, exists := nodeInterface.GetPredeccessor(successor)

	if exists && isBetween(currentNode.id, potentialNewSuccessor.id, successor.id) {
		successor = potentialNewSuccessor
	}
	nodeInterface.Notify(successor)
}

func readCommands() {
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("Reading commands")
		text, _ := reader.ReadString('\n')

		inputs := strings.Split(text, " ")
		fmt.Println("Inputs are ", inputs)
		switch inputs[0] {
		case "lookup":
			lookupFile(inputs[1])
		case "storeFile":
			storeFile(inputs[1])
		case "printState":
			printState()
		case "ping":
			pingSuccessor()
		}
	}
}

type id = *big.Int

type NodeInterface interface {
	Ping(node node) bool
	FindSuccessor(node node, id id) (bool, node)
	GetPredeccessor(node node) node
	Notify(node node)
}

type HttpNodeInterface struct{}

func (h HttpNodeInterface) Ping(node node) bool {
	resp, err := http.Get("http://" + node.address + "/ping")
	if err != nil {
		log.Println("Error pinging : ", err.Error())
		return false
	}
	if resp.StatusCode != http.StatusOK {
		log.Println("Not alive")
		return false
	}
	return true
}

type FindSuccessorHttpResponse struct {
	FoundSuccessor  bool   `json:"foundSuccessor"`
	SuccessorAdress string `json:"successorAdress"`
}

func (h HttpNodeInterface) FindSuccessor(node node, id id) (bool, node) {
	fmt.Println("Make FindSucessor call to ", node)
	resp, err := http.Get("http://" + node.address + "/findSuccessor?id=" + id.String())
	if err != nil {
		log.Panic("Error finding successor: ", err.Error())
	} else if resp.StatusCode != http.StatusOK {
		log.Panic("Error finding successor, status not ok.")
	}

	var parsedResp FindSuccessorHttpResponse
	err = json.NewDecoder(resp.Body).Decode(&parsedResp)
	fmt.Println("Parsed the response to ", parsedResp)
	if err != nil {
		panic(err)
	}
	return parsedResp.FoundSuccessor, createNode(parsedResp.SuccessorAdress)

}

type GetPredeccessorHttpResponse struct {
	Address        string `json:"address"`
	HasPredecessor bool   `json:"hasPredecessor"`
}

func (HttpNodeInterface) GetPredeccessor(node node) (node, bool) {
	resp, err := http.Get("http://" + node.address + "/getPredeccessor")
	if err != nil {
		log.Panic("Error asking predecessor: ", err.Error())
	}
	if resp.StatusCode != http.StatusOK {
		log.Panic("Error asking predecessor, status not ok.")
	}

	var parsedResp GetPredeccessorHttpResponse
	err = json.NewDecoder(resp.Body).Decode(&parsedResp)
	if err != nil {
		log.Panic("Error decoding predecessor response ", err.Error())
	}

	return createNode(parsedResp.Address), parsedResp.HasPredecessor
}

func (HttpNodeInterface) Notify(node node) {
	resp, err := http.Get("http://" + node.address + "/notify?address=" + currentNode.address)

	if err != nil {
		log.Panic("Error notifying node: ", node, err.Error())
	}

	if resp.StatusCode != 200 {
		log.Panic("Bad resposne notifying: ", node)
	}
}

var nodeInterface = HttpNodeInterface{}

func pingSuccessor() {
	gotAnswer := nodeInterface.Ping(successor)
	fmt.Println("Answer: ", gotAnswer)
}

func lookupFile(filename string) {
	id := getId(filename)

	owner := findSuccessorIteratively(id, currentNode)

	// file := nodeInterface.RequestFile(owner, filename)

	// hej.txt -> oasethu134n5hntsoaehu
	// Hash the filename to get the fileid.
	// owner := findSuccessor(id)
	// owner.requestFile(filename)

	fmt.Println("Looking up file: ", filename, " with id: ", id, "at node: ", owner)
}

// Find whether id is in range [currentNode.id, successor.id).
func ownerOfIdIsSuccessor(id id) bool {
	return isBetween(currentNode.id, id, successor.id) || currentNode.id.Cmp(id) == 0
}

// Find the node responsable for storing `id`.
func findSuccessor(id id) (bool, node) {
	if ownerOfIdIsSuccessor(id) {
		return true, successor
	}
	return false, successor
}

func findSuccessorIteratively(id *big.Int, startNode node) node {
	nextNode := startNode
	for {
		println("Looping finding successor iteratively")
		found, nextNode := nodeInterface.FindSuccessor(nextNode, id)
		if found {
			return nextNode
		}
	}
}

func storeFile(filename string) {
	id := getId(filename)

	owner := findSuccessorIteratively(id, currentNode)

	// file := nodeInterface.RequestFile(owner, filename)

	// hej.txt -> oasethu134n5hntsoaehu
	// Hash the filename to get the fileid.
	// owner := findSuccessor(id)
	// owner.requestFile(filename)

	fmt.Println("Storing file: ", filename, " with id: ", id, "at node: ", owner)
	// // id := getId(filename)
	// // ownerNode := findSuccessorIteratively(id)
	// // Read file from disk and make post request to ownerNode.

	// fmt.Println("Storing file: ", filename)
}

// Hashes the name, filename or node address, to get the id in the ring.
func getId(name string) *big.Int {
	return hash(name)
}

func hash(elt string) *big.Int {
	hasher := sha1.New()
	hasher.Write([]byte(elt))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func printState() {
	// fmt.Println("Printing state")
	fmt.Println("Successor is ", successor)
	fmt.Println("Predeccessor is ", predecessor)
}

func listenForConnections(host string, port int) {
	server, error := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))

	if error != nil {
		fmt.Println("Failed to listen to connection ", error.Error())
		os.Exit(1)
	}
	defer server.Close()

	http.HandleFunc("/ping", handlePing)
	http.HandleFunc("/findSuccessor", handleFindSuccessor)
	http.HandleFunc("/getPredeccessor", handleGetPredeccessor)
	http.HandleFunc("/notify", handleNotify)
	http.Serve(server, nil)
}

func handlePing(w http.ResponseWriter, req *http.Request) {
	log.Println("Got a ping request, responding with pong.")
	w.WriteHeader(200)
}

type FindSucessorHttpRequest struct {
	id string
}

func handleFindSuccessor(w http.ResponseWriter, req *http.Request) {
	fmt.Println("Handling find successor")
	id := req.URL.Query().Get("id")
	fmt.Println("Handling find successor, id is ", id)

	// var successorHttpRequest FindSucessorHttpRequest
	// err := json.NewDecoder(req.Body).Decode(&successorHttpRequest)
	// if err != nil {
	// 	panic(err)
	// }

	// String to bigint?
	n := new(big.Int)
	n, ok := n.SetString(id, 10)
	if !ok {
		panic("Not ok")
	}

	fmt.Println("Before")
	found, node := findSuccessor(n)
	fmt.Println("After")
	response := FindSuccessorHttpResponse{
		SuccessorAdress: node.address,
		FoundSuccessor:  found,
	}
	fmt.Println("Response is ", response)

	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		panic(err)
	} else {
		fmt.Println("No error")
	}
}

func handleGetPredeccessor(w http.ResponseWriter, req *http.Request) {
	fmt.Println("Handling get predeccessor")

	var response GetPredeccessorHttpResponse
	if predecessor != nil {
		response = GetPredeccessorHttpResponse{
			Address:        predecessor.address,
			HasPredecessor: true,
		}
	} else {
		response = GetPredeccessorHttpResponse{
			HasPredecessor: false,
		}
	}
	err := json.NewEncoder(w).Encode(response)

	if err != nil {
		log.Panic("Failed to encode response ", response, err)
	}
}

func handleNotify(w http.ResponseWriter, req *http.Request) {
	fmt.Println("Handling notify")

	notifierAddress := req.URL.Query()["address"][0]
	log.Println("Parsed handleNotify address ", notifierAddress)
	notifierNode := createNode(notifierAddress)
	notify(notifierNode)

	w.WriteHeader(200)
}
