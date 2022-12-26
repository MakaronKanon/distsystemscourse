package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type WorkerData struct {
	mapFunction    func(string, string) []KeyValue
	reduceFunction func(string, []string) string

	numTasks    int
	numReducers int
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (worker *WorkerData) getBucketNumber(key string) int {
	return ihash(key) % worker.numReducers
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	numTasks, numReducers := GetInitialWorkerData()
	worker := WorkerData{
		mapFunction:    mapf,
		reduceFunction: reducef,
		numTasks:       numTasks,
		numReducers:    numReducers,
	}

	log.Println("Worker ready, starting to ask for tasks.")
	for {
		shouldQuit := worker.runTask()
		if shouldQuit {
			log.Println("Worker quitting!")
			break
		}
	}
}

func (worker *WorkerData) runTask() bool {
	reply, ok := AskForTask()

	if !ok {
		return true
	}

	// if rand.Intn(100) < 20 {
	// 	log.Println("Task simulating slowness, taskid: ", reply.Task.Id)
	// 	time.Sleep(12 * time.Second)
	// } else if rand.Intn(100) < 20 {
	// 	log.Println("Task simulating crash, taskid: ", reply.Task.Id)
	// 	return false
	// }

	switch reply.TaskType {
	case TASK_MAP:
		log.Println("Running new map task with id ", reply.Task.Id)
		worker.mapFile(reply.Task)
	case TASK_REDUCE:
		log.Println("We are reduce task for bucket ", reply.Task.Id)
		worker.runReduce(reply.Task)
	case TASK_IDLE:
		log.Println("No task ready at the moment, trying again in 1 second.")
		time.Sleep(time.Second)
	case TASK_STOP:
		log.Println("Got stop signal from coordinator.")
		return true
	}
	return false
}

func (worker *WorkerData) mapFile(task Task) {
	content := readFileContent(task.Filename)
	result := worker.mapFunction(task.Filename, content)

	buckets := worker.divideIntermediateResultToBuckets(result)

	for bucket, bucketContent := range buckets {
		writeIntermediateFile(task.Id, bucket, bucketContent)
	}

	if ok := NotifyTaskFinished(task); !ok {
		log.Println("Could not notify task was finished.")
	}
}

func (worker *WorkerData) runReduce(task Task) {
	bucket := task.Id

	groupedValues := worker.groupIntermediateKeys(bucket)
	finalKeyValues := worker.applyReduceFunction(groupedValues)

	writeOutputFile(bucket, finalKeyValues)

	// removeIntermediateFiles(bucket, worker.numTasks)

	if ok := NotifyTaskFinished(task); !ok {
		log.Println("Could not notify task was finished.")
	}
}

func (worker *WorkerData) groupIntermediateKeys(bucket int) map[string][]string {
	groupedValues := make(map[string][]string)

	for task := 0; task < worker.numTasks; task++ {
		keyValues := readIntermediateFile(task, bucket)

		for _, keyValue := range keyValues {
			if _, exists := groupedValues[keyValue.Key]; !exists {
				groupedValues[keyValue.Key] = make([]string, 0)
			}
			groupedValues[keyValue.Key] = append(groupedValues[keyValue.Key], keyValue.Value)
		}
	}
	return groupedValues
}

func (worker *WorkerData) applyReduceFunction(groupedValues map[string][]string) []KeyValue {
	result := make([]KeyValue, 0)
	for key, value := range groupedValues {
		reducedValue := worker.reduceFunction(key, value)
		result = append(result, KeyValue{
			Key:   key,
			Value: reducedValue,
		})
	}
	return result

}

func getIntermediateFilename(task int, bucket int) string {
	return fmt.Sprintf("mr-%d-%d", task, bucket)
}

func getOutputFilename(bucket int) string {
	return fmt.Sprintf("mr-out-%d", bucket)
}

func readIntermediateFile(task int, bucket int) []KeyValue {
	filename := getIntermediateFilename(task, bucket)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open intermediate file `%s` due to `%s`", filename, err)
	}

	result := make([]KeyValue, 0)
	decoder := json.NewDecoder(file)
	decoder.Decode(&result)

	return result
}

func writeIntermediateFile(task int, bucket int, bucketContent []KeyValue) {
	filename := getIntermediateFilename(task, bucket)

	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create intermediate file `%s` due to `%s`", filename, err)
	}

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(bucketContent); err != nil {
		log.Fatalf("Failed to encode to intermediate file `%s` due to `%s`", filename, err)
	}
}

func (worker *WorkerData) divideIntermediateResultToBuckets(result []KeyValue) [][]KeyValue {
	buckets := make([][]KeyValue, worker.numReducers)

	for _, item := range result {
		bucketIndex := worker.getBucketNumber(item.Key)
		buckets[bucketIndex] = append(buckets[bucketIndex], item)
	}
	return buckets
}

// func removeIntermediateFiles(bucket int, numTasks int) {
// 	for i := 0; i < numTasks; i++ {
// 		os.Remove(getIntermediateFilename(i, bucket))
// 	}
// }

func writeOutputFile(bucket int, keyValues []KeyValue) {
	filename := getOutputFilename(bucket)

	// if _, err := os.Stat(filename); err == nil {
	// 	// File already exists, don't want to overwrite it.
	// 	// Another worker probably took over the task and finished before us.
	// 	return
	// }

	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create output file `%s` due to `%s`", filename, err)
	}

	for _, keyValue := range keyValues {
		_, err := fmt.Fprintf(file, "%v %v\n", keyValue.Key, keyValue.Value)
		if err != nil {
			log.Fatalf("Failed to write keyValue to output file `%s` due to `%s`", filename, err)
		}
	}

}

func readFileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open %v, due to %s", file, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v, due to %s", file, err)
	}
	file.Close()
	return string(content)
}

func AskForTask() (*AskForTaskReply, bool) {
	args := AskForTaskArgs{}
	reply := AskForTaskReply{}
	ok := call("Coordinator.AskForTask", &args, &reply)

	if !ok {
		log.Println("Got no response asking for task")
		return nil, false
	}
	return &reply, true
}

func NotifyTaskFinished(task Task) bool {
	args := TaskFinishedArgs{
		Task: task,
	}
	reply := TaskFinishedReply{}
	ok := call("Coordinator.TaskFinished", &args, &reply)

	return ok
}

func GetInitialWorkerData() (int, int) {
	args := GetInitialWorkerDataArgs{}
	reply := GetInitialWorkerDataReply{}

	ok := call("Coordinator.GetInitialWorkerData", &args, &reply)
	if ok {
		return reply.NumTasks, reply.NumReducers
	}
	log.Fatal("Failed to get initial worker data")
	return -1, -1
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
