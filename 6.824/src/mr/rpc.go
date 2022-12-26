package mr

//
// RPC definitions.
//
import (
	"os"
	"strconv"
	"time"
)

type Task struct {
	Id        int    // Id of map task. Bucket for reduce task.
	Filename  string // Only used for map task.
	startedAt time.Time
}

type TaskType = int

const (
	TASK_MAP TaskType = iota
	TASK_REDUCE
	TASK_STOP
	TASK_IDLE
)

type AskForTaskArgs struct{}
type AskForTaskReply struct {
	TaskType TaskType
	Task     Task
}

type GetInitialWorkerDataArgs struct{}
type GetInitialWorkerDataReply struct {
	NumReducers int
	NumTasks    int
}

type TaskFinishedArgs struct {
	Task Task
}
type TaskFinishedReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
