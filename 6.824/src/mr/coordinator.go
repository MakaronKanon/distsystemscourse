package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Stage = int

const (
	STAGE_MAP Stage = iota
	STAGE_REDUCE
	STAGE_FINISHED
)

type Coordinator struct {
	stage           Stage
	tasks           []Task
	tasksInProgress map[int]Task
	tasksTodo       []Task

	mutex sync.Mutex

	numReducers int
}

func (c *Coordinator) GetInitialWorkerData(args *GetInitialWorkerDataArgs, reply *GetInitialWorkerDataReply) error {
	reply.NumReducers = c.numReducers
	reply.NumTasks = len(c.tasks)

	return nil
}

func (c *Coordinator) AskForTask(args *AskForTaskArgs, reply *AskForTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if len(c.tasksTodo) > 0 {
		if c.stage == STAGE_MAP {
			reply.TaskType = TASK_MAP
		} else {
			reply.TaskType = TASK_REDUCE
		}
		reply.Task = c.tasksTodo[len(c.tasksTodo)-1]
		c.tasksTodo = c.tasksTodo[:len(c.tasksTodo)-1]
		log.Println("Assigned new task ", reply.Task.Id)
		reply.Task.startedAt = time.Now()
		c.tasksInProgress[reply.Task.Id] = reply.Task
	} else if len(c.tasksInProgress) > 0 {
		for _, task := range c.tasksInProgress {
			if time.Now().Sub(task.startedAt) > 10*time.Second {
				log.Println("Reassinging task ", task.Id)
				if c.stage == STAGE_MAP {
					reply.TaskType = TASK_MAP
				} else {
					reply.TaskType = TASK_REDUCE
				}
				reply.Task = task
				reply.Task.startedAt = time.Now()
				return nil
			}
		}
		// No tasks should be rescheduled, worker should just wait for others to finish.
		reply.TaskType = TASK_IDLE
	} else {
		reply.TaskType = TASK_STOP
	}

	return nil
}

func (c *Coordinator) TaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	task, exists := c.tasksInProgress[args.Task.Id]
	if !exists {
		log.Println("Task finished that was already finished ", task.Id)
		return nil
	}
	log.Println("Task finished ", task.Id)
	delete(c.tasksInProgress, task.Id)

	c.checkProgress()

	return nil
}

func (c *Coordinator) checkProgress() {

	if len(c.tasksTodo) == 0 && len(c.tasksInProgress) == 0 {
		if c.stage == STAGE_MAP {
			log.Println("Moving to reduce stage")
			c.moveToReduceStage()
		} else if c.stage == STAGE_REDUCE {
			log.Println("Moving to finished stage")
			c.stage = STAGE_FINISHED
		}
	}
}

func (c *Coordinator) moveToReduceStage() {
	c.stage = STAGE_REDUCE

	c.tasksTodo = make([]Task, c.numReducers)
	for i := 0; i < c.numReducers; i++ {
		reduceTask := Task{
			Id: i,
		}
		c.tasksTodo[i] = reduceTask
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.stage == STAGE_FINISHED
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}

	c.stage = STAGE_MAP
	c.tasks = make([]Task, len(files))
	for i, filename := range files {
		task := Task{
			Filename: filename,
			Id:       i,
		}
		c.tasks[i] = task
	}
	c.tasksTodo = make([]Task, len(files))
	copy(c.tasksTodo, c.tasks)
	c.tasksInProgress = make(map[int]Task)

	c.numReducers = nReduce

	log.Println("Coordinator waiting for workers to do the work.")

	c.server()

	return &c
}
