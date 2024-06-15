package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// map = 0, reduce = 1, done = 2
	phase        int
	inputs       []string
	intermediate []KeyValue
	mu           sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// / If any inputs are left, pops an input element and schedules it as a map job.
// / Once no inputs are left, pops a intermediate element and schedules it as
// / as a reduce job.
// / If no inputs and no intermediate values are available, returns none(0) as
// / task type. This means all jobs have finished.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == 0 {
		if len(c.inputs) > 0 {
			reply.Task_type = 1
			job := c.inputs[len(c.inputs)-1]
			reply.Data = fmt.Sprintf(`["%s"]`, job)
			c.inputs = c.inputs[:len(c.inputs)-1]
		} else {
			fmt.Println("Ran out of inputs elements but reduce phase is not started yet.")
			reply.Task_type = 0
			reply.Data = "[]"
		}
	} else if c.phase == 1 {
		if len(c.intermediate) > 0 {
			reply.Task_type = 2
			job := c.intermediate[len(c.inputs)-1]
			reply.Data = fmt.Sprintf(`["%s"]`, job)
			c.intermediate = c.intermediate[:len(c.intermediate)-1]
		} else {
            c.phase = 2
			reply.Task_type = 0
			reply.Data = "[]"
			fmt.Println("Ran out of inputs and intermediate elements.")
		}
	} else {
        reply.Task_type = 0
        reply.Data = "[]"
        fmt.Println("Done.")
    }

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

func (c *Coordinator) monitor() {

    for {
        c.mu.Lock()
        defer c.mu.Unlock()

        // check on map jobs
        // once all map jobs are finished
        // set phase to 2 (reduce)
        // reschedule timed-out tasks by adding to inputs slice

        time.Sleep(1)
    }
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	inputs := make([]string, 10)
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		inputs = append(inputs, string(content))
	}

	c := Coordinator{
		phase: 0,
		inputs:               inputs,
		intermediate:         nil,
	}
	c.server()
    c.monitor()
	return &c
}
