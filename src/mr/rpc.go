package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type GetTaskArgs struct {}

type GetTaskReply struct {
    TaskId int
    // type of task: none (0), map (1), reduce(2)
    // if none (0), worker should exit
	TaskType int
	TaskFile string
    NReduce int
}

type CompleteTaskArgs struct {
    file_name string
}

type CompleteTaskReply struct {}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
