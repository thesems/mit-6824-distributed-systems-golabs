package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

const (
	// Status
	TaskStatusPending    int = 0
	TaskStatusInProgress     = 1
)

const (
	// TaskType
	TaskTypeNone   int = 0
	TaskTypeMap        = 1
	TaskTypeReduce     = 2
	TaskTypeExit       = 3
)

// Add your RPC definitions here.
type GetTaskArgs struct{}

type GetTaskReply struct {
	TaskId       string
	MapReduceNum int // unique task number or reduce task number
	TaskType     int
	TaskFile     []string
	NReduce      int
}

type CompleteTaskArgs struct {
	TaskId    string
	FileNames []string // intermediate or output file
}

type CompleteTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
