package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	PhaseMap    = 0
	PhaseReduce = 1
	PhaseDone   = 2
)

type Task struct {
	TaskId    string
	TaskType  int
	Filename  []string
	Status    int
	StartedAt time.Time
	ReduceNum int
}

type Coordinator struct {
	phase         int
	tasks         []Task
	intermediates []string
	mu            sync.Mutex
	nReduce       int
	mapJobCnt     int
	finished      bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.phase == PhaseMap {
		if len(c.tasks) > 0 {
			var task *Task
			for i := 0; i < len(c.tasks); i++ {
				if c.tasks[i].Status == TaskStatusPending {
					task = &c.tasks[i]
					break
				}
			}
			if task == nil {
				reply.TaskType = TaskTypeNone
				reply.TaskFile = nil
				return nil
			}

			task.Status = TaskStatusInProgress
			task.StartedAt = time.Now()

			reply.TaskId = task.TaskId
			reply.TaskType = task.TaskType
			reply.TaskFile = task.Filename
			reply.MapReduceNum = c.mapJobCnt
			reply.NReduce = c.nReduce

			c.mapJobCnt += 1
			fmt.Printf("Coordinator: assign map task: task_id=%s, task_nr=%d, filenames=%s\n",
				reply.TaskId, reply.MapReduceNum, reply.TaskFile)
		} else {
            fmt.Println("Coordinator: no map tasks left.")
			reply.TaskType = TaskTypeNone
			reply.TaskFile = nil
		}
	} else if c.phase == PhaseReduce {
		if len(c.tasks) > 0 {
			var task *Task
			for i := 0; i < len(c.tasks); i++ {
				if c.tasks[i].Status == TaskStatusPending {
					task = &c.tasks[i]
					break
				}
			}
			// for _, item := range c.tasks {
			// 	if item.Status == TaskStatusPending {
			// 		task = &item
			// 		break
			// 	}
			// }
			if task == nil {
				reply.TaskType = TaskTypeNone
				reply.TaskFile = nil
				return nil
			}

			task.Status = TaskStatusInProgress
			task.StartedAt = time.Now()

			reply.TaskId = task.TaskId
			reply.TaskType = task.TaskType
			reply.TaskFile = task.Filename
			reply.MapReduceNum = task.ReduceNum

			fmt.Printf("Coordinator: assign reduce task: task_id=%s, task_nr=%d, filenames=%s\n",
				reply.TaskId, reply.MapReduceNum, reply.TaskFile)
		} else {
			c.phase = PhaseDone
			reply.TaskType = PhaseDone
			reply.TaskFile = nil
            fmt.Println("Coordinator: all tasks have finished.")
		}
	} else {
        log.Fatalln("Coordinator: done!")
	}

	return nil
}

func removeTask(tasks []Task, index int) ([]Task, Task) {
	task := tasks[index]
	tasks[index] = tasks[len(tasks)-1]
	tasks = tasks[:len(tasks)-1]
	return tasks, task
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task Task
	for idx, item := range c.tasks {
		if item.TaskId == args.TaskId {
			c.tasks, task = removeTask(c.tasks, idx)
			if c.phase == PhaseMap {
				for _, intermediate := range args.FileNames {
					c.intermediates = append(c.intermediates, intermediate)
				}
                fmt.Printf("Coordinator: Map task (%s) finished.\n", task.Filename)
			} else {
                fmt.Printf("Coordinator: Reduce task (%s) finished.\n", task.Filename)
			}
            fmt.Printf("Tasks left: %d.\n", len(c.tasks))
			return nil
		}
	}

    fmt.Printf("Coordinator: Timed-out task (%s) just completed\n", task.TaskId)
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
        log.Fatal("Coordinator: listen error:", e)
	}
	go http.Serve(l, nil)
}

func extractY(s string) int {
	parts := strings.Split(s, "-")
	if len(parts) == 3 {
		y, err := strconv.Atoi(parts[2])
		if err == nil {
			return y
		}
	}
	return 0
}

func sortByY(strings []string) {
	sort.Slice(strings, func(i, j int) bool {
		return extractY(strings[i]) < extractY(strings[j])
	})
}

func (c *Coordinator) monitor() {
	for {
		time.Sleep(1)
		c.mu.Lock()

		if len(c.tasks) == 0 {
			if c.phase == PhaseMap {
				c.phase = PhaseReduce
                fmt.Println("Coordinator: Phase changed to reduce!")

				if len(c.tasks) != 0 {
                    log.Panic("Coordinator: There are still map tasks available.")
				}

				sortByY(c.intermediates)

				group := make([]string, 0)
				for idx, intermediate := range c.intermediates {
					if idx > 0 {
						inter_tokens := strings.Split(c.intermediates[idx], "-")
						last_tokens := strings.Split(c.intermediates[idx-1], "-")

						if inter_tokens[2] != last_tokens[2] {
							reduceNum, err := strconv.Atoi(last_tokens[2])
							if err != nil {
                                log.Panicln("Coordinator: Could not reinterpret reduce task number as a int.")
							}
							c.tasks = append(c.tasks, Task{
								TaskId:    uuid.New().String(),
								TaskType:  TaskTypeReduce,
								Filename:  group,
								Status:    TaskStatusPending,
								ReduceNum: reduceNum,
							})
							// fmt.Println("group", group)
							group = make([]string, 0)
						}
					}
					group = append(group, intermediate)
					if idx == len(c.intermediates)-1 {
						tokens := strings.Split(c.intermediates[idx], "-")
						reduceNum, err := strconv.Atoi(tokens[2])
						if err != nil {
                            log.Panicln("Coordinator: Could not reinterpret reduce task number as a int.")
						}
						c.tasks = append(c.tasks, Task{
							TaskId:    uuid.New().String(),
							TaskType:  TaskTypeReduce,
							Filename:  group,
							Status:    TaskStatusPending,
							ReduceNum: reduceNum,
						})
						group = make([]string, 0)
					}
				}
			} else {
				c.finished = true
				return
			}
		} else {
			for i := range c.tasks {
				task := &c.tasks[i]
				if task.Status == TaskStatusInProgress && time.Now().Sub(task.StartedAt).Seconds() >= 10 {
					task.Status = TaskStatusPending
                    fmt.Printf("Coordinator: Reset task (%s) that has timed-out.\n", task.TaskId)
				}
			}
		}

		c.mu.Unlock()
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.finished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	tasks := make([]Task, 0)
	for _, filename := range files {
		filenames := make([]string, 0)
		filenames = append(filenames, filename)

		tasks = append(tasks, Task{
			TaskId:   uuid.New().String(),
			TaskType: TaskTypeMap,
			Filename: filenames,
			Status:   TaskStatusPending,
		})
	}

	c := Coordinator{
		phase:         0,
		tasks:         tasks,
		intermediates: nil,
		nReduce:       nReduce,
		mapJobCnt:     0,
		finished:      false,
	}
	c.server()
	go c.monitor()
	return &c
}
