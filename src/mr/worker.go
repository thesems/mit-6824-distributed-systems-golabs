package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func perform_map(mapf func(string, string) []KeyValue, fileName string) []KeyValue {
	intermediate := []KeyValue{}
	file, err := os.Open(fileName)
	if err != nil {
        log.Fatalf("Worker: Failed to open file %s.", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
        log.Fatalf("Worker: cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)
	return intermediate
}

func perform_reduce(reducef func(string, []string) string, filenames []string, reduceTask int) string {
	kvs := make(ByKey, 0)

	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
            log.Fatalf("Worker: Failed to open file %s", filename)
		}

		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}

	sort.Sort(kvs)

	oname := fmt.Sprintf("mr-out-%d", reduceTask)
	ofile, err := os.Create(oname)
	if err != nil {
        log.Panicf("Worker: Could not create a file: %s\n", oname)
	}

	start := 0
	for i, kv := range kvs {
		if i == 0 {
			continue
		}
		lastVal := kvs[i-1]
		if lastVal.Key != kv.Key {
			values := make([]string, 0)
			for _, item := range kvs[start:i] {
				values = append(values, item.Value)
			}

			output := reducef(lastVal.Key, values)
			fmt.Fprintf(ofile, "%v %v\n", lastVal.Key, output)

			start = i
		}
		if i == len(kvs)-1 {
			values := make([]string, 0)
			for _, item := range kvs[start : i+1] {
				values = append(values, item.Value)
			}

			output := reducef(kv.Key, values)
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, output)
		}
	}

    fmt.Printf("Worker: reduce output file = %s\n", oname)
	return oname
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

outerloop:
	for {
		reply, ok := CallGetTask()
		if !ok {
			return
		}

        fmt.Printf("Worker: task=%s, type=%d, filenames=%s.\n", reply.TaskId, reply.TaskType, reply.TaskFile)

		switch reply.TaskType {
		case TaskTypeNone:
            fmt.Println("Worker: no tasks currently available.")
			break
		case TaskTypeExit:
            fmt.Println("Worker: all tasks finished.")
			break outerloop
		case TaskTypeMap:
			kvs := perform_map(mapf, reply.TaskFile[0])
			fileMap := make(map[int]*os.File)

			for _, kv := range kvs {
				key := ihash(kv.Key) % reply.NReduce
				file, found := fileMap[key]
				if !found {
					tempFile, err := os.CreateTemp("", "intermediate")
					if err != nil {
                        fmt.Println("Worker: Failed to create temp file")
						continue
					}
					fileMap[key] = tempFile
					file = tempFile
				}

				enc := json.NewEncoder(file)
				err := enc.Encode(kv)
				if err != nil {
                    fmt.Println("Worker: Failed to encode key-value as JSON.")
				}
			}

			intermediates := make([]string, 0)
			for key, file := range fileMap {
				defer file.Close()

				dirPath := filepath.Dir(file.Name())
				oldFilePath := fmt.Sprintf("%s", file.Name())
				newFilePath := fmt.Sprintf("%s/mr-%d-%d", dirPath, reply.MapReduceNum, key)

				os.Rename(oldFilePath, newFilePath)
				intermediates = append(intermediates, newFilePath)
			}

			ok := CallCompleteTask(reply.TaskId, intermediates)
			if !ok {
				return
			}
		case TaskTypeReduce:
			out := perform_reduce(reducef, reply.TaskFile, reply.MapReduceNum)
			ok := CallCompleteTask(reply.TaskId, []string{out})
			if !ok {
				return
			}
		default:
            fmt.Println("Worker: Task type not recognized.")
		}

		time.Sleep(1 * time.Second)
	}
}

// Asks for a new task from the coordinator.
// Task can be either a Map or Reduce task.
// If no task is available yet, it returns an empty JSON file.
func CallGetTask() (GetTaskReply, bool) {
	var args GetTaskArgs
	var reply GetTaskReply
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
        fmt.Printf("Worker: call GetTask failed!\n")
		return reply, false
	}
	return reply, true
}

func CallCompleteTask(taskId string, fileNames []string) bool {
	args := CompleteTaskArgs{
		TaskId:    taskId,
		FileNames: fileNames,
	}
	var reply CompleteTaskReply
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if !ok {
        fmt.Printf("Worker: call CompleteTask failed!\n")
		return false
	}
	return true
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
