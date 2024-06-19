package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
)

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
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	intermediate = append(intermediate, kva...)
	return intermediate
}

func perform_reduce(reducef func(string, []string) string, filename string, reduceTask int) string {
    file, err := os.Open(filename)
    if err != nil {
        fmt.Println("Failed to open the file")
        return ""
    }
    
    decoder := json.NewDecoder(file)
    kvs := make([]KeyValue, 0)

	oname := fmt.Sprintf("mr-out-%d", reduceTask)
	ofile, _ := os.Create(oname)

    for {
        var kv KeyValue
        if err := decoder.Decode(&kv); err != nil {
            fmt.Println("Failed to decode a key/value.")
            break
        }
        
        if len(kvs) == 0 {
            kvs = append(kvs, kv)
            continue
        }
        
        lastVal := kvs[len(kvs) - 1]
        if lastVal.Key != kv.Key {
            values := make([]string, 0)
            for _, item := range kvs {
                values = append(values, item.Value)
            }

            output := reducef(lastVal.Key, values)
		    fmt.Fprintf(ofile, "%v %v\n", lastVal.Key, output)

            clear(kvs)
            kvs = append(kvs, kv)
        }
    }
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := CallGetTask()

	switch reply.TaskType {
	case 0:
		fmt.Println("Master does not have a task available.")
		return
	case 1:
		kvs := perform_map(mapf, reply.TaskFile)
		fileMap := make(map[int]*os.File)

		for _, kv := range kvs {
			key := ihash(kv.Key) % reply.NReduce
			file, found := fileMap[key]
			if !found {
				tempFile, err := os.CreateTemp("", "intermediate")
				if err != nil {
					fmt.Println("Failed to create temp file")
					continue
				}
				fileMap[key] = tempFile
				file = tempFile
			}

			enc := json.NewEncoder(file)
			err := enc.Encode(kv)
			if err != nil {
				fmt.Println("Failed to encode key-value as JSON.")
			}
		}

		for key, file := range fileMap {
            defer file.Close()

			fullPath, err := filepath.Abs(file.Name())
			if err != nil {
				fmt.Println("Failed to get full path of a file")
				continue
			}
			newFileName := fmt.Sprintf("mr-%s-%d", reply.TaskId, key)
			os.Rename(fullPath, newFileName)
		}
	case 3:
		_ = perform_reduce(reducef, reply.TaskFile, reply.NReduce)
	default:
		fmt.Println("Task type not recognized.")
	}
}

// Asks for a new task from the coordinator.
// Task can be either a Map or Reduce task.
// If no task is available yet, it returns an empty JSON file.
func CallGetTask() GetTaskReply {
	var args GetTaskArgs
	var reply GetTaskReply
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		fmt.Printf("call GetTask failed!\n")
	}
	return reply
}

func CallCompleteTask() CompleteTaskReply {
	var args CompleteTaskArgs
	var reply CompleteTaskReply
	ok := call("Coordinator.CompleteTask", &args, &reply)
	if !ok {
		fmt.Printf("call CompleteTask failed!\n")
	}
	return reply
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
