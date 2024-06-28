# MIT 6.824 Distributed Systems

## Lab 1: Map-Reduce 

### Worker
- Worker keeps requesting tasks from the coordinator in a loop.
- As a response, the worker receives:
    - task ID
    - Task type: none, map, reduce or finish.
    - File name(s)
    - Map task number or reduce number
    - Number of reduce jobs (in case of map task)
- For map task, following tasks are done:
    - the specified file is read
    - parsed as JSON
    - executed as parameters in map function
    - output split in hash buckets and saved to intermediate files (temporary files)
    - upon completion, files are renamed to mr-X-Y format
- For reduce task, following tasks are done:
    - the specified files are read and decoded as key-values
    - sorted according to the key
    - reduce function executed on same keys
    - output saved to mr-out-{reduceBucket}
- a task completion RPC is sent, which includes:
    - either the intermediate files or output files

### Coordinator
- Load input files
- Create map tasks based on the read input files
- Assign map tasks to workers
- Monitor currently assigned tasks and reassign timed-out tasks
- Create reduce tasks based on intermediate files

## Lab 2: Key/Value Server

- Key/Value Server provides an in-memory storage in the form of a key/value pair.  
- Handles concurrent access via a locking mechanism.  
- Detects and drops duplicate requests, while keeping memory footprint minimal.  
