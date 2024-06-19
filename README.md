# MIT 6.824 Distributed Systems

## Map-Reduce: Lab 1

Worker tasks:
- Worker request a job from the master (via RPC).
- Once received, executes the job.
- Upon finishing, sends a completion message to master, which includes:
    - message itself implies that job has finished
    - if map task:
        - write key-value pairs to a JSON file (locally).
        - Include filename to the message.
        - Create file as temporary first (os.CreateTemp), rename atomically upon finishing (os.Rename).
        - Filename: mr-X-Y, where X = unique ID, Y = reduce job id.
        - Unique ID is defined by master (simple index of the file).
        - Reduce job id can be obtained by using the ihash function and modulo operator (nReduce variable).
    - if reduce task:
        - read intermediate file as specified by the job
        - perform the reduce function
        - save output file (first as temporary then atomically rename it)
    
Master tasks:
- Obtain a list of files to operate on (called inputs).
- Schedule mapping jobs (called upon an input) to workers.
- If no response is received from the worker within 10 seconds, assign the same job to a different worker.
    - implies of a method to track status type (not started, in-progress, finished)
