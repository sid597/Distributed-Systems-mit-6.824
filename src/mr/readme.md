## JOB

- Your job is to implement a distributed MapReduce, consisting of two programs, the master and the worker.

- There will be just one master process, and one or more worker processes executing in parallel.

- The workers will talk to the master via RPC. 

- Each worker process will ask the master for a task, read the task's input from one or more files, execute the task, and write the task's output to one or more files.

- The master should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

## RULES

- The map phase should divide the intermediate keys into buckets for nReduce reduce tasks, where nReduce is the argument that main/mrmaster.go passes to MakeMaster().
- The worker implementation should put the output of the X'th reduce task in the file mr-out-X.
- A mr-out-X file should contain one line per Reduce function output. The line should be generated with the Go "%v %v" format, called with the key and value. Have a look in main/mrsequential.go for the line commented "this is the correct format". The test script will fail if your implementation deviates too much from this format.
- You can modify mr/worker.go, mr/master.go, and mr/rpc.go. You can temporarily modify other files for testing, but make sure your code works with the original versions; we'll test with the original versions.
- The worker should put intermediate Map output in files in the current directory, where your worker can later read them as input to Reduce tasks.
- main/mrmaster.go expects mr/master.go to implement a Done() method that returns true when the MapReduce job is completely finished; at that point, mrmaster.go will exit.
- When the job is completely finished, the worker processes should exit. A simple way to implement this is to use the return value from call(): if the worker fails to contact the master, it can assume that the master has exited because the job is done, and so the worker can terminate too. Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the master can give to workers.
