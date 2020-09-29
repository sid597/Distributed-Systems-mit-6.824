package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
    Id int
    Filename string
    Type string
    nReduce int
}


type NoArgs struct {}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

/*
TODO :
- Create temporary file names 
- Register worker on master with initial call 
- Write worker contents in temperary file
- Call done when all the map jobs are done
- Create Master data structure to manage workers
- Only increase worker ctr when a task is assigned
*/
