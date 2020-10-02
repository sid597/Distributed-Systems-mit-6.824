package mr

import (
    "fmt"
    "log"
    "net/rpc"
    "net/http"
    "net"
    "time"
)


////////////////////////////////////////////////
// Declarations
////////////////////////////////////////////////

type MasterDetails struct {
    CurrentTaskType string
    MapTaskFiles []string
    ReduceTaskNumbers []int
    R int
    CompletedMapTasks []int
    //OnGoingMapTasks []
    WorkerCtr int
}

var Master MasterDetails
var NoNewFile string =  "No New FIle234"


////////////////////////////////////////////////
// RPC Handlers
////////////////////////////////////////////////

func (m *MasterDetails) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}



func (m *MasterDetails) AssignNewTask(args *NoArgs, nw *WorkerDetails) error {
    // TODO : if all the map tasks are done assign reduce task also set current task type to Reduce else assign a new Map task
    // Should I increase the worker ctr on every 
    if len(Master.CompletedMapTasks) != len(Master.MapTaskFiles) {
        if len(Master.MapTaskFiles) > 0 {
            nw.Task = "Map"
            nw.MapFileName, Master.MapTaskFiles = Master.MapTaskFiles[0], Master.MapTaskFiles[1:]
            nw.Id = Master.WorkerCtr
            nw.R = Master.R
            fmt.Println(nw,time.Now())

            Master.WorkerCtr += 1
        } else {
            nw.Task = "Wait"
        }
    } else if len(Master.CompletedMapTasks) == len(Master.MapTaskFiles) {
        if len(Master.ReduceTaskNumbers) > 0 {
            nw.Task = "Reduce"
            nw.ReduceFileNo, Master.ReduceTaskNumbers = Master.ReduceTaskNumbers[0], Master.ReduceTaskNumbers[1:]
            nw.AllMapWorkers = Master.CompletedMapTasks
        } else {
            nw.Task = "Done"
        }
    }

    return nil


}
////////////////////////////////////////////////
// Master server 
////////////////////////////////////////////////


//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *MasterDetails {
	Master = MasterDetails{}
    NewMaster(files, nReduce)
    fmt.Println(Master)

    Master.server()
	return &Master
}

func NewMaster(files []string, nReduce int) {
    Master.MapTaskFiles = files
    // Master.MapTaskFiles = append(Master.MapTaskFiles, NoNewFile)
    for i := 0; i < nReduce; i++{
        Master.ReduceTaskNumbers = append(Master.ReduceTaskNumbers, i)
    }
    Master.R = nReduce
    Master.CurrentTaskType = "Map"

}
////////////////////////////////////////////////
// Master server 
////////////////////////////////////////////////

//
// start a thread that listens for RPCs from worker.go
//
func (m *MasterDetails) server() {
    rpc.Register(m)
    rpc.HandleHTTP()
    l, e := net.Listen("tcp", ":1234")
//	sockname := masterSock()
//	os.Remove(sockname)
//	l, e := net.Listen("unix", sockname)
    if e != nil {
        log.Fatal("listen error:", e)
    }
    go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *MasterDetails) Done() bool {
    ret := false

    // Your code here.


    return ret
}

