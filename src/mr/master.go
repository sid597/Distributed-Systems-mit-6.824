package mr

import "log"
import "net"
//import "os"
import "net/rpc"
import "net/http"
import "fmt"


////////////////////////////////////////////////
// Declarations
////////////////////////////////////////////////

type MasterDetails struct {
	// Your definitions here.
    AllWorkers []WorkerDetails
    UnfinishedTasks []string

}

type IWorkerDetails struct {
    Id int
    Task string
    Filename string
    Status string
}

// The operations related to files should be global
// so that one can access these without initializing or passing some other data to functions
var (
    R int
    Job Files
    Master MasterDetails
    WorkerCtr int
    NoNewFile string = "No New FIle234"
)

type Files struct {
    AllFiles []string
    FilesLen, CurrentFileCtr int
    CurrentFile string
}



////////////////////////////////////////////////
// RPC Handlers
////////////////////////////////////////////////

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *MasterDetails) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *MasterDetails) GetFilename(noArg *NoArgs, fn *WorkerDetails) error {
    // TODO : Somehow set the Currentfile to reduce file locations also
    NewWorkerDetails(fn,Job.CurrentFile)
    if Job.CurrentFileCtr != Job.FilesLen - 1{
        IncreaseCurrentJobFileCtr()
        WorkerCtr += 1
    } else {
        Job.CurrentFile = NoNewFile
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
    R = nReduce
	// Your code here.
    NewJob(files)
    fmt.Println(Job.AllFiles)

	Master.server()
	return &Master
}

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

////////////////////////////////////////////////
// Helper functions  
/////////////////////////////////////////////////

// New Files struct invocation
func NewJob(files []string) {
    Job = Files{}
    Job.AllFiles = files
    Job.FilesLen = len(files)
    Job.CurrentFileCtr = 0
    Job.CurrentFile = files[0]
}

func NewWorkerDetails(details *WorkerDetails,filename string) {
    details.Id = WorkerCtr
    details.MapFileName = filename
    details.Status = "in-progress"
    details.R = R
    details.Task="MapT"
}



func AddToWorkersList(worker WorkerDetails) {
    Master.AllWorkers = append(Master.AllWorkers, worker)
}

func UpdateJobData(ctr int) bool {
    if Job.CurrentFileCtr == Job.FilesLen {
        return false
    } else {
        Job.CurrentFileCtr = ctr
        Job.CurrentFile = Job.AllFiles[Job.CurrentFileCtr]
    }
    return true
}

func GetJobFile(ctr int) string {
    return Job.AllFiles[ctr]
}

func IncreaseCurrentJobFileCtr() {
    Job.CurrentFileCtr += 1
    Job.CurrentFile = Job.AllFiles[Job.CurrentFileCtr]

}

