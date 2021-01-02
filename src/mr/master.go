package mr

import (
	//  "fmt"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

////////////////////////////////////////////////
// Declarations
////////////////////////////////////////////////

type MasterDetails struct {
	CurrentTaskType   string
	MapTaskFiles      []string
	TotalMapTasks     int
	ReduceTaskNumbers []int
	CompletedReduceTaskNumbers []int
	R                 int
	CompletedMapTasks []int
	OngoingTasks      map[int]WorkerDetails
	WorkerCtr         int
}

var (
	Lok          sync.Mutex
	Master       MasterDetails
	NoNewFile    string = "No New FIle234"
	OnGoingTasks map[int]WorkerDetails
)

////////////////////////////////////////////////
// RPC Handlers
////////////////////////////////////////////////

func (m *MasterDetails) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *MasterDetails) AssignNewTask(args *NoArgs, newTask *WorkerDetails) error {
			Lok.Lock()
	//fmt.Println("Got asked to assign new task",Master.MapTaskFiles)
	if len(Master.CompletedMapTasks) != Master.TotalMapTasks {
		if len(Master.MapTaskFiles) > 0 {
			newTask.State = "Map"
			newTask.MapFileName, Master.MapTaskFiles = Master.MapTaskFiles[0], Master.MapTaskFiles[1:]
			newTask.Id = Master.WorkerCtr
			newTask.R = Master.R
			newTask.StartTime = time.Now()
			Master.WorkerCtr += 1
			OnGoingTasks[newTask.Id] = *newTask
		} else {
			newTask.State = "Wait"
			///fmt.Println(Master.CompletedMapTasks, Master.MapTaskFiles)
		}
	} else {
		if len(Master.ReduceTaskNumbers) > 0 {
			newTask.State = "Reduce"
			newTask.ReduceFileNo, Master.ReduceTaskNumbers = Master.ReduceTaskNumbers[0], Master.ReduceTaskNumbers[1:]
			newTask.Id = Master.WorkerCtr
			newTask.R = Master.R
			newTask.StartTime = time.Now()
			Master.WorkerCtr += 1
			newTask.AllMapWorkers = Master.CompletedMapTasks
			OnGoingTasks[newTask.Id] = *newTask
		} else if len(Master.CompletedReduceTaskNumbers) != Master.R{
			newTask.State = "Wait"
		} else {
			newTask.State = "Done"
		}
	}
			Lok.Unlock()
	//fmt.Println(OnGoingTasks)
	return nil
}

func (m *MasterDetails) WorkerDone(w *WorkerDetails, msg *MessageForWorker) error {
	Lok.Lock()
	defer Lok.Unlock()

	// Add the completed task to the respective completed tasks list
	if w.State == "Map" {
		MakePermanent(w)
		Master.CompletedMapTasks = append(Master.CompletedMapTasks, w.Id)

	} else if w.State == "Reduce" {
		Master.CompletedReduceTaskNumbers = append(Master.CompletedReduceTaskNumbers, w.Id)
	}

	// Remove Ongoing task from Hash because this task is completed
	_, ok := OnGoingTasks[w.Id]
	if ok {
		delete(OnGoingTasks, w.Id)
	}

	//fmt.Println(OnGoingTasks)
	///fmt.Println(Master.CompletedMapTasks)
	msg.Message = "Acknowledged"
	return nil

}

////////////////////////////////////////////////
// Make New Master server constructor
////////////////////////////////////////////////

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *MasterDetails {
	Master = MasterDetails{}
	NewMaster(files, nReduce)
	///fmt.Println(Master)
	go GC()

	Master.server()
	return &Master
}

// Garbage collector removes the unnecessary workers
func GC() {
	for {
		time.Sleep(500 * time.Millisecond)
		Lok.Lock()
		// fmt.Println("Called GC")
		for _, wrkr := range OnGoingTasks {
			if time.Now().Sub(wrkr.StartTime) > (10 * time.Second) {
				// fmt.Println("_______________________GC START______________________________")
	
				// Remove the wrkr from OngoingTasks and add their tasks for reassignment in Map or Reduce
				// fmt.Println("Ongoing Tasks are :  ", OnGoingTasks, time.Now().Sub(wrkr.StartTime))

				removedWorker := OnGoingTasks[wrkr.Id]
				delete(OnGoingTasks, wrkr.Id)
				// fmt.Println("Removed worker is :", removedWorker.Id, removedWorker.State)

				// OnGoingTasks = append(OnGoingTasks[:indx], OnGoingTasks[indx+1:]...)
				// fmt.Println("Ongoing tasks after removiing dead worker is :", OnGoingTasks)
				Reassign(removedWorker)
				// fmt.Println("_______________________GC END______________________________")
	
			}
		}
		Lok.Unlock()
	}
}

// reassign task from the given worker
func Reassign(wrkr WorkerDetails) {
	alreadyAssigned := TaskAlreadyAssigned(wrkr)
	if wrkr.State == "Map" {
		if !alreadyAssigned {
			Master.MapTaskFiles = append(Master.MapTaskFiles, wrkr.MapFileName)
			fmt.Println("Worker reassigned to Map ", Master.MapTaskFiles)
		}
	} else if wrkr.State == "Reduce" {
		if !alreadyAssigned {
			Master.ReduceTaskNumbers = append(Master.ReduceTaskNumbers, wrkr.ReduceFileNo)
			fmt.Println("Worker reassigned to Reduce ", Master.ReduceTaskNumbers)
		}
	}
}

// Check if filename is in list for task assignment in
func TaskAlreadyAssigned(wrkr WorkerDetails) bool {
	if wrkr.State == "Map" {
		for _, file := range Master.MapTaskFiles {
			if wrkr.MapFileName == file {
				return true
			}

		}
		return false
	} else if wrkr.State == "Reduce" {
		for _, TaskNo := range Master.ReduceTaskNumbers {
			if wrkr.ReduceFileNo == TaskNo {
				return true
			}
		}
		return false
	}
	return false
}

// Make temporary files permanent
func MakePermanent(wrkr *WorkerDetails) {
	if wrkr.State == "Map" {
		for i := 0; i < wrkr.R; i++ {
			renameFrom := fmt.Sprint("mr-inter-", wrkr.Id, "-", i, ".tmp")
			renameTo := fmt.Sprint("mr-inter-", wrkr.Id, "-", i)
			err := os.Rename(renameFrom, renameTo)
			if err != nil {
				fmt.Println(err)
			}
		}

	}

}

func NewMaster(files []string, nReduce int) {
	Master.MapTaskFiles = files
	// Master.MapTaskFiles = append(Master.MapTaskFiles, NoNewFile)
	for i := 0; i < nReduce; i++ {
		Master.ReduceTaskNumbers = append(Master.ReduceTaskNumbers, i)
	}
	Master.TotalMapTasks = len(files)
	Master.R = nReduce
	Master.CurrentTaskType = "Map"
	OnGoingTasks = make(map[int]WorkerDetails)

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
	//l, e := net.Listen("tcp", ":1534")

	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		fmt.Println("listen error :", e)
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
	// fmt.Println("Completed Reduce tasks are :",Master.CompletedReduceTaskNumbers, len(Master.CompletedReduceTaskNumbers))
	Lok.Lock()
	if len(Master.CompletedMapTasks) == Master.TotalMapTasks && len(Master.CompletedReduceTaskNumbers) == Master.R {
		// fmt.Println("_____________________________________________________")

		// fmt.Println("_____________________________________________________")

		// fmt.Println("_____________________________________________________")

		// fmt.Println("_____________________________________________________")

		// fmt.Println(Master)
		ret = true

	}
	Lok.Unlock()

	return ret
}
