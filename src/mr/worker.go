package mr

import (
    "fmt"
    "log"
    "net/rpc"
    "hash/fnv"
    "os"
    "io/ioutil"
    "time"
)



////////////////////////////////////////////////
// Declarations
////////////////////////////////////////////////

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerDetails struct {
    Id int
    Task string
    MapFileName string
    ReduceFileName string
    Filename string
    Pid int
    nReduce int
}

var CurrentWorker WorkerDetails

////////////////////////////////////////////////
// RPC
////////////////////////////////////////////////




////////////////////////////////////////////////
// Main Functions  
////////////////////////////////////////////////

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) bool {
    CurrentWorker = GetTask()
    for CurrentWorker.Filename != "Secret key 994" {
        fmt.Println(CurrentWorker)
        if CurrentWorker.Task == "MapT"{
            res := MapTask(CurrentWorker.MapFileName, mapf)
            time.Sleep(500 * time.Millisecond)   // Used to test map running in parallel or not
            return res
        } else if CurrentWorker.Task == "ReduceT" {
            res := ReduceTask(CurrentWorker.ReduceFileName, reducef)
            time.Sleep(500 * time.Millisecond)   // Used to test reduce running in parallel or not
            return res
        }
        CurrentWorker  = GetTask()
    }
}


////////////////////////////////////////////////
// Map Reduce functions 
////////////////////////////////////////////////

// Perform map job for the current file with the passed map func
func MapJob(filename string, mapf func(string,string)[]KeyValue) bool {
        intermediate := []KeyValue{}
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
            return false
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
            return false
		}
        //fmt.Println(content)
		file.Close()
        //fmt.Println(string(content))
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
        // TODO : instead of using intermediate here need to store in a file


        return true
}


// Perform Reduce Job for the current file with the passed reduce function


////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Ask for new task from Master
func GetTask() string{
    na := NoArgs{}
    NewTask := Task{}
    call("MasterDetails.GetFilename",&na, &NewTask)
    // fmt.Println("reply received is ", NewTask.Name)
    return NewTask
}


// Create nReduce Temporary Map files 
func CreateFile(dir, filename string) bool  {
    // TODO
}

// Write Map KeyValue Slice output to R-th file
func WriteMapTo(filename string,KV []KeyValue) bool {}


////////////////////////////////////////////////
// Send RPC
////////////////////////////////////////////////
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := masterSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
