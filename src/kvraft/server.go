package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	// "time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

func Pf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Type string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	resCh chan raft.ApplyMsg
	db    map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	index, _, isLeader := kv.rf.Start(Op{args.Key, "","Get"})
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !isLeader {
		Pf("[%v] Not Leader", kv.me)
		reply.Err = "Not Leader"
		return
	} else {
		Pf("[%v] Get request with args %v", kv.me, args)
		result := <-kv.resCh
		Pf("[%v] result is %v", kv.me, result)
		if result.CommandIndex == index {
			reply.Err = "Leader"
			reply.Value = kv.db[args.Key]
			Pf("[%v] Get replying %v", kv.me, reply)
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	opType := "Append"
	if args.Op == "Put" {
		opType = "Put"
	}
	index, _, isLeader := kv.rf.Start(Op{args.Key, args.Value, opType})

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !isLeader {
		reply.Err = "Not Leader"
		return
	} else {
		Pf("[%v] PutApend request with args %v", kv.me, args)
		result := <-kv.resCh
		if result.CommandIndex == index {
			reply.Err = "Leader"
			Pf("[%v] Result is %v, index is %v, reply %v", kv.me, result, index, reply)
			return
		}
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) Receive() {
	Pf("Receiving")
	for x := range kv.applyCh {
		if x.IsLeader {
			kv.resCh <- x
		}

		kv.mu.Lock()
		key := x.Command.(Op).Key
		value := x.Command.(Op).Value
		opType := x.Command.(Op).Type
		kv.mu.Unlock()

		if opType == "Put" {
			kv.db[key] = value
		} else if opType == "Append" {
			kv.db[key] += value
		}
		Pf("[%v] DB IS %v", kv.me, kv.db)

	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.resCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	go kv.Receive()

	return kv
}
