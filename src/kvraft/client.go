package kvraft

import (
	"crypto/rand"
	"math/big"

	"time"

	"../labrpc"
)

// import "time"

/*

	Clients of raft send all their requests to tthe leader.
	When a client first startys up, it connects to a randomly
	chosen server. If the client's first choice is not the leaser,
	that server will reject the client's request ans supply information
	about the most recent leader it has heard from.

	If the leader crashes, client requests will time out; clients
	then try again with randomly-chosen servers.


	ROLE OF :

	CLERK :
		1. Send the request by "client" to leader and return the result
		2. If the call fails retry some other server



	K/V Service :
		1. Apply the operation to its data
		2. Detect duplicate client requests



*/

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	// Clerk picks a request Id for each request
	// While sending the request to leader clerk sends the request and
	// server Id
	requestId int
	clerkId   int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers

	// Start by assuming server 0 is leader if not will be set to correct one eventually
	ck.leader = 0
	ck.clerkId = nrand()
	ck.requestId = 0
	Pf("Make Client with id %v", ck.clerkId)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	args := GetArgs{key, ck.clerkId, ck.requestId}
	reply := GetReply{}
	ck.requestId++
	ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)

	if ok {
		for reply.Err != "" {
			ok = ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		}
		if reply.Value != "" {
			return reply.Value
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{key, value, op, ck.clerkId, ck.requestId}
	reply := PutAppendReply{}
	ck.requestId++
	ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
	if ok {
		Pf("[%v] Reply is %v, for server %v", ck.clerkId, &reply, ck.leader)
		for reply.Err == "Not Leader" {

			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(500 * time.Millisecond)
			ok = ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)

			//Pf("[%v] Reply is %v, for server %v", ck.clerkId, &reply, ck.leader)
			if reply.Err == "Leader" {
				break
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
