package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	// "fmt"

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



	if some client request to leader fails what to do ?
	  Q. But first what it means for a client request to leader to fail and how does client know request failed ?
	  A. Call() returns false or request times out, that's how client know requset failed
		  1. if server is dead, or request lost
		  2. if server communicated, but request lost

		  And these 2 cases are same for the client all he sees is a failed request
		  But resending request again does not yiels the same results for the above 2 cases
		  for (1) resending is fine but for (2) an append entry will cause havoc if measures 
		  are not taken to stop this.

	  Q. So what measure need to be taken ?
	  A. Client does not have much knowledge to make an informed decision about the resending of request so,
		 The decision need to made at service level and client always resends requests. KV Server somehow need 
		 to mitigate this resend, How ?  
		 The paper mentions of assigning an unique id to each reqeust and state machine keeping track of them.
		 If we again encounrer the same request return from table. But this  arises the question 
		  	What happends if client sends a request, raft applies the request, but before replying leader changes(i.e something happed to 
			this leader maube crashed or partitioned) ?
			Client will send the request to the new leader, but how will this new leader know the request is already applied ?
			So to deal with this problem we need to send additional data to raft (client and request Id) so that when any service sees 
			some command on appltCh it can also sabe the sata from where it came.

			But what if new leader did not apply the latest command before the client resent request ?
			We need to know that a K/V Servoce and its raft are on the same machine so applying some request to service channel >> rpc time
			so it would be fair to assume this situation will not arise.






*/

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader int
	mu      sync.Mutex
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
	ck.mu.Lock()
	args := GetArgs{key, ck.clerkId, ck.requestId}
	reply := GetReply{}
	ck.requestId++
	ck.mu.Unlock()

	ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
	for reply.Err == "Not Leader" || reply.Err == "Timeout" || !ok{
		ck.mu.Lock()
		reply = GetReply{}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
		ok = ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		//  Pf("GET Client Returning %v, for args %v", reply, args)
	}
		//  Pf("GET Client Returning %v, for args %v", reply, args)
	// Pf("GET Client Returning %v", reply)
	return reply.Value
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
	ck.mu.Lock()
	args := PutAppendArgs{key, value, op, ck.clerkId, ck.requestId}
	reply := PutAppendReply{}
	ck.requestId++
	ck.mu.Unlock()
	ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
	for reply.Err == "Not Leader" || reply.Err == "Timeout" || !ok{
		ck.mu.Lock()
		reply = PutAppendReply{}
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
		ok = ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		//  Pf("PA Client Returning %v", reply)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
