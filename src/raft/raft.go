package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
    "sync"
    "sync/atomic"
    "../labrpc"
    "rand"
    "time"
)

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

////////////////////////////////////////////////////////////////////////////////
// Structs
////////////////////////////////////////////////////////////////////////////////

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).

    ElectionTime int
    State string
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

    // Persistant data
    CurrentTerm int
    VotedFor int
    Log []//TODO

    // Volatile data 
    CommitIndex int
    LastApplied int

}

var Candi Candidate
var Follo Follower
var Lead Leader

// Leader Specific data 
type Leader struct{

    // Volatile Leader data
    NextIndex []int
    MatchIndex []int
}

// Candidate Specific data 
type Candidate struct{
    CandidateID int
    LastLogEntry int
    LastLogTerm int
}

// Follower Specific data 
type Follower struct{

}
////////////////////////////////////////////////////////////////////////////////
// Constructors for leader, candidate and Follower
////////////////////////////////////////////////////////////////////////////////

func NewCandidate(){}

func NewFollower(){}
func NewLeader(){}

////////////////////////////////////////////////////////////////////////////////
// 
////////////////////////////////////////////////////////////////////////////////
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
}



//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}



////////////////////////////////////////////////////////////////////////////////
// start election
////////////////////////////////////////////////////////////////////////////////

func(rf *Raft) ResetElectionTimer () {
    rf.mu.Lock
    defer rf.mu.Unlock
    // Set election time between 150-300 milliseconds 
    rf.ElectionTime = (rand.Intn(150) + 150) * time.Millisecond
    return
}


func (rf *raft) NewElection(){
    rf.mu.Lock
    // For New Election we need to do the following things :
    // 1. Increment the current Term
        rf.CurrentTerm += 1
    // 2. Vote for self
        candi.Votes += 1
    rf.mu.Unlock
    // 3. Reset election timer
        rf.ResetElectionTimer()
    // 4. Send requestVoteRPC to all other servers
        // Args for RequestVoteRPC
        Args := RequestVoteArgs{}
        Args.Term = rf.CurrentTerm
        Args.CandidateId = Candi.CandidateId
        Args.LastLogIndex = Candi.LastLogIndex
        Args.LastLogTerm = Candi.LastLogTerm


        // Condition variable 
        cond := sync.NewCond(&mu)
        votesReceived := 0
        finished := 0

        // TODO :HOW DO I LOCK THIS READ FROM rf.peers ?
        for peer in rf.Peers {
            if peer != me {
                // Concurrently ask servers for Votes 
                go func(peer int){
                    // Reply For RequestVoteRPC
                    Reply := RequestVoteReply{}
                    response := rf.SendRequestVote(peer, Args, &Reply)
                    // If response is false means server is dead, partitioned, lossy request
                    if response {
                        if Reply.VoteGranted {
                            rf.mu.Lock
                            defer rf.mu.Unlock
                            VotesReceived += 1
                            /*
                              **Doubt**
                               Can a Receiver reply with a term greater than Candidate's term ?

                               So the rule is if  a receiver sees a term < Receiver's Term
                               it will return false. Otherwise return term and VoteGranted=true
                               NO a candidate cannot receive a term > Cndidate's term 
                               A candidate can only become follower in case it gets a 
                               appendVoteRPC
                            */
                            rf.CurrentTerm = Reply.Term
                        }
                        finished += 1
                        cond.Broadcast()
                    }
                }(peer)
            }
        }
        rf.RequestVote(Args, &Reply)

    // 5. If votes received from majority of server's become Leader

    /* 
        This server will remain in this state until one the following 3 things happen
          1. If votes received from majority of servers, become Leader
          2. Someone else establishes themselves as leader
          3. Election timeout happens

       **QUESTION**
       QUESTION : Do I need to use another thread to chek if this server gained majority or not ?
       I THINK : Yes, I can for that I would have save another state in Candidate struct for remembring 
       the no of votes gathered and total how many replies I have got
       No. Nothing is gained from using another go thread, nothing will be parallized by
       doing this
    
       **DOUBT**
      I suspect I do not need to lock this for loop because the value 
      of rf.peers will not be changed by any thread 
      On further thought this might be a wrong assumption because 
      peers can be changed in a long running system

     */
     rf.mu.Lock
     for VotesReceived < len(rf.peers)/2 && finished != len(rf.peers){
         cond.Wait
     }
     if VotesReceived >= len(rf.peers)/2 {
         rf.State = "Leader"
         NewLeader()
     }
     rf.mu.Unlock
    // 6. If AppendEntriesRPC received from new leader convert to follower
    // 7. If election timeout elapses start new election : This is always running and 
    //    checking if the timeout is elapsed
    // NOTE: 6 and 7 are taken care of by threads running concurrently
}

//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
    Term int
    VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    // This is the implementation on the side of server whom we are asking for vote
    /*
        Terminology: 
            Candidate : the server asking for vote
            Receiver : The server granting vote
        The rule for granting vote is :
        1. The term of Candidate should be equal to or greater than 
           the Receiver.
        2. If (the Receiver's VotedFor is nil or candidateID) && 
           (Candidate's log is atleast as up-to-date as receiver's log )
           then grant vote

    **Clarification**
      Assumption : This is a follower server becaise it received a RequestVoteRPC
      No, This is not a follower it can also be a leader who received a request 
      for granting its vote and if the Candidate's term > Receiver's term then
      this server will become follower
    */
    rf.mu.Lock
    defer rf.mu.Unlock
    if args.Term < rf.CurrentTerm{
        reply.VoteGranted =  false
    } else {
        selfLastLogIndex := rf.Log[-1].Index
        selfLastLogTerm := rf.Log[-1].Term
        candidateId := args.CandidateId
        if (rf.CurrentTermVotedFor == nil || rf.CurrentTermVotedFor == candidateId ) && (args.LastLogIndex >= rf.selfLastLogIndex && args.LastLogTerm, >= self.LastLogTerm) {
            reply.Term = rf.CurrentTerm
            reply.VoteGranted = true
        } else {
            reply.VoteGranted = false
        }
    }

    if args.Term > rf.CurrentTerm {
        rf.State = "Follower"
        NewFollower()

    }
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


////////////////////////////////////////////////////////////////////////////////
// Server Specific 
////////////////////////////////////////////////////////////////////////////////
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
