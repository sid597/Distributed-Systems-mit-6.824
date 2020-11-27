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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	mu0       sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Lab 2A

	raftId        int
	state         State
	lastReceived  time.Time
	electionAlarm time.Duration

	currentTerm int
	votedFor    int

	totalServers int

	// Lab 2B

	applyCh     chan ApplyMsg
	log         []LogEntry
	commitIndex int
	lastApplied int
	index       int

	lastAppliedRpc time.Time
	NextIndex      []int
	MatchIndex     []int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type State string

const (
	Follower  = "Follower"
	Leader    = "Leader"
	Candidate = "Candidate"
)

func (rf *Raft) NewFollower() {
	if rf.killed() {
		Pf("[%v]###################### KILL CALLED New Follower DEAD NOW  ##############################", rf.raftId)
		return
	}

	rf.mu.Lock()
	firstEntry := LogEntry{Term: 0, Command: 0}
	rf.log = append(rf.log, firstEntry)
	Pf("[%v] Asked to become NEW  Follower for term [%v] ", rf.me, rf.currentTerm)
	rf.state = Follower
	rf.ResetElectionAlarm()
	rf.mu.Unlock()
	rf.StartElectionCountdown()
}

func (rf *Raft) BecomeFollower() {
	Pf("[%v] Asked to ____BECOME FOLLOWER____ for term [%v] ", rf.me, rf.currentTerm)
	rf.state = Follower
	rf.ResetElectionAlarm()
}

func (rf *Raft) BecomeLeader() {

	rf.mu.Lock()
	if rf.state != Leader {
		Pf("[%v] Asked to b____BECOME LEADER______ for term [%v] ", rf.me, rf.currentTerm)
		rf.state = Leader
	}
	rf.lastAppliedRpc = time.Now()
	rf.NextIndex = []int{}
	rf.MatchIndex = []int{}
	ri := rand.Intn(5000)
	me := rf.me

	for i := 0; i < rf.totalServers; i++ {
		rf.NextIndex = append(rf.NextIndex, len(rf.log))
		if i == me {
			rf.MatchIndex = append(rf.MatchIndex, len(rf.log)-1)
		} else {
			rf.MatchIndex = append(rf.MatchIndex, 0)
		}
	}
	Pf("[%v] NextIndex and MatchIndex are : %v, %v", rf.me, rf.NextIndex, rf.MatchIndex)
	rf.mu.Unlock()
	Pf("[%v] %v LEADER 1st Heartbeat ", me, ri)
	rf.StartAgreement(true, ri)

	// TODO : Start sending heartbeats

	for {
		time.Sleep(24 * time.Millisecond)
		//rf.CheckCommitIndex()

		rf.mu.Lock()
		timeSince := time.Now().Sub(rf.lastAppliedRpc)
		curState := rf.state
		ri = rand.Intn(5000)
		rf.mu.Unlock()

		if curState != Leader {
			rf.mu.Lock()
			Pf("[%v] ===============NOT LEADER ANYMORE==========%v=%v===", rf.me, rf.state, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		if timeSince > 140*time.Millisecond {

			Pf("")
			Pf("[%v] %v TIME SINCE : %v, So Sending Heartbeat ", me, ri, timeSince)
			rf.StartAgreement(true, ri)
			rf.mu.Lock()

			rf.lastAppliedRpc = time.Now()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) BecomeCandidate() {

	rf.mu.Lock()
	//rf.lastReceived = time.Now()

	Pf("[%v] Asked to _____BECOME CANDIDATE_____ for term [%v] ", rf.me, rf.currentTerm+1)
	rf.state = Candidate
	rf.mu.Unlock()
	rf.NewElection()
}

////////////////////////////////////////////////////////////////////////////////
// Resetting election
////////////////////////////////////////////////////////////////////////////////

//
// Set election time between 150-300 milliseconds
//
func (rf *Raft) ResetElectionAlarm() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	rf.lastReceived = time.Now()
	rf.electionAlarm = time.Duration(rand.Intn(200)+250) * time.Millisecond
	Pf("[%v] Election alarm reset to : [%v] for term [%v]", rf.me, rf.electionAlarm, rf.currentTerm)
}

/*
	Run election timer in follower and candidate state to check if its time
	for another election

	This will be long running thread
*/

func (rf *Raft) StartElectionCountdown() {

	for {
		if rf.killed() {
			Pf("[%v]###################### KILL CALLED Start Election Countdown DEAD NOW  ##############################", rf.raftId)
			return
		}
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		state := rf.state
		lastRpcTime := rf.lastReceived
		electionAlarm := rf.electionAlarm
		// me := rf.me
		// term := rf.currentTerm
		rf.mu.Unlock()

		if state == Leader {

			// Somehow stop this thread / set election alarm infinite
			rf.mu.Lock()
			rf.electionAlarm = 20 * time.Second
			rf.mu.Unlock()
		} else {
			timeElapsed := time.Now().Sub(lastRpcTime)
			if timeElapsed > electionAlarm {
				//Pf("[%v] timeout after [%v] was expected [%v] current state [%v] for term [%v]", me, timeElapsed, electionAlarm, state, term)

				if state == Follower {
					rf.BecomeCandidate()
				} else {
					rf.NewElection()
				}

			}
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// Election
////////////////////////////////////////////////////////////////////////////////

func (rf *Raft) NewElection() {

	rf.mu.Lock()

	rf.currentTerm += 1
	Pf("[%v] New election for term [%v] ", rf.me, rf.currentTerm)

	rf.votedFor = rf.me
	me := rf.me
	forTerm := rf.currentTerm

	rf.ResetElectionAlarm()
	rf.mu.Unlock()

	totalVotes := 1 // Voted for self

	for server, _ := range rf.peers {
		if server != me {
			go func(server int, forTerm int) {
				if rf.killed() {
					// Pf("[%v]###################### KILL CALLED REQUEST VOTE DEAD NOW  ##############################", rf.raftId)
					return
				}

				voteGranted, serverTerm := rf.GetVote(server)
				rf.mu.Lock()
				currentTerm := rf.currentTerm

				rf.mu.Unlock()
				if forTerm == currentTerm {
					if voteGranted {
						rf.mu.Lock()

						totalVotes += 1
						majorityServers := rf.totalServers/2 + 1
						tv := totalVotes

						Pf("[%v] vote from [%v] result [%v] now Total Votes [%v] out of [%v] for Term : [%v]", rf.me, server, voteGranted, totalVotes, majorityServers, rf.currentTerm)

						rf.mu.Unlock()

						if tv >= majorityServers {
							rf.mu.Lock()
							// 	Pf("[%v] total votes received >= majority ", rf.me)
							state := rf.state
							rf.mu.Unlock()

							if state != Leader {
								rf.BecomeLeader()
							}
							return
						}

					} else {
						if serverTerm > currentTerm {

							rf.mu.Lock()
							Pf("[%v] VOTER Term greater than Candidate Term [%v] ", rf.me, rf.currentTerm)
							rf.currentTerm = serverTerm
							rf.BecomeFollower()
							rf.mu.Unlock()

							return
						}
					}
				}
			}(server, forTerm)
		}
	}
	return
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	// For 2A
	Term        int
	CandidateId int

	// For 2B
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) GetVote(server int) (bool, int) {

	rf.mu.Lock()

	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex, args.LastLogTerm = rf.GetLastLogData()
	Pf("[%v] args for Getting Vote %v", rf.me, args)

	reply := RequestVoteReply{}
	Pf("[%v] Get vote from [%v] for term [%v] ", rf.me, server, rf.currentTerm)

	//me := rf.me
	//currentTerm := rf.currentTerm

	rf.mu.Unlock()

	//var ok bool
	ok := rf.sendRequestVote(server, &args, &reply)
	for !ok {
		time.Sleep(10 * time.Millisecond)
		ok = rf.sendRequestVote(server, &args, &reply)
	}
	return reply.VoteGranted, reply.Term
}

func (rf *Raft) GetLastLogData() (int, int) {
	index := len(rf.log) - 1
	term := rf.log[index].Term
	return index, term

}
func (rf *Raft) IsMoreUptoDate(args *RequestVoteArgs) bool {
	lastLogIndex, lastLogTerm := rf.GetLastLogData()
	// If the logs have last entry with different terms then the log with later term is
	// more upto date.
	// If logs end with the same term then whichever log us longer is more upto date
	Pf("[%v] lastLogTerm : %v, args LastlogTerm %v; lastLogIndex %v, args lastLogIndex %v", rf.me, lastLogTerm, args.LastLogTerm, lastLogIndex, args.LastLogIndex)
	if args.LastLogTerm == lastLogTerm {
		return args.LastLogIndex >= lastLogIndex
	} else {
		return args.LastLogTerm > lastLogTerm
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	//rf.ResetElectionAlarm() // This is a **HUGE** mistake DO NOT NEED TO RESET ELECTION TIMER ON EVERY REQUEST VOTE
	// THIS NEEDS TO BE DONE ONLY IF VOTE IS GRANTED

	rf.mu.Lock()
	Pf("")
	Pf("")

	// rf.lastReceived = time.Now()
	//isFollower := (rf.state == Follower)
	currentTerm := rf.currentTerm
	isMoreUptoDate := rf.IsMoreUptoDate(args)

	Pf("[%v] REQUEST RPC more up-to-date ? %v ", rf.me, isMoreUptoDate)
	Pf("[%v] Vote requested by [%v] for term [%v] ", rf.me, args.CandidateId, args.Term)
	Pf("[%v] args Term [%v] current Term [%v]  ", rf.me, args.Term, rf.currentTerm)
	Pf("[%v] Voted For [%v] ", rf.me, rf.votedFor)

	res := true
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {

		reply.VoteGranted = false
		res = false

	} else if args.Term == rf.currentTerm {
		if (rf.votedFor == rf.totalServers+1 || rf.votedFor == args.CandidateId) && isMoreUptoDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	} else {
		if isMoreUptoDate {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		}
	}

	Pf("[%v] reply to [%v]  is : %v, %v", rf.me, args.CandidateId, reply.Term, reply.VoteGranted)
	Pf("[%v] REQUEST RPC _______END ", rf.me)
	if res {
		rf.ResetElectionAlarm()
	}

	if args.Term > currentTerm {
		Pf("[%v] args Term  [%v]  is greater than current Term : %v ", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		curState := rf.state
		if curState != Follower {
			Pf("[%v] args Term  [%v]  is greater than current Term : %v So becoming Follower", rf.me, args.Term, rf.currentTerm)
			rf.BecomeFollower()
		}
	}
	rf.mu.Unlock()

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

/////////////////////////////////////////////////////////////////
// Start Agreement
/////////////////////////////////////////////////////////////////

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

func (rf *Raft) CheckCommitIndex() {
	Pf("")
	Pf("")
	Pf("")
	Pf("[%v] Checking Commit Index, match Indexes %v", rf.me, rf.MatchIndex)
	for _, N := range rf.MatchIndex {
		if N > rf.commitIndex {
			count := 0
			for _, N0 := range rf.MatchIndex {
				//rf.mu.Lock()
				Pf("[%v] Log is %v, N : %v, N0 : %v", rf.me, rf.log, N, N0)
				//rf.mu.Unlock()
				if N0 == N && rf.log[N].Term == rf.currentTerm {
					count += 1
				}
			}
			Pf("[%v] Count for %v is %v", rf.me, N, count)
			if count > (rf.totalServers / 2) {
				rf.commitIndex = N
				Pf("[%v] COMMIT INDEX SET TO %v, Commit Index arr %v ", rf.me, rf.commitIndex, rf.MatchIndex)
				return
			}
		}
	}
	Pf("")
	Pf("")
	Pf("")
	return
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	ri := rand.Intn(3000) + 5000
	curState := rf.state
	Pf("[%v] %v       START        ", rf.me, ri)
	Pf("[%v] %v -------- command received : %v state : %v, term : %v , log : %v", rf.me, ri, command, rf.state, rf.currentTerm, rf.log)

	rf.mu.Unlock()

	if curState == Leader {
		rf.mu.Lock()
		// Index at which need to append entry
		index = rf.NextIndex[rf.me]
		term = rf.currentTerm
		forEntry := LogEntry{Term: term, Command: command}
		rf.log = append(rf.log, forEntry)
		Pf("[%v] %v NEW ENTRY APPENDED log is : %v, for Index : %v,  term %v, forEntry %v, nextIndex : %v, matchIndex : %v", rf.me, ri, rf.log, index, term, forEntry, rf.NextIndex, rf.MatchIndex)
		// Pf("Start Agreement for %v", forEntry)
		rf.NextIndex[rf.me] += 1
		rf.MatchIndex[rf.me] += 1
		rf.lastAppliedRpc = time.Now()
		rf.mu.Unlock()

		go rf.StartAgreement(false, ri)

	} else {
		isLeader = false
	}

	Pf("[%v] %v START command received, result : %v, %v, %v", rf.me, ri, index, term, isLeader)

	return index, term, isLeader
}

func (rf *Raft) AppendEntryToLog(command interface{}) {
	//rf.mu.Lock()
	newEntry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, newEntry)
	Pf("[%v] NEW entry appended to log current log is : %v", rf.me, rf.log)
	//rf.mu.Unlock()
}

func (rf *Raft) GetEntries(after int, forServer int, ri int) []LogEntry {

	Pf("[%v] %v Get entries for server %v, after %v", rf.me, ri, forServer, after )
	entries := []LogEntry{}
	for _, entry := range rf.log[after:] {
		entries = append(entries, entry)
	}
	return entries

}

func (rf *Raft) GetNextEntry(server int) []LogEntry {
	return []LogEntry{rf.log[rf.NextIndex[server]]}
}

func (rf *Raft) SendAppendEntry(server int, heartbeat bool, forFailedServer bool, ri int) (bool, int, int, int, int, int) {

	rf.mu.Lock()

	nextIndex := rf.NextIndex[server]
	prevLogIndex := nextIndex - 1
	Pf("[%v] %v NextIndex : %v, prevLogIndex: %v, commitIndex: %v, NextIndexes: %v, log is %v", rf.me, ri, nextIndex, prevLogIndex, rf.commitIndex, rf.NextIndex, rf.log)

	entries := []LogEntry{}
	//if !heartbeat {
		//if forFailedServer {
			entries = rf.GetEntries(rf.NextIndex[server], server, ri)
		//} else {
		//	entries = rf.GetNextEntry(server)
		//}
	//}
	Pf("[%v] %v Next Index : %v, for server :%v, entries : %v ", rf.me, ri, rf.NextIndex[server], server, entries)
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.log[prevLogIndex].Term
	args.Entries = entries
	args.LeaderCommit = rf.commitIndex
	args.Ri = rand.Intn(5000)
	args.Mri = ri

	reply := AppendEntriesReply{}
	Pf("[%v] %v SendingAppendEntries to server [%v] with arguments [%v] for Term [%v] ", rf.me, ri, server, args, args.Term)
	Pf("[%v] %v SendingAppendEntries to server [%v] for Term: %v, with arguments {Term : %v, LeaderId : %v, PrevLogIndex : %v, PrevLogTerm : %v, Entries : %v, LeaderCommit : %v, Ri : %v, Mri: %v}, currentTerm %v", rf.me, ri, server, args.Term, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, args.Ri, args.Mri, rf.currentTerm)

	Pf("")
	state := rf.state
	rf.mu.Unlock()

	ok := rf.SendAppendEntryRPC(server, &args, &reply)

	if !ok {
		//Pf("[%v] %v Not Ok", rf.me, ri)
		for !ok {
			time.Sleep(100 * time.Millisecond)
			rf.mu.Lock()
			Pf("[%v] %v SENDING append entry with args %v, NextIndex %v", rf.me, ri, &args, rf.NextIndex)
			rf.mu.Unlock()

			ok = rf.SendAppendEntryRPC(server, &args, &reply)
		}
	}

	Pf("[%v] %v %v Reply from server %v [%v] is %v for args %v", rf.me, state, ri, server, args.Mri, reply, args)
	Pf("")
	return reply.Success, reply.Term, args.Term, len(args.Entries), args.PrevLogIndex, args.Ri

}

func (rf *Raft) MakeItTrue(server int, ri int) {

	rf.mu.Lock()

	Pf("[%v] %v MAKE IT TRUE", rf.me, ri)
	rf.NextIndex[server] -= 1
	entries := rf.GetEntries(rf.NextIndex[server], server, ri)
	Pf("[%v] %v Sending Append entry for index: %v, with entries : %v, for server : %v ", rf.me, ri, rf.NextIndex[server], entries, server)

	rf.mu.Unlock()

	for {
		success, term, argTerm, entriesLen, prevIndex, ari := rf.SendAppendEntry(server, false, true, ri)
		Pf("[%v] %v REPLY from server %v [%v], for Term : %v, with arg term : %v ", rf.me, ri, success, ari, term, argTerm)
		if !success {
			rf.mu.Lock()

			rf.NextIndex[server] -= 1
			entries = rf.GetEntries(rf.NextIndex[server], server, ri)
			Pf("[%v] %v Next append Entry for server %v, for index %v, with entries : %v ", rf.me, ri, server, rf.NextIndex[server], entries)

			rf.mu.Unlock()
		} else {
			Pf("")
			rf.mu.Lock()
			rf.NextIndex[server] += entriesLen
			rf.MatchIndex[server] = prevIndex + entriesLen

			Pf("[%v] %v %v AFTER SUCCESSFUL APPEND ENTRY FOR SERVER %v, NextIndex %v and MatchIndex %v", rf.me, ri, ari, server, rf.NextIndex, rf.MatchIndex)

			rf.mu.Unlock()
			return
		}

	}
}

func (rf *Raft) StartAgreement(heartbeat bool, ri int) {
	rf.mu0.Lock()
	defer rf.mu0.Unlock()
	
	rf.mu.Lock()
	Pf("-----------------------------------------------------------------")	
	Pf("-----------------------------------------------------------------")	
	Pf("-----------------------------------------------------------------")	
	Pf("-----------------------------------------------------------------")	
	Pf("-----------------------------------------------------------------")	
	Pf("-----------------------------------------------------------------")	
	Pf("-----------------------------------------------------------------")	
	Pf("-----------------------------------------------------------------")	
	Pf("-----------------------------------------------------------------")	
	Pf("-----------------------------------------------------------------")	

	Pf("[%v] %v  START AGREEMENT         ", rf.me, ri)
	forIndex := len(rf.log) - 1 // Because we appended the new entry
	Pf("[%v] %v Start Agreement for Index %v", rf.me, ri, forIndex)

	totalAgreements := 1
	appended := false
	forTerm := rf.currentTerm
	returned := 0
	cond := sync.NewCond(&rf.mu)
	curState := rf.state
	rf.mu.Unlock()

	if curState != Leader {
		return
	}


	for server, _ := range rf.peers {
		if server != rf.me {

			go func(server int, forTerm int) {
				rf.mu.Lock()

				me := rf.me
				curTerm := rf.currentTerm

				rf.mu.Unlock()

				success, term, argTerm, entriesLen, prevIndex, ari := rf.SendAppendEntry(server, heartbeat, false, ri)
				Pf("[%v] %v Reply from : %v, success : %v, term : %v, argTerm : %v, curTerm : %v", me, ri, server, success, term, argTerm, curTerm)

				//rf.mu.Lock()
				//defer rf.mu.Unlock()
				if forTerm == curTerm {
					if success {
						rf.mu.Lock()
						if !heartbeat && rf.NextIndex[rf.me] != rf.NextIndex[server] {
							 
							rf.NextIndex[server] += entriesLen
						}
						rf.MatchIndex[server] = entriesLen + prevIndex
						totalAgreements += 1
						Pf("[%v] %v SUCCESS FROM SERVER %v [%v]", rf.me, ri, server, ari)
						Pf("[%v] %v MATCH INDEX is %v, NEXT INDEX is %v ", rf.me, ri, rf.MatchIndex, rf.NextIndex)
						ta := totalAgreements
						majorityServers := rf.totalServers / 2 + 1
						Pf("[%v] %v RPC Append by [%v] result [%v] now Total agreements [%v] needed for majority [%v] for Term : [%v]", rf.me, ri, server, success, totalAgreements, majorityServers, rf.currentTerm)
						ap := appended
						rf.CheckCommitIndex()
						returned++
						cond.Broadcast()
						rf.mu.Unlock()

						if ta >= majorityServers && !ap {
							rf.mu.Lock()
							appended = true
							Pf("[%v] %v Total agreements(%v) > majority", me, ri, ta)
							Pf("[%v] %v Commiting for %v, log is %v ", rf.me, ri, forIndex, rf.log)
							rf.commitIndex = forIndex // This is wrong
							Pf("[%v] %v LEADER COMMIT INDEX : %v ", rf.me, ri, rf.commitIndex)
							rf.mu.Unlock()

							return
						}
					} else {
						Pf("[%v] %v %v Result from server %v  is FALSE", rf.me, ri, ari, server)
						if term > argTerm {

							rf.mu.Lock()
							Pf("[%v] %v %v Follower (%v) Term > Leader Term ", rf.me, ri, ari, term)
							rf.currentTerm = term
							rf.BecomeFollower()
							returned++
							cond.Broadcast()
							rf.mu.Unlock()

							return

						} else {

							Pf("[%v] %v  %vFailed due to term not matching for the index", rf.me, ri, ari)
							rf.MakeItTrue(server, ri)

							rf.mu.Lock()
							returned++
							cond.Broadcast()
							rf.mu.Unlock()
							return
						}
					}

				}
			}(server, forTerm)

		}

	}

	//for !needToReturn {
	//for !( returned == ((rf.totalServers - 1) / 2 )) {
	for {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		needToReturn := returned >= ((rf.totalServers - 1) / 2)
		rf.mu.Unlock()

		if needToReturn {
			break	
		}
	}
	Pf("``````````````````````````````````````````````````````````````````````````````````````````````````````````````````")	
	Pf("``````````````````````````````````````````````````````````````````````````````````````````````````````````````````")	
	Pf("``````````````````````````````````````````````````````````````````````````````````````````````````````````````````")	
	Pf("``````````````````````````````````````````````````````````````````````````````````````````````````````````````````")	
	Pf("")
	Pf("")
	Pf("")
	//rf.mu0.Unlock()
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).

	// For 2A
	Term     int
	LeaderId int

	// For 2B
	// ForIndex     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	Ri           int
	Mri          int
}

func (rf *Raft) ApplyCommit() {
	for {
		time.Sleep(35 * time.Millisecond)

		rf.mu.Lock()
		//Pf("[%v] APPLY COMMIT LAST APPLIED %v, COMMIT INDEX %v", rf.me, rf.lastApplied, rf.commitIndex)

		if rf.killed() {
			return
		}
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			Pf("[%v] APPLY COMMIT ; %v, commit Index is : %v ", rf.me, rf.lastApplied, rf.commitIndex)
			//Pf("[%v] APPLYING MESSAGE for log %v, lastApplied : %v, apply command : %v", rf.me, rf.log, rf.lastApplied, rf.log[rf.lastApplied].Command)
			Pf("[%v] APPLYING MESSAGE for log %v, lastApplied : %v", rf.me, rf.log, rf.lastApplied)

			newMsg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied}
			rf.applyCh <- newMsg
			Pf("[%v] ----------Commited for index %v, entry %v now log is %v ", rf.me, rf.lastApplied, rf.log[rf.lastApplied], rf.log)
		}
		rf.mu.Unlock()
	}
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Pf("              				 ")
	Pf("[%v] %v  %v       APPEND ENTRIES          %v  ", rf.me, args.Mri, args.Ri, rf.state)
	needToBecomeFollower := args.Term > rf.currentTerm
	currentTerm := rf.currentTerm
	currentState := rf.state
	logLen := len(rf.log) - 1
	// newEntry := LogEntry{}
	entryIndex := args.PrevLogIndex
	entryToAppend := LogEntry{}
	heartbeat := len(args.Entries) == 0
	reply.Success = true

	// If this RPC is not a heartbeat
	if !heartbeat {
		entryIndex = args.PrevLogIndex + 1
		entryToAppend = args.Entries[len(args.Entries)-1]
		// newEntry = args.Entries[0]
	}

	if heartbeat {
		Pf("[%v]  %v %v       HEARTBEAT     ", rf.me, args.Mri, args.Ri)
		Pf("[%v] %v %v HEARTBEAT Received from [%v] with args {Term : %v, LeaderId : %v, PrevLogIndex : %v, PrevLogTerm : %v, Entries : %v, LeaderCommit : %v, Ri : %v}  for log %v, logLen is %v, currentTerm %v", rf.me, args.Mri, args.Ri, args.LeaderId, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, args.Ri, rf.log, logLen, currentTerm)

	} else {
		Pf("[%v] %v  %v NEW APPEND ENTRY Received from [%v] with args {Term : %v, LeaderId : %v, PrevLogIndex : %v, PrevLogTerm : %v, Entries : %v, LeaderCommit : %v, Ri : %v}  for log %v, log len is %v,  currentTerm %v", rf.me, args.Mri, args.Ri, args.LeaderId, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, args.Ri, rf.log, logLen, currentTerm)

	}
	Pf("[%v] %v %v logLen %v, previousLogIndex %v, entryIndex %v, entryToAppend %v", rf.me, args.Mri, args.Ri, logLen, args.PrevLogIndex, entryIndex, entryToAppend)

	// Conditions for returning false
	// Leader Term < Follower's
	// If the Follower's log is smaller than the index we are talking about
	// If Follower's log and Leader's log term differ for the previous Index
	if args.Term < currentTerm || logLen < args.PrevLogIndex {
		reply.Term = currentTerm
		reply.Success = false
		// Pf("___")
		Pf("[%v] %v %v Leader Term < Follower Term: %v %v, LogLen is < PrevLogIndex from Leader : %v", rf.me, args.Mri, args.Ri, args.Term < currentTerm, rf.currentTerm, logLen < args.PrevLogIndex)
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		Pf("[%v] %v %v Term at PrevLogIndex Does not match, Follower Term: %v, Leader Term: %v, for Index: %v ", rf.me, args.Mri, args.Ri, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm, args.PrevLogIndex)
		// Pf("++++")
		reply.Term = currentTerm
		reply.Success = false
	}

	if needToBecomeFollower {
		rf.currentTerm = args.Term
	}

	if currentState == Candidate || needToBecomeFollower {
		Pf("[%v] %v %v CURRENT STATE %v, Need To Become Follower: %v", rf.me, args.Mri, args.Ri, currentState, needToBecomeFollower)
		rf.BecomeFollower()
	}

	if reply.Success == false {
		return
	}

	// Delete the conflicting entries

		//while
		//if logLen >= entryIndex && !heartbeat {
		if (logLen >= entryIndex && entryIndex != 0) {
			Pf("[%v] %v %v LogLen >= entryIndex: %v, heartbeat: %v", rf.me, args.Mri, args.Ri, logLen >= entryIndex, !heartbeat)
			Pf("[%v] %v %v Log is : [%v], entryIndex is %v, entryToAppend %v, commit Index is  %v ", rf.me, args.Mri, args.Ri, rf.log, entryIndex, entryToAppend, rf.commitIndex)
			if rf.log[entryIndex].Term != entryToAppend.Term {
				Pf("[%v] %v %v Deleting Conflicting Entries", rf.me, args.Mri, args.Ri)
				rf.log = rf.log[:args.PrevLogIndex+1]
				Pf("[%v] %v  %v Modified log is %v", rf.me, args.Mri, args.Ri, rf.log)
			} else if len(args.Entries) > 0{
				args.Entries = args.Entries[1:]
				Pf("[%v] %v %v NEW ENTRIES : %v", rf.me, args.Mri, args.Ri, args.Entries)
			}
		}

	//if reply.Success == true {
		for _, entry := range args.Entries {
			rf.log = append(rf.log, entry)
			Pf("[%v] %v %v Appended a new entry %v now log is %v", rf.me, args.Mri, args.Ri, entry, rf.log)
		}

		if args.LeaderCommit > rf.commitIndex {
			Pf("[%v] %v %v Leader commit Index > commit Index: %v, %v ", rf.me, args.Mri, args.Ri, args.LeaderCommit, rf.commitIndex)

			// math.Min needs a float64 but I have int so instead using if else
			if args.LeaderCommit >= args.PrevLogIndex+1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = args.PrevLogIndex
			}
			Pf("[%v] %v %v UPDATED COMMIT INDEX, Leader Commit index : %v, follower commit Index %v, log %v", rf.me, args.Mri, args.Ri, args.LeaderCommit, rf.commitIndex, rf.log)
		}
	//}

	reply.Term = currentTerm

	Pf("[%v] %v %v After Appending entries from [%v] log is : [%v] and reply is : %v, commit Index : %v", rf.me, args.Mri, args.Ri, args.LeaderId, rf.log, reply, rf.commitIndex)
	Pf("[%v] %v %v     END APPEND ENTRIES       ", rf.me, args.Mri, args.Ri)
	rf.ResetElectionAlarm()
	return
}

//
// example code to send a AppendEntries RPC to a server.
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
func (rf *Raft) SendAppendEntryRPC(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

////////////////////////////////////////////////////////////////////////////////
// Make
////////////////////////////////////////////////////////////////////////////////
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
	rf.applyCh = applyCh

	rf.raftId = rand.Intn(5000) // Created for debugging purposes

	Pf("[%v] Bought to life with raftId : [%v]", rf.me, rf.raftId)
	// Your initialization code here (2A, 2B, 2C).

	// For 2A  only

	rf.totalServers = len(peers)
	rf.currentTerm = 1
	rf.votedFor = rf.totalServers + 1
	rf.lastReceived = time.Now()

	// For 2B
	rf.log = []LogEntry{}
	rf.commitIndex = 0 // To be initialized at 0 because log index starts from 1
	rf.lastApplied = 0

	go rf.NewFollower()
	go rf.ApplyCommit()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

////////////////////////////////////////////////////////////////////////////////
// Lab 2B - 2C
////////////////////////////////////////////////////////////////////////////////

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()

	Pf("0000000000000000000000000000000000000000")
	Pf("[%v] Asking State, current state is [%v] and term is [%v] with raftId [%v], log is : %v", rf.me, rf.state, rf.currentTerm, rf.raftId, rf.log)
	Pf("[%v]  Time since last RPC [%v] was expected [%v] current state [%v] ", rf.me, time.Now().Sub(rf.lastReceived), rf.electionAlarm, rf.state)
	Pf("0000000000000000000000000000000000000000")

	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

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
	Pf("###################### KILL CALLED ##############################")
	Pf("###################### KILL CALLED ##############################")
	Pf("###################### KILL CALLED ##############################")
	Pf("###################### KILL CALLED ##############################")
	Pf("###################### KILL CALLED ##############################")
	Pf("###################### KILL CALLED ##############################")
	Pf("###################### KILL CALLED ##############################")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
