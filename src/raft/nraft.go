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
	rf.mu.Unlock()
	rf.ResetElectionAlarm()
	rf.StartElectionCountdown()
}

func (rf *Raft) BecomeFollower() {
	if rf.killed() {
		Pf("[%v]###################### KILL CALLED Become Follower DEAD NOW  ##############################", rf.raftId)
		return
	}

	rf.mu.Lock()
	Pf("[%v] Asked to ____BECOME FOLLOWER____ for term [%v] ", rf.me, rf.currentTerm)
	rf.state = Follower
	rf.mu.Unlock()
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

	for i := 0; i < rf.totalServers; i++ {
		rf.NextIndex = append(rf.NextIndex, len(rf.log))
		rf.MatchIndex = append(rf.MatchIndex, 0)
	}
	Pf("[%v] NextIndex and MatchIndex are : %v, %v", rf.me, rf.NextIndex, rf.MatchIndex)
	rf.mu.Unlock()

	rf.Heartbeat(ri)

	// TODO : Start sending heartbeats
	for {
		time.Sleep(50 * time.Millisecond)

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

			rf.Heartbeat(ri)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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

	rf.mu.Unlock()

	rf.ResetElectionAlarm()
	totalVotes := 1 // Voted for self

	for server, _ := range rf.peers {
		if server != me {
			go func(server int) {
				if rf.killed() {
					// Pf("[%v]###################### KILL CALLED REQUEST VOTE DEAD NOW  ##############################", rf.raftId)
					return
				}

				voteGranted, serverTerm := rf.GetVote(server)
				rf.mu.Lock()
				currentTerm := rf.currentTerm

				rf.mu.Unlock()
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
						rf.mu.Unlock()
						rf.BecomeLeader()
						return
					}

				} else {
					if serverTerm > currentTerm {

						rf.mu.Lock()
						Pf("[%v] VOTER Term greater than Candidate Term [%v] ", rf.me, rf.currentTerm)
						rf.currentTerm = serverTerm
						rf.mu.Unlock()

						rf.BecomeFollower()
						return
					}
				}
			}(server)
		}
	}
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
	rf.mu.Unlock()

	if res {
		rf.ResetElectionAlarm()
	}
	if args.Term > currentTerm {
		rf.mu.Lock()
		Pf("[%v] args Term  [%v]  is greater than current Term : %v ", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		curState := rf.state
		rf.mu.Unlock()
		if curState != Follower {
			Pf("[%v] args Term  [%v]  is greater than current Term : %v So becoming Follower", rf.me, args.Term, rf.currentTerm)
			rf.BecomeFollower()
		}
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

/////////////////////////////////////////////////////////////////
// Leader
/////////////////////////////////////////////////////////////////

func (rf *Raft) Heartbeat(ri int) {
	rf.mu.Lock()

	me := rf.me
	Pf("[%v] %v SENDING HEARTBEAT", me, ri)
	// timeSince := time.Since(rf.lastAppliedRpc)
	//Pf("TImeSince %v", timeSince)
	rf.mu.Unlock()

	// for timeSince > 100*time.Millisecond {
	// Sleep for any time less than 150ms because the range for timeout of
	// followers is 150-300. As the no of servers increase one should decrease
	// the heartbeat sending time

	// time.Sleep(195 * time.Millisecond)
	//Pf("000000 %v", time.Now())

	rf.mu.Lock()

	forTerm := rf.currentTerm

	rf.mu.Unlock()

	for server, _ := range rf.peers {
		if server != me {
			go func(server int, forTerm int) {
				rf.mu.Lock()

				nextIndex := rf.NextIndex[server]
				entries := []LogEntry{}
				Pf("[%v] %v nextIndex : %v, log is %v", rf.me, ri, nextIndex, rf.log)
				curTerm := rf.currentTerm

				rf.mu.Unlock()

				success, term, argTerm := rf.SendAppendEntry(server, entries, ri)

				// If the reply is for an older term then no need to become follower
				if !success && (forTerm == curTerm) {
					if term > argTerm {

						rf.mu.Lock()
						Pf("[%v] %v Did not get a successful result for Term %v, current Term %v So Becoming follower ", rf.me, ri, term, curTerm)
						rf.currentTerm = term
						state := rf.state
						rf.mu.Unlock()
						if state != Follower {
							rf.BecomeFollower()
						}
					} else if term == argTerm {
						rf.mu.Lock()
						Pf("[%v] %v HEARTBEAT FAILED and term == argTerm", rf.me, ri)
						rf.mu.Unlock()
						rf.MakeItTrue(server, ri)
					}
				}
			}(server, forTerm)
		}
	}
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
		index = len(rf.log)
		term = rf.currentTerm
		forEntry := LogEntry{Term: term, Command: command}
		rf.log = append(rf.log, forEntry)
		Pf("[%v] %v NEW ENTRY APPENDED log is : %v, for Index : %v,  term %v, forEntry %v, nextIndex : %v, matchIndex : %v", rf.me, ri, rf.log, index, term, forEntry, rf.NextIndex, rf.MatchIndex)
		// Pf("Start Agreement for %v", forEntry)
		rf.lastAppliedRpc = time.Now()
		rf.mu.Unlock()

		rf.StartAgreement(false, ri)
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

func (rf *Raft) GetEntries(after int) []LogEntry {
	entries := []LogEntry{}
	for _, entry := range rf.log[after:] {
		entries = append(entries, entry)
	}
	return entries

}

func (rf *Raft) SendAppendEntry(server int, withEntries []LogEntry, ri int) (bool, int, int) {

	rf.mu.Lock()

	nextIndex := rf.NextIndex[server]
	prevLogIndex := nextIndex - 1
	Pf("[%v] %v  nextIndex : %v, prevLogIndex: %v, commitIndex: %v, NextIndexes: %v, log is %v", rf.me, ri, nextIndex, prevLogIndex, rf.commitIndex, rf.NextIndex, rf.log)

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.log[prevLogIndex].Term
	args.Entries = withEntries
	args.LeaderCommit = rf.commitIndex
	args.Ri = rand.Intn(5000)
	args.Mri = ri

	reply := AppendEntriesReply{}
	Pf("[%v] %v SendingAppendEntries to server [%v] with arguments [%v] for Term [%v] ", rf.me, ri, server, args, args.Term)
	Pf("[%v] %v SendingAppendEntries to server [%v] for Term: %v, with arguments {Term : %v, LeaderId : %v, PrevLogIndex : %v, PrevLogTerm : %v, Entries : %v, LeaderCommit : %v, Ri : %v, Mri: %v}, currentTerm %v", rf.me, ri, server, args.Term, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, args.Ri, args.Mri, rf.currentTerm)

	rf.mu.Unlock()

	ok := rf.SendAppendEntryRPC(server, &args, &reply)

	if !ok {
		for !ok {
			time.Sleep(10 * time.Millisecond)
			ok = rf.SendAppendEntryRPC(server, &args, &reply)
		}
	}

	Pf("[%v] %v Reply from server %v is %v for args %v", rf.me, ri, server, reply, args)
	return reply.Success, reply.Term, args.Term

}

func (rf *Raft) MakeItTrue(server int, ri int) {

	rf.mu.Lock()

	rf.NextIndex[server] -= 1
	entries := rf.GetEntries(rf.NextIndex[server])

	rf.mu.Unlock()

	for {
		success, _, _ := rf.SendAppendEntry(server, entries, ri)
		if !success {
			rf.mu.Lock()

			rf.NextIndex[server] -= 1
			entries = rf.GetEntries(rf.NextIndex[server])

			rf.mu.Unlock()
		} else {
			return
		}

	}
}

func (rf *Raft) StartAgreement(heartbeat bool, ri int) {
	rf.mu.Lock()

	Pf("[%v] %v  START AGREEMENT         ", rf.me, ri)
	forIndex := len(rf.log) - 1 // Because we appended the new entry
	Pf("[%v] %v Start Agreement for Index %v", rf.me, ri, forIndex)
	rf.mu.Unlock()
	totalAgreements := 1
	for server, _ := range rf.peers {
		if server != rf.me {

			go func(server int) {
				rf.mu.Lock()
				rf.NextIndex[server] -= 1
				entries := rf.GetEntries(rf.NextIndex[server])
				me := rf.me
				Pf("[%v] %v Next Index : %v, for server :%v, entries : %v ", rf.me, ri, rf.NextIndex[server], server, entries)
				rf.mu.Unlock()

				success, term, argTerm := rf.SendAppendEntry(server, entries, ri)
				Pf("[%v] %v Reply from : %v, success : %v, term : %v, argTerm : %v", me, ri, server, success, term, argTerm)

				if success {
					rf.mu.Lock()
					rf.NextIndex[server] += 1
					totalAgreements += 1
					ta := totalAgreements
					majorityServers := rf.totalServers/2 + 1
					Pf("[%v] %v RPC Append by [%v] result [%v] now Total agreements [%v] needed for majority [%v] for Term : [%v]", rf.me, ri, server, success, totalAgreements, majorityServers, rf.currentTerm)
					rf.mu.Unlock()

					if ta >= majorityServers {
						rf.mu.Lock()
						Pf("[%v] %v Total agreements(%v) > majority", me, ri, ta)
						Pf("[%v] %v Commiting for %v, log is %v ", rf.me, ri, forIndex, rf.log)
						rf.commitIndex = forIndex
						newMsg := ApplyMsg{CommandValid: true, Command: rf.log[rf.NextIndex[server]].Command, CommandIndex: forIndex}
						rf.mu.Unlock()

						rf.applyCh <- newMsg

						return
					}
				} else {
					Pf("[%v] %v Result from server (%v) is FALSE", rf.me, ri, server)
					if term > argTerm {

						rf.mu.Lock()
						Pf("[%v] %v Follower (%v) Term > Leader Term ", rf.me, ri, term)
						rf.currentTerm = argTerm
						rf.mu.Unlock()

						rf.BecomeFollower()
						return

					} else {

						Pf("[%v] %v Failed due to term not matching for the index", rf.me, ri)
						rf.MakeItTrue(server, ri)
						return
					}
				}
			}(server)

			//	go func(server int) {

			//		reply := AppendEntriesReply{}
			//		for !reply.Success {
			//			time.Sleep(20 * time.Millisecond)
			//			rf.mu.Lock()

			//			nextIndex := rf.NextIndex[server]
			//			prevLogIndex := nextIndex - 1
			//			Pf("[%v] %v nextIndex : %v, prevLogIndex: %v, log is %v", rf.me, ri, nextIndex, prevLogIndex, rf.log)
			//			args := AppendEntriesArgs{}
			//			args.Term = rf.currentTerm
			//			args.LeaderId = rf.me
			//			//args.PrevLogIndex = prevLogIndex
			//			args.PrevLogIndex = prevLogIndex
			//			args.PrevLogTerm = rf.log[prevLogIndex].Term
			//			if !heartbeat{
			//				args.Entries = rf.GetEntries(nextIndex)
			//			} else {
			//				args.Entries = []LogEntry{}
			//			}

			//			args.LeaderCommit = rf.commitIndex
			//			args.Ri = rand.Intn(5000)
			//			args.Mri = ri

			//			// args.ForIndex = forIndex

			//			//nextIndex := len(rf.log)
			//			Pf("[%v] %v log is %v, commit Index is %v", rf.me, ri, rf.log, rf.commitIndex)
			//			Pf("[%v] %v SendingAppendEntries to server [%v] with arguments [%v] for Term [%v] ", rf.me, ri, server, args, rf.currentTerm)
			//			Pf(" Leader %v current log is : %v", rf.me, rf.log)
			//			rf.mu.Unlock()

			//			// Send Append Entry to this server and if the RPC is unsuccessful then keep trying sending
			//			ok := rf.SendAppendEntryRPC(server, &args, &reply)

			//			if !ok {
			//				for !ok {
			//					time.Sleep(10 * time.Millisecond)
			//					ok = rf.SendAppendEntryRPC(server, &args, &reply)
			//				}
			//			}
			//				if reply.Success {
			//					rf.mu.Lock()
			//					rf.NextIndex[server] += 1
			//					totalAgreements += 1
			//					ta := totalAgreements
			//					majorityServers := rf.totalServers/2 + 1
			//					Pf("[%v] %v RPC Append by [%v] result [%v] now Total agreements [%v] needed for majority [%v] for Term : [%v]", rf.me, ri, server, reply.Success, totalAgreements, majorityServers, rf.currentTerm)
			//					rf.mu.Unlock()

			//					if ta >= majorityServers {
			//						rf.mu.Lock()
			//						Pf("[%v] %v Commited for %v, log is %v ", rf.me, ri, prevLogIndex+1, rf.log)
			//						rf.commitIndex = forIndex

			//						newMsg := ApplyMsg{CommandValid: true, Command: rf.log[nextIndex].Command, CommandIndex: forIndex}

			//						rf.mu.Unlock()
			//						rf.applyCh <- newMsg

			//					}

			//				} else {
			//					Pf("")
			//					Pf("")
			//					Pf("")
			//					Pf("[%v] %v Reply from server %v is %v for args %v", args.LeaderId, ri, server, reply, args)
			//					Pf("")
			//					Pf("")
			//					Pf("")

			//					rf.mu.Lock()
			//					rplyTerm := reply.Term
			//					rf.mu.Unlock()
			//					if rplyTerm > args.Term {
			//						rf.mu.Lock()
			//						rf.currentTerm = reply.Term
			//						rf.mu.Unlock()
			//						rf.BecomeFollower()
			//						return
			//					}

			//					rf.mu.Lock()
			//					rf.NextIndex[server] -= 1
			//					rf.mu.Unlock()

			//				}
			//		}

			//	}(server)
		}

	}

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
		time.Sleep(15 * time.Millisecond)

		rf.mu.Lock()
		//Pf("[%v] APPLY COMMIT LAST APPLIED %v, COMMIT INDEX %v", rf.me, rf.lastApplied, rf.commitIndex)

		if rf.killed() {
			return
		}
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
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

	rf.mu.Unlock()

	if currentState == Candidate || needToBecomeFollower {
		Pf("[%v] %v %v CURRENT STATE %v, Need To Become Follower: %v", rf.me, args.Mri, args.Ri, currentState, needToBecomeFollower)
		rf.BecomeFollower()
	}

	if reply.Success == false {
		return
	}

	rf.mu.Lock()
	// Delete the conflicting entries
	if logLen >= entryIndex && !heartbeat {
		Pf("[%v] %v %v LogLen >= entryIndex: %v, heartbeat: %v", rf.me, args.Mri, args.Ri, logLen >= entryIndex, !heartbeat)
		Pf("[%v] %v %v Log is : [%v], entryIndex is %v, entryToAppend %v, commit Index is  %v ", rf.me, args.Mri, args.Ri, rf.log, entryIndex, entryToAppend, rf.commitIndex)
		if rf.log[entryIndex].Term != entryToAppend.Term {
			Pf("[%v] %v %v Deleting Conflicting Entries", rf.me, args.Mri, args.Ri)
			rf.log = rf.log[:args.PrevLogIndex]
			Pf("[%v] %v  %v Modified log is %v", rf.me, args.Mri, args.Ri, rf.log)
		}
	}

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

	reply.Term = currentTerm
	reply.Success = true
	rf.mu.Unlock()

	rf.mu.Lock()
	Pf("[%v] %v %v After Appending entries from [%v] log is : [%v] and reply is : %v", rf.me, args.Mri, args.Ri, args.LeaderId, rf.log, reply)
	Pf("[%v] %v %v     END APPEND ENTRIES       ", rf.me, args.Mri, args.Ri)
	rf.mu.Unlock()
	rf.ResetElectionAlarm()
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
	Pf("[%v] Asking State, current state is [%v] and term is [%v] with raftId [%v]", rf.me, rf.state, rf.currentTerm, rf.raftId)
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
