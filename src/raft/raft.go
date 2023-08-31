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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

type State int

const (
	Leader = 1
	Follower = 2
	Candidate = 3
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // Latest term server has seen
	votedFor int // Candidate ID that received vote in current term
	log []LogEntry // Log entries
	state State

	noOfVotes int
	stepdownCh chan bool
	wonElectionCh chan bool
	receivedCommCh chan bool

	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
	noOfCopies map[int]int // Tells how many servers a copy of a command has been replicated on
	applyCh chan ApplyMsg

	testName string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	////rf.print("About to get my state")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	////rf.print("Got my state")
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
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


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	
	if args.Term > rf.currentTerm {
		lastLogTerm := rf.log[len(rf.log) - 1].Term
		lastLogIndex := len(rf.log) - 1
		if lastLogTerm != args.LastLogTerm {
			if lastLogTerm > args.LastLogTerm {
				////rf.print("Rejecting request vote from server ", args.CandidateId, " due to incomplete logs")
				return
			}
		} else {
			if lastLogIndex > args.LastLogIndex {
				////rf.print("Rejecting request vote from server ", args.CandidateId, " due to incomplete logs")
				return
			}
		}
		////rf.print("Granting vote to server ", args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}

	if rf.state != Follower && args.Term > rf.currentTerm {
		////rf.print("Stepping down in request vote after encounter with ", args.CandidateId)
		rf.currentTerm = args.Term
		select {
		case rf.stepdownCh <- true:
			////rf.print("Sent it: Stepping down in request vote after encounter with ", args.CandidateId)
		default:
			rf.state = Follower
			////rf.print("Changing the state directly in RequestVote")
		}
	}
	if reply.VoteGranted {
		if rf.state == Follower {
			rf.currentTerm = args.Term
			////rf.print("Recving comm in request vote from ", args.CandidateId)
			select {
			case rf.receivedCommCh <- true:
				////rf.print("Sent it: Recving comm in request vote from ", args.CandidateId)
			default:
				////rf.print("Too late. Another election about to start")
			}
				
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	var reply RequestVoteReply

	success := false
	for !success && !rf.killed() {
		success = rf.peers[server].Call("Raft.RequestVote", args, &reply)
		
		if !success {
			////rf.print("Failed to send request vote to server ", server, " Retrying")
		}
	}
	if !success {
		return
	}
	////rf.print("Successfully sent request vote to server ", server)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// The request votes are no longer applicable, they're behind the times
	if args.Term != rf.currentTerm || rf.state != Candidate {
		return
	}
	if reply.Term > rf.currentTerm {
		////rf.print(reply.Term, " > ", rf.currentTerm, " in sendRequestVote")
		////rf.print("Sending stepdown after sending request vote and receiving a higher term from server ", server)
		rf.currentTerm = reply.Term
		select {
		case rf.stepdownCh <- true:
			////rf.print("Sent it: Sending stepdown after sending request vote and receiving a higher term from server ", server)
		default:
			rf.state = Follower
			////rf.print("Directly changing state in sendRequestVote")
		}
	}
	if reply.VoteGranted {
		rf.noOfVotes += 1
		if rf.hasMajorityVotes() {
			////rf.print("Sending wonElection after recving vote from ", server)
			rf.state = Leader
			rf.nextIndex = []int{}
			rf.matchIndex = []int{}
			for range rf.peers {
				rf.nextIndex = append(rf.nextIndex, len(rf.log))
				rf.matchIndex = append(rf.matchIndex, 0)
			}
			select {
			case rf.wonElectionCh <- true:
				////rf.print("Sent it: Sending wonElection after recving vote from ", server)
			default:
				////rf.print("Directly changing state to Leader")
			}
		}
	}
}

func (rf *Raft) hasMajorityVotes() bool {
	return rf.noOfVotes == (len(rf.peers) / 2) + 1
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	Entries []LogEntry
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if len(args.Entries) != 0 {
		//rf.print("Received append entries ", args.Entries, " from leader ", args.LeaderId)
	}
	if rf.state == Follower {
		rf.notifyCommRevdFromLeader(args)
	} else {
		////rf.print("Stepping down after receiving append entry from ", args.LeaderId)
		rf.currentTerm = args.Term
		select {
		case rf.stepdownCh <- true:
			////rf.print("Sent it: Stepping down after appending entry from ", args.LeaderId)
		default:
			////rf.print("Directly changing state in AppendEntries")
			rf.state = Follower
		}
	}
	rf.currentTerm = args.Term

	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		if len(args.Entries) != 0 {
			//rf.print("Server with log ", rf.log, " Rejecting append entries ", args.Entries, " with args PrevLogIndex:", args.PrevLogIndex, " and PrevLogTerm: ", args.PrevLogTerm)
		}
		return
	}

	reply.Success = true

	newLog := []LogEntry{}
	////rf.print("hdhdhdh", rf.log)
	for i := 0; i <= args.PrevLogIndex; i++ {
		newLog = append(newLog, rf.log[i])
	}
	rf.log = newLog

	for i := range args.Entries {
		rf.log = append(rf.log, args.Entries[i])
	}

	if args.LeaderCommit > rf.commitIndex {
		min := args.LeaderCommit
		lastNewEntryIndex := len(rf.log) - 1
		if lastNewEntryIndex < min {
			min = lastNewEntryIndex
		}
		//rf.print("Updating rf.commitIndex from ", rf.commitIndex, " to ", min)
		rf.commitIndex = min
	}

	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied += 1
		toapply := ApplyMsg {
			CommandValid: true,
			Command: rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		//rf.print("Applying the entry ", rf.log[rf.lastApplied])
		rf.applyCh <- toapply
	}
}

func (rf *Raft) notifyCommRevdFromLeader(args *AppendEntriesArgs) {
	////rf.print("Recving comm after appending entry from ", args.LeaderId)
	select {
	case rf.receivedCommCh <- true:
		////rf.print("Sent it: Recving comm after appending entry from ", args.LeaderId)
	default:
		// Do nothing
		// It's too late
		// Another election is going to start
		////rf.print("Too late. Another election is about to start")
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	for {
		reply := &AppendEntriesReply{}
		/*////rf.print("I the leader: my log: ", rf.log)
		////rf.print("About to send entries ", args.Entries, "with prevLogTerm", args.PrevLogTerm,
			"and prevLogIndex", args.PrevLogIndex, "to server", server)*/
		if len(args.Entries) != 0 {
			//rf.print("Sending append entries ", args.Entries, " to server ", server, " with args ", *args)
		}
		success := false
		for !success && !rf.killed() {
			success = rf.peers[server].Call("Raft.AppendEntries", args, reply)
			
			if !success {
				////rf.print("Failed to send append entries to server ", server, " Retrying")
				continue
			}
		}
		if !success {
			return
		}
		if len(args.Entries) != 0 {
			//rf.print("Successfully sent append entries to server ", server)
		}
		
		rf.mu.Lock()

		if args.Term != rf.currentTerm || rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if reply.Term > args.Term {
			////rf.print("Stepping down after sending append entry to ", server)
			rf.currentTerm = reply.Term
			select {
			case rf.stepdownCh <- true:
				// The state change will happen in the main loop
				////rf.print("Sent it: Stepping down after sending append entry to ", server)
			default:
				// The main loop is no longer listening for the stepdownCh channel
				// because it's time for a new heartbeat
				// Set state to follower here and when the heartbeat routine
				// acquires the lock, it will abort operation, seeing that it's
				// no longer the leader
				rf.state = Follower
				////rf.print("Directly changing state in the sendAppendEntries because of blocked channel")
			}
			rf.mu.Unlock()
			return
		}
		if len(args.Entries) != 0 {
			if reply.Success {
				cmdIndex := args.PrevLogIndex + 1
				rf.noOfCopies[cmdIndex] += 1
				//rf.print("Append entries to server ", server, " was successful", " no of copies with the command at ", cmdIndex, " is now ", rf.noOfCopies[cmdIndex])
				if rf.noOfCopies[cmdIndex] == len(rf.peers) / 2 + 1 {
					rf.commitIndex = max(cmdIndex, rf.commitIndex)
					for rf.commitIndex > rf.lastApplied {
						rf.lastApplied += 1
						toapply := ApplyMsg {
							CommandValid: true,
							Command: rf.log[rf.lastApplied].Command,
							CommandIndex: rf.lastApplied,
						}
						////rf.print("Applying entry ", rf.log[rf.lastApplied])
						rf.applyCh <- toapply
					}
				}
				rf.nextIndex[server] = rf.nextIndex[server] + 1
				rf.matchIndex[server] = max(rf.matchIndex[server], cmdIndex)
				if len(rf.log) - 1 < rf.nextIndex[server] {
					rf.mu.Unlock()
					return
				}
				args.Entries = []LogEntry{rf.log[rf.nextIndex[server]]}
				args.PrevLogIndex = rf.nextIndex[server] - 1
				args.PrevLogTerm = rf.log[rf.nextIndex[server] - 1].Term
				rf.mu.Unlock()
			} else {
				//rf.print("Server ", server, " doenst have the required entry. Reducing nextIndex from ", rf.nextIndex[server], " to ", rf.nextIndex[server] - 1, " and retrying")
				rf.nextIndex[server] -= 1
				args.Entries = []LogEntry{rf.log[rf.nextIndex[server]]}
				args.PrevLogIndex = rf.nextIndex[server] - 1
				args.PrevLogTerm = rf.log[rf.nextIndex[server] - 1].Term
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
			return
		}
	}
}

func min(a int, b int) int {
	m := a
	if b < a {
		m = b
	}
	return m
}

func max(a int, b int) int {
	m := a
	if b > a {
		m = b
	}
	return m
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	term, index, isLeader = rf.currentTerm, len(rf.log), true

	//rf.print("Received command ", command, " from client")
	////rf.print("About to send append entries to replicate command ", command)
	args := &AppendEntriesArgs {
		Term: rf.currentTerm,
		LeaderId: rf.me,
		Entries: []LogEntry{ { Command: command, Term: rf.currentTerm } },
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm: rf.log[len(rf.log) - 1].Term,
		LeaderCommit: rf.commitIndex,
	}
	entry := LogEntry{ Command: command, Term:  rf.currentTerm }
	rf.log = append(rf.log, entry)
	rf.noOfCopies[len(rf.log) - 1] = 1

	for i := range rf.peers {
		if i != rf.me && rf.nextIndex[i] == len(rf.log) - 1 {
			go rf.sendAppendEntries(i, args)
		}
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) candidateStepdown() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Candidate {
		return
	}
	rf.state = Follower
}

func (rf *Raft) leaderStepdown() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return
	}
	rf.state = Follower
}

func (rf *Raft) runElection(args *RequestVoteArgs) {
	////rf.print("About to create args for running election")
	////rf.print("Finished creating args for running election")
	for i, _ := range rf.peers {
		if i != rf.me {
			////rf.print("Sending vote request to server ", i)
			go rf.sendRequestVote(i, args)
		}
	}
}

func (rf *Raft) initNewElection() *RequestVoteArgs {
	if rf.state != Candidate {
		////rf.print("State is no longer Candidate. Aborting election")
		return nil
	}
	////rf.print("Creating args for running election")
	rf.currentTerm += 1
	rf.noOfVotes = 1
	rf.votedFor = rf.me
	return &RequestVoteArgs {
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm: rf.log[len(rf.log) - 1].Term,
	}
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	if rf.state != Leader {
		////rf.print("State is no longer Leader. Aborting heartbeat")
		rf.mu.Unlock()
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			Entries: []LogEntry{},
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm: rf.log[rf.nextIndex[i] - 1].Term,
			LeaderCommit: rf.commitIndex,
		}
		go rf.sendAppendEntries(i, args)
	}
	rf.mu.Unlock()
}

func (rf *Raft) main() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			////rf.print("Sending heartbeat")
			rf.heartbeat()
			select {
			case <-time.After(heartbeatTime()):
			case <-rf.stepdownCh:
				////rf.print("Stepping down !")
				rf.leaderStepdown();
			}
		case Follower:
			select {
			case <-time.After(electionTimeout()):
				////rf.print("Starting election")
				rf.mu.Lock()
				//rf.print("Oh shit, becoming a candidate")
				rf.state = Candidate
				args := rf.initNewElection()
				rf.mu.Unlock()
				if args != nil {
					rf.runElection(args)
				}
			case <-rf.receivedCommCh:
				continue
			}
		case Candidate:
			select {
			case <-time.After(electionTimeout()):
				////rf.print("Starting election")
				rf.mu.Lock()
				args := rf.initNewElection()
				rf.mu.Unlock()
				if args != nil {
					rf.runElection(args)
				}
			case <-rf.stepdownCh:
				////rf.print("Stepping down !")
				rf.candidateStepdown()
			case <-rf.wonElectionCh:
				////rf.print("I Won Election and was informed in the channel!")
			}
		}
	}
}

func (rf *Raft) print(a ...interface{}) {
	stateStr := "Leader"
	if rf.state == Follower {
		stateStr = "Follower"
	}
	if rf.state == Candidate {
		stateStr = "Candidate"
	}
	print("Server ", rf.me, " in state ", stateStr, " and term, ", rf.currentTerm,
		"with log ", rf.stringifyLog(), " and commit index ", rf.commitIndex, " in test ", rf.testName,": ")
	fmt.Println(a...)
	print("\n")
}

func (rf *Raft) stringifyLog() string {
	s := "[ "
	for _, val := range rf.log {
		s = fmt.Sprintf("%s %v", s, val)
	}
	return s + " ]"
}

func heartbeatTime() time.Duration {
	return time.Millisecond * 150
}

func electionTimeout() time.Duration {
	ms := 500 + (rand.Int63() % 300)
	return time.Millisecond * time.Duration(ms)
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg, testName string) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.receivedCommCh = make(chan bool)
	rf.stepdownCh = make(chan bool)
	rf.wonElectionCh = make(chan bool)

	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = []LogEntry{ { Command: 0, Term: -1 } }
	rf.noOfCopies = make(map[int]int)
	rf.noOfCopies[0] = len(rf.peers)
	rf.matchIndex = []int{}
	rf.nextIndex = []int{}
	rf.testName = testName
	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, 1)
	}


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.main()


	return rf
}
