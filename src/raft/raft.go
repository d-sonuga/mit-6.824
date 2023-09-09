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
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"log"

	"6.5840/labgob"
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

// Contains either a command or a log snapshot
type LogEntry struct {
	Command interface{}
	Term    int
	Index   int

	// nil if this is a command
	Snapshot []byte
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
	commWithFollowers chan bool
	newCommitedEntriesCh chan bool

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

	//rf.print("About to get my state")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == Leader

	//rf.print("Got my state")
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist(snapshot []byte) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log[1:])
	raftstate := w.Bytes()
	if snapshot == nil {
		rf.persister.Save(raftstate, rf.persister.snapshot)
	} else {
		rf.persister.Save(raftstate, snapshot)
	}
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var entries []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&entries) != nil {
			log.Fatal("Error!!! Failed to read persistent state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = entries
	}
}

func (rf *Raft) lastLogIndex() int {
	latestEntry := rf.log[len(rf.log)-1]
	return latestEntry.Index
}

func (rf *Raft) lastLogTerm() int {
	latestEntry := rf.log[len(rf.log)-1]
	return latestEntry.Term
}

func (rf *Raft) entryWithIndex(index int) (*LogEntry, int) {
	at := sort.Search(len(rf.log), func(i int) bool { return rf.log[i].Index >= index })
	if at < len(rf.log) && rf.log[at].Index == index {
		return &rf.log[at], at
	}
	return nil, -1
	
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastEntryInSnapshot, sliceIndex := rf.entryWithIndex(index)
	snapshotEntry := LogEntry { Index: index, Term: lastEntryInSnapshot.Term, Snapshot: snapshot }
	toKeep := rf.log[sliceIndex+1:]
	rf.log = []LogEntry{}
	rf.log = append(rf.log, snapshotEntry)
	rf.log = append(rf.log, toKeep...)
	rf.persist(snapshot)
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
	defer func () {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			if rf.state != Follower {
				rf.convertToFollower(args.CandidateId)
			}
		}
		if reply.VoteGranted && rf.state == Follower {
			rf.notifyCommRevdFromLeader(args.CandidateId)
		}
		rf.persist(nil)
		rf.mu.Unlock()
	}()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()

	if args.Term > rf.currentTerm {
		if lastLogTerm != args.LastLogTerm {
			if lastLogTerm > args.LastLogTerm {
				//rf.print("Rejecting request vote from server ", args.CandidateId, " due to incomplete logs")
				return
			}
		} else {
			if lastLogIndex > args.LastLogIndex {
				//rf.print("Rejecting request vote from server ", args.CandidateId, " due to incomplete logs")
				return
			}
		}
		//rf.print("Granting vote to server ", args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

func (rf *Raft) convertToFollower(serverEncountered int) {
	select {
	case rf.stepdownCh <- true:
		// The state change will happen in the main loop
		//rf.print("Sent it: Stepping down after encounter with ", serverEncountered)
	default:
		// The main loop is no longer listening for the stepdownCh channel
		// because it's time for a new heartbeat
		// Set state to follower here and when the heartbeat routine
		// acquires the lock, it will abort operation, seeing that it's
		// no longer the leader
		rf.state = Follower
		//rf.print("Changing the state directly to stepdown")
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

	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok {
		return
	}
	//rf.print("Successfully sent request vote to server ", server)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// The request votes are no longer applicable, they're behind the times
	if args.Term != rf.currentTerm || rf.state != Candidate {
		return
	}
	if reply.Term > rf.currentTerm {
		//rf.print(reply.Term, " > ", rf.currentTerm, " in sendRequestVote")
		//rf.print("Sending stepdown after sending request vote and receiving a higher term from server ", server)
		rf.currentTerm = reply.Term
		rf.convertToFollower(server)
		rf.persist(nil)
	} else if reply.VoteGranted {
		rf.noOfVotes += 1
		if rf.hasMajorityVotes() {
			rf.convertToLeader(server)
			rf.persist(nil)
		}
	}
}

func (rf *Raft) convertToLeader(tippingVoteFrom int) {
	//rf.print("Sending wonElection after recving vote from ", tippingVoteFrom)
	rf.state = Leader
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	for range rf.peers {
		rf.nextIndex = append(rf.nextIndex, rf.lastLogIndex() + 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	select {
	case rf.wonElectionCh <- true:
		//rf.print("Sent it: Sending wonElection after recving vote from ", tippingVoteFrom)
	default:
		//rf.print("Directly changing state to Leader")
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
	ShouldBeNext int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.persist(nil)
		rf.mu.Unlock()
	}()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ShouldBeNext = -1

	if args.Term < rf.currentTerm {
		return
	}

	if len(args.Entries) != 0 {
		//rf.print("Received append entries ", args.Entries, " from leader ", args.LeaderId)
	}
	if rf.state == Follower {
		rf.notifyCommRevdFromLeader(args.LeaderId)
	} else {
		//rf.print("Stepping down after receiving append entry from ", args.LeaderId)
		rf.currentTerm = args.Term
		rf.convertToFollower(args.LeaderId)
	}
	rf.currentTerm = args.Term

	if args.PrevLogIndex >= rf.lastLogIndex()+1 {
		if len(args.Entries) != 0 {
			//rf.print("Rejecting append entries ", args.Entries, " with args PrevLogIndex:", args.PrevLogIndex, " and PrevLogTerm: ", args.PrevLogTerm, " and saying shouldBeNext: ", len(rf.log))
		}
		reply.ShouldBeNext = rf.lastLogIndex()+1
		return
	}

	entryAtPrevIndex, sliceIndex := rf.entryWithIndex(args.PrevLogIndex)
	if entryAtPrevIndex != nil {
		if entryAtPrevIndex.Term != args.PrevLogTerm {
			for i := sliceIndex - 1; i >= 0; i-- {
				if rf.log[i].Term != entryAtPrevIndex.Term {
					reply.ShouldBeNext = rf.log[i].Index + 1
					break
				}
			}
			return
		}
	}

	reply.Success = true

	rf.log = rf.appendLogEntries(args.Entries, args.PrevLogIndex)

	if args.LeaderCommit > rf.commitIndex {
		min := min(args.LeaderCommit, rf.lastLogIndex())
		//rf.print("Updating rf.commitIndex from ", rf.commitIndex, " to ", min)
		rf.commitIndex = min
		rf.newCommitedEntriesCh <- true
	}

	//go rf.applyEntries()

}

func (rf *Raft) appendLogEntries(newEntries []LogEntry, prevLogIndex int) []LogEntry {
	//newLog := []LogEntry{}
	//newLog = append(newLog, rf.log[:prevLogIndex+1]...)
	//newLog = append(newLog, newEntries...)
	deleteFrom := len(rf.log)
	i := 0
	_, sliceIndexOfPrevEntry := rf.entryWithIndex(prevLogIndex)
	for range newEntries {
		sliceIndex := sliceIndexOfPrevEntry + i + 1
		if sliceIndex >= len(rf.log) {
			break
		}
		entry := rf.log[sliceIndex]
		if entry.Term != newEntries[i].Term {
			deleteFrom = sliceIndex
			break
		}
		i += 1
	}
	newLog := make([]LogEntry, 0, len(rf.log[:deleteFrom]) + len(newEntries[i:]))
	newLog = append(newLog, rf.log[:deleteFrom]...)
	newLog = append(newLog, newEntries[i:]...)

	if len(newEntries) > 0 {
		//rf.print("From ", rf.log, " to ", newLog)
	}
	return newLog
}

func (rf *Raft) notifyCommRevdFromLeader(serverEncountered int) {
	//rf.print("Recving comm after encounter with server ", serverEncountered)
	select {
	case rf.receivedCommCh <- true:
		//rf.print("Sent it: Recving comm after encounter with server ", serverEncountered)
	default:
		// Do nothing
		// It's too late
		// Another election is going to start
		//rf.print("Too late. Another election is about to start")
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	//rf.print("I the leader: my log: ", rf.log)
	//rf.print("About to send entries ", args.Entries, "with prevLogTerm", args.PrevLogTerm, "and prevLogIndex", args.PrevLogIndex, "to server", server)
	if len(args.Entries) != 0 {
		//rf.print("Sending append entries ", args.Entries, " to server ", server, " with args ", *args)
	}
	
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		return
	}

	if len(args.Entries) != 0 {
		//rf.print("Successfully sent append entries to server ", server)
	}
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm || rf.state != Leader || reply.Term < rf.currentTerm {
		//rf.print("NextIndex for server ", server, " from ", rf.nextIndex[server], " to ", rf.nextIndex[server] + len(args.Entries))
		return
	}
	if reply.Term > args.Term {
		//rf.print("Stepping down after sending append entry to ", server)
		rf.currentTerm = reply.Term
		rf.convertToFollower(server)
		rf.persist(nil)
		return
	}
	if reply.Success {
		//rf.print("After sending entries ", logToString(args.Entries), " matchIndex for server ", server, " moves from ", rf.matchIndex[server], " to ", max(rf.matchIndex[server], args.PrevLogIndex + len(args.Entries)))
		rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
		//rf.print("nextIndex for server ", server, " from ", rf.nextIndex[server], " to ", rf.matchIndex[server] + 1)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		//rf.print("Server ", server, " doenst have the required entry. Reducing nextIndex from ", rf.nextIndex[server], " to ", reply.ShouldBeNext, " and retrying")
		rf.nextIndex[server] = reply.ShouldBeNext
	}
	for i := len(rf.log) - 1; i >= 0 && rf.log[i].Index >= rf.commitIndex; i-- {
		count := 1
		for server := range rf.peers {
			if rf.matchIndex[server] >= i && rf.log[i].Term == rf.currentTerm && server != rf.me {
				count += 1
			}
		}
		//rf.print("Count for ", rf.log[i].Index, "!", count)
		if count >= rf.majority() {
			rf.commitIndex = rf.log[i].Index
			//go rf.applyEntries()
			rf.newCommitedEntriesCh <- true
			break
		}
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if rf.state == Follower {
		rf.notifyCommRevdFromLeader(args.LeaderId)
	} else {
		rf.convertToFollower(args.LeaderId)
	}

	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	if rf.snapshotIsOutdated(args.Data, args.LastIncludedIndex, args.LastIncludedTerm) {
		return
	}

	snapshot := make([]byte, len(args.Data))
	copy(snapshot, args.Data)

	_, sliceIndex := rf.entryWithIndex(args.LastIncludedIndex)
	snapshotEntry := []LogEntry{{ Term: args.LastIncludedTerm, 
								Index: args.LastIncludedIndex, Snapshot: snapshot }}
	rf.log = append(snapshotEntry, rf.log[sliceIndex+1:]...)
	rf.persist(snapshot)
	rf.applySnapshot(snapshot, args.LastIncludedIndex, args.LastIncludedTerm)
}

func (rf *Raft) sendInstallSnapshot(server int) {
	var reply InstallSnapshotReply
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.log[0].Snapshot,
	}

	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.convertToFollower(server)
		rf.persist(nil)
		return
	}

	rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex + 1)
}

func (rf *Raft) snapshotIsOutdated(snapshot []byte, liIndex int, liTerm int) bool {
	return rf.log[0].Index > liIndex || 
		(rf.log[0].Index == liIndex && rf.log[0].Term > liTerm)
}

func (rf *Raft) applySnapshot(snapshot []byte, liIndex int, liTerm int) {
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  liIndex,
		SnapshotIndex: liTerm,
	}
}

func (rf *Raft) applyEntries() {
	for !rf.killed() {
		_ = <-rf.newCommitedEntriesCh
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			rf.applyLogEntry(rf.lastApplied)
		}
	}
}

func (rf *Raft) majority() int {
	return len(rf.peers) / 2 + 1
}

func (rf *Raft) applyLogEntry(index int) {
	entry, _ := rf.entryWithIndex(index)
	toapply := ApplyMsg{
		CommandValid: true,
		Command:      entry.Command,
		CommandIndex: index,
	}
	//rf.print("Applying entry ", fmt.Sprintf("%d", entry))
	rf.applyCh <- toapply
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
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, isLeader
	}
	term, index, isLeader = rf.currentTerm, rf.lastLogIndex()+1, true

	//rf.print("Received command ", command, " from client")
	entry := LogEntry{Command: command, Term: rf.currentTerm, Index: index}
	rf.log = append(rf.log, entry)
	rf.persist(nil)

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
	for i := range rf.peers {
		if i != rf.me {
			//rf.print("Sending vote request to server ", i)
			go rf.sendRequestVote(i, args)
		}
	}
}

func (rf *Raft) initNewElection() *RequestVoteArgs {
	if rf.state != Candidate {
		//rf.print("State is no longer Candidate. Aborting election")
		return nil
	}
	//rf.print("Creating args for running election")
	rf.currentTerm += 1
	rf.noOfVotes = 1
	rf.votedFor = rf.me
	rf.persist(nil)
	return &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		//rf.print("State is no longer Leader. Aborting heartbeat")
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		prevEntry, sliceIndex := rf.entryWithIndex(rf.nextIndex[i] - 1)
		//fmt.Printf("Here %d %d %d\n", prevEntry, sliceIndex, rf.nextIndex[i])
		if prevEntry != nil {
			args := &AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm: prevEntry.Term,
				LeaderCommit: rf.commitIndex,
			}
			entries := rf.log[sliceIndex+1:]
			args.Entries = make([]LogEntry, len(entries))
			//rf.print("Ahhhh", logToString(rf.log))
			copy(args.Entries, entries)
			//rf.print(logToString(args.Entries))
			go rf.sendAppendEntries(i, args)
		} else {
			go rf.sendInstallSnapshot(i)
		}
	}
}

func (rf *Raft) main() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			//rf.print("Sending heartbeat")
			rf.heartbeat()
			select {
			//case <-rf.commWithFollowers:
			case <-time.After(heartbeatTime()):
			case <-rf.stepdownCh:
				//rf.print("Stepping down !")
				rf.leaderStepdown();
			}
		case Follower:
			select {
			case <-time.After(electionTimeout()):
				//rf.print("Starting election")
				rf.mu.Lock()
				//rf.print("Becoming a candidate")
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
				//rf.print("Starting election")
				rf.mu.Lock()
				args := rf.initNewElection()
				rf.mu.Unlock()
				if args != nil {
					rf.runElection(args)
				}
			case <-rf.stepdownCh:
				//rf.print("Stepping down !")
				rf.candidateStepdown()
			case <-rf.wonElectionCh:
				//rf.print("I Won Election and was informed in the channel!")
			}
		}
	}
}

func (rf *Raft) print(a ...interface{}) {
	if !Debug {
		return
	}
	stateStr := "Leader"
	if rf.state == Follower {
		stateStr = "Follower"
	}
	if rf.state == Candidate {
		stateStr = "Candidate"
	}
	print("Server ", rf.me, " in state ", stateStr, " ")//and term, ", rf.currentTerm, " ",
		//"with log ", rf.stringifyLog(), " and commit index ", rf.commitIndex, " ")//in test ", rf.testName,": ")
	//print("Server: ", rf.me, " state: ", stateStr)//, " log: ", rf.stringifyLog(), "cmtIndx: ", rf.commitIndex, "lastApplied: ", rf.lastApplied)
	//fmt.Printf(" at time: %s ", time.Now().Format(time.StampMilli))
	fmt.Print(a...)
	print("\n")
}

func (rf *Raft) stringifyLog() string {
	return logToString(rf.log)
}

func logToString(log []LogEntry) string {
	s := "[ "
	for i, val := range log {
		if i == 0 {
			s = fmt.Sprintf("%s { Snapshot, %v %v }", s, log[i].Term, log[i].Index)
		} else {
			s = fmt.Sprintf("%s %v", s, val)
		}
	}
	return s + " ]"
}

func intSliceToString(arr []int) string {
	s := "[ "
	for _, val := range arr {
		s = fmt.Sprintf("%s %v", s, val)
	}
	return s + " ]"
}

func heartbeatTime() time.Duration {
	return time.Millisecond * 110
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
	rf.commWithFollowers = make(chan bool)
	rf.newCommitedEntriesCh = make(chan bool, 100)

	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = []LogEntry{ { Command: 0, Term: -1, Index: 0, Snapshot: []byte{} } }
	rf.testName = testName


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.main()
	go rf.applyEntries()

	return rf
}
