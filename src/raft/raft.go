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
	"labrpc"
	"time"
	"sync"
	"math/rand"
	"fmt"
	"bytes"
	"labgob"
)

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

const leader = 0
const candidate = 1
const follower = 2
const heartbeatDuration = 100
const baseElectionDuration = 500

type ApplyMsg struct {
	CommandValid 	bool
	Command      	interface{}
	CommandIndex 	int
}

//For part B
type LogEntry struct {
	Index			int //start from 1
	Command			interface{}	//same as ApplyMsg
	Term			int
}

type AppendEntriesInput struct {
	Term     		int
	LeaderID 		int

	//For part B, From Paper P4
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term    		int
	Success 		bool

	// can not handle race
	NextIndex		int
}

type RequestVoteArgs struct {
	Term   		    int
	CandidateId 	int

	//For part B, From Paper P4
	LastLogIndex 	int
	LastLogTerm		int
}

type RequestVoteReply struct {
	Term        	int
	VoteGranted 	bool
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        	sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]


	// Our Code Start Here
	state       int
	//ended 		bool
	currentTerm int

	//Which candidate did u vote for?
	votedFor    int
	leaderID    int

	//Leader heartbeat
	lastHeartbeat time.Time
	lastSendTime time.Time

	//Part B, From Paper P4, table state
	log			[]LogEntry
	commitIndex	int		// highest log committed
	lastApplied int		// highest log applied to state machine
	nextIndex	[]int		// next log send to server
	matchIndex	[]int		// highest log replicated on server

	chApplyMsg	chan ApplyMsg
	chTimer		chan struct{}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cT int
	var vF int
	var lG []LogEntry
	err1 := d.Decode(&cT)
	err2 := d.Decode(&vF)
	err3 := d.Decode(&lG)
	if err1 != nil {
		panic(err1)
	}else{
		rf.currentTerm = cT
	}

	if err2 != nil{
		panic(err2)
	}else{
		rf.votedFor = vF
	}

	if err3 != nil{
		panic(err3)
	}else{
		rf.log = lG
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	//toPrint := func() {
	//	fmt.Printf("Vote Return")
	//}

	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//defer toPrint()

	fmt.Printf("vote: %v received vote request from %v at %v", rf.me, args.CandidateId, time.Now())


	currentTerm := rf.getCurrentTerm()

	if args.Term < currentTerm {
		reply.VoteGranted = false
		reply.Term = currentTerm
		go rf.persist()
		return
	}

	if args.Term > currentTerm{
		rf.setToFollower(args.Term)
		//rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else if args.Term == currentTerm {
		if rf.votedFor == -1 {

			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()

			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
			reply.Term = currentTerm
		}
	}

	go rf.persist()

	//Part B, For lastLogTerm

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (rf.log[len(rf.log) -1].Term == args.LastLogTerm) && (len(rf.log) -1 > args.LastLogIndex) {
		reply.VoteGranted = false
	} else if rf.log[len(rf.log) -1].Term > args.LastLogTerm {
		reply.VoteGranted = false
	}

	if reply.VoteGranted {
		fmt.Printf("vote: %v vote to %v at time %v\n", rf.me, args.CandidateId, time.Now())

		rf.lastHeartbeat = time.Now()
		go func() {
			rf.chTimer <- struct{}{}
			fmt.Printf("vote: %v reset Timer at %v\n", rf.me, time.Now())
		}()
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
func (rf *Raft) sendRequestVote(server int, chVote chan int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		chVote <- server
	}
	return ok
}


func (rf *Raft) AppendEntries(args *AppendEntriesInput, reply *AppendEntriesReply) {
	//toPrint := func() {
	//	fmt.Printf("%d End AppendEntries\n", rf.me)
	//
	//}

	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//defer toPrint()

	//fmt.Printf("%d append entries to %d\n", args.LeaderID, rf.me)

	currentTerm := rf.getCurrentTerm()
	reply.Term = currentTerm

	if args.Term < currentTerm {
		reply.Success = false
		go rf.persist()
		return
	}

	go func() {rf.chTimer <- struct{}{}}()
	rf.setToFollower(args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.leaderID = args.LeaderID
	rf.lastHeartbeat = time.Now()

	if args.PrevLogIndex > rf.log[len(rf.log) -1].Index {
		reply.Success = false
		return
	}

	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Success = false
		return
	}

	reply.Success = true
	reply.NextIndex = args.PrevLogIndex + 1 + len(args.Entries)
	ansIndex := -1

	if args.PrevLogIndex + len(args.Entries) <= rf.log[len(rf.log) - 1].Index {
		for i := 0; i < len(args.Entries); i++ {
			if args.Entries[i].Term != rf.log[args.PrevLogIndex + 1 + i].Term { // First Term which not equal
				ansIndex = args.PrevLogIndex + 1 + i
				break
			}
		}
	} else {
		ansIndex = args.PrevLogIndex + 1
	}

	if ansIndex != -1 {
		rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)
	}

	go rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.log[len(rf.log) - 1].Index {
			rf.commitIndex = rf.log[len(rf.log) - 1].Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
}



func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesInput, reply *AppendEntriesReply) bool {
	if rf.me != server{
		return rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	return true
}

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


	term, isLeader := rf.GetState()
	index := -1

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if isLeader {
		index = len(rf.log)

		rf.log = append(rf.log, LogEntry{
			Term:		term,
			Command:	command,
			Index: 		index,
		})
	}

	go rf.persist()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here (3A, 3B, 3C).
	rf := &Raft{
		peers:		peers,
		persister:	persister,
		me:			me,
		state:		follower,
		votedFor:	-1,

		// Part B
		log: 		make([]LogEntry, 1),
		nextIndex: 	make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		chApplyMsg: applyCh,
		chTimer: 	make(chan struct{}),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionTimer()
	go rf.startApplySelf()

	return rf
}

func (rf *Raft) startApplySelf() {
	for {

		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			toApply := make([]LogEntry, rf.commitIndex - rf.lastApplied)
			copy(toApply, rf.log[rf.lastApplied + 1: rf.commitIndex + 1])
			rf.mu.Unlock()

			//for i := rf.lastApplied + 1; i<= rf.commitIndex; i++ {
			//	msg := ApplyMsg{
			//		CommandIndex: 	i,
			//		Command: 		rf.log[i].Command,
			//		CommandValid: 	true,
			//	}
			//	rf.chApplyMsg <- msg
			//}
			//rf.mu.Unlock()

			for _, entry := range toApply {
				rf.chApplyMsg <- ApplyMsg{
					CommandIndex:		entry.Index,
					Command: 			entry.Command,
					CommandValid: 		true,
				}
			}


		} else {
			rf.mu.Unlock()
			<- time.After(heartbeatDuration * time.Millisecond)
		}
	}
}

func getElectionTimeout() time.Duration{
	return (baseElectionDuration + time.Duration(rand.Intn(100))) * time.Millisecond
}

func (rf *Raft) startElectionTimer() {

	electionTimer := time.NewTimer(getElectionTimeout())

	for {
		select {
		case <- rf.chTimer:
			electionTimer.Reset(getElectionTimeout())
			//fmt.Printf("Reset timer %v\n", rf.me)

		case <- electionTimer.C:
			_, isLeader := rf.GetState()
			if isLeader {
				electionTimer.Reset(getElectionTimeout())
			} else {
				fmt.Printf("People %v timeout, start Election, time %v\n", rf.me, time.Now())

				electionTimer.Reset(getElectionTimeout())
				go rf.startElection()
			}
		}
	}

	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	//interval := currentTime.Sub(rf.lastHeartbeat)
	//
	//if  interval>= timeout && rf.state != leader {
	//	go rf.startElection()
	//} else {
	//	//if !rf.ended {
	//	go rf.startElectionTimer()
	//	//}
	//}
}

func (rf *Raft) startElection() {


	rf.setToCandidate()

	rf.mu.Lock()

	replies := make([]RequestVoteReply, len(rf.peers))
	chVote := make(chan int, len(replies))

	args := RequestVoteArgs{
		Term:			rf.currentTerm,
		CandidateId:	rf.me,
		LastLogIndex:	rf.log[len(rf.log) -1].Index,
		LastLogTerm: 	rf.log[len(rf.log) -1].Term,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, chVote, &args, &replies[i])
	}

	rf.mu.Unlock()


	// Calculate Vote
	votes := 1 //One for itself
	gotMajority := false




	for range rf.peers {
		reply := replies[<- chVote]
		//fmt.Printf("I am %v, I got a vote %v\n", rf.me, reply)
		if reply.VoteGranted {
			votes++
		} else if reply.Term > rf.getCurrentTerm() {
			//rf.mu.Lock()
			rf.setToFollower(reply.Term)
			//rf.mu.Unlock()
			break
		}

		if votes > len(replies)/2 {
			gotMajority = true
			break
		}
	}

	if gotMajority {

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.state == candidate && args.Term == rf.currentTerm {

			fmt.Printf("I am the leader: %v\n", rf.me)
			go rf.setToLeader()
		}
	}
}

func (rf *Raft) setLastSentTime(t time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastSendTime = t
}

func (rf *Raft) getLastSentTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.lastSendTime
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm
}

func (rf *Raft) setToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me //itself
	go rf.persist()
}

func (rf *Raft) setToFollower(newTerm int){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	go rf.persist()
}

func (rf *Raft) setToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == leader {
		return
	}

	rf.state = leader
	rf.leaderID = rf.me
	rf.votedFor = -1
	//rf.sendHeartbeats()

	for i := range rf.peers {
		rf.nextIndex[i] = rf.log[len(rf.log) - 1].Index + 1
		rf.matchIndex[i] = 0
	}

	go rf.persist()
	go rf.lead()
}

func (rf *Raft) lead() {
	rf.sendHeartbeats()

	for {
		currentTime := <-time.After(heartbeatDuration * time.Millisecond) //100 ms

		_, isLeader := rf.GetState()

		if !isLeader { //|| rf.ended
			break
		} else if currentTime.Sub(rf.getLastSentTime()) >= heartbeatDuration * time.Millisecond { //Send heartbeats
			//rf.mu.Lock()
			rf.sendHeartbeats()
			//rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendHeartbeat(i int, reply *AppendEntriesReply) bool {


	rf.mu.Lock()
	//defer rf.mu.Unlock()
	//fmt.Printf("i %d, rf.nextIndex[i] %v, len(rf.log) %v, %v\n", i, (rf.nextIndex[i]), len(rf.log), rf.log)
	//fmt.Println(rf.log)
	args := AppendEntriesInput{
		Term:			rf.currentTerm,
		LeaderID:		rf.me,
		// Part B
		PrevLogIndex:	rf.nextIndex[i] - 1,
		PrevLogTerm:	rf.log[rf.nextIndex[i] - 1].Term,
		LeaderCommit:	rf.commitIndex,
	}

	if rf.log[len(rf.log) -1].Index >= rf.nextIndex[i] {
		args.Entries = rf.log[rf.nextIndex[i]:]
		fmt.Printf("Sent to: %v, args.entries size: %v, %v\n", i, len(args.Entries), args.Entries)
	}
	rf.mu.Unlock()

	//fmt.Printf("I am %v, send heartbeat to %v\n", rf.me, i)

	_, isLeader := rf.GetState()

	if isLeader && rf.sendAppendEntries(i, &args, reply) {

		if reply.Success {
			rf.mu.Lock()
			rf.nextIndex[i] = reply.NextIndex
			rf.matchIndex[i] = rf.nextIndex[i] - 1
			rf.mu.Unlock()
		} else {
			_, isLeader := rf.GetState()

			if !isLeader {
				return true
			} else if reply.Term > rf.getCurrentTerm() {
				rf.setToFollower(reply.Term)
				return true
			} else {
				rf.mu.Lock()
				if rf.nextIndex[i] >= 0 {
					rf.nextIndex[i] --
				}
				rf.mu.Unlock()
				fmt.Println("Retry")
				return false
			}
		}
	}

	return true
}

func (rf *Raft) sendHeartbeats() {
	//fmt.Printf("I am leader %v, send heartbeats at %v\n", rf.me, time.Now())

	replies := make([]AppendEntriesReply, len(rf.peers))

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(sendTo int) {
			for {
				if rf.sendHeartbeat(sendTo, &replies[sendTo]) {
					//time.After(heartbeatDuration)
					break
				}
			}
		}(i)

	}

	rf.setLastSentTime(time.Now())

	// Commit
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for logIndex := rf.log[len(rf.log) -1].Index; logIndex > rf.commitIndex; logIndex-- {
		n := 1
		for i, index := range rf.matchIndex {
			if rf.me == i {
				continue
			} else if rf.commitIndex < index {
				n += 1
			}
		}
		if n > len(rf.peers)/2 {
			rf.commitIndex = logIndex
			break;
		}
	}
}
