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
)

// import "bytes"
// import "labgob"

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

const leader = 0
const candidate = 1
const follower = 2
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	id               int

	state            int
	ended bool

	currentTerm int
	//Which candidate did u vote for?
	votedFor    int
	leaderID    int
	//Leader heartbeat
	lastHeartbeat time.Time
	lastSendTime time.Time

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
	// Your code here (3C).
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
	// Your code here (3C).
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else{
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true

	}
	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
	}
	/*else if rf.votedFor == 0 || args.Candidate == rf.votedFor {
	}*/
	reply.Term = rf.currentTerm

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
func (rf *Raft) sendRequestVote(server int, voteChannel chan int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.me!=server {
		rf.peers[server].Call("Raft.RequestVote", args, reply)
		voteChannel <- server
	}
}

type AppendEntriesInput struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesInput, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term >= rf.currentTerm {
		rf.state = follower
		rf.leaderID = args.LeaderID
		rf.currentTerm = args.Term
		rf.lastHeartbeat = time.Now()
		rf.votedFor = 0
		reply.Success = true
	}else{
		reply.Success=false
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesInput, reply *AppendEntriesReply) {
	if rf.me!=server{
		rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = follower

	// Your initialization code here (3A, 3B, 3C).
	rf.id = int(me + 50) //Should change

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startElectionTimer()

	return rf
}

func getElectionTimeout() time.Duration{
	return (500+ time.Duration(rand.Intn(100))) * time.Millisecond
}

func (rf *Raft) startElectionTimer() {
	timeout := getElectionTimeout()
	currentTime := <-time.After(timeout)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	interval  := currentTime.Sub(rf.lastHeartbeat)
	if  interval>= timeout && rf.state != leader {
		go rf.startElection()
	} else if !rf.ended {
		go rf.startElectionTimer()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.id //itself

	go rf.startElectionTimer()

	replies := make([]RequestVoteReply, len(rf.peers))
	voteChannel := make(chan int, len(replies))
	args := RequestVoteArgs{}
	args.Term=rf.currentTerm
	args.CandidateId=rf.id
	for i := range rf.peers {
		go rf.sendRequestVote(i,voteChannel, &args, &replies[i])
	}
	rf.mu.Unlock()

	votes := 1 //One for itself
	gotMajority := false
	for i := 0; i < len(replies); i++ {
		if replies[<- voteChannel].VoteGranted {
			votes++
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
			go rf.setLeader()
		}
	}
}

func (rf *Raft) setLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = leader
	rf.leaderID = rf.id
	rf.sendHeartbeats()
	go rf.lead()
}

func (rf *Raft) lead() {
	for {
		currentTime := <-time.After(100000) //100 ms

		rf.mu.Lock()
		rf.mu.Unlock()
		_,isLeader := rf.GetState()
		if !isLeader || rf.ended {
			break
		} else if rf.state == leader && currentTime.Sub(rf.lastSendTime) >= 100000 { //Send heartbeats
			rf.mu.Lock()
			rf.sendHeartbeats()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) sendHeartbeats() {
	args := AppendEntriesInput{}
	args.Term=rf.currentTerm
	args.LeaderID=rf.id
	replies := make([]AppendEntriesReply, len(rf.peers))
	for i := range rf.peers {
		go rf.sendAppendEntries(i, &args, &replies[i])
	}
	rf.lastSendTime = time.Now()
}

