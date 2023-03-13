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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

type Log struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	currentTerm int
	votedFOR    int
	status      string
	votedCount  int
	heartBeat   chan bool
	winElection chan bool
	entries     []Log
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	Apply       chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	if rf.status == "leader" {
		return rf.currentTerm, true
	}

	return rf.currentTerm, false
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) Compare(args *RequestVoteArgs) bool {
	if rf.entries[len(rf.entries)-1].Term == args.Term {
		return len(rf.entries)-1 < args.LastLogIndex
	}
	return rf.entries[len(rf.entries)-1].Term < args.Term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if (rf.votedFOR == args.CandidateId || rf.votedFOR == -1) && rf.Compare(args) {
		reply.VoteGranted = true
		rf.votedFOR = args.CandidateId
		return
	}
	reply.VoteGranted = false
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) bool {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != "candidate" {
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = "follower"
		rf.votedCount = 0
		rf.votedFOR = -1
		return ok
	}
	if reply.VoteGranted {
		rf.votedCount++
		if rf.votedCount > len(rf.peers)/2 {
			rf.winElection <- true
			rf.status = "leader"
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.entries)
				rf.matchIndex[i] = 0
			}
			go rf.HeartBeat()
		}
	}
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.status = "follower"
	rf.votedCount = 0
	rf.currentTerm = args.Term
	rf.votedFOR = -1
	rf.heartBeat <- true
	if args.PrevLogIndex > len(rf.entries)-1 {
		reply.Success = false
	}
	if args.PrevLogTerm != rf.entries[args.PrevLogIndex].Term {
		reply.Success = false
	}
	rf.entries = rf.entries[:args.PrevLogIndex+1]
	rf.entries = append(rf.entries, args.Entries...)
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= len(rf.entries)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.entries) - 1
		}
		go rf.ApplyMsg()
	}

	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) bool {
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if !ok {
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.status = "follower"
		rf.votedCount = 0
		rf.votedFOR = -1
		return ok
	}
	if !reply.Success {
		rf.nextIndex[server]--
	} else {
		rf.nextIndex[server] = len(rf.entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}
	for n := len(rf.entries) - 1; n > rf.commitIndex; n-- {
		cnt := 1
		if rf.entries[n].Term == rf.currentTerm {
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = n
				go rf.ApplyMsg()
				break
			}
		}
	}

	return ok
}

func (rf *Raft) HeartBeat() {
	if rf.status != "leader" {
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		prevlogindex := rf.nextIndex[i] - 1
		prevlogterm := rf.entries[prevlogindex].Term
		entries := make([]Log, len(rf.entries[prevlogindex+1:]))
		copy(entries, rf.entries[prevlogindex+1:])
		args := AppendEntriesArgs{rf.currentTerm, rf.me, prevlogindex, prevlogterm, entries, rf.commitIndex}
		go rf.sendAppendEntries(i, &args)
	}
	return
}

func (rf *Raft) Election() {
	//fmt.Println("election")
	time.Sleep(50 * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != "candidate" {
		return
	}
	rf.votedFOR = rf.me
	rf.votedCount++
	rf.currentTerm++
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := RequestVoteArgs{rf.currentTerm, rf.me, len(rf.entries) - 1, rf.entries[len(rf.entries)-1].Term}
		go rf.sendRequestVote(i, &args)
	}
	return
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
	if rf.status != "leader" {
		return -1, rf.currentTerm, false
	}
	rf.entries = append(rf.entries, Log{command, rf.currentTerm})
	return len(rf.entries) - 1, rf.currentTerm, true
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

func (rf *Raft) ticker() {
	for rf.killed() == false {
		//fmt.Println(rf.me)
		//fmt.Println(rf.status)
		switch rf.status {
		case "follower":
			select {
			case <-rf.heartBeat:
			case <-time.After(randTime()):
				rf.mu.Lock()
				rf.status = "candidate"
				go rf.Election()
				rf.mu.Unlock()
			}
		case "candidate":
			select {
			case <-rf.winElection:
			case <-rf.heartBeat:
			case <-time.After(randTime()):
				rf.mu.Lock()
				rf.status = "candidate"
				rf.votedFOR = -1
				rf.votedCount = 0
				go rf.Election()
				rf.mu.Unlock()
			}
		case "leader":
			//fmt.Println("leaderheart")
			select {
			case <-time.After(60 * time.Millisecond):
				go rf.HeartBeat()
			}
		}
		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) ApplyMsg() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.Apply <- ApplyMsg{CommandValid: true, Command: rf.entries[i].Command, CommandIndex: i}
		rf.lastApplied = i
	}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFOR = -1
	rf.votedCount = 0
	rf.currentTerm = 0
	rf.status = "follower"
	rf.heartBeat = make(chan bool)
	rf.winElection = make(chan bool)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.entries = make([]Log, 1)
	rf.entries = append(rf.entries, Log{0, 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.Apply = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func randTime() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return time.Millisecond * time.Duration((r.Intn(300) + 200))
}
