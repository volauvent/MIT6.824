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
	"time"
	"labrpc"
	"sync"
	"bytes"
	"encoding/gob"
)

// import "bytes"
// import "encoding/gob"

const(
	FOLLOWER int = iota
	CANDIDATE
	LEADER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}


type LogEntry struct {
	LogIndex int
	LogTerm int
	LogCmd interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all servers
	currentTerm int
	votedFor int //init -1
	log []LogEntry

	//volatile state on all servers
	commitIndex int
	lastApplied int

	//volatile state on leaders
	nextIndex []int
	matchIndex []int

	//channel
	state int
	voteCount int
	heartBeatCh chan bool
	grantVoteCh chan bool
	leaderCh chan bool
	commitCh chan bool
	quit chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == LEADER
	rf.mu.Unlock()
	return term, isLeader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
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
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)


	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}


type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogTerm int
	PreLogIndex int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	PrevLogIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// get new heart beat frame
	rf.heartBeatCh <- true
	reply.Term = args.Term
	//rf.persist()
	// 1
	// if leader's current term is smaller than this node's term
	// 5.1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.PrevLogIndex = 1
		//rf.persist()
		reply.Success = false
		return
	}

	// update this node's state
	// if this leader's current term is bigger than this node's term
	// be careful
	// if it's equal, cannot update
	// or, leader(/this node) will turn to follower each time
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = args.Term

	//rf.persist()
	// 2
	// if leader's prevLogIndex is bigger than this node's last index
	// return false, roll back
	// zz... log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PreLogIndex > rf.getLastIndex() {
		reply.PrevLogIndex = rf.getLastIndex() + 1
		//fmt.Printf("prevLogIndex: %v\n", reply.PrevLogIndex)
		//rf.persist()
		reply.Success = false
		return
	}

	if args.PreLogIndex == 0 || rf.log[args.PreLogIndex].LogTerm == args.PrevLogTerm {
		// update log
		// from leader's prelogindex
		if len(args.Entries) > 0 {
			rf.log = append(rf.log[:args.PreLogIndex + 1], args.Entries...)
			//rf.persist()
		}
		// for commit
		// If leaderCommit > commitIndex, set commitIndex =
		// min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			last := rf.getLastIndex()
			if args.LeaderCommit > last {
				rf.commitIndex = last
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			//rf.persist()
			rf.commitCh <- true
		}
		reply.PrevLogIndex = rf.getLastIndex() + 1
		//rf.persist()
		reply.Success = true
	} else {
		// 2
		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm

		// speed up, find the prev term
		term := rf.log[args.PreLogIndex].LogTerm
		for i := args.PreLogIndex - 1; i >= 0; i-- {
			if rf.log[i].LogTerm != term {
				reply.PrevLogIndex = i + 1
				break
			}
		}
		//rf.persist()
		reply.Success = false
	}
	//rf.persist()
	return
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()

	if ok {
		//
		if rf.state != LEADER || args.Term != rf.currentTerm {
			return ok
		}
		// if leader's term is smaller
		// roll back to FOLLOWER
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
			return ok
		}
		//
		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries) - 1].LogIndex + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.persist()
			}
		} else { // fail
			rf.nextIndex[server] = reply.PrevLogIndex
			rf.persist()
		}
	}
	rf.persist()
	return ok

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool //vote for the applicant or not
}


func (rf *Raft) getLastIndex() int {
	//fmt.Printf("len: %v, rf: %v \n", len(rf.log) - 1, rf.log[len(rf.log) - 1].LogIndex)
	//return len(rf.log) - 1
	return rf.log[len(rf.log) - 1].LogIndex
}

func (rf *Raft) getLastTerm() int {
	return rf.log[len(rf.log) - 1].LogTerm
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()


	reply.VoteGranted = false

	//if the applicant's term is smaller than node's term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		//rf.persist()
		return
	}

	// if other nodes' term is bigger than this node's term
	// first turn to FOLLOWER
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		//rf.persist()
	}


	reply.Term = rf.currentTerm
	nLastLogIndex := rf.getLastIndex()
	nLastLogTerm := rf.getLastTerm()

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 && (args.LastLogTerm > nLastLogTerm ||
		(args.LastLogTerm == nLastLogTerm &&
			args.LastLogIndex >= nLastLogIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.grantVoteCh <- true
		rf.state = FOLLOWER
		//rf.persist()
	}
	//rf.persist()
	return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()

	if ok {
		if rf.state != CANDIDATE {
			return ok
		}
		term := rf.currentTerm
		if args.Term != term {
			return ok
		}
		if reply.Term > term {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.voteCount++
			if rf.state == CANDIDATE && rf.voteCount > len(rf.peers)/2 {
				rf.state = LEADER
				rf.leaderCh <- true
				//rf.persist()
			}
		}
	}
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	//index := -1
	//term := -1
	//isLeader := true

	// Your code here (2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		index = rf.getLastIndex() + 1
		rf.log = append(rf.log, LogEntry{LogIndex : index, LogTerm: term, LogCmd: command})
		//fmt.Printf(" -- get new index: %v, term:%v\n", index, term)
		rf.persist()
	}

	return index, term, isLeader
}

//func (rf *Raft) StartRead(command interface{}) (int, int, bool) {
//	//index := -1
//	//term := -1
//	//isLeader := true
//
//	// Your code here (2B).
//
//	rf.mu.Lock()
//	index := -1
//	term := rf.currentTerm
//	isLeader := rf.state == LEADER
//	rf.mu.Unlock()
//	return index, term, isLeader
//}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.quit)
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

	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.state = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.log = append(rf.log, LogEntry{LogTerm : 0})

	rf.heartBeatCh = make(chan bool, 100)
	rf.grantVoteCh = make(chan bool, 100)
	rf.leaderCh = make(chan bool, 100)
	rf.commitCh = make(chan bool, 100)
	rf.quit = make(chan bool)
	rf.mu.Unlock()

	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			rf.mu.Lock()
			currState := rf.state
			rf.mu.Unlock()
			switch currState {
			case FOLLOWER:
				select {
				case <- rf.heartBeatCh:
				case <- rf.grantVoteCh:
				case <-rf.quit:
					return
				case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
					rf.mu.Lock()
					rf.state = CANDIDATE
					//rf.persist()
					rf.mu.Unlock()
				}
			case CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.persist()
				//fmt.Printf("-- currentTerm: %v \n",rf.currentTerm)
				rf.mu.Unlock()
				go rf.broatcastRequestVote()
				select {
				case <- rf.heartBeatCh:
					rf.mu.Lock()
					rf.state = FOLLOWER
					//rf.persist()
					rf.mu.Unlock()
				case <- rf.leaderCh:
					rf.mu.Lock()
					//fmt.Printf("-----------------------------%v\n",rf.me)
					rf.state = LEADER
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.getLastIndex() + 1
						rf.matchIndex[i] = 0
					}
					rf.persist()
					rf.mu.Unlock()
				case <-rf.quit:
					return
				case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):

				//rf.currentTerm++
				//fmt.Printf("\n currentTerm: %v \n",rf.currentTerm)
				}
			case LEADER:
				select {
				case <-rf.quit:
					return
				case <-time.After(50 * time.Millisecond):
					go rf.broatcastAppendEntries()
				}


			}
		}
	}()

	//commit log
	go func() {
		for {
			select {
			case <- rf.commitCh:
				rf.mu.Lock()
				commitIndex := rf.commitIndex
				//send all the commit log to server
				baseIndex := rf.log[0].LogIndex
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{Index: i, Command: rf.log[i-baseIndex].LogCmd}
					applyCh <- msg
					rf.lastApplied = i
				}
				//rf.lastApplied = commitIndex
				rf.persist()
				rf.mu.Unlock()
			case <-rf.quit:
				return
			}
		}
	}()
	return rf
}

func (rf *Raft) broatcastRequestVote(){
	var args RequestVoteArgs
	rf.mu.Lock()
	//RequestVote RPC
	args.Term = rf.currentTerm
	args.CandidateID = rf.me
	args.LastLogIndex = rf.getLastIndex()
	args.LastLogTerm = rf.getLastTerm()
	numOfPeers := len(rf.peers)
	rf.mu.Unlock()

	for i := 0; i < numOfPeers; i++ {

		rf.mu.Lock()
		if rf.state != CANDIDATE {
			break
		}
		rf.mu.Unlock()

		if i == rf.me {
			continue
		}
		//rf.mu.Unlock()

		go func(server int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(server, &args, &reply)
		}(i)
	}
}

func (rf *Raft) broatcastAppendEntries () {
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// only leader can broadcast AppendEntries
		if rf.state == LEADER {

			N := rf.commitIndex
			last := rf.getLastIndex()
			for i := N + 1; i <= last; i++ {
				num := 1
				for j := range rf.peers {
					//fmt.Printf("matchIndex %v, j = %v\n", rf.matchIndex[j], j)
					//fmt.Printf("LogTem: %v, currTerm: %v \n", rf.log[i].LogTerm, rf.currentTerm)
					if j != rf.me && rf.matchIndex[j] >= i {
						num++
					}
				}
				//fmt.Printf("he: %v, %v, %v\n", rf.commitIndex +1 ,last, rf.me)
				if 2*num > len(rf.peers) && rf.log[i].LogTerm == rf.currentTerm {
					N = i
					//fmt.Printf("aa: %v, %v, %v, I, %v, %v , %v\n", rf.commitIndex +1 ,last, i, N, num, len(rf.peers))
				} //else{
				// fmt.Printf("bb: %v, %v, %v, I, %v, %v , %v\n",rf.commitIndex +1 ,last, i,  N, num, len(rf.peers))
				// cannot add break here
				// since a AppendEntry may lost
				// zz... there is no guarantee that this command will ever be committed
				// to the Raft log, since the leader may fail or lose an election.

				// break
				//}

			}

			if N != rf.commitIndex && rf.log[N].LogTerm == rf.currentTerm {
				rf.commitIndex = N
				rf.commitCh <- true
				//rf.persist()
			}
		}
	}()

	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		//if rf.state == LEADER {
		//broadcast commit log to every nodes
		for i := range rf.peers {
			if rf.state != LEADER {
				break
			}
			if i == rf.me {
				continue
			}

			if rf.nextIndex[i] > rf.log[0].LogIndex {
				//AppendEntries RPC
				var args AppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PreLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[args.PreLogIndex].LogTerm
				args.LeaderCommit = rf.commitIndex

				args.Entries = make([]LogEntry, len(rf.log[args.PreLogIndex+1: ])) //len(rf.log) - args.PreLogIndex - 1)
				copy(args.Entries, rf.log[args.PreLogIndex+1: ])

				go func(server int, args AppendEntriesArgs) {
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(server, args, &reply)
				}(i, args)
			}
		}
	}()
	//
}