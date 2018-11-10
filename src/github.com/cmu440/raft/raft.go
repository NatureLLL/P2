//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

import (
	"fmt"
	"github.com/cmu440/rpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = Make(...)
//   Create a new Raft peer.
//
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me" (see line 58), its current term, and whether it thinks it
//   is a leader
//
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (e.g. tester) on the
//   same peer, via the applyCh channel passed to Make()
//

//TODO: when state trnsition, check carefully what args need to change
//
// ApplyMsg
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same peer, via the applyCh passed to Make()
//
type ApplyMsg struct {
	Index   int // todo: check whether it's log index
	Command interface{}
}

//
// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
//
type Raft struct {
	mux         sync.Mutex       // Lock to protect shared access to this peer's state
	peers       []*rpc.ClientEnd // RPC end points of all peers
	me          int              // this peer's index into peers[]
	applied     chan ApplyMsg    // channel for passing applyMsg
	currentTerm int              // latest term server has seen
	logEntries  []LogEntry       // todo: cautious when using slice, index starts from 1
	votedFor    int              // candidateId that received vote in current term, set to -1 when update currentTerm
	isLeader    bool             // whether this peer believes it is the leader
	isFollower  bool             // whether it's follower
	leaderId    int              // id of leader

	commitIndex int //index of highest log entry known to be committed (initialized to 0, update when receive appendEntries RPC
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, update when send to ApplyCh

	numVotes int // votes a candidate get, initialize to 1 when transit to candidate

	// leader owns, when transit to leader, re-initialize this
	nextIndex []int // when issue appendRpc[not heartbeat],
	// leader relies on it to know which log to send to peer[i] when send append
	// when success, update nextIndex[i]=min(nextIndex[i]++, len(logEntries))
	// when unsuccess, decrement it accordingly
	matchIndex []int       // rely on reply to update itself. todo: how to?
	numReplica map[int]int // key: log index; value: number of successful replication

	//resetElection chan bool // reset election timer
	stopElectionTimer  chan bool // when transit to leader, use it
	stopHeartbeatTimer chan bool // when transit from leader to follower, use it

	debug bool // fmt print
}

// custom struct
type LogEntry struct {
	Term    int         // term of the log
	Command interface{} //
}

//
// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
//
func (rf *Raft) GetState() (int, int, bool) {

	var me int
	var term int
	var isleader bool

	rf.mux.Lock()
	me = rf.me
	term = rf.currentTerm
	isleader = rf.isLeader
	rf.mux.Unlock()

	return me, term, isleader
}

//
// RequestVoteArgs
// ===============
// Field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // id of candidate
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply
// ================
type RequestVoteReply struct {
	Term        int  // currentTerm of receiver
	VoteGranted bool // true means candidate get this vote
}

// RPC struct, capital
type AppendEntriesArgs struct {
	Term         int        // leader's currentTerm
	LeaderId     int        // follower use this to redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones, aka, last log index
	PrevLogTerm  int        // term of prevLogIndex entry
	LeaderCommit int        // leader’s commitIndex
	Entries      []LogEntry // todo: log entries to store (empty for heartbeat; may send more than one for efficiency)
	// send to peer i, all the log entries start from nextIndex[i] to lastIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm of receiver
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
// for checkpoint, just heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	success := false
	rf.mux.Lock()
	// term check
	if rf.currentTerm <= args.Term {
		if rf.currentTerm < args.Term {
			// update term
			rf.currentTerm = args.Term
			// update votedFor, in this new term, doesn't vote yet
			rf.votedFor = -1
		}
		rf.leaderId = args.LeaderId
		rf.transitToFollower()

		if len(rf.logEntries)-1 >= args.PrevLogIndex {
			// first consider whether prevLogIndex match
			if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
				rf.logEntries = rf.logEntries[:args.PrevLogIndex]
			} else {
				// todo: need to consider if receive a heartbeat, how to update commitIndex
				// only delete entries that conflict!
				// process RPC, check consistency
				//1. length >= prev, can proceed
				index := args.PrevLogIndex + 1
				for i, v := range args.Entries {
					if (index+i >= len(rf.logEntries)) || rf.logEntries[index+i].Term != v.Term {
						rf.logEntries = append(rf.logEntries[:index+i], args.Entries[i:]...)
						break
					}
				}
				// consistent
				success = true
				//todo: update commitIndex = min(leaderCommit, index of last new entry)
				if args.LeaderCommit > rf.commitIndex {
					lastNewLogIndex := max(args.PrevLogIndex+len(args.Entries), rf.commitIndex)
					rf.commitIndex = min(args.LeaderCommit, lastNewLogIndex)
					fmt.Printf("server %d update commitIndex to %d\n", rf.me, rf.commitIndex)
				}

			}
		}
	}
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applied <- ApplyMsg{rf.lastApplied, rf.logEntries[rf.lastApplied].Command}
		fmt.Printf("server %d applied log %d, command %d\n", rf.me, rf.lastApplied, rf.logEntries[rf.lastApplied].Command)
	}
	//fmt.Printf("server %d, last applied: %d, commitIndex: %d \n",rf.me,rf.lastApplied,rf.commitIndex)
	reply.Term = rf.currentTerm
	rf.mux.Unlock()
	reply.Success = success

}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	rf.mux.Lock()
	if rf.isLeader {
		if ok {
			if !reply.Success {
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.votedFor = -1

					rf.transitToFollower()

				} else if rf.matchIndex[peer] < args.PrevLogIndex {
					// log doesn't match
					// todo: need to consider matchIndex first, to see whether we need to consider this failure
					// todo: for all --, consider whether it's > 0
					rf.nextIndex[peer] = args.PrevLogIndex
					newArgs := &AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: args.PrevLogIndex - 1,
						PrevLogTerm:  rf.logEntries[args.PrevLogIndex-1].Term,
						LeaderCommit: rf.commitIndex,
						Entries:      rf.logEntries[rf.nextIndex[peer]:],
					}
					fmt.Printf("leader %d resend to server %d, nextIndex %d, newArgs.prevlog %d\n", rf.me, peer, rf.nextIndex[peer], newArgs.PrevLogIndex)
					go rf.sendAppendEntries(peer, newArgs, &AppendEntriesReply{})
				}

			} else if len(args.Entries) > 0 {
				// all logs in args.Entries are appended successfully

				// update matchIndex
				rf.matchIndex[peer] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[peer])
				fmt.Printf("matchIndex[%d] = %d \n", peer, rf.matchIndex[peer])
				// update nextIndex
				rf.nextIndex[peer] = max(rf.matchIndex[peer]+1, rf.nextIndex[peer])

				// find N
				indexs := make([]int, len(rf.matchIndex))
				copy(indexs, rf.matchIndex)
				sort.Ints(indexs)
				// start looking from idx = indexs[len(peers)]/2
				for i := len(rf.peers) / 2; (i >= 0) && (indexs[i] > rf.commitIndex); i-- {
					if rf.logEntries[indexs[i]].Term == rf.currentTerm {
						rf.commitIndex = indexs[i]
						break
					}
				}

				// apply all un applied messages
				for rf.lastApplied < rf.commitIndex {
					rf.lastApplied++
					rf.applied <- ApplyMsg{rf.lastApplied, rf.logEntries[rf.lastApplied].Command}
					fmt.Printf("Leader %d applied log %d, command %d\n", rf.me, rf.lastApplied, rf.logEntries[rf.lastApplied].Command)
				}
			}
		} else if (len(args.Entries) > 0) && (rf.matchIndex[peer] < args.PrevLogIndex) {
			// retry, dont' resend heartbeat!!!
			go rf.sendAppendEntries(peer, args, reply)
		}
	}
	rf.mux.Unlock()

	return ok
}

//
// RequestVote
// ===========
//
// Example RequestVote RPC handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	voteGranted := false

	rf.mux.Lock()
	// debug
	//fmt.Printf("RequestVote %d to %d: args term: %d, current term: %d \n", args.CandidateId, rf.me, args.Term, rf.currentTerm)
	if (rf.currentTerm < args.Term) ||
		((rf.currentTerm == args.Term) && ((rf.votedFor == -1) || (rf.votedFor == args.CandidateId))) {
		// check log freshness
		lastLogIndex := len(rf.logEntries) - 1
		lastLogTerm := rf.logEntries[lastLogIndex].Term
		if (lastLogTerm < args.LastLogTerm) ||
			((lastLogTerm == args.LastLogTerm) && (lastLogIndex <= args.LastLogIndex)) {
			voteGranted = true
			rf.votedFor = args.CandidateId
			if rf.debug {
				//fmt.Printf("%d grant vote to %d, with term %d\n", rf.me, rf.votedFor, rf.currentTerm)
			}
		}

		// update current term
		rf.currentTerm = args.Term
		rf.transitToFollower()
	}

	reply.Term = rf.currentTerm
	rf.mux.Unlock()

	reply.VoteGranted = voteGranted

	/*
		if rf.currentTerm < args.Term {
		} else if (rf.currentTerm == RequestVoteArgs.Term) &&
				(rf.votedFor==0 || rf.votedFor==RequestVoteArgs.candidateId) {
		// whether it's up-to-date log
		rf.lastLogTerm < RequestVoteArgs.LastLogTerm ||
		rf.lastLogTerm == RequestVoteArgs.LastLogTerm && rf.lastLogIndex <= RequestVoteArgs.LastLogIndex {
		// this holds for every state; 1. for peers, it hold. 2. for leader, a leader will typically have the most
		// up-to-date logs, so this case will rarely happen
		}
	*/
}

//
// sendRequestVote
// ===============
//
// Example code to send a RequestVote RPC to a peer
//
// peer int -- index of the target peer in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which peers
// may be unreachable, and in which requests and replies may be lost
//
// Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// Thus Call() may not return for a while
//
// A false return can be caused by a dead peer, a live peer that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the peer side does not return
//
// Thus there is no need to implement your own timeouts around Call()
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
//
func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
	rf.mux.Lock()
	// if it's still candidate
	if (!rf.isLeader) && (!rf.isFollower) {
		if ok {
			// decode reply message, check for valid votes for this term
			if reply.VoteGranted && (rf.currentTerm == reply.Term) {
				rf.numVotes++
				if rf.debug {
					fmt.Printf("candidate %d have %d votes\n", rf.me, rf.numVotes)
				}
				if 2*rf.numVotes > len(rf.peers) {
					rf.transitToLeader()
				}

			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.transitToFollower()
				if rf.debug {
					fmt.Printf("candidate %d transits to follower\n", rf.me)
				}
			}
		}
		// todo: question? do we need to retry requestvoterpc?

	}
	rf.mux.Unlock()
	return ok
}

//
// Start
// =====
//
// The service using Raft (e.g. a k/v peer) wants to start
// agreement on the next command to be appended to Raft's log
//
// If this peer is not the leader, return false
//
// Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// The second return value is the current term
//
// The third return value is true if this peer believes it is
// the leader
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	rf.mux.Lock()
	isLeader := rf.isLeader
	if isLeader {
		//start agreement
		rf.logEntries = append(rf.logEntries, LogEntry{rf.currentTerm, command})
		index = len(rf.logEntries) - 1
		term = rf.currentTerm
		rf.numReplica[index] = 1
		rf.matchIndex[rf.me] = index

		fmt.Printf("Leader %d append log %d , current term: %d\n", rf.me, index, rf.currentTerm)
		// todo: issue appendEntries to other servers in parallel, according to their nextIndex
		rf.issueAppend()
	}
	rf.mux.Unlock()

	return index, term, isLeader
}

// require lock before calling
func (rf *Raft) issueAppend() {
	// According to nextIndex[i], send logentries
	for i, v := range rf.nextIndex {
		if i != rf.me {
			Entries := rf.logEntries[v:]
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				PrevLogIndex: v - 1,
				PrevLogTerm:  rf.logEntries[v-1].Term,
				LeaderCommit: rf.commitIndex,
				Entries:      Entries,
			}
			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}
}

//
// Kill
// ====
//
// The tester calls Kill() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance
//
func (rf *Raft) Kill() {
	// Your code here, if desired
	rf.mux.Lock()
	rf.debug = false
	rf.stopElectionTimer <- true
	rf.stopHeartbeatTimer <- true
	rf.mux.Unlock()

}

//
// Make
// ====
//
// The service or tester wants to create a Raft peer
//
// The port numbers of all the Raft peers (including this one)
// are in peers[]
//
// This peer's port is peers[me]
//
// All the peers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyMsg messages
//
// Make() must return quickly, so it should start Goroutines
// for any long-running work
//
func Make(peers []*rpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	// initialize arguments in rf struct
	rf := &Raft{
		peers:              peers,
		me:                 me,
		applied:            make(chan ApplyMsg, 500), // big room in case of deadlock
		currentTerm:        0,
		isLeader:           false,
		isFollower:         true,
		leaderId:           -1,
		votedFor:           -1,
		logEntries:         make([]LogEntry, 1), // access log entry by logEntries[logIndex]
		commitIndex:        0,
		lastApplied:        0,
		stopElectionTimer:  make(chan bool, 1),
		stopHeartbeatTimer: make(chan bool, 1),
		debug:              true,
		nextIndex:          make([]int, len(peers)),
		matchIndex:         make([]int, len(peers)),
		numReplica:         make(map[int]int),
	}
	rf.logEntries[0] = LogEntry{Term: 0}

	// create background goroutine pass applyCh as argument
	go rf.sendApply(applyCh)
	// starts as follower
	go rf.electionTimer()

	return rf
}

func (rf *Raft) sendApply(applyCh chan ApplyMsg) {
	for {
		select {
		case msg := <-rf.applied:
			applyCh <- msg
		}
	}
}

// only runs for follower and candidate
// randomize election time between 200-400ms
func (rf *Raft) electionTimer() {
	t := time.NewTimer(time.Duration(rand.Intn(250)+250) * time.Millisecond)
	for {
		select {
		case <-t.C:
			// handle election
			rf.mux.Lock()
			rf.startElection()
			rf.mux.Unlock()

		case <-rf.stopElectionTimer:
			return

		}
		t.Reset(time.Duration(rand.Intn(250)+250) * time.Millisecond)
	}
}

func (rf *Raft) heartbeatTimer() {
	t := time.NewTimer(time.Duration(120) * time.Millisecond)
	for {
		select {
		case <-t.C:
			// spread heart beat
			rf.mux.Lock()
			rf.spreadHeartbeat()
			rf.mux.Unlock()

		case <-rf.stopHeartbeatTimer:
			return

		}
		t.Reset(time.Duration(120) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	//rf.mux.Lock()
	rf.isFollower = false
	rf.isLeader = false
	rf.currentTerm++
	if rf.debug {
		//fmt.Println("candidate ", rf.me, "start election with term", rf.currentTerm)
	}
	// vote for itself
	rf.votedFor = rf.me
	rf.numVotes = 1

	// issue voteRequests to other peers in parallel
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logEntries) - 1,
		LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
	//rf.mux.Unlock()
}

// require lock beforehead
func (rf *Raft) spreadHeartbeat() {
	// issue heartbeat to other peers in parallel
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// todo: heartbeat should contain useful information!
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.logEntries[rf.nextIndex[i]-1].Term,
				LeaderCommit: rf.commitIndex,
				Entries:      make([]LogEntry, 0),
			}
			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}
	//rf.mux.Unlock()
}

// require lock before calling it
// stop election timer, start heartbeat timer, isLeader=true
func (rf *Raft) transitToLeader() {
	if rf.debug {
		fmt.Println("server ", rf.me, "becomes leader with term", rf.currentTerm)
	}

	// todo: reinitialze nextIndex, matchIndex
	nextIndex := len(rf.logEntries)
	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = nextIndex
			rf.matchIndex[i] = 0
		} else {
			rf.matchIndex[i] = nextIndex - 1
		}
	}

	rf.stopElectionTimer <- true
	rf.isLeader = true
	rf.isFollower = false
	rf.spreadHeartbeat()
	go rf.heartbeatTimer()
}

// require lock before calling it
// think about whether need to pass votedFor as argument, or initialize outside the function
//transit to follower: reset election timer, isLeader=false
func (rf *Raft) transitToFollower() {
	// if leader -> follower
	if rf.isLeader {
		if rf.debug {
			fmt.Println("leader", rf.me, "transit to follower")
		}
		rf.stopHeartbeatTimer <- true
		//go rf.electionTimer()
	} else {
		// reset timer
		rf.stopElectionTimer <- true
	}
	rf.isLeader = false
	rf.isFollower = true

	go rf.electionTimer()
}

// return min
func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

// return max
func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}
