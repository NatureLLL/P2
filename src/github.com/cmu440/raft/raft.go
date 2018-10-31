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
	applyCh     chan ApplyMsg    // channel for passing applyMsg
	currentTerm int              // latest term server has seen
	logEntries  []LogEntry       // todo: cautious when using slice, index starts from 1
	votedFor    int              // candidateId that received vote in current term, set to -1 when update currentTerm
	isLeader    bool             // whether this peer believes it is the leader
	isFollower  bool             // whether it's follower
	leaderId    int              // id of leader

	commitIndex int //index of highest log entry known to be committed (initialized to 0, update when receive appendEntries RPC
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, update when send to ApplyCh

	numVotes int // votes a candidate get, initialize to 0 when transit to candidate

	// leader owns, when transit to leader, re-initialize this
	nextIndex  []int //
	matchIndex []int

	//resetElection chan bool // reset election timer
	stopElectionTimer  chan bool // when transit to leader, use it
	stopHeartbeatTimer chan bool // when transit from leader to follower, use it
	resetElectionTimer chan bool

	debug bool // fmt print
}

// custom struct
type LogEntry struct {
	term    int         // term of the log
	command interface{} //
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
	Term         int // leader's currentTerm
	LeaderId     int // follower use this to redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones, aka, last log index
	PrevLogTerm  int // term of prevLogIndex entry
	//Entries      []LogEntry // todo: be aware, when it's empty, segfault, log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int // leader’s commitIndex
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
	reply.Term = rf.currentTerm
	// term check
	//fmt.Println("AppendEntries", args.LeaderId, "to ", rf.me, ": args term", args.Term, "current term", rf.currentTerm)

	if rf.currentTerm <= args.Term {
		if rf.currentTerm < args.Term {
			// update term
			rf.currentTerm = args.Term
			// update votedFor, in this new term, doesn't vote yet
			rf.votedFor = -1
		}
		success = true // for checkpoint, just set success to true
		rf.leaderId = args.LeaderId
		rf.transitToFollower()
		if rf.debug && (!rf.isLeader) && (!rf.isFollower) {
			fmt.Printf("leader %d makes candidate %d transits to follower\n", args.LeaderId,rf.me)
		}
		// todo: process RPC
	}
	//fmt.Println("serve ", rf.me, "reply ", success)
	rf.mux.Unlock()
	reply.Success = success

	/* todo
	consistency check
	prevLogIndex, prevLogTerm not match
	1. I need to delete:
	if len(rf.logEntries) >= prevLogIndex {
		rf.logEntries = rf.logEntries[:prevLogIndex]
		if rf.logEntries[prevLogIndex-1].term !=
	2.
	}
	*/

}

func (rf *Raft) sendAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", args, reply)
	rf.mux.Lock()
	if rf.isLeader {
		if ok {
			if !reply.Success {
				// for checkpoint, the only case it fails is that leader's term < receiver's term
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.votedFor = -1

					rf.transitToFollower()
					if rf.debug {
						fmt.Printf("%d makes %d transits to follower\n", peer,rf.me)
					}

				} else {
					// todo: consider log doesn't match
				}

			} else {
				// todo: handle success
			}
		} else {
			// retry
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
	if rf.currentTerm < args.Term {
		voteGranted = true
	} else if (rf.currentTerm == args.Term) &&
		((rf.votedFor == -1) || (rf.votedFor == args.CandidateId)) {
		// check log freshness
		lastLogIndex := len(rf.logEntries) - 1
		lastLogTerm := rf.logEntries[lastLogIndex].term
		if (lastLogTerm < args.LastLogTerm) ||
			((lastLogTerm == args.LastLogTerm) && (lastLogIndex <= args.LastLogIndex)) {
			voteGranted = true
		}
	}
	if voteGranted {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.transitToFollower()
		if rf.debug {
			fmt.Printf("%d grant vote to %d, with term %d\n", rf.me, rf.votedFor, rf.currentTerm)
		}
	}
	reply.Term = rf.currentTerm
	//fmt.Println("server ", rf.me, "vote ", voteGranted)
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
			// decode reply message, check for valid votes for this term:todo, correct?
			if reply.VoteGranted && (rf.currentTerm == reply.Term) {
				rf.numVotes++
				if rf.debug {
					fmt.Printf("candidate %d have %d votes\n",rf.me, rf.numVotes)
				}
				if 2*rf.numVotes > len(rf.peers) {
					rf.transitToLeader()
				}

			} else if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.transitToFollower()
				if rf.debug {
					fmt.Printf("candidate %d transits to follower\n",rf.me)
				}
			}
		} // todo: question? do we need to retry requestvoterpc?
		//else {
		//	// retry
		//	go rf.sendRequestVote(peer, args, reply)
		//}
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
		index = len(rf.logEntries)
		term = rf.currentTerm

		// todo: issue appendEntries to other servers in parallel
	}
	// todo: else, redirect client

	rf.mux.Unlock()

	return index, term, isLeader
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
		applyCh:            applyCh,
		currentTerm:        0,
		isLeader:           false,
		isFollower:         true,
		leaderId:           -1,
		votedFor:           -1,
		logEntries:         make([]LogEntry, 1), // make life easier
		stopElectionTimer:  make(chan bool, 1),
		stopHeartbeatTimer: make(chan bool, 1),
		resetElectionTimer: make(chan bool, 1),
		debug:              true,
	}
	rf.logEntries[0] = LogEntry{term: 0}

	// create background goroutine pass applyCh as argument
	// starts as follower
	go rf.electionTimer()

	return rf
}

// only runs for follower and candidate
// randomize election time between 200-400ms
func (rf *Raft) electionTimer() {
	t := time.NewTimer(time.Duration(rand.Intn(150)+200) * time.Millisecond)
	for {
		select {
		case <-t.C:
			// handle election
			rf.mux.Lock()
			rf.startElection()
			rf.mux.Unlock()

		case <-rf.stopElectionTimer:
			return

		//case <-rf.resetElectionTimer:
			//t.Reset(time.Duration(rand.Intn(200)+200) * time.Millisecond)
			//fmt.Println("server",rf.me, "reset election timer")
		}
		t.Reset(time.Duration(rand.Intn(150)+200) * time.Millisecond)
	}
}

func (rf *Raft) heartbeatTimer() {
	t := time.NewTimer(time.Duration(100) * time.Millisecond)
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
		t.Reset(time.Duration(100) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {

	rf.isFollower = false
	rf.isLeader = false
	rf.currentTerm++
	if rf.debug {
		fmt.Println("server ", rf.me, "start election with term", rf.currentTerm)
	}
	// vote for itself
	rf.votedFor = rf.me
	rf.numVotes = 1

	// issue voteRequests to other peers in parallel
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logEntries) - 1,
		LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].term,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
}

// require lock beforehead
func (rf *Raft) spreadHeartbeat() {
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.logEntries) - 1,
		PrevLogTerm:  rf.logEntries[len(rf.logEntries)-1].term,
	}
	// issue heartbeat to other peers in parallel
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendAppendEntries(i, args, &AppendEntriesReply{})
		}
	}
}

// require lock before calling it
// stop election timer, start heartbeat timer, isLeader=true
func (rf *Raft) transitToLeader() {
	if rf.debug {
		fmt.Println("server ", rf.me, "becomes leader with term", rf.currentTerm)
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
		//rf.resetElectionTimer <- true
		rf.stopElectionTimer <- true
	}
	rf.isLeader = false
	rf.isFollower = true

	go rf.electionTimer()
}
