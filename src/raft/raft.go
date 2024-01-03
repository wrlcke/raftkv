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
	// "bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "6.5840/labgob"
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

// Log entry structure
type LogEntry struct {
	Term int
	Command interface{}
}

type RaftRoleState int
const (
	StateFollower RaftRoleState = iota
	StateCandidate
	StateLeader
	NumStates
)

const (
	TermNone = -1
	TermStart = 0
	VotedForNone = -1
	LeaderNone = -1
	IndexNone = -1
	IndexStart = 0
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	// persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	// currentTerm int               // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	// votedFor int                  // candidateId that received vote in current term (or null if none)
	// log []LogEntry                // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	store RaftStore               

	// volatile state on all servers
	commitIndex int               // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int			      // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders
	nextIndex []int               // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int              // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	inflight []int                // for each server, number of inflight append entries RPCs
	retrying []bool               // for each server, whether the leader is retrying append entries RPCs
	maxInflight int               // maximum number of inflight append entries RPCs

	// volatile state for internal messages
	state         RaftRoleState   // current state of the server
	leader        int             // id of current leader (initialized to -1)
	electionTimer *time.Timer     // Timer for election timeout
	heartbeatTimer *time.Timer    // Timer for heartbeat
	msgs          RaftMessages	  // Channel for internal messages
	applyCh chan ApplyMsg         // Channel for sending ApplyMsg to service

	// volatile state on candidates
	voteCount     int  		 	  // Number of votes received
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// // Your code here (2A).
	// return term, isleader
	rf.msgs.externalRoutines.add()
	defer rf.msgs.externalRoutines.done()
	if rf.killed() {
		return TermNone, false
	}
	done := make(chan struct{})
	reply := GetStateReply{}
	rf.msgs.getStateReq <- GetStateRequest{&reply, done}
	<-done
	return reply.term, reply.isLeader
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.msgs.externalRoutines.add()
	defer rf.msgs.externalRoutines.done()
	if rf.killed() {
		return
	}
	done := make(chan struct{})
	rf.msgs.startSnapReq <- StartSnapshotRequest{index, snapshot, done}
	<-done
}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int                      // candidate's term
	CandidateId int               // candidate requesting vote
	LastLogIndex int              // index of candidate's last log entry
	LastLogTerm int               // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term int                      // currentTerm, for candidate to update itself
	VoteGranted bool              // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVoteRPC(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.msgs.externalRoutines.add()
	defer rf.msgs.externalRoutines.done()
	if rf.killed() {
		reply.Term = TermNone
		return
	}
	done := make(chan struct{})
	rf.msgs.voteReq <- VoteRequest{args, reply, done}
	<-done
}

func (rf* Raft) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	// Reply false if term < currentTerm
	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote

	if args.Term < rf.store.CurrentTerm() {
		return &RequestVoteReply{rf.store.CurrentTerm(), false}
	}
	if args.Term > rf.store.CurrentTerm() {
		rf.store.SetCurrentTermAndVotedFor(args.Term, VotedForNone)
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	if rf.store.VotedFor() != VotedForNone {
		if rf.store.VotedFor() != args.CandidateId {
			return &RequestVoteReply{rf.store.CurrentTerm(), false}
		} else {
			rf.resetElectionTimer(rf.randomizedElectionTimeout())
			return &RequestVoteReply{rf.store.CurrentTerm(), true}
		}
	}
	if args.LastLogTerm < rf.store.LogTerm(rf.store.LastLogIndex()) || 
		args.LastLogTerm == rf.store.LogTerm(rf.store.LastLogIndex()) && args.LastLogIndex < rf.store.LastLogIndex() {
		return &RequestVoteReply{rf.store.CurrentTerm(), false}
	}
	rf.store.SetVotedFor(args.CandidateId)
	rf.resetElectionTimer(rf.randomizedElectionTimeout())
	return &RequestVoteReply{rf.store.CurrentTerm(), true}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVoteRPC", args, reply)
	if ok {
		rf.msgs.externalRoutines.add()
		defer rf.msgs.externalRoutines.done()
		if rf.killed() {
			return
		}
		rf.msgs.voteResp <- VoteResponse{server, args, reply}
	}
}

func (rf *Raft) broadcastRequestVoteAsync(args *RequestVoteArgs) {
	for i:=0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendRequestVote(i, args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) requestVoteReturn(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	// If votes received from majority of servers: become leader

	if reply.Term > rf.store.CurrentTerm() {
		rf.store.SetCurrentTermAndVotedFor(reply.Term, VotedForNone)
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
		return
	}
	if reply.VoteGranted && rf.store.CurrentTerm() == args.Term && rf.state == StateCandidate {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.becomeLeader()
		}
	}
}

type AppendEntriesArgs struct {
	Term int 					  // leader's term
	LeaderId int 				  // so follower can redirect clients
	PrevLogIndex int 			  // index of log entry immediately preceding new ones
	PrevLogTerm int 			  // term of prevLogIndex entry
	Entries []LogEntry 			  // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int 			  // leader's commitIndex
}

type AppendEntriesReply struct {
	Term int                      // currentTerm, for leader to update itself
	Success bool                  // true if follower contained entry matching prevLogIndex and prevLogTerm
	RetryMatchIndex int             // possible match index for leader to retry
	RetryMatchTerm int              // possible log term for leader to retry
}

func (rf *Raft) AppendEntriesRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.msgs.externalRoutines.add()
	defer rf.msgs.externalRoutines.done()
	if rf.killed() {
		reply.Term = TermNone
		return
	}
	done := make(chan struct{})
	rf.msgs.appReq <- AppendRequest{args, reply, done}
	<-done
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	// Reply false if term < currentTerm
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// Append any new entries not already in the log
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	if args.Term < rf.store.CurrentTerm() {
		return &AppendEntriesReply{rf.store.CurrentTerm(), false, IndexNone, TermNone}
	}
	if args.Term > rf.store.CurrentTerm() {
		rf.store.SetCurrentTermAndVotedFor(args.Term, VotedForNone)
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	if rf.state == StateCandidate {
		rf.becomeFollower()
	}
	rf.leader = args.LeaderId
	rf.resetElectionTimer(rf.randomizedElectionTimeout())

	if args.PrevLogIndex > rf.store.LastLogIndex() || 
	args.PrevLogIndex >= rf.store.FirstLogIndex() &&
	args.PrevLogTerm != rf.store.LogTerm(args.PrevLogIndex) {
		retryMatchIndex, retryMatchTerm := IndexNone, TermNone
		// The `args.PrevLogIndex` is guaranteed to be a mismatch. 
		// Therefore, we initially try `args.PrevLogIndex - 1` (or `lastLogIndex` if `args.PrevLogIndex` is too large).		
		if args.PrevLogIndex > rf.store.LastLogIndex() {
			retryMatchIndex, retryMatchTerm = rf.store.LastLogIndex(), rf.store.LogTerm(rf.store.LastLogIndex())
		} else {
			retryMatchIndex, retryMatchTerm = args.PrevLogIndex - 1, rf.store.LogTerm(args.PrevLogIndex - 1)
		}
		// If the term of the index we're trying is greater than `args.PrevLogTerm`, 
		// then because the tried index is less than `args.PrevLogIndex`, 
		// the term of the leader's log at this index is definitely less than or equal to 
		// `args.PrevLogTerm` (because terms of the logs increase monotonically). 
		// Hence, it must be less than (and so mismatch) the term of my log at this index. 
		if retryMatchTerm > args.PrevLogTerm {
			// We continue to search backwards until we find an index where the term is 
			// less than or equal to `args.PrevLogTerm`. 
			// Only then, the term of the leader at this index could possibly be equal to
			//  the term of my log at this index.
			for i:=retryMatchIndex - 1; i >= rf.store.FirstLogIndex(); i-- {
				if rf.store.LogTerm(i) <= args.PrevLogTerm {
					retryMatchIndex, retryMatchTerm = i, rf.store.LogTerm(i)
					break
				}
			}
		}
		return &AppendEntriesReply{rf.store.CurrentTerm(), false, retryMatchIndex, retryMatchTerm}
	}
	for i:=0; i < len(args.Entries); i++ {
		if args.PrevLogIndex + 1 + i > rf.store.LastLogIndex() {
			rf.store.AppendLogs(args.Entries[i:]...)
			rf.store.FlushLogs()
			break
		}
		if args.PrevLogIndex + 1 + i >= rf.store.FirstLogIndex() &&
		rf.store.LogTerm(args.PrevLogIndex + 1 + i) != args.Entries[i].Term {
			rf.store.RemoveLogSuffix(args.PrevLogIndex + 1 + i)
			rf.store.AppendLogs(args.Entries[i:]...)
			rf.store.FlushLogs()
			break
		}
	}
	// Committed entries will never be overwritten, so even we do storage asynchronously, we can update commitIndex here
	rf.commitIndex = Max(rf.commitIndex, Min(args.LeaderCommit, rf.store.LastLogIndex()))
	if rf.commitIndex > rf.lastApplied {
		rf.msgs.applyReq <- ApplyRequest{
			entries: rf.store.Logs(rf.lastApplied + 1, rf.commitIndex + 1),
			index: rf.lastApplied + 1,
		}
		rf.lastApplied = rf.commitIndex
	}
	return &AppendEntriesReply{rf.store.CurrentTerm(), true, IndexNone, TermNone}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	if ok {
		rf.msgs.externalRoutines.add()
		defer rf.msgs.externalRoutines.done()
		if rf.killed() {
			return
		}
		rf.msgs.appResp <- AppendResponse{server, args, reply}
	}
}

func (rf *Raft) sendAppendEntriesAsync(server int, heartbeat bool) {
	if (rf.inflight[server] >= rf.maxInflight || rf.retrying[server]) && !heartbeat {
		return
	}
	rf.inflight[server]++
	if rf.nextIndex[server] >= rf.store.FirstLogIndex() {
		args := &AppendEntriesArgs{}
		args.Term = rf.store.CurrentTerm()
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.store.LogTerm(args.PrevLogIndex)
		args.Entries = rf.store.Logs(rf.nextIndex[server], rf.store.LastLogIndex() + 1)
		args.LeaderCommit = rf.commitIndex
		LogPrint(dLeader, "S%d Sending AppendEntries To: S%d Term: %d PrevLogIndex: %d PrevLogTerm: %d Entries: %d-%d LeaderCommit: %d", rf.me, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.PrevLogIndex + 1, args.PrevLogIndex + len(args.Entries), args.LeaderCommit)
		go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
	} else {
		args := &InstallSnapshotArgs{}
		args.Term = rf.store.CurrentTerm()
		args.LeaderId = rf.me
		args.LastIncludedIndex = rf.store.FirstLogIndex() - 1
		args.LastIncludedTerm = rf.store.LogTerm(args.LastIncludedIndex)
		args.Data = rf.store.Snapshot()
		go rf.sendInstallSnapshot(server, args, &InstallSnapshotReply{})
	}
}

func (rf *Raft) broadcastAppendEntriesAsync(heartbeat bool) {
	for i:=0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.sendAppendEntriesAsync(i, heartbeat)
		}
	}
}

func (rf *Raft) appendEntriesReturn(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	// If successful: update nextIndex and matchIndex for follower
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N
	// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry

	if reply.Term > rf.store.CurrentTerm() {
		rf.store.SetCurrentTermAndVotedFor(reply.Term, VotedForNone)
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	if reply.Term < rf.store.CurrentTerm() || args.Term != rf.store.CurrentTerm() || rf.state != StateLeader {
		return
	}
	rf.inflight[server]--
	if reply.Success {
		if rf.matchIndex[server] >= args.PrevLogIndex + len(args.Entries) {
			return
		}
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.retrying[server] = false
		for i := rf.matchIndex[server]; i > rf.commitIndex && i > args.PrevLogIndex; i-- {
			if rf.store.LogTerm(i) == rf.store.CurrentTerm() && rf.countLogReplicas(i) > len(rf.peers)/2 {
				rf.commitIndex = i
				break
			}
		}
		if rf.commitIndex > rf.lastApplied {
			rf.msgs.applyReq <- ApplyRequest{
				entries: rf.store.Logs(rf.lastApplied + 1, rf.commitIndex + 1),
				index: rf.lastApplied + 1,
			}
			rf.lastApplied = rf.commitIndex
		}
	} else {
		if rf.matchIndex[server] < args.PrevLogIndex && rf.nextIndex[server] > reply.RetryMatchIndex {
			retryMatchIndex, retryMatchTerm := reply.RetryMatchIndex, reply.RetryMatchTerm
			// If the log at `retryMatchIndex` the follower returned turns out to be a mismatch,
			// If my term of the log at `retryMatchIndex` is greater than the follower's term,
			// then we follow the same procedure as in `AppendEntries` to find a possible match index.
			// Otherwise, we simply decrement `retryMatchIndex`.
			if retryMatchIndex >= rf.store.FirstLogIndex() { 
				if rf.store.LogTerm(retryMatchIndex) > retryMatchTerm {
					for i:=retryMatchIndex - 1; i >= rf.store.FirstLogIndex(); i-- {
						if rf.store.LogTerm(i) <= retryMatchTerm {
							retryMatchIndex = i
							break
						}
					}
				} else if rf.store.LogTerm(retryMatchIndex) < retryMatchTerm {
					retryMatchIndex--
				}
			}
			rf.nextIndex[server] = retryMatchIndex + 1
			rf.retrying[server] = true
			rf.sendAppendEntriesAsync(server, false)
		}
	}
}

type InstallSnapshotArgs struct {
	Term int                      // leader's term
	LeaderId int                  // so follower can redirect clients
	LastIncludedIndex int         // the snapshot replaces all entries up through and including this index
	LastIncludedTerm int          // term of lastIncludedIndex
	Data []byte                   // raw bytes of the snapshot
}

type InstallSnapshotReply struct {
	Term int                      // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshotRPC(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.msgs.externalRoutines.add()
	defer rf.msgs.externalRoutines.done()
	if rf.killed() {
		reply.Term = TermNone
		return
	}
	done := make(chan struct{})
	rf.msgs.snapReq <- SnapRequest{args, reply, done}
	<-done
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	// Reply false if term < currentTerm
	// Create new snapshot file 
	// Write data into snapshot file
	// Save snapshot file, discard any existing or partial snapshot with a smaller index
	// If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply
	// Discard the entire log
	// Reset state machine using snapshot contents (and load snapshot's cluster configuration)

	if args.Term < rf.store.CurrentTerm() {
		return &InstallSnapshotReply{rf.store.CurrentTerm()}
	}
	if args.Term > rf.store.CurrentTerm() {
		rf.store.SetCurrentTermAndVotedFor(args.Term, VotedForNone)
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	rf.leader = args.LeaderId
	rf.resetElectionTimer(rf.randomizedElectionTimeout())

	if args.LastIncludedIndex < rf.store.FirstLogIndex() {
		return &InstallSnapshotReply{rf.store.CurrentTerm()}
	}

	if args.LastIncludedIndex <= rf.store.LastLogIndex() && args.LastIncludedTerm == rf.store.LogTerm(args.LastIncludedIndex) {
		rf.store.RemoveLogPrefix(args.LastIncludedIndex)
	} else {
		rf.store.ResetLog(args.LastIncludedIndex, args.LastIncludedTerm)
	}
	rf.store.SetSnapshot(args.Data)
	rf.store.FlushLogs()

	rf.commitIndex = Max(rf.commitIndex, args.LastIncludedIndex)
	if rf.commitIndex > rf.lastApplied {
		rf.msgs.applyReq <- ApplyRequest {
			index: args.LastIncludedIndex,
			term: args.LastIncludedTerm,
			snapshot: args.Data,
		}
		rf.lastApplied = args.LastIncludedIndex
	}
	return &InstallSnapshotReply{rf.store.CurrentTerm()}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshotRPC", args, reply)
	if ok {
		rf.msgs.externalRoutines.add()
		defer rf.msgs.externalRoutines.done()
		if rf.killed() {
			return
		}
		rf.msgs.snapResp <- SnapResponse{server, args, reply}
	}
}

func (rf *Raft) InstallSnapshotReturn(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower

	if reply.Term > rf.store.CurrentTerm() {
		rf.store.SetCurrentTermAndVotedFor(reply.Term, VotedForNone)
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	if reply.Term < rf.store.CurrentTerm() || args.Term != rf.store.CurrentTerm() || rf.state != StateLeader {
		return
	}
	rf.inflight[server]--
	if rf.matchIndex[server] >= args.LastIncludedIndex {
		return
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = rf.nextIndex[server] - 1
	rf.retrying[server] = false
	for i := rf.matchIndex[server]; i > rf.commitIndex; i-- {
		if rf.store.LogTerm(i) == rf.store.CurrentTerm() && rf.countLogReplicas(i) > len(rf.peers)/2 {
			rf.commitIndex = i
			break
		}
	}
	if rf.commitIndex > rf.lastApplied {
		rf.msgs.applyReq <- ApplyRequest{
			entries: rf.store.Logs(rf.lastApplied + 1, rf.commitIndex + 1),
			index: rf.lastApplied + 1,
		}
		rf.lastApplied = rf.commitIndex
	}
}

func (rf *Raft) storageReturn(firstLogIndex int, lastLogIndex int) {
	if rf.state == StateLeader && rf.matchIndex[rf.me] < lastLogIndex {
		rf.matchIndex[rf.me] = lastLogIndex
		for i := lastLogIndex; i > rf.commitIndex; i-- {
			if rf.store.LogTerm(i) == rf.store.CurrentTerm() && rf.countLogReplicas(i) > len(rf.peers)/2 {
				rf.commitIndex = i
				break
			}
		}
		if rf.commitIndex > rf.lastApplied {
			rf.msgs.applyReq <- ApplyRequest{
				entries: rf.store.Logs(rf.lastApplied + 1, rf.commitIndex + 1),
				index: rf.lastApplied + 1,
			}
			rf.lastApplied = rf.commitIndex
		}
	}
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
	// index := -1
	// term := -1
	// isLeader := true

	// Your code here (2B).
	rf.msgs.externalRoutines.add()
	defer rf.msgs.externalRoutines.done()
	if rf.killed() {
		return IndexNone, TermNone, false
	}
	done := make(chan struct{})
	reply := StartCommandReply{}
	rf.msgs.startCmdReq <- StartCommandRequest{command, &reply, done}
	<-done
	return reply.index, reply.term, reply.isLeader
	// return index, term, isLeader
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
	rf.cleanup()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) cleanup() {
	rf.electionTimer.Stop()
	rf.heartbeatTimer.Stop()
	rf.msgs.externalRoutines.wait()
	close(rf.msgs.shutdown)
	rf.msgs.internalRoutines.wait("runner")
	close(rf.msgs.applyReq)
	rf.msgs.internalRoutines.wait("applier")
	close(rf.applyCh)
	if rf.msgs.storageReq != nil {
		close(rf.msgs.storageReq)
		rf.msgs.internalRoutines.wait("persister")
	}
}

func (rf *Raft) countLogReplicas(index int) int {
	count := 0
	for i := 0; i < len(rf.peers); i++ {
		// if i == rf.me, matchIndex represents the index of the last log entry persisted in leader's self storage
		if rf.matchIndex[i] >= index {
			count++
		}
	}
	return count
}

func (rf *Raft) leaderWaitFlush() {
	close(rf.msgs.storageReq)
	rf.msgs.internalRoutines.wait("persister")
	rf.msgs.storageReq = nil
}

func (rf *Raft) randomizedElectionTimeout() time.Duration {
	return time.Duration(500 + (rand.Intn(300))) * time.Millisecond
}

func (rf *Raft) stableHeartbeatTimeout() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func (rf *Raft) immediateTimeout() time.Duration {
	return time.Duration(0) * time.Millisecond
}

func (rf *Raft) resetElectionTimer(d time.Duration) {
	if !rf.electionTimer.Stop() {
		select {
			case <-rf.electionTimer.C:
			default:
		}
	}
	rf.electionTimer.Reset(d)
}

func (rf *Raft) resetHeartbeatTimer(d time.Duration) {
	if !rf.heartbeatTimer.Stop() {
		select {
			case <-rf.heartbeatTimer.C:
			default:
		}
	}
	rf.heartbeatTimer.Reset(d)
}

func (rf *Raft) run() {
	select {
		case req := <-rf.msgs.appReq:

			LogPrint(dLog2, "S%d Receiving AppendEntries From: S%d Term: %d PrevLogIndex: %d PrevLogTerm: %d Entries: %d-%d LeaderCommit: %d RequestTerm: %d", rf.me, req.args.LeaderId, rf.store.CurrentTerm(), req.args.PrevLogIndex, req.args.PrevLogTerm, req.args.PrevLogIndex + 1, req.args.PrevLogIndex + len(req.args.Entries), req.args.LeaderCommit, req.args.Term)
			*req.reply = *rf.AppendEntries(req.args)
			LogPrint(dLog2, "S%d Finished AppendEntries From S%d Term: %d Success: %t PrevLogIndex: %d PrevLogTerm: %d Entries: %d-%d LeaderCommit: %d RequestTerm: %d ResponseTerm: %d", rf.me, req.args.LeaderId, rf.store.CurrentTerm(), req.reply.Success, req.args.PrevLogIndex, req.args.PrevLogTerm, req.args.PrevLogIndex + 1, req.args.PrevLogIndex + len(req.args.Entries), req.args.LeaderCommit, req.args.Term, req.reply.Term)
			close(req.done)

		case resp := <-rf.msgs.appResp:

			LogPrint(dLog, "S%d AppendEntries Return From S%d Term: %d Success: %t PrevLogIndex: %d PrevLogTerm: %d Entries: %d-%d LeaderCommit: %d RequestTerm: %d ResponseTerm: %d", rf.me, resp.server, rf.store.CurrentTerm(), resp.reply.Success, resp.args.PrevLogIndex, resp.args.PrevLogTerm, resp.args.PrevLogIndex + 1, resp.args.PrevLogIndex + len(resp.args.Entries), resp.args.LeaderCommit, resp.args.Term, resp.reply.Term)
			rf.appendEntriesReturn(resp.server, resp.args, resp.reply)

		case req := <-rf.msgs.voteReq:

			LogPrint(dVote, "S%d Receiving RequestVote From: S%d Term: %d RequestTerm: %d", rf.me, req.args.CandidateId, rf.store.CurrentTerm(), req.args.Term)
			*req.reply = *rf.RequestVote(req.args)
			close(req.done)

		case resp := <-rf.msgs.voteResp:

			LogPrint(dVote, "S%d RequestVote Return From S%d Term: %d VoteGranted: %t RequestTerm: %d ResponseTerm: %d", rf.me, resp.server, rf.store.CurrentTerm(), resp.reply.VoteGranted, resp.args.Term, resp.reply.Term)
			rf.requestVoteReturn(resp.server, resp.args, resp.reply)

		case req := <-rf.msgs.snapReq:

			LogPrint(dSnap, "S%d Receiving InstallSnapshot From: S%d Term: %d LastIncludedIndex: %d LastIncludedTerm: %d RequestTerm: %d", rf.me, req.args.LeaderId, rf.store.CurrentTerm(), req.args.LastIncludedIndex, req.args.LastIncludedTerm, req.args.Term)
			*req.reply = *rf.InstallSnapshot(req.args)
			close(req.done)

		case req := <- rf.msgs.snapResp:

			LogPrint(dSnap, "S%d InstallSnapshot Return From S%d Term: %d LastIncludedIndex: %d LastIncludedTerm: %d RequestTerm: %d ResponseTerm: %d", rf.me, req.server, rf.store.CurrentTerm(), req.args.LastIncludedIndex, req.args.LastIncludedTerm, req.args.Term, req.reply.Term)
			rf.InstallSnapshotReturn(req.server, req.args, req.reply)

		case req := <-rf.msgs.getStateReq:

			req.reply.term, req.reply.isLeader = rf.store.CurrentTerm(), rf.state == StateLeader
			close(req.done)
		
		case req := <-rf.msgs.startCmdReq:

			if rf.state == StateLeader {
				begin_index := rf.store.LastLogIndex() + 1
				rf.store.AppendLogs(LogEntry{rf.store.CurrentTerm(), req.args})
				*req.reply = StartCommandReply{rf.store.LastLogIndex(), rf.store.CurrentTerm(), true}
				close(req.done)
			batching_loop:
				for {
					select {
						case req := <-rf.msgs.startCmdReq:
							rf.store.AppendLogs(LogEntry{rf.store.CurrentTerm(), req.args})
							*req.reply = StartCommandReply{rf.store.LastLogIndex(), rf.store.CurrentTerm(), true}
							close(req.done)
						default:
							break batching_loop
					}
				}
				LogPrint(dLeader, "S%d Starting Command Term: %d Index: %d-%d", rf.me, rf.store.CurrentTerm(), begin_index, rf.store.LastLogIndex())
				rf.msgs.storageReq <- StorageRequest{}
				rf.broadcastAppendEntriesAsync(false)
				rf.resetHeartbeatTimer(rf.stableHeartbeatTimeout())
			} else {
				*req.reply = StartCommandReply{IndexNone, rf.store.CurrentTerm(), false}
				close(req.done)
			}

		case req := <- rf.msgs.startSnapReq:

			LogPrint(dSnap, "S%d Starting Snapshot Term: %d Index: %d", rf.me, rf.store.CurrentTerm(), req.index)
			if req.index >= rf.store.FirstLogIndex() {
				rf.store.RemoveLogPrefix(req.index)
				rf.store.SetSnapshot(req.snapshot)
				if rf.state == StateLeader {
					rf.msgs.storageReq <- StorageRequest{req.done}
				} else {
					rf.store.FlushLogs()
					close(req.done)
				}
			} else {
				close(req.done)
			}
		
		case <-rf.msgs.electionT:
			
			if rf.state != StateLeader {
				LogPrint(dTimer, "S%d Election Timeout Term: %d", rf.me, rf.store.CurrentTerm())
				rf.becomeCandidate()
			}
		
		case <-rf.msgs.heartbeatT:

			if rf.state == StateLeader {
				LogPrint(dTimer, "S%d Heartbeat Timeout Term: %d", rf.me, rf.store.CurrentTerm())
				rf.broadcastAppendEntriesAsync(true)
				rf.resetHeartbeatTimer(rf.stableHeartbeatTimeout())
			}

		case resp := <- rf.msgs.storageResp:
			
			LogPrint(dPersist, "S%d Storage Completed Term:%d Memory: %d-%d Storage: %d-%d", rf.me, rf.store.CurrentTerm(), rf.store.FirstLogIndex(), rf.store.LastLogIndex(), resp.firstLogIndex, resp.lastLogIndex)
			rf.storageReturn(resp.firstLogIndex, resp.lastLogIndex)
		
		case <-rf.msgs.shutdown:

			LogPrint(dInfo, "S%d Shutdown", rf.me)
			return
	}	
}

func (rf *Raft) runner() {
	defer rf.msgs.internalRoutines.done("runner")
	for rf.running() {
		rf.run()
	}
}

func (rf *Raft) running() bool {
	select {
		case <-rf.msgs.shutdown:
			return false
		default:
			return true
	}
}

func (rf *Raft) applier() {
	defer rf.msgs.internalRoutines.done("applier")
	for req := range rf.msgs.applyReq {
		if req.snapshot != nil {
			LogPrint(dCommit, "S%d Applying Snapshot LastIndex: %d LastTerm: %d", rf.me, req.index, req.term)
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot: req.snapshot,
				SnapshotIndex: req.index,
				SnapshotTerm: req.term,
			}
		} else {
			LogPrint(dCommit, "S%d Applying Command: %d-%d", rf.me, req.index, req.index + len(req.entries) - 1)
			for i := range req.entries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command: req.entries[i].Command,
					CommandIndex: req.index + i,
				}
			}
		}
	}
}

func (rf *Raft) persister() {
	defer rf.msgs.internalRoutines.done("persister")
	for req := range rf.msgs.storageReq {
		dones := []chan struct{}{req.done}
batching_loop:
		for {
			select {
				case more, ok := <-rf.msgs.storageReq:
					if !ok {
						break batching_loop
					}
					dones = append(dones, more.done)
				default: // no-latency batching
					break batching_loop
			}
		}
		first, last := rf.store.FlushLogs()
		for i := range dones {
			if dones[i] != nil {
				close(dones[i])
			}
		}
		rf.msgs.storageResp <- StorageResponse{first, last}
	}
}

func (rf *Raft) becomeFollower() {
	if rf.state == StateLeader {
		rf.leaderWaitFlush()
	}
	rf.resetElectionTimer(rf.randomizedElectionTimeout())
	rf.state = StateFollower
	rf.leader = LeaderNone
}

func (rf *Raft) becomeCandidate() {
	if rf.state == StateLeader {
		rf.leaderWaitFlush()
	}
	rf.state = StateCandidate
	rf.leader = LeaderNone
	rf.startElection()
}

func (rf *Raft) becomeLeader() {
	rf.resetHeartbeatTimer(rf.immediateTimeout())
	rf.state = StateLeader
	rf.leader = rf.me
	for i:=0; i < len(rf.peers); i++ { 
		rf.nextIndex[i] = rf.store.LastLogIndex() + 1
		if i == rf.me {
			rf.matchIndex[i] = rf.store.LastLogIndex()
		} else {
			rf.matchIndex[i] = IndexStart
		}
		rf.inflight[i] = 0
		rf.retrying[i] = false
	}
	rf.msgs.storageReq = make(chan StorageRequest, 1000)
	rf.msgs.storageResp = make(chan StorageResponse, 1000)
	rf.msgs.internalRoutines.add("persister")
	go rf.persister()
}

func (rf *Raft) startElection() {
	rf.store.SetCurrentTermAndVotedFor(rf.store.CurrentTerm() + 1, rf.me)
	rf.resetElectionTimer(rf.randomizedElectionTimeout())

	rf.voteCount = 1
	rf.broadcastRequestVoteAsync(&RequestVoteArgs{rf.store.CurrentTerm(), rf.me, rf.store.LastLogIndex(), rf.store.LogTerm(rf.store.LastLogIndex())})
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
	// rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.inflight = make([]int, len(rf.peers))
	rf.retrying = make([]bool, len(rf.peers))
	rf.maxInflight = 10

	rf.leader = LeaderNone
	rf.msgs	= MakeRaftMessages()
	rf.electionTimer = time.NewTimer(rf.randomizedElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(rf.stableHeartbeatTimeout())
	rf.msgs.electionT = rf.electionTimer.C
	rf.msgs.heartbeatT = rf.heartbeatTimer.C
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.store = MakeLabPersister(persister)
	rf.commitIndex = rf.store.FirstLogIndex() - 1
	rf.lastApplied = rf.store.FirstLogIndex() - 1

	// start ticker goroutine to start elections
	// go rf.ticker()
	rf.becomeFollower()
	rf.msgs.internalRoutines.add("runner", "applier")
	go rf.runner()
	go rf.applier()

	return rf
}
