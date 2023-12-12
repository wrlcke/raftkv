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

	// volatile state for internal messages
	state         RaftRoleState   // current state of the server
	leader        int             // id of current leader (initialized to -1)
	electionTimer *time.Timer     // Timer for election timeout
	heartbeatTimer *time.Timer    // Timer for heartbeat
	msgs          RaftMessages	  // Channel for internal messages
	applyCh chan ApplyMsg         // Channel for sending ApplyMsg to service
	steppedDown   atomic.Bool	  // Flag for whether the leader has stepped down, to shutdown the persister

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
	if args.LastLogTerm < rf.store.LastLogTerm() || 
		args.LastLogTerm == rf.store.LastLogTerm() && args.LastLogIndex < rf.store.LastLogIndex() {
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
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, args, &RequestVoteReply{})
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

	if args.PrevLogIndex > rf.store.LastLogIndex() || args.PrevLogTerm != rf.store.Log(args.PrevLogIndex).Term {
		retryMatchIndex, retryMatchTerm := IndexNone, TermNone
		// The `args.PrevLogIndex` is guaranteed to be a mismatch. 
		// Therefore, we initially try `args.PrevLogIndex - 1` (or `lastLogIndex` if `args.PrevLogIndex` is too large).		
		if args.PrevLogIndex > rf.store.LastLogIndex() {
			retryMatchIndex, retryMatchTerm = rf.store.LastLogIndex(), rf.store.LastLogTerm()
		} else {
			retryMatchIndex, retryMatchTerm = args.PrevLogIndex - 1, rf.store.Log(args.PrevLogIndex - 1).Term
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
			for i:=retryMatchIndex - 1; i > IndexStart; i-- {
				if rf.store.Log(i).Term <= args.PrevLogTerm {
					retryMatchIndex, retryMatchTerm = i, rf.store.Log(i).Term
					break
				}
			}
		}
		return &AppendEntriesReply{rf.store.CurrentTerm(), false, retryMatchIndex, retryMatchTerm}
	}
	for i:=0; i < len(args.Entries); i++ {
		if rf.store.LastLogIndex() < args.PrevLogIndex + 1 + i {
			rf.store.AppendLogs(args.Entries[i:]...)
			rf.store.FlushLogs()
			break
		}
		if rf.store.Log(args.PrevLogIndex + 1 + i).Term != args.Entries[i].Term {
			rf.store.RemoveLogSuffix(args.PrevLogIndex + 1 + i)
			rf.store.AppendLogs(args.Entries[i:]...)
			rf.store.FlushLogs()
			break
		}
	}
	// Committed entries will never be overwritten, so even we do storage asynchronously, we can update commitIndex here
	rf.commitIndex = Max(rf.commitIndex, Min(args.LeaderCommit, rf.store.LastLogIndex()))
	if rf.commitIndex > rf.lastApplied {
		entries := rf.store.Logs(rf.lastApplied + 1, rf.commitIndex + 1)
		rf.msgs.applyReq <- entries
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

func (rf *Raft) sendAppendEntriesAsync(server int) {
	args := &AppendEntriesArgs{}
	args.Term = rf.store.CurrentTerm()
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.store.Log(args.PrevLogIndex).Term
	args.Entries = rf.store.Logs(rf.nextIndex[server], rf.store.LastLogIndex() + 1)
	args.LeaderCommit = rf.commitIndex
	go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
}

func (rf *Raft) broadcastAppendEntriesAsync() {
	for i:=0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.sendAppendEntriesAsync(i)
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
	if reply.Success {
		if rf.matchIndex[server] >= args.PrevLogIndex + len(args.Entries) {
			return
		}
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		for i := rf.matchIndex[server]; i > rf.commitIndex && i > args.PrevLogIndex; i-- {
			if rf.store.Log(i).Term == rf.store.CurrentTerm() && rf.countLogReplicas(i) > len(rf.peers)/2 {
				rf.commitIndex = i
				break
			}
		}
		if rf.commitIndex > rf.lastApplied {
			entries := rf.store.Logs(rf.lastApplied + 1, rf.commitIndex + 1)
			rf.msgs.applyReq <- entries
			rf.lastApplied = rf.commitIndex
		}
	} else {
		if rf.matchIndex[server] < args.PrevLogIndex && rf.nextIndex[server] > reply.RetryMatchIndex {
			retryMatchIndex, retryMatchTerm := reply.RetryMatchIndex, reply.RetryMatchTerm
			// If the log at `retryMatchIndex` the follower returned turns out to be a mismatch,
			// If my term of the log at `retryMatchIndex` is greater than the follower's term,
			// then we follow the same procedure as in `AppendEntries` to find a possible match index.
			// Otherwise, we simply decrement `retryMatchIndex`.
			if rf.store.Log(retryMatchIndex).Term > retryMatchTerm {
				for i:=retryMatchIndex - 1; i > IndexStart; i-- {
					if rf.store.Log(i).Term <= retryMatchTerm {
						retryMatchIndex = i
						break
					}
				}
			} else if rf.store.Log(retryMatchIndex).Term < retryMatchTerm {
				retryMatchIndex--
			}
			rf.nextIndex[server] = retryMatchIndex + 1
			rf.sendAppendEntriesAsync(server)
		}
	}
}

func (rf *Raft) storageReturn(firstLogIndex int, lastLogIndex int) {
	if rf.state == StateLeader && rf.matchIndex[rf.me] < lastLogIndex {
		rf.matchIndex[rf.me] = lastLogIndex
		for i := lastLogIndex; i > rf.commitIndex; i-- {
			if rf.store.Log(i).Term == rf.store.CurrentTerm() && rf.countLogReplicas(i) > len(rf.peers)/2 {
				rf.commitIndex = i
				break
			}
		}
		if rf.commitIndex > rf.lastApplied {
			entries := rf.store.Logs(rf.lastApplied + 1, rf.commitIndex + 1)
			rf.msgs.applyReq <- entries
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
}

func (rf *Raft) runnerCleanup() {
	close(rf.msgs.applyReq)
	close(rf.msgs.storageReq)
}

func (rf *Raft) applierCleanup() {
	close(rf.applyCh)
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
	rf.steppedDown.Store(true)
	rf.msgs.storageReq <- StorageRequest{}
	for resp := range rf.msgs.storageResp {
		if resp.firstLogIndex == IndexNone {
			break
		}
	}
}

func (rf *Raft) randomizedElectionTimeout() time.Duration {
	return time.Duration(300 + (rand.Intn(200))) * time.Millisecond
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

			LogPrint(dLog2, "S%d Receiving AppendEntries From: S%d Term: %d PrevLogIndex: %d PrevLogTerm: %d Entries: %v LeaderCommit: %d RequestTerm: %d", rf.me, req.args.LeaderId, rf.store.CurrentTerm(), req.args.PrevLogIndex, req.args.PrevLogTerm, req.args.Entries, req.args.LeaderCommit, req.args.Term)
			*req.reply = *rf.AppendEntries(req.args)
			close(req.done)

		case resp := <-rf.msgs.appResp:

			LogPrint(dLog, "S%d AppendEntries Return From S%d Term: %d Success: %t PrevLogIndex: %d PrevLogTerm: %d Entries: %v LeaderCommit: %d RequestTerm: %d ResponseTerm: %d", rf.me, resp.server, rf.store.CurrentTerm(), resp.reply.Success, resp.args.PrevLogIndex, resp.args.PrevLogTerm, resp.args.Entries, resp.args.LeaderCommit, resp.args.Term, resp.reply.Term)
			rf.appendEntriesReturn(resp.server, resp.args, resp.reply)

		case req := <-rf.msgs.voteReq:

			LogPrint(dVote, "S%d Receiving RequestVote From: S%d Term: %d RequestTerm: %d", rf.me, req.args.CandidateId, rf.store.CurrentTerm(), req.args.Term)
			*req.reply = *rf.RequestVote(req.args)
			close(req.done)

		case resp := <-rf.msgs.voteResp:

			LogPrint(dVote, "S%d RequestVote Return From S%d Term: %d VoteGranted: %t RequestTerm: %d ResponseTerm: %d", rf.me, resp.server, rf.store.CurrentTerm(), resp.reply.VoteGranted, resp.args.Term, resp.reply.Term)
			rf.requestVoteReturn(resp.server, resp.args, resp.reply)

		case req := <-rf.msgs.getStateReq:

			req.reply.term, req.reply.isLeader = rf.store.CurrentTerm(), rf.state == StateLeader
			close(req.done)
		
		case req := <-rf.msgs.startCmdReq:

			if rf.state == StateLeader {
				batchReq := []StartCommandRequest{req}
			batching_loop:
				for {
					select {
						case req := <-rf.msgs.startCmdReq:
							batchReq = append(batchReq, req)
						default:
							break batching_loop
					}
				}
				for i := range batchReq {
					rf.store.AppendLogs(LogEntry{rf.store.CurrentTerm(), batchReq[i].args})
					LogPrint(dLeader, "S%d Starting Command: %v Term: %d Index: %d", rf.me, batchReq[i].args, rf.store.CurrentTerm(), rf.store.LastLogIndex())
					*batchReq[i].reply = StartCommandReply{rf.store.LastLogIndex(), rf.store.CurrentTerm(), true}
					close(batchReq[i].done)
				}
				rf.msgs.storageReq <- StorageRequest{}
				rf.broadcastAppendEntriesAsync()
				rf.resetHeartbeatTimer(rf.stableHeartbeatTimeout())
			} else {
				*req.reply = StartCommandReply{IndexNone, rf.store.CurrentTerm(), false}
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
				rf.broadcastAppendEntriesAsync()
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
	for rf.running() {
		rf.run()
	}
	rf.runnerCleanup()
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
	index := IndexStart
	for entries := range rf.msgs.applyReq {
		for i := range entries {
			index++
			msg := ApplyMsg{}
			msg.CommandValid = true
			msg.Command = entries[i].Command
			msg.CommandIndex = index
			rf.applyCh <- msg
			LogPrint(dCommit, "S%d Applying Command: %v Term: %d Index: %d", rf.me, msg.Command, entries[i].Term, msg.CommandIndex)
		}
	}
	rf.applierCleanup()
}

func (rf *Raft) persister() {
persister_loop:
	for range rf.msgs.storageReq {
batching_loop:
		for {
			select {
				case _, ok := <-rf.msgs.storageReq:
					if !ok {
						break persister_loop
					}
				default: // no-latency batching
					break batching_loop
			}
		}
		first, last := rf.store.FlushLogs()
		rf.msgs.storageResp <- StorageResponse{first, last}
		if rf.steppedDown.Load() {
			rf.msgs.storageResp <- StorageResponse{IndexNone, IndexNone}
			return
		}
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
	}
	for i:=0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = IndexStart
	}
	rf.matchIndex[rf.me] = rf.store.LastLogIndex()
	rf.steppedDown.Store(false)
	go rf.persister()
}

func (rf *Raft) startElection() {
	rf.store.SetCurrentTermAndVotedFor(rf.store.CurrentTerm() + 1, rf.me)
	rf.resetElectionTimer(rf.randomizedElectionTimeout())

	rf.voteCount = 1
	rf.broadcastRequestVoteAsync(&RequestVoteArgs{rf.store.CurrentTerm(), rf.me, rf.store.LastLogIndex(), rf.store.LastLogTerm()})
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
	rf.commitIndex = IndexStart
	rf.lastApplied = IndexStart
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.leader = LeaderNone
	rf.msgs	= MakeRaftMessages()
	rf.electionTimer = time.NewTimer(rf.randomizedElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(rf.stableHeartbeatTimeout())
	rf.msgs.electionT = rf.electionTimer.C
	rf.msgs.heartbeatT = rf.heartbeatTimer.C
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.store = MakeLabPersister(persister)

	// start ticker goroutine to start elections
	// go rf.ticker()
	rf.becomeFollower()
	go rf.runner()
	go rf.applier()

	return rf
}
