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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
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

const (
	StateFollower = iota
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
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int               // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor int                  // candidateId that received vote in current term (or null if none)
	log []LogEntry		          // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// volatile state on all servers
	commitIndex int               // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int			      // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders
	nextIndex []int               // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int              // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// volatile state for internal messages
	state         int
	leader        int             // id of current leader (initialized to -1)
	electionTimer *time.Timer     // Timer for election timeout
	heartbeatTimer *time.Timer    // Timer for heartbeat
	msgs          RaftMessages	  // Channel for internal messages
	applyCh chan ApplyMsg         // Channel for sending ApplyMsg to service
	commitCh chan []LogEntry      // Channel for sending committed log entries to applier

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
	rf.msgs.senderGroup.add()
	defer rf.msgs.senderGroup.done()
	if rf.killed() {
		return TermNone, false
	}
	rf.msgs.getStateReq.inC <- struct{}{}
	res := <- rf.msgs.getStateReq.outC
	return res.term, res.isLeader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.currentTerm = TermStart
		rf.votedFor = VotedForNone
		rf.log = make([]LogEntry, IndexStart + 1, 10000)
		rf.persist()
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
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.log) != nil {
		panic("Error reading persisted state")
	}
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
	rf.msgs.senderGroup.add()
	defer rf.msgs.senderGroup.done()
	if rf.killed() {
		reply.Term = TermNone
		return
	}
	rf.msgs.voteReq.inC <- args
	*reply = *<-rf.msgs.voteReq.outC
}

func (rf* Raft) RequestVote(args *RequestVoteArgs) *RequestVoteReply {
	// Reply false if term < currentTerm
	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote

	if args.Term < rf.currentTerm {
		return &RequestVoteReply{rf.currentTerm, false}
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, VotedForNone
		rf.persist()
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	if rf.votedFor != VotedForNone {
		if rf.votedFor != args.CandidateId {
			return &RequestVoteReply{rf.currentTerm, false}
		} else {
			rf.resetElectionTimer(rf.randomizedElectionTimeout())
			return &RequestVoteReply{rf.currentTerm, true}
		}
	}
	if args.LastLogTerm < rf.lastLogTerm() || 
		args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex < rf.lastLogIndex() {
		return &RequestVoteReply{rf.currentTerm, false}
	}
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.resetElectionTimer(rf.randomizedElectionTimeout())
	return &RequestVoteReply{rf.currentTerm, true}
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
		rf.msgs.senderGroup.add()
		defer rf.msgs.senderGroup.done()
		if rf.killed() {
			return
		}
		rf.msgs.voteResp.inC <- &RequestVoteResult{server, args, reply}
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

	if reply.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = reply.Term, VotedForNone
		rf.persist()
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
		return
	}
	if reply.VoteGranted && rf.currentTerm == args.Term && rf.state == StateCandidate {
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
	rf.msgs.senderGroup.add()
	defer rf.msgs.senderGroup.done()
	if rf.killed() {
		reply.Term = TermNone
		return
	}
	rf.msgs.appReq.inC <- args
	*reply = *<-rf.msgs.appReq.outC
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	// Reply false if term < currentTerm
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// Append any new entries not already in the log
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	if args.Term < rf.currentTerm {
		return &AppendEntriesReply{rf.currentTerm, false, IndexNone, TermNone}
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, VotedForNone
		rf.persist()
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	if rf.state == StateCandidate {
		rf.becomeFollower()
	}
	rf.leader = args.LeaderId
	rf.resetElectionTimer(rf.randomizedElectionTimeout())

	if args.PrevLogIndex > rf.lastLogIndex() || args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		retryMatchIndex, retryMatchTerm := IndexNone, TermNone
		// The `args.PrevLogIndex` is guaranteed to be a mismatch. 
		// Therefore, we initially try `args.PrevLogIndex - 1` (or `lastLogIndex` if `args.PrevLogIndex` is too large).		
		if args.PrevLogIndex > rf.lastLogIndex() {
			retryMatchIndex, retryMatchTerm = rf.lastLogIndex(), rf.lastLogTerm()
		} else {
			retryMatchIndex, retryMatchTerm = args.PrevLogIndex - 1, rf.log[args.PrevLogIndex - 1].Term
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
				if rf.log[i].Term <= args.PrevLogTerm {
					retryMatchIndex, retryMatchTerm = i, rf.log[i].Term
					break
				}
			}
		}
		return &AppendEntriesReply{rf.currentTerm, false, retryMatchIndex, retryMatchTerm}
	}

	for i:=0; i < len(args.Entries); i++ {
		if rf.lastLogIndex() < args.PrevLogIndex + 1 + i {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
		if rf.log[args.PrevLogIndex + 1 + i].Term != args.Entries[i].Term {
			rf.log = append(rf.log[:args.PrevLogIndex + 1 + i], args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
		if rf.commitIndex > rf.lastApplied {
			entries := rf.copyLogEntries(rf.log[rf.lastApplied + 1:rf.commitIndex + 1])
			rf.commitCh <- entries
			rf.lastApplied = rf.commitIndex
		}
	}
	return &AppendEntriesReply{rf.currentTerm, true, IndexNone, TermNone}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntriesRPC", args, reply)
	if ok {
		rf.msgs.senderGroup.add()
		defer rf.msgs.senderGroup.done()
		if rf.killed() {
			return
		}
		rf.msgs.appResp.inC <- &AppendEntriesResult{server, args, reply}
	}
}

func (rf *Raft) sendAppendEntriesAsync(server int) {
	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.PrevLogIndex = rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
	args.Entries = rf.copyLogEntries(rf.log[rf.nextIndex[server]:])
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

	if reply.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = reply.Term, VotedForNone
		rf.persist()
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	if reply.Term < rf.currentTerm || args.Term != rf.currentTerm || rf.state != StateLeader {
		return
	}
	if reply.Success {
		if rf.matchIndex[server] >= args.PrevLogIndex + len(args.Entries) {
			return
		}
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		for i := rf.matchIndex[server]; i > rf.commitIndex && i > args.PrevLogIndex; i-- {
			if rf.log[i].Term == rf.currentTerm && rf.countLogReplicas(i) > len(rf.peers)/2 {
				rf.commitIndex = i
				break
			}
		}
		if rf.commitIndex > rf.lastApplied {
			entries := rf.copyLogEntries(rf.log[rf.lastApplied + 1:rf.commitIndex + 1])
			rf.commitCh <- entries
			rf.lastApplied = rf.commitIndex
		}
	} else {
		if rf.nextIndex[server] > reply.RetryMatchIndex {
			retryMatchIndex, retryMatchTerm := reply.RetryMatchIndex, reply.RetryMatchTerm
			// If the log at `retryMatchIndex` the follower returned turns out to be a mismatch,
			// If my term of the log at `retryMatchIndex` is greater than the follower's term,
			// then we follow the same procedure as in `AppendEntries` to find a possible match index.
			// Otherwise, we simply decrement `retryMatchIndex`.
			if rf.log[retryMatchIndex].Term > retryMatchTerm {
				for i:=retryMatchIndex - 1; i > IndexStart; i-- {
					if rf.log[i].Term <= retryMatchTerm {
						retryMatchIndex = i
						break
					}
				}
			} else if rf.log[retryMatchIndex].Term < retryMatchTerm {
				retryMatchIndex--
			}
			rf.nextIndex[server] = retryMatchIndex + 1
			rf.sendAppendEntriesAsync(server)
		}
	}

}

func (rf *Raft) copyLogEntries(src []LogEntry) []LogEntry {
	dst := make([]LogEntry, len(src))
	copy(dst, src)
	return dst
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
	rf.msgs.senderGroup.add()
	defer rf.msgs.senderGroup.done()
	if rf.killed() {
		return IndexNone, TermNone, false
	}
	rf.msgs.startCmdReq.inC <- command
	res := <- rf.msgs.startCmdReq.outC
	return res.index, res.term, res.isLeader
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
	rf.msgs.senderGroup.wait()
	close(rf.msgs.shutdown)
}

func (rf *Raft) runnerCleanup() {
	close(rf.commitCh)
}

func (rf *Raft) applierCleanup() {
	close(rf.applyCh)
}

func (rf *Raft) lastLogIndex() int {
	// raft's log index starts from 1
	// but the index of log slice starts from 0
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.log[rf.lastLogIndex()].Term
}

func (rf *Raft) countLogReplicas(index int) int {
	count := 0
	for i := 0; i < len(rf.peers); i++ {
		if rf.matchIndex[i] >= index {
			count++
		}
	}
	// matchIndex only counts replicas for log entries that are on the follower's log, so we need to count the leader as well
	if index <= rf.lastLogIndex() { 
		count++
	}
	return count
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
		case args := <-rf.msgs.appReq.inC:

			LogPrint(dLog2, "S%d Receiving AppendEntries From: S%d Term: %d PrevLogIndex: %d PrevLogTerm: %d Entries: %v LeaderCommit: %d RequestTerm: %d", rf.me, args.LeaderId, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit, args.Term)
			rf.msgs.appReq.outC <- rf.AppendEntries(args)

		case reply := <-rf.msgs.appResp.inC:

			LogPrint(dLog, "S%d AppendEntries Return From S%d Term: %d Success: %t PrevLogIndex: %d PrevLogTerm: %d Entries: %v LeaderCommit: %d RequestTerm: %d ResponseTerm: %d", rf.me, reply.server, rf.currentTerm, reply.reply.Success, reply.args.PrevLogIndex, reply.args.PrevLogTerm, reply.args.Entries, reply.args.LeaderCommit, reply.args.Term, reply.reply.Term)
			rf.appendEntriesReturn(reply.server, reply.args, reply.reply)

		case args := <-rf.msgs.voteReq.inC:

			LogPrint(dVote, "S%d Receiving RequestVote From: S%d Term: %d RequestTerm: %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
			rf.msgs.voteReq.outC <- rf.RequestVote(args)

		case reply := <-rf.msgs.voteResp.inC:

			LogPrint(dVote, "S%d RequestVote Return From S%d Term: %d VoteGranted: %t RequestTerm: %d ResponseTerm: %d", rf.me, reply.server, rf.currentTerm, reply.reply.VoteGranted, reply.args.Term, reply.reply.Term)
			rf.requestVoteReturn(reply.server, reply.args, reply.reply)

		case <-rf.msgs.getStateReq.inC:

			res := &GetStateResult{rf.currentTerm, rf.state == StateLeader}
			rf.msgs.getStateReq.outC <- res
		
		case command := <-rf.msgs.startCmdReq.inC:

			var res *StartCommandResult
			if rf.state == StateLeader {
				rf.log = append(rf.log, LogEntry{rf.currentTerm, command})
				LogPrint(dLeader, "S%d Starting Command: %v Term: %d Index: %d", rf.me, command, rf.currentTerm, rf.lastLogIndex())
				rf.persist()
				rf.broadcastAppendEntriesAsync()
				rf.resetHeartbeatTimer(rf.stableHeartbeatTimeout())
				res = &StartCommandResult{rf.lastLogIndex(), rf.currentTerm, true}
			} else {
				res = &StartCommandResult{IndexNone, rf.currentTerm, false}
			}
			rf.msgs.startCmdReq.outC <- res
		
		case <-rf.msgs.electionT.inC:
			
			if rf.state != StateLeader {
				LogPrint(dTimer, "S%d Election Timeout Term: %d", rf.me, rf.currentTerm)
				rf.becomeCandidate()
			}
		
		case <-rf.msgs.heartbeatT.inC:

			if rf.state == StateLeader {
				LogPrint(dTimer, "S%d Heartbeat Timeout Term: %d", rf.me, rf.currentTerm)
				rf.broadcastAppendEntriesAsync()
				rf.resetHeartbeatTimer(rf.stableHeartbeatTimeout())
			}
		
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
	for entries := range rf.commitCh {
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

func (rf *Raft) becomeFollower() {
	rf.resetElectionTimer(rf.randomizedElectionTimeout())
	rf.state = StateFollower
	rf.leader = LeaderNone
}

func (rf *Raft) becomeCandidate() {
	rf.state = StateCandidate
	rf.leader = LeaderNone
	rf.startElection()
}

func (rf *Raft) becomeLeader() {
	rf.resetHeartbeatTimer(rf.immediateTimeout())
	rf.state = StateLeader
	rf.leader = rf.me
	rf.nextIndex = make([]int, len(rf.peers))
	for i:=0; i < len(rf.peers); i++ { 
		rf.nextIndex[i] = rf.lastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer(rf.randomizedElectionTimeout())

	rf.voteCount = 1
	rf.broadcastRequestVoteAsync(&RequestVoteArgs{rf.currentTerm, rf.me, rf.lastLogIndex(), rf.lastLogTerm()})
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

	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = IndexStart
	rf.lastApplied = IndexStart

	rf.leader = LeaderNone
	rf.msgs	= MakeRaftMessages()
	rf.electionTimer = time.NewTimer(rf.randomizedElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(rf.stableHeartbeatTimeout())
	rf.msgs.electionT.inC = rf.electionTimer.C
	rf.msgs.heartbeatT.inC = rf.heartbeatTimer.C
	rf.applyCh = applyCh
	rf.commitCh = make(chan []LogEntry, 1000)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()
	rf.becomeFollower()
	go rf.runner()
	go rf.applier()

	return rf
}
