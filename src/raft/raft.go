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
	// "sync"
	// "sync/atomic"
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
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState int

const (
	StateFollower RaftState = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	NumStates
)

const (
	TermNone     = -1
	TermStart    = 0
	VotedForNone = -1
	LeaderNone   = -1
	IndexNone    = -1
	IndexStart   = 0
)

// A Go object implementing a single Raft peer.
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	// persister *Persister          // Object to hold this peer's persisted state
	me int // this peer's index into peers[]
	// dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	// currentTerm int               // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	// votedFor int                  // candidateId that received vote in current term (or null if none)
	// log []LogEntry                // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	storage   Storage
	transport Transport

	// volatile state on all servers
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders
	nextIndex   []int  // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int  // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	inflight    []int  // for each server, number of inflight append entries RPCs
	retrying    []bool // for each server, whether the leader is retrying append entries RPCs
	maxInflight int    // maximum number of inflight append entries RPCs

	// volatile state for internal messages
	state          RaftState         // current state of the server
	leader         int               // id of current leader (initialized to -1)
	electionTimer  ElapsedTimer      // timer for election
	heartbeatTimer ElapsedTimer      // timer for heartbeat
	startCh        chan StartMessage // receiving requests from service
	applyCh        chan ApplyMessage // sending apply messages to service
	recvCh         chan Message      // receiving messages from transport
	writeCh        chan Message      // sending messages to asynchronous storage writer
	shutdown       chan struct{}     // shutdown the server, Set by Kill()

	// volatile state on candidates
	votes map[int]bool // votes received from peers
}

type ElapsedTimer struct {
	*time.Timer
	startTime time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// // Your code here (2A).
	// return term, isleader
	status := rf.startRequest(Message{Type: MsgGetState})
	return status.Term, status.IsLeader
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
	// return index, term, isLeader
	status := rf.startRequest(Message{Type: MsgStartCmd, Entries: []LogEntry{{Command: command}}})
	return status.Index, status.Term, status.IsLeader
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.startRequest(Message{Type: MsgStartSnap, LogIndex: index, Snapshot: Snapshot(snapshot)})
}

func (rf *Raft) startRequest(msg Message) Status {
	statusCh := make(chan Status)
	status := Status{Term: TermNone, Index: IndexNone, IsLeader: false}
	select {
	case rf.startCh <- StartMessage{msg, statusCh}:
	case <-rf.shutdown:
		return status
	}
	select {
	case status = <-statusCh:
	case <-rf.shutdown:
	}
	return status
}

func (rf *Raft) handleRequestVote(args Message) {
	// Reply false if term < currentTerm
	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote

	LogPrint(args.LogTopic(), "s%d received %v from s%d at term %d %v", rf.me, args.Type, args.From, rf.storage.CurrentTerm(), &args)
	if args.Term < rf.storage.CurrentTerm() {
		rf.send(args.From, Message{Type: MsgVoteResp, Success: false})
		return
	}
	if args.Term > rf.storage.CurrentTerm() {
		rf.storage.SetCurrentTerm(args.Term)
		rf.storage.SetVotedFor(VotedForNone)
		rf.saveTerm()
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	if rf.storage.VotedFor() != VotedForNone {
		if rf.storage.VotedFor() != args.From {
			rf.send(args.From, Message{Type: MsgVoteResp, Success: false})
			return
		} else {
			rf.electionTimer.Reset(rf.randomizedElectionTimeout())
			rf.send(args.From, Message{Type: MsgVoteResp, Success: true})
			return
		}
	}
	if args.LogTerm < rf.storage.LogTerm(rf.storage.LastLogIndex()) ||
		args.LogTerm == rf.storage.LogTerm(rf.storage.LastLogIndex()) && args.LogIndex < rf.storage.LastLogIndex() {
		rf.send(args.From, Message{Type: MsgVoteResp, Success: false})
		return
	}
	rf.storage.SetVotedFor(args.From)
	rf.saveVote()
	rf.electionTimer.Reset(rf.randomizedElectionTimeout())
	rf.send(args.From, Message{Type: MsgVoteResp, Success: true})
}

func (rf *Raft) requestVoteReturn(reply Message) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	// If votes received from majority of servers: become leader

	LogPrint(reply.LogTopic(), "s%d received %v from s%d at term %d %v", rf.me, reply.Type, reply.From, rf.storage.CurrentTerm(), &reply)
	if reply.Term > rf.storage.CurrentTerm() {
		rf.storage.SetCurrentTerm(reply.Term)
		rf.storage.SetVotedFor(VotedForNone)
		rf.saveTerm()
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
		return
	}
	if reply.Success && rf.storage.CurrentTerm() == reply.Term && rf.state == StateCandidate {
		rf.votes[reply.From] = true
		if len(rf.votes) > len(rf.peers)/2 {
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) handlePreVote(args Message) {
	// Reply false if term < currentTerm
	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote

	LogPrint(args.LogTopic(), "s%d received %v from s%d at term %d %v", rf.me, args.Type, args.From, rf.storage.CurrentTerm(), &args)
	if args.Term < rf.storage.CurrentTerm() {
		rf.send(args.From, Message{Type: MsgPreVoteResp, Success: false})
		return
	}
	if rf.leader != LeaderNone && rf.electionTimer.Elapsed() < rf.baselineElectionTimeout() ||
		args.LogTerm < rf.storage.LogTerm(rf.storage.LastLogIndex()) ||
		args.LogTerm == rf.storage.LogTerm(rf.storage.LastLogIndex()) &&
			args.LogIndex < rf.storage.LastLogIndex() {
		rf.transport.Send(Message{Type: MsgPreVoteResp, From: rf.me, To: args.From, Term: args.Term, Success: false})
		return
	}
	rf.electionTimer.Reset(rf.randomizedElectionTimeout())
	rf.transport.Send(Message{Type: MsgPreVoteResp, From: rf.me, To: args.From, Term: args.Term, Success: true})
}

func (rf *Raft) preVoteReturn(reply Message) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	// If votes received from majority of servers: become leader

	LogPrint(reply.LogTopic(), "s%d received %v from s%d at term %d %v", rf.me, reply.Type, reply.From, rf.storage.CurrentTerm(), &reply)
	if reply.Term > rf.storage.CurrentTerm() {
		rf.storage.SetCurrentTerm(reply.Term)
		rf.storage.SetVotedFor(VotedForNone)
		rf.saveTerm()
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
		return
	}
	if reply.Success && reply.Term == rf.storage.CurrentTerm() && rf.state == StatePreCandidate {
		rf.votes[reply.From] = true
		if len(rf.votes) > len(rf.peers)/2 {
			rf.becomeCandidate()
		}
	}
}

func (rf *Raft) handleAppendEntries(args Message) {
	// Reply false if term < currentTerm
	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	// Append any new entries not already in the log
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	LogPrint(args.LogTopic(), "s%d received %v from s%d at term %d %v", rf.me, args.Type, args.From, rf.storage.CurrentTerm(), &args)
	if args.Term < rf.storage.CurrentTerm() {
		rf.send(args.From, Message{Type: MsgAppResp, Success: false, LogIndex: IndexNone, LogTerm: TermNone})
		return
	}
	if args.Term > rf.storage.CurrentTerm() {
		rf.storage.SetCurrentTerm(args.Term)
		rf.storage.SetVotedFor(VotedForNone)
		rf.saveTerm()
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	if rf.state == StateCandidate {
		rf.becomeFollower()
	}
	rf.leader = args.From
	rf.electionTimer.Reset(rf.randomizedElectionTimeout())

	if args.LogIndex > rf.storage.LastLogIndex() ||
		args.LogIndex >= rf.storage.FirstLogIndex() &&
			args.LogTerm != rf.storage.LogTerm(args.LogIndex) {
		retryMatchIndex, retryMatchTerm := rf.searchPotentialMatch(args.LogIndex, args.LogTerm)
		rf.send(args.From, Message{Type: MsgAppResp, Success: false, LogIndex: retryMatchIndex, LogTerm: retryMatchTerm})
		return
	}
	matchIndex := args.LogIndex + len(args.Entries)
	appended := []LogEntry(nil)
	for i := 0; i < len(args.Entries); i++ {
		if args.LogIndex+1+i > rf.storage.LastLogIndex() ||
			args.LogIndex+1+i >= rf.storage.FirstLogIndex() &&
				args.Entries[i].Term != rf.storage.LogTerm(args.LogIndex+1+i) {
			matchIndex = args.LogIndex + i
			appended = args.Entries[i:]
			rf.storage.RemoveLogSuffix(args.LogIndex + 1 + i)
			rf.storage.AppendLog(args.Entries[i:]...)
			break
		}
	}
	// Committed entries will never be overwritten, so even we do storage asynchronously, we can update commitIndex here
	rf.commitIndex = Max(rf.commitIndex, Min(args.CommitIndex, rf.storage.LastLogIndex()))
	if rf.commitIndex > rf.lastApplied {
		rf.apply(ApplyCommand, Message{Type: MsgApply, LogIndex: rf.lastApplied,
			Entries: rf.storage.LogEntries(rf.lastApplied+1, rf.commitIndex+1)})
		rf.lastApplied = rf.commitIndex
	}
	// But if we do storage asynchronously, we have to pass the entries to the storage writer
	// no matter whether appended is nil
	// because we are using memory storage to find matched index,
	// and the storage writer may not have persisted the matched entries
	rf.save(args.From, Message{Type: MsgAppReq, LogIndex: matchIndex, Entries: appended})
}

func (rf *Raft) appendEntriesReturn(reply Message) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	// If successful: update nextIndex and matchIndex for follower
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N
	// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry

	LogPrint(reply.LogTopic(), "s%d received %v from s%d at term %d %v", rf.me, reply.Type, reply.From, rf.storage.CurrentTerm(), &reply)
	if reply.Term > rf.storage.CurrentTerm() {
		rf.storage.SetCurrentTerm(reply.Term)
		rf.storage.SetVotedFor(VotedForNone)
		rf.saveTerm()
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	if reply.Term < rf.storage.CurrentTerm() || reply.LogIndex == IndexNone || rf.state != StateLeader {
		return
	}
	if reply.From != rf.me {
		rf.inflight[reply.From]--
	}
	if reply.Success {
		if rf.matchIndex[reply.From] >= reply.LogIndex {
			return
		}
		rf.nextIndex[reply.From] = reply.LogIndex + 1
		rf.matchIndex[reply.From] = reply.LogIndex
		rf.retrying[reply.From] = false
		for i := reply.LogIndex; i > rf.commitIndex; i-- {
			replicas := rf.countLogReplicas(i)
			if rf.storage.LogTerm(i) == rf.storage.CurrentTerm() && replicas > len(rf.peers)/2 || replicas == len(rf.peers) {
				rf.commitIndex = i
				break
			}
		}
		if rf.commitIndex > rf.lastApplied {
			rf.apply(ApplyCommand, Message{Type: MsgApply, LogIndex: rf.lastApplied,
				Entries: rf.storage.LogEntries(rf.lastApplied+1, rf.commitIndex+1)})
			rf.lastApplied = rf.commitIndex
		}
	} else {
		if rf.matchIndex[reply.From] <= reply.LogIndex && rf.nextIndex[reply.From] > reply.LogIndex {
			retryMatchIndex, retryMatchTerm := reply.LogIndex, reply.LogTerm
			if retryMatchIndex >= rf.storage.FirstLogIndex() &&
				retryMatchIndex <= rf.storage.LastLogIndex() && // this should always be true
				retryMatchTerm != rf.storage.LogTerm(retryMatchIndex) {
				retryMatchIndex, _ = rf.searchPotentialMatch(retryMatchIndex, retryMatchTerm)
			}
			rf.nextIndex[reply.From] = retryMatchIndex + 1
			rf.retrying[reply.From] = true
			if rf.inflight[reply.From] < rf.maxInflight {
				rf.sendReplication(reply.From)
			}
		}
	}
}

func (rf *Raft) handleInstallSnapshot(args Message) {
	// Reply false if term < currentTerm
	// Create new snapshot file
	// Write data into snapshot file
	// Save snapshot file, discard any existing or partial snapshot with a smaller index
	// If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply
	// Discard the entire log
	// Reset state machine using snapshot contents (and load snapshot's cluster configuration)

	LogPrint(args.LogTopic(), "s%d received %v from s%d at term %d %v", rf.me, args.Type, args.From, rf.storage.CurrentTerm(), &args)
	if args.Term < rf.storage.CurrentTerm() {
		rf.send(args.From, Message{Type: MsgSnapResp, LogIndex: IndexNone})
		return
	}
	if args.Term > rf.storage.CurrentTerm() {
		rf.storage.SetCurrentTerm(args.Term)
		rf.storage.SetVotedFor(VotedForNone)
		rf.saveTerm()
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	rf.leader = args.From
	rf.electionTimer.Reset(rf.randomizedElectionTimeout())
	if args.LogIndex < rf.storage.FirstLogIndex() {
		rf.save(args.From, Message{Type: MsgSnapReq, LogIndex: args.LogIndex, LogTerm: args.LogTerm, Snapshot: nil})
		return
	}
	if args.LogIndex <= rf.storage.LastLogIndex() && args.LogTerm == rf.storage.LogTerm(args.LogIndex) {
		rf.storage.RemoveLogPrefix(args.LogIndex)
	} else {
		rf.storage.ResetLog(args.LogIndex, args.LogTerm)
	}
	rf.storage.SetSnapshot(args.Snapshot)

	rf.commitIndex = Max(rf.commitIndex, args.LogIndex)
	if rf.commitIndex > rf.lastApplied {
		rf.apply(ApplySnapshot, Message{Type: MsgApply, LogIndex: args.LogIndex, LogTerm: args.LogTerm, Snapshot: args.Snapshot})
		rf.lastApplied = args.LogIndex
	}
	rf.save(args.From, Message{Type: MsgSnapReq, LogIndex: args.LogIndex, LogTerm: args.LogTerm, Snapshot: args.Snapshot})
}

func (rf *Raft) installSnapshotReturn(reply Message) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower

	LogPrint(reply.LogTopic(), "s%d received %v from s%d at term %d %v", rf.me, reply.Type, reply.From, rf.storage.CurrentTerm(), &reply)
	if reply.Term > rf.storage.CurrentTerm() {
		rf.storage.SetCurrentTerm(reply.Term)
		rf.storage.SetVotedFor(VotedForNone)
		rf.saveTerm()
		if rf.state != StateFollower {
			rf.becomeFollower()
		}
	}
	if reply.Term < rf.storage.CurrentTerm() || reply.LogIndex == IndexNone || rf.state != StateLeader {
		return
	}
	if rf.matchIndex[reply.From] >= reply.LogIndex {
		return
	}
	rf.nextIndex[reply.From] = reply.LogIndex + 1
	rf.matchIndex[reply.From] = reply.LogIndex
	rf.retrying[reply.From] = false
	for i := reply.LogIndex; i > rf.commitIndex; i-- {
		replicas := rf.countLogReplicas(i)
		if rf.storage.LogTerm(i) == rf.storage.CurrentTerm() && replicas > len(rf.peers)/2 || replicas == len(rf.peers) {
			rf.commitIndex = i
			break
		}
	}
	if rf.commitIndex > rf.lastApplied {
		rf.apply(ApplyCommand, Message{LogIndex: rf.lastApplied,
			Entries: rf.storage.LogEntries(rf.lastApplied+1, rf.commitIndex+1)})
		rf.lastApplied = rf.commitIndex
	}
}

func (rf *Raft) startCommand(msg Message, status chan Status) {
	if rf.state != StateLeader {
		rf.sendStatus(status)
		return
	}
	startIndex := rf.storage.LastLogIndex() + 1
	msg.fillEntriesTerm(rf.storage.CurrentTerm())
	rf.storage.AppendLog(msg.Entries...)
	rf.sendStatus(status)
batching_loop:
	for {
		select {
		case s := <-rf.startCh:
			msg, status = s.Message, s.status
		case <-rf.shutdown:
			return
		default:
			break batching_loop
		}
		switch msg.Type {
		case MsgStartCmd:
			msg.fillEntriesTerm(rf.storage.CurrentTerm())
			rf.storage.AppendLog(msg.Entries...)
			rf.sendStatus(status)
		case MsgStartSnap:
			rf.startSnapshot(msg, status)
		case MsgGetState:
			rf.sendStatus(status)
		default:
			panic("unexpected message type from startCh")
		}
	}
	endIndex := rf.storage.LastLogIndex()
	if startIndex == endIndex {
		LogPrint(dLeader, "s%d start command at term %d <entries: [%d]>", rf.me, rf.storage.CurrentTerm(), startIndex)
	} else {
		LogPrint(dLeader, "s%d start command at term %d <entries: [%d-%d]>", rf.me, rf.storage.CurrentTerm(), startIndex, endIndex)
	}
	rf.broadcastReplication(false)
	rf.save(rf.me, Message{
		Type:     MsgAppReq,
		LogIndex: startIndex - 1,
		Entries:  rf.storage.LogEntries(startIndex, endIndex+1),
	})
}

func (rf *Raft) startSnapshot(msg Message, status chan Status) {
	LogPrint(dSnap, "s%d start snapshot at term %d %v", rf.me, rf.storage.CurrentTerm(), &msg)
	if msg.LogIndex >= rf.storage.FirstLogIndex() {
		rf.storage.RemoveLogPrefix(msg.LogIndex)
		rf.storage.SetSnapshot(msg.Snapshot)
		rf.save(rf.me, Message{
			Type:     MsgSnapReq,
			LogIndex: msg.LogIndex,
			LogTerm:  rf.storage.LogTerm(msg.LogIndex),
			Snapshot: msg.Snapshot,
		})
	}
	rf.sendStatus(status)
}

func (rf *Raft) sendStatus(status chan Status) {
	select {
	case status <- Status{rf.storage.LastLogIndex(), rf.storage.CurrentTerm(), rf.state == StateLeader}:
	case <-rf.shutdown:
	}
}

func (rf *Raft) onElectionTimeout() {
	if rf.state != StateLeader {
		LogPrint(dTimer, "s%d election timeout", rf.me)
		rf.becomePreCandidate()
	}
}

func (rf *Raft) onHeartbeatTimeout() {
	if rf.state == StateLeader {
		LogPrint(dTimer, "s%d heartbeat timeout", rf.me)
		rf.broadcastReplication(true)
		rf.heartbeatTimer.Reset(rf.stableHeartbeatTimeout())
	}
}

func (rf *Raft) sendReplication(i int) {
	if rf.nextIndex[i] >= rf.storage.FirstLogIndex() {
		rf.inflight[i]++
		rf.send(i, Message{
			Type:        MsgAppReq,
			LogIndex:    rf.nextIndex[i] - 1,
			LogTerm:     rf.storage.LogTerm(rf.nextIndex[i] - 1),
			Entries:     rf.storage.LogEntries(rf.nextIndex[i], rf.storage.LastLogIndex()+1),
			CommitIndex: rf.commitIndex,
		})
	} else {
		rf.retrying[i] = true
		rf.send(i, Message{
			Type:     MsgSnapReq,
			LogIndex: rf.storage.FirstLogIndex() - 1,
			LogTerm:  rf.storage.LogTerm(rf.storage.FirstLogIndex() - 1),
			Snapshot: rf.storage.Snapshot(),
		})
	}
}

func (rf *Raft) broadcastReplication(heartbeat bool) {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			if (rf.inflight[i] >= rf.maxInflight || rf.retrying[i]) && !heartbeat {
				continue
			}
			rf.sendReplication(i)
		}
	}
}

func (rf *Raft) send(peerId int, msg Message) {
	msg.From, msg.To = rf.me, peerId
	msg.Term = rf.storage.CurrentTerm()
	rf.transport.Send(msg)
}

func (rf *Raft) save(leaderId int, msg Message) {
	msg.Term = rf.storage.CurrentTerm()
	msg.From, msg.To = leaderId, rf.me
	select {
	case rf.writeCh <- msg:
	case <-rf.shutdown:
	}
}

func (rf *Raft) apply(applyType ApplyType, msg Message) {
	msg.Term = rf.storage.CurrentTerm()
	select {
	case rf.applyCh <- ApplyMessage{Message: msg, applyType: applyType}:
	case <-rf.shutdown:
	}
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
	// atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.shutdown)
	rf.electionTimer.Stop()
	rf.heartbeatTimer.Stop()
	rf.transport.Stop()
	LogPrint(dInfo, "s%d shutdown", rf.me)
}

func (rf *Raft) saveVote() {
	rf.storage.Stable().SetVotedFor(rf.storage.VotedFor())
	rf.storage.Stable().Sync()
}

func (rf *Raft) saveTerm() {
	rf.storage.Stable().SetCurrentTerm(rf.storage.CurrentTerm())
	rf.storage.Stable().SetVotedFor(rf.storage.VotedFor())
	rf.storage.Stable().Sync()
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

func (rf *Raft) searchPotentialMatch(mismatchIndex int, mismatchTerm int) (int, int) {
	retryMatchIndex, retryMatchTerm := IndexNone, TermNone
	// The `mismatchIndex` is guaranteed to be a mismatch.
	// Therefore, we initially try `mismatchIndex - 1` (or `lastLogIndex` if `mismatchIndex` is too large).
	if mismatchIndex > rf.storage.LastLogIndex() {
		retryMatchIndex, retryMatchTerm = rf.storage.LastLogIndex(), rf.storage.LogTerm(rf.storage.LastLogIndex())
	} else {
		retryMatchIndex, retryMatchTerm = mismatchIndex-1, rf.storage.LogTerm(mismatchIndex-1)
	}
	// If the term of the index we're trying is greater than `mismatchTerm`,
	// then because the tried index is less than `mismatchIndex`,
	// the term of the peer's log at this index is definitely less than or equal to
	// `mismatchTerm` (because terms of the logs increase monotonically).
	// Hence, it must be less than (and so mismatch) the term of my log at this index.
	if retryMatchTerm > mismatchTerm {
		// We continue to search backwards until we find an index where the term is
		// less than or equal to `mismatchTerm`.
		// Only then, the term of the leader at this index could possibly be equal to
		// the term of my log at this index.
		for i := retryMatchIndex - 1; i >= rf.storage.FirstLogIndex(); i-- {
			if rf.storage.LogTerm(i) <= mismatchTerm {
				retryMatchIndex, retryMatchTerm = i, rf.storage.LogTerm(i)
				break
			}
		}
	}
	return retryMatchIndex, retryMatchTerm
}

func (rf *Raft) baselineElectionTimeout() time.Duration {
	return time.Duration(500) * time.Millisecond
}

func (rf *Raft) randomizedElectionTimeout() time.Duration {
	return time.Duration(500+(rand.Intn(300))) * time.Millisecond
}

func (rf *Raft) stableHeartbeatTimeout() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func (rf *Raft) immediateTimeout() time.Duration {
	return time.Duration(0) * time.Millisecond
}

func (timer *ElapsedTimer) Reset(d time.Duration) {
	timer.startTime = time.Now()
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Timer.Reset(d)
}

func (timer *ElapsedTimer) Elapsed() time.Duration {
	return time.Since(timer.startTime)
}

func (rf *Raft) runner() {
	var msg Message
	var status chan Status
	for {
		select {
		case s := <-rf.startCh:
			msg, status = s.Message, s.status
		case msg = <-rf.recvCh:
		case <-rf.electionTimer.C:
			msg.Type = MsgElectionT
		case <-rf.heartbeatTimer.C:
			msg.Type = MsgHeartbeatT
		case <-rf.shutdown:
			return
		}
		switch msg.Type {
		case MsgVoteReq:
			rf.handleRequestVote(msg)
		case MsgVoteResp:
			rf.requestVoteReturn(msg)
		case MsgAppReq:
			rf.handleAppendEntries(msg)
		case MsgAppResp:
			rf.appendEntriesReturn(msg)
		case MsgSnapReq:
			rf.handleInstallSnapshot(msg)
		case MsgSnapResp:
			rf.installSnapshotReturn(msg)
		case MsgPreVoteReq:
			rf.handlePreVote(msg)
		case MsgPreVoteResp:
			rf.preVoteReturn(msg)
		case MsgStartCmd:
			rf.startCommand(msg, status)
		case MsgStartSnap:
			rf.startSnapshot(msg, status)
		case MsgGetState:
			rf.sendStatus(status)
		case MsgElectionT:
			rf.onElectionTimeout()
		case MsgHeartbeatT:
			rf.onHeartbeatTimeout()
		default:
			panic("Unknown Message Type")
		}
	}
}

func (rf *Raft) persister() {
	var msgs []Message
	var dirty bool
	for {
		select {
		case <-rf.shutdown:
			return
		case msg := <-rf.writeCh:
			msgs = append(msgs, msg)
		}
	batching_loop:
		for {
			select {
			case <-rf.shutdown:
				return
			case msg := <-rf.writeCh:
				msgs = append(msgs, msg)
			default:
				break batching_loop
			}
		}
		dirty = false
		for i := range msgs {
			if msgs[i].Type == MsgAppReq {
				if msgs[i].Entries != nil {
					if msgs[i].LogIndex != rf.storage.Stable().LastLogIndex() {
						rf.storage.Stable().RemoveLogSuffix(msgs[i].LogIndex + 1)
					}
					rf.storage.Stable().AppendLog(msgs[i].Entries...)
					dirty = true
				}
				msgs[i] = Message{
					Type:     MsgAppResp,
					From:     rf.me,
					To:       msgs[i].From,
					Term:     msgs[i].Term,
					Success:  true,
					LogIndex: msgs[i].LogIndex + len(msgs[i].Entries),
				}
			} else if msgs[i].Type == MsgSnapReq {
				if msgs[i].Snapshot != nil {
					if msgs[i].LogIndex <= rf.storage.Stable().LastLogIndex() && msgs[i].LogTerm == rf.storage.Stable().LogTerm(msgs[i].LogIndex) {
						rf.storage.Stable().RemoveLogPrefix(msgs[i].LogIndex)
					} else {
						rf.storage.Stable().ResetLog(msgs[i].LogIndex, msgs[i].LogTerm)
					}
					rf.storage.Stable().SetSnapshot(msgs[i].Snapshot)
					dirty = true
				}
				msgs[i] = Message{
					Type:     MsgSnapResp,
					From:     rf.me,
					To:       msgs[i].From,
					Term:     msgs[i].Term,
					LogIndex: msgs[i].LogIndex,
					LogTerm:  msgs[i].LogTerm,
				}
			} else {
				panic("unexpected message type from writeCh")
			}
		}
		if dirty {
			rf.storage.Stable().Sync()
		}
		for i := range msgs {
			if msgs[i].To == rf.me {
				select {
				case rf.recvCh <- msgs[i]:
				case <-rf.shutdown:
					return
				}
			} else {
				rf.transport.Send(msgs[i])
			}
		}
		msgs = msgs[:0]
	}
}

func (rf *Raft) applier(applyCh chan ApplyMsg) {
	var msg ApplyMessage
	var msgs []ApplyMsg
	defer close(applyCh)
	for {
		select {
		case <-rf.shutdown:
			return
		case msg = <-rf.applyCh:
		}
		switch msg.applyType {
		case ApplyCommand:
			LogPrint(dCommit, "s%d apply command at term %d %v", rf.me, msg.Term, &msg)
			for i := range msg.Entries {
				msgs = append(msgs, ApplyMsg{
					CommandValid: true,
					Command:      msg.Entries[i].Command,
					CommandIndex: msg.LogIndex + i + 1,
					CommandTerm:  msg.Entries[i].Term,
				})
			}
		case ApplySnapshot:
			LogPrint(dCommit, "s%d apply snapshot at term %d %v", rf.me, msg.Term, &msg)
			msgs = append(msgs, ApplyMsg{
				SnapshotValid: true,
				Snapshot:      msg.Snapshot,
				SnapshotIndex: msg.LogIndex,
				SnapshotTerm:  msg.LogTerm,
			})
		default:
			panic("unexpected apply type")
		}
		for i := range msgs {
			select {
			case applyCh <- msgs[i]:
			case <-rf.shutdown:
				return
			}
		}
		msgs = msgs[:0]
	}
}

func (rf *Raft) becomeFollower() {
	LogPrint(dInfo, "s%d became follower at term %d", rf.me, rf.storage.CurrentTerm())
	rf.electionTimer.Reset(rf.randomizedElectionTimeout())
	rf.state = StateFollower
	rf.leader = LeaderNone
}

func (rf *Raft) becomePreCandidate() {
	LogPrint(dInfo, "s%d became precandidate at term %d", rf.me, rf.storage.CurrentTerm())
	rf.electionTimer.Reset(rf.randomizedElectionTimeout())
	rf.state = StatePreCandidate
	rf.leader = LeaderNone
	rf.startPreElection()
}

func (rf *Raft) becomeCandidate() {
	LogPrint(dInfo, "s%d became candidate at term %d", rf.me, rf.storage.CurrentTerm()+1)
	rf.electionTimer.Reset(rf.randomizedElectionTimeout())
	rf.state = StateCandidate
	rf.leader = LeaderNone
	rf.startElection()
}

func (rf *Raft) becomeLeader() {
	LogPrint(dInfo, "s%d became leader at term %d", rf.me, rf.storage.CurrentTerm())
	rf.heartbeatTimer.Reset(rf.immediateTimeout())
	rf.state = StateLeader
	rf.leader = rf.me
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.storage.LastLogIndex() + 1
		if i == rf.me {
			rf.matchIndex[i] = rf.storage.LastLogIndex()
		} else {
			rf.matchIndex[i] = IndexStart
		}
		rf.inflight[i] = 0
		rf.retrying[i] = false
	}
}

func (rf *Raft) startPreElection() {
	rf.votes = map[int]bool{rf.me: true}
	for i := range rf.peers {
		if i != rf.me {
			rf.send(i, Message{
				Type:     MsgPreVoteReq,
				LogIndex: rf.storage.LastLogIndex(),
				LogTerm:  rf.storage.LogTerm(rf.storage.LastLogIndex()),
			})
		}
	}
}

func (rf *Raft) startElection() {
	rf.storage.SetCurrentTerm(rf.storage.CurrentTerm() + 1)
	rf.storage.SetVotedFor(rf.me)
	rf.saveTerm()
	rf.votes = map[int]bool{rf.me: true}
	for i := range rf.peers {
		if i != rf.me {
			rf.send(i, Message{
				Type:     MsgVoteReq,
				LogIndex: rf.storage.LastLogIndex(),
				LogTerm:  rf.storage.LogTerm(rf.storage.LastLogIndex()),
			})
		}
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
	// rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.inflight = make([]int, len(rf.peers))
	rf.retrying = make([]bool, len(rf.peers))
	rf.votes = make(map[int]bool)
	rf.maxInflight = 100

	rf.leader = LeaderNone
	rf.electionTimer = ElapsedTimer{time.NewTimer(0), time.Now()}
	rf.heartbeatTimer = ElapsedTimer{time.NewTimer(0), time.Now()}
	rf.startCh = make(chan StartMessage)
	rf.applyCh = make(chan ApplyMessage, 10000)
	rf.recvCh = make(chan Message, 10000)
	rf.writeCh = make(chan Message, 10000)
	rf.shutdown = make(chan struct{})
	// initialize from state persisted before a crash
	rf.storage = NewLabPersisterStorage(persister)
	rf.transport = NewLabRpcTransport(peers, me, rf.recvCh)
	rf.commitIndex = rf.storage.FirstLogIndex() - 1
	rf.lastApplied = rf.storage.FirstLogIndex() - 1

	// start ticker goroutine to start elections
	// go rf.ticker()
	rf.becomeFollower()
	go rf.runner()
	go rf.applier(applyCh)
	go rf.persister()
	return rf
}
