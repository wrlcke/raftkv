package raft

import "fmt"

type MessageType int

const (
	MsgVoteReq MessageType = iota
	MsgVoteResp
	MsgAppReq
	MsgAppResp
	MsgSnapReq
	MsgSnapResp
	MsgPreVoteReq
	MsgPreVoteResp
	MsgStartCmd
	MsgStartSnap
	MsgGetState
	MsgElectionT
	MsgHeartbeatT
	MsgApply
)

type Message struct {
	Type        MessageType
	From        int
	To          int
	Term        int
	LogIndex    int
	LogTerm     int
	Entries     []LogEntry
	CommitIndex int
	Success     bool
	Snapshot    Snapshot
}

type Status struct {
	Index    int
	Term     int
	IsLeader bool
}

type StartMessage struct {
	Message
	status chan Status
}

type ApplyMessage struct {
	Message
	applyType ApplyType
}

// Log entry structure
type LogEntry struct {
	Term    int
	Command interface{}
}

type Snapshot []byte

type ApplyType int

const (
	ApplyCommand ApplyType = iota
	ApplySnapshot
)

func (msg *Message) String() string {
	switch msg.Type {
	case MsgVoteReq, MsgPreVoteReq:
		return fmt.Sprintf("<term: %d, latest: %d(term %d)>", msg.Term, msg.LogIndex, msg.LogTerm)
	case MsgVoteResp, MsgPreVoteResp:
		return fmt.Sprintf("<voted: %t, term: %d>", msg.Success, msg.Term)
	case MsgAppReq:
		switch len(msg.Entries) {
		case 0:
			return fmt.Sprintf("<term: %d, prev: %d(term %d), entries:[], commit: %d>", msg.Term, msg.LogIndex, msg.LogTerm, msg.CommitIndex)
		case 1:
			return fmt.Sprintf("<term: %d, prev: %d(term %d), entries: [%d], commit: %d>", msg.Term, msg.LogIndex, msg.LogTerm, msg.LogIndex+1, msg.CommitIndex)
		default:
			return fmt.Sprintf("<term: %d, prev: %d(term %d), entries: [%d-%d], commit: %d>", msg.Term, msg.LogIndex, msg.LogTerm, msg.LogIndex+1, msg.LogIndex+len(msg.Entries), msg.CommitIndex)
		}
	case MsgAppResp:
		switch msg.Success {
		case true:
			return fmt.Sprintf("<success: %t, term: %d, lastappend: %d>", msg.Success, msg.Term, msg.LogIndex)
		default:
			return fmt.Sprintf("<success: %t, term: %d, retryat: %d(term %d)>", msg.Success, msg.Term, msg.LogIndex, msg.LogTerm)
		}
	case MsgSnapReq:
		return fmt.Sprintf("<term: %d, lastinclude: %d(term: %d)>", msg.Term, msg.LogIndex, msg.LogTerm)
	case MsgSnapResp:
		return fmt.Sprintf("<term: %d, lastinclude: %d(term: %d)>", msg.Term, msg.LogIndex, msg.LogTerm)
	case MsgStartSnap:
		return fmt.Sprintf("<lastinclude: %d>", msg.LogIndex)
	case MsgApply:
		switch len(msg.Entries) {
		case 1:
			return fmt.Sprintf("<entries: [%d]>", msg.LogIndex+1)
		case 0:
			return fmt.Sprintf("<lastinclude: %d(term: %d)>", msg.LogIndex, msg.LogTerm)
		default:
			return fmt.Sprintf("<entries: [%d-%d]>", msg.LogIndex+1, msg.LogIndex+len(msg.Entries))
		}
	default:
		return ""
	}
}

func (mt MessageType) String() string {
	return MessageTypeString[mt]
}

func (msg *Message) LogTopic() logTopic {
	return MessageLogTopic[msg.Type]
}

func (msg *Message) fillEntriesTerm(term int) {
	for i := range msg.Entries {
		msg.Entries[i].Term = term
	}
}

var MessageTypeString = map[MessageType]string{
	MsgVoteReq:     "MsgVoteReq",
	MsgVoteResp:    "MsgVoteResp",
	MsgAppReq:      "MsgAppReq",
	MsgAppResp:     "MsgAppResp",
	MsgSnapReq:     "MsgSnapReq",
	MsgSnapResp:    "MsgSnapResp",
	MsgPreVoteReq:  "MsgPreVoteReq",
	MsgPreVoteResp: "MsgPreVoteResp",
}

var MessageLogTopic = map[MessageType]logTopic{
	MsgVoteReq:     dVote,
	MsgVoteResp:    dVote,
	MsgAppReq:      dLog,
	MsgAppResp:     dLog2,
	MsgSnapReq:     dSnap,
	MsgSnapResp:    dSnap,
	MsgPreVoteReq:  dVote,
	MsgPreVoteResp: dVote,
}

// // example RequestVote RPC arguments structure.
// // field names must start with capital letters!
// type RequestVoteArgs struct {
// 	// Your data here (2A, 2B).
// 	Term int                      // candidate's term
// 	CandidateId int               // candidate requesting vote
// 	LastLogIndex int              // index of candidate's last log entry
// 	LastLogTerm int               // term of candidate's last log entry
// }

// // example RequestVote RPC reply structure.
// // field names must start with capital letters!
// type RequestVoteReply struct {
// 	// Your data here (2A).
// 	Term int                      // currentTerm, for candidate to update itself
// 	VoteGranted bool              // true means candidate received vote
// }

// type AppendEntriesArgs struct {
// 	Term         int        // leader's term
// 	LeaderId     int        // so follower can redirect clients
// 	PrevLogIndex int        // index of log entry immediately preceding new ones
// 	PrevLogTerm  int        // term of prevLogIndex entry
// 	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
// 	LeaderCommit int        // leader's commitIndex
// }

// type AppendEntriesReply struct {
// 	Term            int  // currentTerm, for leader to update itself
// 	Success         bool // true if follower contained entry matching prevLogIndex and prevLogTerm
// 	RetryMatchIndex int  // possible match index for leader to retry
// 	RetryMatchTerm  int  // possible log term for leader to retry
// }

// type InstallSnapshotArgs struct {
// 	Term              int    // leader's term
// 	LeaderId          int    // so follower can redirect clients
// 	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
// 	LastIncludedTerm  int    // term of lastIncludedIndex
// 	Data              []byte // raw bytes of the snapshot
// }

// type InstallSnapshotReply struct {
// 	Term int // currentTerm, for leader to update itself
// }
