package raft

import (
	"sync"
	"time"
)


type GetStateResult struct {
	term int
	isLeader bool
}

type StartCommandResult struct {
	index int
	term int
	isLeader bool
}

// RequestVoteResult is used to pass the args and reply of RequestVote RPC from async caller to runner
type RequestVoteResult struct {
	server int
	args *RequestVoteArgs
	reply *RequestVoteReply
}

// AppendEntriesResult is used to pass the args and reply of AppendEntries RPC from async caller to runner
type AppendEntriesResult struct {
	server int
	args *AppendEntriesArgs
	reply *AppendEntriesReply
}

type RaftInternalMessageAppendEntriesRequest struct {
	inC chan *AppendEntriesArgs
	outC chan *AppendEntriesReply
}

type RaftInternalMessageAppendEntriesResponse struct {
	inC chan *AppendEntriesResult
}

type RaftInternalMessageRequestVoteRequest struct {
	inC chan *RequestVoteArgs
	outC chan *RequestVoteReply
}

type RaftInternalMessageRequestVoteResponse struct {
	inC chan *RequestVoteResult
}

type RaftInternalMessageGetStateRequest struct {
	inC chan struct{}
	outC chan *GetStateResult
}

type RaftInternalMessageStartCommandRequest struct {
	inC chan interface{}
	outC chan *StartCommandResult
}

type RaftInternalMessageElectionTimer struct {
	inC <-chan time.Time
}

type RaftInternalMessageHeartbeatTimer struct {
	inC <-chan time.Time
}

type RaftMessages struct {
	appReq RaftInternalMessageAppendEntriesRequest
	appResp RaftInternalMessageAppendEntriesResponse
	voteReq RaftInternalMessageRequestVoteRequest
	voteResp RaftInternalMessageRequestVoteResponse
	getStateReq RaftInternalMessageGetStateRequest
	startCmdReq RaftInternalMessageStartCommandRequest
	electionT RaftInternalMessageElectionTimer
	heartbeatT RaftInternalMessageHeartbeatTimer
	senderGroup RaftMessageSenderWaitGroup
	shutdown chan struct{}
}

func MakeRaftMessages() RaftMessages {
	appReq := RaftInternalMessageAppendEntriesRequest{ make(chan *AppendEntriesArgs), make(chan *AppendEntriesReply) }
	appResp := RaftInternalMessageAppendEntriesResponse{ make(chan *AppendEntriesResult) }
	voteReq := RaftInternalMessageRequestVoteRequest{ make(chan *RequestVoteArgs), make(chan *RequestVoteReply) }
	voteResp := RaftInternalMessageRequestVoteResponse{ make(chan *RequestVoteResult) }
	getStateReq := RaftInternalMessageGetStateRequest{ make(chan struct{}), make(chan *GetStateResult) }
	startCmdReq := RaftInternalMessageStartCommandRequest{ make(chan interface{}), make(chan *StartCommandResult) }
	electionT := RaftInternalMessageElectionTimer{ make(chan time.Time) }
	heartbeatT := RaftInternalMessageHeartbeatTimer{ make(chan time.Time) }
	senderGroup := RaftMessageSenderWaitGroup{&sync.RWMutex{}}
	shutdown := make(chan struct{}) 
	return RaftMessages{ appReq, appResp, voteReq, voteResp, getStateReq, startCmdReq, electionT, heartbeatT, senderGroup, shutdown }
}

// This waitgroup is designed to wait for all message sender goroutines 
// that are unaware of the raft being killed 
// (and would use the channel in RaftMessages to send messages) to finish. 
// Any subsequent sender goroutines are guaranteed to be aware of the raft being killed 
// (thus, they will not attempt to use the channel). 
// Only then, the receiving end of the RaftMessages's channels are closed, 
// preventing goroutines from being permanently blocked when there is no receiver for their messages.

// The reason for using `sync.RWLock` instead of `sync.WaitGroup` for this waitgroup is 
// that the places where the waitgroup count is increased (Add) may be concurrent 
// with the places where completion is awaited (Wait). 
// `sync.WaitGroup` does not allow concurrent Add and Wait after the count drops to zero. 
// However, this application does not require that the count not increase after it drops to zero. 
// It only needs to separate the goroutines that are unaware of `raft.killed` being true 
// from those that are aware. 

// Therefore, I used a read-write lock. 
// All threads will only access the read lock before the Kill occurs. 
// The read locks that have not been released before the Kill occurs will be waited for. 
// The read locks obtained after the write lock is obtained will be aware that `killed` is true.

// The requirement here is essentially to wait for a bunch of goroutines to complete. 
// This is very similar to the purpose of a `sync.Waitgroup`, 
// but a `sync.Waitgroup` does not support this well. 
// Instead, a read-write lock is more suitable for this requirement. 
// However, it's important to note that the requirement itself is not to protect shared memory, 
// but rather to block until previous readers have completed.
type RaftMessageSenderWaitGroup struct {
	rw *sync.RWMutex
}

func (wg *RaftMessageSenderWaitGroup) add() {
	wg.rw.RLock()
}

func (wg *RaftMessageSenderWaitGroup) done() {
	wg.rw.RUnlock()
}

func (wg *RaftMessageSenderWaitGroup) wait() {
	wg.rw.Lock()
	defer wg.rw.Unlock()
}