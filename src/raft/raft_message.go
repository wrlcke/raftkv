package raft

import (
	"sync"
	"time"
)

type GetStateReply struct {
	term int
	isLeader bool
}

type StartCommandReply struct {
	index int
	term int
	isLeader bool
}

type AppendRequest struct {
	args *AppendEntriesArgs
	reply *AppendEntriesReply
	done chan struct{}
}

type AppendResponse struct {
	server int
	args *AppendEntriesArgs
	reply *AppendEntriesReply
}

type VoteRequest struct {
	args *RequestVoteArgs
	reply *RequestVoteReply
	done chan struct{}
}

type VoteResponse struct {
	server int
	args *RequestVoteArgs
	reply *RequestVoteReply
}

type SnapRequest struct {
	args *InstallSnapshotArgs
	reply *InstallSnapshotReply
	done chan struct{}
}

type SnapResponse struct {
	server int
	args *InstallSnapshotArgs
	reply *InstallSnapshotReply
}

type GetStateRequest struct {
	reply *GetStateReply
	done chan struct{}
}

type StartCommandRequest struct {
	args interface{}
	reply *StartCommandReply
	done chan struct{}
}

type StartSnapshotRequest struct {
	index int
	snapshot []byte
	done chan struct{}
}

type ApplyRequest ApplyMsg

type StorageRequest struct {
	done chan struct{}
}

type StorageResponse struct {
	firstLogIndex int
	lastLogIndex int
}

type RaftMessages struct {
	// Messages handled by Raft.runner
	appReq chan AppendRequest
	appResp chan AppendResponse
	voteReq chan VoteRequest
	voteResp chan VoteResponse
	snapReq chan SnapRequest
	snapResp chan SnapResponse
	getStateReq chan GetStateRequest
	startCmdReq chan StartCommandRequest
	startSnapReq chan StartSnapshotRequest
	electionT <-chan time.Time
	heartbeatT <-chan time.Time
	storageResp chan StorageResponse
	// Messages handled by Raft.applier
	applyReq chan ApplyRequest
	// Messages handled by Raft.persister
	storageReq chan StorageRequest
	// External goroutines that call raft's service methods (rpc service or exposed API) and send messages to Raft.runner
	externalRoutines RaftMessageWaitGroup
	// Shutdown signal for runner (Kill() waits for all previous external goroutines, and then use this signal to shutdown runner)
	shutdown chan struct{}
}

func MakeRaftMessages() RaftMessages {
	return RaftMessages{
		appReq: make(chan AppendRequest),
		appResp: make(chan AppendResponse),
		voteReq: make(chan VoteRequest),
		voteResp: make(chan VoteResponse),
		snapReq: make(chan SnapRequest),
		snapResp: make(chan SnapResponse),
		getStateReq: make(chan GetStateRequest),
		startCmdReq: make(chan StartCommandRequest),
		startSnapReq: make(chan StartSnapshotRequest),
		electionT: chan time.Time(nil),
		heartbeatT: chan time.Time(nil),
		storageResp: make(chan StorageResponse, 1000),
		applyReq: make(chan ApplyRequest, 1000),
		storageReq: make(chan StorageRequest, 1000),
		externalRoutines: RaftMessageWaitGroup{},
		shutdown: make(chan struct{}),
	}
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
type RaftMessageWaitGroup struct {
	rw sync.RWMutex
}

func (wg *RaftMessageWaitGroup) add() {
	wg.rw.RLock()
}

func (wg *RaftMessageWaitGroup) done() {
	wg.rw.RUnlock()
}

func (wg *RaftMessageWaitGroup) wait() {
	wg.rw.Lock()
	defer wg.rw.Unlock()
}
