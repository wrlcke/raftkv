package raft

import "time"

type RaftInternalMessageAppendEntriesRequest struct {
	inC chan *AppendEntriesArgs
	outC chan *AppendEntriesReply
}

type RaftInternalMessageAppendEntriesResponse struct {
	inC chan *AppendEntriesReply
}

type RaftInternalMessageRequestVoteRequest struct {
	inC chan *RequestVoteArgs
	outC chan *RequestVoteReply
}

type RaftInternalMessageRequestVoteResponse struct {
	inC chan *RequestVoteReply
}

type RaftInternalMessageGetStateRequest struct {
	inC chan struct{}
	outC chan struct{}
	term int
	isLeader bool
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
	electionT RaftInternalMessageElectionTimer
	heartbeatT RaftInternalMessageHeartbeatTimer
	shutdown chan struct{}
}

func MakeRaftMessages() RaftMessages {
	appReq := RaftInternalMessageAppendEntriesRequest{ make(chan *AppendEntriesArgs), make(chan *AppendEntriesReply) }
	appResp := RaftInternalMessageAppendEntriesResponse{ make(chan *AppendEntriesReply) }
	voteReq := RaftInternalMessageRequestVoteRequest{ make(chan *RequestVoteArgs), make(chan *RequestVoteReply) }
	voteResp := RaftInternalMessageRequestVoteResponse{ make(chan *RequestVoteReply) }
	getStateReq := RaftInternalMessageGetStateRequest{ make(chan struct{}), make(chan struct{}), 0, false }
	electionT := RaftInternalMessageElectionTimer{ make(chan time.Time) }
	heartbeatT := RaftInternalMessageHeartbeatTimer{ make(chan time.Time) }
	shutdown := make(chan struct{}) 
	return RaftMessages{ appReq, appResp, voteReq, voteResp, getStateReq, electionT, heartbeatT, shutdown }
}