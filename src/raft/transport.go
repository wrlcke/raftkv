package raft

import "6.5840/labrpc"

type Transport interface {
	Send(m Message)
	Recv() chan Message
	Stop()
}

// Use labrpc to achieve one-way transport layer
// Raft implementation abstracts the transport layer to
// asynchoronously send one-way messages to other peers,
// and also receive these messages from other peers.
// The labrpc however only supports two-way RPC call
// So messages depends on the message type are sent and receive in different ways
// Request messages are sent by RPC call and received in rpc args,
// and response messages are sent by RPC return and received in rpc reply
type LabRpcTransport struct {
	me         int
	peers      []*labrpc.ClientEnd
	recvCh     chan Message   // all messages received from other peers are sent to this channel and then accessiable by Recv()
	sendReqCh  []chan Message // notify the rpcSender to send the request message
	recvReqCh  chan Message   // inside rpc service method, the request message(rpc args) are sent to this channel
	sendRespCh []chan Message // insdie rpc service method, block until raft send the response message(rpc reply) to this channel
	recvRespCh chan Message   // when rpc return, the response message is sent to this channel
	shutdown   chan struct{}
}

func NewLabRpcTransport(peers []*labrpc.ClientEnd, me int, recvCh chan Message) *LabRpcTransport {
	if recvCh == nil {
		recvCh = make(chan Message, 100*len(peers))
	}
	lt := &LabRpcTransport{
		me:         me,
		peers:      peers,
		sendReqCh:  make([]chan Message, len(peers)),
		sendRespCh: make([]chan Message, len(peers)),
		recvCh:     recvCh,
		shutdown:   make(chan struct{}),
	}
	lt.recvReqCh = lt.recvCh
	lt.recvRespCh = lt.recvCh
	for i := range peers {
		if i != lt.me {
			lt.sendReqCh[i] = make(chan Message, 100)
			lt.sendRespCh[i] = make(chan Message, 100)
		}
	}
	lt.Start()
	return lt
}

func (t *LabRpcTransport) Start() {
	for i := range t.peers {
		if i != t.me {
			go t.rpcSender(i)
		}
	}
}

func (t *LabRpcTransport) Send(m Message) {
	sendCh := t.sendReqCh[m.To]
	if m.Type == MsgAppResp || m.Type == MsgSnapResp || m.Type == MsgVoteResp || m.Type == MsgPreVoteResp {
		sendCh = t.sendRespCh[m.To]
	}
	// For MsgPreVoteResp, m.Term is not the term of node itself, but the term of the MsgPreVoteReq
	if m.Type != MsgPreVoteResp {
		LogPrint(m.LogTopic(), "s%d sent %v to s%d at term %d %v", m.From, m.Type, m.To, m.Term, &m)
	} else {
		LogPrint(m.LogTopic(), "s%d sent %v to s%d %v", m.From, m.Type, m.To, &m)
	}
	select {
	case sendCh <- m:
	case <-t.shutdown:
	}
}

func (t *LabRpcTransport) Recv() chan Message {
	return t.recvCh
}

func (t *LabRpcTransport) Stop() {
	close(t.shutdown)
}

func (t *LabRpcTransport) rpcSender(to int) {
	rpcCall := make(chan *labrpc.Call, 100)
	go t.rpcWaiter(rpcCall)
	for {
		select {
		case m := <-t.sendReqCh[to]:
			t.peers[to].Go("Raft.HandleLabRpc", m, &Message{}, rpcCall)
		case <-t.shutdown:
			return
		}
	}
}

func (t *LabRpcTransport) rpcWaiter(rpcCall <-chan *labrpc.Call) {
	for {
		select {
		case call := <-rpcCall:
			if call.Ok {
				select {
				case t.recvRespCh <- *call.Reply.(*Message):
				case <-t.shutdown:
					return
				}
			}
		case <-t.shutdown:
			return
		}
	}
}

func (rf *Raft) HandleLabRpc(args Message, reply *Message) {
	reply.Term = TermNone
	select {
	case rf.transport.(*LabRpcTransport).recvReqCh <- args:
	case <-rf.transport.(*LabRpcTransport).shutdown:
		return
	}
	select {
	case *reply = <-rf.transport.(*LabRpcTransport).sendRespCh[args.From]:
	case <-rf.transport.(*LabRpcTransport).shutdown:
	}
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
// func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
// 	ok := rf.peers[server].Call("Raft.RequestVoteRPC", args, reply)
// }
