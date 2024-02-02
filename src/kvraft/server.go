package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Operation struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key          string
	Value        string
	Type         string // "Put" or "Append" or "Get"
	ClientId     int64
	RequestId    int64
	MaxCompleted int64
}

var Noop struct{} = struct{}{}

type OperationResult struct {
	Err   Err
	Value string
}

type KVServer struct {
	// mu      sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	dead     int32         // set by Kill()
	shutdown chan struct{} // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store KVStore
	reg   OperationRegistry
	watch OperationWatch
}

var (
	errAborted      = OperationResult{Err: ErrAborted}
	canceledByRetry = OperationResult{Err: ErrCanceledByRetry}
)

type OperationRegistry struct {
	mu        sync.Mutex
	res       map[int64]map[int64]OperationResult
	completed map[int64]int64
}

type OperationWatch struct {
	mu   sync.Mutex
	wait map[int64]map[int64]chan OperationResult
}

func (kv *KVServer) HandleOperation(args *KVOperationArgs, reply *KVOperationReply) {
	// Your code here.
	LogPrint(dClient, "s%d received operation %d <type: \"%s\", key: \"%s\", value: \"%s\">", kv.me, args.RequestId, args.Type, args.Key, args.Value)
	if kv.reg.IsApplied(args.ClientId, args.RequestId) {
		res := kv.reg.Get(args.ClientId, args.RequestId)
		reply.Value, reply.Err = res.Value, res.Err
		LogPrint(dClient, "s%d had applied operation %d <type: \"%s\", key: \"%s\", value: \"%s\">", kv.me, args.RequestId, args.Type, args.Key, args.Value)
		return
	}
	if kv.watch.IsWatching(args.ClientId, args.RequestId) {
		kv.watch.Notify(args.ClientId, args.RequestId, canceledByRetry)
	}
	waitCh := kv.watch.Watch(args.ClientId, args.RequestId)
	index, _, isLeader := kv.rf.Start(Operation{args.Key, args.Value, args.Type, args.ClientId, args.RequestId, args.MaxCompleted})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.watch.Cancel(args.ClientId, args.RequestId)
		return
	}
	LogPrint(dClient, "s%d started operaton %d <index: %d, type: \"%s\", key: \"%s\", value: \"%s\">", kv.me, args.RequestId, index, args.Type, args.Key, args.Value)
	select {
	case res := <-waitCh:
		reply.Value, reply.Err = res.Value, res.Err
		if res.Err != OK {
			LogPrint(dClient, "s%d failed operation %d <index: %d, type: \"%s\", key: \"%s\", value: \"%s\", error: \"%s\">", kv.me, args.RequestId, index, args.Type, args.Key, args.Value, res.Err)
		} else {
			LogPrint(dClient, "s%d finished operation %d <index: %d, type: \"%s\", key: \"%s\", value: \"%s\">", kv.me, args.RequestId, index, args.Type, args.Key, args.Value)
		}
	case <-kv.shutdown:
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) applier() {
	var applyMsg raft.ApplyMsg
	var ok bool
	for {
		select {
		case applyMsg, ok = <-kv.applyCh:
		case <-kv.shutdown:
			return
		}
		if !ok {
			return
		}
		switch {
		case applyMsg.CommandValid:
			kv.applyCommand(applyMsg.Command)
		case applyMsg.SnapshotValid:
		case applyMsg.LeaderChange:
			if applyMsg.Leader == kv.me {
				LogPrint(dLeader, "s%d started a noop", kv.me)
				kv.rf.Start(Noop)
			}
		case applyMsg.CommandAbort:
			kv.abortCommand(applyMsg.Command)
		default:
			panic("ApplyMsg has no valid field")
		}
	}
}

func (kv *KVServer) applyCommand(command interface{}) {
	if command == Noop {
		return
	}
	op := command.(Operation)
	applied := kv.reg.IsApplied(op.ClientId, op.RequestId)
	res := kv.reg.Get(op.ClientId, op.RequestId)
	switch op.Type {
	case "Put":
		if !applied {
			kv.store.Put(op.Key, op.Value)
			res.Err = OK
		}
	case "Append":
		if !applied {
			kv.store.Append(op.Key, op.Value)
			res.Err = OK
		}
	case "Get":
		res.Err = OK
		res.Value = kv.store.Get(op.Key)
	}
	kv.reg.MarkApplied(op.ClientId, op.RequestId, res)
	kv.reg.MarkCompleted(op.ClientId, op.MaxCompleted)
	kv.watch.Notify(op.ClientId, op.RequestId, res)
}

func (kv *KVServer) abortCommand(command interface{}) {
	if command == Noop {
		return
	}
	op := command.(Operation)
	kv.watch.Notify(op.ClientId, op.RequestId, errAborted)
}

func (or *OperationRegistry) IsApplied(clientId, requestId int64) bool {
	or.mu.Lock()
	defer or.mu.Unlock()
	if _, ok := or.res[clientId]; !ok {
		or.res[clientId] = make(map[int64]OperationResult)
	}
	_, ok := or.res[clientId][requestId]
	return requestId <= or.completed[clientId] || ok
}

func (or *OperationRegistry) Get(clientId, requestId int64) OperationResult {
	or.mu.Lock()
	defer or.mu.Unlock()
	if _, ok := or.completed[clientId]; !ok {
		or.completed[clientId] = 0
	}
	return or.res[clientId][requestId]
}

func (or *OperationRegistry) MarkApplied(clientId, requestId int64, res OperationResult) {
	or.mu.Lock()
	defer or.mu.Unlock()
	if _, ok := or.res[clientId]; !ok {
		or.res[clientId] = make(map[int64]OperationResult)
	}
	if requestId > or.completed[clientId] {
		or.res[clientId][requestId] = res
	}
}

func (or *OperationRegistry) MarkCompleted(clientId, requestId int64) {
	or.mu.Lock()
	defer or.mu.Unlock()
	if _, ok := or.res[clientId]; !ok {
		or.res[clientId] = make(map[int64]OperationResult)
	}
	if requestId > or.completed[clientId] {
		or.completed[clientId] = requestId
		for id := range or.res[clientId] {
			if id <= requestId {
				delete(or.res[clientId], id)
			}
		}
	}
}

func (ow *OperationWatch) Watch(clientId, requestId int64) <-chan OperationResult {
	ow.mu.Lock()
	if _, ok := ow.wait[clientId]; !ok {
		ow.wait[clientId] = make(map[int64]chan OperationResult)
	}
	ch := make(chan OperationResult, 1)
	ow.wait[clientId][requestId] = ch
	ow.mu.Unlock()
	return ch
}

func (ow *OperationWatch) Notify(clientId, requestId int64, res OperationResult) {
	ow.mu.Lock()
	if _, ok := ow.wait[clientId]; !ok {
		ow.wait[clientId] = make(map[int64]chan OperationResult)
	}
	ch := ow.wait[clientId][requestId]
	delete(ow.wait[clientId], requestId)
	ow.mu.Unlock()
	if ch != nil {
		ch <- res
	}
}

func (ow *OperationWatch) IsWatching(clientId, requestId int64) bool {
	ow.mu.Lock()
	if _, ok := ow.wait[clientId]; !ok {
		ow.wait[clientId] = make(map[int64]chan OperationResult)
	}
	_, ok := ow.wait[clientId][requestId]
	ow.mu.Unlock()
	return ok
}

func (ow *OperationWatch) Cancel(clientId, requestId int64) {
	ow.mu.Lock()
	if _, ok := ow.wait[clientId]; !ok {
		ow.wait[clientId] = make(map[int64]chan OperationResult)
	}
	delete(ow.wait[clientId], requestId)
	ow.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdown)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Operation{})
	labgob.Register(Noop)

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shutdown = make(chan struct{})

	// You may need initialization code here.
	kv.store = NewMapKVStore()
	kv.reg = OperationRegistry{res: make(map[int64]map[int64]OperationResult), completed: make(map[int64]int64)}
	kv.watch = OperationWatch{wait: make(map[int64]map[int64]chan OperationResult)}
	go kv.applier()
	return kv
}
