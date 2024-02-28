package shardctrler

import (
	"sort"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

var Noop struct{} = struct{}{}

var (
	errAborted      = OperationResult{Err: ErrAborted}
	canceledByRetry = OperationResult{Err: ErrCanceledByRetry}
)

type ShardCtrler struct {
	// mu       sync.Mutex
	me       int
	rf       *raft.Raft
	applyCh  chan raft.ApplyMsg
	shutdown chan struct{}

	// Your data here.

	ConfigStore
	reg   OperationRegistry
	watch OperationWatch
}

type ConfigStore struct {
	configs []Config // indexed by config num
}

type OperationRegistry struct {
	mu        sync.Mutex
	res       map[int64]map[int64]OperationResult
	completed map[int64]int64
}

type OperationWatch struct {
	mu   sync.Mutex
	wait map[int64]map[int64]chan OperationResult
}

func (sc *ShardCtrler) HandleOperation(args Operation, reply *OperationResult) {
	// Your code here.
	if sc.reg.IsApplied(args.ClientId, args.RequestId) {
		*reply = sc.reg.Get(args.ClientId, args.RequestId)
		return
	}
	if sc.watch.IsWatching(args.ClientId, args.RequestId) {
		sc.watch.Notify(args.ClientId, args.RequestId, canceledByRetry)
	}
	waitCh := sc.watch.Watch(args.ClientId, args.RequestId)
	_, _, isLeader := sc.rf.Start(args)
	if !isLeader {
		reply.Err = ErrWrongLeader
		sc.watch.Cancel(args.ClientId, args.RequestId)
		return
	}
	select {
	case *reply = <-waitCh:
	case <-sc.shutdown:
		reply.Err = ErrShutdown
	}
}

func (sc *ShardCtrler) applier() {
	var applyMsg raft.ApplyMsg
	var ok bool
	for {
		select {
		case applyMsg, ok = <-sc.applyCh:
		case <-sc.shutdown:
			return
		}
		if !ok {
			return
		}
		switch {
		case applyMsg.CommandValid:
			sc.applyCommand(applyMsg.Command)
		case applyMsg.LeaderChange:
			if applyMsg.Leader == sc.me {
				sc.rf.Start(Noop)
			}
		case applyMsg.CommandAbort:
			sc.abortCommand(applyMsg.Command)
		default:
			panic("ApplyMsg has no valid field")
		}
	}
}

func (sc *ShardCtrler) applyCommand(cmd interface{}) {
	if cmd == Noop {
		return
	}
	op := cmd.(Operation)
	res := sc.reg.Get(op.ClientId, op.RequestId)
	if !sc.reg.IsApplied(op.ClientId, op.RequestId) {
		res.Err = OK
		switch args := op.Args.(type) {
		case JoinArgs:
			sc.join(args)
		case LeaveArgs:
			sc.leave(args)
		case MoveArgs:
			sc.move(args)
		case QueryArgs:
			res.Config = sc.query(op.Args.(QueryArgs))
		default:
			panic("Unknown operation type")
		}
	}
	sc.reg.MarkApplied(op.ClientId, op.RequestId, res)
	sc.reg.MarkCompleted(op.ClientId, op.MaxCompleted)
	sc.watch.Notify(op.ClientId, op.RequestId, res)
}

func (cf *ConfigStore) join(args JoinArgs) {
	old := cf.configs[len(cf.configs)-1]
	config := Config{Num: len(cf.configs), Shards: old.Shards, Groups: make(map[int][]string)}
	for gid, servers := range old.Groups {
		config.Groups[gid] = servers
	}
	for gid, servers := range args.Servers {
		config.Groups[gid] = servers
	}
	cf.rebalance(&config)
	cf.configs = append(cf.configs, config)
}

func (cf *ConfigStore) leave(args LeaveArgs) {
	old := cf.configs[len(cf.configs)-1]
	config := Config{Num: len(cf.configs), Shards: old.Shards, Groups: make(map[int][]string)}
	for gid, servers := range old.Groups {
		config.Groups[gid] = servers
	}
	for _, gid := range args.GIDs {
		delete(config.Groups, gid)
	}
	for shard, gid := range old.Shards {
		if _, ok := config.Groups[gid]; !ok {
			config.Shards[shard] = 0
		}
	}
	cf.rebalance(&config)
	cf.configs = append(cf.configs, config)
}

func (cf *ConfigStore) rebalance(config *Config) {
	if len(config.Groups) == 0 {
		return
	}
	gids := make([]int, 0, len(config.Groups))
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	x := 0
	count := make(map[int]int)
	for shard, gid := range config.Shards {
		if gid == 0 {
			x, gid = (x+1)%len(gids), gids[x]
			config.Shards[shard] = gid
		}
		count[gid]++
	}
	for {
		minGid, maxGid := gids[0], gids[0]
		minCount, maxCount := count[minGid], count[maxGid]
		for _, gid := range gids {
			if count[gid] < minCount {
				minGid, minCount = gid, count[gid]
			}
			if count[gid] > maxCount {
				maxGid, maxCount = gid, count[gid]
			}
		}
		if maxCount-minCount <= 1 {
			break
		}
		for shard, gid := range config.Shards {
			if gid == maxGid {
				config.Shards[shard] = minGid
				count[maxGid]--
				count[minGid]++
				break
			}
		}
	}
}

func (cf *ConfigStore) move(args MoveArgs) {
	old := cf.configs[len(cf.configs)-1]
	config := Config{Num: len(cf.configs), Shards: old.Shards, Groups: make(map[int][]string)}
	for gid, servers := range old.Groups {
		config.Groups[gid] = servers
	}
	config.Shards[args.Shard] = args.GID
	cf.configs = append(cf.configs, config)
}

func (cf *ConfigStore) query(args QueryArgs) Config {
	if args.Num < 0 || args.Num >= len(cf.configs) {
		return cf.configs[len(cf.configs)-1]
	}
	return cf.configs[args.Num]
}

func (sc *ShardCtrler) abortCommand(cmd interface{}) {
	if cmd == Noop {
		return
	}
	op := cmd.(Operation)
	sc.watch.Notify(op.ClientId, op.RequestId, errAborted)
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

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.shutdown)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Operation{})
	labgob.Register(Noop)
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.shutdown = make(chan struct{})

	// Your code here.
	sc.reg = OperationRegistry{res: make(map[int64]map[int64]OperationResult), completed: make(map[int64]int64)}
	sc.watch = OperationWatch{wait: make(map[int64]map[int64]chan OperationResult)}
	go sc.applier()
	return sc
}
