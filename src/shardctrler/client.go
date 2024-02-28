package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

const LeaderNone = -1

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	lastLeader   atomic.Int64
	clientId     int64
	requestId    atomic.Int64
	maxCompleted atomic.Int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader.Store(LeaderNone)
	ck.clientId = nrand()
	ck.requestId.Store(0)
	ck.maxCompleted.Store(0)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	res := ck.SendOperation(QueryArgs{num})
	return res.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.SendOperation(JoinArgs{servers})
}

func (ck *Clerk) Leave(gids []int) {
	ck.SendOperation(LeaveArgs{gids})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.SendOperation(MoveArgs{shard, gid})
}

func (ck *Clerk) SendOperation(args interface{}) OperationResult {
	op := Operation{args, ck.clientId, ck.requestId.Add(1), ck.maxCompleted.Load()}
	res := OperationResult{}
	lastLeader := int64(LeaderNone)
	call := new(labrpc.Call)
	for {
		calls := make(chan *labrpc.Call, len(ck.servers))
		servers := make(map[*labrpc.Call]int64)
		timer := time.NewTimer(300 * time.Millisecond)
		lastLeader = ck.lastLeader.Load()
		for i := range ck.servers {
			if i == int(lastLeader) || lastLeader == LeaderNone {
				call = ck.servers[i].Go("ShardCtrler.HandleOperation", op, &OperationResult{}, calls)
				servers[call] = int64(i)
			}
		}
	wait:
		for range servers {
			select {
			case call = <-calls:
			case <-timer.C:
				break wait
			}
			res = *call.Reply.(*OperationResult)
			if call.Ok && res.Err != ErrWrongLeader {
				ck.lastLeader.Store(servers[call])
				ck.updateCompleted(op.RequestId)
				timer.Stop()
				return res
			}
		}
		ck.lastLeader.Store(LeaderNone)
		// Retry with backoff if timeout not reached
		if timer.Stop() {
			time.Sleep(200 * time.Millisecond)
		}
	}
}

func (ck *Clerk) updateCompleted(requestId int64) {
	for {
		maxc := ck.maxCompleted.Load()
		if maxc >= requestId || ck.maxCompleted.CompareAndSwap(maxc, requestId) {
			return
		}
	}
}
