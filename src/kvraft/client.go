package kvraft

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
	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.lastLeader.Store(LeaderNone)
	ck.clientId = nrand()
	ck.requestId.Store(0)
	ck.maxCompleted.Store(0)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	reply := ck.SendOperation(key, "", "Get")
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.SendOperation(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.SendOperation(key, value, "Append")
}

// shared by Put and Append and Get.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) SendOperation(key, value, t string) KVOperationReply {
	args := KVOperationArgs{key, value, t, ck.clientId, ck.requestId.Add(1), ck.maxCompleted.Load()}
	reply := KVOperationReply{}
	lastLeader := int64(LeaderNone)
	call := new(labrpc.Call)
	for {
		calls := make(chan *labrpc.Call, len(ck.servers))
		servers := make(map[*labrpc.Call]int64)
		timer := time.NewTimer(300 * time.Millisecond)
		lastLeader = ck.lastLeader.Load()
		for i := range ck.servers {
			if i == int(lastLeader) || lastLeader == LeaderNone {
				call = ck.servers[i].Go("KVServer.HandleOperation", &args, &KVOperationReply{}, calls)
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
			reply = *call.Reply.(*KVOperationReply)
			if call.Ok && reply.Err == OK {
				ck.lastLeader.Store(servers[call])
				ck.updateCompleted(args.RequestId)
				timer.Stop()
				return reply
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
