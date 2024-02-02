package kvraft

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrAborted         = "ErrAborted"         // leader started operation but lost leadership before committing it and then a new leader conflicted with the operation aborting it
	ErrCanceledByRetry = "ErrCanceledByRetry" // request was canceld by a newer request from the same client (when the client times out and sends a retry)
)

type Err string

// Put or Append or Get
type KVOperationArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type         string
	ClientId     int64
	RequestId    int64
	MaxCompleted int64
}

type KVOperationReply struct {
	Err   Err
	Value string
}
