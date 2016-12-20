package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrTimeOut    = "ErrTimeOut"
	ErrPutAppend  = "ErrPutAppend"
	ErrTransfer   = "ErrTransfer"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	Id        int64
	CommandId int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id        int64
	CommandId int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type TransferArgs struct {
	Shard int
	Data  map[string]string
}

type TransferReply struct {
	WrongLeader bool
	Err         Err
}
