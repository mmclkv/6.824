package shardkv

import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//
const (
	ToClient = iota
	ToServer
	Private
)

const (
	OK                = "OK"
	ErrNoKey          = "ErrNoKey"
	ErrNotAlive       = "ErrNotAlive"
	ErrNotLeader      = "ErrNotLeader"
	ErrWrongGroup     = "ErrWrongGroup"
	ErrTimeOut        = "ErrTimeOut"
	ErrGet            = "ErrGet"
	ErrPutAppend      = "ErrPutAppend"
	ErrTransfer       = "ErrTransfer"
	ErrOldConfig      = "ErrOldConfig"
	ErrDuplicateShard = "ErrDuplicateShard"
	ErrConfiguring    = "ErrConfiguring"
	ErrNotConfiguring = "ErrNotConfiguring"
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
	Config    shardmaster.Config
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key       string
	Value     string // You'll have to add definitions here.
	Id        int64
	CommandId int
	Config    shardmaster.Config
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type TransferArgs struct {
	Shard  int
	Data   map[string]string
	Config shardmaster.Config
}

type TransferReply struct {
	WrongLeader bool
	Err         Err
}
