package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
//import "fmt"


type Clerk struct {
	servers []*labrpc.ClientEnd
	Id int64
	commandId int
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
	ck.Id = nrand()
	ck.commandId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	var args GetArgs
	args.Key = key
	args.Id = ck.Id
	ck.commandId++
	args.CommandId = ck.commandId
	for {
		for i := 0; i < len(ck.servers); i++ {
			//fmt.Println("Get", key, "from server", i)
			var reply GetReply
			ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
			//fmt.Println("reply.WrongLeader:", reply.WrongLeader)
			//fmt.Println("reply.Err:", reply.Err)
			//fmt.Println("reply.Value:", reply.Value)
			if ok {
				if reply.WrongLeader == false && reply.Err == OK {
					return reply.Value
				}
			}
		}
	}
	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	var args PutAppendArgs
	args.Key = key
	args.Value = value
	args.Op = op
	args.Id = ck.Id
	ck.commandId++
	args.CommandId = ck.commandId
	for {
		for i := 0; i < len(ck.servers); i++ {
			//fmt.Println("Put", key, "to", value, "at server", i)
			var reply PutAppendReply
			ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
			//fmt.Println("reply.WrongLeader:", reply.WrongLeader)
			//fmt.Println("reply.Err:", reply.Err)
			if ok {
				if reply.WrongLeader == false && reply.Err == OK {
					return
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
