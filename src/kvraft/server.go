package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	//"fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientId  int64
	CommandId int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data    map[string]string
	exeChan map[int]chan Op
	ack     map[int64]int
	alive   bool
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	opArg := Op{Operation: "Get", Key: args.Key, ClientId: args.Id, CommandId: args.CommandId}
	index, _, isLeader := kv.rf.Start(opArg)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		kv.mu.Lock()
		_, ok := kv.exeChan[index]
		if !ok {
			kv.exeChan[index] = make(chan Op, 1)
		}
		kv.mu.Unlock()
		select {
		case exeArg := <-kv.exeChan[index]:
			if opArg == exeArg {
				reply.WrongLeader = false
				reply.Err = OK
				kv.mu.Lock()
				reply.Value = kv.data[opArg.Key]
				kv.mu.Unlock()
				return
			} else {
				reply.WrongLeader = true
				return
			}
		case <-time.After(1234 * time.Millisecond):
			reply.WrongLeader = true
			return
		}

	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	opArg := Op{Operation: args.Op, Key: args.Key, Value: args.Value, ClientId: args.Id, CommandId: args.CommandId}
	index, _, isLeader := kv.rf.Start(opArg)
	if !isLeader {
		reply.WrongLeader = true
		return
	} else {
		kv.mu.Lock()
		_, ok := kv.exeChan[index]
		if !ok {
			kv.exeChan[index] = make(chan Op, 1)
		}
		kv.mu.Unlock()
		select {
		case exeArg := <-kv.exeChan[index]:
			if opArg == exeArg {
				reply.WrongLeader = false
				reply.Err = OK
				return
			} else {
				reply.WrongLeader = true
				return
			}
		case <-time.After(1234 * time.Millisecond):
			reply.WrongLeader = true
			return
		}

	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *RaftKV) Applyloop() {
	for {
		applymsg := <-kv.applyCh
		//fmt.Println("get")
		if applymsg.UseSnapshot {
			kv.mu.Lock()
			r := bytes.NewBuffer(applymsg.Snapshot)
			d := gob.NewDecoder(r)
			var dummy int
			d.Decode(&dummy)
			d.Decode(&dummy)
			d.Decode(&kv.data)
			d.Decode(&kv.ack)
			kv.mu.Unlock()
			continue
		}
		comm := applymsg.Command.(Op)
		kv.mu.Lock()
		_, ok := kv.ack[comm.ClientId]
		if !ok {
			kv.ack[comm.ClientId] = 0
		}
		if kv.ack[comm.ClientId] < comm.CommandId {
			if comm.Operation == "Put" {
				kv.data[comm.Key] = comm.Value
			} else if comm.Operation == "Append" {
				kv.data[comm.Key] += comm.Value
			}
			kv.ack[comm.ClientId] = comm.CommandId
		}
		//fmt.Println("peer", kv.me, ":", kv.data)
		_, ok = kv.exeChan[applymsg.Index]
		if !ok {
			kv.exeChan[applymsg.Index] = make(chan Op, 1)
		} else {
			select {
			case <-kv.exeChan[applymsg.Index]:
			default:
			}
			kv.exeChan[applymsg.Index] <- comm
		}
		if kv.maxraftstate > 0 && kv.rf.RaftStateSize() >= kv.maxraftstate {
			kv.TakeSnapshot(applymsg.Index)
		}
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) TakeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.ack)
	data := w.Bytes()
	kv.rf.TakeSnapshot(data, index)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.exeChan = make(map[int]chan Op)
	kv.ack = make(map[int64]int)

	go kv.Applyloop()

	return kv
}
