package shardkv

import (
	"bytes"
	"encoding/gob"
	//"fmt"
	"labrpc"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	Shard     int
	Data      map[string]string
	Config    shardmaster.Config
	ClientId  int64
	CommandId int
}

type ShardKV struct {
	rpc          sync.Mutex
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	alive       bool
	endofConfig bool
	mode        int
	sm          *shardmaster.Clerk
	config      shardmaster.Config
	data        map[string]string
	ack         map[int64]int
	exeChan     map[int]chan Op
	shardChan   map[int][shardmaster.NShards]chan int
	transferAck [shardmaster.NShards]int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.rpc.Lock()
	defer kv.rpc.Unlock()
	if !kv.alive {
		reply.Err = ErrNotAlive
		return
	}
	if !kv.rf.IsLeader() {
		reply.Err = ErrNotLeader
		reply.WrongLeader = true
		return
	}
	if kv.mode != ToClient {
		reply.Err = ErrConfiguring
		return
	}
	if args.Config.Num != kv.config.Num {
		reply.Err = ErrOldConfig
		return
	}
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	opArg := Op{Operation: "Get", Key: args.Key, Config: kv.config, ClientId: args.Id, CommandId: args.CommandId}
	index, _, isLeader := kv.rf.Start(opArg)
	if !isLeader {
		reply.Err = ErrNotLeader
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	kv.mu.Lock()
	_, ok := kv.exeChan[index]
	if !ok {
		kv.exeChan[index] = make(chan Op, 1)
	}
	kv.mu.Unlock()
	select {
	case exeArg := <-kv.exeChan[index]:
		if opArg.ClientId == exeArg.ClientId && opArg.CommandId == exeArg.CommandId {
			kv.mu.Lock()
			value, ok := kv.data[opArg.Key]
			if ok {
				reply.Err = OK
				reply.Value = value
				//fmt.Println("value of key", opArg.Key, ":", value)
			} else {
				reply.Err = ErrNoKey
			}
			kv.mu.Unlock()
		} else {
			reply.Err = ErrGet
		}
		return
	case <-time.After(1234 * time.Millisecond):
		reply.Err = ErrTimeOut
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.rpc.Lock()
	defer kv.rpc.Unlock()
	if !kv.alive {
		reply.Err = ErrNotAlive
		return
	}
	if !kv.rf.IsLeader() {
		reply.Err = ErrNotLeader
		reply.WrongLeader = true
		return
	}
	if kv.mode != ToClient {
		reply.Err = ErrConfiguring
		return
	}
	if args.Config.Num != kv.config.Num {
		reply.Err = ErrOldConfig
		return
	}
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		return
	}
	opArg := Op{Operation: args.Op, Key: args.Key, Value: args.Value, Config: kv.config, ClientId: args.Id, CommandId: args.CommandId}
	index, _, isLeader := kv.rf.Start(opArg)
	if !isLeader {
		reply.Err = ErrNotLeader
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	kv.mu.Lock()
	_, ok := kv.exeChan[index]
	if !ok {
		kv.exeChan[index] = make(chan Op, 1)
	}
	kv.mu.Unlock()
	select {
	case exeArg := <-kv.exeChan[index]:
		if opArg.ClientId == exeArg.ClientId && opArg.CommandId == exeArg.CommandId {
			//fmt.Println("value of key", opArg.Key, ":", kv.data[opArg.Key])
			reply.Err = OK
		} else {
			//fmt.Println("value of key", opArg.Key, ":", kv.data[opArg.Key], ErrPutAppend)
			reply.Err = ErrPutAppend
		}
		return
	case <-time.After(2000 * time.Millisecond):
		//fmt.Println("value of key", opArg.Key, ":", kv.data[opArg.Key], ErrTimeOut)
		reply.Err = ErrTimeOut
		return
	}
}

func (kv *ShardKV) Transfer(args *TransferArgs, reply *TransferReply) {
	kv.rpc.Lock()
	defer kv.rpc.Unlock()
	if kv.mode != ToServer {
		reply.Err = ErrNotConfiguring
		return
	}
	if args.Config.Num != kv.config.Num {
		reply.Err = ErrOldConfig
		return
	}
	if args.Config.Num <= kv.transferAck[args.Shard] {
		reply.Err = ErrDuplicateShard
		return
	}
	opArg := Op{Operation: "Transfer", Shard: args.Shard, Data: args.Data, Config: args.Config}
	index, _, isLeader := kv.rf.Start(opArg)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	kv.mu.Lock()
	_, ok := kv.exeChan[index]
	if !ok {
		kv.exeChan[index] = make(chan Op, 1)
	}
	kv.mu.Unlock()
	select {
	case <-kv.exeChan[index]:
		reply.Err = OK
		return
	case <-time.After(1234 * time.Millisecond):
		reply.Err = ErrTimeOut
		return
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.alive = false
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) ApplyLoop() {
	for kv.alive {
		applymsg := <-kv.applyCh
		if applymsg.UseSnapshot {
			r := bytes.NewBuffer(applymsg.Snapshot)
			d := gob.NewDecoder(r)
			var dummy int
			d.Decode(&dummy)
			d.Decode(&dummy)
			d.Decode(&kv.data)
			d.Decode(&kv.ack)
			d.Decode(&kv.config)
			d.Decode(&kv.transferAck)
			continue
		}
		comm := applymsg.Command.(Op)
		for comm.Config.Num > kv.config.Num {
			time.Sleep(200 * time.Millisecond)
		}
		//if kv.rf.IsLeader() {
		//	fmt.Println("applymsg of group", kv.gid, ":", applymsg)
		//}		
		if comm.Operation == "Transfer" {
			kv.mu.Lock()
			for key, value := range comm.Data {
				kv.data[key] = value
			}
			kv.transferAck[comm.Shard] = comm.Config.Num
			_, ok := kv.shardChan[comm.Config.Num]
			if !ok {
				var temp [shardmaster.NShards]chan int
				for i := 0; i < shardmaster.NShards; i++ {
					temp[i] = make(chan int, 10)
				}
				kv.shardChan[comm.Config.Num] = temp
			}
			kv.shardChan[comm.Config.Num][comm.Shard] <- 1
			_, ok = kv.exeChan[applymsg.Index]
			if !ok {
				kv.exeChan[applymsg.Index] = make(chan Op, 10)
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
			continue
		}
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
		_, ok = kv.exeChan[applymsg.Index]
		if !ok {
			kv.exeChan[applymsg.Index] = make(chan Op, 10)
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

func (kv *ShardKV) UpdateConfig() {
	for kv.alive {
		newConfig := kv.sm.Query(kv.config.Num + 1)
		if kv.config.Num == newConfig.Num {
			time.Sleep(100 * time.Millisecond)
		} else {
			kv.mode = Private
			time.Sleep(1500 * time.Millisecond)
			kv.mode = ToServer
			kv.Reconfigure(kv.config, newConfig)
			kv.config = newConfig
			kv.mode = ToClient
		}
	}
}

func (kv *ShardKV) Reconfigure(oldConfig shardmaster.Config, newConfig shardmaster.Config) {
	if newConfig.Num == 1 {
		return
	}
	var sendMaps [shardmaster.NShards]map[string]string
	for index := range sendMaps {
		sendMaps[index] = make(map[string]string)
	}
	for key, value := range kv.data {
		shard := key2shard(key)
		if newConfig.Shards[shard] != kv.gid {
			sendMaps[shard][key] = value
			delete(kv.data, key)
		}
	}

	for shard := 0; shard < shardmaster.NShards; shard++ {
		if oldConfig.Shards[shard] == kv.gid && newConfig.Shards[shard] != kv.gid {
			args := TransferArgs{}
			args.Shard = shard
			args.Data = sendMaps[shard]
			args.Config = oldConfig
			if servers, ok := newConfig.Groups[newConfig.Shards[shard]]; ok {
				go func(args TransferArgs) {
					si := 0
					for {
						srv := kv.make_end(servers[si])
						var reply TransferReply
						ok := srv.Call("ShardKV.Transfer", &args, &reply)
						if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrDuplicateShard) {
							//fmt.Println("group", kv.gid, "successfully sends shard", args.Shard, "to group", newConfig.Shards[args.Shard], "at the end of config", oldConfig.Num)							
							break
						}
						si = (si + 1) % len(servers)
					}
				}(args)
			}
		}
	}

	for shard := 0; shard < shardmaster.NShards; shard++ {
		if oldConfig.Shards[shard] != kv.gid && newConfig.Shards[shard] == kv.gid {
			if kv.rf.IsLeader() {
				//fmt.Println("group", kv.gid, "waits for shard", shard, "at the end of config", oldConfig.Num)
			}
			if kv.transferAck[shard] >= kv.config.Num {
				if kv.rf.IsLeader() {
					//fmt.Println("group", kv.gid, "get shard", shard, "at the end of config", oldConfig.Num)
				}
				continue
			}		
			_, ok := kv.shardChan[oldConfig.Num]
			if !ok {
				kv.mu.Lock()
				var temp [shardmaster.NShards]chan int
				for i := 0; i < shardmaster.NShards; i++ {
					temp[i] = make(chan int, 10)
				}
				kv.shardChan[oldConfig.Num] = temp
				kv.mu.Unlock()
			}
			<-kv.shardChan[oldConfig.Num][shard]
			if kv.rf.IsLeader() {
				//fmt.Println("group", kv.gid, "get shard", shard, "at the end of config", oldConfig.Num)
			}
			
		}
	}
}

func (kv *ShardKV) TakeSnapshot(index int) {
	//fmt.Println("group", kv.gid, "id", kv.me, "takes snapshot at index", index)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.ack)
	e.Encode(kv.config)
	e.Encode(kv.transferAck)
	data := w.Bytes()
	kv.rf.TakeSnapshot(data, index)

	////fmt.Println("RaftStatSize of group", kv.gid, "id", kv.me, "after snapshot:", kv.rf.RaftStateSize())
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// //fmt) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// //fmt. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.config.Num = 0
	// Your initialization code here.

	//fmt.Println("group", kv.gid, "starts")

	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.mode = ToClient
	kv.data = make(map[string]string)
	kv.exeChan = make(map[int]chan Op)
	kv.ack = make(map[int64]int)
	kv.shardChan = make(map[int][shardmaster.NShards]chan int)

	select {
	case applymsg := <-kv.applyCh:
		////fmt.Println("snapshot:", applymsg)
		if applymsg.UseSnapshot {
			r := bytes.NewBuffer(applymsg.Snapshot)
			d := gob.NewDecoder(r)
			var dummy int
			d.Decode(&dummy)
			d.Decode(&dummy)
			d.Decode(&kv.data)
			d.Decode(&kv.ack)
			d.Decode(&kv.config)
			d.Decode(&kv.transferAck)
		}
	default:
	}

	//fmt.Println("data of group", kv.gid, ":", kv.data)
	//fmt.Println("log of group", kv.gid, ":", kv.rf.Log)
	//fmt.Println("config of group", kv.gid, ":", kv.config)

	kv.alive = true

	go kv.ApplyLoop()
	go kv.UpdateConfig()

	return kv
}
