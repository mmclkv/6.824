package shardmaster

import (
	"encoding/gob"
	"labrpc"
	"raft"
	"sync"
	"time"
	//"fmt"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	Shards    [NShards]int
	Groups    map[int][]string
	configs   []Config // indexed by config num
	configNum int
	exeChan   map[int]chan Op
	ack       map[int64]int
	alive     bool
}

type Op struct {
	Conf      Config
	ClientId  int64
	CommandId int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	if !sm.rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for key, _ := range args.Servers {
		_, ok := sm.Groups[key]
		if ok {
			delete(args.Servers, key)
		}
	}
	if len(args.Servers) == 0 {
		reply.WrongLeader = false
		reply.Err = "Duplicated groups or empty groups"
		return
	}
	reverseShardTable := map[int][]int{}
	for index, value := range sm.Shards {
		if value == 0 {
			continue
		}
		_, ok := reverseShardTable[value]
		if !ok {
			reverseShardTable[value] = make([]int, 0)
		}
		reverseShardTable[value] = append(reverseShardTable[value], index)
	}
	var thisConfig Config
	thisConfig.Groups = make(map[int][]string)
	newLen := len(args.Servers) + len(sm.Groups)
	less := NShards / newLen
	more := less + 1
	totalMore := NShards % newLen
	totalLess := newLen - totalMore
	numOfMore := 0
	numOfLess := 0
	if len(reverseShardTable) == 0 {
		num := make([]int, NShards)
		for i := 0; i < NShards; i++ {
			num[i] = i
		}
		for key, _ := range args.Servers {
			if numOfMore < totalMore {
				reverseShardTable[key] = num[0:more]
				num = num[more:]
				numOfMore++
			} else {
				reverseShardTable[key] = num[0:less]
				num = num[less:]
				numOfLess++
			}
		}
	} else {
		num := make([]int, 0)
		for key, _ := range reverseShardTable {
			if len(reverseShardTable[key]) == more {
				if numOfMore < totalMore {
					numOfMore++
				} else {
					num = append(num, reverseShardTable[key][less:]...)
					reverseShardTable[key] = reverseShardTable[key][0:less]
					numOfLess++
				}
			} else if len(reverseShardTable[key]) == less {
				if numOfLess < totalLess {
					numOfLess++
				}
			} else {
				if numOfLess < totalLess {
					num = append(num, reverseShardTable[key][less:]...)
					reverseShardTable[key] = reverseShardTable[key][0:less]
					numOfLess++
				} else if numOfMore < totalMore {
					num = append(num, reverseShardTable[key][more:]...)
					reverseShardTable[key] = reverseShardTable[key][0:more]
					numOfMore++
				}
			}
		}
		for key, _ := range args.Servers {
			if numOfMore < totalMore {
				reverseShardTable[key] = num[0:more]
				num = num[more:]
				numOfMore++
			} else {
				reverseShardTable[key] = num[0:less]
				num = num[less:]
				numOfLess++
			}
		}
	}

	for key, value := range sm.Groups {
		thisConfig.Groups[key] = value
	}
	for key, value := range args.Servers {
		thisConfig.Groups[key] = value
	}

	for key, values := range reverseShardTable {
		for _, value := range values {
			thisConfig.Shards[value] = key
		}
	}
	OpArg := Op{Conf: thisConfig, ClientId: args.Id, CommandId: args.CommandId}
	index, _, _ := sm.rf.Start(OpArg)
	//fmt.Println("joins", args.Servers)
	//sm.mu.Lock()
	_, ok := sm.exeChan[index]
	if !ok {
		sm.exeChan[index] = make(chan Op, 1)
	}
	//sm.mu.Unlock()
	select {
	case exeArg := <-sm.exeChan[index]:
		if OpArg.ClientId == exeArg.ClientId && OpArg.CommandId == exeArg.CommandId {
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

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if !sm.rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	temp := make([]int, 0)
	for _, value := range args.GIDs {
		_, ok := sm.Groups[value]
		if ok {
			temp = append(temp, value)
		}
	}
	args.GIDs = temp
	if len(args.GIDs) == 0 {
		reply.WrongLeader = false
		reply.Err = "Duplicated groups or empty groups"
		return
	}
	reverseShardTable := map[int][]int{}
	for key, _ := range sm.Groups {
		reverseShardTable[key] = make([]int, 0)
	}
	for index, value := range sm.Shards {
		if value == 0 {
			continue
		}
		_, ok := reverseShardTable[value]
		if !ok {
			reverseShardTable[value] = make([]int, 0)
		}
		reverseShardTable[value] = append(reverseShardTable[value], index)
	}
	num := make([]int, 0)
	count := 0
	for _, value := range args.GIDs {
		_, ok := reverseShardTable[value]
		if ok {
			num = append(num, reverseShardTable[value]...)
			delete(reverseShardTable, value)
			count++
		}
	}
	newLen := len(sm.Groups) - count
	less := NShards / newLen
	more := less + 1
	totalMore := NShards % newLen
	totalLess := newLen - totalMore
	numOfMore := 0
	numOfLess := 0
	for key, _ := range reverseShardTable {
		if len(reverseShardTable[key]) < less {
			if numOfLess < totalLess {
				addLen := less - len(reverseShardTable[key])
				reverseShardTable[key] = append(reverseShardTable[key], num[0:addLen]...)
				num = num[addLen:]
				numOfLess++
			} else if numOfMore < totalMore {
				addLen := more - len(reverseShardTable[key])
				reverseShardTable[key] = append(reverseShardTable[key], num[0:addLen]...)
				num = num[addLen:]
				numOfMore++
			}
		} else if len(reverseShardTable[key]) == less {
			if numOfLess < totalLess {
				numOfLess++
			} else {
				addLen := more - len(reverseShardTable[key])
				reverseShardTable[key] = append(reverseShardTable[key], num[0:addLen]...)
				num = num[addLen:]
				numOfMore++
			}
		} else if len(reverseShardTable[key]) == more {
			if numOfMore < totalMore {
				numOfMore++
			}
		}
	}
	var thisConfig Config
	thisConfig.Groups = make(map[int][]string)
	for key, values := range reverseShardTable {
		thisConfig.Groups[key] = sm.Groups[key]
		for _, value := range values {
			thisConfig.Shards[value] = key
			//fmt.Println("thisConfig.Shards[", value, "]=", key)
		}
	}
	OpArg := Op{Conf: thisConfig, ClientId: args.Id, CommandId: args.CommandId}
	index, _, _ := sm.rf.Start(OpArg)
	//fmt.Println("current Shard:", sm.Shards)
	//fmt.Println("current Group:", sm.Groups)
	//fmt.Println("leaves", args.GIDs)
	//sm.mu.Lock()
	_, ok := sm.exeChan[index]
	if !ok {
		sm.exeChan[index] = make(chan Op, 1)
	}
	//sm.mu.Unlock()
	select {
	case exeArg := <-sm.exeChan[index]:
		if OpArg.ClientId == exeArg.ClientId && OpArg.CommandId == exeArg.CommandId {
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

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	if !sm.rf.IsLeader() {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var thisConfig Config
	thisConfig.Shards = sm.Shards
	thisConfig.Shards[args.Shard] = args.GID
	thisConfig.Groups = make(map[int][]string)
	for key, value := range sm.Groups {
		thisConfig.Groups[key] = value
	}
	OpArg := Op{Conf: thisConfig, ClientId: args.Id, CommandId: args.CommandId}
	index, _, _ := sm.rf.Start(OpArg)
	//fmt.Println("current Shard:", sm.Shards)
	//fmt.Println("current Group:", sm.Groups)
	//fmt.Println("move shard", args.Shard, "to group", args.GID)
	//sm.mu.Lock()
	_, ok := sm.exeChan[index]
	if !ok {
		sm.exeChan[index] = make(chan Op, 1)
	}
	//sm.mu.Unlock()
	select {
	case exeArg := <-sm.exeChan[index]:
		if OpArg.ClientId == exeArg.ClientId && OpArg.CommandId == exeArg.CommandId {
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

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if !sm.rf.IsLeader() {
		reply.WrongLeader = true
		return
	} else {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = OK
		//fmt.Println("current configs of peer", sm.me, ":", sm.configs)
		//fmt.Println("configNum:", sm.configNum)
		if args.Num == -1 || args.Num >= sm.configNum {
			reply.Config = sm.configs[sm.configNum-1]
		} else {
			reply.Config = sm.configs[args.Num]
		}
		//fmt.Println("reply.Config:", reply.Config)
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//

func (sm *ShardMaster) Applyloop() {
	for {
		applymsg := <-sm.applyCh
		comm := applymsg.Command.(Op)
		//sm.mu.Lock()
		_, ok := sm.ack[comm.ClientId]
		if !ok {
			sm.ack[comm.ClientId] = 0
		}
		if sm.ack[comm.ClientId] < comm.CommandId {
			sm.Shards = comm.Conf.Shards
			sm.Groups = make(map[int][]string)
			for key, value := range comm.Conf.Groups {
				sm.Groups[key] = value
			}
			comm.Conf.Num = sm.configNum
			sm.configNum++
			sm.configs = append(sm.configs, comm.Conf)
			if sm.rf.IsLeader() {
				//fmt.Println("Shards after apply:", sm.Shards)
			}
			//fmt.Println("Configs of peer", sm.me, "after apply:", sm.configs)
			//fmt.Println("ConfigNum of peer", sm.me, "after apply:", sm.configNum)
			sm.ack[comm.ClientId] = comm.CommandId
		}
		_, ok = sm.exeChan[applymsg.Index]
		if !ok {
			sm.exeChan[applymsg.Index] = make(chan Op, 1)
		} else {
			select {
			case <-sm.exeChan[applymsg.Index]:
			default:
			}
			sm.exeChan[applymsg.Index] <- comm
		}
		//sm.mu.Unlock()
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.configNum = 1

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.Groups = make(map[int][]string)
	sm.exeChan = make(map[int]chan Op)
	sm.ack = make(map[int64]int)

	if len(sm.rf.Log) > 1 {
		lastConfig := sm.rf.Log[len(sm.rf.Log)-1].Command.(Op).Conf
		sm.Shards = lastConfig.Shards
		for key, value := range lastConfig.Groups {
			sm.Groups[key] = value
		}
		sm.configNum = len(sm.rf.Log)
		for i := 1; i < len(sm.rf.Log); i++ {
			currConfig := sm.rf.Log[i].Command.(Op).Conf
			currConfig.Num = i
			sm.configs = append(sm.configs, currConfig)
		}
	}

	go sm.Applyloop()

	return sm
}
