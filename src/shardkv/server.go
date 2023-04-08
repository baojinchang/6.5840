package shardkv

import (
	"6.5840/labrpc"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"
import "6.5840/shardctrler"

type Op struct {
	OpId     int64
	ClientId int64
	Name     string
	Key      string
	Value    string
	Shards   map[int]map[string]string
	Configs  shardctrler.Config
}

type ShardKV struct {
	mu            sync.Mutex
	dead          int32
	me            int
	rf            *raft.Raft
	applyCh       chan raft.ApplyMsg
	make_end      func(string) *labrpc.ClientEnd
	gid           int
	ctrlers       []*labrpc.ClientEnd
	maxraftstate  int // snapshot if log grows this big
	sc            shardctrler.Clerk
	opId          map[int64]int64
	currentConfig shardctrler.Config
	lastConfig    shardctrler.Config
	shards        map[int]Shard
	recv          map[int]chan Op
}

type Shard struct {
	Data  map[string]string
	State string
}

type ShardOperationRequest struct {
	ShardIds  []int
	ConfigNum int
}

type ShardOperationResponse struct {
	Shards map[int]map[string]string
	Err
}

func (kv *ShardKV) apply() {
	for !kv.killed() {
		select {
		case item := <-kv.applyCh:
			if item.CommandValid {
				command := item.Command.(Op)
				switch command.Name {
				case "Get":
				case "Put":
					if command.OpId > kv.opId[command.ClientId] {
						fmt.Println("<<<<<<<<<<")
						fmt.Println(kv.me)
						kv.mu.Lock()
						kv.opId[command.ClientId] = command.OpId
						kv.shards[key2shard(command.Key)].Data[command.Key] = command.Value
						kv.mu.Unlock()
					}
				case "Append":
					if command.OpId > kv.opId[command.ClientId] {
						kv.mu.Lock()
						kv.opId[command.ClientId] = command.OpId
						kv.shards[key2shard(command.Key)].Data[command.Key] += command.Value
						kv.mu.Unlock()
					}
				case "config":
					kv.mu.Lock()
					kv.applyConfig(command)
					kv.mu.Unlock()
				case "shard":
					kv.mu.Lock()
					kv.applyShard(command)
					kv.mu.Unlock()
				}
				if _, ok := kv.recv[item.CommandIndex]; ok {
					fmt.Println("<<<<")
					kv.mu.Lock()
					kv.recv[item.CommandIndex] <- command
					kv.mu.Unlock()
				}
				if kv.rf.NeedSnapShot(kv.maxraftstate) && kv.maxraftstate != -1 {

				}
			}
			if item.SnapshotValid {

			}
		}
	}
}

func (kv *ShardKV) applyConfig(command Op) {
	if command.Configs.Num != kv.currentConfig.Num+1 {
		return
	}
	kv.lastConfig = kv.currentConfig
	kv.currentConfig = command.Configs
	for index, gid := range kv.currentConfig.Shards {
		if gid == kv.gid {
			if kv.shards[index].State == "server" {
				continue
			}
			if kv.shards[index].State == "no" {
				if kv.currentConfig.Num == 1 {
					kv.shards[index] = Shard{
						State: "server",
						Data:  kv.shards[index].Data,
					}
					continue
				}
				kv.shards[index] = Shard{
					State: "wait",
					Data:  kv.shards[index].Data,
				}
				continue
			}
		} else {
			kv.shards[index] = Shard{
				State: "no",
				Data:  kv.shards[index].Data,
			}
		}
	}
	//fmt.Println(kv.shards[0].State)
	return
}

func (kv *ShardKV) applyShard(command Op) {
	for index, Data := range command.Shards {
		if kv.shards[index].State == "wait" {
			kv.shards[index] = Shard{
				Data:  DeepCopy(Data),
				State: "server",
			}
		}
	}
	return
}

func (kv *ShardKV) shard() {
	kv.mu.Lock()
	var shardId []int
	for index, i := range kv.shards {
		if i.State == "wait" {
			shardId = append(shardId, index)
		}
	}
	var gid2shardId map[int][]int
	for _, index := range shardId {
		gid := kv.lastConfig.Shards[index]
		gid2shardId[gid] = append(gid2shardId[gid], index)
	}
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardId {
		wg.Add(1)
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			pullTaskRequest := ShardOperationRequest{shardIDs, configNum}
			for _, server := range servers {
				var pullTaskResponse ShardOperationResponse
				srv := kv.make_end(server)
				if srv.Call("ShardKV.GetShardsData", &pullTaskRequest, &pullTaskResponse) && pullTaskResponse.Err == OK {

					command := Op{Name: "shard", Shards: pullTaskResponse.Shards}
					kv.rf.Start(command)
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIDs)
	}
	kv.mu.Unlock()
	wg.Wait()
}

func (kv *ShardKV) GetShardsData(args *ShardOperationRequest, reply *ShardOperationResponse) {
	kv.mu.Lock()
	if args.ConfigNum != kv.currentConfig.Num {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	for _, index := range args.ShardIds {
		reply.Shards[index] = DeepCopy(kv.shards[index].Data)
	}
	reply.Err = OK
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) config() {
	canConfig := true
	kv.mu.Lock()
	for _, i := range kv.shards {
		if i.State == "wait" {
			canConfig = false
			break
		}
	}
	if canConfig {
		nextConfigNum := kv.currentConfig.Num + 1
		nowConfig := kv.sc.Query(-1)
		if nextConfigNum <= nowConfig.Num {
			newConfig := kv.sc.Query(nextConfigNum)
			fmt.Println(newConfig)
			fmt.Println(nextConfigNum)
			if newConfig.Num == nextConfigNum {
				command := Op{Name: "config", Configs: newConfig}
				kv.rf.Start(command)
			}
		}
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) canServer(shardId int) bool {
	return kv.currentConfig.Shards[shardId] == kv.gid && kv.shards[shardId].State == "server"
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if !kv.canServer(args.Shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.opId[args.ClientId] >= args.OpId {
		reply.Value = kv.shards[args.Shard].Data[args.Key]
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	command := Op{Name: "Get", Key: args.Key, OpId: args.OpId, ClientId: args.ClientId}
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.recv[index] = make(chan Op)
	kv.mu.Unlock()
	select {
	case item := <-kv.recv[index]:
		if item.OpId != args.OpId || item.ClientId != args.ClientId {
			reply.Err = ErrWrongLeader
		} else {
			if item.Name == "Get" {
				kv.mu.Lock()
				reply.Value = kv.shards[args.Shard].Data[args.Key]
				kv.mu.Unlock()
			}
			reply.Err = OK
		}
	case <-time.After(time.Second):
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	close(kv.recv[index])
	delete(kv.recv, index)
	kv.mu.Unlock()
	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if !kv.canServer(args.Shard) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.opId[args.ClientId] >= args.OpId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	command := Op{Name: args.Op, Key: args.Key, Value: args.Value, OpId: args.OpId, ClientId: args.ClientId}
	index, _, isLeader := kv.rf.Start(command)
	if isLeader {
		fmt.Println(kv.me)
	}
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	kv.recv[index] = make(chan Op)
	kv.mu.Unlock()
	select {
	case item := <-kv.recv[index]:
		if item.OpId != args.OpId || item.ClientId != args.ClientId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			fmt.Println(">>>>>>>>>>>>>>")
		}
	case <-time.After(time.Second):
		reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	close(kv.recv[index])
	delete(kv.recv, index)
	kv.mu.Unlock()
	return
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(Shard{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers
	kv.opId = make(map[int64]int64)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.shards = make(map[int]Shard)
	for i := 0; i < NShards; i++ {
		kv.shards[i] = Shard{
			State: "no",
			Data:  make(map[string]string),
		}
	}
	kv.recv = make(map[int]chan Op)
	kv.sc = *shardctrler.MakeClerk(ctrlers)
	kv.currentConfig = kv.sc.Query(-1)
	kv.lastConfig = shardctrler.Config{}

	r := bytes.NewBuffer(persister.ReadSnapshot())
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.shards) != nil && d.Decode(&kv.opId) != nil {

	}

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.apply()
	go kv.Monitor(kv.shard, time.Millisecond)
	go kv.Monitor(kv.config, time.Millisecond)

	return kv
}

func DeepCopy(groups map[string]string) map[string]string {
	newGroups := make(map[string]string)
	for gid, servers := range groups {
		newGroups[gid] = servers
	}
	return newGroups
}

func (kv *ShardKV) Monitor(action func(), timeout time.Duration) {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}
