package shardctrler

import (
	"6.5840/raft"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	opId    map[int64]int64
	recv    map[int]chan Op

	configs []Config // indexed by config num
}

type Op struct {
	Op       string
	Num      int
	Servers  map[int][]string
	Gids     []int
	Shard    int
	Gid      int
	OpId     int64
	ClientId int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	if sc.opId[args.ClientId] >= args.OpId {
		sc.mu.Lock()
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	command := Op{Op: args.Op, Servers: args.Servers, OpId: args.OpId, ClientId: args.ClientId}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		return
	}
	sc.mu.Lock()
	sc.recv[index] = make(chan Op)
	sc.mu.Unlock()
	select {
	case item := <-sc.recv[index]:
		if item.OpId != args.OpId || item.ClientId != args.ClientId {
			reply.Err = "ErrWrongLeader"
		} else {
			reply.Err = OK
		}
	case <-time.After(time.Second):
		reply.Err = "ErrWrongLeader"
	}
	sc.mu.Lock()
	close(sc.recv[index])
	delete(sc.recv, index)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	if sc.opId[args.ClientId] >= args.OpId {
		sc.mu.Lock()
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	command := Op{Op: args.Op, Gids: args.GIDs, OpId: args.OpId, ClientId: args.ClientId}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		return
	}
	sc.mu.Lock()
	sc.recv[index] = make(chan Op)
	sc.mu.Unlock()
	select {
	case item := <-sc.recv[index]:
		if item.OpId != args.OpId || item.ClientId != args.ClientId {
			reply.Err = "ErrWrongLeader"
		} else {
			reply.Err = OK
		}
	case <-time.After(time.Second):
		reply.Err = "ErrWrongLeader"
	}
	sc.mu.Lock()
	close(sc.recv[index])
	delete(sc.recv, index)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	if sc.opId[args.ClientId] >= args.OpId {
		sc.mu.Lock()
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	command := Op{Op: args.Op, Gid: args.GID, Shard: args.Shard, OpId: args.OpId, ClientId: args.ClientId}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		return
	}
	sc.mu.Lock()
	sc.recv[index] = make(chan Op)
	sc.mu.Unlock()
	select {
	case item := <-sc.recv[index]:
		if item.OpId != args.OpId || item.ClientId != args.ClientId {
			reply.Err = "ErrWrongLeader"
		} else {
			reply.Err = OK
		}
	case <-time.After(time.Second):
		reply.Err = "ErrWrongLeader"
	}
	sc.mu.Lock()
	close(sc.recv[index])
	delete(sc.recv, index)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	if sc.opId[args.ClientId] >= args.OpId {
		if args.Num == -1 {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	command := Op{Op: args.Op, Num: args.Num, OpId: args.OpId, ClientId: args.ClientId}
	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		return
	}
	sc.mu.Lock()
	sc.recv[index] = make(chan Op)
	sc.mu.Unlock()
	select {
	case item := <-sc.recv[index]:
		if item.OpId != args.OpId || item.ClientId != args.ClientId {
			reply.Err = "ErrWrongLeader"
		} else {
			sc.mu.Lock()
			if args.Num == -1 {
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[args.Num]
			}
			sc.mu.Unlock()
			reply.Err = OK
		}
	case <-time.After(time.Second):
		reply.Err = "ErrWrongLeader"
	}
	sc.mu.Lock()
	close(sc.recv[index])
	delete(sc.recv, index)
	sc.mu.Unlock()
	return
}

func (sc *ShardCtrler) Apply() {
	for true {
		select {
		case item := <-sc.applyCh:
			if !item.CommandValid {
				continue
			}
			command := item.Command.(Op)
			if command.OpId > sc.opId[command.ClientId] {
				sc.mu.Lock()
				sc.opId[command.ClientId] = command.OpId
				sc.mu.Unlock()
				switch command.Op {
				case "Join":
					sc.mu.Lock()
					sc.ApplyJoin(command.Servers)
					sc.mu.Unlock()
				case "Leave":
					sc.mu.Lock()
					sc.ApplyLeave(command.Gids)
					sc.mu.Unlock()
				case "Move":
					sc.mu.Lock()
					sc.ApplyMove(command.Shard, command.Gid)
					sc.mu.Unlock()
				case "Query":
				}
				if _, ok := sc.recv[item.CommandIndex]; ok {
					sc.mu.Lock()
					sc.recv[item.CommandIndex] <- command
					sc.mu.Unlock()
				}
			}
		}
	}
}

func (sc *ShardCtrler) ApplyMove(shard int, gid int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: DeepCopy(lastConfig.Groups)}
	newConfig.Shards[shard] = gid
	sc.configs = append(sc.configs, newConfig)
	return
}

func (sc *ShardCtrler) ApplyJoin(servers map[int][]string) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: DeepCopy(lastConfig.Groups)}
	for index, server := range servers {
		newConfig.Groups[index] = server
	}
	shards := make([]int, 0)
	gids := make([]int, 0)
	for shard, _ := range newConfig.Shards {
		shards = append(shards, shard)
	}
	for gid, _ := range newConfig.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(shards)
	sort.Ints(gids)
	i := 0
	for _, shard := range shards {
		newConfig.Shards[shard] = gids[i]
		i = (i + 1) % len(gids)
	}
	sc.configs = append(sc.configs, newConfig)
	return
}

func (sc *ShardCtrler) ApplyLeave(g []int) {
	lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{Num: lastConfig.Num + 1, Shards: lastConfig.Shards, Groups: DeepCopy(lastConfig.Groups)}
	for _, gid := range g {
		delete(newConfig.Groups, gid)
	}
	shards := make([]int, 0)
	gids := make([]int, 0)
	for shard, _ := range newConfig.Shards {
		shards = append(shards, shard)
	}
	for gid, _ := range newConfig.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(shards)
	sort.Ints(gids)
	if len(gids) == 0 {
		newConfig.Shards = [NShards]int{}
	} else {
		i := 0
		for _, shard := range shards {
			newConfig.Shards[shard] = gids[i]
			i = (i + 1) % len(gids)
		}
	}
	sc.configs = append(sc.configs, newConfig)
	return
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.configs[0].Num = 0

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.recv = make(map[int]chan Op)
	sc.opId = make(map[int64]int64)

	go sc.Apply()

	return sc
}

func DeepCopy(groups map[int][]string) map[int][]string {
	newGroups := make(map[int][]string)
	for gid, servers := range groups {
		newServers := make([]string, len(servers))
		copy(newServers, servers)
		newGroups[gid] = newServers
	}
	return newGroups
}
