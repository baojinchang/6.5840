package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	me       int64
	opId     int64
	leaderId int
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
	ck.me = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{Op: "Query", OpId: ck.opId, ClientId: ck.me}
	args.Num = num
	reply := QueryReply{}
	for {
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
		if ok && reply.Err == OK {
			return reply.Config
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
	return reply.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Op: "Join", OpId: ck.opId, ClientId: ck.me}
	args.Servers = servers
	reply := JoinReply{}
	for {
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
		if ok && reply.Err == OK {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
	return
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{Op: "Leave", OpId: ck.opId, ClientId: ck.me}
	args.GIDs = gids
	reply := LeaveReply{}
	for {
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
		if ok && reply.Err == OK {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
	return
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Op: "Move", OpId: ck.opId, ClientId: ck.me}
	args.Shard = shard
	args.GID = gid
	reply := MoveReply{}
	for {
		ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
		if ok && reply.Err == OK {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
	return
}
