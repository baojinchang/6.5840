package kvraft

import (
	"6.5840/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	mu       sync.Mutex
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

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.opId++
	ck.mu.Unlock()
	args := Args{Key: key, Op: "Get", ClientId: ck.me, OpId: ck.opId}
	reply := Reply{}
	for true {
		ok := ck.servers[ck.leaderId].Call("KVServer.Op", &args, &reply)
		if ok && reply.Err == OK {
			return reply.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}

	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.opId++
	ck.mu.Unlock()
	args := Args{Key: key, Op: op, Value: value, ClientId: ck.me, OpId: ck.opId}
	reply := Reply{}
	for true {
		ok := ck.servers[ck.leaderId].Call("KVServer.Op", &args, &reply)
		if ok && reply.Err == OK {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}

	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
