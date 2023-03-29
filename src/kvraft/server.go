package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Op       string
	Key      string
	Value    string
	OpId     int64
	ClientId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	opId    map[int64]int64
	data    map[string]string
	recv    map[int]chan Op

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) Op(args *Args, reply *Reply) {
	kv.mu.Lock()
	if kv.opId[args.ClientId] >= args.OpId {
		if args.Op == "Get" {
			reply.Value = kv.data[args.Key]
		}
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	command := Op{Value: args.Value, Op: args.Op, Key: args.Key, OpId: args.OpId, ClientId: args.ClientId}
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
			if args.Op == "Get" {
				kv.mu.Lock()
				reply.Value = kv.data[args.Key]
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

func (kv *KVServer) Apply() {
	for !kv.killed() {
		select {
		case item := <-kv.applyCh:
			if item.CommandValid {
				command := item.Command.(Op)
				if command.OpId > kv.opId[command.ClientId] {
					kv.mu.Lock()
					kv.opId[command.ClientId] = command.OpId
					kv.mu.Unlock()
					switch command.Op {
					case "Put":
						kv.mu.Lock()
						kv.data[command.Key] = command.Value
						kv.mu.Unlock()
					case "Append":
						kv.mu.Lock()
						kv.data[command.Key] += command.Value
						kv.mu.Unlock()
					case "Get":
					}
					if _, ok := kv.recv[item.CommandIndex]; ok {
						kv.mu.Lock()
						kv.recv[item.CommandIndex] <- command
						kv.mu.Unlock()
					}
				}
				if kv.rf.NeedSnapShot(kv.maxraftstate) && kv.maxraftstate != -1 {
					kv.mu.Lock()
					w := new(bytes.Buffer)
					e := labgob.NewEncoder(w)
					e.Encode(kv.data)
					e.Encode(kv.Op)
					data := w.Bytes()
					kv.rf.Snapshot(item.CommandIndex, data)
					kv.mu.Unlock()
				}
			}
			if item.SnapshotValid {
				kv.mu.Lock()
				if item.Snapshot == nil || len(item.Snapshot) < 1 { // bootstrap without any state?
					continue
				}
				r := bytes.NewBuffer(item.Snapshot)
				d := labgob.NewDecoder(r)
				if d.Decode(&kv.data) != nil && d.Decode(&kv.opId) != nil {

				}
				kv.mu.Unlock()
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.opId = make(map[int64]int64)

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.recv = make(map[int]chan Op)

	r := bytes.NewBuffer(persister.ReadSnapshot())
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.data) != nil && d.Decode(&kv.opId) != nil {

	}

	go kv.Apply()

	// You may need initialization code here.

	return kv
}
