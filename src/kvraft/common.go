package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type Args struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	OpId     int64
	ClientId int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type Reply struct {
	Err   Err
	Value string
}
