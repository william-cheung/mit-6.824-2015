package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"

	ErrNotReady   = "ErrNotReady"
)

type Err string

type GetArgs struct {
	Key    string
	// You'll have to add definitions here.
	CID    string  // client identifier
	Seq    int     // request seq
}

type GetReply struct {
	Err   Err
	Value string
}

type PutAppendArgs struct {
	Key    string
	Value  string
	Op     string // "Put" or "Append"
	// You'll have to add definitions here.
	CID    string
	Seq    int
	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type PutAppendReply struct {
	Err Err
}

type TransferStateArgs struct {
	ConfigNum  int
	Shard      int
}

type TransferStateReply struct {
	Err     Err
	XState  XState
}
