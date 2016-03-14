package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"


const Debug = 0 

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Get      = "Get"
	Put      = "Put"
	Append   = "Append"
	Reconf   = "Reconf"
)

type Op struct {
	OpID  int64
	CID   string
	Seq   int
	Op	  string
	Key   string
	Value string
}

type Rep struct {
	Err   Err
	Value string
}

type XState struct {
	KVStore  map[string]string
	MRRSMap  map[string]int
	Replies  map[string]Rep
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID


	seq        int

	config     shardmaster.Config
	
	xstate     XState
}

func (kv *ShardKV) sync(xop *Op) {
	seq := kv.seq
	DPrintf("----- server %d sync %v\n", kv.me, xop)
	
	wait_init := func() time.Duration {
		return 10 * time.Millisecond
	}
	
	wait := wait_init()
	for {
		fate, v := kv.px.Status(seq)
		if fate == paxos.Decided {
			op := v.(Op)
			DPrintf("----- server %d : seq %d : %v\n", kv.me, seq, op)
			if xop.OpID == op.OpID {
				break
			} else if op.Op == Put || op.Op == Append {
				kv.doPutAppend(op.Op, op.Key, op.Value)
				kv.recordOperation(op.CID, op.Seq, &Rep{OK, ""})
			} else {
				value, ok := kv.doGet(op.Key)
				if ok {
					kv.recordOperation(op.CID, op.Seq, &Rep{OK, value})
				} else {
					kv.recordOperation(op.CID, op.Seq, &Rep{ErrNoKey, ""})
				}
			}
			kv.px.Done(seq)
			seq++
			wait = wait_init()
		} else { // Pending
			DPrintf("----- server %d starts a new paxos instance : %d %v\n", kv.me, seq, xop)
			kv.px.Start(seq, *xop)
			time.Sleep(wait)
			if wait < time.Second {
				wait *= 2
			}
		}
	}
	kv.px.Done(seq)
	kv.seq = seq + 1
}

func (kv *ShardKV) recordOperation(cid string, seq int, reply *Rep) {
	kv.xstate.MRRSMap[cid] = seq
	kv.xstate.Replies[cid] = *reply
}

func (kv *ShardKV) filterDuplicate(cid string, seq int) (*Rep, bool) {
	last_seq := kv.xstate.MRRSMap[cid]
	if seq < last_seq {
		return nil, true 
	} else if seq == last_seq {
		rp := kv.xstate.Replies[cid]
		return &rp, true
	} 
	return nil, false
}

// check if a key's shard is assigned to the server's group
func (kv *ShardKV) checkGroup(key string) bool {
	shard := key2shard(key)
	if kv.gid != kv.config.Shards[shard] {
		return false
	}
	return true
}

func (kv *ShardKV) doGet(key string) (value string, ok bool) {
	value, ok = kv.xstate.KVStore[key]
	return
}

func (kv *ShardKV) doPutAppend(op string, key string, value string) {
	if op == Put {
		kv.xstate.KVStore[key] = value
	} else if op == Append {
		kv.xstate.KVStore[key] += value
	}
}
	
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("RPC Get : server %d : args %v\n", kv.me, args)
	
	if !kv.checkGroup(args.Key) {
		reply.Err = ErrWrongGroup
		return nil
	}

	rp, yes := kv.filterDuplicate(args.CID, args.Seq)
	if yes {
		DPrintf("dup-op detected : %v\n", args)
		if rp != nil {
			reply.Err, reply.Value = rp.Err, rp.Value
		}
		return nil
	}

	xop := &Op{OpID:nrand(), CID:args.CID, Seq:args.Seq, 
		Op:Get, Key:args.Key}
	kv.sync(xop)

	value, ok := kv.doGet(args.Key)
	if ok {
		reply.Err, reply.Value = OK, value
	} else {
		reply.Err = ErrNoKey
	}

	kv.recordOperation(args.CID, args.Seq, &Rep{reply.Err, reply.Value})

	return nil
}


// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	DPrintf("RPC PutAppend : server %d : args %v\n", kv.me, args)
	
	if !kv.checkGroup(args.Key) {
		DPrintf("RPC PutAppend : ErrWrongGroup : server %d : args %v\n", kv.me, args)
		DPrintf("--------------- config : %v\n", kv.config);
		reply.Err = ErrWrongGroup
		return nil
	}

	rp, yes := kv.filterDuplicate(args.CID, args.Seq) 
	if yes {
		DPrintf("RPC PutAppend : server %d : dup-op detected %v\n", kv.me, args)
		if rp != nil {
			reply.Err = rp.Err
		}
		return nil
	}
	
	xop := &Op{OpID:nrand(), CID:args.CID, Seq:args.Seq,
		Op:args.Op, Key:args.Key, Value:args.Value}
	kv.sync(xop)

	kv.doPutAppend(args.Op, args.Key, args.Value)
	reply.Err = OK
	
	kv.recordOperation(args.CID, args.Seq, &Rep{reply.Err, ""})

	return nil
}

func (kv *ShardKV) reconfigure(config *shardmaster.Config) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for shard := 0; shard < shardmaster.NShards; shard++ {
		gid := kv.config.Shards[shard]
		if config.Shards[shard] == kv.gid && gid != kv.gid {
			if !kv.requestShard(gid, shard) { 
				return false
			}
		}
	}
	kv.config = *config
	return true
}

func (kv *ShardKV) requestShard(gid int64, shard int) bool {
	for _, server := range kv.config.Groups[gid] {
		args := &TransferStateArgs{}
		args.ConfigNum, args.Shard = kv.config.Num, shard
		var reply TransferStateReply
		ok := call(server, "ShardKV.TransferState", args, &reply)
		if ok {
			if reply.Err == OK {
				kv.mergeXState(&reply.XState)	
			} else {
				return false
			}
			break
		}
	}
	return true
}

func (kv *ShardKV) TransferState(args *TransferStateArgs, reply *TransferStateReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("RPC TransferState : server %d : args %v\n", kv.me, args)
	
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return nil
	}

	kv.initXState(&reply.XState)
	
	for key := range kv.xstate.KVStore {
		if key2shard(key) == args.Shard {
			value := kv.xstate.KVStore[key]
			reply.XState.KVStore[key] = value
		}
	}
	for client := range kv.xstate.MRRSMap {
		reply.XState.MRRSMap[client] = kv.xstate.MRRSMap[client] 
		reply.XState.Replies[client] = kv.xstate.Replies[client]
	}

	reply.Err = OK
	return nil
}

func (kv *ShardKV) initXState(xstate *XState) {
	xstate.KVStore = map[string]string{}
	xstate.MRRSMap = map[string]int{}
	xstate.Replies = map[string]Rep{}
}

func (kv *ShardKV) mergeXState(xstate *XState) {
	for key, value := range xstate.KVStore {
		kv.xstate.KVStore[key] = value
	}

	for client, seq := range xstate.MRRSMap {
		xseq := kv.xstate.MRRSMap[client] 
		if xseq < seq {
			kv.xstate.MRRSMap[client] = seq
			kv.xstate.Replies[client] = xstate.Replies[client]
		}
	}
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	DPrintf("---*--- tick ---*---\n")

	latest_config := kv.sm.Query(-1)
	for n := kv.config.Num + 1; n <= latest_config.Num; n++ {
		config := kv.sm.Query(n)
		if !kv.reconfigure(&config) {
			break
		}
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	kv.initXState(&kv.xstate)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
