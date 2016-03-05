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


type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	config     shardmaster.Config
	
	store      map[string]string   // key-value store
	op_seq     int                 // paxos seq-num, all ops whose corresponding seq-num are 
	                               // less than log_seq have been applied to the key-value store
	
	// for at-most-once semantics
	cli_seq    map[string]int          // map : client-id -> most recent request-seq-num from the corresponding client
	replies    map[string]interface{}  // map : client-id -> answer to the last request
}

func (kv *KVPaxos) sync(xop *Op) {
	seq := kv.op_seq
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
				kv.recordOperation(op.CID, op.Seq, &PutAppendReply{OK})
			} else {
				value, ok := kv.doGet(op.Key)
				if ok {
					kv.recordOperation(op.CID, op.Seq, &GetReply{OK, value})
				} else {
					kv.recordOperation(op.CID, op.Seq, &GetReply{ErrNoKey, ""})
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
	kv.op_seq = seq + 1
}

func (kv *KVPaxos) recordOperation(cid string, seq int, reply interface{}) {
	kv.cli_seq[cid] = seq
	kv.replies[cid] = reply
}

func (kv *KVPaxos) filterDuplicate(cid string, seq int) (interface{}, bool) {
	last_seq = kv.cli_seq[cid]
	if seq < last_seq {
		return nil, true 
	} else if seq == last_seq {
		rp = kv.replies[cid]
		return rp, true
	} 
	return nil, false
}

func (kv *KVPaxos) doGet(key string) (value string, ok bool) {
	value, ok = kv.store[key]
}

func (kv *KVPaxos) doPutAppend(op string, key string, value string) {
	if op == Put {
		kv.store[key] = value
	} else if op == Append {
		kv.store[key] += value
	}
}
	

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	rp, yes := kv.filterDuplicate(args.CID, args.Seq)
	if yes {
		DPrintf("dup-op detected : %v\n", args)
		if rp != nil {
			tmp_rp := rp.(*GetReply)
			reply.Err, reply.Value = tmp_rp.Err, tmp_rp.Value
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

	kv.recordOperation(args.CID, args.Seq, reply)

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	DPrintf("RPC PutAppend : server %d : op %s : key %s : val %s\n", 
		kv.me, args.Op, args.Key, args.Value)
	
	rp, yes := kv.filterDuplicate(args.CID, args.Seq) 
	if yes {
		DPrintf("dup-op detected : %v\n", args)
		if rp != nil {
			reply.Err = rp.(*PutAppendReply).Err
		}
		return nil
	}
	
	xop := &Op{OpID:nrand(), CID:args.CID, Seq:args.Seq,
		Op:args.Op, Key:args.Key, Value:args.Value}
	kv.sync(xop)

	kv.doPutAppend(args.Op, args.Key, args.Value)
	reply.Err = OK
	
	kv.recordOperation(args.CID, args.Seq, reply)

	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
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
