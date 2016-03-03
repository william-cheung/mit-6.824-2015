package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

import "time"

const Debug = 0
func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos
	seq        int 

	configs []Config // indexed by config num
}


const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Op struct {
	OpID    int64
	Op      string
	Shard   int
	GID     int64
	Servers []string
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	xop := &Op{OpID:nrand(), Op:Join, GID:args.GID, Servers:args.Servers}
	sm.sync(xop)

	sm.doJoin(args.GID, args.Servers)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	xop := &Op{OpID:nrand(), Op:Leave, GID:args.GID}
	sm.sync(xop)

	sm.doLeave(args.GID)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	xop := &Op{OpID:nrand(), Op:Leave, Shard:args.Shard, GID:args.GID}
	sm.sync(xop)

	sm.doMove(args.Shard, args.GID)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	xop := &Op{OpID:nrand(), Op:Query}
	sm.sync(xop)

	last := len(sm.configs) - 1
	if args.Num < 0 || args.Num > last {
		reply.Config = sm.configs[last]
	} else {
		reply.Config = sm.configs[args.Num]
	}

	DPrintf("--- server %d : Query(Num %d) : Config %v\n", sm.me, args.Num, reply.Config)
	return nil
}

func (sm *ShardMaster) sync(xop *Op) {
	seq := sm.seq
	
	wait_init := func() time.Duration {
		return 10 * time.Millisecond
	}
								
	wait := wait_init()
	for {
		fate, v := sm.px.Status(seq)
		if fate == paxos.Decided {
			op := v.(Op)
			if xop.OpID == op.OpID {
				break
			} else {		
				sm.applyOp(&op)	
			} 

			seq++
			wait = wait_init()
		} else { 
			sm.px.Start(seq, *xop)
			
			time.Sleep(wait)
			if wait < time.Second {	
				wait *= 2		
			}
		}
	}
	sm.px.Done(seq)
	sm.seq = seq + 1
}

func (sm *ShardMaster) applyOp(xop *Op) {
	switch xop.Op {
	case Join:
		sm.doJoin(xop.GID, xop.Servers)
	case Leave:
		sm.doLeave(xop.GID)
	case Move:
		sm.doMove(xop.Shard, xop.GID)
	default:
	}
}

func (sm *ShardMaster) doJoin(gid int64, servers []string) {
	DPrintf("--- server %d : doJoin(gid %d, servers %v)\n", sm.me, gid, servers)
	var config Config
	sm.prepareNextConfig(&config)
	_, exists := config.Groups[gid]
	if !exists {
		config.Groups[gid] = servers
		sm.rebalance(&config, Join, gid)
	}
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) doLeave(gid int64) {
	DPrintf("--- server %d : doLeave(gid %d)\n", sm.me, gid)
	var config Config
	sm.prepareNextConfig(&config)
	_, exists := config.Groups[gid]
	if exists {
		delete(config.Groups, gid)
		sm.rebalance(&config, Leave, gid)
	}
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) doMove(shard int, gid int64) {
	DPrintf("--- server %d : doMove(shard %d, gid %d)\n", sm.me, shard, gid)
	var config Config
	sm.prepareNextConfig(&config)
	config.Shards[shard] = gid
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) prepareNextConfig(config *Config) {
	last_config := sm.configs[len(sm.configs)-1]
	config.Num = len(sm.configs)
	config.Shards = last_config.Shards
	config.Groups = map[int64][]string{}
	for gid, servers := range last_config.Groups {
		config.Groups[gid] = servers
	}
}

func (sm *ShardMaster) rebalance(config *Config, op string, gid int64) {
	count_map := map[int64]int{}
	shard_map := map[int64][]int{}
	for shard, xgid := range config.Shards {
		count_map[xgid] += 1
		shard_map[xgid] = append(shard_map[xgid], shard)
	}
	max_nshards, max_gid := 0, int64(0)
	min_nshards, min_gid := NShards + 1, int64(0)
	for xgid := range config.Groups {
		nshards := count_map[xgid]

		if max_nshards < nshards {
			max_nshards, max_gid = nshards, xgid
		}
		if min_nshards > nshards {
			min_nshards, min_gid = nshards, xgid
		}
	}

	if op == Join {
		spg := NShards / len(config.Groups)
		for i := 0; i < spg; i++ {
			shard := shard_map[max_gid][i]
			config.Shards[shard] = gid
		}
	} else if op == Leave {
		for _, shard := range shard_map[gid] {
			config.Shards[shard] = min_gid
		}
	}
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
