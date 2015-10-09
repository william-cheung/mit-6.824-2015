package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

const Debug = 0
func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
}

// life of a filter : 10 sec
const FilterLife = int(10000 * time.Millisecond / viewservice.PingInterval)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	
	// Your declarations here.
	init       bool                   // for initialization
	view       *viewservice.View      // the view we hold
	kvstore    map[string]string      // in memory key-value store 
	filters    map[int64]int          // filters to filter dup-ops
	replies    map[int64]interface{}  // cached replies to ops
}

func (pb *PBServer) isInitialized() bool {
	return pb.init
}

func (pb *PBServer) InitState(args *InitStateArgs, reply *InitStateReply) error {
	pb.mu.Lock()
	if !pb.isInitialized() {
		pb.init = true
		pb.kvstore = args.State
	}
	pb.mu.Unlock()

	reply.Err = OK
	return nil
}

func (pb *PBServer) filterDuplicate(opid int64, method string, reply interface{}) bool {
	_, ok := pb.filters[opid]
	if !ok {
		return false
	}
	
	DPrintf("duplicate op detected, opid : %v, op : %s\n", opid, method)
	rp, ok2 := pb.replies[opid]
	if !ok2 {
		return false
	}

	// bad design, because there would be various types 
	// of operations to be filtered 
	if method == Get {
		reply, ok1 := reply.(*GetReply)
		saved, ok2 := rp.(*GetReply) 
		if ok1 && ok2 {
			copyGetReply(reply, saved)
			return true
		}
	} else if method == Put || method == Append {
		reply, ok1 := reply.(*PutAppendReply)
		saved, ok2 := rp.(*PutAppendReply)
		if ok1 && ok2 {
			copyPutAppendReply(reply, saved)
			return true
		}
	}
	return false
}

func (pb *PBServer) recordOperation(opid int64, reply interface{}) {
	pb.filters[opid] = FilterLife
	pb.replies[opid] = reply
}

func (pb *PBServer) doGet(args *GetArgs, reply *GetReply) error {
	DPrintf("--- op : %s, key : %s\n", Get, args.Key)
	
	key := args.Key
	value, ok := pb.kvstore[key]
	if ok {
		reply.Err = OK
		reply.Value = value
	} else {
		reply.Err = ErrNoKey
	}
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	DPrintf("RPC : Get : server %s\n", pb.me)
	
	// the client (not a PBServer) thinks we are primary
	if pb.me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return nil
	}
	
	ok := pb.filterDuplicate(args.OpID, Get, reply)
	if ok {
		DPrintf("Duplicate : reply.Err : %s\n", reply.Err)
		return nil
	}

	// we think we are primary, forward the request to backup
	if pb.view.Backup != "" {
		DPrintf("forward %s to backup %s\n", Get, pb.view.Backup);
	
		ok := call(pb.view.Backup, "PBServer.BackupGet", args, reply)
		if ok {
			if reply.Err == ErrUninitServer {
				pb.transferState(pb.view.Backup)
			} else {
				return nil
			}
			// data on backup is more trusted than primary
		} else { 
			// unreliable backup / backup is down / network partition
			reply.Err = ErrWrongServer 
			return nil
		}
	}

	pb.doGet(args, reply)

	pb.recordOperation(args.OpID, reply)

	return nil
}

func (pb *PBServer) BackupGet(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	DPrintf("RPC : BackupGet : server %s\n", pb.me)
	
	if pb.me != pb.view.Backup {
		reply.Err = ErrWrongServer
		return nil
	}
	
	// the request is from primary and we are backup

	if !pb.isInitialized() {
		reply.Err = ErrUninitServer
		return nil
	}

	ok := pb.filterDuplicate(args.OpID, Get, reply)
	if ok {
		DPrintf("Duplicate : reply.Err : %s\n", reply.Err)
		return nil
	}

	pb.doGet(args, reply)

	pb.recordOperation(args.OpID, reply)

	return nil
}

func (pb *PBServer) doPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	DPrintf("--- op : %s, key : %s, value : %s\n", args.Method, args.Key, args.Value)
	
	key, value := args.Key, args.Value
	method := args.Method
	if method == Put {
		pb.kvstore[key] = value
	} else if method == Append {
		pb.kvstore[key] += value
	} 
	reply.Err = OK

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	DPrintf("RPC : PutAppend : server %s\n", pb.me)
	
	if pb.me != pb.view.Primary {
		reply.Err = ErrWrongServer
		return nil
	}

	ok := pb.filterDuplicate(args.OpID, args.Method, reply)
	if ok {
		DPrintf("Duplicate : reply.Err : %s\n", reply.Err)
		return nil
	}

	xferafter :=  false
	if pb.view.Backup != "" {
		DPrintf("forward %s to backup %s\n", args.Method, pb.view.Backup);
		
		tries := 1 // tring again doesn't help in many cases
		for tries > 0 {
			ok := call(pb.view.Backup, "PBServer.BackupPutAppend", args, reply)
			if ok {
				if reply.Err == ErrWrongServer {
					return nil
				} 
				if reply.Err == ErrUninitServer {
					xferafter = true
				} 
				break
			}
			DPrintf("retry RPC BackupPutAppend %d ...\n", tries)
			tries--
		} 
		if tries == 0 {
			reply.Err = ErrWrongServer
			return nil
		}
	} 

	pb.doPutAppend(args, reply)
	pb.recordOperation(args.OpID, reply)

	if xferafter {
		pb.transferState(pb.view.Backup)
	}

	return nil
}

func (pb *PBServer) BackupPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	DPrintf("RPC : BackupPutAppend : server %s\n", pb.me)
	
	if pb.me != pb.view.Backup {
		reply.Err = ErrWrongServer
		return nil
	}

	if !pb.isInitialized() {
		reply.Err = ErrUninitServer
		return nil
	}

	ok := pb.filterDuplicate(args.OpID, args.Method, reply)
	if ok {
		DPrintf("Duplicate : reply.Err : %s\n", reply.Err)
		return nil
	}

	pb.doPutAppend(args, reply)
	pb.recordOperation(args.OpID, reply)

	return nil
}

func (pb *PBServer) transferState(target string) bool {
	if target != pb.view.Backup {
		return false
	}
	
	args := &InitStateArgs{pb.kvstore}
	var reply InitStateReply
	call(target, "PBServer.InitState", args, &reply)
	if reply.Err == OK {
		return true
	}
	return false
}

func (pb *PBServer) TransferState(
	args *TransferStateArgs, reply *TransferStateReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.transferState(args.Target)

	return nil
}

func (pb *PBServer) pingViewServer() *viewservice.View {
	viewno := uint(0)
	if pb.view != nil {
		viewno = pb.view.Viewnum
	}
	v, e := pb.vs.Ping(viewno)
	if e == nil {
		return &v
	}
	return nil
}

func (pb *PBServer) cleanUpFilters() {
	for op := range pb.filters {
		if pb.filters[op] <= 0 {
			delete(pb.filters, op)
			delete(pb.replies, op)
		} else {
			pb.filters[op]--
		}
	}
}

func (pb *PBServer) requestState(server string) {
	args := &TransferStateArgs{}
	args.Target = pb.me
	var reply TransferStateReply
	go call(server, "PBServer.TransferState", args, &reply)
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	view := pb.pingViewServer()
	if view != nil {
		if !pb.init {
			if pb.me == view.Primary {
				// fatal error
				// pb.kill()
			} else if pb.me == view.Backup {
				pb.requestState(view.Primary)
			}
		} 
		pb.view = view
	}

	pb.cleanUpFilters()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.kvstore = make(map[string]string)
	pb.filters = make(map[int64]int)
	pb.replies = make(map[int64]interface{})

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
