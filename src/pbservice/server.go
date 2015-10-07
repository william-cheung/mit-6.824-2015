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

const Debug = 1
func debug_printf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
}

const FilterLife = int(10000 * time.Millisecond / viewservice.PingInterval)    // 10 sec

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	init       bool                // initialized
	view       *viewservice.View
	kvstore    map[string]string
	filters    map[int64]int
	replies    map[int64]interface{}
}

func (pb *PBServer) get_view() bool {
	v, ok := pb.vs.Get()
	if ok {
		pb.view = &v
		return true
	}
	return false
}

func (pb *PBServer) get_viewno() uint {
	return pb.view.Viewnum
}

func (pb *PBServer) get_primary() string {
	return pb.view.Primary
}

func (pb *PBServer) get_backup() string {
	return pb.view.Backup
}

func (pb *PBServer) init_backup() bool {
	args := &InitKvsArgs{pb.kvstore}
	var reply InitKvsReply
	call(pb.get_backup(), "PBServer.InitKvs", args, &reply)
	if reply.Err == OK {
		return true
	}
	return false
}


func (pb *PBServer) filter_duplicate(opid int64, method string, preply interface{}) bool {
	_, ok := pb.filters[opid]
	if !ok {
		return false
	}
	
	debug_printf("duplicate Append detected, opid : %v, op : %s\n", opid, method)
	rp, ok2 := pb.replies[opid]
	if !ok2 {
		return false
	}

	// bad design, because there may be 
	if method == Get {
		rpptr, ok1 := preply.(**GetReply)
		saved, ok2 := rp.(*GetReply) 
		if ok1 && ok2 {
			reply := *rpptr
			copy_GetReply(reply, saved)
			return true
		}
	} else if method == Put || method == Append {
		rpptr, ok1 := preply.(**PutAppendReply)
		saved, ok2 := rp.(*PutAppendReply)
		if ok1 && ok2 {
			reply := *rpptr
			copy_PutAppendReply(reply, saved)
			return true
		}
	}
	return false
}

func (pb *PBServer) record_operation(opid int64, reply interface{}) {
	pb.filters[opid] = FilterLife
	pb.replies[opid] = reply
}

func (pb *PBServer) aux_kvs_op(client string, viewno uint, op string) (errx Err, cont bool) {
	debug_printf("- RPC %s : viewno %d, client %s, server %s\n", op, viewno, client, pb.me);
	cont = true
	if client != "" && client == pb.get_backup() {
		if viewno < pb.get_viewno() { // if the client(pbserver) has a wrong view
			errx = ErrWrongServer
			cont = false
		} else { // we have a wrong view
			if !pb.get_view() {
				// fatal error
			}
		}
	} else if (client == pb.get_primary()) {
		if !pb.init {
			debug_printf("uninitialized backup : %s\n", pb.me)
			errx = ErrUninitServer
			cont = false
		}
	}
	return
}

func (pb *PBServer) InitKvs(args *InitKvsArgs, reply *InitKvsReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	
	debug_printf("- RPC InitKvs, server %s\n", pb.me)
	
	if pb.init {
		debug_printf("duplicated backup init operation detected")
		return nil
	}
	
	pb.kvstore = args.Kvstore
	pb.init = true
	reply.Err = OK
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	
	client, vnc := args.Client, args.Viewnum
	errx, cont := pb.aux_kvs_op(client, vnc, "Get")
	if !cont {
		reply.Err = errx
		return nil
	}

	// if we are primary, forward the request to backup
	if pb.me == pb.get_primary() && pb.get_backup() != "" {
		args.Client, args.Viewnum = pb.me, pb.get_viewno()
		// suppose that backup always holds same data with primary
		debug_printf("forward the request to backup %s\n", pb.get_backup());
		call(pb.get_backup(), "PBServer.Get", args, reply)
		// if we have a wrong view, then get view from the ViewServer 
		if reply.Err == ErrWrongServer { 
			if !pb.get_view() {
				// fatal error
			}
		} 
		if reply.Err == ErrUninitServer {
			pb.init_backup()
		} else {
			return nil
		}
	} 

	debug_printf("--- key : %s\n", args.Key)
	
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

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	ok := pb.filter_duplicate(args.OpID, args.Method, &reply)
	if ok {
		debug_printf("Duplicate : reply.Err : %s\n", reply.Err)
		return nil
	}

	errx, cont := pb.aux_kvs_op(args.Client, args.Viewnum, args.Method)
	if !cont {
		reply.Err = errx
		return nil
	}

	initb := false
	// if we are primary, forward the request to backup
	if pb.me == pb.get_primary() && pb.get_backup() != "" {
		args.Client, args.Viewnum = pb.me, pb.get_viewno()
		// suppose that backup always holds same data with primary
		debug_printf("forward the request to backup %s\n", pb.get_backup());
		call(pb.get_backup(), "PBServer.PutAppend", args, reply)
		// if we have a wrong view, then get view from the ViewServer 
		if reply.Err == ErrWrongServer {
			if !pb.get_view() {
				// fatal error
			}
			return nil
		} 
		if reply.Err == ErrUninitServer {
			initb = true
		} 
	} 

	debug_printf("--- key : %s, value : %s\n", args.Key, args.Value)
	
	key, value := args.Key, args.Value
	method, opid := args.Method, args.OpID
	if method == Put {
		pb.kvstore[key] = value
	} else if method == Append {
		pb.kvstore[key] += value
	} 
	reply.Err = OK

	if initb {
		pb.init_backup()
	}	
	
	pb.record_operation(opid, reply)

	return nil
}


func (pb *PBServer) do_ping() {
	viewno := uint(0)
	if pb.view != nil {
		viewno = pb.view.Viewnum
	}
	v, e := pb.vs.Ping(viewno)
	if e == nil {
		pb.view = &v
	}
}

func (pb *PBServer) clean_filters() {
	for op := range pb.filters {
		if pb.filters[op] <= 0 {
			delete(pb.filters, op)
			delete(pb.replies, op)
		} else {
			pb.filters[op]--
		}
	}
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
	
	pb.do_ping()
	pb.clean_filters()
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
