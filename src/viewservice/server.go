package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

//import "container/list"
//import "errors"

const Debug = 0
func debug_printf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
}

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	view     *View                 // current view				
	newv     *View                 // new view
	packed   bool                  // is current view acked by the primary
	pttl     int                   // ttl of current primary
	bttl     int                   // ttl of current backup 
	svrset   map[string]int        // extra servers, server address -> ttl
}

func create_view(viewno uint, primary string, backup string) (view *View) {
	view = new(View)
	view.Viewnum = viewno
	view.Primary = primary
	view.Backup = backup
	return
}

func (vs *ViewServer) is_primary_dead() bool {
	return vs.pttl <= 0
}

func (vs *ViewServer) is_backup_dead() bool {
	return vs.bttl <= 0
}

func (vs *ViewServer) print_view() {
	if vs.view == nil {
		debug_printf("no view in the view server\n");
	} else {
		debug_printf("viewno  %d\n", vs.view.Viewnum)
		debug_printf("primary %s\n", vs.view.Primary)
		debug_printf("backup  %s\n", vs.view.Backup)
	}
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	client := args.Me
	viewno := args.Viewnum
	debug_printf("RPC : viewno %d, client %s\n", viewno, client);
	
	vs.print_view()
	
	if viewno == 0 {
		if vs.view == nil {
			// first server, let it be the primary
			vs.view = create_view(1, client, "")
		} else {
			// restarted p is treated as dead
			if client == vs.view.Primary { 
				debug_printf("primary was restarted\n");
				vs.pttl = 0;
				if (vs.packed && vs.switch_to_new_view()) {
					vs.packed = false
				}
			} 
			if client != vs.view.Backup {
				// extra servers
				vs.svrset[client] = DeadPings
			}
		}
	} else {
		if client == vs.view.Primary {
			if viewno == vs.view.Viewnum {
				if (vs.do_view_switch()) {
					//debug_printf("switch to new view in RPC Ping\n");
					vs.packed = false
				} else {
					debug_printf("primary Acked the %d-th view\n", viewno);
					vs.packed = true
				}
			}
		}
	}

	if client == vs.view.Primary {
		vs.pttl = DeadPings
	} else if client == vs.view.Backup {
		vs.bttl = DeadPings
	} else {
		vs.svrset[client] = DeadPings
	}
	
	reply.View = *vs.view
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.view == nil {
		//return errors.New("ViewServer : RPC Get error : no view created yet")
		reply.View = *create_view(0, "", "")
	} else {
		reply.View = *vs.view
	}	
	return nil
}

func (vs *ViewServer) update_newv(primary string, backup string) {
	if vs.view == nil { return }
	if vs.newv == nil {
		vs.newv = create_view(vs.view.Viewnum + 1, primary, backup)
	} else {
		vs.newv.Primary = primary
		vs.newv.Backup = backup
	}
}

func get_and_del(m *map[string]int) string {
	for elem := range *m {
		delete(*m, elem)
		debug_printf("select server : %s\n", elem);
		return elem
	}
	return ""
}

func (vs *ViewServer) switch_to_new_view() bool {
	if vs.view.Backup == "" && len(vs.svrset) == 0 {
		return false
	}
	if !vs.is_primary_dead() && vs.is_backup_dead() {
		vs.update_newv(
			vs.view.Primary, get_and_del(&vs.svrset))
	} else if vs.is_primary_dead() && !vs.is_backup_dead() {
		vs.update_newv(
			vs.view.Backup, get_and_del(&vs.svrset))
	} else if vs.is_primary_dead() && vs.is_backup_dead() {
		// uninitialized servers cannot be promoted to primary
		vs.update_newv("", "")
	}
	return vs.do_view_switch()
}

func (vs *ViewServer) do_view_switch() bool {
	if vs.newv != nil {
		vs.view, vs.newv = vs.newv, nil
		return true
	}
	return false
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	
	if vs.view == nil { return }

	for server := range vs.svrset {
		if vs.svrset[server] <= 0 {
			delete(vs.svrset, server)
		} else {
			vs.svrset[server]--
		}
	}	
	
	if (vs.packed && vs.switch_to_new_view()) {
		vs.packed = false
	}

	if vs.view.Primary == "" { vs.pttl = 0 }
	if vs.view.Backup  == "" { vs.bttl = 0 }
	
	if vs.pttl > 0 { vs.pttl-- }
	if vs.bttl > 0 { vs.bttl-- }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.svrset = make(map[string] int)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
