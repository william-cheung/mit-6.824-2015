package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	status     map[int]Fate          // status of each instance
	values     map[int]interface{}   // decided value of each instance	

	accpState   map[int]State         // acceptor state of each instance
}

type State struct {
	prepProposal int
	accpProposal int
	accpValue    interface{}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	if seq < px.Min() { return }
	go propose(seq, v)
}

func (px *Paxos) propose(seq int, v interface{}) {
	_, fate := px.Status(seq) 
	while fate != Decided {
		n := px.chooseProposalNumber(seq)
		v1, ok := px.sendPrepareToAll(seq, n, v)
		if !ok {
			continue
		}
		ok = px.sendAcceptToAll(seq, n, v1)
		if ok {
			px.sendDecidedToAll(seq, v1)
			break;
		}
	}
}

func (px *Paxos) chooseProposalNumber(seq int) int {
	n := px.accpState[seq].prepProposal
	return n + 1
}

func (px *Paxos) sendPrepareToAll(seq int, 
	n int, v interface{}) (interface{}, bool) {
	cntok, maxna := 0
	var v1 interface{}
	for _, peer := range px.peers {
		na, va, ok := px.prepare(peer, seq, n)
		if ok {
			if na > maxna {
				v1 = va
			}
			cntok++
		}
	}
	if cntok > (len(px.peers) + 1) / 2 {
		if maxna == 0 {
			v1 = v
		}
		return v1, true
	}
	return nil, false
}

func (px *Paxos) prepare(peer string, seq int, n int) 
	(int, interface{}, bool) {
	if peer == px.me {
		return px.prepareHandler(seq, n)
	} else {
		args := &PrepareArgs{seq, n}
		var reply PrepareReply
		ok := call(peer, "Paxos.Prepare", args, &reply)
		if !ok || reply.Err != OK {
			return 0, nil, false
		}
		return reply.Proposal, reply.Value, true
	}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	n, v, ok := px.prepareHandler(args.Instance, args.Proposal)
	if ok {
		reply.Err = OK
		reply.Proposal, reply.Value = n, v
	} else {
		reply.Err = ErrRejected
	}
	return nil
}

func (px &Paxos) prepareHandler(seq int, n int) (int, interface{}, bool) {
	state := &px.accpState[seq]
	if n > state.prepProposal {
		state.prepProposal = n
		n, v := state.accpProposal, state.accpValue
		return n, v, true
	} else {
		return 0, nil, false
	}
}

func (px *Paxos) sendAcceptToAll(seq int, n int, v interface{}) bool {
	cntok := 0
	for _, peer := range px.peers {
		ok := px.accept(peer, seq, n, v)
		if ok {
			cntok++
		}
	}
	if cntok > (len(px.peers) + 1) / 2 {
		return true
	}
	return false
}

func (px *Paxos) accept(peer string, seq int, n int, v interface{}) bool {
	if peer == px.me {
		return px.acceptHandler(seq, n, v)
	} else {
		args := AcceptArgs{seq, n, v}
		var reply AcceptReply
		ok := call(peer, "Paxos.Accept", args, &reply)
		if !ok || reply.Err != OK {
			return false
		}
		return true
	}
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	ok := px.accpetHandler(args.Instance, args.Proposal, args.Value)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrRejected
	}
	return nil
}

func (px *Paxos) acceptHandler(seq int, n int, v interface{}) bool {
	state := &px.accpState[seq]
	if n >= state.prepProposal {
		state.prepProposal = n
		state.accpProposal = n
		state.accpValue = v
		return true
	} 
	return false
}

func (px *Paxos) sendDecidedToAll(seq int, v interface{}) {
	for _, peer := range px.peers {
		//px.decided(peer, seq, v)
	}
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return 0
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	return Pending, nil
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me


	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
