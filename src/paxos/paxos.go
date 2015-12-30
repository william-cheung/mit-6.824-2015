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

// for debugging
const Debug = 0
func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
}

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
	doneSeqs   []int                 // doneSeqs[i] is highest seq passed to Done() 
	                                 // on peer i known to this peer

	maxSeqSeen int                   // highest seq known to this peer

	values     map[int]interface{}   // decided value of each instance	

	accpState  map[int]State         // acceptor state of each instance
}

// acceptor state
type State struct { 
	prepProposal int            // highest prepare seen 
	accpProposal int            // highest accept seen         				
	accpValue    interface{}    // value of highest accepted proposal
}

func (px *Paxos) lock() {
	//DPrintf("try lock %s\n", px.self());
	px.mu.Lock();
	//DPrintf("lock %s\n", px.self());
}

func (px *Paxos) unlock() {
	//DPrintf("unlock %s\n", px.self());
	px.mu.Unlock();
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	if seq < px.Min() {
		return
	}
	
	px.mu.Lock()
	px.updateMaxSeqSeen(seq)
	px.mu.Unlock()

	go px.propose(seq, v)
}

func (px *Paxos) updateMaxSeqSeen(seq int) {
	if seq > px.maxSeqSeen {
		px.maxSeqSeen = seq
	}
}

func (px *Paxos) isDecided(seq int) bool {
	decided, _ := px.Status(seq)
	return decided == Decided
}
 
func (px *Paxos) propose(seq int, v interface{}) {	
	for !px.isDecided(seq) {
		
		// choose n, unique and higher than any proposal number seen
		n := px.chooseProposalNumber(seq)
		
		// using channels to synchronize and avoid deadlock
		chan1 := make(chan bool)
		chan2 := make(chan interface{})
		
		// boadcast n to all peers
		go px.sendPrepareToAll(seq, n, v, chan1, chan2)
		
		// if ok is true, we received prepare_ok from majority servers
		ok := <- chan1
		if !ok {
			continue
		}
		v1 := <- chan2 // get the value choosed in the prepare phase
			
		chan3 := make(chan bool)
		// send n, v1 to all peers
		go px.sendAcceptToAll(seq, n, v1, chan3)

		ok = <- chan3
		if ok { // we reach agreement on value v1
			go px.sendDecidedToAll(seq, v1)
			break;
		}
	}
}

func (px *Paxos) chooseProposalNumber(seq int) int {	
	px.mu.Lock()     
	n := px.accpState[seq].prepProposal
	px.mu.Unlock()
	return n + 1
}

func (px *Paxos) sendPrepareToAll(seq int, 
	n int, v interface{}, chan1 chan<- bool, chan2 chan<- interface{}) {
	cntok, maxna := 0, 0
	var v1 interface{}
	for _, peer := range px.peers {
		na, va, ok := px.prepare(peer, seq, n)
		if ok {
			if na > maxna {
				maxna = na
				v1 = va
			}
			cntok++
		} else {
			// update highest proposal number we seen
			if (px.updateProposalNumber(seq, na)) {
				chan1 <- false
				return
			}
		}
	}
	if cntok > len(px.peers) / 2 {
		if maxna == 0 {
			v1 = v
		}
		chan1 <- true 
		chan2 <- v1
	} else {
		chan1 <- false
	}
}

func (px *Paxos) updateProposalNumber(seq int, n int) bool {
	px.mu.Lock()
	defer px.mu.Unlock()
	
	state := px.accpState[seq]
	if (n > state.prepProposal) {
		state.prepProposal = n
		px.accpState[seq] = state
		return true
	}
	return false
}


func (px *Paxos) self() string {
	return px.peers[px.me]
}

func (px *Paxos) isSelf(peer string) bool {
	return peer == px.self()
}

func (px *Paxos) prepare(peer string, seq int, n int) (int, interface{}, bool) {
	if px.isSelf(peer) {
		return px.prepareHandler(seq, n)
	} else {
		args := &PrepareArgs{seq, n}
		var reply PrepareReply
		ok := call(peer, "Paxos.Prepare", args, &reply)
		if !ok {
			return 0, nil, false
		} else if reply.Err != OK { 
			return reply.Proposal, nil, false
		}
		return reply.Proposal, reply.Value, true
	}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	DPrintf("RPC Prepare : inst %d : prop %d : serv %s\n", 
		args.Instance, args.Proposal, px.self())
	n, v, ok := px.prepareHandler(args.Instance, args.Proposal)
	if ok {
		reply.Err = OK
		reply.Proposal, reply.Value = n, v
	} else {
		reply.Err = ErrRejected
		reply.Proposal = n // reply the highest proposal number we seen to the client
	}
	return nil
}

func (px *Paxos) prepareHandler(seq int, n int) (int, interface{}, bool) {
	px.updateMaxSeqSeen(seq)
	px.mu.Lock()
	defer px.mu.Unlock()
	state := px.accpState[seq]
	if n > state.prepProposal {
		state.prepProposal = n
		n, v := state.accpProposal, state.accpValue
		px.accpState[seq] = state
		return n, v, true
	} else {
		return state.prepProposal, nil, false
	}
}

func (px *Paxos) sendAcceptToAll(seq int, n int, v interface{}, okch chan<- bool) {
	cntok := 0
	for _, peer := range px.peers {
		ok := px.accept(peer, seq, n, v)
		if ok {
			cntok++
		}
	}
	if cntok > len(px.peers) / 2 {
		okch <- true
	}
	okch <- false
}

func (px *Paxos) accept(peer string, seq int, n int, v interface{}) bool {
	if px.isSelf(peer) {
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
	DPrintf("RPC Accept : inst %d : prop %d : value %v : serv %s\n", 
		args.Instance, args.Proposal, args.Value, px.self())
	
	ok := px.acceptHandler(args.Instance, args.Proposal, args.Value)
	if ok {
		reply.Err = OK
	} else {
		reply.Err = ErrRejected
	}
	return nil
}

func (px *Paxos) acceptHandler(seq int, n int, v interface{}) bool {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.updateMaxSeqSeen(seq)
	state := px.accpState[seq]
	if n >= state.prepProposal {
		state.prepProposal = n
		state.accpProposal = n
		state.accpValue = v
		px.accpState[seq] = state
		return true
	} 
	return false
}

func (px *Paxos) sendDecidedToAll(seq int, v interface{}) {
	//px.status[seq] = Decided
	for _, peer := range px.peers {
		px.decided(peer, seq, v)
	}
}

func (px *Paxos) decided(peer string, seq int, v interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.isSelf(peer) {
		px.values[seq] = v
	} else {
		args := &DecidedArgs{px.me, px.doneSeqs[px.me], seq, v}
		var reply DecidedReply
		go call(peer, "Paxos.Decided", args, &reply)
	}
}

func (px *Paxos) Decided(args *DecidedArgs, reply *DecidedReply) error {
	DPrintf("RPC Decided : inst %d : value %v : serv %s\n", 
		args.Instance, args.Value, px.self())
	px.mu.Lock()
	px.values[args.Instance] = args.Value
	if px.doneSeqs[args.Sender] < args.DoneIns {
		px.doneSeqs[args.Sender] = args.DoneIns
	}
	px.mu.Unlock()
	return nil
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	if px.doneSeqs[px.me] < seq {
		px.doneSeqs[px.me] = seq
	}
	px.doMemShrink()
	px.mu.Unlock()
}


func (px *Paxos) doMemShrink() int {
	// fmt.Printf("%v\n", px.doneSeqs)
	mm := px.doneSeqs[px.me]
	for i := range px.doneSeqs {
		if mm > px.doneSeqs[i] {
			mm = px.doneSeqs[i]
		}
	}
	
	for seq := range px.values {
		if seq <= mm {
			delete(px.values, seq)
			delete(px.accpState, seq)
		}
	}
	return mm
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	return px.maxSeqSeen
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
	px.mu.Lock()
	defer px.mu.Unlock()
	
	return px.doMemShrink() + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	if seq < px.Min() {
		return Forgotten, nil
	} 

	px.mu.Lock()
	defer px.mu.Unlock()

	v, ok := px.values[seq]
	if ok {
		return Decided, v
	}
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
	npeers := len(px.peers)
	px.doneSeqs = make([]int, npeers)
	for i := 0; i < npeers; i++ {
		px.doneSeqs[i] = -1
	}
	px.maxSeqSeen = -1
	
	px.values = make(map[int]interface{})
	px.accpState = make(map[int]State)

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
