package paxos

import "fmt"
import "net"
import "net/rpc"
import "syscall"

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}


const (
	OK          = "OK"
	ErrRejected = "ErrRejected"
)

type Err string

type PrepareArgs struct {
	Instance int
	Proposal int
}

type PrepareReply struct {
	Err      Err
	Instance int
	Proposal int
	Value    interface{}
}

type AcceptArgs struct {
	Instance int
	Proposal int
	Value    interface{}
}

type AcceptReply struct {
	Err      Err
}

type DecidedArgs struct {
	Sender   int
	DoneIns  int

	Instance int
	Value    interface{}
}

type DecidedReply struct {
}

