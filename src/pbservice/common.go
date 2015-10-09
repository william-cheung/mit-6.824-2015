package pbservice

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongServer  = "ErrWrongServer"
	ErrUninitServer = "ErrUninitServer"
)

type Err string

const (
	Get       = "Get"
	Put       = "Put"
	Append    = "Append"
)

//type Method string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	
	// You'll have to add definitions here.
	OpID     int64
	Method   string

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	
	// You'll have to add definitions here.
	OpID    int64
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.

/*
type Reply interface {
	GetErr() Err
	SetErr(err Err)
}

func (rp *GetReply) GetErr() Err {
	return rp.Err
}

func (rp *GetReply) SetErr(err Err) {
	rp.Err = err
}

func (rp *PutAppendReply) GetErr() Err {
	return rp.Err
}

func (rp *PutAppendReply) SetErr(err Err) {
	rp.Err = err
}
*/

type InitStateArgs struct {
	State map[string]string
}

type InitStateReply struct {
	Err   Err
}

type TransferStateArgs struct {
	Target string
}

type TransferStateReply struct {
}

// Utility funcs
func copyGetReply(dst *GetReply, src *GetReply) {
	dst.Err = src.Err
	dst.Value = src.Value
}

func copyPutAppendReply(dst *PutAppendReply, src *PutAppendReply) {
	dst.Err = src.Err
}
