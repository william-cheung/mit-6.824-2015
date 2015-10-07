package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrDuplicate   = "ErrDuplicate"
	ErrWrongServer = "ErrWrongServer"
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
	Client   string
	Viewnum	 uint
	Method   string

	OpID     int64
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Client  string
	Viewnum uint
	OpID    int64
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
type InitKvsArgs struct {
	Kvstore map[string]string
}

type InitKvsReply struct {
	Err   Err
}


// utility funcs
func copy_GetReply(dst *GetReply, src *GetReply) {
	dst.Err = src.Err
	dst.Value = src.Value
}
func copy_PutAppendReply(dst *PutAppendReply, src *PutAppendReply) {
	dst.Err = src.Err
}
