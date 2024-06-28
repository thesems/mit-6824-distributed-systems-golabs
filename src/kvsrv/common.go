package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Uid   string
    Client string
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
    Client string
}

type GetReply struct {
	Value string
}
