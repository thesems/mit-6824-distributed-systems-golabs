package kvsrv

import (
	"log"
	"strings"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Request struct {
	uid   string
	reply string
}

type KVServer struct {
	mu           sync.Mutex
	data         map[string]string
	completed    map[string]*Request
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, found := kv.data[args.Key]
	if !found {
		reply.Value = ""
		return
	}

	reply.Value = value
    delete(kv.completed, args.Client)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

    req, found := kv.completed[args.Client]
    if found {
       if req.uid == args.Uid {
            return
        }
    }

    kv.completed[args.Client] = &Request{uid: args.Uid, reply: ""}
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

    req, found := kv.completed[args.Client]
    if found {
       if req.uid == args.Uid {
			reply.Value = req.reply
            return
        }
    }

	value, found := kv.data[args.Key]
	if !found {
		kv.data[args.Key] = ""
	}

	reply.Value = strings.Clone(value)
    kv.completed[args.Client] = &Request{uid: args.Uid, reply: strings.Clone(value)}
	kv.data[args.Key] += args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.completed = make(map[string]*Request)
	return kv
}
