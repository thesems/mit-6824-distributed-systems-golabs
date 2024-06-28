package kvsrv

import (
	"crypto/rand"
	"github.com/google/uuid"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
    clientID string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
    ck.clientID = uuid.New().String()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key, ck.clientID}

	var reply GetReply
	ok := false
	for !ok {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
		// if !ok {
		// 	fmt.Printf("Failed to perform Get: %s\n", key)
		// }
	}
	// fmt.Printf("Succeeded to perform Get: %s\n", key)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
    uid := uuid.New().String()[:5]
	args := PutAppendArgs{key, value, uid, ck.clientID}
	var reply PutAppendReply
	ok := false
	for !ok {
		ok = ck.server.Call("KVServer."+op, &args, &reply)
		// if !ok {
		// 	fmt.Printf("Failed to perform PutAppend: %s\n", key)
		// }
	}
	// fmt.Printf("Succeeded to perform PutAppend: %s\n", key)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
