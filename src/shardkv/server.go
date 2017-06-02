package shardkv

// import "shardmaster"
import (
	"bytes"
	"encoding/gob"
	"fmt"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		s := fmt.Sprintf(format, a...)
		log.Output(2, s)
	}
	return
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

const (
	PutAppendOP = iota
	GetOP
	ConfigOp
	PreConfigOp
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Kind          int
	Key           string
	Value         string
	Op            string
	Config        shardmaster.Config
	GetShardReply GetShardReply

	// my code
	CID int64
	RID int64

	ApplyCode int
}

const WrongGruopCode = 1

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	// need snapshoot
	db     map[string]string
	dup    map[int64]int64
	config []shardmaster.Config

	result map[int]chan Op

	mck *shardmaster.Clerk
}

func (kv *ShardKV) checkGroup(k string) bool {
	shard := key2shard(k)

	if len(kv.config) == 0 {
		return false
	}

	c := kv.config[len(kv.config)-1]
	return c.Shards[shard] == kv.gid
}

func (kv *ShardKV) isDup(cid int64, rid int64) bool {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	if cid == 0 {
		return false
	}

	v, ok := kv.dup[cid]

	if !ok {
		return false
	}

	return v >= rid
}

func (kv *ShardKV) handleApply() {
	for msg := range kv.applyCh {
		if msg.UseSnapshot {
			buffer := bytes.NewBuffer(msg.Snapshot)
			d := gob.NewDecoder(buffer)
			var index int
			var term int

			kv.mu.Lock()
			d.Decode(&index)
			d.Decode(&term)
			kv.db = make(map[string]string)
			kv.dup = make(map[int64]int64)
			d.Decode(&kv.db)
			d.Decode(&kv.dup)
			d.Decode(&kv.config)

			kv.mu.Unlock()
		} else {
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}
			kv.mu.Lock()
			if !kv.isDup(op.CID, op.RID) {
				kv.applyOp(&op)
			}

			// kv.mu.Lock()
			opChan, ok := kv.result[msg.Index]

			if ok {
				select {
				case <-opChan:
				default:
				}

				opChan <- op
			} else {
				opChan = make(chan Op, 1)
				opChan <- op
				kv.result[msg.Index] = opChan
			}
			maxraftstateTemp := kv.maxraftstate

			kv.mu.Unlock()

			if maxraftstateTemp != -1 && kv.rf.GetPersistSize() > maxraftstateTemp {
				buffer := new(bytes.Buffer)
				e := gob.NewEncoder(buffer)
				e.Encode(kv.db)
				e.Encode(kv.dup)
				e.Encode(kv.config)

				go kv.rf.StartSnapshot(buffer.Bytes(), msg.Index)
			}
			// kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) applyOp(op *Op) {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	if op.Kind == PutAppendOP || op.Kind == GetOP {
		if kv.checkGroup(op.Key) == false {
			op.ApplyCode = WrongGruopCode
			return
		}
	}

	switch op.Kind {
	case PutAppendOP:
		if op.Op == "Put" {
			kv.db[op.Key] = op.Value
		} else if op.Op == "Append" {
			kv.db[op.Key] += op.Value
		} else {
			panic(op.Op)
		}
	case GetOP:
	case ConfigOp:
		if op.Config.Num != len(kv.config) {
			break
		}

		for k, v := range op.GetShardReply.DB {
			kv.db[k] = v
		}
		for k, v := range op.GetShardReply.Dup {
			if v > kv.dup[k] {
				kv.dup[k] = v
			}
		}
		kv.config = append(kv.config, op.Config)
	case PreConfigOp:
	}

	kv.dup[op.CID] = op.RID
}

func (kv *ShardKV) appendOp(op Op) Err {
	// kv.mu.Lock()
	if op.CID == 0 {
		panic("0 cid")
	}
	kv.mu.Lock()
	if op.Kind != ConfigOp && op.Kind != PreConfigOp {
		if kv.checkGroup(op.Key) == false {
			kv.mu.Unlock()
			return ErrWrongGroup
		}
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		// kv.mu.Unlock()
		return ErrWrongLeader
	}
	kv.mu.Lock()
	resultChan, ok := kv.result[index]
	if !ok {
		resultChan = make(chan Op, 1)
		kv.result[index] = resultChan
	}
	kv.mu.Unlock()

	select {
	case respOp := <-resultChan:
		if respOp.RID == op.RID && respOp.CID == op.CID && respOp.ApplyCode == 0 {
			return OK
		} else if respOp.ApplyCode == WrongGruopCode {
			return ErrWrongGroup
		} else {
			return ErrWrongLeader
		}
	case <-time.NewTimer(time.Second).C:
		return ErrWrongLeader
	}

	return ErrWrongLeader
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {

	op := Op{
		Kind: GetOP,
		Key:  args.Key,
		CID:  args.CID,
		RID:  args.RID,
	}

	err := kv.appendOp(op)

	if err != OK {
		reply.WrongLeader = (err == ErrWrongLeader)
		reply.Err = err
		return
	}

	kv.mu.Lock()
	var hasKey bool
	reply.Value, hasKey = kv.db[args.Key]
	kv.mu.Unlock()
	if hasKey == false {
		reply.Err = ErrNoKey
	} else {
		reply.Err = OK
	}

	return
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	op := Op{
		Kind:  PutAppendOP,
		Key:   args.Key,
		Value: args.Value,
		Op:    args.Op,
		CID:   args.CID,
		RID:   args.RID,
	}

	err := kv.appendOp(op)

	if err != OK {
		reply.WrongLeader = (err == ErrWrongLeader)
		reply.Err = err
		return
	}

	reply.Err = OK

	return
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) {

	kv.mu.Lock()
	if len(kv.config)-1 < args.Config.Num {
		kv.mu.Unlock()
		reply.Err = ErrNotReady
		return
	}
	kv.mu.Unlock()

	op := Op{
		CID:    nrand(),
		Kind:   PreConfigOp,
		Config: args.Config,
	}

	err := kv.appendOp(op)
	if err != OK {
		reply.Err = err
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.DB = make(map[string]string)
	reply.Dup = make(map[int64]int64)
	for k, v := range kv.db {
		if key2shard(k) == args.Shard {
			reply.DB[k] = v
		}
	}
	for k, v := range kv.dup {
		reply.Dup[k] = v
	}

	reply.Err = OK
}

func (kv *ShardKV) tryReconfig(now shardmaster.Config) bool {

	kv.mu.Lock()

	if now.Num != len(kv.config) {
		kv.mu.Unlock()
		return false
	}

	var resp GetShardReply
	if len(kv.config) > 0 {
		last := kv.config[len(kv.config)-1]
		kv.mu.Unlock()
		for s := 0; s < shardmaster.NShards; s++ {
			if now.Shards[s] == kv.gid && (last.Shards[s] != kv.gid && last.Shards[s] != 0) {
				kv.mu.Lock()
				names := last.Groups[last.Shards[s]]
				kv.mu.Unlock()
				getShardOK := false

				for _, name := range names {
					srv := kv.make_end(name)
					var args GetShardArgs
					args.Config = now
					args.Shard = s

					var reply GetShardReply

					ok := srv.Call("ShardKV.GetShard", &args, &reply)
					if ok && reply.Err == OK {
						resp.Merge(&reply)
						getShardOK = true
						break
					}
				}
				if !getShardOK {
					return false
				}
			}
		}
	} else {
		kv.mu.Unlock()
	}
	// kv.mu.Unlock()

	op := Op{
		CID:           nrand(),
		Kind:          ConfigOp,
		Config:        now,
		GetShardReply: resp,
	}

	err := kv.appendOp(op)

	if err != OK {
		return false
	}

	return true
}

func (kv *ShardKV) fetchConfig() {
	kv.mu.Lock()
	nextNum := len(kv.config)
	kv.mu.Unlock()
	for _ = range time.Tick(time.Millisecond * 101) {
		kv.mu.Lock()
		if len(kv.config) > nextNum {
			nextNum = len(kv.config)
		}
		kv.mu.Unlock()
		c := kv.mck.Query(nextNum)
		if c.Num != nextNum {
			time.Sleep(time.Second)
			continue
		}

		for {
			kv.mu.Lock()
			if len(kv.config) > c.Num {
				nextNum++
				kv.mu.Unlock()
				break
			}
			kv.mu.Unlock()

			ok := kv.tryReconfig(c)
			if !ok {
				time.Sleep(time.Second)
			} else {
				nextNum++
				break
			}
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.dup = make(map[int64]int64)
	kv.result = make(map[int]chan Op)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.handleApply()
	go kv.fetchConfig()

	return kv
}
