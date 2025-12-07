package commit

import (
	"sync"
	"fmt"
)

type StoreItem struct {
	value	interface{}		// Stored val
	lock	sync.RWMutex	// Read-write lock for concurrency
}

type Server struct {
	mu				sync.Mutex				// Protects all server state
	store			map[string]*StoreItem	// Key-val storage
	transactions	map[int]*Transaction	// Active transactions (key: transaction ID)
	myKeys			map[string]struct{}		// Keys this server is responsible for
}

// Tracks all info about an ongoing transaction
type Transaction struct {
	tid         int
	operations	[]Operation			// Array of Get/Set operations
	state		TransactionState	// Current state in 3PC protocol
	locks		map[string]bool		// Held locks (true=exclusive/write, false=shared/read)
}

// Prepare handler
//
// This function should:
// 1. Attempt to obtain locks for the given transaction
// 2. If this succeeds, vote Yes
// 3. If this fails, release any obtained locks and vote No
func (sv *Server) Prepare(args *RPCArgs, reply *PrepareReply) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	fmt.Printf("Server Prepare START for TID=%d\n", args.Tid) // Debug
	
	tid := args.Tid
	t, exists := sv.transactions[tid]
	
	// Check relevance
	if !exists || len(t.operations) == 0 {
		reply.Relevant = false
		return
	}
	reply.Relevant = true
	
	// Already past Prepare phase
	if t.state != stateActive {
		reply.Vote = (t.state == statePrepared)
		return
	}
	
	// Attempt to acquire locks for all operations
	fmt.Printf("TID=%d attempting %d operations\n", tid, len(t.operations)) // Debug
    for _, op := range t.operations {
        fmt.Printf("TID=%d trying %s on key=%s\n", tid, op.OpType, op.Key) // Debug
		item := sv.store[op.Key]
		if item == nil {
			continue
		}
	
		isWrite := op.OpType == OpSet
		var acquired bool
		var lockType string // Debug VAR
	
		// Try to acquire appropriate lock
		if isWrite {
			lockType = "write"
			fmt.Printf("TID=%d trying %s on key=%s\n", tid, lockType, op.Key) // Debug
			acquired = item.lock.TryLock()
			if acquired {
				fmt.Printf("TID=%d ACQUIRED %s lock on %s\n", tid, lockType, op.Key) // Debug
				t.locks[op.Key] = true // Only add if acquired
			}
		} else {
			lockType = "read"
        	fmt.Printf("TID=%d trying %s on key=%s\n", tid, lockType, op.Key) // Debug
			acquired = item.lock.TryRLock()
			if acquired {
				fmt.Printf("TID=%d ACQUIRED %s lock on %s\n", tid, lockType, op.Key) // Debug
				t.locks[op.Key] = false // Only add if acquired
			}
		}
	
		if !acquired {
            fmt.Printf("TID=%d FAILED to acquire %s lock on %s\n", tid, lockType, op.Key) // Debug
			sv.rollbackTransaction(t)
			t.state = stateAborted
			reply.Vote = false
			return
		}
    }
    fmt.Printf("TID=%d PREPARED successfully\n", tid) // Debug
	
	t.state = statePrepared
	reply.Vote = true
}

// Releases all locks and resets transaction state
func (sv *Server) rollbackTransaction(t *Transaction) {
	fmt.Printf("ROLLBACK TID=%d releasing %d locks\n", t.tid, len(t.locks))
    for key, isWrite := range t.locks {
        item := sv.store[key]
        if item == nil {
            continue
        }
        fmt.Printf("Releasing lock on key=%s (write=%v)\n", key, isWrite) // Debug
        if isWrite {
            item.lock.Unlock()
        } else {
            item.lock.RUnlock()
        }
    }
    t.locks = make(map[string]bool)
}

// Abort handler
//
// This function should abort the given transaction
// Make sure to release any held locks
func (sv *Server) Abort(args *RPCArgs, reply *struct{}) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	
	tid := args.Tid
	if t, exists := sv.transactions[tid]; exists {
		sv.rollbackTransaction(t)
		delete(sv.transactions, tid)
		t.state = stateAborted
	}
}

// Query handler
//
// This function should reply with information about all known transactions
func (sv *Server) Query(args struct{}, reply *QueryReply) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	
	reply.Transactions = make(map[int]TransactionState)
	for tid, t := range sv.transactions {
		reply.Transactions[tid] = t.state
	}
}

// PreCommit handler
//
// This function should confirm that the server is ready to commit
// Hint: the protocol tells us to always just acknowledge preCommit,
// so there isn't too much to do here
func (sv *Server) PreCommit(args *RPCArgs, reply *struct{}) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	
	tid := args.Tid
	if t, exists := sv.transactions[tid]; exists && t.state == statePrepared {
		t.state = statePreCommitted
	}
}

// Commit handler
//
// This function should actually apply the logged operations
// Make sure to release any held locks
func (sv *Server) Commit(args *RPCArgs, reply *CommitReply) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	fmt.Printf("Server Commit START for TID=%d\n", args.Tid) // Debug

	tid := args.Tid
	t, exists := sv.transactions[tid]
    // If transaction is already committed or doesn't exist, return success
	if !exists || t.state == stateCommitted {
		return
	}
	
	if t.state != statePreCommitted {
		return // Not in a committable state
	}
	
	reply.ReadValues = make(map[string]interface{})
	
	// Apply ops to the store
	for _, op := range t.operations {
		item := sv.store[op.Key]
		if item == nil {
			continue
		}
		
		switch op.OpType {
		case OpSet:
			item.value = op.Value
		case OpGet:
			reply.ReadValues[op.Key] = item.value
		}
	}
	
	// Final cleanup
	sv.rollbackTransaction(t)
	delete(sv.transactions, tid)
	t.state = stateCommitted
	fmt.Printf("TID=%d COMMITTED with values: %v\n", tid, reply.ReadValues) // Debug
}

// Get
//
// This function should log a Get operation
func (sv *Server) Get(tid int, key string) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	
	// Ignore keys we're not responsible for
	if _, exists := sv.myKeys[key]; !exists {
		return
	}
	
	t := sv.getOrCreateTransaction(tid)
	t.operations = append(t.operations, Operation{
		OpType: OpGet,
		Key:  key,
	})
}

// Set
//
// This function should log a Set operation
func (sv *Server) Set(tid int, key string, value interface{}) {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	// Ignore keys we're not responsible for
	if _, exists := sv.myKeys[key]; !exists {
		return
	}
	
	t := sv.getOrCreateTransaction(tid)
	t.operations = append(t.operations, Operation{
		OpType: OpSet,
		Key:    key,
		Value:  value,
	})
}

// Retrieves or inits a transaction
func (sv *Server) getOrCreateTransaction(tid int) *Transaction {
	if t, exists := sv.transactions[tid]; exists {
		return t
	}
	
	// Init new transaction
	t := &Transaction{
		tid:    tid,
		state:	stateActive,
		locks:	make(map[string]bool),
	}
	sv.transactions[tid] = t
	return t
}

// Initialize new Server
//
// keys is a slice of the keys that this server is responsible for storing
func MakeServer(keys []string) *Server {
	sv := &Server{
		store:			make(map[string]*StoreItem),
		transactions:	make(map[int]*Transaction),
		myKeys:			make(map[string]struct{}),
	}
	
	// Init responsible keys
	for _, k := range keys {
		sv.myKeys[k] = struct{}{}
		if sv.store[k] == nil {
			sv.store[k] = &StoreItem{}
		}
	}
	return sv
}