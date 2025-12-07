package commit

// ------------------------------------------
//                  COMMON
// ------------------------------------------

type TransactionState int
type OperationType int	// Differentiates between Get & Set ops

const (
	OpGet		OperationType = iota	// Read op
	OpSet								// Write op

	stateActive TransactionState = iota	// Default state
	statePrepared						// Locked, ready for PreCommit
	statePreCommitted					// PreCommited sent & acknowledged statePreCommitted  
	stateCommitted						// Transaction completed
	stateAborted						// Transaction aborted
)

// Represents a single key-val op in a transaction
type Operation struct {
	OpType	OperationType	// Type of op (Get/Set)
	Key		string			// The key getting accessed
	Value  	interface{}		// The val for Set ops
}

// Server's response to Prepare RPC
type PrepareReply struct {
	Vote     bool	// T -> server votes to commit
	Relevant bool	// T -> server has ops for this transaction
}

// Results of Commit RPC
type CommitReply struct {
	ReadValues	map[string]interface{}	// Collected results of Get ops
}

// Transaction state info for coordinator recovery
type QueryReply struct {
	Transactions	map[int]TransactionState	// Map of TID to transaction state
}

// Common args struct because RPCs generally have the transaction ID as their only argument
type RPCArgs struct {
	Tid int
}

func (ot OperationType) String() string {
	switch ot {
	case OpGet:
		return "Get"
	case OpSet:
		return "Set"
	default:
		return "Unknown"
	}
}