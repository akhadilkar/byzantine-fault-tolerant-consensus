# Byzantine Fault-Tolerant Banking System

A production-grade implementation of the Linear PBFT (Practical Byzantine Fault Tolerance) consensus protocol for distributed banking applications, tolerating up to f=2 Byzantine (malicious) nodes in a 7-replica cluster.

## 🎯 Key Features

- **Byzantine Fault Tolerance**: Tolerates arbitrary malicious behavior from up to 2 nodes (3f+1 architecture)
- **Linear Communication Complexity**: O(n) message complexity vs O(n²) in classical PBFT
- **Optimistic Phase Reduction**: Eliminates commit phase when all replicas respond quickly (bonus optimization)
- **Cryptographic Security**: Ed25519 signatures for all messages
- **View-Change Protocol**: Automatic leader failover with safety guarantees
- **State Machine Replication**: Consistent account balances across all replicas

## 🏗️ Architecture
```
Client → Primary (Leader) → Replicas → Commit → Execute
         ↓ PrePrepare
         ↓ PrepareVote → PrepareBundle (quorum)
         ↓ CommitVote → CommitBundle (quorum)
         ↓ Execute (in sequence order)
```

### Byzantine Attacks Handled
- ✅ **Invalid Signatures**: Malicious replicas sending unsigned/corrupted messages
- ✅ **Crash Failures**: Nodes that stop responding
- ✅ **Equivocation**: Leader sending conflicting messages to different replicas
- ✅ **In-Dark Attacks**: Selective message dropping to specific nodes
- ✅ **Timing Attacks**: Intentional delays to trigger view changes

## 🚀 Quick Start

### Prerequisites
- Go 1.21+
- 7 terminal windows (or tmux/screen)

### Build
```bash
go build -o pbft
```

### Run Test Suite
```bash
# Terminal 1: Start driver
./pbft -role=driver -port=9000 -n=7 -csv=CSE535-F25-Project-2-Testcases.csv

# Terminal 2-8: Start 7 nodes
./pbft -role=node -id=n1 -port=8001 -driver=127.0.0.1:9000 -optimistic=true
./pbft -role=node -id=n2 -port=8002 -driver=127.0.0.1:9000 -optimistic=true
# ... (repeat for n3-n7)
```

### Run Single Transaction
```bash
# In driver terminal:
> next        # Execute test set 1
> db          # View balances across all replicas
> log 1       # View consensus log
> slot 1      # Detailed view of slot 1
```

## 📊 Test Coverage

- **10 Test Sets**: Covering normal operation, single/multiple Byzantine failures, view changes
- **50+ Transactions**: Validated across all fault scenarios
- **100% Consistency**: All honest replicas maintain identical state

## 🔧 Key Implementation Details

### Normal Case Operation
```go
// Linear PBFT: Collector-based prepare/commit
1. Client → Leader: ClientRequest
2. Leader → All: PrePrepare(seq, digest, op)
3. Replicas → Leader: PrepareVote(seq, digest, sig)
4. Leader → All: PrepareBundle(2f+1 votes)  // Quorum
5. Replicas → Leader: CommitVote(seq, digest, sig)
6. Leader → All: CommitBundle(2f+1 votes)
7. Execute when committed
```

### Optimistic Phase Reduction
```go
// When all n=7 votes arrive quickly:
1-3. Same as above
4. Leader → All: OptimisticCommitBundle(7 votes)
5. Skip commit phase entirely → Execute
// Result: 33% faster consensus
```

### View Change Protocol
```go
// Triggered by: leader timeout, equivocation, or 2f+1 view-change msgs
1. Replicas → All: ViewChange(new_view, prepared_ops)
2. New Leader: Collects 2f+1 ViewChange messages
3. New Leader → All: NewView(view, VCs)
4. Resume normal operation in new view
```

## 📁 Project Structure
```
.
├── main.go              # Entry point, CLI flags
├── node.go              # Node struct, REPL, lifecycle
├── rpc_handlers.go      # PBFT protocol handlers (PrePrepare, Prepare, Commit)
├── structs.go           # Message types, Entry, Op
├── transport.go         # HTTP transport, crypto helpers
├── driver.go            # Test driver, CSV executor
├── rpc_client.go        # Client implementation
├── go.mod               # Dependencies
└── CSE535-F25-Project-2-Testcases.csv  # Test scenarios
```

## 🧪 Example Test Output
```
> log 1
LOG @ 127.0.0.1:8001:
  slot 1: view=0 status=executed ✨ OPTIMISTIC PATH (all 7 votes)
    op: A -> B amt=1 tau=1 c=A
    votes=[1 2 3 4 5 6 7] (7) - optimistic commit

> db
[driver] DB @ 127.0.0.1:8001: A=9 B=11 C=10 D=10 E=10 ...
[driver] DB @ 127.0.0.1:8002: A=9 B=11 C=10 D=10 E=10 ...  ✅ Consistent!
```

## 📚 References

- [PBFT Paper](http://pmg.csail.mit.edu/papers/osdi99.pdf) - Castro & Liskov, 1999
- [Linear PBFT](https://decentralizedthoughts.github.io/2022-06-27-linear-pbft/) - Optimized communication pattern
- Course: CSE 535 Distributed Systems, Stony Brook University

## 👤 Author

**Atharva Khadilkar**  
MS Computer Science, Stony Brook University  
[LinkedIn](https://linkedin.com/in/atharva-khadilkar) | [GitHub](https://github.com/akhadilkar)

## 📄 License

MIT License - see LICENSE file for details
