package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Node struct {
	ID    string
	NumID int
	Addr  string

	DriverAddr string

	Replicas map[int]string
	Infos    []ReplicaInfo

	view      View
	primaryID int
	seqNext   int
	execNext  int

	logMu sync.Mutex
	log   map[int]*Entry

	bankMu sync.Mutex
	bank   map[string]int64
	dedup  map[string]map[int64]bool

	keys         NodeKeys
	httpSrv      *http.Server
	client       *http.Client
	start        time.Time
	malicious    bool
	malType      string
	dropRate     float64
	delayMs      int
	vcMu         sync.Mutex
	vcAcks       map[View]map[int]bool
	lastProgress time.Time
	vcTimeoutMs  int
	lastLeaderOK time.Time

	nvHistory []NewViewMsg

	effectiveN int

	isFailed bool

	muLinks   sync.Mutex
	linkRules map[string]linkRule
	lastTau   map[string]int64

	outDrop   map[string]bool
	outDelay  map[string]time.Duration
	linkStats map[string]*LinkStats

	byzCrash         bool
	byzEquivocate    bool
	byzGoodSign      bool
	byzTiming        bool
	byzTimingDelayMs int

	// Optimistic phase reduction
	optimisticMode      bool
	optimisticTimeoutMs int

	// Track pending bundles for optimistic path
	//pendingBundleMu     sync.Mutex
	pendingBundleTimers map[int]*time.Timer // seq -> timer
}
type linkRule struct {
	dark  bool
	delay time.Duration
}
type LinkStats struct {
	Dropped uint64
	Delayed uint64
}

var allowWhenFailed = map[string]bool{
	"/status":  true,
	"/db":      true,
	"/leader":  false,
	"/ping":    true,
	"/fail":    true,
	"/recover": true,
}

var DefaultAccounts = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}

const DefaultInitBalance int64 = 10

func NewNode(id, addr, driver string) *Node {
	k := keysFromAddr(addr)
	n := &Node{
		ID:                  id,
		NumID:               0,
		Addr:                addr,
		DriverAddr:          driver,
		view:                0,
		primaryID:           1,
		seqNext:             1,
		execNext:            1,
		log:                 make(map[int]*Entry),
		bank:                make(map[string]int64),
		dedup:               make(map[string]map[int64]bool),
		keys:                k,
		client:              &http.Client{Timeout: 1200 * time.Millisecond},
		start:               time.Now(),
		linkRules:           make(map[string]linkRule),
		lastTau:             make(map[string]int64),
		outDrop:             make(map[string]bool),
		outDelay:            make(map[string]time.Duration),
		linkStats:           make(map[string]*LinkStats),
		byzEquivocate:       false,
		byzGoodSign:         true,
		byzTiming:           false,
		byzTimingDelayMs:    0,
		effectiveN:          0,
		optimisticMode:      false, // Can be enabled via flag or REPL
		optimisticTimeoutMs: 300,   // 300ms timeout for collecting all votes]
		pendingBundleTimers: make(map[int]*time.Timer),
	}
	for _, a := range DefaultAccounts {
		n.bank[a] = DefaultInitBalance
	}

	return n
}

func (n *Node) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ping", n.hPing)
	mux.HandleFunc("/status", n.hStatus)
	mux.HandleFunc("/db", n.hDB)
	mux.HandleFunc("/log", n.hLog)

	mux.HandleFunc("/submit", n.hSubmit)
	mux.HandleFunc("/balance", n.hBalance)
	mux.HandleFunc("/preprepare", n.hPrePrepare)
	mux.HandleFunc("/prepareVote", n.hPrepareVote)
	mux.HandleFunc("/prepareBundle", n.hPrepareBundle)
	mux.HandleFunc("/commitVote", n.hCommitVote)
	mux.HandleFunc("/commitBundle", n.hCommitBundle)
	mux.HandleFunc("/help", n.hHelp)
	mux.HandleFunc("/peers", n.hPeers)
	mux.HandleFunc("/slot", n.hSlot)
	mux.HandleFunc("/flush", n.hFlush)
	mux.HandleFunc("/leader", n.hLeader)
	mux.HandleFunc("/viewchange", n.hViewChange)
	mux.HandleFunc("/newview", n.hNewView)
	mux.HandleFunc("/fail", n.hFail)
	mux.HandleFunc("/recover", n.hRecover)
	mux.HandleFunc("/setN", n.hSetN)

	mux.HandleFunc("/dark", n.hDark)
	mux.HandleFunc("/undark", n.hUndark)
	mux.HandleFunc("/delay", n.hDelay)
	mux.HandleFunc("/undelay", n.hUndelay)
	mux.HandleFunc("/resendCommitted", n.hResendCommitted)
	mux.HandleFunc("/byz", n.hByz)
	mux.HandleFunc("/vc", n.hVC)
	mux.HandleFunc("/optimisticCommitBundle", n.hOptimisticCommitBundle)
	n.httpSrv = &http.Server{Addr: n.Addr, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if err := n.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[%s] serve: %v", n.ID, err)
		}
	}()

	// Start REPL immediately
	go n.startREPL()

	// Discovery: background if driver provided; else single-node self-membership
	if strings.TrimSpace(n.DriverAddr) == "" {
		n.buildMembershipSelf()
		return nil
	}
	go func() {
		if err := n.discover(); err != nil {
			log.Printf("[%s] discovery failed (%v) — starting in standalone mode", n.ID, err)
			n.buildMembershipSelf()
		}
	}()
	n.vcMu.Lock()
	n.lastProgress = time.Now()
	n.lastLeaderOK = time.Now()
	if n.vcTimeoutMs == 0 {
		n.vcTimeoutMs = 1500
	}
	n.vcMu.Unlock()
	go n.viewLoop()
	go n.periodicSync()

	return nil
}

func (n *Node) Stop(ctx context.Context) error { return n.httpSrv.Shutdown(ctx) }

func (n *Node) discover() error {
	// Register self
	rep, err := DriverRegister(n.DriverAddr, n.Addr)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}
	n.NumID = rep.ID
	n.buildMembership(rep.Members)
	log.Printf("[%s] registered as id=%d (driver=%s)", n.ID, n.NumID, n.DriverAddr)

	// Wait for full membership
	for {
		mrs, err := DriverMembers(n.DriverAddr)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		n.buildMembership(mrs.Members)
		if len(mrs.Members) >= mrs.N && mrs.N > 0 {
			log.Printf("[%s] discovery: all %d nodes up", n.ID, mrs.N)
			return nil
		}
		missing := mrs.N - len(mrs.Members)
		if missing > 0 {
			log.Printf("[%s] waiting for nodes... need %d more", n.ID, missing)
		}
		time.Sleep(700 * time.Millisecond)
	}
}

func (n *Node) buildMembership(members []string) {
	n.Replicas = make(map[int]string)
	n.Infos = make([]ReplicaInfo, 0, len(members))
	for i, a := range members {
		id := i + 1
		n.Replicas[id] = a
		n.Infos = append(n.Infos, ReplicaInfo{ID: id, Addr: a, Pub: pubFromAddr(a)})
	}
	n.primaryID = 1
	n.vcMu.Lock()
	n.lastLeaderOK = time.Now()
	n.vcMu.Unlock()

}

// ---- Helpers ----
func (n *Node) quorum() (f int, q int) {
	nn := len(n.Replicas)
	if n.effectiveN > 0 && n.effectiveN <= nn {
		nn = n.effectiveN
	}
	f = (nn - 1) / 3
	return f, 2*f + 1
}
func shaOp(op Op) Digest        { b, _ := json.Marshal(op); return sha256.Sum256(b) }
func (n *Node) isPrimary() bool { return n.myID() == n.primaryID }
func (n *Node) myID() int       { return n.NumID }

// ---- Diagnostics ----
func (n *Node) hPing(w http.ResponseWriter, r *http.Request) {
	if n.isFailed {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if n.byzCrash {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (n *Node) hStatus(w http.ResponseWriter, r *http.Request) {
	f, _ := n.quorum()
	nn := len(n.Replicas)
	out := StatusReply{ID: n.ID, Addr: n.Addr, View: n.view, Primary: n.primaryID, Uptime: int64(time.Since(n.start) / time.Millisecond), SeqNext: n.seqNext, ExecNext: n.execNext, N: nn, F: f}
	writeJSON(w, http.StatusOK, out)
}

func (n *Node) hDB(w http.ResponseWriter, r *http.Request) {
	// Failed nodes should not respond
	if n.isFailed {
		http.Error(w, "node failed", http.StatusServiceUnavailable)
		return
	}

	n.bankMu.Lock()
	copy := map[string]int64{}
	for k, v := range n.bank {
		copy[k] = v
	}
	n.bankMu.Unlock()
	writeJSON(w, http.StatusOK, copy)
}

func (n *Node) hLog(w http.ResponseWriter, r *http.Request) {
	n.logMu.Lock()
	defer n.logMu.Unlock()

	type logEntry struct {
		Seq         int        `json:"seq"`
		View        View       `json:"view"`
		Status      SlotStatus `json:"status"`
		Digest      string     `json:"digest"`
		Op          *Op        `json:"op,omitempty"`
		PrepareFrom []int      `json:"prepare_from"`
		CommitFrom  []int      `json:"commit_from"`
	}

	seqs := make([]int, 0, len(n.log))
	for s := range n.log {
		seqs = append(seqs, s)
	}
	sort.Ints(seqs)

	out := struct {
		Entries []logEntry `json:"entries"`
	}{Entries: make([]logEntry, 0, len(seqs))}

	for _, s := range seqs {
		e := n.log[s]
		le := logEntry{
			Seq:    e.Seq,
			View:   e.View,
			Status: e.Status,
			Digest: fmt.Sprintf("%x", e.D[:]),
			Op:     e.Op,
		}
		for id := range e.PrepareVotes {
			le.PrepareFrom = append(le.PrepareFrom, id)
		}
		for id := range e.CommitVotes {
			le.CommitFrom = append(le.CommitFrom, id)
		}
		sort.Ints(le.PrepareFrom)
		sort.Ints(le.CommitFrom)
		out.Entries = append(out.Entries, le)
	}

	writeJSON(w, http.StatusOK, out)
}

func (n *Node) buildMembershipSelf() {
	n.Replicas = map[int]string{1: n.Addr}
	n.Infos = []ReplicaInfo{{ID: 1, Addr: n.Addr, Pub: pubFromAddr(n.Addr)}}
	n.primaryID = 1
	n.NumID = 1
	log.Printf("[%s] standalone mode: no driver; cluster size=1 (primary=self)", n.ID)
}
func (n *Node) hHelp(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	io.WriteString(w, `HTTP endpoints:
  GET /ping
  GET /status
  GET /db
  GET /log
  GET /peers
  GET /slot?seq=<num>

REPL commands (type in the node's terminal):
  help | ?
  whoami
  status
  peers
  db
  log
  slot <seq>
  submit <from> <to> <amt> <cid> <tau>
  quit | exit
`)
}

func (n *Node) hPeers(w http.ResponseWriter, r *http.Request) {
	m := map[int]string{}
	for id, a := range n.Replicas {
		m[id] = a
	}
	writeJSON(w, http.StatusOK, m)
}

func (n *Node) hSlot(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("seq")
	if q == "" {
		http.Error(w, "missing seq", http.StatusBadRequest)
		return
	}
	seq, err := strconv.Atoi(q)
	if err != nil {
		http.Error(w, "bad seq", http.StatusBadRequest)
		return
	}
	n.logMu.Lock()
	e, ok := n.log[seq]
	n.logMu.Unlock()
	if !ok {
		http.Error(w, "no such slot", http.StatusNotFound)
		return
	}

	prepIDs := make([]int, 0, len(e.PrepareVotes))
	for id := range e.PrepareVotes {
		prepIDs = append(prepIDs, id)
	}
	sort.Ints(prepIDs)
	commIDs := make([]int, 0, len(e.CommitVotes))
	for id := range e.CommitVotes {
		commIDs = append(commIDs, id)
	}
	sort.Ints(commIDs)

	out := map[string]any{
		"view":          e.View,
		"seq":           e.Seq,
		"status":        e.Status,
		"op":            e.Op,
		"digest":        fmt.Sprintf("%x", e.D[:]),
		"prepare_count": len(e.PrepareVotes),
		"commit_count":  len(e.CommitVotes),
		"prepare_from":  prepIDs,
		"commit_from":   commIDs,
		"bad_sig_from":  e.BadSigFrom,
	}
	writeJSON(w, http.StatusOK, out)
}
func (n *Node) startREPL() {
	in := bufio.NewScanner(os.Stdin)
	fmt.Println("REPL ready — type 'help' for commands")
	for {
		fmt.Print("> ")
		if !in.Scan() {
			return
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "help", "?":
			n.printHelp()
		case "whoami":
			fmt.Printf("id=%s numID=%d addr=%s primaryID=%d isPrimary=%v view=%d\n",
				n.ID, n.NumID, n.Addr, n.primaryID, n.isPrimary(), n.view)
		case "status":
			n.printStatus()
		case "peers":
			n.printPeers()
		case "db":
			n.printDB()
		case "log":
			n.printLog()
		case "slot":
			if len(parts) < 2 {
				fmt.Println("usage: slot <seq>")
				continue
			}
			seq, err := strconv.Atoi(parts[1])
			if err != nil {
				fmt.Println("bad seq")
				continue
			}
			n.printSlot(seq)
		case "submit":
			if len(parts) < 6 {
				fmt.Println("usage: submit <from> <to> <amt> <cid> <tau>")
				continue
			}
			amt, err1 := strconv.ParseInt(parts[3], 10, 64)
			tau, err2 := strconv.ParseInt(parts[5], 10, 64)
			if err1 != nil || err2 != nil {
				fmt.Println("bad amt or tau")
				continue
			}
			op := Op{From: parts[1], To: parts[2], Amt: amt, C: ClientID(parts[4]), Tau: tau}
			req := ClientRequest{View: n.view, Op: op}
			req.SigC = signClient(op.C, op)
			var reply ClientReply
			if err := n.sendJSON(n.Addr, "/submit", req, &reply); err != nil {
				fmt.Println("submit error:", err)
			} else {
				fmt.Printf("committed seq=%d view=%d\n", reply.Seq, reply.View)
			}
		case "optimistic":
			if len(parts) < 2 {
				fmt.Printf("optimistic mode: %v (timeout: %dms)\n", n.optimisticMode, n.optimisticTimeoutMs)
				fmt.Println("usage: optimistic on|off [timeout_ms]")
				continue
			}
			cmd := strings.ToLower(parts[1])
			if cmd == "on" || cmd == "true" || cmd == "1" {
				n.optimisticMode = true
				if len(parts) >= 3 {
					if ms, err := strconv.Atoi(parts[2]); err == nil && ms > 0 {
						n.optimisticTimeoutMs = ms
					}
				}
				fmt.Printf("optimistic mode enabled (timeout: %dms)\n", n.optimisticTimeoutMs)
			} else {
				n.optimisticMode = false
				fmt.Println("optimistic mode disabled")
			}
		case "dia", "diagnostic", "diag":
			fmt.Printf("=== Node Diagnostic ===\n")
			fmt.Printf("optimisticMode: %v\n", n.optimisticMode)
			fmt.Printf("optimisticTimeoutMs: %d\n", n.optimisticTimeoutMs)
			fmt.Printf("isPrimary: %v\n", n.isPrimary())
			fmt.Printf("view: %d\n", n.view)

		case "quit", "exit":
			fmt.Println("bye")
			n.shutdownNow()
			return
		default:
			fmt.Println("unknown cmd; type 'help'")
		}
	}
}

func (n *Node) printHelp() {
	fmt.Println(`commands:
  help | ?                 - this help
  whoami                   - show id/addr/primary/view
  status                   - print status summary
  peers                    - list replica ids and addrs
  db                       - print balances
  log                      - list slots and brief info
  slot <seq>               - detailed info for a slot (votes, digest)
  submit <from> <to> <amt> <cid> <tau>  - propose a transfer here
  optimistic on|off [ms]   - toggle optimistic phase reduction
  quit | exit`)
}

func (n *Node) printStatus() {
	f, _ := n.quorum()
	nn := len(n.Replicas)
	fmt.Printf("status: id=%s addr=%s view=%d primary=%d isPrimary=%v n=%d f=%d seqNext=%d execNext=%d uptime_ms=%d\n",
		n.ID, n.Addr, n.view, n.primaryID, n.isPrimary(), nn, f, n.seqNext, n.execNext, int64(time.Since(n.start)/time.Millisecond))
}

func (n *Node) printPeers() {
	ids := make([]int, 0, len(n.Replicas))
	for id := range n.Replicas {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	for _, id := range ids {
		fmt.Printf("peer[%d]=%s\n", id, n.Replicas[id])
	}
}

func (n *Node) printDB() {
	n.bankMu.Lock()
	defer n.bankMu.Unlock()
	var parts []string
	for _, a := range DefaultAccounts {
		parts = append(parts, fmt.Sprintf("%s=%d", a, n.bank[a]))
	}
	fmt.Printf("DB: %s\n", strings.Join(parts, " "))
}

func (n *Node) printLog() {
	n.logMu.Lock()
	defer n.logMu.Unlock()

	statusName := func(s SlotStatus) string {
		switch s {
		case SSPrePrepared:
			return "pre-prepared"
		case SSPrepared:
			return "prepared"
		case SSCommitted:
			return "committed"
		case SSExecuted:
			return "executed"
		default:
			return fmt.Sprintf("unknown(%d)", int(s))
		}
	}

	seqs := make([]int, 0, len(n.log))
	for k := range n.log {
		seqs = append(seqs, k)
	}
	sort.Ints(seqs)

	for _, s := range seqs {
		e := n.log[s]
		fmt.Printf("slot %d:\n", s)
		fmt.Printf("  view=%d status=%s digest=%x\n", e.View, statusName(e.Status), e.D)
		if e.Op != nil {
			fmt.Printf("  op: %s -> %s amt=%d tau=%d c=%s\n", e.Op.From, e.Op.To, e.Op.Amt, e.Op.Tau, e.Op.C)
		} else {
			fmt.Println("  op: <nil>")
		}
		preIDs := make([]int, 0, len(e.PrepareVotes))
		comIDs := make([]int, 0, len(e.CommitVotes))
		for id := range e.PrepareVotes {
			preIDs = append(preIDs, id)
		}
		for id := range e.CommitVotes {
			comIDs = append(comIDs, id)
		}
		sort.Ints(preIDs)
		sort.Ints(comIDs)
		fmt.Printf("  prepares=%v (%d)\n", preIDs, len(preIDs))
		fmt.Printf("  commits=%v (%d)\n", comIDs, len(comIDs))
	}
}

func (n *Node) printSlot(seq int) {
	n.logMu.Lock()
	e, ok := n.log[seq]
	n.logMu.Unlock()
	if !ok {
		fmt.Println("no such slot")
		return
	}
	prepIDs := make([]int, 0, len(e.PrepareVotes))
	for id := range e.PrepareVotes {
		prepIDs = append(prepIDs, id)
	}
	sort.Ints(prepIDs)
	commIDs := make([]int, 0, len(e.CommitVotes))
	for id := range e.CommitVotes {
		commIDs = append(commIDs, id)
	}
	sort.Ints(commIDs)
	fmt.Printf("slot %d:\n  view=%d status=%d digest=%x\n  op: %s -> %s amt=%d (c=%s tau=%d)\n  prepares=%v commits=%v\n",
		e.Seq, e.View, e.Status, e.D, e.Op.From, e.Op.To, e.Op.Amt, string(e.Op.C), e.Op.Tau, prepIDs, commIDs)
}

func (n *Node) shutdownNow() {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if n.httpSrv != nil {
			_ = n.httpSrv.Shutdown(ctx)
		}
		os.Exit(0)
	}()
}

func (n *Node) hFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	n.bankMu.Lock()
	n.bank = make(map[string]int64, len(DefaultAccounts))
	for _, a := range DefaultAccounts {
		n.bank[a] = DefaultInitBalance
	}
	n.dedup = make(map[string]map[int64]bool)
	n.bankMu.Unlock()

	n.logMu.Lock()
	n.log = make(map[int]*Entry)
	n.seqNext = 1
	n.execNext = 1
	n.view = 0
	n.primaryID = 1
	n.logMu.Unlock()

	n.lastTau = make(map[string]int64)
	n.isFailed = false
	n.byzCrash = false
	n.byzEquivocate = false
	n.byzGoodSign = true
	n.byzTiming = false
	n.byzTimingDelayMs = 0

	n.muLinks.Lock()
	n.linkRules = make(map[string]linkRule)
	n.muLinks.Unlock()
	n.outDrop = make(map[string]bool)
	n.outDelay = make(map[string]time.Duration)

	n.vcMu.Lock()
	n.lastLeaderOK = time.Now()
	n.vcMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (n *Node) updatePrimaryFromView() {
	nn := len(n.Replicas)
	if nn <= 0 {
		return
	}
	// PBFT primary(view v) = 1 + (v mod n)
	n.primaryID = 1 + int(n.view)%nn
}
func (n *Node) bumpSeqNext(s int) {
	if s+1 > n.seqNext {
		n.seqNext = s + 1
	}
}

func (n *Node) onProgress() {
	n.vcMu.Lock()
	n.lastProgress = time.Now()
	n.vcMu.Unlock()
}

func (n *Node) periodicSync() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if n.isFailed {
			continue
		}

		n.logMu.Lock()
		behind := n.seqNext > n.execNext+5
		n.logMu.Unlock()

		if behind && !n.isPrimary() {
			if leader := n.Replicas[n.primaryID]; leader != "" {
				_ = n.sendJSON(leader, "/resendCommitted", nil, nil)
			}
		}
	}
}
func (n *Node) initiateViewChange(to View) {
	n.vcMu.Lock()
	if to <= n.view {
		n.vcMu.Unlock()
		return
	}
	n.view = to
	n.updatePrimaryFromView()
	if n.vcAcks == nil {
		n.vcAcks = make(map[View]map[int]bool)
	}
	if _, ok := n.vcAcks[to]; !ok {
		n.vcAcks[to] = map[int]bool{}
	}
	n.vcAcks[to][n.myID()] = true
	n.lastProgress = time.Now()
	n.vcMu.Unlock()

	msg := ViewChangeMsg{To: to, From: n.myID()}
	msg.Sig = signPhase(n, "VC", to, 0, Digest{})
	for _, a := range n.Replicas {
		_ = n.sendJSON(a, "/viewchange", msg, nil)
	}
}

func (n *Node) viewLoop() {
	if n.vcTimeoutMs == 0 {
		n.vcTimeoutMs = 1500
	}
	t := time.NewTicker(time.Duration(n.vcTimeoutMs) * time.Millisecond)
	for range t.C {
		now := time.Now()

		n.vcMu.Lock()
		curView := n.view
		lastProg := n.lastProgress
		lastOK := n.lastLeaderOK
		me := n.myID()
		primary := n.primaryID
		n.vcMu.Unlock()

		if me == primary {
			continue
		}

		// Case 1: stalled in-flight work (classic PBFT trigger)
		stalled := n.hasInflight() && now.Sub(lastProg) > time.Duration(n.vcTimeoutMs)*time.Millisecond
		if stalled {
			n.initiateViewChange(curView + 1)
			continue
		}

		// Case 2: primary unreachable for too long (idle cluster, but leader died)
		if now.Sub(lastOK) > time.Duration(n.vcTimeoutMs)*time.Millisecond {
			if n.primaryAliveProbe() {
				n.vcMu.Lock()
				n.lastLeaderOK = time.Now()
				n.vcMu.Unlock()
			} else {
				n.initiateViewChange(curView + 1)
			}
		}
	}
}

func (n *Node) hLeader(w http.ResponseWriter, r *http.Request) {
	// return current primary id/addr (used by driver/client)
	id := n.primaryID
	addr := n.Replicas[id]
	writeJSON(w, http.StatusOK, LeaderReply{ID: id, Addr: addr})
}

func (n *Node) hViewChange(w http.ResponseWriter, r *http.Request) {
	if n.isFailed && !allowWhenFailed[r.URL.Path] {
		http.Error(w, "failed", http.StatusServiceUnavailable)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var m ViewChangeMsg
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	// verify sender's VC sig
	addr := n.Replicas[m.From]
	if !verifyPhaseWithAddr("VC", addr, m.To, 0, Digest{}, m.Sig) {
		http.Error(w, "bad VC sig", http.StatusForbidden)
		return
	}

	n.vcMu.Lock()

	if n.vcAcks == nil {
		n.vcAcks = make(map[View]map[int]bool)
	}
	if _, ok := n.vcAcks[m.To]; !ok {
		n.vcAcks[m.To] = map[int]bool{}
	}
	n.vcAcks[m.To][m.From] = true

	_, q := n.quorum()
	nn := len(n.Replicas)
	// Calculate the expected primary for target view m.To
	newPrimaryID := 1 + int(m.To)%nn
	isNewPrimary := (n.myID() == newPrimaryID)
	acks := len(n.vcAcks[m.To])
	n.vcMu.Unlock()

	// If I'm the new primary and I have >= 2f+1 VC acks, announce NewView
	if isNewPrimary && acks >= q {
		n.vcMu.Lock()
		ids := make([]int, 0, len(n.vcAcks[m.To]))
		for id := range n.vcAcks[m.To] {
			ids = append(ids, id)
		}
		n.vcMu.Unlock()
		if n.byzCrash { // malicious mode: skip sending NewView
			return
		}
		// Collect prepared operations to include in NewView
		nv := NewViewMsg{View: m.To, From: n.myID(), Acks: ids}
		nv.Sig = signPhase(n, "NV", nv.View, 0, Digest{})
		for _, a := range n.Replicas {
			_ = n.sendJSON(a, "/newview", nv, nil)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (n *Node) hNewView(w http.ResponseWriter, r *http.Request) {
	if n.isFailed && !allowWhenFailed[r.URL.Path] {
		http.Error(w, "failed", http.StatusServiceUnavailable)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var nv NewViewMsg
	if err := json.NewDecoder(r.Body).Decode(&nv); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	// verify sig from the claimed new primary for that view
	nn := len(n.Replicas)
	expectPrimary := 1 + int(nv.View)%nn
	addr := n.Replicas[expectPrimary]
	if !verifyPhaseWithAddr("NV", addr, nv.View, 0, Digest{}, nv.Sig) {
		http.Error(w, "bad NV sig", http.StatusForbidden)
		return
	}

	n.vcMu.Lock()
	if nv.View >= n.view {
		n.view = nv.View
		n.updatePrimaryFromView()
		n.lastProgress = time.Now()
		// Clear conflicting uncommitted operations in new view
		for seq, e := range n.log {
			if e.Status < SSCommitted {
				delete(n.log, seq)
			}
		}
		// Reset sequence counter to after last committed
		n.seqNext = n.execNext
	}
	n.vcMu.Unlock()
	// after view/sig checks and before returning:
	n.vcMu.Lock()
	n.nvHistory = append(n.nvHistory, NewViewMsg{View: nv.View, From: nv.From, Acks: append([]int(nil), nv.Acks...)})
	n.lastLeaderOK = time.Now()
	n.vcMu.Unlock()

	// Delay to let new view stabilize, then resend committed ops
	go func() {
		time.Sleep(100 * time.Millisecond)
		n.resendCommitted()
	}()
	n.vcMu.Lock()
	n.lastLeaderOK = time.Now()
	n.vcMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// hasInflight returns true if there are unexecuted slots.
func (n *Node) hasInflight() bool {
	n.logMu.Lock()
	inflight := n.seqNext > n.execNext
	n.logMu.Unlock()
	return inflight
}
func (n *Node) primaryAliveProbe() bool {
	if n.myID() == n.primaryID {
		return true
	}
	addr := n.Replicas[n.primaryID]
	if addr == "" {
		return false
	}
	// short, independent client so we don't block on the normal timeout
	h := &http.Client{Timeout: 250 * time.Millisecond}
	resp, err := h.Get("http://" + addr + "/ping")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
func (n *Node) setDark(to string, v bool) {
	n.muLinks.Lock()
	r := n.linkRules[to]
	r.dark = v
	n.linkRules[to] = r
	n.muLinks.Unlock()
}

func (n *Node) setDelay(to string, ms int) {
	n.muLinks.Lock()
	r := n.linkRules[to]
	r.delay = time.Duration(ms) * time.Millisecond
	n.linkRules[to] = r
	n.muLinks.Unlock()
}

func (n *Node) applyLinkRules(to string) bool {
	n.muLinks.Lock()
	r := n.linkRules[to]
	n.muLinks.Unlock()
	if r.dark {
		// drop
		return false
	}
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	return true
}
func (n *Node) hByz(w http.ResponseWriter, r *http.Request) {
	mode := strings.ToLower(r.URL.Query().Get("mode"))
	if mode == "" {
		mode = "crash"
	}
	switch mode {
	case "crash":
		on := strings.ToLower(r.URL.Query().Get("on"))
		n.byzCrash = (on == "1" || on == "true" || on == "on")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "crash": n.byzCrash})
	case "equivocate":
		on := strings.ToLower(r.URL.Query().Get("on"))
		n.byzEquivocate = (on == "1" || on == "true" || on == "on")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "equivocate": n.byzEquivocate})
	case "sign":
		on := strings.ToLower(r.URL.Query().Get("on"))
		n.byzGoodSign = (on == "1" || on == "true" || on == "on")
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "goodSign": n.byzGoodSign})
	case "time", "timing":
		on := strings.ToLower(r.URL.Query().Get("on"))
		n.byzTiming = (on == "1" || on == "true" || on == "on")
		msStr := r.URL.Query().Get("ms")
		if msStr != "" {
			if ms, err := strconv.Atoi(msStr); err == nil {
				n.byzTimingDelayMs = ms
			}
		}
		if n.byzTimingDelayMs == 0 {
			n.byzTimingDelayMs = 800 // default delay
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "timing": n.byzTiming, "delayMs": n.byzTimingDelayMs})
	default:
		http.Error(w, "unknown mode", http.StatusBadRequest)
	}

}
func (n *Node) hVC(w http.ResponseWriter, r *http.Request) {
	n.vcMu.Lock()
	view := n.view
	primary := n.primaryID
	var acks []int
	if n.vcAcks != nil {
		if m, ok := n.vcAcks[view]; ok {
			for id := range m {
				acks = append(acks, id)
			}
			sort.Ints(acks)
		}
	}
	// copy history under lock
	hist := append([]NewViewMsg(nil), n.nvHistory...)
	n.lastLeaderOK = time.Now()
	n.vcMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]any{
		"view":     view,
		"primary":  primary,
		"acks":     acks,
		"newviews": hist,
	})
}

func (n *Node) hSetN(w http.ResponseWriter, r *http.Request) {
	nStr := r.URL.Query().Get("n")
	if nStr == "" {
		http.Error(w, "missing n parameter", http.StatusBadRequest)
		return
	}
	newN, err := strconv.Atoi(nStr)
	if err != nil || newN < 1 {
		http.Error(w, "invalid n value", http.StatusBadRequest)
		return
	}
	n.effectiveN = newN
	log.Printf("[%s] updated effective N to %d for quorum calculation", n.ID, newN)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "n": newN})
}
