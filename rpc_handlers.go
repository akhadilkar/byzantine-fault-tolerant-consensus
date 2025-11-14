package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ---------- Client → leader ----------
func (n *Node) hSubmit(w http.ResponseWriter, r *http.Request) {
	if n.isFailed && !allowWhenFailed[r.URL.Path] {
		http.Error(w, "failed", http.StatusServiceUnavailable)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !n.isPrimary() {
		http.Error(w, "not primary", http.StatusConflict)
		return
	}
	if n.byzCrash {
		// Byzantine crash: leader receives request but does NOT process it
		http.Error(w, "crashed", http.StatusServiceUnavailable)
		return
	}
	var req ClientRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	// After: if err := json.NewDecoder(r.Body).Decode(&req) ...
	if !verifyClient(req.Op.C, req.Op, req.SigC) {
		http.Error(w, "bad client signature", http.StatusUnauthorized)
		return
	}

	op := req.Op
	seq := n.seqNext
	n.seqNext++
	d := shaOp(op)

	n.logMu.Lock()
	if _, ok := n.log[seq]; !ok {
		opCopy := op
		n.log[seq] = &Entry{
			View:         n.view,
			Seq:          seq,
			Op:           &opCopy,
			D:            d,
			Status:       SSPrePrepared,
			PrepareVotes: map[int][]byte{}, CommitVotes: map[int][]byte{},
		}
		// Leader implicitly prepares by sending PrePrepare
		e := n.log[seq]
		e.PrepareVotes[n.myID()] = signPhase(n, "P", n.view, seq, d)

		n.log[seq] = e
		e.PrePrepareFrom = n.myID() // Leader is PP senders
	}
	n.logMu.Unlock()

	// Timing attack: Byzantine leader delays before sending PrePrepare
	if n.byzTiming {
		time.Sleep(time.Duration(n.byzTimingDelayMs) * time.Millisecond)
	}
	// Compute the "honest" digest for the op we stored
	dHonest := shaOp(op)

	var opDishonest Op
	var dDishonest Digest
	if n.byzEquivocate {
		opDishonest = op
		// make tau different
		opDishonest.Tau = op.Tau + 1
		dDishonest = shaOp(opDishonest)
	}

	alt := 0
	for id, addr := range n.Replicas {
		if id == n.primaryID {
			continue
		}
		var mPP PrePrepare
		if n.byzEquivocate && (alt == 1 || alt == 3 || alt == 5) {
			mPP = PrePrepare{
				View: n.view,
				Seq:  seq,
				D:    dDishonest,
				Op:   opDishonest,
				SigL: signPhase(n, "PP", n.view, seq, dDishonest),
			}
		} else {
			mPP = PrePrepare{
				View: n.view,
				Seq:  seq,
				D:    dHonest,
				Op:   op,
				SigL: signPhase(n, "PP", n.view, seq, dHonest),
			}
		}
		alt++
		go n.sendJSON(addr, "/preprepare", mPP, nil)
	}

	n.onProgress()
	writeJSON(w, http.StatusOK, ClientReply{OK: true, Seq: seq, View: n.view})
}

// ---------- Client → replica : balance query (read-only) ----------
func (n *Node) hBalance(w http.ResponseWriter, r *http.Request) {
	if n.isFailed && !allowWhenFailed[r.URL.Path] {
		http.Error(w, "failed", http.StatusServiceUnavailable)
		return
	}

	// Byzantine crash: don't respond to read-only requests
	if n.byzCrash {
		http.Error(w, "crashed", http.StatusServiceUnavailable)
		return
	}

	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "GET or POST only", http.StatusMethodNotAllowed)
		return
	}

	acct := r.URL.Query().Get("acct")
	if acct == "" {
		http.Error(w, "missing acct parameter", http.StatusBadRequest)
		return
	}

	n.bankMu.Lock()
	balance := n.bank[strings.ToUpper(acct)]
	n.bankMu.Unlock()

	writeJSON(w, http.StatusOK, map[string]int64{
		strings.ToUpper(acct): balance,
	})
}

// ---------- Primary → replicas ----------
func (n *Node) hPrePrepare(w http.ResponseWriter, r *http.Request) {
	if n.isFailed && !allowWhenFailed[r.URL.Path] {
		http.Error(w, "failed", http.StatusServiceUnavailable)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var m PrePrepare

	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	totalN := len(n.Replicas)
	expectPrimary := 1 + int(m.View)%totalN

	leaderAddr := n.Replicas[expectPrimary]
	if !verifyPhaseWithAddr("PP", leaderAddr, m.View, m.Seq, m.D, m.SigL) {
		http.Error(w, "bad leader sig", http.StatusForbidden)
		n.logMu.Lock()
		e := n.ensureEntry(m.View, m.Seq)
		e.Rejects = append(e.Rejects, "reject: PP bad leader sig")
		n.logMu.Unlock()
		return
	}
	if shaOp(m.Op) != m.D {
		http.Error(w, "bad digest", http.StatusForbidden)
		n.logMu.Lock()
		e := n.ensureEntry(m.View, m.Seq)
		e.Rejects = append(e.Rejects, "reject: PP digest mismatch")
		n.logMu.Unlock()
		return
	}
	n.vcMu.Lock()
	n.lastLeaderOK = time.Now()
	n.vcMu.Unlock()

	n.logMu.Lock()
	e, exists := n.log[m.Seq]
	if !exists {
		e = &Entry{
			View: m.View, Seq: m.Seq, Op: &m.Op, D: m.D, Status: SSPrePrepared,
			PrepareVotes: map[int][]byte{}, CommitVotes: map[int][]byte{},
		}
		n.log[m.Seq] = e
	} else {
		if e.D != m.D {
			currentView := n.view
			n.logMu.Unlock()
			log.Printf("[%s] EQUIVOCATION in PrePrepare: seq=%d old_digest=%x new_digest=%x",
				n.ID, m.Seq, e.D[:8], m.D[:8])
			go n.initiateViewChange(currentView + 1)
			http.Error(w, "equivocation: conflicting PrePrepare digest", http.StatusConflict)
			return
		}
		if e.View != m.View {
			n.logMu.Unlock()
			http.Error(w, "conflicting PrePrepare view", http.StatusConflict)
			return
		}
		if e.Op == nil {
			e.Op = new(Op)
			*e.Op = m.Op
		}
	}
	n.logMu.Unlock()

	if m.View < n.view {
		http.Error(w, "stale view", http.StatusConflict)
		n.logMu.Lock()
		e := n.ensureEntry(m.View, m.Seq)
		e.Rejects = append(e.Rejects, "reject: PP stale view")
		n.logMu.Unlock()
		return
	}
	n.bumpSeqNext(m.Seq)
	if n.byzCrash {
		n.onProgress()
		return
	}

	vote := PrepareVote{View: m.View, Seq: m.Seq, D: m.D, From: n.myID()}
	vote.Sig = signPhase(n, "P", vote.View, vote.Seq, vote.D)
	_ = n.sendJSON(leaderAddr, "/prepareVote", vote, nil)
	n.onProgress()
	writeJSON(w, http.StatusOK, PrePrepareAck{OK: true})

}

// ---------- Replica → collector (leader) : prepare votes ----------
func (n *Node) hPrepareVote(w http.ResponseWriter, r *http.Request) {
	if n.isFailed && !allowWhenFailed[r.URL.Path] {
		http.Error(w, "failed", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !n.isPrimary() {
		http.Error(w, "not primary", http.StatusConflict)
		return
	}

	var v PrepareVote
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	// Verify sender's signature
	addr := n.Replicas[v.From]
	if !verifyPhaseWithAddr("P", addr, v.View, v.Seq, v.D, v.Sig) {
		n.logMu.Lock()

		e := n.ensureEntry(v.View, v.Seq)
		e.BadSigFrom = appendUniqueInt(e.BadSigFrom, v.From)
		n.logMu.Unlock()
		log.Printf("[%s] rejecting prepare vote from %d: bad signature", n.ID, v.From)
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "bad signature"})
		return
	}

	n.logMu.Lock()
	e, ok := n.log[v.Seq]
	n.logMu.Unlock()
	if !ok {
		http.Error(w, "unknown seq (no PrePrepare on leader)", http.StatusConflict)
		return
	}
	if e.View != v.View || e.D != v.D {
		http.Error(w, "vote (view/digest) mismatch", http.StatusForbidden)
		return
	}

	n.logMu.Lock()
	if _, seen := e.PrepareVotes[v.From]; !seen {
		e.PrepareVotes[v.From] = v.Sig
	}

	votes := len(e.PrepareVotes)
	_, q := n.quorum()
	totalN := len(n.Replicas)
	hasAllVotes := votes >= totalN
	hasQuorum := votes >= q
	alreadySent := e.BundleSent

	var ids []int
	if hasQuorum || hasAllVotes {
		ids = make([]int, 0, len(e.PrepareVotes))
		for id := range e.PrepareVotes {
			ids = append(ids, id)
		}
		sort.Ints(ids)
	}
	n.logMu.Unlock()

	if n.byzCrash {
		http.Error(w, "crashed", http.StatusServiceUnavailable)
		return
	}

	// OPTIMISTIC: If we have ALL votes, send OptimisticCommitBundle immediately
	if n.optimisticMode && hasAllVotes && !alreadySent {
		n.logMu.Lock()
		e.BundleSent = true
		arr := make([][]byte, 0, len(ids))
		for _, id := range ids {
			arr = append(arr, e.PrepareVotes[id])
		}
		ocb := OptimisticCommitBundle{View: v.View, Seq: v.Seq, D: v.D, Votes: arr, FromID: ids, Op: e.Op}

		e.Status = SSCommitted
		if e.CommitVotes == nil {
			e.CommitVotes = map[int][]byte{}
		}
		for i, id := range ids {
			e.CommitVotes[id] = arr[i]
		}
		n.logMu.Unlock()

		log.Printf("[%s] OPTIMISTIC PATH: sending OptimisticCommitBundle for seq=%d with %d votes", n.ID, v.Seq, len(ids))
		for _, addr := range n.Replicas {
			_ = n.sendJSON(addr, "/optimisticCommitBundle", ocb, nil)
		}
		n.executeReady()
		n.onProgress()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "optimistic": true})
		return
	}

	// Start timeout goroutine when we first hit quorum (but don't have all votes yet)
	if n.optimisticMode && hasQuorum && !hasAllVotes && !alreadySent {
		// Start a goroutine to wait and then send PrepareBundle if we don't get all votes
		go func(seq int, view View, d Digest, currentVotes int) {
			time.Sleep(time.Duration(n.optimisticTimeoutMs) * time.Millisecond)

			n.logMu.Lock()
			e := n.log[seq]
			if e.BundleSent {
				n.logMu.Unlock()
				return // Already sent (optimistic path won)
			}
			e.BundleSent = true

			// Collect current votes
			ids := make([]int, 0, len(e.PrepareVotes))
			for id := range e.PrepareVotes {
				ids = append(ids, id)
			}
			sort.Ints(ids)
			arr := make([][]byte, 0, len(ids))
			for _, id := range ids {
				arr = append(arr, e.PrepareVotes[id])
			}
			pb := PrepareBundle{View: view, Seq: seq, D: d, Votes: arr, FromID: ids, Op: e.Op}
			e.Status = SSPrepared
			if e.CommitVotes == nil {
				e.CommitVotes = map[int][]byte{}
			}
			e.CommitVotes[n.myID()] = signPhase(n, "C", view, seq, d)
			n.logMu.Unlock()

			log.Printf("[%s] FALLBACK: timeout expired for seq=%d, sending PrepareBundle with %d votes", n.ID, seq, len(ids))
			for _, addr := range n.Replicas {
				_ = n.sendJSON(addr, "/prepareBundle", pb, nil)
			}
			n.onProgress()
		}(v.Seq, v.View, v.D, votes)

		log.Printf("[%s] Started optimistic timer for seq=%d (%d/%d votes)", n.ID, v.Seq, votes, totalN)
	}

	// NORMAL MODE: Send PrepareBundle immediately when quorum reached
	if !n.optimisticMode && hasQuorum && !alreadySent {
		n.logMu.Lock()
		e.BundleSent = true
		arr := make([][]byte, 0, len(ids))
		for _, id := range ids {
			arr = append(arr, e.PrepareVotes[id])
		}
		pb := PrepareBundle{View: v.View, Seq: v.Seq, D: v.D, Votes: arr, FromID: ids, Op: e.Op}

		e.Status = SSPrepared

		if e.CommitVotes == nil {
			e.CommitVotes = map[int][]byte{}
		}

		e.CommitVotes[n.myID()] = signPhase(n, "C", v.View, v.Seq, v.D)

		n.logMu.Unlock()

		log.Printf("[%s] PrepareBundle ready: needBundle=%v byzCrash=%v isPrimary=%v", n.ID, true, n.byzCrash, n.isPrimary())
		for _, addr := range n.Replicas {
			_ = n.sendJSON(addr, "/prepareBundle", pb, nil)
		}
		n.onProgress()
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (n *Node) ensureEntry(view View, seq int) *Entry {
	e, ok := n.log[seq]
	if ok {
		return e
	}
	e = &Entry{View: view, Seq: seq, Status: SSPrePrepared, PrepareVotes: map[int][]byte{}, CommitVotes: map[int][]byte{}}
	n.log[seq] = e
	return e
}

// ---------- Collector → all : prepare bundle ----------
func (n *Node) hPrepareBundle(w http.ResponseWriter, r *http.Request) {
	if n.isFailed && !allowWhenFailed[r.URL.Path] {
		http.Error(w, "failed", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var b PrepareBundle
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	n.vcMu.Lock()
	n.lastLeaderOK = time.Now()
	n.vcMu.Unlock()

	n.logMu.Lock()
	if ex, ok := n.log[b.Seq]; ok {
		if ex.D != b.D {
			currentView := n.view
			n.logMu.Unlock()
			log.Printf("[%s] EQUIVOCATION detected: seq=%d existing_digest=%x new_digest=%x",
				n.ID, b.Seq, ex.D[:8], b.D[:8])
			go n.initiateViewChange(currentView + 1)
			http.Error(w, "equivocation detected: conflicting digest", http.StatusConflict)
			return
		}
	}
	n.logMu.Unlock()

	if b.Op != nil && shaOp(*b.Op) != b.D {
		http.Error(w, "bundle op/digest mismatch", http.StatusForbidden)
		return
	}
	if err := n.verifyBundle("P", b.View, b.Seq, b.D, b.FromID, b.Votes); err != nil {
		http.Error(w, "invalid prepare bundle: "+err.Error(), http.StatusForbidden)
		return
	}

	if n.byzCrash && !n.isPrimary() {
		n.logMu.Lock()
		e := n.ensureEntry(b.View, b.Seq)
		e.D = b.D
		if e.Op == nil && b.Op != nil && shaOp(*b.Op) == b.D {
			e.Op = new(Op)
			*e.Op = *b.Op
		}
		if e.PrepareVotes == nil {
			e.PrepareVotes = map[int][]byte{}
		}
		if len(b.FromID) > 0 {
			e.PrepareVotes = make(map[int][]byte, len(b.FromID))
			for i, id := range b.FromID {
				e.PrepareVotes[id] = b.Votes[i]
			}
		}
		n.logMu.Unlock()

		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}

	n.logMu.Lock()
	e := n.ensureEntry(b.View, b.Seq)
	e.D = b.D
	if e.PrepareVotes == nil {
		e.PrepareVotes = map[int][]byte{}
	}
	if len(b.FromID) > 0 {
		e.PrepareVotes = make(map[int][]byte, len(b.FromID))
		for i, id := range b.FromID {
			e.PrepareVotes[id] = b.Votes[i]
		}
	}
	e.Status = SSPrepared
	n.bumpSeqNext(b.Seq)
	if e.Op == nil && b.Op != nil && shaOp(*b.Op) == b.D {
		e.Op = new(Op)
		*e.Op = *b.Op
	}
	n.logMu.Unlock()

	cv := CommitVote{View: b.View, Seq: b.Seq, D: b.D, From: n.myID()}
	cv.Sig = signPhase(n, "C", cv.View, cv.Seq, cv.D)
	nn := len(n.Replicas)
	expectPrimary := 1 + int(b.View)%nn
	leaderAddr := n.Replicas[expectPrimary]
	_ = n.sendJSON(leaderAddr, "/commitVote", cv, nil)
	n.onProgress()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})

}

// ---------- Replica → collector : commit votes ----------
func (n *Node) hCommitVote(w http.ResponseWriter, r *http.Request) {
	if n.isFailed && !allowWhenFailed[r.URL.Path] {
		http.Error(w, "failed", http.StatusServiceUnavailable)
		return
	}
	if n.byzCrash {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !n.isPrimary() {
		http.Error(w, "not primary", http.StatusConflict)
		return
	}

	var v CommitVote
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	addr := n.Replicas[v.From]
	if !verifyPhaseWithAddr("C", addr, v.View, v.Seq, v.D, v.Sig) {
		n.logMu.Lock()

		e := n.ensureEntry(v.View, v.Seq)
		e.BadSigFrom = appendUniqueInt(e.BadSigFrom, v.From)
		n.logMu.Unlock()
		log.Printf("[%s] rejecting commit vote from %d: bad signature", n.ID, v.From)
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": "bad signature"})
		return
	}

	n.logMu.Lock()
	e, ok := n.log[v.Seq]
	n.logMu.Unlock()
	if !ok {
		http.Error(w, "unknown seq (no PrePrepare on leader)", http.StatusConflict)
		return
	}
	if e.View != v.View || e.D != v.D {
		http.Error(w, "commit vote (view/digest) mismatch", http.StatusForbidden)
		return
	}

	n.logMu.Lock()
	if e.CommitVotes == nil {
		e.CommitVotes = map[int][]byte{}
	}
	if _, seen := e.CommitVotes[v.From]; !seen {
		e.CommitVotes[v.From] = v.Sig
	}
	votes := len(e.CommitVotes)
	_, q := n.quorum()
	needBundle := votes >= q

	var ids []int
	if needBundle {
		ids = make([]int, 0, len(e.CommitVotes))
		for id := range e.CommitVotes {
			ids = append(ids, id)
		}
		sort.Ints(ids)
	}
	n.logMu.Unlock()

	if needBundle && n.byzCrash && n.isPrimary() {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
		return
	}

	if needBundle {
		arr := make([][]byte, 0, len(ids))
		n.logMu.Lock()
		for _, id := range ids {
			arr = append(arr, e.CommitVotes[id])
		}
		cb := CommitBundle{View: v.View, Seq: v.Seq, D: v.D, Votes: arr, FromID: ids, Op: e.Op}
		n.logMu.Unlock()

		for _, addr := range n.Replicas {
			_ = n.sendJSON(addr, "/commitBundle", cb, nil)
		}

		n.logMu.Lock()
		e.Status = SSCommitted
		n.logMu.Unlock()

		n.executeReady()
		n.onProgress()

		go func() {
			time.Sleep(200 * time.Millisecond)
			for _, addr := range n.Replicas {
				_ = n.sendJSON(addr, "/resendCommitted", struct{}{}, nil)
			}
		}()
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// ---------- Collector → all : commit bundle ----------
func (n *Node) hCommitBundle(w http.ResponseWriter, r *http.Request) {
	if n.isFailed && !allowWhenFailed[r.URL.Path] {
		http.Error(w, "failed", http.StatusServiceUnavailable)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var b CommitBundle
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	n.vcMu.Lock()
	n.lastLeaderOK = time.Now()
	n.vcMu.Unlock()
	n.logMu.Lock()
	if ex, ok := n.log[b.Seq]; ok {
		if ex.D != b.D {
			n.logMu.Unlock()
			http.Error(w, "commit-bundle conflicts with existing digest", http.StatusConflict)
			return
		}
	}
	n.logMu.Unlock()

	if b.Op != nil && shaOp(*b.Op) != b.D {
		http.Error(w, "bundle op/digest mismatch", http.StatusForbidden)
		return
	}
	if err := n.verifyBundle("C", b.View, b.Seq, b.D, b.FromID, b.Votes); err != nil {
		http.Error(w, "invalid commit bundle: "+err.Error(), http.StatusForbidden)
		return
	}

	n.logMu.Lock()
	e := n.ensureEntry(b.View, b.Seq)
	e.D = b.D
	e.Status = SSCommitted
	n.bumpSeqNext(b.Seq)

	if e.CommitVotes == nil {
		e.CommitVotes = map[int][]byte{}
	}
	if len(b.FromID) > 0 {
		e.CommitVotes = make(map[int][]byte, len(b.FromID))
		for i, id := range b.FromID {
			e.CommitVotes[id] = b.Votes[i]
		}
	}

	if e.Op == nil && b.Op != nil && shaOp(*b.Op) == b.D {
		e.Op = new(Op)
		*e.Op = *b.Op
	}
	n.logMu.Unlock()

	// execute in-order
	n.executeReady()
	n.onProgress()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// ---------- Collector → all : optimistic commit bundle ----------
func (n *Node) hOptimisticCommitBundle(w http.ResponseWriter, r *http.Request) {
	if n.isFailed && !allowWhenFailed[r.URL.Path] {
		http.Error(w, "failed", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var b OptimisticCommitBundle
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	n.vcMu.Lock()
	n.lastLeaderOK = time.Now()
	n.vcMu.Unlock()

	totalN := len(n.Replicas)
	if len(b.FromID) < totalN {
		http.Error(w, fmt.Sprintf("optimistic requires all %d votes, got %d", totalN, len(b.FromID)), http.StatusForbidden)
		return
	}

	if b.Op != nil && shaOp(*b.Op) != b.D {
		http.Error(w, "bundle op/digest mismatch", http.StatusForbidden)
		return
	}

	if err := n.verifyBundle("P", b.View, b.Seq, b.D, b.FromID, b.Votes); err != nil {
		http.Error(w, "invalid optimistic bundle: "+err.Error(), http.StatusForbidden)
		return
	}

	log.Printf("[%s] OPTIMISTIC PATH: received OptimisticCommitBundle for seq=%d with %d votes", n.ID, b.Seq, len(b.FromID))

	n.logMu.Lock()
	e := n.ensureEntry(b.View, b.Seq)
	e.D = b.D
	e.Status = SSCommitted
	n.bumpSeqNext(b.Seq)

	if e.PrepareVotes == nil {
		e.PrepareVotes = map[int][]byte{}
	}
	if e.CommitVotes == nil {
		e.CommitVotes = map[int][]byte{}
	}
	for i, id := range b.FromID {
		e.PrepareVotes[id] = b.Votes[i]
		e.CommitVotes[id] = b.Votes[i]
	}

	if e.Op == nil && b.Op != nil {
		e.Op = new(Op)
		*e.Op = *b.Op
	}
	n.logMu.Unlock()

	n.executeReady()
	n.onProgress()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "optimistic": true})
}

// ---------- Execution ----------
func (n *Node) executeReady() {
	for {
		n.logMu.Lock()
		e, ok := n.log[n.execNext]
		if !ok || e.Status < SSCommitted {
			n.logMu.Unlock()
			return
		}
		if e.Op == nil {
			// can't execute yet wait for Op via bundle
			n.logMu.Unlock()
			return
		}
		op := *e.Op
		n.logMu.Unlock()

		n.apply(op)

		n.logMu.Lock()
		e.Status = SSExecuted
		n.execNext++
		n.logMu.Unlock()
		n.onProgress()
	}
}

func (n *Node) apply(op Op) {
	if strings.ToUpper(op.From) != string(op.C) {
		return
	}
	if op.Amt <= 0 {
		return
	}
	if n.lastTau[string(op.C)] >= op.Tau {
		return
	}

	n.logMu.Lock()
	if n.dedup[string(op.C)] == nil {
		n.dedup[string(op.C)] = map[int64]bool{}
	}
	if n.dedup[string(op.C)][op.Tau] {
		n.logMu.Unlock()
		return
	}
	n.dedup[string(op.C)][op.Tau] = true

	fromBal := n.bank[op.From]
	if fromBal < op.Amt {
		n.logMu.Unlock()
		return // drop: insufficient funds
	}

	n.bank[op.From] = fromBal - op.Amt
	n.bank[op.To] = n.bank[op.To] + op.Amt
	n.lastTau[string(op.C)] = op.Tau
	n.logMu.Unlock()
}

func (n *Node) hFail(w http.ResponseWriter, r *http.Request) {
	n.isFailed = true
	w.WriteHeader(http.StatusOK)
}

func (n *Node) hRecover(w http.ResponseWriter, r *http.Request) {
	n.isFailed = false

	n.vcMu.Lock()
	n.lastProgress = time.Now()
	n.lastLeaderOK = time.Now()
	n.vcMu.Unlock()

	go func() {
		// 1) Learn the highest view from peers
		maxView := n.view
		for _, addr := range n.Replicas {
			var st StatusReply
			_ = getJSON(n.client, "http://"+addr+"/status", &st)
			if st.View > maxView {
				maxView = st.View
			}
		}
		n.vcMu.Lock()
		if maxView > n.view {
			n.view = maxView
			n.updatePrimaryFromView()
			n.lastProgress = time.Now()
		}
		n.vcMu.Unlock()

		// 2) Ask whichever node is primary to rebroadcast all committed entries
		for _, addr := range n.Replicas {
			_ = n.sendJSON(addr, "/resendCommitted", struct{}{}, nil)
		}
	}()

	w.WriteHeader(http.StatusOK)
}

func (n *Node) resendCommitted() {
	if !n.isPrimary() {
		return
	}

	n.logMu.Lock()
	type pair struct {
		s int
		e *Entry
	}
	commits := make([]pair, 0, len(n.log))
	for s, e := range n.log {
		if e.Status >= SSCommitted && e.Op != nil {
			commits = append(commits, pair{s, e})
		}
	}
	n.logMu.Unlock()

	for _, p := range commits {
		e := p.e

		var ids []int
		var votes [][]byte
		n.logMu.Lock()
		if len(e.CommitVotes) > 0 {
			ids = make([]int, 0, len(e.CommitVotes))
			for id := range e.CommitVotes {
				ids = append(ids, id)
			}
			sort.Ints(ids)
			votes = make([][]byte, 0, len(ids))
			for _, id := range ids {
				votes = append(votes, e.CommitVotes[id])
			}
		}
		cb := CommitBundle{
			View: e.View, Seq: p.s, D: e.D,
			Votes:  votes,
			FromID: ids,
			Op:     e.Op,
		}
		n.logMu.Unlock()

		for _, addr := range n.Replicas {
			_ = n.sendJSON(addr, "/commitBundle", cb, nil)
		}
	}
}

// POST /dark?to=HOST:PORT
func (n *Node) hDark(w http.ResponseWriter, r *http.Request) {
	to := r.URL.Query().Get("to")
	if to == "" {
		http.Error(w, "missing to", 400)
		return
	}
	n.setDark(to, true)
	writeJSON(w, 200, map[string]string{"ok": "dark"})
}

// POST /undark?to=HOST:PORT
func (n *Node) hUndark(w http.ResponseWriter, r *http.Request) {
	to := r.URL.Query().Get("to")
	if to == "" {
		http.Error(w, "missing to", 400)
		return
	}
	n.setDark(to, false)
	writeJSON(w, 200, map[string]string{"ok": "undark"})
}

// POST /delay?to=HOST:PORT&ms=123
func (n *Node) hDelay(w http.ResponseWriter, r *http.Request) {
	to := r.URL.Query().Get("to")
	msStr := r.URL.Query().Get("ms")
	ms, _ := strconv.Atoi(msStr)
	if to == "" {
		http.Error(w, "missing to", 400)
		return
	}
	n.setDelay(to, ms)
	writeJSON(w, 200, map[string]string{"ok": "delay"})
}

// POST /undelay?to=HOST:PORT
func (n *Node) hUndelay(w http.ResponseWriter, r *http.Request) {
	to := r.URL.Query().Get("to")
	if to == "" {
		http.Error(w, "missing to", 400)
		return
	}
	n.setDelay(to, 0)
	writeJSON(w, 200, map[string]string{"ok": "undelay"})
}

func (n *Node) hResendCommitted(w http.ResponseWriter, r *http.Request) {
	if n.isFailed && !allowWhenFailed[r.URL.Path] {
		http.Error(w, "failed", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !n.isPrimary() {
		http.Error(w, "not primary", http.StatusConflict)
		return
	}
	go n.resendCommitted()
	w.WriteHeader(http.StatusOK)
}
func postNoBody(addr, path string) {
	http.Post("http://"+addr+path, "text/plain", http.NoBody)
}

func (n *Node) verifyBundle(phase string, view View, seq int, d Digest, ids []int, sigs [][]byte) error {
	_, q := n.quorum()
	nn := len(n.Replicas)
	if len(ids) != len(sigs) {
		return fmt.Errorf("ids/sigs length mismatch")
	}
	if len(ids) < q {
		return fmt.Errorf("not enough votes: have=%d need=%d", len(ids), q)
	}
	seen := map[int]bool{}
	for i, id := range ids {
		if id <= 0 || id > nn {
			return fmt.Errorf("unknown replica id %d", id)
		}
		if seen[id] {
			return fmt.Errorf("duplicate vote from id %d", id)
		}
		seen[id] = true
		addr := n.Replicas[id]
		if !verifyPhaseWithAddr(phase, addr, view, seq, d, sigs[i]) {
			return fmt.Errorf("bad %s signature from id %d", phase, id)
		}
	}
	return nil
}

func appendUniqueInt(xs []int, v int) []int {
	for _, x := range xs {
		if x == v {
			return xs
		}
	}
	return append(xs, v)
}
