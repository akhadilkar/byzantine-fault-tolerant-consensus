package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Driver struct {
	addr     string
	expected int
	csvPath  string

	mu      sync.Mutex
	members []string
	index   map[string]int
	httpSrv *http.Server
	start   time.Time

	tauByClient map[string]int64

	idToAddr map[int]string
	addrToID map[string]int

	csvPaused bool
	csvNextCh chan struct{}
	csvJumpTo string
	csvIndex  int
}

func NewDriver(addr string, expected int, csvPath string) *Driver {
	return &Driver{
		addr: addr, expected: expected, csvPath: csvPath,
		index: make(map[string]int), start: time.Now(),
		tauByClient: map[string]int64{},
		idToAddr:    make(map[int]string),
		addrToID:    make(map[string]int),
		csvPaused:   true,
		csvNextCh:   make(chan struct{}, 1),
	}
}

func statusLabel(s int) string {
	switch s {
	case 0:
		return "X" // No status
	case 1:
		return "PP" // Pre-prepared
	case 2:
		return "P" // Prepared
	case 3:
		return "C" // Committed
	case 4:
		return "E" // Executed
	default:
		return fmt.Sprintf("?(%d)", s)
	}
}
func (d *Driver) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/register", d.hRegister)
	mux.HandleFunc("/members", d.hMembers)
	mux.HandleFunc("/leader", d.hLeader)

	d.httpSrv = &http.Server{Addr: d.addr, Handler: mux, ReadHeaderTimeout: 2 * time.Second}
	go func() {
		if err := d.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[driver] serve: %v", err)
		}
	}()

	go d.progressLoop()

	// When all nodes are up, run CSV if provided
	go func() {
		for {
			d.mu.Lock()
			ready := len(d.members) >= d.expected && d.expected > 0
			d.mu.Unlock()
			if ready {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if strings.TrimSpace(d.csvPath) != "" {
			d.runCSV(d.csvPath)

		}
	}()

	go d.startREPL()

	return nil
}

func (d *Driver) Stop(ctx context.Context) error { return d.httpSrv.Shutdown(ctx) }

// ---------- membership ----------

func (d *Driver) hRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	var req RegisterReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if id, ok := d.index[req.Addr]; ok {
		writeJSON(w, http.StatusOK, RegisterReply{ID: id, N: d.expected, Members: append([]string(nil), d.members...)})
		return
	}
	if len(d.members) >= d.expected {
		http.Error(w, "cluster full", http.StatusForbidden)
		return
	}
	// FIFO assignment: first arrival gets ID=1, next ID=2, ...
	d.members = append(d.members, req.Addr)
	d.index[req.Addr] = len(d.members)
	log.Printf("[driver] registered %s as id=%d (%d/%d up)", req.Addr, d.index[req.Addr], len(d.members), d.expected)
	id := d.index[req.Addr]
	addr := req.Addr
	d.idToAddr[id] = addr
	d.addrToID[addr] = id

	writeJSON(w, http.StatusOK, RegisterReply{ID: d.index[req.Addr], N: d.expected, Members: append([]string(nil), d.members...)})
}

func (d *Driver) hMembers(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	defer d.mu.Unlock()
	writeJSON(w, http.StatusOK, MembersReply{N: d.expected, Members: append([]string(nil), d.members...)})
}

func (d *Driver) hLeader(w http.ResponseWriter, r *http.Request) {
	addr := d.chooseLeaderAddr()
	d.mu.Lock()
	id := d.addrToID[addr]
	d.mu.Unlock()
	writeJSON(w, http.StatusOK, LeaderReply{ID: id, Addr: addr})
}

func (d *Driver) progressLoop() {
	t := time.NewTicker(1 * time.Second)
	defer t.Stop()
	for range t.C {
		d.mu.Lock()
		up := len(d.members)
		if up >= d.expected {
			log.Printf("[driver] all %d nodes up: %v", d.expected, d.members)
			d.mu.Unlock()
			return
		}
		upCopy := append([]string(nil), d.members...)
		waitIDs := make([]int, 0, d.expected-up)
		for i := up + 1; i <= d.expected; i++ {
			waitIDs = append(waitIDs, i)
		}
		d.mu.Unlock()

		log.Printf("[driver] %d/%d up; waiting for IDs: %v; up so far: %v", up, d.expected, waitIDs, upCopy)
	}
}
func (d *Driver) startREPL() {
	in := bufio.NewScanner(os.Stdin)
	fmt.Println("Driver REPL — commands: help | members | flush | submit A B 10 | db [all|addr] | status [all|addr] | sleep ms")
	for {
		fmt.Print("> ")
		if !in.Scan() {
			return
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		up := strings.ToUpper(line)
		if up == "" || up == "SET NUMBER" || up == "LIVE" || up == "ATTACK" || up == "COMMENT" {
			continue
		}
		if _, err := strconv.Atoi(up); err == nil {
			continue
		}
		fs := strings.Fields(line)
		switch strings.ToLower(fs[0]) {
		case "help":
			fmt.Println("members | active members | flush | submit A B 10 | db [all|addr] | status [all|addr] | sleep ms | leader | fail [id|addr] | recover <id|addr> | next | crash <id|addr> [on|off] | slot <seq> [all|id|addr] | exit")

		case "next", "continue", "n":
			if d.csvPaused {
				d.csvPaused = false
				select {
				case d.csvNextCh <- struct{}{}:
				default:
				}
				fmt.Println("[driver] continuing CSV...")
			} else {
				fmt.Println("[driver] CSV is not waiting at a set boundary")
			}

		case "members", "peers":
			d.mu.Lock()
			fmt.Printf("members (%d/%d): %v\n", len(d.members), d.expected, d.members)
			d.mu.Unlock()
		case "active members", "active peers", "active":
			d.cmdMembers(fs[1:])
		case "flush":
			d.broadcastFlush()

		case "set":
			if len(fs) != 2 {
				fmt.Println("usage: set <N>")
				break
			}

			// 1) hard reset the cluster state
			d.broadcastFlush()

			// 2) set jump target and REWIND the CSV cursor
			d.mu.Lock()
			d.csvJumpTo = fs[1]
			d.csvIndex = -1
			d.mu.Unlock()

			// 3) nudge the CSV runner in case it is waiting
			select {
			case d.csvNextCh <- struct{}{}:
			default:
			}

		case "submit":
			if len(fs) < 4 {
				fmt.Println("usage: submit <From> <To> <Amt>")
				continue
			}
			from, to := fs[1], fs[2]
			amt, err := strconv.ParseInt(fs[3], 10, 64)
			if err != nil {
				fmt.Println("bad amt")
				continue
			}
			tau := d.nextTau(strings.ToUpper(from))
			d.submitTransfer(from, to, amt, tau)
		case "db", "balance", "printDB", "printdb", "PrintDB":
			arg := ""
			if len(fs) >= 2 {
				arg = fs[1]
			}
			d.cmdDB(arg)
		case "status", "PrintStatus", "printstatus", "printStatus":
			arg := ""
			if len(fs) >= 2 {
				arg = fs[1]
			}
			d.cmdStatus(arg)

		case "views", "PrintView", "printView", "printview", "view":
			h := &http.Client{}
			d.mu.Lock()
			members := append([]string(nil), d.members...)
			d.mu.Unlock()
			for _, a := range members {
				var out struct {
					View     int   `json:"view"`
					Primary  int   `json:"primary"`
					Acks     []int `json:"acks"`
					NewViews []struct {
						View int   `json:"view"`
						From int   `json:"from"`
						Acks []int `json:"acks"`
					} `json:"newviews"`
				}
				if err := getJSON(h, fmt.Sprintf("http://%s/vc", a), &out); err == nil {
					fmt.Printf("VIEW @ %s: view=%d primaryID=%d acks=%v\n", a, out.View, out.Primary, out.Acks)
					for _, nv := range out.NewViews {
						fmt.Printf("  newview: view=%d from=%d acks=%v\n", nv.View, nv.From, nv.Acks)
					}
				} else {
					fmt.Printf("VIEW @ %s: error: %v\n", a, err)
				}
			}

		case "sleep":
			if len(fs) < 2 {
				fmt.Println("usage: sleep <ms>")
				continue
			}
			ms, _ := strconv.Atoi(fs[1])
			time.Sleep(time.Duration(ms) * time.Millisecond)

		case "leader":
			d.cmdLeader()

		case "log":
			arg := ""
			if len(fs) >= 2 {
				arg = fs[1]
			}
			d.cmdLog(arg)

		case "fail":
			// formats:
			//   fail           -> fail current leader
			//   fail <id>      -> fail node with that numeric id
			//   fail <addr>    -> fail node at that address (backward compatible)
			var addr string
			if len(fs) == 1 {
				// no args: default to current leader
				lid, laddr := d.leaderInfo()
				if lid == 0 {
					fmt.Println("cannot resolve leader")
					break
				}
				addr = laddr
				fmt.Printf("failing leader: id=%d addr=%s\n", lid, addr)
			} else if id, err := strconv.Atoi(fs[1]); err == nil {
				// argument is numeric id
				a, ok := d.addrForID(id)
				if !ok {
					fmt.Printf("unknown id %d\n", id)
					break
				}
				addr = a
				fmt.Printf("failing id=%d addr=%s\n", id, addr)
			} else {
				// treat as address
				addr = fs[1]
				if !strings.Contains(addr, ":") {
					fmt.Printf("unknown arg %q; use an id or addr like host:port\n", addr)
					break
				}
				fmt.Printf("failing addr=%s\n", addr)
			}
			_ = postJSON(&http.Client{Timeout: 800 * time.Millisecond}, addr, "/fail", map[string]any{}, nil)

		case "recover":
			// formats:
			//   recover <id>   -> recover node with that numeric id
			//   recover <addr> -> recover by address
			if len(fs) < 2 {
				fmt.Println("usage: recover <id|addr>")
				break
			}
			var addr string
			if id, err := strconv.Atoi(fs[1]); err == nil {
				a, ok := d.addrForID(id)
				if !ok {
					fmt.Printf("unknown id %d\n", id)
					break
				}
				addr = a
				fmt.Printf("recovering id=%d addr=%s\n", id, addr)
			} else {
				addr = fs[1]
				if !strings.Contains(addr, ":") {
					fmt.Printf("unknown arg %q; use an id or addr like host:port\n", addr)
					break
				}
				fmt.Printf("recovering addr=%s\n", addr)
			}
			_ = postJSON(&http.Client{Timeout: 800 * time.Millisecond}, addr, "/recover", map[string]any{}, nil)

		case "isolated", "dark":
			// Show which nodes are isolated
			h := &http.Client{Timeout: 500 * time.Millisecond}
			d.mu.Lock()
			members := append([]string(nil), d.members...)
			d.mu.Unlock()

			fmt.Println("Checking node connectivity...")
			for _, addr := range members {
				resp, err := h.Get("http://" + addr + "/ping")
				if err != nil || resp == nil || resp.StatusCode != 200 {
					fmt.Printf("  %s: ISOLATED or FAILED\n", addr)
				} else {
					fmt.Printf("  %s: CONNECTED\n", addr)
					resp.Body.Close()
				}
			}

		case "undark":
			if len(fs) < 3 {
				fmt.Println("usage: undark <from> <to>")
				break
			}
			faddr := d.normalizeAddr(fs[1])
			taddr := d.normalizeAddr(fs[2])
			postNoBody(faddr, "/undark?to="+url.QueryEscape(taddr))
			fmt.Printf("undark %s -> %s\n", faddr, taddr)

		case "delay":
			// delay <from> <to> <ms>
			if len(fs) < 4 {
				fmt.Println("usage: delay <from> <to> <ms>")
				break
			}
			if _, err := strconv.Atoi(fs[3]); err != nil {
				fmt.Println("delay: ms must be an integer")
				break
			}
			faddr := d.normalizeAddr(fs[1])
			taddr := d.normalizeAddr(fs[2])
			postNoBody(faddr, "/delay?to="+url.QueryEscape(taddr)+"&ms="+fs[3])
			fmt.Printf("delay %s -> %s = %sms\n", faddr, taddr, fs[3])

		case "undelay":
			if len(fs) < 3 {
				fmt.Println("usage: undelay <from> <to>")
				break
			}
			faddr := d.normalizeAddr(fs[1])
			taddr := d.normalizeAddr(fs[2])
			postNoBody(faddr, "/undelay?to="+url.QueryEscape(taddr))
			fmt.Printf("undelay %s -> %s\n", faddr, taddr)

		case "crash":
			if len(fs) < 2 {
				fmt.Println("usage: crash <id|addr> [on|off]")
				break
			}
			target := fs[1]
			on := true
			if len(fs) >= 3 {
				on = strings.ToLower(fs[2]) != "off"
			}
			// normalize id -> addr
			addr := d.normalizeAddr(target) // implement if not present; or reuse your idToAddr map
			if addr == "" {
				fmt.Printf("unknown node %q\n", target)
				break
			}
			if on {
				postNoBody(addr, "/fail")
				fmt.Printf("crashed %s\n", addr)
			} else {
				postNoBody(addr, "/recover")
				fmt.Printf("recovered %s\n", addr)
			}

		case "slot":
			if len(fs) < 2 {
				fmt.Println("usage: slot <seq> [all|id|addr]")
				break
			}
			seq := fs[1]
			var addrs []string
			if len(fs) == 2 || strings.ToLower(fs[2]) == "all" {
				d.mu.Lock()
				addrs = append(addrs, d.members...)
				d.mu.Unlock()
			} else if id, err := strconv.Atoi(fs[2]); err == nil {
				if a, ok := d.addrForID(id); ok {
					addrs = []string{a}
				} else {
					fmt.Printf("unknown id %d\n", id)
					break
				}
			} else {
				addrs = []string{d.normalizeAddr(fs[2])}
			}
			for _, a := range addrs {
				resp, err := http.Get("http://" + a + "/slot?seq=" + url.QueryEscape(seq))
				if err != nil || resp.StatusCode != 200 {
					if resp != nil {
						resp.Body.Close()
					}
					fmt.Printf("slot %s @ %s: error\n", seq, a)
					continue
				}
				var body map[string]any
				_ = json.NewDecoder(resp.Body).Decode(&body)
				resp.Body.Close()

				if statusNum, ok := body["status"].(float64); ok {
					body["status_label"] = statusLabel(int(statusNum))
				}

				// Check for optimistic path
				prepCount, _ := body["prepare_count"].(float64)
				commitCount, _ := body["commit_count"].(float64)
				totalN := 7 // or d.expected
				isOptimistic := int(prepCount) == totalN && int(prepCount) == int(commitCount)

				if isOptimistic {
					fmt.Printf("slot %s @ %s: OPTIMISTIC (all %d votes) %v\n", seq, a, int(prepCount), body)
				} else {
					fmt.Printf("slot %s @ %s: %v\n", seq, a, body)
				}
			}
		case "optimistic", "opr":
			fmt.Println("=== Optimistic Phase Reduction Status ===")
			fmt.Println("Checking if nodes are using optimistic mode...")

			h := &http.Client{}
			d.mu.Lock()
			members := append([]string(nil), d.members...)
			d.mu.Unlock()

			for i, addr := range members {
				// Check last transaction's vote counts
				url := fmt.Sprintf("http://%s/slot?seq=1", addr)
				var out map[string]interface{}
				if err := getJSON(h, url, &out); err == nil {
					prepCount, _ := out["prepare_count"].(float64)
					commitCount, _ := out["commit_count"].(float64)

					if prepCount == 7 && commitCount == 7 {
						fmt.Printf("  Node %d (%s): OPTIMISTIC MODE ACTIVE (collected all 7 votes)\n", i+1, addr)
					} else {
						fmt.Printf("  Node %d (%s): Normal mode (prepare=%d, commit=%d)\n", i+1, addr, int(prepCount), int(commitCount))
					}
				}
			}
		case "exit", "quit":
			fmt.Println("thx")
			os.Exit(0)
			return
		default:
			fmt.Println("unknown; type 'help'")
		}
	}
}

func (d *Driver) nextTau(client string) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.tauByClient[client]++
	return d.tauByClient[client]
}

func (d *Driver) leaderAddr() string {
	d.mu.Lock()
	members := append([]string(nil), d.members...)
	d.mu.Unlock()
	if len(members) == 0 {
		return ""
	}
	h := &http.Client{Timeout: 500 * time.Millisecond}
	for _, a := range members {
		var out LeaderReply
		if err := getJSON(h, fmt.Sprintf("http://%s/leader", a), &out); err == nil && out.Addr != "" {
			return out.Addr
		}
	}
	// Fallback: first member (best effort)
	return members[0]
}

func (d *Driver) broadcastFlush() {
	d.clearAttacks() // keep this
	h := &http.Client{Timeout: 1500 * time.Millisecond}

	d.mu.Lock()
	peers := append([]string(nil), d.members...)
	d.tauByClient = map[string]int64{}
	d.mu.Unlock()

	for _, addr := range peers {
		_ = postJSON(h, addr, "/flush", struct{}{}, nil)
	}
	d.recoverAllNodes()
}

func (d *Driver) clearAttacks() {
	d.mu.Lock()
	addrs := append([]string(nil), d.members...)
	d.mu.Unlock()

	// symmetric undark/undelay for all pairs, and disable equivocation/sign attacks
	for i := range addrs {
		for j := range addrs {
			if i == j {
				continue
			}
			postNoBody(addrs[i], "/undark?to="+url.QueryEscape(addrs[j]))
			postNoBody(addrs[i], "/undelay?to="+url.QueryEscape(addrs[j]))
		}
		postNoBody(addrs[i], "/byz?mode=equivocate&on=0")
		postNoBody(addrs[i], "/byz?mode=sign&on=1") // honest signing
	}
}

func (d *Driver) submitTransfer(from, to string, amt, tau int64) {
	// helper to POST once
	tryOnce := func(addr string) (*ClientReply, error) {
		req := ClientRequest{
			View: 0,
			Op: Op{
				From: from,
				To:   to,
				Amt:  amt,
				C:    ClientID(strings.ToUpper(from)),
				Tau:  tau,
			},
		}
		req.SigC = signClient(req.Op.C, req.Op) // sign the Op
		var out ClientReply
		cli := &http.Client{Timeout: 1500 * time.Millisecond}
		if err := postJSON(cli, addr, "/submit", req, &out); err != nil {
			return nil, err
		}
		return &out, nil
	}

	// 1) pick dynamic leader
	leader := d.chooseLeaderAddr()
	if leader == "" {
		log.Println("[driver] submit: no leader known yet")
		return
	}
	// avoid dead leader
	if !d.ping(leader) {
		leader = d.chooseLeaderAddr()
	}

	// 2) first attempt
	if out, err := tryOnce(leader); err == nil {
		log.Printf("[driver] OK seq=%d view=%d", out.Seq, out.View)
		return
	} else {
		log.Printf("[driver] submit error to %s: %v (will reselect)", leader, err)
	}

	// 3) reselect + retry once
	leader2 := d.chooseLeaderAddr()
	if leader2 != "" && leader2 != leader && d.ping(leader2) {
		if out, err := tryOnce(leader2); err == nil {
			log.Printf("[driver] OK seq=%d view=%d (after reselect)", out.Seq, out.View)
			return
		} else {
			log.Printf("[driver] submit retry error to %s: %v", leader2, err)
		}
	}

	// if both attempts failed with crashed errors, wait for view change
	log.Printf("[driver] leader appears crashed, waiting for view change...")
	time.Sleep(8 * time.Second) // Wait for view change timeout

	// Try again with new leader after view change
	leader3 := d.chooseLeaderAddr()
	if leader3 != "" && leader3 != leader && leader3 != leader2 {
		if out, err := tryOnce(leader3); err == nil {
			log.Printf("[driver] OK seq=%d view=%d (after view change)", out.Seq, out.View)
			return
		}
	}

	// 4) last resort: scan all live members
	d.mu.Lock()
	members := append([]string(nil), d.members...)
	d.mu.Unlock()
	for _, m := range members {
		if !d.ping(m) {
			continue
		}
		if out, err := tryOnce(m); err == nil {
			log.Printf("[driver] OK seq=%d view=%d (fallback %s)", out.Seq, out.View, m)
			return
		}
	}
	log.Printf("[driver] submit failed on all candidates (from=%s to=%s amt=%d tau=%d)", from, to, amt, tau)
}

func (d *Driver) cmdDB(arg string) {
	h := &http.Client{}

	if arg != "" && !strings.EqualFold(arg, "all") {
		if addr, ok := d.resolveAddrToken(arg); ok {
			arg = addr
		}
	}

	if arg == "" || strings.EqualFold(arg, "all") {
		d.mu.Lock()
		members := append([]string(nil), d.members...)
		d.mu.Unlock()
		for _, a := range members {
			d.printDBOne(h, a)
		}
		return
	}
	d.printDBOne(h, arg)
}

func (d *Driver) printDBOne(h *http.Client, addr string) {
	var db map[string]int64
	if err := getJSON(h, fmt.Sprintf("http://%s/db", addr), &db); err != nil {
		log.Printf("[driver] db %s: %v", addr, err)
		return
	}
	order := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	out := make([]string, 0, len(order))
	for _, k := range order {
		out = append(out, fmt.Sprintf("%s=%d", k, db[k]))
	}
	fmt.Printf("[driver] DB @ %s: %s"+"\n", addr, strings.Join(out, " "))
}

func (d *Driver) cmdStatus(arg string) {
	h := &http.Client{}

	if arg != "" && !strings.EqualFold(arg, "all") {
		if addr, ok := d.resolveAddrToken(arg); ok {
			arg = addr
		}
	}

	if arg == "" || strings.EqualFold(arg, "all") {
		d.mu.Lock()
		members := append([]string(nil), d.members...)
		d.mu.Unlock()
		for _, a := range members {
			d.printStatusOne(h, a)
		}
		return
	}
	d.printStatusOne(h, arg)
}

func (d *Driver) printStatusOne(h *http.Client, addr string) {
	var st StatusReply
	if err := getJSON(h, fmt.Sprintf("http://%s/status", addr), &st); err != nil {
		log.Printf("[driver] status %s: %v", addr, err)
		return
	}

	fmt.Printf("STATUS @ %s: view=%d primaryID=%d seqNext=%d execNext=%d\n",
		addr, st.View, st.Primary, st.SeqNext, st.ExecNext)

}

// Run the CSV grouped by "Set Number".
// Two pauses per set:
//  1. next -> run the whole set (apply LIVE, then all Transactions)
//  2. next -> FLUSH, recover all nodes, continue to next set
//  3. can type the set number to run that set directly
//  4. first commmand irrespective will always run the first set, then you can change the set
func (d *Driver) runCSV(path string) {
	d.mu.Lock()
	d.csvIndex = -1 // no set has been executed yet
	d.mu.Unlock()

	f, err := os.Open(path)
	if err != nil {
		log.Printf("[driver] csv open: %v", err)
		return
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if err != nil {
		log.Printf("[driver] csv header: %v", err)
		return
	}

	// header indexes
	idx := map[string]int{}
	for i, h := range header {
		idx[strings.ToUpper(strings.TrimSpace(h))] = i
	}
	colSet := idx["SET NUMBER"]
	colTx := idx["TRANSACTIONS"]
	colLive := idx["LIVE"]
	colByz := idx["BYZANTINE"]
	colAtk := idx["ATTACK"]

	type csvSet struct {
		label   string
		live    []int
		byz     []int
		attacks []string
		txns    []string
	}

	var sets []csvSet
	var cur *csvSet

	// Group contiguous rows by Set Number
	for {
		rec, err := r.Read()
		if err == io.EOF {
			if cur != nil {
				sets = append(sets, *cur)
			}
			break
		}
		if err != nil {
			log.Printf("[driver] csv read: %v", err)
			return
		}
		for i := range rec {
			rec[i] = strings.TrimSpace(rec[i])
		}

		label := ""
		if colSet >= 0 && colSet < len(rec) {
			label = rec[colSet]
		}
		if label != "" {
			if cur != nil {
				sets = append(sets, *cur)
			}
			cur = &csvSet{label: label}
		}
		if cur == nil {
			continue
		}

		// pick LIVE/BYZ/ATTACK first time they appear within this set
		if len(cur.live) == 0 && colLive >= 0 && colLive < len(rec) {
			if v := rec[colLive]; v != "" {
				cur.live = parseNList(v)
			}
		}
		if len(cur.byz) == 0 && colByz >= 0 && colByz < len(rec) {
			if v := rec[colByz]; v != "" {
				cur.byz = parseNList(v)
			}
		}
		if len(cur.attacks) == 0 && colAtk >= 0 && colAtk < len(rec) {
			if v := rec[colAtk]; v != "" {
				if len(v) >= 2 && v[0] == '[' && v[len(v)-1] == ']' {
					v = v[1 : len(v)-1]
				}
				if v != "" {
					cur.attacks = strings.Split(v, ";")
					for i := range cur.attacks {
						cur.attacks[i] = strings.TrimSpace(cur.attacks[i])
					}
				}
			}
		}

		if colTx >= 0 && colTx < len(rec) && rec[colTx] != "" {
			cur.txns = append(cur.txns, rec[colTx])
		}
	}
	if len(sets) == 0 {
		log.Printf("[driver] csv: no sets found")
		return
	}

	// Helpers for a row "(A,B,1)" or "(E)"
	parseTxn := func(s string) (kind, a, b string, amt int64) {
		s = strings.TrimSpace(s)
		if s == "" {
			return "", "", "", 0
		}
		if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
			inner := strings.TrimSpace(s[1 : len(s)-1])
			parts := strings.Split(inner, ",")
			for i := range parts {
				parts[i] = strings.TrimSpace(parts[i])
			}
			if len(parts) == 1 && len(parts[0]) == 1 {
				return "balance", strings.ToUpper(parts[0]), "", 0
			}
			if len(parts) == 3 {
				amt64, _ := strconv.ParseInt(parts[2], 10, 64)
				return "transfer", strings.ToUpper(parts[0]), strings.ToUpper(parts[1]), amt64
			}
		}
		return "", "", "", 0
	}

	for {
		d.mu.Lock()
		currentIdx := d.csvIndex
		jump := d.csvJumpTo
		d.mu.Unlock()

		nextIdx := currentIdx + 1

		if nextIdx >= len(sets) {
			fmt.Println("[driver] All sets completed!")
			return
		}

		s := sets[nextIdx]

		if jump != "" && s.label != jump {
			d.mu.Lock()
			d.csvIndex = nextIdx
			d.mu.Unlock()
			continue
		}

		if jump != "" && s.label == jump {
			d.mu.Lock()
			d.csvJumpTo = ""
			d.csvIndex = nextIdx
			d.mu.Unlock()
			fmt.Printf("[driver] Jumped to Set %q with %d txn(s). Executing now...\n", s.label, len(s.txns))
		} else {
			fmt.Printf("[driver] Ready to run Set %q with %d txn(s). Type: next\n", s.label, len(s.txns))
			d.mu.Lock()
			d.csvIndex = nextIdx
			d.mu.Unlock()
			d.waitCSV()
		}

		if len(s.live) > 0 {
			d.enforceLive(s.live)
			time.Sleep(150 * time.Millisecond)
		}
		// Byzantine behavior modes (crash, sign, equivocation) are handled by configureByzantine
		if len(s.byz) > 0 {
			d.configureByzantine(s.byz, s.attacks)
			time.Sleep(200 * time.Millisecond)
		}

		// for network-level attacks dark, delay
		for _, atk := range s.attacks {
			parts := strings.Fields(atk)
			if len(parts) == 0 {
				continue
			}
			// compact tokens support
			tok := parts[0]

			// time or time(ms)
			if strings.HasPrefix(tok, "time") {
				ms := 1200
				if i := strings.IndexByte(tok, '('); i != -1 && strings.HasSuffix(tok, ")") {
					v := tok[i+1 : len(tok)-1]
					if vv, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
						ms = vv
					}
				}
				time.Sleep(time.Duration(ms) * time.Millisecond)
				continue
			}

			if strings.Contains(tok, "(") && strings.HasSuffix(tok, ")") {
				tokHead := strings.ToLower(parts[0])
				if strings.HasPrefix(tokHead, "dark(") || strings.HasPrefix(tokHead, "undark(") ||
					strings.HasPrefix(tokHead, "equivocation(") || strings.HasPrefix(tokHead, "sign(") {

					paren := tokHead[strings.Index(tokHead, "("):]
					ids, err := parseNListStrict(paren)
					if err != nil {
						log.Printf("[csv] bad arg in %q: %v", atk, err)
						continue
					}

					switch {
					case strings.HasPrefix(tokHead, "dark("):
						for _, id := range ids {
							if addr, ok := d.addrForID(id); ok {
								postNoBody(addr, "/dark")
							} else {
								fmt.Printf("[csv] unknown id n%d in %q\n", id, atk)
							}
						}
					case strings.HasPrefix(tokHead, "undark("):
						for _, id := range ids {
							if addr, ok := d.addrForID(id); ok {
								postNoBody(addr, "/undark")
							} else {
								fmt.Printf("[csv] unknown id n%d in %q\n", id, atk)
							}
						}

					}
					continue
				}
			}

			switch parts[0] {
			case "dark", "undark":
				if len(parts) < 2 || !strings.Contains(parts[1], "->") {
					fmt.Printf("[csv] bad %s spec: %q\n", parts[0], atk)
					continue
				}
				fromTo := strings.SplitN(parts[1], "->", 2)
				fromID, err1 := strconv.Atoi(strings.TrimSpace(fromTo[0]))
				toID, err2 := strconv.Atoi(strings.TrimSpace(fromTo[1]))
				if err1 != nil || err2 != nil {
					fmt.Printf("[csv] bad ids in %q\n", atk)
					continue
				}
				fromAddr, okF := d.addrForID(fromID)
				toAddr, okT := d.addrForID(toID)
				if !okF || !okT {
					fmt.Printf("[csv] unknown id(s) in %q\n", atk)
					continue
				}
				postNoBody(fromAddr, "/"+parts[0]+"?to="+url.QueryEscape(toAddr))

			case "delay":
				if len(parts) < 3 || !strings.Contains(parts[1], "->") {
					fmt.Printf("[csv] bad delay spec: %q\n", atk)
					continue
				}
				fromTo := strings.SplitN(parts[1], "->", 2)
				fromID, err1 := strconv.Atoi(strings.TrimSpace(fromTo[0]))
				toID, err2 := strconv.Atoi(strings.TrimSpace(fromTo[1]))
				ms, err3 := strconv.Atoi(strings.TrimSpace(parts[2]))
				if err1 != nil || err2 != nil || err3 != nil {
					fmt.Printf("[csv] bad delay args in %q\n", atk)
					continue
				}
				fromAddr, okF := d.addrForID(fromID)
				toAddr, okT := d.addrForID(toID)
				if !okF || !okT {
					fmt.Printf("[csv] unknown id(s) in %q\n", atk)
					continue
				}
				postNoBody(fromAddr, "/delay?to="+url.QueryEscape(toAddr)+"&ms="+strconv.Itoa(ms))

			case "undelay":
				if len(parts) < 2 || !strings.Contains(parts[1], "->") {
					fmt.Printf("[csv] bad undelay spec: %q\n", atk)
					continue
				}
				fromTo := strings.SplitN(parts[1], "->", 2)
				fromID, err1 := strconv.Atoi(strings.TrimSpace(fromTo[0]))
				toID, err2 := strconv.Atoi(strings.TrimSpace(fromTo[1]))
				if err1 != nil || err2 != nil {
					fmt.Printf("[csv] bad ids in %q\n", atk)
					continue
				}
				fromAddr, okF := d.addrForID(fromID)
				toAddr, okT := d.addrForID(toID)
				if !okF || !okT {
					fmt.Printf("[csv] unknown id(s) in %q\n", atk)
					continue
				}
				postNoBody(fromAddr, "/undelay?to="+url.QueryEscape(toAddr))

			default:
				fmt.Printf("[csv] ignoring attack token %q (handled elsewhere or not needed)\n", parts[0])
			}
		}

		for _, raw := range s.txns {
			kind, a, b, amt := parseTxn(raw)
			switch kind {
			case "transfer":
				tau := d.nextTau(a)
				d.submitTransfer(a, b, amt, tau)
				time.Sleep(100 * time.Millisecond)
			case "balance":
				time.Sleep(200 * time.Millisecond)
				if v, ok := d.submitBalanceQuorum(a); ok {
					fmt.Printf("[driver] BALANCE[%s]=%d\n", a, v)
				} else {
					fmt.Printf("[driver] BALANCE[%s]=<no quorum>\n", a)
				}
			default:
				log.Printf("[driver] set %q: unknown txn %q", s.label, raw)
			}
		}
		fmt.Printf("[driver] Set %q executed. Type: next to FLUSH and continue\n", s.label)
		d.waitCSV()

		// flush between sets
		d.broadcastFlush()
	}

}

// Block until REPL 'next' or 'set <n>'
func (d *Driver) waitCSV() {
	d.mu.Lock()
	ch := d.csvNextCh
	d.csvPaused = true
	d.mu.Unlock()

	if ch != nil {
		<-ch
	}

	d.mu.Lock()
	d.csvPaused = false
	d.mu.Unlock()
}

func (d *Driver) cmdLeader() {
	addr := d.chooseLeaderAddr()
	if addr == "" {
		fmt.Println("leader unknown (cluster not ready?)")
		return
	}

	d.mu.Lock()
	id := -1
	for i, m := range d.members {
		if m == addr {
			id = i + 1 // 1-based id
			break
		}
	}
	d.mu.Unlock()

	if id < 1 {
		fmt.Printf("leader: addr=%s (dynamic; id unknown)\n", addr)
		return
	}
	fmt.Printf("leader: id=%d addr=%s (dynamic)\n", id, addr)
}

func (d *Driver) addrForID(id int) (string, bool) {
	a, ok := d.idToAddr[id]
	return a, ok
}

func (d *Driver) resolveAddrToken(tok string) (string, bool) {
	tok = strings.TrimSpace(tok)
	if tok == "" {
		return "", false
	}
	if n, err := strconv.Atoi(tok); err == nil {
		return d.addrForID(n)
	}
	if strings.Contains(tok, ":") || strings.Contains(tok, ".") {
		return tok, true
	}
	return "", false
}

func parseNListStrict(s string) ([]int, error) {

	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "(")
	s = strings.TrimSuffix(s, ")")
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	s = strings.ReplaceAll(s, " ", "")
	if s == "" {
		return nil, fmt.Errorf("empty list")
	}
	parts := strings.Split(s, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if !strings.HasPrefix(strings.ToLower(p), "n") {
			return nil, fmt.Errorf("bad token %q", p)
		}
		id, err := strconv.Atoi(p[1:])
		if err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, nil
}

func parseNList(s string) []int {
	ids, err := parseNListStrict(s)
	if err != nil {
		return nil
	}
	return ids
}

func (d *Driver) leaderInfo() (id int, addr string) {
	h := &http.Client{Timeout: 800 * time.Millisecond}
	d.mu.Lock()
	nodes := append([]string(nil), d.members...)
	d.mu.Unlock()
	for _, a := range nodes {
		var out LeaderReply
		if err := getJSON(h, "http://"+a+"/leader", &out); err == nil && out.ID != 0 {
			return out.ID, out.Addr
		}
	}
	return 0, ""
}
func (d *Driver) cmdMembers(args []string) {
	showAll := len(args) > 0 && strings.ToLower(args[0]) == "all"
	d.mu.Lock()
	addrs := append([]string(nil), d.members...)
	d.mu.Unlock()

	h := &http.Client{Timeout: 400 * time.Millisecond}
	live := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if showAll {
			live = append(live, a)
			continue
		}
		resp, err := h.Get("http://" + a + "/ping")
		if err == nil && resp.StatusCode == http.StatusOK {
			live = append(live, a)
		}
		if resp != nil {
			resp.Body.Close()
		}
	}

	fmt.Printf("members (%d/%d): [", len(live), len(addrs))
	for i, a := range live {
		id := d.addrToID[a]
		if i > 0 {
			fmt.Print(" ")
		}
		if id != 0 {
			fmt.Printf("%s(id=%d)", a, id)
		} else {
			fmt.Printf("%s", a)
		}
	}
	fmt.Println("]")
}

func (d *Driver) fetchStatus(addr string) (nodeStatus, bool) {
	var s nodeStatus
	req, _ := http.NewRequest("GET", "http://"+addr+"/status", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		if resp != nil {
			resp.Body.Close()
		}
		return s, false
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return s, false
	}
	return s, true
}

func (d *Driver) chooseLeaderAddr() string {
	d.mu.Lock()
	members := append([]string(nil), d.members...)
	d.mu.Unlock()
	if len(members) == 0 {
		return d.leaderAddr()
	}

	viewCount := map[int]int{}
	for _, m := range members {
		if st, ok := d.fetchStatus(m); ok {
			viewCount[st.View]++
		}
	}

	modeView, best := 0, -1
	for v, c := range viewCount {
		if c > best {
			best, modeView = c, v
		}
	}
	if best <= 0 {
		return d.leaderAddr()
	}

	primaryID := (modeView % len(members)) + 1
	d.mu.Lock()
	addr := d.idToAddr[primaryID]
	d.mu.Unlock()
	if addr == "" {
		return d.leaderAddr()
	}
	return addr

}

func (d *Driver) ping(addr string) bool {
	resp, err := http.Get("http://" + addr + "/ping")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
func (d *Driver) normalizeAddr(x string) string {
	if id, err := strconv.Atoi(x); err == nil {
		d.mu.Lock()
		defer d.mu.Unlock()
		return d.idToAddr[id]
	}
	return x
}

func (d *Driver) recoverAllNodes() {
	d.mu.Lock()
	addrs := make([]string, 0, len(d.idToAddr))
	for _, addr := range d.idToAddr {
		if strings.TrimSpace(addr) != "" {
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) == 0 && len(d.members) > 0 {
		addrs = append(addrs, d.members...)
	}
	d.mu.Unlock()

	h := &http.Client{Timeout: 1500 * time.Millisecond}
	for _, addr := range addrs {
		base := httpBase(addr)
		_ = postJSON(h, base, "/recover", map[string]any{}, nil)
		time.Sleep(20 * time.Millisecond)
	}
}

func httpBase(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr
	}
	return "http://" + addr
}

func (d *Driver) enforceLive(alive []int) {
	if len(alive) == 0 {
		return
	}
	log.Printf("[driver] enforceLive: setting live nodes to %v", alive)

	aliveSet := make(map[int]bool, len(alive))
	for _, id := range alive {
		aliveSet[id] = true
	}

	d.mu.Lock()
	id2addr := make(map[int]string, len(d.idToAddr))
	for id, addr := range d.idToAddr {
		id2addr[id] = addr
	}
	d.mu.Unlock()

	h := &http.Client{Timeout: 1500 * time.Millisecond}

	for id, addr := range id2addr {
		if addr == "" {
			continue
		}
		if aliveSet[id] {
			resp, err := h.Post("http://"+addr+"/recover", "text/plain", http.NoBody)
			if err != nil {
				log.Printf("[driver] enforceLive: failed to recover node %d (%s): %v", id, addr, err)
			} else {
				resp.Body.Close()
				log.Printf("[driver] enforceLive: recovered node %d (%s)", id, addr)
			}
		} else {
			resp, err := h.Post("http://"+addr+"/fail", "text/plain", http.NoBody)
			if err != nil {
				log.Printf("[driver] enforceLive: failed to fail node %d (%s): %v", id, addr, err)
			} else {
				resp.Body.Close()
				log.Printf("[driver] enforceLive: failed node %d (%s)", id, addr)
			}
		}
	}
	effectiveN := len(alive)
	for _, liveID := range alive {
		addr := id2addr[liveID]
		if addr == "" {
			continue
		}
		url := fmt.Sprintf("http://%s/setN?n=%d", addr, effectiveN)
		resp, err := h.Get(url)
		if err == nil {
			resp.Body.Close()
		}
	}
}

func (d *Driver) cmdLog(arg string) {
	h := &http.Client{Timeout: 1500 * time.Millisecond}

	d.mu.Lock()
	addrs := append([]string(nil), d.members...)
	d.mu.Unlock()

	if arg == "" || strings.EqualFold(arg, "all") {
		for _, addr := range addrs {
			d.printLogOne(h, addr)
		}
		return
	}

	if addr, ok := d.resolveAddrToken(arg); ok {
		d.printLogOne(h, addr)
		return
	}

	d.printLogOne(h, arg)
}

func (d *Driver) printLogOne(h *http.Client, addr string) {
	type entry struct {
		Seq         int    `json:"seq"`
		View        int    `json:"view"`
		Status      int    `json:"status"`
		Digest      string `json:"digest"`
		Op          *Op    `json:"op"`
		PrepareFrom []int  `json:"prepare_from"`
		CommitFrom  []int  `json:"commit_from"`
		BadSigFrom  []int  `json:"bad_sig_from,omitempty"`
	}
	var out struct {
		Entries []entry `json:"entries"`
	}
	if err := getJSON(h, fmt.Sprintf("http://%s/log", addr), &out); err != nil {
		log.Printf("[driver] log %s: %v", addr, err)
		return
	}
	// status label (same labels as node)
	statusName := func(s int) string {
		switch s {
		case 1:
			return "pre-prepared"
		case 2:
			return "prepared"
		case 3:
			return "committed"
		case 4:
			return "executed"
		default:
			return fmt.Sprintf("unknown(%d)", s)
		}
	}
	fmt.Printf("LOG @ %s:\n", addr)
	for _, e := range out.Entries {
		// Detect optimistic path: all prepare votes collected (7) and prepare count equals commit count
		totalN := d.expected // or make it dynamic based on d.expected
		isOptimistic := len(e.PrepareFrom) == totalN && len(e.PrepareFrom) == len(e.CommitFrom)

		if isOptimistic {
			fmt.Printf("  slot %d: view=%d status=%s OPTIMISTIC PATH (all %d votes) digest=%s\n",
				e.Seq, e.View, statusName(e.Status), len(e.PrepareFrom), e.Digest)
		} else {
			fmt.Printf("  slot %d: view=%d status=%s digest=%s\n",
				e.Seq, e.View, statusName(e.Status), e.Digest)
		}

		if e.Op != nil {
			fmt.Printf("    op: %s -> %s amt=%d tau=%d c=%s\n", e.Op.From, e.Op.To, e.Op.Amt, e.Op.Tau, e.Op.C)
		} else {
			fmt.Println("    op: <nil>")
		}

		if isOptimistic {
			fmt.Printf("    votes=%v (%d) - optimistic commit\n", e.PrepareFrom, len(e.PrepareFrom))
		} else {
			fmt.Printf("    prepares=%v (%d)\n", e.PrepareFrom, len(e.PrepareFrom))
			fmt.Printf("    commits=%v (%d)\n", e.CommitFrom, len(e.CommitFrom))
		}

		if len(e.BadSigFrom) > 0 {
			fmt.Printf("    rejects(badsig): %v\n", e.BadSigFrom)
		}
	}
}

func (d *Driver) submitBalanceQuorum(acct string) (int64, bool) {
	d.mu.Lock()
	members := append([]string(nil), d.members...)
	d.mu.Unlock()

	type resp struct {
		v   int64
		ok  bool
		err error
	}
	ch := make(chan resp, len(members))
	var wg sync.WaitGroup

	for _, addr := range members {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()

			r, err := http.Get("http://" + a + "/balance?acct=" + url.QueryEscape(acct))
			if err != nil {
				ch <- resp{0, false, err}
				return
			}
			defer r.Body.Close()
			if r.StatusCode != http.StatusOK {
				ch <- resp{0, false, fmt.Errorf("status %d", r.StatusCode)}
				return
			}
			var m map[string]int64
			if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
				ch <- resp{0, false, err}
				return
			}
			ch <- resp{m[strings.ToUpper(acct)], true, nil}
		}(addr)
	}

	go func() { wg.Wait(); close(ch) }()

	need := quorumN(len(members))
	counts := map[int64]int{}
	for r := range ch {
		if !r.ok {
			continue
		}
		counts[r.v]++
		if counts[r.v] >= need {
			return r.v, true
		}
	}
	return 0, false
}

func (d *Driver) configureByzantine(byzIDs []int, attacks []string) {
	if len(byzIDs) == 0 {
		return
	}

	d.mu.Lock()
	id2addr := make(map[int]string)
	for id, addr := range d.idToAddr {
		id2addr[id] = addr
	}
	d.mu.Unlock()

	h := &http.Client{Timeout: 1500 * time.Millisecond}

	for _, byzID := range byzIDs {
		addr := id2addr[byzID]
		if addr == "" {
			continue
		}

		for _, attack := range attacks {
			attack = strings.ToLower(strings.TrimSpace(attack))

			if attack == "crash" {
				url := fmt.Sprintf("http://%s/byz?mode=crash&on=1", addr)
				resp, err := h.Get(url)
				if err == nil {
					resp.Body.Close()
					log.Printf("[driver] configured node %d (%s) for crash attack", byzID, addr)
				} else {
					log.Printf("[driver] failed to configure crash on node %d: %v", byzID, err)
				}

			} else if attack == "sign" {
				url := fmt.Sprintf("http://%s/byz?mode=sign&on=0", addr)
				resp, err := h.Get(url)
				if err == nil {
					resp.Body.Close()
					log.Printf("[driver] configured node %d (%s) for invalid signature attack", byzID, addr)
				} else {
					log.Printf("[driver] failed to configure sign on node %d: %v", byzID, err)
				}

			} else if strings.HasPrefix(attack, "equivocation") {
				url := fmt.Sprintf("http://%s/byz?mode=equivocate&on=1", addr)
				resp, err := h.Get(url)
				if err == nil {
					resp.Body.Close()
					log.Printf("[driver] configured node %d (%s) for equivocation attack", byzID, addr)
				} else {
					log.Printf("[driver] failed to configure equivocation on node %d: %v", byzID, err)
				}

				start := strings.Index(attack, "(")
				end := strings.Index(attack, ")")
				if start > 0 && end > start {
					targetsStr := attack[start+1 : end]

					log.Printf("[driver] equivocation targets for node %d: %s", byzID, targetsStr)
				}

			} else if strings.HasPrefix(attack, "dark") {
				start := strings.Index(attack, "(")
				end := strings.Index(attack, ")")
				if start > 0 && end > start {
					targets := strings.Split(attack[start+1:end], ",")
					for _, t := range targets {
						t = strings.TrimSpace(t)
						t = strings.TrimPrefix(strings.ToLower(t), "n")
						if targetID, err := strconv.Atoi(t); err == nil {
							targetAddr := id2addr[targetID]
							if targetAddr != "" {
								url := fmt.Sprintf("http://%s/dark?to=%s", addr, targetAddr)
								resp, err := h.Post(url, "text/plain", http.NoBody)
								if err == nil {
									resp.Body.Close()
									log.Printf("[driver] configured node %d (%s) dark to node %d (%s)", byzID, addr, targetID, targetAddr)
								} else {
									log.Printf("[driver] failed to configure dark: %v", err)
								}
							}
						}
					}
				}
			} else if attack == "time" {
				url := fmt.Sprintf("http://%s/byz?mode=time&on=1&ms=800", addr)
				resp, err := h.Get(url)
				if err == nil {
					resp.Body.Close()
					log.Printf("[driver] configured node %d (%s) for timing attack (800ms delay)", byzID, addr)
				} else {
					log.Printf("[driver] failed to configure timing on node %d: %v", byzID, err)
				}
			}
		}
	}
}
