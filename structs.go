package main

import (
	"crypto/ed25519"
)

type View uint64
type Digest [32]byte
type ClientID string

type SlotStatus int

const (
	SSNone        SlotStatus = iota
	SSPrePrepared            // PP
	SSPrepared               // P
	SSCommitted              // C
	SSExecuted               // E
)

type Op struct {
	From string   `json:"from"`
	To   string   `json:"to"`
	Amt  int64    `json:"amt"`
	C    ClientID `json:"c"`
	Tau  int64    `json:"tau"`
}

type Entry struct {
	View   View
	Seq    int
	Op     *Op
	D      Digest
	Status SlotStatus

	PrepareVotes   map[int][]byte
	CommitVotes    map[int][]byte
	Rejects        []string
	BadSigFrom     []int `json:"bad_sig_from,omitempty"`
	PrePrepareFrom int   `json:"preprepare_from,omitempty"`
	BundleSent     bool  `json:"bundle_sent,omitempty"`
}

type NodeKeys struct {
	Pub  ed25519.PublicKey
	Priv ed25519.PrivateKey
}

type ReplicaInfo struct {
	ID   int
	Addr string
	Pub  ed25519.PublicKey
}
type Slot struct {
	View   View
	Seq    int
	D      Digest
	Op     *Op
	Status SlotStatus // SSNone, SSPrePrepared, SSPrepared, SSCommitted

	PrepareFrom map[int]bool // replica id -> true
	CommitFrom  map[int]bool

	Executed bool // applied to DB
}

// --------- Messages ---------

// Client → leader
type ClientRequest struct {
	View View   `json:"view"`
	Op   Op     `json:"op"`
	SigC []byte `json:"sigc,omitempty"`
}

type ClientReply struct {
	OK   bool             `json:"ok"`
	Seq  int              `json:"seq"`
	View View             `json:"view"`
	DB   map[string]int64 `json:"db,omitempty"`
	Err  string           `json:"err,omitempty"`
}

// Primary → replicas
type PrePrepare struct {
	View View   `json:"view"`
	Seq  int    `json:"seq"`
	D    Digest `json:"d"`
	Op   Op     `json:"op"`
	SigL []byte `json:"sigl"`
}

type OptimisticCommitBundle struct {
	View   View     `json:"view"`
	Seq    int      `json:"seq"`
	D      Digest   `json:"d"`
	Votes  [][]byte `json:"votes"`
	FromID []int    `json:"from_id"`
	Op     *Op      `json:"op,omitempty"`
}
type PrePrepareAck struct {
	OK bool `json:"ok"`
}

// Replica → collector (leader)
type PrepareVote struct {
	View View   `json:"view"`
	Seq  int    `json:"seq"`
	D    Digest `json:"d"`
	From int    `json:"from"`
	Sig  []byte `json:"sig"`
}

// Collector → all
type PrepareBundle struct {
	View   View     `json:"view"`
	Seq    int      `json:"seq"`
	D      Digest   `json:"d"`
	Votes  [][]byte `json:"votes"`
	FromID []int    `json:"from_id"`
	Op     *Op      `json:"op,omitempty"`
}

// Commit phase
type CommitVote struct {
	View View   `json:"view"`
	Seq  int    `json:"seq"`
	D    Digest `json:"d"`
	From int    `json:"from"`
	Sig  []byte `json:"sig"`
}
type CommitBundle struct {
	View   View     `json:"view"`
	Seq    int      `json:"seq"`
	D      Digest   `json:"d"`
	Votes  [][]byte `json:"votes"`
	FromID []int    `json:"from_id"`
	Op     *Op      `json:"op,omitempty"`
}

// Diagnostics
type StatusReply struct {
	ID       string `json:"id"`
	Addr     string `json:"addr"`
	View     View   `json:"view"`
	Primary  int    `json:"primary"`
	Uptime   int64  `json:"uptime_ms"`
	SeqNext  int    `json:"seq_next"`
	ExecNext int    `json:"exec_next"`
	N        int    `json:"n"`
	F        int    `json:"f"`
}

// ----- Driver discovery messages -----
type RegisterReq struct {
	Addr string `json:"addr"`
}
type RegisterReply struct {
	ID      int      `json:"id"`
	N       int      `json:"n"`
	Members []string `json:"members"`
}
type MembersReply struct {
	N       int      `json:"n"`
	Members []string `json:"members"`
}
type LeaderReply struct {
	ID   int    `json:"id"`
	Addr string `json:"addr"`
}

// ---- View-change messages ----
type ViewChangeMsg struct {
	To       View           `json:"to"`
	From     int            `json:"from"`
	Sig      []byte         `json:"sig"`
	Prepared []PreparedInfo `json:"prepared,omitempty"`
}

type PreparedInfo struct {
	View View   `json:"view"`
	Seq  int    `json:"seq"`
	D    Digest `json:"d"`
	Op   *Op    `json:"op,omitempty"`
}

type NewViewMsg struct {
	View View   `json:"view"` // established view
	From int    `json:"from"` // sender (new primary)
	Acks []int  `json:"acks"` // ids that sent VC for this view (diagnostic)
	Sig  []byte `json:"sig"`  // sign("NV", View, 0, zeroDigest)
}

func quorumN(n int) int {
	// PBFT with 3f+1 replicas
	if n <= 0 {
		return 1
	}
	f := (n - 1) / 3
	return 2*f + 1
}

type NewViewHistory struct {
	View   View  `json:"view"`
	From   int   `json:"from"`   // who sent the NV
	Acks   []int `json:"acks"`   // which VC senders were included
	WhenMs int64 `json:"whenMs"` // for ordering/debug
}
type nodeStatus struct {
	View int `json:"view"`
}
