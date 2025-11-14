package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type Client struct {
	httpc  *http.Client
	leader string
}

func NewClient(leaderAddr string) *Client { return &Client{httpc: &http.Client{}, leader: leaderAddr} }

func (c *Client) SubmitTransfer(cid string, tau int64, from, to string, amt int64) (*ClientReply, error) {
	req := ClientRequest{View: 0, Op: Op{From: from, To: to, Amt: amt, C: ClientID(cid), Tau: tau}}
	req.SigC = signClient(req.Op.C, req.Op)
	//req.SigC = signClient(req.Op.C, req.Op) //above line was commmented in here and in client * submit as well
	b, _ := json.Marshal(req)

	url := fmt.Sprintf("http://%s/submit", c.leader)
	resp, err := c.httpc.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out ClientReply
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	// fetch db from leader for demo
	db := map[string]int64{}
	_ = getJSON(c.httpc, fmt.Sprintf("http://%s/db", c.leader), &db)
	out.DB = db
	return &out, nil
}

func (c *Client) QueryBalance(acct string) (*struct{ Balance int64 }, error) {
	var db map[string]int64
	if err := getJSON(c.httpc, fmt.Sprintf("http://%s/db", c.leader), &db); err != nil {
		return nil, err
	}
	return &struct{ Balance int64 }{Balance: db[acct]}, nil
}

func (c *Client) Submit(from, to string, amt, tau int64) (*ClientReply, error) {
	req := ClientRequest{View: 0, Op: Op{From: from, To: to, Amt: amt, C: ClientID(from), Tau: tau}}
	req.SigC = signClient(req.Op.C, req.Op)
	b, _ := json.Marshal(req)

	url := fmt.Sprintf("http://%s/submit", c.leader)
	resp, err := c.httpc.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out ClientReply
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}
