package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"
)

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func postJSON(client *http.Client, addr, path string, in any, out any) error {
	b, _ := json.Marshal(in)
	url := "http://" + addr + path
	resp, err := client.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		if len(body) > 128 {
			body = body[:128]
		}
		return fmt.Errorf("%s %s -> http %d: %s", http.MethodPost, url, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	io.Copy(io.Discard, resp.Body)
	return nil
}

func getJSON(h *http.Client, url string, dst any) error {
	resp, err := h.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(resp.Body)
		msg := strings.TrimSpace(string(b))
		if msg == "" {
			msg = resp.Status
		}
		return fmt.Errorf("%d %s", resp.StatusCode, msg)
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

// ----- Sign/Verify helpers (deterministic keys from addr) -----
func keysFromAddr(addr string) NodeKeys {
	seed := sha256.Sum256([]byte("bft:" + addr))
	priv := ed25519.NewKeyFromSeed(seed[:])
	pub := priv.Public().(ed25519.PublicKey)
	return NodeKeys{Pub: pub, Priv: priv}
}
func pubFromAddr(addr string) ed25519.PublicKey { return keysFromAddr(addr).Pub }

func signPhase(n *Node, phase string, v View, seq int, d Digest) []byte {
	// Byzantine mode
	if !n.byzGoodSign {
		// Return a same-sized but INVALID signature to guarantee verification failure
		return make([]byte, ed25519.SignatureSize)
	}

	msg := signBytes(phase, v, seq, d)
	return ed25519.Sign(n.keys.Priv, msg)
}
func verifyPhaseWithAddr(phase, addr string, v View, seq int, d Digest, sig []byte) bool {
	pub := pubFromAddr(addr)
	msg := signBytes(phase, v, seq, d)
	return ed25519.Verify(pub, msg, sig)
}
func signBytes(phase string, v View, seq int, d Digest) []byte {
	buf := new(bytes.Buffer)
	buf.WriteString(phase)
	_ = binary.Write(buf, binary.BigEndian, uint64(v))
	_ = binary.Write(buf, binary.BigEndian, int64(seq))
	buf.Write(d[:])
	return buf.Bytes()
}

// ----- Driver HTTP helpers -----
func DriverRegister(driverAddr, self string) (*RegisterReply, error) {
	h := &http.Client{}
	var out RegisterReply
	err := postJSON(h, driverAddr, "/register", RegisterReq{Addr: self}, &out)
	return &out, err
}
func DriverMembers(driverAddr string) (*MembersReply, error) {
	h := &http.Client{}
	var out MembersReply
	err := getJSON(h, fmt.Sprintf("http://%s/members", driverAddr), &out)
	return &out, err
}
func MustGetLeader(driverAddr string) string {
	h := &http.Client{}
	var out LeaderReply
	_ = getJSON(h, fmt.Sprintf("http://%s/leader", driverAddr), &out)
	return out.Addr
}

func (n *Node) sendJSON(to string, path string, in any, out any) error {
	if !n.applyLinkRules(to) {
		return nil
	}
	if n.byzCrash {
		return nil
	}
	if n.malicious {
		if n.delayMs > 0 {
			time.Sleep(time.Duration(n.delayMs) * time.Millisecond)
		}
		if n.dropRate > 0 && rand.Float64() < n.dropRate {
			return fmt.Errorf("malicious drop")
		}
	}
	if n.isFailed {
		return nil
	}

	return postJSON(n.client, to, path, in, out)
}

func clientKeyPair(c ClientID) (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := sha256.Sum256([]byte("PBFT-C-" + string(c)))
	priv := ed25519.NewKeyFromSeed(seed[:])
	pub := priv.Public().(ed25519.PublicKey)
	return pub, priv
}

func clientPub(c ClientID) ed25519.PublicKey {
	pub, _ := clientKeyPair(c)
	return pub
}

func clientSignBytes(op Op) []byte {
	return []byte(fmt.Sprintf("%s|%s|%d|%d",
		strings.ToUpper(op.From), strings.ToUpper(op.To), op.Amt, op.Tau))
}

func signClient(c ClientID, op Op) []byte {
	_, priv := clientKeyPair(c)
	return ed25519.Sign(priv, clientSignBytes(op))
}

func verifyClient(c ClientID, op Op, sig []byte) bool {
	return ed25519.Verify(clientPub(c), clientSignBytes(op), sig)
}
