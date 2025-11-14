package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	role := flag.String("role", "node", "role: driver | node | client")
	id := flag.String("id", "n1", "node id (e.g., n1)")
	//host := flag.String("host", "127.0.0.1", "bind host")
	port := flag.Int("port", 8001, "bind port for node/driver")

	// discovery
	driverAddr := flag.String("driver", "", "driver discovery addr host:port (optional; empty = standalone)")

	// client flags
	leaderAddr := flag.String("leader", "", "(optional) override leader addr; if empty, query driver")
	op := flag.String("op", "transfer", "client op: transfer | balance")
	from := flag.String("from", "A", "sender acct")
	to := flag.String("to", "B", "receiver acct")
	amt := flag.Int64("amt", 10, "amount")
	acct := flag.String("acct", "A", "acct for balance")
	ctau := flag.Int64("tau", 1, "client monotonic tau")
	cid := flag.String("cid", "c1", "client id")
	csvPath := flag.String("csv", "", "path to CSV script to execute after cluster is up")

	malicious := flag.Bool("malicious", false, "Enable malicious behaviour")
	malType := flag.String("malType", "", "drop|delay|equivocate|stale")
	dropRate := flag.Float64("dropRate", 0.0, "0..1 probability of dropping outbound msgs")
	delayMs := flag.Int("delayMs", 0, "Artificial network delay in ms")
	optimistic := flag.Bool("optimistic", false, "Enable optimistic phase reduction")
	optimisticTimeout := flag.Int("optimisticTimeout", 300, "Timeout for optimistic mode (ms)")

	// driver flags
	expected := flag.Int("n", 7, "expected number of nodes (driver)")

	flag.Parse()

	switch *role {
	case "driver":
		addr := fmt.Sprintf("127.0.0.1:%d", *port)
		if strings.TrimSpace(*csvPath) == "" {
			log.Fatal("driver: -csv <path-to-testcases.csv> is required")
		}
		d := NewDriver(addr, *expected, *csvPath)
		if err := d.Start(); err != nil {
			log.Fatalf("driver: %v", err)
		}
		log.Printf("[driver] up at http://%s expecting %d nodes", addr, *expected)
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		<-sigc
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = d.Stop(ctx)

	case "node":
		addr := fmt.Sprintf("127.0.0.1:%d", *port)
		n := NewNode(*id, addr, *driverAddr)
		n.malicious = *malicious
		n.malType = *malType
		n.dropRate = *dropRate
		n.delayMs = *delayMs
		n.optimisticMode = *optimistic
		n.optimisticTimeoutMs = *optimisticTimeout
		if err := n.Start(); err != nil {
			log.Fatalf("start: %v", err)
		}
		log.Printf("[%s] up at http://%s | discovery=%s", n.ID, n.Addr, *driverAddr)
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		<-sigc
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = n.Stop(ctx)

	case "client":
		var leader string
		if strings.TrimSpace(*leaderAddr) != "" {
			leader = *leaderAddr
		} else if strings.TrimSpace(*driverAddr) != "" {
			leader = MustGetLeader(*driverAddr)
			if strings.TrimSpace(leader) == "" {
				log.Fatalf("no leader available from driver; pass -leader explicitly")
			}
		} else {
			log.Fatalf("no -leader and no -driver; pass -leader=<host:port> for standalone")
		}
		c := NewClient(leader)
		switch *op {
		case "transfer":
			res, err := c.SubmitTransfer(*cid, *ctau, *from, *to, *amt)
			if err != nil {
				log.Fatalf("client: %v", err)
			}
			fmt.Printf("OK: committed seq=%d view=%d new_balances=%v\n", res.Seq, res.View, res.DB)
		case "balance":
			res, err := c.QueryBalance(*acct)
			if err != nil {
				log.Fatalf("client: %v", err)
			}
			fmt.Printf("Balance[%s]=%d\n", *acct, res.Balance)
		default:
			log.Fatalf("unknown -op")
		}
	default:
		log.Fatalf("unknown -role")
	}
}
