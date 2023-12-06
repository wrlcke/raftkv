package raft

//
// Raft bench tests.
//

import (
	"math/rand"
	"runtime"
	"testing"
	"time"
)


func (cfg *config) startCommand(cmd interface{}, lastLeader int) (int, int, int) {
	index, term, ok := cfg.rafts[lastLeader].Start(cmd)
	if ok {
		return index, term, lastLeader
	}
	// if no leader is found, wait for a while, but don't wait forever
	for _, to := range []int{500, 500, 500} {
		for i:=0; i < cfg.n; i++ {
			index, term, ok = cfg.rafts[i].Start(cmd)
			if ok {
				return index, term, i
			}
		}
		time.Sleep(time.Duration(to) * time.Millisecond)
	}
	return -1, -1, -1
}

func (cfg *config) checkApplied(index int) bool {
	for i := 0; i < len(cfg.rafts); i++ {
		if cfg.applyErr[i] != "" {
			cfg.t.Fatal(cfg.applyErr[i])
		}
		cfg.mu.Lock()
		_, ok := cfg.logs[i][index]
		cfg.mu.Unlock()
		if ok {
			return true
		}
	}
	return false
}

func (cfg *config) waitCommand(index int, term int) {
	to := 10 * time.Millisecond
	for iters := 0; iters < 30; iters++ {
		if cfg.checkApplied(index) {
			return
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if term > -1 {
			for _, r := range cfg.rafts {
				if t, _ := r.GetState(); t > term {
					// someone has moved on
					// can no longer guarantee that we'll "win"
					return
				}
			}
		}
	}
}

func BenchmarkConcurrentStart(b *testing.B) {
	server := 5
	cfg := make_config(b, server, false, false)
	runtime.GOMAXPROCS(40)
	defer cfg.cleanup()
	leaderFirst := cfg.checkOneLeader()
	b.SetParallelism(5)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		lastLeader := leaderFirst
		lastIndex, lastTerm := -1, -1
		for pb.Next() {
			lastIndex, lastTerm, lastLeader = cfg.startCommand(rand.Int(), lastLeader)
			if lastLeader == -1 {
				b.Fatalf("no leader")
			}
		}
		if lastIndex != -1 {
			cfg.waitCommand(lastIndex, lastTerm)
		}
	})
	finished := cfg.maxIndex - cfg.maxIndex0
	b.ReportMetric(float64(b.N) / b.Elapsed().Seconds(), "ops/sec")
	b.ReportMetric(float64(b.N - finished), "failed")
}
