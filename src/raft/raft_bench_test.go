package raft

//
// Raft bench tests.
//

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"6.5840/labgob"
)

type extend_config struct {
	*config
	lastAppliedIndex int
	waitAppliedMu    sync.Mutex
	waitApplied      map[int]chan struct{}
	benchresmu       sync.Mutex
	testing.BenchmarkResult
}

func (cfg *extend_config) startCommand(cmd interface{}, lastLeader int) (int, int, int) {
	index, term, ok := cfg.rafts[lastLeader].Start(cmd)
	if ok {
		return index, term, lastLeader
	}
	// if no leader is found, wait for a while, but don't wait forever
	for _, to := range []int{800, 800, 800} {
		for i := 0; i < cfg.n; i++ {
			index, term, ok = cfg.rafts[i].Start(cmd)
			if ok {
				return index, term, i
			}
		}
		time.Sleep(time.Duration(to) * time.Millisecond)
	}
	return -1, -1, -1
}

func (cfg *extend_config) waitCommand(index int, term int) {
	cfg.waitAppliedMu.Lock()
	if cfg.lastAppliedIndex < index {
		// fmt.Printf("waitCommand %d/%d\n", cfg.lastAppliedIndex, index)
		ch := make(chan struct{})
		cfg.waitApplied[index] = ch
		cfg.waitAppliedMu.Unlock()
		for i := 0; i < 4; i++ {
			select {
			case <-ch:
				return
			case <-time.After(3 * time.Second):
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
	} else {
		cfg.waitAppliedMu.Unlock()
	}
}

func (cfg *extend_config) onApply(n int, m ApplyMsg) {
	if m.CommandValid {
		cfg.mu.Lock()
		cfg.maxIndex = Max(cfg.maxIndex, m.CommandIndex)
		cfg.mu.Unlock()
		cfg.waitAppliedMu.Lock()
		cfg.lastAppliedIndex = Max(cfg.lastAppliedIndex, m.CommandIndex)
		if ch, ok := cfg.waitApplied[m.CommandIndex]; ok {
			close(ch)
			delete(cfg.waitApplied, m.CommandIndex)
		}
		cfg.waitAppliedMu.Unlock()
	}
}

// returns "" or error string
func (cfg *extend_config) ingestSnap(i int, snapshot []byte, index int) string {
	if snapshot == nil {
		log.Fatalf("nil snapshot")
		return "nil snapshot"
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var lastIncludedIndex int
	if d.Decode(&lastIncludedIndex) != nil {
		log.Fatalf("snapshot decode error")
		return "snapshot Decode() error"
	}
	if index != -1 && index != lastIncludedIndex {
		err := fmt.Sprintf("server %v snapshot doesn't match m.SnapshotIndex", i)
		return err
	}
	cfg.logs[i] = map[int]interface{}{}
	for j := 0; j <= index; j++ {
		cfg.logs[i][j] = 1
	}
	cfg.lastApplied[i] = lastIncludedIndex
	return ""
}

// periodically snapshot raft state
func (cfg *extend_config) applierSnap(i int, applyCh chan ApplyMsg) {
	cfg.mu.Lock()
	rf := cfg.rafts[i]
	cfg.mu.Unlock()
	if rf == nil {
		return // ???
	}

	for m := range applyCh {
		err_msg := ""
		if m.SnapshotValid {
			cfg.mu.Lock()
			err_msg = cfg.ingestSnap(i, m.Snapshot, m.SnapshotIndex)
			cfg.mu.Unlock()
		} else if m.CommandValid {
			if m.CommandIndex != cfg.lastApplied[i]+1 {
				err_msg = fmt.Sprintf("server %v apply out of order, expected index %v, got %v", i, cfg.lastApplied[i]+1, m.CommandIndex)
			}

			if err_msg == "" {
				cfg.onApply(i, m)
				cfg.mu.Lock()
				var prevok bool
				err_msg, prevok = cfg.checkLogs(i, m)
				cfg.mu.Unlock()
				if m.CommandIndex > 1 && prevok == false {
					err_msg = fmt.Sprintf("server %v apply out of order %v", i, m.CommandIndex)
				}
			}

			cfg.mu.Lock()
			cfg.lastApplied[i] = m.CommandIndex
			cfg.mu.Unlock()

			// if (m.CommandIndex+1)%SnapShotInterval == 0 {
			// 	w := new(bytes.Buffer)
			// 	e := labgob.NewEncoder(w)
			// 	e.Encode(m.CommandIndex)
			// 	var xlog []interface{}
			// 	for j := 0; j <= m.CommandIndex; j++ {
			// 		xlog = append(xlog, cfg.logs[i][j])
			// 	}
			// 	e.Encode(xlog)
			// 	rf.Snapshot(m.CommandIndex, w.Bytes())
			// }
			if (m.CommandIndex+1)%10000 == 0 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(m.CommandIndex)
				// cfg.mu.Lock()
				// for j := 0; j <= m.CommandIndex; j++ {
				// 	cfg.logs[i][j] = 1
				// }
				// cfg.mu.Unlock()
				rf.Snapshot(m.CommandIndex, w.Bytes())
			}
		} else {
			// Ignore other types of ApplyMsg.
		}
		if err_msg != "" {
			log.Fatalf("apply error: %v", err_msg)
			cfg.applyErr[i] = err_msg
			// keep reading after error so that Raft doesn't block
			// holding locks...
		}
	}
}

func (cfg *config) extend_config() *extend_config {
	ext_cfg := &extend_config{cfg, -1, sync.Mutex{}, make(map[int]chan struct{}), sync.Mutex{}, testing.BenchmarkResult{Extra: make(map[string]float64)}}
	for i := 0; i < cfg.n; i++ {
		ext_cfg.start1(i, ext_cfg.applierSnap)
	}
	for i := 0; i < cfg.n; i++ {
		cfg.connect(i)
	}
	return ext_cfg
}

func (cfg *extend_config) tick() {
	cfg.benchresmu.Lock()
	cfg.BenchmarkResult.N++
	cfg.BenchmarkResult.T = cfg.t.(*testing.B).Elapsed()
	cfg.BenchmarkResult.Extra["ops/sec"] = float64(cfg.N) / cfg.BenchmarkResult.T.Seconds()
	cfg.BenchmarkResult.Extra["failed"] = float64(cfg.N - cfg.maxIndex + cfg.maxIndex0)
	if cfg.N%10000 == 0 {
		fmt.Println(cfg.BenchmarkResult)
	}
	cfg.benchresmu.Unlock()
}

func BenchmarkSequentialStart(b *testing.B) {
	server := 5
	orig_cfg := make_config(b, server, false, false)
	cfg := orig_cfg.extend_config()
	runtime.GOMAXPROCS(6)
	defer cfg.cleanup()
	leaderFirst := cfg.checkOneLeader()
	b.ResetTimer()
	lastLeader := leaderFirst
	lastIndex, lastTerm := -1, -1
	for i := 0; i < b.N; i++ {
		lastIndex, lastTerm, lastLeader = cfg.startCommand(1, lastLeader)
		if lastLeader == -1 {
			b.Fatalf("no leader")
		}
		cfg.waitCommand(lastIndex, lastTerm)
		cfg.tick()
	}
	finished := cfg.maxIndex - cfg.maxIndex0
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
	b.ReportMetric(float64(b.N-finished), "failed")
}

// func BenchmarkConcurrentStart(b *testing.B) {
// 	server := 5
// 	orig_cfg := make_config(b, server, false, false)
// 	cfg := orig_cfg.extend_config()
// 	runtime.GOMAXPROCS(40)
// 	defer cfg.cleanup()
// 	leaderFirst := cfg.checkOneLeader()
// 	b.SetParallelism(5)
// 	b.ResetTimer()

// 	b.RunParallel(func(pb *testing.PB) {
// 		lastLeader := leaderFirst
// 		lastIndex, lastTerm := -1, -1
// 		for pb.Next() {
// 			lastIndex, lastTerm, lastLeader = cfg.startCommand(1, lastLeader)
// 			if lastLeader == -1 {
// 				b.Fatalf("no leader")
// 			}
// 		}
// 		if lastIndex != -1 {
// 			cfg.waitCommand(lastIndex, lastTerm)
// 		}
// 	})
// 	finished := cfg.maxIndex - cfg.maxIndex0
// 	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
// 	b.ReportMetric(float64(b.N-finished), "failed")
// }

func BenchmarkConcurrentStart(b *testing.B) {
	server := 5
	orig_cfg := make_config(b, server, false, false)
	cfg := orig_cfg.extend_config()
	runtime.GOMAXPROCS(40)
	defer cfg.cleanup()
	leaderFirst := cfg.checkOneLeader()
	b.SetParallelism(5)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		lastLeader := leaderFirst
		lastIndex, lastTerm := -1, -1
		for pb.Next() {
			lastIndex, lastTerm, lastLeader = cfg.startCommand(1, lastLeader)
			if lastLeader == -1 {
				b.Fatalf("no leader")
			}
			if lastIndex != -1 {
				cfg.waitCommand(lastIndex, lastTerm)
			}
			cfg.tick()
		}
	})
	finished := cfg.maxIndex - cfg.maxIndex0
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
	b.ReportMetric(float64(b.N-finished), "failed")
}
