package kvraft

//
// KVRaft bench tests.
//

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkSequentialPut(b *testing.B) {
	const nservers = 5
	cfg := make_config(b, nservers, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	// wait until first op completes, so we know a leader is elected
	// and KV servers are ready to process client requests
	ck.Put("x", "")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ck.Put("x", "x 0 "+strconv.Itoa(i)+" y")
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

func BenchmarkConcurrentPut(b *testing.B) {
	const nservers = 5
	const nclients = 200
	cfg := make_config(b, nservers, false, 5000)
	defer cfg.cleanup()

	var cks []*Clerk
	var ncli atomic.Int64
	ck := cfg.makeClient(cfg.All())
	for i := 0; i < nclients; i++ {
		cks = append(cks, ck)
	}
	// wait until first op completes, so we know a leader is elected
	// and KV servers are ready to process client requests
	cks[0].Put("x", "")
	runtime.GOMAXPROCS(40)
	b.SetParallelism(nclients / 40)
	br := &BenchmarkResult{start: time.Now(), BenchmarkResult: testing.BenchmarkResult{Extra: map[string]float64{}}}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		cli := int(ncli.Add(1)) - 1
		var nop atomic.Int64
		for pb.Next() {
			cks[cli].Put(fmt.Sprintf("c%d", cli), fmt.Sprintf("x 0 %d y", nop.Add(1)))
			br.tick()
		}
	})
	fmt.Printf("\ntotal\t")
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

type BenchmarkResult struct {
	mu sync.Mutex
	testing.BenchmarkResult
	start time.Time
}

func (br *BenchmarkResult) tick() {
	br.mu.Lock()
	br.N++
	br.T = time.Since(br.start)
	br.Extra["ops/sec"] = float64(br.N) / br.T.Seconds()
	if br.T.Seconds() >= 0.98 {
		fmt.Printf("\nreport:\t%v", br.BenchmarkResult)
		br.start = time.Now()
		br.N = 0
	}
	br.mu.Unlock()
}
