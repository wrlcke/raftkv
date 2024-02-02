package kvraft

//
// KVRaft bench tests.
//

import (
	"strconv"
	"testing"
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
