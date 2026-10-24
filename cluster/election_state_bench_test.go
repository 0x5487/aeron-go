package cluster

import (
	"fmt"
	"testing"
)

// BenchmarkElectionState is one ElectionState call with n counters allocated before this cluster's election state
// counter (a media driver has a counter per connection, publication and image). The cost must not grow with n.
func BenchmarkElectionState(b *testing.B) {
	for _, n := range []int{10, 1000} {
		b.Run(fmt.Sprintf("counters=%d", n), func(b *testing.B) {
			const clusterID = int32(7)
			rec := newElectionRecords(b, n+1)
			for i := 0; i < n; i++ {
				rec.set(int32(i), commitPosCounterTypeId, clusterID, int64(i+1), 42)
			}
			rec.set(int32(n), electionStateCounterTypeId, clusterID, int64(n+1), int64(ElectionStateClosed))
			opts := NewOptions()
			opts.ClusterId = clusterID
			agent := &ClusteredServiceAgent{counters: rec.reader, opts: opts}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := agent.ElectionState(); !ok {
					b.Fatal("counter not found")
				}
			}
		})
	}
}
