package cluster

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
)

// electionCounters builds a counters buffer pair with one allocated counter per entry: type id typeIDs[i], key cluster
// id clusterIDs[i], value values[i], and returns the reader and the values buffer (so a test can write a new value, as
// the consensus module does). It is the on-driver shape of ClusterCounters.allocate: an int32 cluster id at the start of
// the key, then the label.
func electionCounters(t *testing.T, typeIDs, clusterIDs []int32, values []int64) (*counters.Reader, *atomic.Buffer) {
	t.Helper()
	require.Len(t, clusterIDs, len(typeIDs))
	require.Len(t, values, len(typeIDs))

	// One record more than there are counters, left unused (as a driver's values buffer has many more records than
	// allocated counters): Reader.FindCounter stops before the last record of the buffer.
	const spare = 1
	meta := atomic.MakeBuffer(make([]byte, (len(typeIDs)+spare)*int(counters.MetadataLength)))
	vals := atomic.MakeBuffer(make([]byte, (len(typeIDs)+spare)*int(counters.CounterLength)))
	for i := range typeIDs {
		off := int32(i) * counters.MetadataLength
		meta.PutInt32(off, counters.RecordAllocated)
		meta.PutInt32(off+counters.TypeIdOffset, typeIDs[i])
		meta.PutInt32(off+counters.KeyOffset, clusterIDs[i])
		vals.PutInt64(int32(i)*counters.CounterLength, values[i])
	}
	return counters.NewReader(vals, meta), vals
}

// electionAgent is an agent with the counters of the test and the cluster id it looks them up with.
func electionAgent(t *testing.T, clusterID int32, reader *counters.Reader) *ClusteredServiceAgent {
	t.Helper()
	opts := NewOptions()
	opts.ClusterId = clusterID
	return &ClusteredServiceAgent{counters: reader, opts: opts}
}

// The election state is read from the counter the consensus module allocates for this cluster (type
// ELECTION_STATE_TYPE_ID, keyed by cluster id); the read is of the counter itself, so a value changed by the consensus
// module is seen at the next call.
func TestElectionStateReadsTheCounterOfThisClusterAtEveryCall(t *testing.T) {
	t.Parallel()
	const clusterID = int32(7)
	reader, values := electionCounters(t,
		[]int32{electionStateCounterTypeId}, []int32{clusterID}, []int64{int64(ElectionStateClosed)})
	agent := electionAgent(t, clusterID, reader)

	got, ok := agent.ElectionState()
	require.True(t, ok)
	assert.Equal(t, ElectionStateClosed, got)

	// The consensus module writes INIT as a new election starts; the next call reads the counter, not a cached copy.
	values.PutInt64(0, 0)
	got, ok = agent.ElectionState()
	require.True(t, ok)
	assert.Equal(t, ElectionState(0), got, "an election state of INIT")
}

// A counter of another cluster on the same media driver is not this member's election state: the key's cluster id
// decides. With no counter for this cluster the state is unknown (ok false), never a guess.
func TestElectionStateIsUnknownWithoutTheCounterOfThisCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		typeIDs   []int32
		clusterID int32
		values    []int64
	}{
		{
			name:      "another cluster's counter only",
			typeIDs:   []int32{electionStateCounterTypeId},
			clusterID: 9, values: []int64{int64(ElectionStateClosed)},
		},
		{
			name:      "no election state counter at all",
			typeIDs:   []int32{commitPosCounterTypeId},
			clusterID: 7, values: []int64{42},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reader, _ := electionCounters(t, tt.typeIDs, []int32{7}, tt.values)
			agent := electionAgent(t, tt.clusterID, reader)

			_, ok := agent.ElectionState()

			assert.False(t, ok, "a counter that is not there is unknown")
		})
	}
}

// Two clusters sharing one media driver each have their own election state counter; the lookup takes the one whose key
// names this cluster.
func TestElectionStateTakesTheCounterOfItsOwnCluster(t *testing.T) {
	t.Parallel()
	reader, _ := electionCounters(t,
		[]int32{electionStateCounterTypeId, electionStateCounterTypeId},
		[]int32{7, 9},
		[]int64{int64(ElectionStateClosed), 0})
	agent := electionAgent(t, 9, reader)

	got, ok := agent.ElectionState()

	require.True(t, ok)
	assert.Equal(t, ElectionState(0), got, "cluster 9's counter, not cluster 7's")
}

// The lookup runs on every call, not only the first: a consensus module that restarts allocates its counters again, and the
// counter of this cluster can sit at another record index. A member that kept the id of its first lookup would read the
// record that now holds another counter.
func TestElectionStateFindsTheCounterAgainAfterItMovesRecords(t *testing.T) {
	t.Parallel()
	const clusterID = int32(7)
	reader, _ := electionCounters(t,
		[]int32{electionStateCounterTypeId}, []int32{clusterID}, []int64{int64(ElectionStateClosed)})
	agent := electionAgent(t, clusterID, reader)

	got, ok := agent.ElectionState()
	require.True(t, ok)
	require.Equal(t, ElectionStateClosed, got)

	// After a restart record 0 is another counter and this cluster's election state counter is at record 1.
	moved, _ := electionCounters(t,
		[]int32{commitPosCounterTypeId, electionStateCounterTypeId},
		[]int32{clusterID, clusterID},
		[]int64{42, int64(ElectionState(10))}) // FOLLOWER_REPLAY
	agent.counters = moved

	got, ok = agent.ElectionState()
	require.True(t, ok)
	assert.Equal(t, ElectionState(10), got, "the value of the counter at its new record")
}
