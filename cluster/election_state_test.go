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

// The kept counter id is checked before each read: when the counters buffer no longer holds this cluster's election state
// counter at that id (a restart allocated the counters again and this cluster's counter sits at another record), the
// counter is looked up again rather than the record that now holds another counter being read.
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

// electionRecords is a counters buffer pair a test edits record by record, the way a media driver frees and re-allocates
// counters: set writes a whole record (allocated, type, key cluster id, registration id, value), free reclaims it.
type electionRecords struct {
	meta, vals *atomic.Buffer
	reader     *counters.Reader
}

func newElectionRecords(t testing.TB, records int) *electionRecords {
	t.Helper()
	// One record more than used, so Reader.FindCounter (which stops before the last record) sees all of them.
	meta := atomic.MakeBuffer(make([]byte, (records+1)*int(counters.MetadataLength)))
	vals := atomic.MakeBuffer(make([]byte, (records+1)*int(counters.CounterLength)))
	return &electionRecords{meta: meta, vals: vals, reader: counters.NewReader(vals, meta)}
}

func (r *electionRecords) set(id int32, typeID, clusterID int32, registrationID, value int64) {
	off := id * counters.MetadataLength
	r.meta.PutInt32(off, counters.RecordAllocated)
	r.meta.PutInt32(off+counters.TypeIdOffset, typeID)
	r.meta.PutInt32(off+counters.KeyOffset, clusterID)
	r.vals.PutInt64(id*counters.CounterLength+counters.RegistrationIdOffset, registrationID)
	r.vals.PutInt64(id*counters.CounterLength, value)
}

func (r *electionRecords) free(id int32) {
	r.meta.PutInt32(id*counters.MetadataLength, counters.RecordReclaimed)
}

// Once the counter is found its id is kept: the next call validates that id and reads it, it does not scan. Record 0 is
// made a matching counter after the first call; a scan from record 0 would take it, the kept id (record 1) is still the
// counter that was found and is still valid.
func TestElectionStateReadsTheKeptCounterWithoutScanning(t *testing.T) {
	t.Parallel()
	const clusterID = int32(7)
	rec := newElectionRecords(t, 2)
	rec.set(0, commitPosCounterTypeId, clusterID, 1, 100)
	rec.set(1, electionStateCounterTypeId, clusterID, 2, int64(ElectionStateClosed))
	agent := electionAgent(t, clusterID, rec.reader)

	got, ok := agent.ElectionState()
	require.True(t, ok)
	require.Equal(t, ElectionStateClosed, got)

	rec.set(0, electionStateCounterTypeId, clusterID, 3, 5)
	rec.vals.PutInt64(1*counters.CounterLength, 10)

	got, ok = agent.ElectionState()
	require.True(t, ok)
	assert.Equal(t, ElectionState(10), got, "the kept counter at record 1, not what a scan from record 0 finds")
}

// A kept id that no longer names this member's election state counter is not read: the counter is looked up again, and
// the state is unknown when there is none.
func TestElectionStateDoesNotReadAKeptIdThatIsNoLongerItsCounter(t *testing.T) {
	t.Parallel()
	const clusterID = int32(7)
	tests := []struct {
		name   string
		mutate func(rec *electionRecords)
	}{
		{"freed", func(rec *electionRecords) { rec.free(1) }},
		{"re-allocated to another cluster's election state counter", func(rec *electionRecords) {
			rec.set(1, electionStateCounterTypeId, 9, 2, 3)
		}},
		{"re-allocated to a counter of another type", func(rec *electionRecords) {
			rec.set(1, commitPosCounterTypeId, clusterID, 2, 3)
		}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := newElectionRecords(t, 2)
			rec.set(0, commitPosCounterTypeId, clusterID, 1, 100)
			rec.set(1, electionStateCounterTypeId, clusterID, 2, int64(ElectionStateClosed))
			agent := electionAgent(t, clusterID, rec.reader)
			_, ok := agent.ElectionState()
			require.True(t, ok)

			tt.mutate(rec)

			_, ok = agent.ElectionState()
			assert.False(t, ok, "the unknown state, not the value of a counter that is not ours")
		})
	}
}

// A freed counter is replaced by the consensus module's new one at another record: the next call finds and reads it, and
// keeps that id from then on.
func TestElectionStateFindsTheNewCounterAfterTheKeptOneIsFreed(t *testing.T) {
	t.Parallel()
	const clusterID = int32(7)
	rec := newElectionRecords(t, 3)
	rec.set(0, commitPosCounterTypeId, clusterID, 1, 100)
	rec.set(1, electionStateCounterTypeId, clusterID, 2, int64(ElectionStateClosed))
	agent := electionAgent(t, clusterID, rec.reader)
	_, ok := agent.ElectionState()
	require.True(t, ok)

	rec.free(1)
	rec.set(2, electionStateCounterTypeId, clusterID, 4, 10)

	got, ok := agent.ElectionState()
	require.True(t, ok)
	assert.Equal(t, ElectionState(10), got, "the new counter")

	rec.vals.PutInt64(2*counters.CounterLength, 11)
	got, ok = agent.ElectionState()
	require.True(t, ok)
	assert.Equal(t, ElectionState(11), got, "the new counter, kept")
}

// The same id re-allocated to a counter of the same type and cluster but another registration id is a different counter:
// the lookup is made again. Record 0 holds another matching counter, which only a new lookup reaches.
func TestElectionStateLooksUpAgainWhenTheRegistrationIdChanges(t *testing.T) {
	t.Parallel()
	const clusterID = int32(7)
	rec := newElectionRecords(t, 2)
	rec.set(0, commitPosCounterTypeId, clusterID, 1, 100)
	rec.set(1, electionStateCounterTypeId, clusterID, 2, int64(ElectionStateClosed))
	agent := electionAgent(t, clusterID, rec.reader)
	_, ok := agent.ElectionState()
	require.True(t, ok)

	rec.set(0, electionStateCounterTypeId, clusterID, 5, 5)
	rec.set(1, electionStateCounterTypeId, clusterID, 6, 9)

	got, ok := agent.ElectionState()
	require.True(t, ok)
	assert.Equal(t, ElectionState(5), got, "looked up again: the first matching counter")
}

// A kept id whose counter is freed with no other counter in the buffer, and one that never existed, are unknown; once the
// counter is allocated it is found.
func TestElectionStateIsUnknownUntilTheCounterIsAllocated(t *testing.T) {
	t.Parallel()
	const clusterID = int32(7)
	rec := newElectionRecords(t, 1)
	agent := electionAgent(t, clusterID, rec.reader)

	_, ok := agent.ElectionState()
	require.False(t, ok)
	_, ok = agent.ElectionState()
	require.False(t, ok)

	rec.set(0, electionStateCounterTypeId, clusterID, 1, int64(ElectionStateClosed))
	got, ok := agent.ElectionState()
	require.True(t, ok)
	assert.Equal(t, ElectionStateClosed, got)
}
