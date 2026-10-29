package cluster

import (
	"math"
	"testing"

	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A ClientSession whose cluster session id is outside its schema range is not added to the sessions of the snapshot
// (snapshotLoader.onFragment).
func TestSnapshotLoaderRejectsOutOfRangeClientSession(t *testing.T) {
	event := codecs.ClientSession{
		ClusterSessionId: math.MinInt64,
		ResponseStreamId: 3,
		ResponseChannel:  []byte("aeron:ipc"),
	}
	img := &scriptedImage{queue: []scriptedFragment{{
		buf: encodedClusterMessage(t, &event), flags: testFlagUnfragmented,
	}}}
	loader := newSnapshotLoader(&ClusteredServiceAgent{}, img)

	require.NotPanics(t, func() { loader.poll() },
		"an out-of-schema client session must be rejected before it is added")

	require.Error(t, loader.err)
	assert.ErrorContains(t, loader.err, "Range check failed on c.ClusterSessionId")
	assert.True(t, loader.isDone)
}

// A SnapshotMarker whose type id is outside its schema range is not applied (snapshotLoader.onFragment).
func TestSnapshotLoaderRejectsOutOfRangeSnapshotMarker(t *testing.T) {
	event := codecs.SnapshotMarker{
		TypeId:     math.MinInt64,
		Mark:       codecs.SnapshotMark.BEGIN,
		TimeUnit:   codecs.ClusterTimeUnit.MILLIS,
		AppVersion: 1,
	}
	img := &scriptedImage{queue: []scriptedFragment{{
		buf: encodedClusterMessage(t, &event), flags: testFlagUnfragmented,
	}}}
	loader := newSnapshotLoader(&ClusteredServiceAgent{}, img)

	loader.poll()

	require.Error(t, loader.err)
	assert.ErrorContains(t, loader.err, "Range check failed on s.TypeId")
	assert.True(t, loader.isDone)
	assert.False(t, loader.inSnapshot)
}

// A BEGIN marker whose type is not this service's stops the load: the client session after it is not read or added
// (snapshotLoader.onFragment).
func TestSnapshotLoaderStopsAtAWrongSnapshotType(t *testing.T) {
	marker := codecs.SnapshotMarker{
		TypeId:     snapshotTypeId + 1,
		Mark:       codecs.SnapshotMark.BEGIN,
		TimeUnit:   codecs.ClusterTimeUnit.MILLIS,
		AppVersion: 1,
	}
	session := codecs.ClientSession{
		ClusterSessionId: 7,
		ResponseStreamId: 3,
		ResponseChannel:  []byte("aeron:ipc"),
	}
	agent := &ClusteredServiceAgent{sessions: map[int64]ClientSession{}}
	img := &scriptedImage{queue: []scriptedFragment{
		{buf: encodedClusterMessage(t, &marker), flags: testFlagUnfragmented},
		{buf: encodedClusterMessage(t, &session), flags: testFlagUnfragmented},
	}}
	loader := newSnapshotLoader(agent, img)

	require.Equal(t, 1, loader.poll())
	require.Error(t, loader.err)
	assert.ErrorContains(t, loader.err, "unexpected snapshot type")
	assert.True(t, loader.isDone, "a marker of another type ends the load")

	require.NotPanics(t, func() { assert.Zero(t, loader.poll()) }, "the loader does not read past the marker")
	assert.Empty(t, agent.sessions)
}
