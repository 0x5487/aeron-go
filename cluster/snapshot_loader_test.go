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
