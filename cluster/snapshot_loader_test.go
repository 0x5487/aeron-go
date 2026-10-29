package cluster

import (
	"encoding/hex"
	"math"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// A ClientSession whose cluster session id is outside its schema range ends the load with the entry's fields. The
// loader reports the term of the BEGIN marker it is inside, and it does not read the client session after it
// (snapshotLoader.onFragment).
func TestSnapshotLoaderRejectsOutOfRangeClientSession(t *testing.T) {
	logs := captureErrorLogs(t)
	begin := codecs.SnapshotMarker{
		TypeId:           snapshotTypeId,
		LogPosition:      1,
		LeadershipTermId: 5,
		Mark:             codecs.SnapshotMark.BEGIN,
		TimeUnit:         codecs.ClusterTimeUnit.MILLIS,
		AppVersion:       1,
	}
	event := codecs.ClientSession{
		ClusterSessionId: math.MinInt64,
		ResponseStreamId: 3,
		ResponseChannel:  []byte("aeron:ipc"),
	}
	next := codecs.ClientSession{
		ClusterSessionId: 8,
		ResponseStreamId: 4,
		ResponseChannel:  []byte("aeron:ipc"),
	}
	msg := encodedClusterMessage(t, &event)
	img := &scriptedImage{queue: []scriptedFragment{
		{buf: encodedClusterMessage(t, &begin), flags: testFlagUnfragmented},
		{buf: msg, flags: testFlagUnfragmented},
		{buf: encodedClusterMessage(t, &next), flags: testFlagUnfragmented},
	}}
	agent := &ClusteredServiceAgent{sessions: map[int64]ClientSession{}}
	loader := newSnapshotLoader(agent, img)

	require.Equal(t, 1, loader.poll(), "the BEGIN marker is read")
	require.NoError(t, loader.err)
	require.False(t, loader.isDone)

	require.NotPanics(t, func() { loader.poll() }, "the loader reports the entry; the caller stops the service")

	var entry *undecodableLogEntry
	require.ErrorAs(t, loader.err, &entry)
	assert.Equal(t, uint16(clientSessionTemplateId), entry.templateId)
	assert.EqualValues(t, 96, entry.position)
	assert.EqualValues(t, 5, entry.leadershipTermId, "the term comes from the BEGIN marker")
	assert.ErrorContains(t, entry.cause, "Range check failed on c.ClusterSessionId")
	assert.Equal(t, hexOf(msg), hex.EncodeToString(entry.payload))
	assert.True(t, loader.isDone)
	assert.Zero(t, loader.poll(), "the loader does not read the client session after the bad one")
	assert.Empty(t, agent.sessions)
	assert.Zero(t, logs.FilterLevelExact(zapcore.ErrorLevel).Len(),
		"the loader does not write the line; the caller that stops the service does")
}

// A SnapshotMarker whose type id is outside its schema range ends the load with the entry's fields: the term is read
// from the marker itself and nothing is applied (snapshotLoader.onFragment).
func TestSnapshotLoaderRejectsOutOfRangeSnapshotMarker(t *testing.T) {
	event := codecs.SnapshotMarker{
		TypeId:           math.MinInt64,
		LogPosition:      1,
		LeadershipTermId: 5,
		Mark:             codecs.SnapshotMark.BEGIN,
		TimeUnit:         codecs.ClusterTimeUnit.MILLIS,
		AppVersion:       1,
	}
	msg := encodedClusterMessage(t, &event)
	img := &scriptedImage{queue: []scriptedFragment{{
		buf: msg, flags: testFlagUnfragmented,
	}}}
	loader := newSnapshotLoader(&ClusteredServiceAgent{}, img)

	require.NotPanics(t, func() { loader.poll() })

	var entry *undecodableLogEntry
	require.ErrorAs(t, loader.err, &entry)
	assert.Equal(t, uint16(snapshotMarkerTemplateId), entry.templateId)
	assert.EqualValues(t, 96, entry.position)
	assert.EqualValues(t, 5, entry.leadershipTermId)
	assert.ErrorContains(t, entry.cause, "Range check failed on s.TypeId")
	assert.Equal(t, hexOf(msg), hex.EncodeToString(entry.payload))
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

// A snapshot fragment the loader cannot decode stops the service that would start from it: the caller writes the one
// Error line and panics, does not call OnStart, and does not read the next fragment.
func TestLoadSnapshotImageStopsOnAnUndecodableFragment(t *testing.T) {
	logs := captureErrorLogs(t)
	service := &allCallsRecorder{}
	agent := &ClusteredServiceAgent{
		opts:     NewOptions(),
		service:  service,
		sessions: map[int64]ClientSession{},
	}
	marker := codecs.SnapshotMarker{
		TypeId:           math.MinInt64,
		LogPosition:      1,
		LeadershipTermId: 5,
		Mark:             codecs.SnapshotMark.BEGIN,
		TimeUnit:         codecs.ClusterTimeUnit.MILLIS,
		AppVersion:       1,
	}
	session := codecs.ClientSession{
		ClusterSessionId: 7,
		ResponseStreamId: 3,
		ResponseChannel:  []byte("aeron:ipc"),
	}
	msg := encodedClusterMessage(t, &marker)
	img := &scriptedImage{queue: []scriptedFragment{
		{buf: msg, flags: testFlagUnfragmented},
		{buf: encodedClusterMessage(t, &session), flags: testFlagUnfragmented},
	}}

	stopOf(t, func() { agent.loadSnapshotImage(img) })

	requireUndecodableEntry(t, logs, msg, snapshotMarkerTemplateId, 5)
	assert.Zero(t, service.calls, "the service is not started from the snapshot")
	assert.Empty(t, agent.sessions)
	require.Len(t, img.queue, 1, "the fragment after the stop is not read")
}

// A loader error that is not a decode error (a marker of another type) also reaches the caller, which does not start
// the service from that snapshot.
func TestLoadSnapshotImageReturnsALoaderError(t *testing.T) {
	logs := captureErrorLogs(t)
	service := &allCallsRecorder{}
	agent := &ClusteredServiceAgent{
		opts:     NewOptions(),
		service:  service,
		sessions: map[int64]ClientSession{},
	}
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
	img := &scriptedImage{queue: []scriptedFragment{
		{buf: encodedClusterMessage(t, &marker), flags: testFlagUnfragmented},
		{buf: encodedClusterMessage(t, &session), flags: testFlagUnfragmented},
	}}

	err := agent.loadSnapshotImage(img)
	require.ErrorContains(t, err, "unexpected snapshot type")
	assert.Zero(t, service.calls, "the service is not started from the snapshot")
	assert.Empty(t, agent.sessions)
	require.Len(t, img.queue, 1, "the fragment after the marker is not read")
	assert.Zero(t, logs.FilterLevelExact(zapcore.ErrorLevel).Len(), "a semantic loader error is not a decode stop")
}

// A snapshot fragment the loader cannot decode ends the load with the entry's fields and reads no further fragment
// (snapshotLoader.undecodable). No BEGIN marker came before these entries, so their term is NullValue.
func TestSnapshotLoaderRecordsAnUndecodableFragment(t *testing.T) {
	cases := []struct {
		name       string
		templateId uint16
		cause      string
		message    func(t *testing.T) *atomic.Buffer
	}{
		{
			name:       "fragment shorter than the SBE header",
			templateId: 0,
			cause:      "shorter than the SBE header",
			message: func(*testing.T) *atomic.Buffer {
				return atomic.MakeBuffer([]byte{0x01, 0x02, 0x03})
			},
		},
		{
			name:       "schema id is not the cluster schema",
			templateId: snapshotMarkerTemplateId,
			cause:      "unexpected schemaId",
			message: func(t *testing.T) *atomic.Buffer {
				msg := encodedClusterMessage(t, &codecs.SnapshotMarker{
					TypeId:      snapshotTypeId,
					LogPosition: 1,
					Mark:        codecs.SnapshotMark.BEGIN,
					TimeUnit:    codecs.ClusterTimeUnit.MILLIS,
					AppVersion:  1,
				})
				msg.PutUInt16(4, ClusterSchemaId+1)
				return msg
			},
		},
		{
			name:       "unknown template",
			templateId: 99,
			cause:      "unexpected templateId=99",
			message: func(*testing.T) *atomic.Buffer {
				return clusterMessage(99, 0, 0, nil)
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			msg := tt.message(t)
			session := codecs.ClientSession{ClusterSessionId: 7, ResponseStreamId: 3, ResponseChannel: []byte("aeron:ipc")}
			agent := &ClusteredServiceAgent{sessions: map[int64]ClientSession{}}
			img := &scriptedImage{queue: []scriptedFragment{
				{buf: msg, flags: testFlagUnfragmented},
				{buf: encodedClusterMessage(t, &session), flags: testFlagUnfragmented},
			}}
			loader := newSnapshotLoader(agent, img)

			require.Equal(t, 1, loader.poll(), "the bad fragment is read")
			require.True(t, loader.isDone, "an undecodable fragment ends the load")
			var entry *undecodableLogEntry
			require.ErrorAs(t, loader.err, &entry)
			assert.Equal(t, tt.templateId, entry.templateId)
			assert.EqualValues(t, NullValue, entry.leadershipTermId)
			assert.ErrorContains(t, entry.cause, tt.cause)
			assert.Equal(t, hexOf(msg), hex.EncodeToString(entry.payload))
			assert.Zero(t, loader.poll(), "the loader reads no fragment after the bad one")
			require.Len(t, img.queue, 1, "the next fragment is not read")
			assert.Empty(t, agent.sessions)
		})
	}
}

// A snapshot marker whose body is too short to hold the term reads no term (snapshotLoader.onFragment). The marker is
// undecodable and its Error line reports NullValue, the term the loader had before the marker. The fragment is given
// its real length inside a buffer with bytes after it, so a read past the body finds non-null bytes.
func TestSnapshotLoaderReadsNoTermFromAShortMarker(t *testing.T) {
	msg, length := fragmentWithTrailer(snapshotMarkerTemplateId, uint16((&codecs.SnapshotMarker{}).SbeBlockLength()),
		[]byte{1, 0, 0, 0, 0, 0, 0, 0})
	loader := newSnapshotLoader(&ClusteredServiceAgent{}, nil)

	loader.fragmentHandler(msg, 0, length, fragmentHeader(testFlagUnfragmented))

	require.True(t, loader.isDone)
	var entry *undecodableLogEntry
	require.ErrorAs(t, loader.err, &entry)
	assert.Equal(t, uint16(snapshotMarkerTemplateId), entry.templateId)
	assert.EqualValues(t, NullValue, entry.leadershipTermId, "the short marker's term is not read")
}
