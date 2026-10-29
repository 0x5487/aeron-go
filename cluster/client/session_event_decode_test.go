package client

import (
	"bytes"
	"math"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// mustEncodeSessionEvent builds the egress message of a session event. The test encodes with the range check off so
// that the client is the one that must reject the value.
func mustEncodeSessionEvent(t *testing.T, e codecs.SessionEvent) *atomic.Buffer {
	t.Helper()
	header := codecs.MessageHeader{
		BlockLength: e.SbeBlockLength(),
		TemplateId:  e.SbeTemplateId(),
		SchemaId:    e.SbeSchemaId(),
		Version:     e.SbeSchemaVersion(),
	}
	var w bytes.Buffer
	require.NoError(t, header.Encode(marshaller, &w))
	require.NoError(t, e.Encode(marshaller, &w, false))
	return atomic.MakeBuffer(w.Bytes())
}

// A SessionEvent whose correlation id is outside its schema range was decoded, reported, and then treated as the
// connect reply (AeronCluster.onSessionEvent).
func TestSessionEventOutOfSchemaRangeIsNotApplied(t *testing.T) {
	rig := newConnectedRig(t, 0)
	rig.ac.state = clientAwaitConnectReply
	rig.ac.correlationId = math.MinInt64
	leadershipTermID := rig.ac.leadershipTermId
	clusterSessionID := rig.ac.clusterSessionId

	rig.deliver(mustEncodeSessionEvent(t, codecs.SessionEvent{
		ClusterSessionId: 11,
		CorrelationId:    math.MinInt64,
		LeadershipTermId: 3,
		Code:             codecs.EventCode.OK,
	}))

	assert.Equal(t, clientAwaitConnectReply, rig.ac.state, "an out-of-schema session event is not applied")
	assert.Zero(t, rig.rec.connects)
	assert.Equal(t, leadershipTermID, rig.ac.leadershipTermId)
	assert.Equal(t, clusterSessionID, rig.ac.clusterSessionId)
	assert.False(t, rig.ac.IsConnected())
}

// A session event outside its schema range is reported once: the client says why it dropped the reply instead of
// keeping the state that waited for it in silence (AeronCluster.onSessionEvent).
func TestSessionEventDecodeErrorIsReported(t *testing.T) {
	rig := newConnectedRig(t, 0)
	rig.ac.state = clientAwaitConnectReply
	rig.ac.correlationId = math.MinInt64
	logs := captureClientLogs(t)

	rig.deliver(mustEncodeSessionEvent(t, codecs.SessionEvent{
		ClusterSessionId: 11,
		CorrelationId:    math.MinInt64,
		LeadershipTermId: 3,
		Code:             codecs.EventCode.OK,
	}))

	assert.Equal(t, clientAwaitConnectReply, rig.ac.state, "an out-of-schema session event is not applied")
	assert.Zero(t, rig.rec.connects)
	entries := logs.FilterLevelExact(zapcore.ErrorLevel).All()
	require.Len(t, entries, 1, "the dropped session event is reported once")
	assert.Contains(t, entries[0].Message, "session event decode error")
	assert.Contains(t, entries[0].Message, "Range check failed on s.CorrelationId")
}
