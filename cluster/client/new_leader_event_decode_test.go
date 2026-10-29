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

// mustEncodeNewLeaderEvent builds the egress message of a new leader event. The test encodes with the range check off
// so that the client is the one that must reject the value.
func mustEncodeNewLeaderEvent(t *testing.T, e codecs.NewLeaderEvent) *atomic.Buffer {
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

// A new leader event outside its schema range is reported once and not applied: the decode error must end the
// fragment, not be logged and then applied anyway (AeronCluster.onNewLeaderEvent). The event names this client's
// session, so a client that ignored the error would move to the leader it could not read.
func TestNewLeaderEventDecodeErrorIsReported(t *testing.T) {
	rig := newConnectedRig(t, 0)
	logs := captureClientLogs(t)

	rig.deliver(mustEncodeNewLeaderEvent(t, codecs.NewLeaderEvent{
		LeadershipTermId: math.MinInt64,
		ClusterSessionId: testSessionID,
		LeaderMemberId:   2,
		IngressEndpoints: []byte(rig.ac.opts.IngressEndpoints),
	}))

	assert.Equal(t, clientConnected, rig.ac.state, "an out-of-schema new leader event is not applied")
	assert.Empty(t, rig.rec.newLeaders, "the listener is not told of a leader that was never decoded")
	assert.EqualValues(t, 1, rig.ac.leadershipTermId)
	assert.Equal(t, int32(0), rig.ac.leaderMemberId)
	entries := logs.FilterLevelExact(zapcore.ErrorLevel).All()
	require.Len(t, entries, 1, "the dropped new leader event is reported once")
	assert.Contains(t, entries[0].Message, "new leader event decode error")
	assert.Contains(t, entries[0].Message, "Range check failed on n.LeadershipTermId")
}
