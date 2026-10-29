package cluster

import (
	"math"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// Every way a service control message can fail to decode stops the service: the agent is not given the join, and the
// Error line reports the term the service was in, because the join log carries no term of its own
// (serviceAdapter.onFragment).
func TestServiceAdapterStopsOnAnUndecodableMessage(t *testing.T) {
	cases := []struct {
		name       string
		templateId uint16
		cause      string
		message    func(t *testing.T) *atomic.Buffer
	}{
		{
			name:       "join log with an out-of-range log position",
			templateId: joinLogTemplateId,
			cause:      "Range check failed on j.LogPosition",
			message: func(t *testing.T) *atomic.Buffer {
				return encodedClusterMessage(t, &codecs.JoinLog{
					LogPosition:    math.MinInt64,
					MaxLogPosition: 1,
					IsStartup:      codecs.BooleanType.TRUE,
					LogChannel:     []byte("aeron:ipc"),
				})
			},
		},
		{
			name:       "schema id is not the cluster schema",
			templateId: joinLogTemplateId,
			cause:      "unexpected schemaId",
			message: func(t *testing.T) *atomic.Buffer {
				msg := encodedClusterMessage(t, &codecs.JoinLog{
					LogPosition:    1,
					MaxLogPosition: 2,
					IsStartup:      codecs.BooleanType.TRUE,
					LogChannel:     []byte("aeron:ipc"),
				})
				msg.PutUInt16(4, ClusterSchemaId+1)
				return msg
			},
		},
		{
			name:       "fragment shorter than the SBE header",
			templateId: 0,
			cause:      "shorter than the SBE header",
			message: func(*testing.T) *atomic.Buffer {
				return atomic.MakeBuffer([]byte{0x01, 0x02, 0x03})
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			logs := captureErrorLogs(t)
			agent := &ClusteredServiceAgent{
				logAdapter:          newBoundedLogAdapter(NewOptions()),
				sessionMsgHdrBuffer: codecs.MakeClusterMessageBuffer(SessionMessageHeaderTemplateId, SessionMessageHdrBlockLength),
			}
			agent.sessionMsgHdrBuffer.PutInt64(SBEHeaderLength, 42)
			adapter := newServiceAdapter(nil)
			adapter.agent = agent
			msg := tt.message(t)

			stopOf(t, func() {
				adapter.fragmentHandler(msg, 0, msg.Capacity(), fragmentHeader(testFlagUnfragmented))
			})

			fields := requireUndecodableEntry(t, logs, msg, tt.templateId, 42)
			assert.Contains(t, fields.cause, tt.cause)
			assert.Nil(t, agent.activeLogEvent, "the join log is not applied")
			assert.Zero(t, agent.logAdapter.maxLogPosition)
		})
	}
}

// An unknown template is not a join-log decode failure: the service adapter skips it without an Error line and reads
// on to the next message (serviceAdapter.onFragment, default case).
func TestServiceAdapterSkipsAnUnknownTemplate(t *testing.T) {
	logs := captureErrorLogs(t)
	agent := &ClusteredServiceAgent{terminationPosition: NullPosition}
	adapter := newServiceAdapter(nil)
	adapter.agent = agent
	unknown := clusterMessage(99, 0, 0, nil)
	termination := clusterMessage(serviceTerminationPosTemplateId, 8, 0, []byte{77, 0, 0, 0, 0, 0, 0, 0})

	require.NotPanics(t, func() {
		adapter.fragmentHandler(unknown, 0, unknown.Capacity(), fragmentHeader(testFlagUnfragmented))
	}, "the unknown template does not stop the service")
	assert.Zero(t, logs.FilterLevelExact(zapcore.ErrorLevel).Len(), "the skip writes no Error line")
	assert.EqualValues(t, NullPosition, agent.terminationPosition, "the unknown template is not applied")

	adapter.fragmentHandler(termination, 0, termination.Capacity(), fragmentHeader(testFlagUnfragmented))
	assert.EqualValues(t, 77, agent.terminationPosition, "the message after the unknown template is read")
	assert.Zero(t, logs.FilterLevelExact(zapcore.ErrorLevel).Len())
}

// Without a session message header buffer the agent has no term of its own, so an entry that carries none is reported
// with NullValue (ClusteredServiceAgent.leadershipTermId).
func TestServiceAdapterReportsNullTermWithoutASessionHeader(t *testing.T) {
	logs := captureErrorLogs(t)
	agent := &ClusteredServiceAgent{logAdapter: newBoundedLogAdapter(NewOptions())}
	adapter := newServiceAdapter(nil)
	adapter.agent = agent
	msg := encodedClusterMessage(t, &codecs.JoinLog{
		LogPosition:    math.MinInt64,
		MaxLogPosition: 1,
		IsStartup:      codecs.BooleanType.TRUE,
		LogChannel:     []byte("aeron:ipc"),
	})

	stopOf(t, func() {
		adapter.fragmentHandler(msg, 0, msg.Capacity(), fragmentHeader(testFlagUnfragmented))
	})

	requireUndecodableEntry(t, logs, msg, joinLogTemplateId, NullValue)
}
