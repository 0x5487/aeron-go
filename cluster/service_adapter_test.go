package cluster

import (
	"math"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
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
