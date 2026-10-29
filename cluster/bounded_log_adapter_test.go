package cluster

import (
	"bytes"
	"io"
	"math"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clusterEncodable is the part of a generated cluster codec a test needs to put a message on the log.
type clusterEncodable interface {
	SbeBlockLength() uint16
	SbeTemplateId() uint16
	SbeSchemaVersion() uint16
	Encode(*codecs.SbeGoMarshaller, io.Writer, bool) error
}

// encodedClusterMessage builds a cluster SBE message. The test encodes with the range check off so that the decoder
// under test is the one that must reject the value.
func encodedClusterMessage(t *testing.T, event clusterEncodable) *atomic.Buffer {
	t.Helper()
	var body bytes.Buffer
	require.NoError(t, event.Encode(codecs.NewSbeGoMarshaller(), &body, false))
	return clusterMessage(event.SbeTemplateId(), event.SbeBlockLength(), event.SbeSchemaVersion(), body.Bytes())
}

// leadershipTermRecorder records the new leadership terms the agent applies.
type leadershipTermRecorder struct {
	ClusteredService
	calls int
}

func (s *leadershipTermRecorder) OnNewLeadershipTermEvent(
	_, _, _, _ int64,
	_ int32,
	_ int32,
	_ codecs.ClusterTimeUnitEnum,
	_ int32,
) {
	s.calls++
}

// sessionRecorder counts the session events the agent applies.
type sessionRecorder struct {
	ClusteredService
	closeCalls   int
	messageCalls int
}

func (s *sessionRecorder) OnSessionClose(ClientSession, int64, codecs.CloseReasonEnum) {
	s.closeCalls++
}

func (s *sessionRecorder) OnSessionMessage(
	ClientSession,
	int64,
	*atomic.Buffer,
	int32,
	int32,
	*logbuffer.Header,
) {
	s.messageCalls++
}

// Every event the bounded log adapter decodes is checked against the schema: a value outside its range is not applied
// to the agent (boundedLogAdapter.onMessage).
func TestBoundedLogAdapterRejectsOutOfRangeEvents(t *testing.T) {
	t.Run("ClusterActionRequest with an unknown action", func(t *testing.T) {
		agent := &ClusteredServiceAgent{}
		adapter := newBoundedLogAdapter(NewOptions())
		adapter.agent = agent
		event := codecs.ClusterActionRequest{
			LeadershipTermId: 1,
			LogPosition:      2,
			Timestamp:        500,
			Action:           99,
		}
		adapter.image = &scriptedImage{queue: []scriptedFragment{{
			buf: encodedClusterMessage(t, &event), flags: testFlagUnfragmented,
		}}}

		assert.Equal(t, 1, adapter.poll(math.MaxInt64))
		assert.Zero(t, agent.clusterTime, "an out-of-schema action is not applied")
		assert.Zero(t, agent.logPosition)
	})

	t.Run("NewLeadershipTermEvent with an out-of-range term id", func(t *testing.T) {
		service := &leadershipTermRecorder{}
		agent := &ClusteredServiceAgent{
			opts:                &Options{AppVersion: 0},
			service:             service,
			sessionMsgHdrBuffer: codecs.MakeClusterMessageBuffer(SessionMessageHeaderTemplateId, SessionMessageHdrBlockLength),
		}
		adapter := newBoundedLogAdapter(NewOptions())
		adapter.agent = agent
		event := codecs.NewLeadershipTermEvent{
			LeadershipTermId: math.MinInt64,
			LogPosition:      2,
			Timestamp:        500,
			TimeUnit:         codecs.ClusterTimeUnit.MILLIS,
		}
		adapter.image = &scriptedImage{queue: []scriptedFragment{{
			buf: encodedClusterMessage(t, &event), flags: testFlagUnfragmented,
		}}}

		assert.Equal(t, 1, adapter.poll(math.MaxInt64))
		assert.Zero(t, agent.clusterTime, "an out-of-schema new leadership term is not applied")
		assert.Zero(t, service.calls)
	})

	t.Run("MembershipChangeEvent with an out-of-range term id", func(t *testing.T) {
		agent := &ClusteredServiceAgent{}
		adapter := newBoundedLogAdapter(NewOptions())
		adapter.agent = agent
		event := codecs.MembershipChangeEvent{
			LeadershipTermId: math.MinInt64,
			LogPosition:      2,
			Timestamp:        500,
			ChangeType:       codecs.ChangeType.JOIN,
			MemberId:         1,
			ClusterSize:      1,
		}
		adapter.image = &scriptedImage{queue: []scriptedFragment{{
			buf: encodedClusterMessage(t, &event), flags: testFlagUnfragmented,
		}}}

		assert.Equal(t, 1, adapter.poll(math.MaxInt64))
		assert.Zero(t, agent.clusterTime, "an out-of-schema membership change is not applied")
		assert.Zero(t, agent.logPosition)
	})

	t.Run("TimerEvent with a null correlation id", func(t *testing.T) {
		service := &timerRecorder{}
		agent := &ClusteredServiceAgent{service: service}
		adapter := newBoundedLogAdapter(NewOptions())
		adapter.agent = agent
		event := codecs.TimerEvent{
			LeadershipTermId: 1,
			CorrelationId:    math.MinInt64,
			Timestamp:        500,
		}
		adapter.image = &scriptedImage{queue: []scriptedFragment{{
			buf: encodedClusterMessage(t, &event), flags: testFlagUnfragmented,
		}}}

		assert.Equal(t, 1, adapter.poll(math.MaxInt64))
		assert.Zero(t, agent.clusterTime, "an out-of-schema timer event is not applied")
		assert.Zero(t, agent.logPosition)
		assert.Empty(t, service.events)
	})

	t.Run("SessionCloseEvent with an unknown close reason", func(t *testing.T) {
		service := &sessionRecorder{}
		agent := &ClusteredServiceAgent{
			service:  service,
			sessions: map[int64]ClientSession{7: &containerClientSession{id: 7}},
		}
		adapter := newBoundedLogAdapter(NewOptions())
		adapter.agent = agent
		event := codecs.SessionCloseEvent{
			LeadershipTermId: 1,
			ClusterSessionId: 7,
			Timestamp:        500,
			CloseReason:      codecs.CloseReasonEnum(99),
		}
		adapter.image = &scriptedImage{queue: []scriptedFragment{{
			buf: encodedClusterMessage(t, &event), flags: testFlagUnfragmented,
		}}}

		assert.Equal(t, 1, adapter.poll(math.MaxInt64))
		assert.Zero(t, agent.clusterTime, "an out-of-schema session close is not applied")
		assert.Zero(t, agent.logPosition)
		assert.Zero(t, service.closeCalls)
		assert.Contains(t, agent.sessions, int64(7))
	})

	t.Run("SessionMessageHeader with a null cluster session id", func(t *testing.T) {
		service := &sessionRecorder{}
		agent := &ClusteredServiceAgent{
			service:  service,
			sessions: map[int64]ClientSession{7: &containerClientSession{id: 7}},
		}
		adapter := newBoundedLogAdapter(NewOptions())
		adapter.agent = agent
		event := codecs.SessionMessageHeader{
			LeadershipTermId: 1,
			ClusterSessionId: math.MinInt64,
			Timestamp:        500,
		}
		adapter.image = &scriptedImage{queue: []scriptedFragment{{
			buf: encodedClusterMessage(t, &event), flags: testFlagUnfragmented,
		}}}

		assert.Equal(t, 1, adapter.poll(math.MaxInt64))
		assert.Zero(t, agent.clusterTime, "an out-of-schema session message is not applied")
		assert.Zero(t, agent.logPosition)
		assert.Zero(t, service.messageCalls)
	})
}
