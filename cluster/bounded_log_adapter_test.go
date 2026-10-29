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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// clusterEncodable is the part of a generated cluster codec a test needs to put a message on the log.
type clusterEncodable interface {
	SbeBlockLength() uint16
	SbeTemplateId() uint16
	SbeSchemaVersion() uint16
	Encode(*codecs.SbeGoMarshaller, io.Writer, bool) error
}

// encodedClusterMessage builds a cluster SBE message at the codec's own schema version.
func encodedClusterMessage(t *testing.T, event clusterEncodable) *atomic.Buffer {
	t.Helper()
	return clusterMessageAt(t, event, event.SbeSchemaVersion())
}

// clusterMessageAt builds a cluster SBE message whose header carries the given acting version. The test encodes with
// the range check off so that the decoder under test is the one that must reject the value.
func clusterMessageAt(t *testing.T, event clusterEncodable, version uint16) *atomic.Buffer {
	t.Helper()
	var body bytes.Buffer
	require.NoError(t, event.Encode(codecs.NewSbeGoMarshaller(), &body, false))
	return clusterMessage(event.SbeTemplateId(), event.SbeBlockLength(), version, body.Bytes())
}

// captureErrorLogs records every entry the cluster logger writes at Error level until the test ends. A rejected
// message is reported by exactly one Error line (boundedLogAdapter.onMessage), so the tests count these lines.
func captureErrorLogs(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	prevLogger, prevSugar := logger.Logger(), logger.Sugar()
	logger.SetLogger(zap.New(core))
	logger.SetSugar(zap.New(core).Sugar())
	t.Cleanup(func() {
		logger.SetLogger(prevLogger)
		logger.SetSugar(prevSugar)
	})
	return logs
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
	closes       []sessionClose
	messageCalls int
}

// sessionClose is one OnSessionClose the agent applied, with the values it passed.
type sessionClose struct {
	session   ClientSession
	timestamp int64
	reason    codecs.CloseReasonEnum
}

func (s *sessionRecorder) OnSessionClose(session ClientSession, timestamp int64, reason codecs.CloseReasonEnum) {
	s.closeCalls++
	s.closes = append(s.closes, sessionClose{session: session, timestamp: timestamp, reason: reason})
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

// Every event the bounded log adapter decodes is checked against the schema: a value outside its range stops the
// service with one Error line and is not applied to the agent (boundedLogAdapter.onMessage). The manually read
// templates (timer, session close, session message header) have their own tables below.
func TestBoundedLogAdapterRejectsOutOfRangeEvents(t *testing.T) {
	t.Run("ClusterActionRequest with an unknown action", func(t *testing.T) {
		logs := captureErrorLogs(t)
		agent := &ClusteredServiceAgent{}
		adapter := newBoundedLogAdapter(NewOptions())
		adapter.agent = agent
		event := codecs.ClusterActionRequest{
			LeadershipTermId: 1,
			LogPosition:      2,
			Timestamp:        500,
			Action:           99,
		}
		msg := encodedClusterMessage(t, &event)
		adapter.image = &scriptedImage{queue: []scriptedFragment{{
			buf: msg, flags: testFlagUnfragmented,
		}}}

		stopOf(t, func() { adapter.poll(math.MaxInt64) })
		requireUndecodableEntry(t, logs, msg, clusterActionReqTemplateId, 1)
		assert.Zero(t, agent.clusterTime, "an out-of-schema action is not applied")
		assert.Zero(t, agent.logPosition)
	})

	t.Run("NewLeadershipTermEvent with an out-of-range term id", func(t *testing.T) {
		logs := captureErrorLogs(t)
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
		msg := encodedClusterMessage(t, &event)
		adapter.image = &scriptedImage{queue: []scriptedFragment{{
			buf: msg, flags: testFlagUnfragmented,
		}}}

		stopOf(t, func() { adapter.poll(math.MaxInt64) })
		requireUndecodableEntry(t, logs, msg, newLeadershipTermTemplateId, math.MinInt64)
		assert.Zero(t, agent.clusterTime, "an out-of-schema new leadership term is not applied")
		assert.Zero(t, service.calls)
	})

	t.Run("MembershipChangeEvent with an out-of-range term id", func(t *testing.T) {
		logs := captureErrorLogs(t)
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
		msg := encodedClusterMessage(t, &event)
		adapter.image = &scriptedImage{queue: []scriptedFragment{{
			buf: msg, flags: testFlagUnfragmented,
		}}}

		stopOf(t, func() { adapter.poll(math.MaxInt64) })
		requireUndecodableEntry(t, logs, msg, membershipChangeTemplateId, math.MinInt64)
		assert.Zero(t, agent.clusterTime, "an out-of-schema membership change is not applied")
		assert.Zero(t, agent.logPosition)
	})
}

// The manual reads of the timer event and the session close event assume the generated block layouts, so the lengths
// that guard them must be the lengths the codecs declare.
func TestManualLogBlockLengthsMatchGeneratedCodecs(t *testing.T) {
	assert.EqualValues(t, (&codecs.TimerEvent{}).SbeBlockLength(), timerEventBlockLength)
	assert.EqualValues(t, (&codecs.SessionCloseEvent{}).SbeBlockLength(), sessionCloseEventBlockLength)
}

// The timer event (template 20) is read by hand, so the range check must see every field it reads: one row per field
// of TimerEvent.RangeCheck. A rejected event stops the service with one Error line and neither the agent's time nor
// its position changes.
func TestBoundedLogAdapterRejectsOutOfRangeTimerEvent(t *testing.T) {
	cases := []struct {
		name  string
		event codecs.TimerEvent
	}{
		{"LeadershipTermId is null", codecs.TimerEvent{LeadershipTermId: math.MinInt64, CorrelationId: 2, Timestamp: 500}},
		{"CorrelationId is null", codecs.TimerEvent{LeadershipTermId: 1, CorrelationId: math.MinInt64, Timestamp: 500}},
		{"Timestamp is null", codecs.TimerEvent{LeadershipTermId: 1, CorrelationId: 2, Timestamp: math.MinInt64}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			service := &timerRecorder{}
			agent := &ClusteredServiceAgent{service: service, clusterTime: 1234, logPosition: 99}
			logs := captureErrorLogs(t)
			adapter := newBoundedLogAdapter(NewOptions())
			adapter.agent = agent
			event := tt.event
			msg := encodedClusterMessage(t, &event)
			adapter.image = &scriptedImage{queue: []scriptedFragment{{
				buf: msg, flags: testFlagUnfragmented,
			}}}

			stopOf(t, func() { adapter.poll(math.MaxInt64) })
			requireUndecodableEntry(t, logs, msg, timerEventTemplateId, tt.event.LeadershipTermId)
			assert.Empty(t, service.events, "an out-of-schema timer event is not applied")
			assert.EqualValues(t, 1234, agent.clusterTime)
			assert.EqualValues(t, 99, agent.logPosition)
		})
	}
}

// The adapter reuses one TimerEvent for every timer event, so each message is judged on its own bytes: a legal event,
// then one with a null field, then a legal one. The middle one stops the service with one Error line; the last one is
// not read.
func TestBoundedLogAdapterJudgesEachTimerEventOnItsOwnBytes(t *testing.T) {
	service := &timerRecorder{}
	agent := &ClusteredServiceAgent{service: service}
	logs := captureErrorLogs(t)
	adapter := newBoundedLogAdapter(&Options{LogFragmentLimit: 10})
	adapter.agent = agent
	legalFirst := codecs.TimerEvent{LeadershipTermId: 1, CorrelationId: 2, Timestamp: 500}
	nullField := codecs.TimerEvent{LeadershipTermId: math.MinInt64, CorrelationId: 3, Timestamp: 600}
	legalLast := codecs.TimerEvent{LeadershipTermId: 1, CorrelationId: 4, Timestamp: 700}
	nullMsg := encodedClusterMessage(t, &nullField)
	img := &scriptedImage{queue: []scriptedFragment{
		{buf: encodedClusterMessage(t, &legalFirst), flags: testFlagUnfragmented},
		{buf: nullMsg, flags: testFlagUnfragmented},
		{buf: encodedClusterMessage(t, &legalLast), flags: testFlagUnfragmented},
	}}
	adapter.image = img

	stopOf(t, func() { adapter.poll(math.MaxInt64) })
	requireUndecodableEntry(t, logs, nullMsg, timerEventTemplateId, math.MinInt64)
	assert.Equal(t, [][2]int64{{2, 500}}, service.events, "only the first legal event is applied")
	require.Len(t, img.queue, 1, "the fragment after the stop is not read")
}

// The session close (template 22) is read by hand. One row per field of SessionCloseEvent.RangeCheck. The last rows
// check the acting version: a close written at the schema version minus one, with an unknown reason, is still checked.
func TestBoundedLogAdapterRejectsOutOfRangeSessionClose(t *testing.T) {
	current := (&codecs.SessionCloseEvent{}).SbeSchemaVersion()
	olderVersion := current - 1
	require.GreaterOrEqual(t, olderVersion, uint16(1), "the schema has an older version to test")
	closeReason := codecs.CloseReason.CLIENT_ACTION
	cases := []struct {
		name    string
		event   codecs.SessionCloseEvent
		version uint16
	}{
		{"LeadershipTermId is null", codecs.SessionCloseEvent{LeadershipTermId: math.MinInt64, ClusterSessionId: 7, Timestamp: 500, CloseReason: closeReason}, current},
		{"ClusterSessionId is null", codecs.SessionCloseEvent{LeadershipTermId: 1, ClusterSessionId: math.MinInt64, Timestamp: 500, CloseReason: closeReason}, current},
		{"Timestamp is null", codecs.SessionCloseEvent{LeadershipTermId: 1, ClusterSessionId: 7, Timestamp: math.MinInt64, CloseReason: closeReason}, current},
		{"CloseReason is unknown", codecs.SessionCloseEvent{LeadershipTermId: 1, ClusterSessionId: 7, Timestamp: 500, CloseReason: codecs.CloseReasonEnum(99)}, current},
		{"CloseReason is unknown at an older acting version", codecs.SessionCloseEvent{LeadershipTermId: 1, ClusterSessionId: 7, Timestamp: 500, CloseReason: codecs.CloseReasonEnum(99)}, olderVersion},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			service := &sessionRecorder{}
			agent := &ClusteredServiceAgent{
				service:     service,
				sessions:    map[int64]ClientSession{7: &containerClientSession{id: 7}},
				clusterTime: 1234,
				logPosition: 99,
			}
			logs := captureErrorLogs(t)
			adapter := newBoundedLogAdapter(NewOptions())
			adapter.agent = agent
			event := tt.event
			msg := clusterMessageAt(t, &event, tt.version)
			adapter.image = &scriptedImage{queue: []scriptedFragment{{
				buf: msg, flags: testFlagUnfragmented,
			}}}

			stopOf(t, func() { adapter.poll(math.MaxInt64) })
			requireUndecodableEntry(t, logs, msg, sessionCloseTemplateId, tt.event.LeadershipTermId)
			assert.Zero(t, service.closeCalls, "an out-of-schema session close is not applied")
			assert.Contains(t, agent.sessions, int64(7), "the session stays open")
			assert.EqualValues(t, 1234, agent.clusterTime)
			assert.EqualValues(t, 99, agent.logPosition)
		})
	}
}

// A legal session close is applied to the session it names: the agent's clock takes the close's timestamp and the
// service is told once with those values (boundedLogAdapter.onMessage). A close whose cluster session id and timestamp
// are read from each other's offsets leaves session 7 open and moves the clock to the session id instead.
func TestBoundedLogAdapterAppliesALegalSessionClose(t *testing.T) {
	service := &sessionRecorder{}
	session := &containerClientSession{id: 7}
	agent := &ClusteredServiceAgent{
		service:     service,
		sessions:    map[int64]ClientSession{7: session},
		clusterTime: 1234,
		logPosition: 99,
	}
	logs := captureErrorLogs(t)
	adapter := newBoundedLogAdapter(NewOptions())
	adapter.agent = agent
	event := codecs.SessionCloseEvent{
		LeadershipTermId: 1,
		ClusterSessionId: 7,
		Timestamp:        500,
		CloseReason:      codecs.CloseReason.SERVICE_ACTION,
	}
	adapter.image = &scriptedImage{queue: []scriptedFragment{{
		buf: encodedClusterMessage(t, &event), flags: testFlagUnfragmented,
	}}}

	assert.Equal(t, 1, adapter.poll(math.MaxInt64))
	assert.NotContains(t, agent.sessions, int64(7), "the close removes the session it names")
	assert.EqualValues(t, 500, agent.clusterTime, "the close sets the cluster time to its timestamp")
	require.Len(t, service.closes, 1, "the service is told once")
	assert.Same(t, session, service.closes[0].session)
	assert.EqualValues(t, 500, service.closes[0].timestamp)
	assert.Equal(t, codecs.CloseReason.SERVICE_ACTION, service.closes[0].reason)
	assert.Zero(t, logs.FilterLevelExact(zapcore.ErrorLevel).Len(), "a legal close writes no Error line")
}

// The session message header (template 1) is read by hand. One row per field of SessionMessageHeader.RangeCheck.
func TestBoundedLogAdapterRejectsOutOfRangeSessionMessageHeader(t *testing.T) {
	cases := []struct {
		name  string
		event codecs.SessionMessageHeader
	}{
		{"LeadershipTermId is null", codecs.SessionMessageHeader{LeadershipTermId: math.MinInt64, ClusterSessionId: 7, Timestamp: 500}},
		{"ClusterSessionId is null", codecs.SessionMessageHeader{LeadershipTermId: 1, ClusterSessionId: math.MinInt64, Timestamp: 500}},
		{"Timestamp is null", codecs.SessionMessageHeader{LeadershipTermId: 1, ClusterSessionId: 7, Timestamp: math.MinInt64}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			service := &sessionRecorder{}
			agent := &ClusteredServiceAgent{
				service:     service,
				sessions:    map[int64]ClientSession{7: &containerClientSession{id: 7}},
				clusterTime: 1234,
				logPosition: 99,
			}
			logs := captureErrorLogs(t)
			adapter := newBoundedLogAdapter(NewOptions())
			adapter.agent = agent
			event := tt.event
			msg := encodedClusterMessage(t, &event)
			adapter.image = &scriptedImage{queue: []scriptedFragment{{
				buf: msg, flags: testFlagUnfragmented,
			}}}

			stopOf(t, func() { adapter.poll(math.MaxInt64) })
			requireUndecodableEntry(t, logs, msg, SessionMessageHeaderTemplateId, tt.event.LeadershipTermId)
			assert.Zero(t, service.messageCalls, "an out-of-schema session message is not applied")
			assert.EqualValues(t, 1234, agent.clusterTime)
			assert.EqualValues(t, 99, agent.logPosition)
		})
	}
}

// The session message header is judged on its own bytes too: a legal header, then one with a null timestamp, then a
// legal one. The reused struct must not carry the timestamp of the first message into the second; the null header
// stops the service and the header after it is not read.
func TestBoundedLogAdapterJudgesEachSessionMessageOnItsOwnBytes(t *testing.T) {
	service := &sessionRecorder{}
	agent := &ClusteredServiceAgent{
		service:  service,
		sessions: map[int64]ClientSession{7: &containerClientSession{id: 7}},
	}
	logs := captureErrorLogs(t)
	adapter := newBoundedLogAdapter(&Options{LogFragmentLimit: 10})
	adapter.agent = agent
	legalFirst := codecs.SessionMessageHeader{LeadershipTermId: 1, ClusterSessionId: 7, Timestamp: 500}
	nullTimestamp := codecs.SessionMessageHeader{LeadershipTermId: 1, ClusterSessionId: 7, Timestamp: math.MinInt64}
	legalLast := codecs.SessionMessageHeader{LeadershipTermId: 1, ClusterSessionId: 7, Timestamp: 700}
	nullMsg := encodedClusterMessage(t, &nullTimestamp)
	img := &scriptedImage{queue: []scriptedFragment{
		{buf: encodedClusterMessage(t, &legalFirst), flags: testFlagUnfragmented},
		{buf: nullMsg, flags: testFlagUnfragmented},
		{buf: encodedClusterMessage(t, &legalLast), flags: testFlagUnfragmented},
	}}
	adapter.image = img

	stopOf(t, func() { adapter.poll(math.MaxInt64) })
	requireUndecodableEntry(t, logs, nullMsg, SessionMessageHeaderTemplateId, 1)
	assert.Equal(t, 1, service.messageCalls, "only the first legal header is applied")
	assert.EqualValues(t, 500, agent.clusterTime)
	require.Len(t, img.queue, 1, "the fragment after the stop is not read")
}
