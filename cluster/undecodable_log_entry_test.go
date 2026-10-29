package cluster

import (
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"testing"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// allCallsRecorder is a ClusteredService that counts every callback, so a test can assert that the agent applied
// nothing at all for a log entry it could not decode.
type allCallsRecorder struct {
	ClusteredService
	calls int
}

func (s *allCallsRecorder) OnStart(Cluster, aeron.Image) { s.calls++ }

func (s *allCallsRecorder) OnSessionOpen(ClientSession, int64) { s.calls++ }

func (s *allCallsRecorder) OnSessionClose(ClientSession, int64, codecs.CloseReasonEnum) { s.calls++ }

func (s *allCallsRecorder) OnSessionMessage(
	ClientSession,
	int64,
	*atomic.Buffer,
	int32,
	int32,
	*logbuffer.Header,
) {
	s.calls++
}

func (s *allCallsRecorder) OnTimerEvent(int64, int64) { s.calls++ }

func (s *allCallsRecorder) OnTakeSnapshot(*aeron.Publication) { s.calls++ }

func (s *allCallsRecorder) OnRoleChange(Role) { s.calls++ }

func (s *allCallsRecorder) OnTerminate(Cluster) { s.calls++ }

func (s *allCallsRecorder) OnNewLeadershipTermEvent(
	int64,
	int64,
	int64,
	int64,
	int32,
	int32,
	codecs.ClusterTimeUnitEnum,
	int32,
) {
	s.calls++
}

// stopOf runs fn and returns the value the service stopped with. The test fails when fn returns normally: an entry
// the service cannot decode stops it, it is never skipped and read past.
func stopOf(t *testing.T, fn func()) any {
	t.Helper()
	var stop any
	func() {
		defer func() { stop = recover() }()
		fn()
	}()
	require.NotNil(t, stop, "the service must stop instead of reading on")
	require.Contains(t, fmt.Sprint(stop), "undecodable cluster log entry",
		"the service stops for the entry, not for another failure")
	return stop
}

// undecodableEntryFields is the fixed key=value order of the one Error line written when the service stops.
type undecodableEntryFields struct {
	templateId       uint16
	position         int64
	leadershipTermId int64
	cause            string
	payload          string
}

var undecodableEntryLinePattern = regexp.MustCompile(
	`^undecodable cluster log entry: templateId=(\d+) position=(-?\d+) leadershipTermId=(-?\d+) error=(.+) payload=([0-9a-f]*)$`)

// requireUndecodableEntryLine asserts that the stop wrote exactly one Error line in the fixed field order and returns
// the parsed fields.
func requireUndecodableEntryLine(t *testing.T, logs *observer.ObservedLogs) undecodableEntryFields {
	t.Helper()
	entries := logs.FilterLevelExact(zapcore.ErrorLevel).All()
	require.Len(t, entries, 1, "the stop writes exactly one Error line")
	match := undecodableEntryLinePattern.FindStringSubmatch(entries[0].Message)
	require.NotNil(t, match, "the Error line carries every field: %q", entries[0].Message)

	templateId, err := strconv.ParseUint(match[1], 10, 16)
	require.NoError(t, err)
	position, err := strconv.ParseInt(match[2], 10, 64)
	require.NoError(t, err)
	leadershipTermId, err := strconv.ParseInt(match[3], 10, 64)
	require.NoError(t, err)

	return undecodableEntryFields{
		templateId:       uint16(templateId),
		position:         position,
		leadershipTermId: leadershipTermId,
		cause:            match[4],
		payload:          match[5],
	}
}

// hexOf is the raw payload the Error line must carry: the whole log entry as it was read.
func hexOf(buf *atomic.Buffer) string {
	return hex.EncodeToString(buf.GetBytesArray(0, buf.Capacity()))
}

// requireUndecodableEntry asserts that the stop wrote one Error line naming the entry the test fed the service.
func requireUndecodableEntry(
	t *testing.T,
	logs *observer.ObservedLogs,
	msg *atomic.Buffer,
	templateId uint16,
	termId int64,
) undecodableEntryFields {
	t.Helper()
	fields := requireUndecodableEntryLine(t, logs)
	assert.Equal(t, templateId, fields.templateId)
	assert.EqualValues(t, 96, fields.position, "the position is header.Position()")
	assert.EqualValues(t, termId, fields.leadershipTermId)
	assert.Equal(t, hexOf(msg), fields.payload)
	return fields
}

// Every event kind the bounded log adapter decodes stops the service when a value is outside its schema range: the
// agent applies nothing, the next fragment in the same poll is not read, and the stop writes one Error line with the
// template id, the log position, the leadership term, the cause and the raw payload (boundedLogAdapter.onMessage).
func TestBoundedLogAdapterStopsOnAnUndecodableEntry(t *testing.T) {
	cases := []struct {
		name       string
		templateId uint16
		termId     int64
		cause      string
		message    func(t *testing.T) *atomic.Buffer
	}{
		{
			name:       "session message",
			templateId: SessionMessageHeaderTemplateId,
			termId:     1,
			cause:      "Range check failed on s.Timestamp",
			message: func(t *testing.T) *atomic.Buffer {
				return encodedClusterMessage(t, &codecs.SessionMessageHeader{
					LeadershipTermId: 1,
					ClusterSessionId: 7,
					Timestamp:        math.MinInt64,
				})
			},
		},
		{
			name:       "timer event",
			templateId: timerEventTemplateId,
			termId:     1,
			cause:      "Range check failed on t.CorrelationId",
			message: func(t *testing.T) *atomic.Buffer {
				return encodedClusterMessage(t, &codecs.TimerEvent{
					LeadershipTermId: 1,
					CorrelationId:    math.MinInt64,
					Timestamp:        500,
				})
			},
		},
		{
			name:       "session open",
			templateId: sessionOpenTemplateId,
			termId:     1,
			cause:      "Range check failed on s.ClusterSessionId",
			message: func(t *testing.T) *atomic.Buffer {
				return encodedClusterMessage(t, &codecs.SessionOpenEvent{
					LeadershipTermId: 1,
					CorrelationId:    2,
					ClusterSessionId: math.MinInt64,
					Timestamp:        500,
					ResponseStreamId: 3,
					ResponseChannel:  []byte("aeron:ipc"),
				})
			},
		},
		{
			name:       "session close",
			templateId: sessionCloseTemplateId,
			termId:     1,
			cause:      "Range check failed on CloseReason, unknown enumeration value 99",
			message: func(t *testing.T) *atomic.Buffer {
				return encodedClusterMessage(t, &codecs.SessionCloseEvent{
					LeadershipTermId: 1,
					ClusterSessionId: 7,
					Timestamp:        500,
					CloseReason:      codecs.CloseReasonEnum(99),
				})
			},
		},
		{
			name:       "cluster action request",
			templateId: clusterActionReqTemplateId,
			termId:     1,
			cause:      "Range check failed on ClusterAction, unknown enumeration value 99",
			message: func(t *testing.T) *atomic.Buffer {
				return encodedClusterMessage(t, &codecs.ClusterActionRequest{
					LeadershipTermId: 1,
					LogPosition:      2,
					Timestamp:        500,
					Action:           99,
				})
			},
		},
		{
			name:       "new leadership term",
			templateId: newLeadershipTermTemplateId,
			termId:     math.MinInt64,
			cause:      "Range check failed on n.LeadershipTermId",
			message: func(t *testing.T) *atomic.Buffer {
				return encodedClusterMessage(t, &codecs.NewLeadershipTermEvent{
					LeadershipTermId: math.MinInt64,
					LogPosition:      2,
					Timestamp:        500,
					TimeUnit:         codecs.ClusterTimeUnit.MILLIS,
				})
			},
		},
		{
			name:       "membership change",
			templateId: membershipChangeTemplateId,
			termId:     math.MinInt64,
			cause:      "Range check failed on m.LeadershipTermId",
			message: func(t *testing.T) *atomic.Buffer {
				return encodedClusterMessage(t, &codecs.MembershipChangeEvent{
					LeadershipTermId: math.MinInt64,
					LogPosition:      2,
					Timestamp:        500,
					ChangeType:       codecs.ChangeType.JOIN,
					MemberId:         1,
					ClusterSize:      1,
				})
			},
		},
		{
			name:       "schema id is not the cluster schema",
			templateId: timerEventTemplateId,
			termId:     1,
			cause:      "unexpected schemaId",
			message: func(t *testing.T) *atomic.Buffer {
				msg := encodedClusterMessage(t, &codecs.TimerEvent{
					LeadershipTermId: 1,
					CorrelationId:    2,
					Timestamp:        500,
				})
				msg.PutUInt16(4, ClusterSchemaId+1)
				return msg
			},
		},
		{
			name:       "fragment shorter than the SBE header",
			templateId: 0,
			termId:     NullValue,
			cause:      "shorter than the SBE header",
			message: func(*testing.T) *atomic.Buffer {
				return atomic.MakeBuffer([]byte{0x01, 0x02, 0x03})
			},
		},
		{
			name:       "timer event body shorter than its block",
			templateId: timerEventTemplateId,
			termId:     1,
			cause:      "timer event body is 8 bytes, shorter than 24",
			message: func(*testing.T) *atomic.Buffer {
				return clusterMessage(timerEventTemplateId, timerEventBlockLength, 0, []byte{1, 0, 0, 0, 0, 0, 0, 0})
			},
		},
		{
			name:       "unknown template",
			templateId: 99,
			termId:     NullValue,
			cause:      "unexpected templateId=99",
			message: func(*testing.T) *atomic.Buffer {
				return clusterMessage(99, 0, 0, nil)
			},
		},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			logs := captureErrorLogs(t)
			service := &allCallsRecorder{}
			agent := &ClusteredServiceAgent{
				opts:        NewOptions(),
				service:     service,
				sessions:    map[int64]ClientSession{7: &containerClientSession{id: 7}},
				clusterTime: 1234,
				logPosition: 99,
			}
			adapter := newBoundedLogAdapter(&Options{LogFragmentLimit: 10})
			adapter.agent = agent
			msg := tt.message(t)
			next := encodedClusterMessage(t, &codecs.TimerEvent{
				LeadershipTermId: 1,
				CorrelationId:    99,
				Timestamp:        999,
			})
			img := &scriptedImage{queue: []scriptedFragment{
				{buf: msg, flags: testFlagUnfragmented},
				{buf: next, flags: testFlagUnfragmented},
			}}
			adapter.image = img

			stopOf(t, func() { adapter.poll(math.MaxInt64) })

			fields := requireUndecodableEntry(t, logs, msg, tt.templateId, tt.termId)
			assert.Contains(t, fields.cause, tt.cause)

			assert.Zero(t, service.calls, "the agent applies nothing")
			assert.EqualValues(t, 1234, agent.clusterTime)
			assert.EqualValues(t, 99, agent.logPosition)
			assert.Len(t, agent.sessions, 1, "the session table is untouched")
			require.Len(t, img.queue, 1, "the next fragment in the same poll is not read")
		})
	}
}
