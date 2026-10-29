package cluster

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/cluster/codecs"
)

// idleImage is an aeron.Image that never has a fragment. Only BoundedPoll and Poll are implemented; calling any other
// method panics on the nil embedded interface.
type idleImage struct{ aeron.Image }

func (idleImage) BoundedPoll(term.FragmentHandler, int64, int) int { return 0 }
func (idleImage) Poll(term.FragmentHandler, int) int               { return 0 }

// scriptedFragment is one fragment handed to the handler by scriptedImage.
type scriptedFragment struct {
	buf   *atomic.Buffer
	flags uint8
}

// scriptedImage delivers its queued fragments in order to the handler it is polled with, up to the fragment limit.
// It stands in for the transport, which is the only boundary the adapters are not built to see through.
type scriptedImage struct {
	aeron.Image
	queue []scriptedFragment
}

func (img *scriptedImage) BoundedPoll(handler term.FragmentHandler, _ int64, limit int) int {
	return img.deliver(handler, limit)
}

func (img *scriptedImage) Poll(handler term.FragmentHandler, limit int) int {
	return img.deliver(handler, limit)
}

func (img *scriptedImage) deliver(handler term.FragmentHandler, limit int) int {
	n := 0
	for n < limit && len(img.queue) > 0 {
		f := img.queue[0]
		img.queue = img.queue[1:]
		handler(f.buf, 0, f.buf.Capacity(), fragmentHeader(f.flags))
		n++
	}
	return n
}

const (
	testFlagBegin        uint8 = 0x80
	testFlagEnd          uint8 = 0x40
	testFlagUnfragmented uint8 = 0x80 | 0x40
)

// fragmentHeader returns a log header whose frame carries the given flags. The frame length and term id give the header
// the position 96, so a test can tell the header's position from the zero value of an unset header.
func fragmentHeader(flags uint8) *logbuffer.Header {
	frame := atomic.MakeBuffer(make([]byte, logbuffer.DataFrameHeader.Length))
	logbuffer.FrameFlags(frame, 0, flags)
	logbuffer.SetFrameLength(frame, 0, 64)
	frame.PutInt32(logbuffer.DataFrameHeader.TermIDFieldOffset, 32)
	return new(logbuffer.Header).Wrap(frame.Ptr(), frame.Capacity())
}

// clusterMessage builds a cluster SBE message: the 8 byte header followed by body.
func clusterMessage(templateID, blockLength, version uint16, body []byte) *atomic.Buffer {
	var msg bytes.Buffer
	var hdr [8]byte
	binary.LittleEndian.PutUint16(hdr[0:], blockLength)
	binary.LittleEndian.PutUint16(hdr[2:], templateID)
	binary.LittleEndian.PutUint16(hdr[4:], ClusterSchemaId)
	binary.LittleEndian.PutUint16(hdr[6:], version)
	msg.Write(hdr[:])
	msg.Write(body)
	return atomic.MakeBuffer(msg.Bytes())
}

func timerEventMessage(correlationID, timestamp int64) *atomic.Buffer {
	var body [24]byte
	binary.LittleEndian.PutUint64(body[8:], uint64(correlationID))
	binary.LittleEndian.PutUint64(body[16:], uint64(timestamp))
	return clusterMessage(timerEventTemplateId, 24, 0, body[:])
}

// timerRecorder is a ClusteredService that records timer events. Every other method panics (nil embedded interface).
type timerRecorder struct {
	ClusteredService
	events [][2]int64
}

func (s *timerRecorder) OnTimerEvent(correlationID, timestamp int64) {
	s.events = append(s.events, [2]int64{correlationID, timestamp})
}

func TestBoundedLogAdapterPollAllocatesNothingWhenIdle(t *testing.T) {
	adapter := newBoundedLogAdapter(&Options{LogFragmentLimit: 10})
	adapter.image = idleImage{}

	allocs := testing.AllocsPerRun(1000, func() { adapter.poll(math.MaxInt64) })
	assert.Equal(t, float64(0), allocs)
}

// A legal session message takes the manual decode path without allocating (boundedLogAdapter.onMessage). The measured
// baseline before the range check was added is 0 allocations per message.
func TestBoundedLogAdapterSessionMessageAllocatesNothing(t *testing.T) {
	msg := encodedClusterMessage(t, &codecs.SessionMessageHeader{
		LeadershipTermId: 1,
		ClusterSessionId: 7,
		Timestamp:        500,
	})
	header := fragmentHeader(testFlagUnfragmented)
	agent := &ClusteredServiceAgent{
		service:  &sessionRecorder{},
		sessions: map[int64]ClientSession{7: &containerClientSession{id: 7}},
	}
	adapter := newBoundedLogAdapter(NewOptions())
	adapter.agent = agent

	allocs := testing.AllocsPerRun(1000, func() {
		adapter.onMessage(msg, 0, msg.Capacity(), header)
	})
	assert.Equal(t, float64(0), allocs)
}

func TestBoundedLogAdapterDeliversEveryFragmentInOrder(t *testing.T) {
	svc := &timerRecorder{}
	adapter := newBoundedLogAdapter(&Options{LogFragmentLimit: 10})
	adapter.agent = &ClusteredServiceAgent{service: svc}
	adapter.image = &scriptedImage{queue: []scriptedFragment{
		{buf: timerEventMessage(7, 100), flags: testFlagUnfragmented},
		{buf: timerEventMessage(8, 200), flags: testFlagUnfragmented},
	}}

	assert.Equal(t, 2, adapter.poll(math.MaxInt64))
	assert.Equal(t, [][2]int64{{7, 100}, {8, 200}}, svc.events)
}

func TestBoundedLogAdapterReassemblesFragmentedMessage(t *testing.T) {
	msg := timerEventMessage(9, 300)
	raw := msg.GetBytesArray(0, msg.Capacity())
	half := len(raw) / 2

	svc := &timerRecorder{}
	adapter := newBoundedLogAdapter(&Options{LogFragmentLimit: 10})
	adapter.agent = &ClusteredServiceAgent{service: svc}
	adapter.image = &scriptedImage{queue: []scriptedFragment{
		{buf: atomic.MakeBuffer(raw[:half]), flags: testFlagBegin},
		{buf: atomic.MakeBuffer(raw[half:]), flags: testFlagEnd},
	}}

	adapter.poll(math.MaxInt64)
	assert.Equal(t, [][2]int64{{9, 300}}, svc.events)
}

func TestServiceAdapterPollAllocatesNothingWhenIdle(t *testing.T) {
	sub := aeron.NewSubscription(nil, "aeron:ipc", 1, 1, 1, nil, nil)
	adapter := newServiceAdapter(sub)

	allocs := testing.AllocsPerRun(1000, func() { adapter.poll() })
	assert.Equal(t, float64(0), allocs)
}

func TestServiceAdapterFragmentHandlerDispatchesServiceTerminationPosition(t *testing.T) {
	adapter := newServiceAdapter(nil)
	agent := &ClusteredServiceAgent{}
	adapter.agent = agent

	var body [8]byte
	binary.LittleEndian.PutUint64(body[:], 4242)
	msg := clusterMessage(serviceTerminationPosTemplateId, 8, 0, body[:])
	adapter.fragmentHandler(msg, 0, msg.Capacity(), fragmentHeader(testFlagUnfragmented))

	assert.Equal(t, int64(4242), agent.terminationPosition)
}

func TestSnapshotLoaderPollAllocatesNothingWhenIdle(t *testing.T) {
	loader := newSnapshotLoader(nil, idleImage{})

	allocs := testing.AllocsPerRun(1000, func() { loader.poll() })
	assert.Equal(t, float64(0), allocs)
}

func snapshotMarkerMessage(t *testing.T, mark codecs.SnapshotMarkEnum, appVersion int32) *atomic.Buffer {
	t.Helper()
	marker := codecs.SnapshotMarker{
		TypeId:     snapshotTypeId,
		Mark:       mark,
		AppVersion: appVersion,
		TimeUnit:   codecs.ClusterTimeUnit.MILLIS,
	}
	var body bytes.Buffer
	require.NoError(t, marker.Encode(codecs.NewSbeGoMarshaller(), &body, true))
	return clusterMessage(snapshotMarkerTemplateId, marker.SbeBlockLength(), marker.SbeSchemaVersion(), body.Bytes())
}

func TestSnapshotLoaderReadsBeginAndEndMarkers(t *testing.T) {
	const appVersion = int32(0x00010203)
	img := &scriptedImage{queue: []scriptedFragment{
		{buf: snapshotMarkerMessage(t, codecs.SnapshotMark.BEGIN, appVersion), flags: testFlagUnfragmented},
		{buf: snapshotMarkerMessage(t, codecs.SnapshotMark.END, appVersion), flags: testFlagUnfragmented},
	}}
	loader := newSnapshotLoader(nil, img)

	for i := 0; !loader.isDone && i < 2; i++ {
		loader.poll()
	}
	require.True(t, loader.isDone)
	require.NoError(t, loader.err)
	assert.False(t, loader.inSnapshot)
	assert.Equal(t, appVersion, loader.appVersion)
}

// A session open whose cluster session id is outside its schema range stops the service with one Error line before
// the agent sees it. onSessionOpen sets clusterTime first, so an applied event would leave the agent's clock at 500.
func TestBoundedLogAdapterRejectsOutOfRangeSessionOpen(t *testing.T) {
	logs := captureErrorLogs(t)
	event := codecs.SessionOpenEvent{
		LeadershipTermId: 1,
		CorrelationId:    2,
		ClusterSessionId: math.MinInt64,
		Timestamp:        500,
		ResponseStreamId: 3,
		ResponseChannel:  []byte("aeron:ipc"),
	}
	var body bytes.Buffer
	// Encoded without the range check: the decoder under test must reject the event.
	require.NoError(t, event.Encode(codecs.NewSbeGoMarshaller(), &body, false))
	msg := clusterMessage(sessionOpenTemplateId, event.SbeBlockLength(), event.SbeSchemaVersion(), body.Bytes())

	agent := &ClusteredServiceAgent{sessions: map[int64]ClientSession{}}
	adapter := newBoundedLogAdapter(NewOptions())
	adapter.agent = agent
	adapter.image = &scriptedImage{queue: []scriptedFragment{{
		buf: msg, flags: testFlagUnfragmented,
	}}}

	stopOf(t, func() { adapter.poll(math.MaxInt64) })
	requireUndecodableEntry(t, logs, msg, sessionOpenTemplateId, 1)
	assert.Zero(t, agent.clusterTime)
	assert.Empty(t, agent.sessions)
}
