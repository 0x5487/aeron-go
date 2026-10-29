package archive

import (
	"math"
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
)

// The archive schema gives every id the range [MinInt64+1, MaxInt64]; MinInt64 is outside it. A response field outside
// its range is a decode error: it is reported once, and the fragment ends without a result or a listener call.
const outOfRangeID = math.MinInt64

// controlFragment returns a Control whose ErrorListener and RecordingSignalListener record their calls.
func controlFragment(t *testing.T) (*Control, *errorRecorder) {
	t.Helper()
	control, _ := newTestControl(t)
	rec := &errorRecorder{}
	control.archive.Listeners.ErrorListener = rec.onError
	control.archive.Listeners.RecordingSignalListener = rec.onSignal
	return control, rec
}

type errorRecorder struct {
	errs    []error
	signals []*codecs.RecordingSignalEvent
}

func (r *errorRecorder) onError(err error)                       { r.errs = append(r.errs, err) }
func (r *errorRecorder) onSignal(e *codecs.RecordingSignalEvent) { r.signals = append(r.signals, e) }
func (r *errorRecorder) requireOneError(t *testing.T, contains string) {
	t.Helper()
	if assert.Len(t, r.errs, 1) {
		assert.ErrorContains(t, r.errs[0], contains)
	}
}

func storeCorrelation(t *testing.T, correlationID int64, control *Control) {
	t.Helper()
	correlations.Store(correlationID, control)
	t.Cleanup(func() { correlations.Delete(correlationID) })
}

// A RecordingSignalEvent whose RecordingId is outside its schema range was reported to the ErrorListener and then
// handed to the RecordingSignalListener as the half-decoded struct (controlFragmentHandler).
func TestControlFragmentHandlerRejectsOutOfRangeRecordingSignal(t *testing.T) {
	control, rec := controlFragment(t)
	storeCorrelation(t, 9, control)
	msg := encode(t, &codecs.RecordingSignalEvent{
		ControlSessionId: 1,
		CorrelationId:    2,
		RecordingId:      outOfRangeID,
		Signal:           codecs.RecordingSignal.DELETE,
	})

	controlFragmentHandler(&PollContext{control, 9}, msg, 0, msg.Capacity(), nil)

	assert.Empty(t, rec.signals, "a signal outside its schema range is not delivered")
	rec.requireOneError(t, "failed to decode recording signal")
	rec.requireOneError(t, "Range check failed on r.RecordingId")
}

// A Challenge whose ControlSessionId is outside its schema range was decoded, reported, and then compared with the
// expected auth challenge and used to update the connection state (ConnectionControlFragmentHandler).
func TestConnectionControlFragmentHandlerRejectsOutOfRangeChallenge(t *testing.T) {
	control, rec := controlFragment(t)
	control.archive.Options.AuthChallenge = []byte("expected")
	control.archive.SessionID = 5
	control.State.state = ControlStateConnectRequestSent
	storeCorrelation(t, 42, control)
	msg := encode(t, &codecs.Challenge{
		ControlSessionId: outOfRangeID,
		CorrelationId:    42,
		EncodedChallenge: []byte("received"),
	})

	ConnectionControlFragmentHandler(&PollContext{control, 42}, msg, 0, msg.Capacity(), nil)

	assert.Nil(t, control.State.err, "a challenge outside its schema range is not compared or applied")
	assert.Equal(t, ControlStateConnectRequestSent, control.State.state)
	assert.EqualValues(t, 5, control.archive.SessionID)
	rec.requireOneError(t, "failed to decode challenge")
	rec.requireOneError(t, "Range check failed on c.ControlSessionId")
}

// A Challenge whose CorrelationId is outside its schema range was reported and then matched against the control
// session, setting the error response (errorResponseFragmentHandler).
func TestErrorResponseRejectsOutOfRangeChallenge(t *testing.T) {
	control, rec := controlFragment(t)
	msg := encode(t, &codecs.Challenge{ControlSessionId: control.archive.SessionID, CorrelationId: outOfRangeID})

	control.errorResponseFragmentHandler(msg, 0, msg.Capacity(), nil)

	assert.Nil(t, control.Results.ErrorResponse)
	rec.requireOneError(t, "failed to decode challenge")
}

// A RecordingDescriptor whose RecordingId is outside its schema range was reported and then matched against the
// control session, setting the error response (control.go errorResponseFragmentHandler).
func TestErrorResponseRejectsOutOfRangeRecordingDescriptor(t *testing.T) {
	control, rec := controlFragment(t)
	msg := encode(t, &codecs.RecordingDescriptor{
		ControlSessionId: control.archive.SessionID,
		RecordingId:      outOfRangeID,
	})

	control.errorResponseFragmentHandler(msg, 0, msg.Capacity(), nil)

	assert.Nil(t, control.Results.ErrorResponse)
	rec.requireOneError(t, "failed to decode recordingDescriptor")
	rec.requireOneError(t, "Range check failed on r.RecordingId")
}

// A RecordingSubscriptionDescriptor whose SubscriptionId is outside its schema range was reported and then matched
// against the control session, setting the error response (errorResponseFragmentHandler).
func TestErrorResponseRejectsOutOfRangeRecordingSubscriptionDescriptor(t *testing.T) {
	control, rec := controlFragment(t)
	msg := encode(t, &codecs.RecordingSubscriptionDescriptor{
		ControlSessionId: control.archive.SessionID,
		CorrelationId:    3,
		SubscriptionId:   outOfRangeID,
	})

	control.errorResponseFragmentHandler(msg, 0, msg.Capacity(), nil)

	assert.Nil(t, control.Results.ErrorResponse)
	rec.requireOneError(t, "failed to decode recordingSubscription")
	rec.requireOneError(t, "Range check failed on r.SubscriptionId")
}

// A RecordingSignalEvent whose RecordingId is outside its schema range was reported and then delivered to the
// RecordingSignalListener as the half-decoded struct (errorResponseFragmentHandler).
func TestErrorResponseRejectsOutOfRangeRecordingSignal(t *testing.T) {
	control, rec := controlFragment(t)
	control.archive.SessionID = 77
	msg := encode(t, &codecs.RecordingSignalEvent{
		ControlSessionId: 77,
		RecordingId:      outOfRangeID,
		Signal:           codecs.RecordingSignal.DELETE,
	})

	control.errorResponseFragmentHandler(msg, 0, msg.Capacity(), nil)

	assert.Empty(t, rec.signals)
	rec.requireOneError(t, "failed to decode recording signal")
	rec.requireOneError(t, "Range check failed on r.RecordingId")
}

// A RecordingSubscriptionDescriptor whose SubscriptionId is outside its schema range is not added to the descriptors
// of the poll (DescriptorFragmentHandler).
func TestDescriptorFragmentHandlerRejectsOutOfRangeRecordingSubscriptionDescriptor(t *testing.T) {
	control, rec := controlFragment(t)
	storeCorrelation(t, 9, control)
	msg := encode(t, &codecs.RecordingSubscriptionDescriptor{
		ControlSessionId: control.archive.SessionID,
		CorrelationId:    9,
		SubscriptionId:   outOfRangeID,
	})

	DescriptorFragmentHandler(&PollContext{control, 9}, msg, 0, msg.Capacity(), nil)

	assert.Empty(t, control.Results.RecordingSubscriptionDescriptors)
	assert.Zero(t, control.Results.FragmentsReceived)
	rec.requireOneError(t, "failed to decode RecordingSubscriptioDescriptor")
	rec.requireOneError(t, "Range check failed on r.SubscriptionId")
}

// A ControlResponse whose RelevantId is outside its schema range does not complete the descriptor poll
// (DescriptorFragmentHandler).
func TestDescriptorFragmentHandlerRejectsOutOfRangeControlResponse(t *testing.T) {
	control, rec := controlFragment(t)
	storeCorrelation(t, 9, control)
	msg := encode(t, &codecs.ControlResponse{
		ControlSessionId: control.archive.SessionID,
		CorrelationId:    9,
		RelevantId:       outOfRangeID,
		Code:             codecs.ControlResponseCode.RECORDING_UNKNOWN,
	})

	DescriptorFragmentHandler(&PollContext{control, 9}, msg, 0, msg.Capacity(), nil)

	assert.Nil(t, control.Results.ControlResponse)
	assert.False(t, control.Results.IsPollComplete)
	rec.requireOneError(t, "failed to decode control response")
	rec.requireOneError(t, "Range check failed on c.RelevantId")
}

// A RecordingSignalEvent whose RecordingId is outside its schema range is not delivered by the descriptor poll
// (DescriptorFragmentHandler).
func TestDescriptorFragmentHandlerRejectsOutOfRangeRecordingSignal(t *testing.T) {
	control, rec := controlFragment(t)
	storeCorrelation(t, 9, control)
	msg := encode(t, &codecs.RecordingSignalEvent{
		ControlSessionId: control.archive.SessionID,
		RecordingId:      outOfRangeID,
		Signal:           codecs.RecordingSignal.DELETE,
	})

	DescriptorFragmentHandler(&PollContext{control, 9}, msg, 0, msg.Capacity(), nil)

	assert.Empty(t, rec.signals)
	rec.requireOneError(t, "failed to decode recording signal")
	rec.requireOneError(t, "Range check failed on r.RecordingId")
}

// truncatedMessage is a message header cut after its TemplateId: the header decode fails and the handler must stop.
func truncatedMessage(templateID uint16) *atomic.Buffer {
	buf := atomic.MakeBuffer(make([]byte, 4))
	buf.PutUInt16(0, 0)
	buf.PutUInt16(2, templateID)
	return buf
}

// The connection handler reported a header decode error and then kept going into its switch
// (ConnectionControlFragmentHandler).
func TestConnectionControlFragmentHandlerStopsAfterHeaderDecodeError(t *testing.T) {
	control, rec := controlFragment(t)
	msg := truncatedMessage(codecIds.controlResponse)

	out := captureStdout(t, func() {
		ConnectionControlFragmentHandler(&PollContext{control, 0}, msg, 0, msg.Capacity(), nil)
	})

	rec.requireOneError(t, "failed to decode control message header")
	assert.NotContains(t, out, "Insert decoder", "the fragment ends after the header decode error")
}

// The error-response handler reported a header decode error and then kept going into its switch
// (errorResponseFragmentHandler).
func TestErrorResponseStopsAfterHeaderDecodeError(t *testing.T) {
	control, rec := controlFragment(t)
	msg := truncatedMessage(codecIds.controlResponse)

	out := captureStdout(t, func() {
		control.errorResponseFragmentHandler(msg, 0, msg.Capacity(), nil)
	})

	rec.requireOneError(t, "failed to decode control message header")
	assert.NotContains(t, out, "Insert decoder", "the fragment ends after the header decode error")
}
