package archive

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pingMsg is template 106 of the archive schema (Ping, since schema version 11). The client has no codec for it: it
// must be skipped quietly like any other message that is not meant for the poller.
type pingMsg struct{ controlSessionID int64 }

func (pingMsg) SbeBlockLength() uint16   { return 8 }
func (pingMsg) SbeTemplateId() uint16    { return 106 }
func (pingMsg) SbeSchemaId() uint16      { return 101 }
func (pingMsg) SbeSchemaVersion() uint16 { return 14 }
func (p pingMsg) Encode(m *codecs.SbeGoMarshaller, w io.Writer, _ bool) error {
	return m.WriteInt64(w, p.controlSessionID)
}

func signalEvent(session int64, recordingID int64, signal codecs.RecordingSignalEnum) *codecs.RecordingSignalEvent {
	return &codecs.RecordingSignalEvent{ControlSessionId: session, RecordingId: recordingID, Signal: signal}
}

// captureStdout returns what fn writes to os.Stdout.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	orig := os.Stdout
	os.Stdout = w
	var buf bytes.Buffer
	copied := make(chan struct{})
	go func() {
		_, _ = buf.ReadFrom(r)
		close(copied)
	}()
	func() {
		defer func() { os.Stdout = orig; _ = w.Close() }()
		fn()
	}()
	<-copied
	_ = r.Close()
	return buf.String()
}

// The RecordingSignalEvent must reach RecordingSignalListener on every poll path (trading-poc ticket
// aeron-migration 01; PurgeSegments is answered with a DELETE signal).
func TestControl_RecordingSignalReachesListener(t *testing.T) {
	const session = 77
	tests := []struct {
		name string
		poll func(c *Control) error
	}{
		{"PollForResponse", func(c *Control) error {
			correlations.Store(int64(9), c)
			t.Cleanup(func() { correlations.Delete(int64(9)) })
			_, err := c.PollForResponse(9, session)
			return err
		}},
		{"Poll", func(c *Control) error { c.Poll(); return nil }},
		{"PollForErrorResponse", func(c *Control) error { _, err := c.PollForErrorResponse(); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			control, image := newTestControl(t)
			control.archive.SessionID = session
			msgs := []encodable{signalEvent(session, 5, codecs.RecordingSignal.DELETE)}
			if tt.name == "PollForResponse" {
				msgs = append(msgs, &codecs.ControlResponse{Code: codecs.ControlResponseCode.OK, CorrelationId: 9, ControlSessionId: session})
			}
			mockPollResponses(t, image, msgs...)
			var got []*codecs.RecordingSignalEvent
			control.archive.Listeners.RecordingSignalListener = func(ev *codecs.RecordingSignalEvent) { got = append(got, ev) }

			require.NoError(t, tt.poll(control))

			require.Len(t, got, 1, "listener must be called exactly once per event")
			assert.EqualValues(t, 5, got[0].RecordingId)
			assert.Equal(t, codecs.RecordingSignal.DELETE, got[0].Signal)
		})
	}
}

// A signal addressed to another control session sharing the response stream is not ours: PollForErrorResponse's own
// contract is "ignore messages not on our session ID".
func TestControl_PollForErrorResponse_IgnoresOtherSessionsSignal(t *testing.T) {
	control, image := newTestControl(t)
	control.archive.SessionID = 77
	mockPollResponses(t, image, signalEvent(78, 5, codecs.RecordingSignal.DELETE))
	called := 0
	control.archive.Listeners.RecordingSignalListener = func(*codecs.RecordingSignalEvent) { called++ }

	_, err := control.PollForErrorResponse()
	require.NoError(t, err)
	assert.Zero(t, called)
}

// Template 106 (Ping) shows up on the response stream of a busy archive: no output on stdout, the poll goes on.
func TestControl_PingIsSkippedQuietly(t *testing.T) {
	control, image := newTestControl(t)
	mockPollResponses(t, image,
		pingMsg{controlSessionID: 77},
		&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK, CorrelationId: 9, RelevantId: 4},
	)
	correlations.Store(int64(9), control)
	t.Cleanup(func() { correlations.Delete(int64(9)) })

	var id int64
	var err error
	out := captureStdout(t, func() { id, err = control.PollForResponse(9, 0) })

	require.NoError(t, err)
	assert.EqualValues(t, 4, id)
	assert.NotContains(t, out, "Unexpected message type")
}
