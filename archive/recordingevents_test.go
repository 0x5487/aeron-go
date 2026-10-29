package archive

import (
	"math"
	"testing"

	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// recordingEventsRecorder records what the recording-events handler tells its listeners.
type recordingEventsRecorder struct {
	started  []*codecs.RecordingStarted
	progress []*codecs.RecordingProgress
	stopped  []*codecs.RecordingStopped
	errs     []error
}

func (r *recordingEventsRecorder) listeners() *ArchiveListeners {
	return &ArchiveListeners{
		ErrorListener:                  func(err error) { r.errs = append(r.errs, err) },
		RecordingEventStartedListener:  func(e *codecs.RecordingStarted) { r.started = append(r.started, e) },
		RecordingEventProgressListener: func(e *codecs.RecordingProgress) { r.progress = append(r.progress, e) },
		RecordingEventStoppedListener:  func(e *codecs.RecordingStopped) { r.stopped = append(r.stopped, e) },
	}
}

func (r *recordingEventsRecorder) delivered() int {
	return len(r.started) + len(r.progress) + len(r.stopped)
}

// captureArchiveLogs sends the archive logger to an observer for the duration of the test.
func captureArchiveLogs(t *testing.T) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	previous := logger.Sugar()
	logger.SetSugar(zap.New(core).Sugar())
	t.Cleanup(func() { logger.SetSugar(previous) })
	return logs
}

// A recording event whose id is outside its schema range is reported once and is not delivered
// (reFragmentHandler).
func TestRecordingEventsRejectsOutOfRangeEvents(t *testing.T) {
	tests := []struct {
		name  string
		event encodable
	}{
		{"RecordingStarted", &codecs.RecordingStarted{RecordingId: math.MinInt64}},
		{"RecordingProgress", &codecs.RecordingProgress{RecordingId: math.MinInt64}},
		{"RecordingStopped", &codecs.RecordingStopped{RecordingId: math.MinInt64}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingEventsRecorder{}
			rea := &RecordingEventsAdapter{archive: &Archive{Listeners: rec.listeners()}}
			msg := encode(t, tt.event)

			rea.reFragmentHandler(rea.archive.Listeners, msg, 0, msg.Capacity(), nil)

			assert.Zero(t, rec.delivered(), "an event outside its schema range is not delivered")
			if assert.Len(t, rec.errs, 1) {
				assert.ErrorContains(t, rec.errs[0], "Decode() of "+tt.name+" failed")
				assert.ErrorContains(t, rec.errs[0], "Range check failed on r.RecordingId")
			}
		})
	}
}

// The recording-events handler reported a header decode error and then kept going into its switch
// (reFragmentHandler): the default branch logged another error for template 0.
func TestRecordingEventsStopsAfterHeaderDecodeError(t *testing.T) {
	rec := &recordingEventsRecorder{}
	rea := &RecordingEventsAdapter{archive: &Archive{Listeners: rec.listeners()}}
	logs := captureArchiveLogs(t)
	msg := truncatedMessage(0)

	rea.reFragmentHandler(rea.archive.Listeners, msg, 0, msg.Capacity(), nil)

	if assert.Len(t, rec.errs, 1) {
		assert.ErrorContains(t, rec.errs[0], "failed to decode control message header")
	}
	assert.Empty(t, logs.All(), "the fragment ends after the header decode error")
}
