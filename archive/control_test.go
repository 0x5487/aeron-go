package archive

import (
	"bytes"
	"io"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestControl_PollForResponse(t *testing.T) {
	t.Run("times out when nothing to poll", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.Zero(t, id)
        assert.EqualError(t, err, `timeout waiting for correlationID 1`)
	})

	t.Run("returns the error response", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{
				Code:             codecs.ControlResponseCode.ERROR,
				CorrelationId:    1,
				ErrorMessage:     []byte(`b0rk`),
				RelevantId:       3,
			},
		)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.EqualValues(t, 3, id)
		assert.EqualError(t, err, `Control Response failure: b0rk`)
	})

	t.Run("discards all responses preceding the first error", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
			&codecs.ControlResponse{
				Code:             codecs.ControlResponseCode.ERROR,
				CorrelationId:    1,
				ErrorMessage:     []byte(`b0rk`),
				RelevantId:       3,
			},
		)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.EqualValues(t, 3, id)
		assert.EqualError(t, err, `Control Response failure: b0rk`)
	})

	t.Run("does not process messages after result", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{
				Code:             codecs.ControlResponseCode.ERROR,
				CorrelationId:    1,
				ErrorMessage:     []byte(`b0rk`),
				RelevantId:       3,
			},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
		)
		correlations.Store(int64(1), control)
		id, err := control.PollForResponse(1, 0)
		assert.EqualValues(t, 3, id)
		assert.EqualError(t, err, `Control Response failure: b0rk`)
		fragments := image.Poll(func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) {}, 1)
		assert.EqualValues(t, 1, fragments)
	})
}

func TestControl_PollForErrorResponse(t *testing.T) {
	t.Run("returns zero when nothing to poll", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image)
		cnt, err := control.PollForErrorResponse()
		assert.Zero(t, cnt)
		assert.NoError(t, err)
	})

	t.Run("returns the error response", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{
				Code:         codecs.ControlResponseCode.ERROR,
				ErrorMessage: []byte(`b0rk`),
			},
		)
		cnt, err := control.PollForErrorResponse()
		assert.EqualValues(t, 1, cnt)
		assert.EqualError(t, err, `PollForErrorResponse received a ControlResponse (correlationId:0 Code:ERROR error="b0rk"`)
	})

	t.Run("discards all queued responses unless error", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
		)
		cnt, err := control.PollForErrorResponse()
		assert.EqualValues(t, 2, cnt)
		assert.NoError(t, err)
	})

	t.Run("does not process messages after error", func(t *testing.T) {
		control, image := newTestControl(t)
		mockPollResponses(t, image,
			&codecs.ControlResponse{
				Code:         codecs.ControlResponseCode.ERROR,
				ErrorMessage: []byte(`b0rk`),
			},
			&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK},
		)
		cnt, err := control.PollForErrorResponse()
		assert.EqualValues(t, 1, cnt)
		assert.Error(t, err)
		cnt, err = control.PollForErrorResponse()
		assert.EqualValues(t, 1, cnt)
		assert.NoError(t, err)
	})
}

func mockPollResponses(t *testing.T, image *aeron.MockImage, responses ...encodable) {
	// Subscription.IsConnected, called by PollForErrorResponse, asks every image.
	image.On("IsClosed").Return(false).Maybe()
	poll := image.On("Poll", mock.Anything, mock.Anything)
	poll.Maybe()
	poll.Run(func(args mock.Arguments) {
		handler := args.Get(0).(term.FragmentHandler)
		fragmentCount := args.Get(1).(int)
		count := image.ControlledPoll(func(buffer *atomic.Buffer, offset, length int32, header *logbuffer.Header) term.ControlledPollAction {
			handler(buffer, offset, length, header)
			return term.ControlledPollActionContinue
		}, fragmentCount)
		poll.Return(count)
	})
	controlledPoll := image.On("ControlledPoll", mock.Anything, mock.Anything)
	controlledPoll.Run(func(args mock.Arguments) {
		fragmentCount := args.Get(1).(int)
		count := 0
		for count < fragmentCount {
			if len(responses) == 0 {
				break
			}
			buffer := encode(t, responses[0])
			action := args.Get(0).(term.ControlledFragmentHandler)(buffer, 0, buffer.Capacity(), newTestHeader())
			if action == term.ControlledPollActionAbort {
				break
			}
			responses = responses[1:]
			count++
			if action == term.ControlledPollActionBreak {
				break
			}
		}
		controlledPoll.Return(count)
	})
}

type encodable interface {
	SbeBlockLength() uint16
	SbeTemplateId() uint16
	SbeSchemaId() uint16
	SbeSchemaVersion() uint16
	Encode(*codecs.SbeGoMarshaller, io.Writer, bool) error
}

func encode(t *testing.T, data encodable) *atomic.Buffer {
	m := codecs.NewSbeGoMarshaller()
	buf := new(bytes.Buffer)
	header := codecs.MessageHeader{
		BlockLength: data.SbeBlockLength(),
		TemplateId:  data.SbeTemplateId(),
		SchemaId:    data.SbeSchemaId(),
		Version:     data.SbeSchemaVersion(),
	}
	if !assert.NoError(t, header.Encode(m, buf)) {
		return nil
	}
	// Encoded without the range check: a test builds a message the decoder under test must reject.
	if !assert.NoError(t, data.Encode(m, buf, false)) {
		return nil
	}
	return atomic.MakeBuffer(buf.Bytes())
}

func newTestControl(t *testing.T) (*Control, *aeron.MockImage) {
	image := aeron.NewMockImage(t)
	c := newControl(&Archive{
		Listeners: &ArchiveListeners{},
		Options:   &Options{
			Timeout: 100*time.Millisecond,
			IdleStrategy: idlestrategy.Yielding{},
		},
	})
	c.Subscription = newTestSub(image)
	return c, image
}

func newTestSub(image aeron.Image) *aeron.Subscription {
	images := aeron.NewImageList()
	images.Set([]aeron.Image{image})
	sub := aeron.NewSubscription(nil, "", 1, 2, 3, nil, nil)
	rsub := reflect.ValueOf(sub)
	rfimages := rsub.Elem().FieldByName("images")
	rfimages = reflect.NewAt(rfimages.Type(), unsafe.Pointer(rfimages.UnsafeAddr())).Elem()
	rfimages.Set(reflect.ValueOf(images))
	return sub
}

func newTestHeader() *logbuffer.Header {
	buffer := atomic.MakeBuffer(make([]byte, logbuffer.DataFrameHeader.Length))
	buffer.PutUInt8(logbuffer.DataFrameHeader.FlagsFieldOffset, 0xc0) // unfragmented
	return new(logbuffer.Header).Wrap(buffer.Ptr(), buffer.Capacity())
}

// Two clients decode in one process, the trading-poc case of several readers and writers each with its own Archive.
// Each Control polls on its own goroutine; with -race any state the decoders share between clients is reported.
func TestControl_TwoControlsPollConcurrently(t *testing.T) {
	var wg sync.WaitGroup
	for i := int64(1); i <= 2; i++ {
		id := i
		control, image := newTestControl(t)
		mockPollResponses(t, image, &codecs.ControlResponse{
			Code:          codecs.ControlResponseCode.OK,
			CorrelationId: id,
			RelevantId:    id * 10,
		})
		correlations.Store(id, control)
		t.Cleanup(func() { correlations.Delete(id) })

		wg.Add(1)
		go func() {
			defer wg.Done()
			relevantId, err := control.PollForResponse(id, 0)
			assert.NoError(t, err)
			assert.Equal(t, id*10, relevantId)
		}()
	}
	wg.Wait()
}

// The decoders check every field against the schema, including the enumeration values of ControlResponse.Code.
// A code the schema does not list is an error, reported to the ErrorListener, and the response is not delivered.
func TestControl_UnknownControlResponseCodeIsAnError(t *testing.T) {
	const unknownCode = codecs.ControlResponseCodeEnum(99)

	t.Run("PollForResponse", func(t *testing.T) {
		control, image := newTestControl(t)
		var errs []error
		control.archive.Listeners.ErrorListener = func(err error) { errs = append(errs, err) }
		mockPollResponses(t, image, &codecs.ControlResponse{Code: unknownCode, CorrelationId: 1})
		correlations.Store(int64(1), control)
		t.Cleanup(func() { correlations.Delete(int64(1)) })

		_, err := control.PollForResponse(1, 0)
		assert.EqualError(t, err, `timeout waiting for correlationID 1`)
		assert.Nil(t, control.Results.ControlResponse)
		if assert.Len(t, errs, 1) {
			assert.ErrorContains(t, errs[0], "failed to decode control response")
			assert.ErrorContains(t, errs[0], "unknown enumeration value 99")
		}
	})

	t.Run("PollForErrorResponse", func(t *testing.T) {
		control, image := newTestControl(t)
		var errs []error
		control.archive.Listeners.ErrorListener = func(err error) { errs = append(errs, err) }
		mockPollResponses(t, image, &codecs.ControlResponse{Code: unknownCode, CorrelationId: 1})

		_, err := control.PollForErrorResponse()
		assert.NoError(t, err)
		assert.Nil(t, control.Results.ErrorResponse)
		if assert.Len(t, errs, 1) {
			assert.ErrorContains(t, errs[0], "unknown enumeration value 99")
		}
	})
}

// RecordingId is an int64 whose schema range is [MinInt64+1, MaxInt64]; MinInt64 is outside it.
func TestControl_RecordingDescriptorOutOfRangeIsAnError(t *testing.T) {
	control, image := newTestControl(t)
	var errs []error
	control.archive.Listeners.ErrorListener = func(err error) { errs = append(errs, err) }
	mockPollResponses(t, image, &codecs.RecordingDescriptor{CorrelationId: 1, RecordingId: math.MinInt64})
	correlations.Store(int64(1), control)
	t.Cleanup(func() { correlations.Delete(int64(1)) })

	err := control.PollForDescriptors(1, 0, 1)
	assert.Error(t, err)
	assert.Empty(t, control.Results.RecordingDescriptors)
	if assert.Len(t, errs, 1) {
		assert.ErrorContains(t, errs[0], "failed to decode RecordingDescriptor")
		assert.ErrorContains(t, errs[0], "Range check failed on r.RecordingId")
	}
}
