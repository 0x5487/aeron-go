package archive

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/archive/codecs"
)

// idleControlImage is an aeron.Image with no fragments. Only ControlledPoll is implemented; any other method panics
// on the nil embedded interface.
type idleControlImage struct{ aeron.Image }

func (idleControlImage) ControlledPoll(term.ControlledFragmentHandler, int) int { return 0 }

// responseImage delivers its queued control responses, one per fragment, to the handler it is polled with.
type responseImage struct {
	aeron.Image
	t       *testing.T
	pending []encodable
}

func (img *responseImage) ControlledPoll(handler term.ControlledFragmentHandler, limit int) int {
	n := 0
	for n < limit && len(img.pending) > 0 {
		buf := encode(img.t, img.pending[0])
		img.pending = img.pending[1:]
		n++
		if handler(buf, 0, buf.Capacity(), newTestHeader()) != term.ControlledPollActionContinue {
			break
		}
	}
	return n
}

func TestControlPollAllocatesNothingWhenIdle(t *testing.T) {
	control := newControl(&Archive{Options: DefaultOptions()})
	control.Subscription = newTestSub(idleControlImage{})

	allocs := testing.AllocsPerRun(1000, func() { control.Poll() })
	assert.Equal(t, float64(0), allocs)
}

func TestControlPollDeliversControlResponse(t *testing.T) {
	control := newControl(&Archive{Options: DefaultOptions(), Listeners: &ArchiveListeners{}})
	control.Subscription = newTestSub(&responseImage{t: t, pending: []encodable{
		&codecs.ControlResponse{Code: codecs.ControlResponseCode.OK, CorrelationId: 5},
	}})

	require.Equal(t, 1, control.Poll())
	require.NotNil(t, control.Results.ControlResponse)
	assert.True(t, control.Results.IsPollComplete)
	assert.Equal(t, int64(5), control.Results.CorrelationId)
}
