package archive

import (
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultPollIntervalIs50ms(t *testing.T) {
	s, ok := DefaultOptions().IdleStrategy.(idlestrategy.Sleeping)
	require.True(t, ok)
	assert.Equal(t, 50*time.Millisecond, s.SleepFor)
	assert.Equal(t, 50*time.Millisecond, DefaultPollInterval)
}

func TestSetPollInterval(t *testing.T) {
	o := DefaultOptions()
	o.SetPollInterval(2 * time.Millisecond)
	s, ok := o.IdleStrategy.(idlestrategy.Sleeping)
	require.True(t, ok)
	assert.Equal(t, 2*time.Millisecond, s.SleepFor)

	// Changing one Options copy must not leak into the defaults.
	assert.Equal(t, DefaultPollInterval, DefaultOptions().IdleStrategy.(idlestrategy.Sleeping).SleepFor)
}

type countingIdler struct{ calls int }

func (c *countingIdler) Idle(int) { c.calls++ }

// TestProxyOfferIdlesWithConfiguredStrategy: the request-side retry loop waits through Options.IdleStrategy, so the
// interval set by SetPollInterval (or any Idler) is what bounds the retry latency.
func TestProxyOfferIdlesWithConfiguredStrategy(t *testing.T) {
	lb, err := logbuffer.NewTestingLogbuffer()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logbuffer.RemoveTestingLogbufferFile()) })
	pub := aeron.NewTestingPublication(lb, 4096, 0) // limit 0: every Offer is back pressured

	idler := &countingIdler{}
	opts := DefaultOptions()
	opts.IdleStrategy = idler
	opts.Timeout = 5 * time.Millisecond
	proxy := &Proxy{Publication: pub, archive: &Archive{Options: opts}}

	res := proxy.Offer(atomic.MakeBuffer(make([]byte, 16)), 0, 16, nil)
	assert.Less(t, res, int64(0))
	assert.Greater(t, idler.calls, 0)
}
