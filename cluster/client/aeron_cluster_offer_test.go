package client

import (
	"testing"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAeronClusterOffer_Oversize: payload at the limit (term length / 8 minus the 32 byte session header) is
// accepted, one more byte returns MessageTooLong without a panic (trading-poc experiment 04, P1).
func TestAeronClusterOffer_Oversize(t *testing.T) {
	lb, err := logbuffer.NewTestingLogbuffer()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logbuffer.RemoveTestingLogbufferFile()) })
	pub := aeron.NewTestingPublication(lb, 4096, 1<<30)

	ac := &AeronCluster{
		state:               clientConnected,
		ingressPub:          pub,
		sessionMsgHdrBuffer: codecs.MakeClusterMessageBuffer(cluster.SessionMessageHeaderTemplateId, cluster.SessionMessageHdrBlockLength),
	}
	hdr := int32(ac.sessionMsgHdrBuffer.Capacity())
	require.EqualValues(t, 32, hdr)
	maxPayload := pub.MaxMessageLength() - hdr
	buf := func(n int32) *atomic.Buffer { return atomic.MakeBuffer(make([]byte, n)) }

	var res int64
	require.NotPanics(t, func() { res = ac.Offer(buf(maxPayload), 0, maxPayload) })
	assert.Greater(t, res, int64(0))

	require.NotPanics(t, func() { res = ac.Offer(buf(maxPayload+1), 0, maxPayload+1) })
	assert.Equal(t, aeron.MessageTooLong, res)
	assert.NotEqual(t, aeron.BackPressured, res)
}
