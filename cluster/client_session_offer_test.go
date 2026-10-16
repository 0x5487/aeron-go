package cluster

import (
	"testing"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newOfferTestSession(t *testing.T, role Role) (*containerClientSession, *aeron.Publication) {
	t.Helper()
	lb, err := logbuffer.NewTestingLogbuffer()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, logbuffer.RemoveTestingLogbufferFile()) })
	pub := aeron.NewTestingPublication(lb, 4096, 1<<30)
	agent := &ClusteredServiceAgent{
		role:                role,
		sessionMsgHdrBuffer: codecs.MakeClusterMessageBuffer(SessionMessageHeaderTemplateId, SessionMessageHdrBlockLength),
	}
	return &containerClientSession{id: 7, agent: agent, response: pub}, pub
}

// TestContainerClientSessionOffer_Oversize: the largest payload that still fits (term length / 8 minus the 32 byte
// session header) is accepted, one more byte returns MessageTooLong without a panic. The leader's egress Offer used
// to panic and kill the leader (trading-poc experiment 04, P2).
func TestContainerClientSessionOffer_Oversize(t *testing.T) {
	s, pub := newOfferTestSession(t, Leader)
	hdr := int32(s.agent.sessionMsgHdrBuffer.Capacity())
	require.EqualValues(t, 32, hdr)
	maxPayload := pub.MaxMessageLength() - hdr

	buf := func(n int32) *atomic.Buffer { return atomic.MakeBuffer(make([]byte, n)) }

	var res int64
	require.NotPanics(t, func() { res = s.Offer(buf(maxPayload), 0, maxPayload, nil) })
	assert.Greater(t, res, int64(0))

	require.NotPanics(t, func() { res = s.Offer(buf(maxPayload+1), 0, maxPayload+1, nil) })
	assert.Equal(t, aeron.MessageTooLong, res)
	assert.NotEqual(t, aeron.BackPressured, res)
}

// TestContainerClientSessionOffer_FollowerIsMocked: followers never offer, whatever the size.
func TestContainerClientSessionOffer_FollowerIsMocked(t *testing.T) {
	s, pub := newOfferTestSession(t, Follower)
	n := pub.MaxMessageLength() * 2
	res := s.Offer(atomic.MakeBuffer(make([]byte, n)), 0, n, nil)
	assert.Equal(t, int64(ClientSessionMockedOffer), res)
}
