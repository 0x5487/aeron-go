package client

import (
	"testing"
	"time"

	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The client ends itself when the connect does not finish, tells the listener why, and does not try again by itself: the
// owner decides when to build a new client (cloud-perf 40, item 3).
func TestConnectFailureClosesTheClientAndReportsIt(t *testing.T) {
	const correlationID = 5
	connectReply := func(t *testing.T, rig *clientRig) {
		t.Helper()
		rig.ac.Poll() // creates the publications of the three members
		rig.driver.openOn(t, endpointOf(0)).connected = true
		rig.ac.ingressPub = rig.driver.openOn(t, endpointOf(0))
		rig.ac.state = clientAwaitConnectReply
		rig.ac.correlationId = correlationID
		rig.ac.awaitTimeoutTime = rig.clock.Now().UnixMilli() + 3_000
	}
	tests := []struct {
		name    string
		arrange func(t *testing.T, rig *clientRig)
		act     func(t *testing.T, rig *clientRig)
		want    string
	}{
		{
			name:    "no ingress publication connects",
			arrange: func(_ *testing.T, rig *clientRig) { rig.ac.Poll() },
			act:     func(_ *testing.T, rig *clientRig) { rig.clock.advance(5*time.Second + time.Millisecond); rig.ac.Poll() },
			want:    "timed out waiting for connected publication",
		},
		{
			name:    "the connect reply does not come",
			arrange: connectReply,
			act:     func(_ *testing.T, rig *clientRig) { rig.clock.advance(3*time.Second + time.Millisecond); rig.ac.Poll() },
			want:    "timed out waiting for session connect reply",
		},
		{
			name:    "the publication disconnects before the connect reply",
			arrange: connectReply,
			act: func(t *testing.T, rig *clientRig) {
				rig.driver.openOn(t, endpointOf(0)).connected = false
				rig.ac.Poll()
			},
			want: "timed out waiting for session connect reply",
		},
		{
			name:    "the cluster answers ERROR",
			arrange: connectReply,
			act: func(t *testing.T, rig *clientRig) {
				rig.deliver(sessionEventFrame(t, codecs.SessionEvent{
					CorrelationId: correlationID, Code: codecs.EventCode.ERROR, Detail: []byte("max sessions"),
				}))
			},
			want: "max sessions",
		},
		{
			name:    "the cluster rejects the authentication",
			arrange: connectReply,
			act: func(t *testing.T, rig *clientRig) {
				rig.deliver(sessionEventFrame(t, codecs.SessionEvent{
					CorrelationId: correlationID, Code: codecs.EventCode.AUTHENTICATION_REJECTED, Detail: []byte("bad key"),
				}))
			},
			want: "authentication rejected (bad key)",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rig := newConnectingRig(t)
			tt.arrange(t, rig)
			require.False(t, rig.ac.IsClosed())
			require.Empty(t, rig.rec.errors)

			tt.act(t, rig)

			assert.True(t, rig.ac.IsClosed())
			assert.Equal(t, []string{tt.want}, rig.rec.errors)

			added := len(rig.driver.added)
			rig.clock.advance(time.Hour)
			rig.ac.Poll()
			assert.True(t, rig.ac.IsClosed(), "it does not start again by itself")
			assert.Len(t, rig.driver.added, added, "and adds no publication")
			assert.Equal(t, []string{tt.want}, rig.rec.errors, "the listener is told once")
		})
	}
}

// A publication of a member that something else closed is made again when the client creates its publications (the
// connect starts over after a disconnect).
func TestCreatePublicationsMakesAgainAClosedOne(t *testing.T) {
	rig := newConnectingRig(t)
	rig.ac.Poll()
	require.Len(t, rig.driver.open(), 3)
	closed := rig.driver.openOn(t, endpointOf(1))
	require.NoError(t, closed.Close())

	rig.ac.state = clientCreatePublications
	rig.ac.Poll()

	assert.NotSame(t, closed, rig.driver.openOn(t, endpointOf(1)))
	assert.Len(t, rig.driver.open(), 3)
}

// A redirect to a member whose publication was closed makes it again, the client does not use the closed one.
func TestRedirectToAMemberWithAClosedPublication(t *testing.T) {
	rig := newConnectingRig(t)
	rig.ac.Poll()
	closed := rig.driver.openOn(t, endpointOf(2))
	require.NoError(t, closed.Close())
	rig.ac.state = clientAwaitConnectReply
	rig.ac.correlationId = 9
	rig.ac.awaitTimeoutTime = rig.clock.Now().UnixMilli() + 3_000
	rig.ac.ingressPub = rig.driver.openOn(t, endpointOf(0))

	rig.deliver(sessionEventFrame(t, codecs.SessionEvent{
		CorrelationId: 9, LeaderMemberId: 2, Code: codecs.EventCode.REDIRECT, Detail: []byte(rig.ac.opts.IngressEndpoints),
	}))

	assert.NotSame(t, closed, rig.ac.ingressPub)
	assert.False(t, rig.ac.ingressPub.IsClosed())
	assert.Equal(t, endpointOf(2), rig.ac.ingressPub.(*fakePublication).endpoint())
}
