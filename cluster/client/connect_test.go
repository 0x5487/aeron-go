package client

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/lirm/aeron-go/cluster"
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

// A redirect to a member whose endpoint changed while its publication from the connect phase is still open: the
// publication of the address the member left is closed and one is made on the new address (cloud-perf 40, review G3).
func TestRedirectToAMemberWhoseEndpointChanged(t *testing.T) {
	rig := newConnectingRig(t)
	rig.ac.Poll() // the publications of the three members
	oldOnTwo := rig.driver.openOn(t, endpointOf(2))
	rig.driver.openOn(t, endpointOf(0)).connected = true
	rig.ac.ingressPub = rig.driver.openOn(t, endpointOf(0))
	rig.ac.state = clientAwaitConnectReply
	rig.ac.correlationId = 5
	rig.ac.awaitTimeoutTime = rig.clock.Now().UnixMilli() + 3_000
	moved := endpointsOf("m0:20110", "m1:20110", "m2-new:30000")

	rig.deliver(sessionEventFrame(t, codecs.SessionEvent{
		CorrelationId: 5, LeaderMemberId: 2, Code: codecs.EventCode.REDIRECT, Detail: []byte(moved),
	}))

	assert.True(t, oldOnTwo.closed, "the publication of the address the member left is closed")
	assert.Equal(t, "m2-new:30000", rig.driver.openOn(t, "m2-new:30000").endpoint())
	require.Len(t, rig.driver.open(), 1, "only the new leader's publication is open")
	assert.NotSame(t, oldOnTwo, rig.ac.ingressPub)
	require.NotNil(t, rig.ac.ingressPub)
	assert.False(t, rig.ac.ingressPub.IsClosed())
}

// A client whose IngressEndpoints are empty makes its one publication on the ingress channel and connects (cloud-perf 40,
// review G6).
func TestAClientWithoutIngressEndpointsConnects(t *testing.T) {
	rig := newConnectingRig(t)
	rig.ac.opts.IngressEndpoints = ""
	rig.ac.memberByIdMap = make(map[int32]*memberIngress) // no endpoints, no members to know
	rig.ac.ingressPub = nil
	rig.driver.added = nil

	rig.ac.Poll() // the publication on the ingress channel
	require.Equal(t, clientAwaitPublicationConnected, rig.ac.state)
	require.Len(t, rig.driver.added, 1)
	rig.driver.added[0].connected = true

	rig.ac.Poll() // the connect request goes out
	require.Equal(t, clientAwaitConnectReply, rig.ac.state)
	rig.deliver(sessionEventFrame(t, codecs.SessionEvent{
		CorrelationId: rig.ac.correlationId, ClusterSessionId: 11, LeadershipTermId: 3, LeaderMemberId: 0,
		Code: codecs.EventCode.OK,
	}))

	assert.True(t, rig.ac.IsConnected(), "the client is connected")
	assert.EqualValues(t, 11, rig.ac.ClusterSessionId())
	assert.Equal(t, 1, rig.rec.connects)
}

// Both deadlines of the connect are the last instant the client still waits: exactly at them it has not given up, one
// millisecond later it has (cloud-perf 40, review G7).
func TestTheConnectDeadlinesAreInclusive(t *testing.T) {
	t.Run("waiting for the ingress publication", func(t *testing.T) {
		rig := newConnectingRig(t)
		rig.ac.Poll() // the publications; the client waits five seconds for one of them

		rig.clock.advance(5 * time.Second) // exactly the deadline
		rig.ac.Poll()
		require.False(t, rig.ac.IsClosed(), "exactly at the deadline the client still waits")
		assert.Empty(t, rig.rec.errors)

		rig.clock.advance(time.Millisecond)
		rig.ac.Poll()
		assert.True(t, rig.ac.IsClosed(), "one tick after the deadline it has given up")
	})

	t.Run("waiting for the connect reply", func(t *testing.T) {
		rig := newConnectingRig(t)
		rig.ac.Poll()
		rig.driver.openOn(t, endpointOf(0)).connected = true
		rig.ac.ingressPub = rig.driver.openOn(t, endpointOf(0))
		rig.ac.state = clientAwaitConnectReply
		rig.ac.correlationId = 5
		rig.ac.awaitTimeoutTime = rig.clock.Now().UnixMilli() + 3_000 // the three seconds the reply is waited

		rig.clock.advance(3 * time.Second) // exactly the deadline
		rig.ac.Poll()
		require.False(t, rig.ac.IsClosed(), "exactly at the deadline the client still waits")
		assert.Empty(t, rig.rec.errors)

		rig.clock.advance(time.Millisecond)
		rig.ac.Poll()
		assert.True(t, rig.ac.IsClosed(), "one tick after the deadline it has given up")
	})
}

// An ERROR for another connect is not this connect's: the client keeps waiting and connects on its own reply
// (cloud-perf 40, review G8).
func TestAnErrorForAnotherConnectIsIgnored(t *testing.T) {
	rig := newConnectingRig(t)
	rig.ac.Poll()
	rig.driver.openOn(t, endpointOf(0)).connected = true
	rig.ac.ingressPub = rig.driver.openOn(t, endpointOf(0))
	rig.ac.state = clientAwaitConnectReply
	rig.ac.correlationId = 5
	rig.ac.awaitTimeoutTime = rig.clock.Now().UnixMilli() + 3_000

	rig.deliver(sessionEventFrame(t, codecs.SessionEvent{
		CorrelationId: 6, Code: codecs.EventCode.ERROR, Detail: []byte("another connect"),
	}))

	assert.False(t, rig.ac.IsClosed(), "another connect's error is not this one's")
	assert.Equal(t, clientAwaitConnectReply, rig.ac.state)
	assert.Empty(t, rig.rec.errors)

	rig.deliver(sessionEventFrame(t, codecs.SessionEvent{
		CorrelationId: 5, ClusterSessionId: 11, LeadershipTermId: 3, LeaderMemberId: 0, Code: codecs.EventCode.OK,
	}))

	assert.True(t, rig.ac.IsConnected(), "the client connects on its own reply")
	assert.Equal(t, 1, rig.rec.connects)
}

// While the client awaits the connect reply, one Poll reads one frame: the frame after the reply waits for the next Poll
// (cloud-perf 40, review N3).
func TestTheConnectReplyPollReadsOneFrame(t *testing.T) {
	const correlationID = 5
	rig := newConnectingRig(t)
	rig.ac.Poll() // the publications of the three members
	rig.driver.openOn(t, endpointOf(0)).connected = true
	rig.ac.ingressPub = rig.driver.openOn(t, endpointOf(0))
	rig.ac.state = clientAwaitConnectReply
	rig.ac.correlationId = correlationID
	rig.ac.awaitTimeoutTime = rig.clock.Now().UnixMilli() + 3_000
	rec := &messageRecorder{}
	rig.ac.egressListener = &delegatingListener{listenerRecorder: rig.rec, messages: rec}
	rig.queue(
		sessionEventFrame(t, codecs.SessionEvent{
			CorrelationId: correlationID, ClusterSessionId: 11, LeadershipTermId: 3, Code: codecs.EventCode.OK,
		}),
		sessionMessage(11, "after the reply"),
	)

	rig.ac.Poll()
	require.True(t, rig.ac.IsConnected())
	require.Equal(t, 1, rig.rec.connects)
	assert.Empty(t, rec.payloads, "the frame after the reply waits for the next Poll")

	rig.ac.Poll()
	assert.Equal(t, []string{"after the reply"}, rec.payloads)
}

// The connect request on the wire carries the correlation id the client matches the reply to: the id the driver gave it is
// in the bytes, and the reply that carries those bytes' id connects (cloud-perf 40, review N1).
func TestTheConnectRequestCarriesTheCorrelationIdTheReplyMatches(t *testing.T) {
	const driverID int64 = 0x5EED
	rig := newConnectingRig(t)
	rig.ac.nextCorrelationId = func() int64 { return driverID }
	rig.ac.Poll() // the publications of the three members
	rig.driver.openOn(t, endpointOf(0)).connected = true
	rig.ac.Poll() // the connect request goes out on the connected publication
	require.Equal(t, clientAwaitConnectReply, rig.ac.state)

	pub := rig.driver.openOn(t, endpointOf(0))
	require.Len(t, pub.offers, 1, "one connect request")
	wire := pub.offers[0]
	wireID := int64(binary.LittleEndian.Uint64(wire[cluster.SBEHeaderLength:]))
	require.Equal(t, driverID, wireID, "the request carries the id the driver gave the client")

	rig.deliver(sessionEventFrame(t, codecs.SessionEvent{
		CorrelationId: wireID, ClusterSessionId: 11, LeadershipTermId: 3, Code: codecs.EventCode.OK,
	}))

	assert.True(t, rig.ac.IsConnected(), "the reply to the request connects")
	assert.Equal(t, 1, rig.rec.connects)
}
