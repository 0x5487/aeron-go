package client

import (
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// moveLeader tells the client the cluster elected leader in term and lets the new leader's ingress publication connect,
// as the driver does a moment after the client adds it.
func (r *clientRig) moveLeader(t *testing.T, term int64, leader int32) {
	t.Helper()
	r.deliver(newLeaderFrame(t, testSessionID, term, leader, r.ac.opts.IngressEndpoints))
	r.driver.openOn(t, endpointOf(leader)).connected = true
	r.ac.Poll()
}

// After the leader went A -> B -> A, the client offers on a publication of A that is open, without a rebuild by the
// caller (cloud-perf 40, rare path 1).
func TestLeaderReturnsToAMemberItLeftBefore(t *testing.T) {
	rig := newConnectedRig(t, 0)

	rig.moveLeader(t, 2, 1)
	require.True(t, rig.ac.IsConnected())
	require.Positive(t, rig.offer(), "offers go to B")

	rig.moveLeader(t, 3, 0)

	assert.True(t, rig.ac.IsConnected(), "the client is connected to A again")
	assert.Positive(t, rig.offer(), "the offer is taken: the publication of A is open")
	assert.Equal(t, []int32{1, 0}, rig.rec.newLeaders)
	assert.Empty(t, rig.rec.errors)
	assert.Len(t, rig.driver.open(), 1, "only the leader's publication is open")
}

// A new leader's publication that connects late is not a disconnect: the client waits for it and carries on with the
// same session when it connects (cloud-perf 40, rare path 6).
func TestNewLeaderPublicationConnectsLate(t *testing.T) {
	rig := newConnectedRig(t, 0)

	rig.deliver(newLeaderFrame(t, testSessionID, 2, 1, rig.ac.opts.IngressEndpoints))
	rig.ac.Poll()
	assert.False(t, rig.ac.IsConnected(), "the new leader's publication is not connected yet")
	assert.False(t, rig.ac.IsClosed())
	assert.Equal(t, aeron.NotConnected, rig.offer())

	rig.clock.advance(newLeaderConnectTimeout - time.Millisecond)
	rig.ac.Poll()
	assert.False(t, rig.ac.IsClosed(), "still inside the wait")
	assert.Empty(t, rig.rec.errors)

	rig.driver.openOn(t, endpointOf(1)).connected = true
	rig.ac.Poll()
	assert.True(t, rig.ac.IsConnected(), "connected when the publication connects")
	assert.Positive(t, rig.offer())
	assert.Empty(t, rig.rec.disconnects, "the session was never reported as lost")
	assert.Empty(t, rig.rec.errors)
	assert.Equal(t, 0, rig.rec.connects, "the session is the same one: OnConnect is not called again")
	assert.Equal(t, []int32{1}, rig.rec.newLeaders)
}

// A new leader's publication that does not connect within the wait ends the client, and the caller is told why
// (cloud-perf 40, rare path 6).
func TestNewLeaderPublicationNeverConnects(t *testing.T) {
	rig := newConnectedRig(t, 0)
	rig.deliver(newLeaderFrame(t, testSessionID, 2, 1, rig.ac.opts.IngressEndpoints))
	added := len(rig.driver.added)

	rig.clock.advance(newLeaderConnectTimeout)
	rig.ac.Poll()

	assert.True(t, rig.ac.IsClosed())
	require.Len(t, rig.rec.errors, 1)
	assert.Contains(t, rig.rec.errors[0], "new leader")

	rig.clock.advance(time.Hour)
	rig.ac.Poll()
	assert.Len(t, rig.rec.errors, 1, "reported once")
	assert.Len(t, rig.driver.added, added, "the client does not try again by itself")
	assert.True(t, rig.ac.IsClosed())
}

// Leaders A -> B -> C -> B: B's first publication was closed when C took over, so B gets a new one (rare path 2).
func TestLeaderReturnsAfterTwoOthersLed(t *testing.T) {
	rig := newConnectedRig(t, 0)

	rig.moveLeader(t, 2, 1)
	rig.moveLeader(t, 3, 2)
	rig.moveLeader(t, 4, 1)

	assert.True(t, rig.ac.IsConnected())
	assert.Positive(t, rig.offer())
	assert.Equal(t, []int32{1, 2, 1}, rig.rec.newLeaders)
	assert.Empty(t, rig.rec.errors)
	assert.Len(t, rig.driver.open(), 1)
	assert.Equal(t, endpointOf(1), rig.driver.open()[0].endpoint())
}

// A member that never led becomes leader: the publication is made for it, as before (rare path 4).
func TestMemberThatNeverLedBecomesLeader(t *testing.T) {
	rig := newConnectedRig(t, 0)
	require.Len(t, rig.driver.added, 1, "only the leader has a publication after the connect")

	rig.moveLeader(t, 2, 2)

	assert.True(t, rig.ac.IsConnected())
	assert.Positive(t, rig.offer())
	assert.Equal(t, []int32{2}, rig.rec.newLeaders)
	assert.Len(t, rig.driver.added, 2)
	assert.True(t, rig.driver.added[0].closed, "the old leader's publication is closed")
	assert.Equal(t, endpointOf(2), rig.driver.open()[0].endpoint())
}

// The new leader's endpoint is not the one the client knew: the old publication of that member is closed and a new one
// is made on the endpoint the cluster announced (rare path 3).
func TestLeaderEndpointChangesOnLeaderChange(t *testing.T) {
	rig := newConnectedRig(t, 0)
	rig.moveLeader(t, 2, 1)
	oldOnB := rig.driver.added[len(rig.driver.added)-1]
	require.Equal(t, endpointOf(1), oldOnB.endpoint())

	moved := endpointsOf("m0:20110", "m1-new:30000", "m2:20110")
	rig.deliver(newLeaderFrame(t, testSessionID, 3, 0, moved)) // A leads, and B's address changed
	rig.driver.openOn(t, endpointOf(0)).connected = true
	rig.ac.Poll()
	rig.deliver(newLeaderFrame(t, testSessionID, 4, 1, moved)) // B leads at its new address
	rig.driver.openOn(t, "m1-new:30000").connected = true
	rig.ac.Poll()

	assert.True(t, oldOnB.closed)
	assert.True(t, rig.ac.IsConnected())
	assert.Positive(t, rig.offer())
	assert.Len(t, rig.driver.open(), 1)
}

// A new leader announced while the client still waits for the previous one's publication: the newest leader wins.
func TestNewLeaderWhileAwaitingTheLastOne(t *testing.T) {
	rig := newConnectedRig(t, 0)

	rig.deliver(newLeaderFrame(t, testSessionID, 2, 1, rig.ac.opts.IngressEndpoints))
	rig.deliver(newLeaderFrame(t, testSessionID, 3, 2, rig.ac.opts.IngressEndpoints))
	rig.driver.openOn(t, endpointOf(2)).connected = true
	rig.ac.Poll()

	assert.True(t, rig.ac.IsConnected())
	assert.Positive(t, rig.offer())
	assert.Equal(t, []int32{1, 2}, rig.rec.newLeaders)
	assert.Len(t, rig.driver.open(), 1)
}

// While the client waits for the new leader's publication, a reply of the cluster still reaches the application, and the
// cluster closing the session ends the client.
func TestEgressWhileAwaitingTheNewLeader(t *testing.T) {
	rig := newConnectedRig(t, 0)
	rig.deliver(newLeaderFrame(t, testSessionID, 2, 1, rig.ac.opts.IngressEndpoints))

	rec := &messageRecorder{}
	rig.ac.egressListener = &delegatingListener{listenerRecorder: rig.rec, messages: rec}
	rig.deliver(sessionMessage(testSessionID, "reply"))
	assert.Equal(t, []string{"reply"}, rec.payloads)

	rig.deliver(sessionEventFrame(t, codecs.SessionEvent{
		ClusterSessionId: testSessionID, Code: codecs.EventCode.CLOSED, Detail: []byte("terminated"),
	}))
	assert.True(t, rig.ac.IsClosed())
	assert.Equal(t, []string{"terminated"}, rig.rec.disconnects)
}

type delegatingListener struct {
	*listenerRecorder
	messages *messageRecorder
}

func (d *delegatingListener) OnMessage(
	ac *AeronCluster, ts int64, buf *atomic.Buffer, off, length int32, h *logbuffer.Header,
) {
	d.messages.OnMessage(ac, ts, buf, off, length, h)
}
