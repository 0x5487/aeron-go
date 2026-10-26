package client

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster"
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

// The driver cannot make the publication of the new leader: the client waits for it as for one that is slow, and gives up
// when the wait ends, telling the listener once. The publication that is not there is never dereferenced (cloud-perf 40,
// review G1).
func TestANewLeaderWhosePublicationTheDriverCannotMake(t *testing.T) {
	rig := newConnectedRig(t, 0)
	rig.driver.failOn = endpointOf(1)

	rig.deliver(newLeaderFrame(t, testSessionID, 2, 1, rig.ac.opts.IngressEndpoints))
	require.False(t, rig.ac.IsClosed())

	require.NotPanics(t, func() { rig.ac.Poll() }, "the publication that could not be made is not dereferenced")
	assert.False(t, rig.ac.IsClosed(), "still inside the wait")
	assert.Empty(t, rig.rec.errors)

	rig.clock.advance(newLeaderConnectTimeout)
	require.NotPanics(t, func() { rig.ac.Poll() })
	assert.True(t, rig.ac.IsClosed())
	require.Len(t, rig.rec.errors, 1, "the listener is told once")
	assert.Contains(t, rig.rec.errors[0], "new leader")
}

// The cluster closes the session in the egress of the poll that also finds the new leader's publication connected: the
// client stays closed, it does not connect through the publication it was about to use (cloud-perf 40, review G2a).
func TestASessionClosedWhileAwaitingTheNewLeaderIsNotRevived(t *testing.T) {
	rig := newConnectedRig(t, 0)
	rig.deliver(newLeaderFrame(t, testSessionID, 2, 1, rig.ac.opts.IngressEndpoints))
	rig.driver.openOn(t, endpointOf(1)).connected = true
	rig.queue(sessionEventFrame(t, codecs.SessionEvent{
		ClusterSessionId: testSessionID, Code: codecs.EventCode.CLOSED, Detail: []byte("terminated"),
	}))

	rig.ac.Poll()

	assert.True(t, rig.ac.IsClosed(), "the session the cluster closed does not come back")
	assert.False(t, rig.ac.IsConnected())
	assert.Equal(t, []string{"terminated"}, rig.rec.disconnects)
	assert.Equal(t, aeron.NotConnected, rig.offer())
}

// A reply of the cluster is read by Poll while the client waits for the new leader's publication (cloud-perf 40, review
// G2b).
func TestAReplyIsReadWhileAwaitingTheNewLeader(t *testing.T) {
	rig := newConnectedRig(t, 0)
	rig.deliver(newLeaderFrame(t, testSessionID, 2, 1, rig.ac.opts.IngressEndpoints))
	rec := &messageRecorder{}
	rig.ac.egressListener = &delegatingListener{listenerRecorder: rig.rec, messages: rec}
	rig.queue(sessionMessage(testSessionID, "reply"))

	rig.ac.Poll()

	assert.Equal(t, []string{"reply"}, rec.payloads)
}

// The message offered after NEW_LEADER carries the new leadership term in its session header, at the offset the cluster
// reads (cloud-perf 40, review G4).
func TestTheSessionHeaderCarriesTheNewTerm(t *testing.T) {
	rig := newConnectedRig(t, 0)
	before := rig.driver.openOn(t, endpointOf(0))
	require.Positive(t, rig.offer())
	assert.EqualValues(t, 1, headerInt64(t, before, 0), "the term of the session before the change")

	rig.moveLeader(t, 2, 1)
	after := rig.driver.openOn(t, endpointOf(1))
	require.Positive(t, rig.offer())

	assert.EqualValues(t, 2, headerInt64(t, after, 0), "the term the new leader announced")
	assert.EqualValues(t, testSessionID, headerInt64(t, after, 8), "the session is the same")
}

// headerInt64 is the Int64 at offset of the session header of the last message offered on pub: the header the cluster
// reads, not the payload.
func headerInt64(t *testing.T, pub *fakePublication, offset int) int64 {
	t.Helper()
	require.NotEmpty(t, pub.sessionHeaders, "no message was offered on %s", pub.endpoint())
	hdr := pub.sessionHeaders[len(pub.sessionHeaders)-1]
	return int64(binary.LittleEndian.Uint64(hdr[int(cluster.SBEHeaderLength)+offset:]))
}

// A NEW_LEADER for another session is ignored: the leader, the endpoints and the state stay, and the listener is not told
// (cloud-perf 40, review G5).
func TestANewLeaderForAnotherSessionIsIgnored(t *testing.T) {
	rig := newConnectedRig(t, 0)
	moved := endpointsOf("m0:20110", "m1-new:30000", "m2:20110")

	rig.deliver(newLeaderFrame(t, testSessionID+1, 2, 1, moved))

	assert.Equal(t, int32(0), rig.ac.LeaderMemberId())
	assert.EqualValues(t, 1, rig.ac.LeadershipTermId())
	assert.Equal(t, clientConnected, rig.ac.state)
	assert.Empty(t, rig.rec.newLeaders)
	require.Len(t, rig.driver.open(), 1)
	assert.Equal(t, endpointOf(0), rig.driver.open()[0].endpoint())
	assert.Positive(t, rig.offer(), "still offered to the same leader")
}

// The client's own publication (its IngressEndpoints named no member) is closed when a new leader is announced: it does
// not belong to the member table, and a closed publication never connects (cloud-perf 40, review G9).
func TestANewLeaderClosesTheIngressPublicationOfAClientWithoutMembers(t *testing.T) {
	rig := newConnectedRig(t, 0)
	own, ok := rig.ac.ingressPub.(*fakePublication)
	require.True(t, ok)
	rig.ac.memberByIdMap = make(map[int32]*memberIngress) // the endpoints name no member
	rig.ac.opts.IngressEndpoints = "not-a-member"         // but they are not empty: the client reads them again

	rig.deliver(newLeaderFrame(t, testSessionID, 2, 1, rig.ac.opts.IngressEndpoints))

	assert.True(t, own.closed, "the publication of the client is closed, no member owns it")
	assert.Nil(t, rig.ac.ingressPub)
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

// The first fragment of a message from the old leader is not joined with the last fragment that comes after NEW_LEADER:
// the client drops the fragments it kept when the leader changed, so the half message is never delivered, and the next whole
// message of the new leader is (cloud-perf 40, review N5). Two arrangements reach the drop: the endpoints name the new leader
// (updateMemberEndpoints drops them), and a client without member endpoints (only onNewLeaderEvent does).
func TestAFragmentOfTheOldLeaderIsNotJoinedAcrossALeaderChange(t *testing.T) {
	const cut = int(cluster.SBEHeaderLength) + int(cluster.SessionMessageHeaderLength) + 5
	tests := []struct {
		name    string
		arrange func(rig *clientRig)
	}{
		{name: "the endpoints name the new leader", arrange: func(*clientRig) {}},
		{name: "the client has no member endpoints", arrange: func(rig *clientRig) {
			rig.ac.opts.IngressEndpoints = ""
			rig.ac.memberByIdMap = make(map[int32]*memberIngress)
		}},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rig := newConnectedRig(t, 0)
			endpoints := rig.ac.opts.IngressEndpoints
			tt.arrange(rig)
			rec := &messageRecorder{}
			rig.ac.egressListener = &delegatingListener{listenerRecorder: rig.rec, messages: rec}
			whole := sessionMessage(testSessionID, "old leader's message")
			raw := whole.GetBytesArray(0, whole.Capacity())
			head := atomic.MakeBuffer(raw[:cut])
			tail := atomic.MakeBuffer(raw[cut:])

			rig.queueFragment(head, frameBegin)
			rig.ac.Poll()
			require.Empty(t, rec.payloads, "the message is not complete yet")

			rig.queueFragment(newLeaderFrame(t, testSessionID, 2, 1, endpoints), frameUnfragmented)
			rig.ac.Poll()
			require.Equal(t, []int32{1}, rig.rec.newLeaders, "the new leader is announced")

			rig.queueFragment(tail, frameEnd)
			rig.ac.Poll()
			assert.Empty(t, rec.payloads, "the end of the old leader's message is not joined with its beginning")

			rig.queue(sessionMessage(testSessionID, "new leader's message"))
			rig.ac.Poll()
			assert.Equal(t, []string{"new leader's message"}, rec.payloads, "a whole message after the change is delivered")
			assert.Empty(t, rig.rec.errors)
		})
	}
}
