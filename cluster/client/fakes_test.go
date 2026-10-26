package client

import (
	"bytes"
	"testing"
	"time"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/cluster"
	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/require"
)

// fakePublication stands for the ingress publication the media driver would give the client. It is connected only when a
// test says so, and refuses every offer once closed, as *aeron.Publication does.
type fakePublication struct {
	channel   string
	connected bool
	closed    bool
}

func (p *fakePublication) IsConnected() bool { return p.connected && !p.closed }
func (p *fakePublication) IsClosed() bool    { return p.closed }
func (p *fakePublication) Channel() string   { return p.channel }
func (p *fakePublication) Close() error      { p.closed = true; return nil }

// endpoint is the endpoint of the channel the client created the publication with.
func (p *fakePublication) endpoint() string {
	uri, err := aeron.ParseChannelUri(p.channel)
	if err != nil {
		return ""
	}
	return uri.Get("endpoint")
}

func (p *fakePublication) result() int64 {
	switch {
	case p.closed:
		return aeron.PublicationClosed
	case !p.connected:
		return aeron.NotConnected
	default:
		return 1000
	}
}

func (p *fakePublication) Offer(*atomic.Buffer, int32, int32, term.ReservedValueSupplier) int64 {
	return p.result()
}

func (p *fakePublication) Offer2(
	*atomic.Buffer, int32, int32, *atomic.Buffer, int32, int32, term.ReservedValueSupplier,
) int64 {
	return p.result()
}

// fakeDriver records every publication the client adds, in order.
type fakeDriver struct {
	added []*fakePublication
}

func (d *fakeDriver) add(channel string) (ingressPublication, error) {
	p := &fakePublication{channel: channel}
	d.added = append(d.added, p)
	return p, nil
}

// open lists the publications that are not closed.
func (d *fakeDriver) open() []*fakePublication {
	var out []*fakePublication
	for _, p := range d.added {
		if !p.closed {
			out = append(out, p)
		}
	}
	return out
}

// openOn is the one open publication on endpoint; the test fails when there is not exactly one.
func (d *fakeDriver) openOn(t *testing.T, endpoint string) *fakePublication {
	t.Helper()
	var found []*fakePublication
	for _, p := range d.open() {
		if p.endpoint() == endpoint {
			found = append(found, p)
		}
	}
	require.Lenf(t, found, 1, "open publications on %s", endpoint)
	return found[0]
}

// fakeEgress is an egress subscription with nothing to read, whose channel is bound.
type fakeEgress struct{ closed bool }

func (*fakeEgress) Poll(term.FragmentHandler, int) int { return 0 }
func (*fakeEgress) TryResolveChannelEndpointPort() string {
	return "aeron:udp?endpoint=localhost:40456"
}
func (e *fakeEgress) Close() error { e.closed = true; return nil }

// fakeClock is the time source of the client's timeouts.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

// listenerRecorder records what the client tells its listener.
type listenerRecorder struct {
	connects    int
	disconnects []string
	errors      []string
	newLeaders  []int32
}

func (r *listenerRecorder) OnConnect(*AeronCluster) { r.connects++ }
func (r *listenerRecorder) OnDisconnect(_ *AeronCluster, d string) {
	r.disconnects = append(r.disconnects, d)
}
func (r *listenerRecorder) OnError(_ *AeronCluster, details string) {
	r.errors = append(r.errors, details)
}
func (r *listenerRecorder) OnNewLeader(_ *AeronCluster, _ int64, m int32) {
	r.newLeaders = append(r.newLeaders, m)
}
func (r *listenerRecorder) OnMessage(*AeronCluster, int64, *atomic.Buffer, int32, int32, *logbuffer.Header) {
}

const testSessionID int64 = 7

// clientRig is a client of a three member cluster, with every seam to the driver replaced.
type clientRig struct {
	ac     *AeronCluster
	driver *fakeDriver
	clock  *fakeClock
	rec    *listenerRecorder
}

// newConnectedRig builds a client that has a session with the cluster and whose leader is member leader, as it is
// after the connect reply (the publications of the other members are closed then).
func newConnectedRig(t *testing.T, leader int32) *clientRig {
	t.Helper()
	endpoints := endpointsOf("m0:20110", "m1:20110", "m2:20110")
	channel, err := aeron.ParseChannelUri("aeron:udp?term-length=64k")
	require.NoError(t, err)
	rig := &clientRig{driver: &fakeDriver{}, clock: &fakeClock{now: time.Unix(1_000, 0)}, rec: &listenerRecorder{}}
	rig.ac = &AeronCluster{
		opts:                &Options{IngressEndpoints: endpoints, IngressStreamId: 101},
		ingressChannel:      &channel,
		clusterSessionId:    testSessionID,
		leadershipTermId:    1,
		leaderMemberId:      leader,
		memberByIdMap:       make(map[int32]*memberIngress),
		egressListener:      rig.rec,
		egressSub:           &fakeEgress{},
		sessionMsgHdrBuffer: codecs.MakeClusterMessageBuffer(cluster.SessionMessageHeaderTemplateId, cluster.SessionMessageHdrBlockLength),
		keepAliveBuffer:     codecs.MakeClusterMessageBuffer(cluster.SessionKeepAliveTemplateId, 16),
		addPublication:      rig.driver.add,
		clock:               rig.clock.Now,
	}
	rig.ac.bindEgressHandler()
	rig.ac.updateMemberEndpoints(endpoints)
	rig.driver.openOn(t, endpointOf(leader)).connected = true
	rig.ac.state = clientConnected
	return rig
}

func endpointsOf(addrs ...string) string {
	var b bytes.Buffer
	for i, a := range addrs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(string(rune('0'+i)) + "=" + a)
	}
	return b.String()
}

func endpointOf(member int32) string { return "m" + string(rune('0'+member)) + ":20110" }

func newLeaderFrame(t *testing.T, session, term int64, leader int32, endpoints string) *atomic.Buffer {
	t.Helper()
	e := codecs.NewLeaderEvent{
		LeadershipTermId: term, ClusterSessionId: session, LeaderMemberId: leader, IngressEndpoints: []byte(endpoints),
	}
	header := codecs.MessageHeader{
		BlockLength: e.SbeBlockLength(), TemplateId: e.SbeTemplateId(), SchemaId: e.SbeSchemaId(), Version: e.SbeSchemaVersion(),
	}
	var w bytes.Buffer
	require.NoError(t, header.Encode(marshaller, &w))
	require.NoError(t, e.Encode(marshaller, &w, true))
	return atomic.MakeBuffer(w.Bytes())
}

func sessionEventFrame(t *testing.T, e codecs.SessionEvent) *atomic.Buffer {
	t.Helper()
	header := codecs.MessageHeader{
		BlockLength: e.SbeBlockLength(), TemplateId: e.SbeTemplateId(), SchemaId: e.SbeSchemaId(), Version: e.SbeSchemaVersion(),
	}
	var w bytes.Buffer
	require.NoError(t, header.Encode(marshaller, &w))
	require.NoError(t, e.Encode(marshaller, &w, true))
	return atomic.MakeBuffer(w.Bytes())
}

// deliver hands an egress message to the client as the fragment assembler would.
func (r *clientRig) deliver(buf *atomic.Buffer) {
	r.ac.onFragment(buf, 0, buf.Capacity(), nil)
}

// offer offers one message the way the application does.
func (r *clientRig) offer() int64 {
	payload := atomic.MakeBuffer(make([]byte, 16))
	return r.ac.Offer(payload, 0, 16)
}

// newConnectingRig builds a client that has not connected yet, as NewAeronCluster leaves it: no leader is known and no
// member has a publication.
func newConnectingRig(t *testing.T) *clientRig {
	t.Helper()
	rig := newConnectedRig(t, 0)
	rig.ac.memberByIdMap = make(map[int32]*memberIngress)
	rig.ac.ingressPub = nil
	rig.ac.leaderMemberId = cluster.NullValue
	rig.ac.clusterSessionId = cluster.NullValue
	rig.driver.added = nil
	rig.ac.state = clientCreatePublications
	rig.ac.updateMemberEndpoints(rig.ac.opts.IngressEndpoints)
	return rig
}
