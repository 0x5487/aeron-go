package client

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/cluster"
)

// messageRecorder is an EgressListener that records session messages. Every other method panics (nil embedded
// interface).
type messageRecorder struct {
	EgressListener
	payloads []string
}

func (r *messageRecorder) OnMessage(
	_ *AeronCluster,
	_ int64,
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	_ *logbuffer.Header,
) {
	r.payloads = append(r.payloads, string(buffer.GetBytesArray(offset, length)))
}

// sessionMessage builds a cluster session message from the leader for clusterSessionID carrying payload.
func sessionMessage(clusterSessionID int64, payload string) *atomic.Buffer {
	const bodyAt = cluster.SBEHeaderLength
	msg := make([]byte, bodyAt+cluster.SessionMessageHeaderLength+len(payload))
	binary.LittleEndian.PutUint16(msg[0:], cluster.SessionMessageHdrBlockLength)
	binary.LittleEndian.PutUint16(msg[2:], cluster.SessionMessageHeaderTemplateId)
	binary.LittleEndian.PutUint16(msg[4:], cluster.ClusterSchemaId)
	binary.LittleEndian.PutUint16(msg[6:], 0)
	binary.LittleEndian.PutUint64(msg[bodyAt:], 1)                          // leadershipTermId
	binary.LittleEndian.PutUint64(msg[bodyAt+8:], uint64(clusterSessionID)) // clusterSessionId
	binary.LittleEndian.PutUint64(msg[bodyAt+16:], 1000)                    // timestamp
	copy(msg[bodyAt+cluster.SessionMessageHeaderLength:], payload)
	return atomic.MakeBuffer(msg)
}

func newPollTestClient(t *testing.T, listener EgressListener) *AeronCluster {
	t.Helper()
	ac := &AeronCluster{
		egressSub:        aeron.NewSubscription(nil, "aeron:ipc", 1, 1, 1, nil, nil),
		egressListener:   listener,
		state:            clientConnected,
		clusterSessionId: 7,
	}
	ac.bindEgressHandler()
	return ac
}

func TestAeronClusterPollEgressAllocatesNothingWhenIdle(t *testing.T) {
	ac := newPollTestClient(t, &messageRecorder{})

	allocs := testing.AllocsPerRun(1000, func() { ac.pollEgress(10) })
	assert.Equal(t, float64(0), allocs)
}

func TestAeronClusterEgressHandlerDeliversSessionMessage(t *testing.T) {
	rec := &messageRecorder{}
	ac := newPollTestClient(t, rec)

	msg := sessionMessage(7, "hello")
	frame := atomic.MakeBuffer(make([]byte, logbuffer.DataFrameHeader.Length))
	logbuffer.FrameFlags(frame, 0, 0xC0) // unfragmented
	ac.egressHandler(msg, 0, msg.Capacity(), new(logbuffer.Header).Wrap(frame.Ptr(), frame.Capacity()))

	assert.Equal(t, []string{"hello"}, rec.payloads)
}
