package codecs

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every cluster encoder that returns an error passes the range check: an id outside its schema range is an error and
// no bytes are returned, so nothing out of range reaches the wire. Each row passes one out-of-range argument to one
// helper; that helper's encode error must reach the caller. MakeClusterMessageBuffer has no encode and no error
// return, so it has no row.
func TestClusterEncodersRejectOutOfRangeArguments(t *testing.T) {
	const lowest = math.MinInt64
	cases := []struct {
		name string
		call func(m *SbeGoMarshaller) ([]byte, error)
	}{
		{"ServiceAckRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return ServiceAckRequestPacket(m, lowest, 0, 0, 0, 0)
		}},
		{"SnapshotMarkerPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return SnapshotMarkerPacket(m, lowest, 1, 2, 3, SnapshotMark.BEGIN, ClusterTimeUnit.MILLIS, 0)
		}},
		{"ClientSessionPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return ClientSessionPacket(m, lowest, 1, nil, nil)
		}},
		{"CloseSessionRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return CloseSessionRequestPacket(m, lowest)
		}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.call(NewSbeGoMarshaller())

			require.Error(t, err)
			assert.ErrorContains(t, err, "Range check failed")
			assert.Nil(t, got, "a packet that failed to encode returns no bytes")
		})
	}
}
