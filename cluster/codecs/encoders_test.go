package codecs

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The cluster encoders pass the range check: an id outside its schema range is an error, not bytes on the wire
// (ServiceAckRequestPacket).
func TestServiceAckRequestPacketRejectsOutOfRangeLogPosition(t *testing.T) {
	_, err := ServiceAckRequestPacket(NewSbeGoMarshaller(), math.MinInt64, 0, 0, 0, 0)

	require.Error(t, err)
	assert.ErrorContains(t, err, "Range check failed on s.LogPosition")
}
