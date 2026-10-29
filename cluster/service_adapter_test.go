package cluster

import (
	"math"
	"testing"

	"github.com/lirm/aeron-go/cluster/codecs"
	"github.com/stretchr/testify/assert"
)

// A JoinLog whose log position is outside its schema range is not handed to the agent
// (serviceAdapter.onFragment).
func TestServiceAdapterRejectsOutOfRangeJoinLog(t *testing.T) {
	agent := &ClusteredServiceAgent{logAdapter: newBoundedLogAdapter(NewOptions())}
	adapter := newServiceAdapter(nil)
	adapter.agent = agent
	event := codecs.JoinLog{
		LogPosition:    math.MinInt64,
		MaxLogPosition: 1,
		IsStartup:      codecs.BooleanType.TRUE,
		LogChannel:     []byte("aeron:ipc"),
	}
	msg := encodedClusterMessage(t, &event)

	adapter.fragmentHandler(msg, 0, msg.Capacity(), fragmentHeader(testFlagUnfragmented))

	assert.Nil(t, agent.activeLogEvent, "an out-of-schema join log is not applied")
	assert.Zero(t, agent.logAdapter.maxLogPosition)
}
