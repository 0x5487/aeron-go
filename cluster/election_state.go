package cluster

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/counters"
)

// ElectionState is the code the consensus module writes into its election state counter; the codes are those of Aeron's
// io.aeron.cluster.ElectionState (INIT 0 through CLOSED 17), not of this package's Role.
type ElectionState int32

// ElectionStateClosed is ElectionState.CLOSED: the election is over, the member has joined the cluster, and its service
// has acknowledged the log position the election was decided at.
const ElectionStateClosed ElectionState = 17

// electionStateCounterTypeId is ConsensusModule.Configuration.ELECTION_STATE_TYPE_ID, the type id of the counter the
// consensus module of a cluster publishes its election state in (AeronCounters.CLUSTER_ELECTION_STATE_TYPE_ID).
const electionStateCounterTypeId = 207

// ElectionState returns the election state of this cluster's consensus module. It reads the counter of type
// ELECTION_STATE_TYPE_ID whose key begins with this member's cluster id (ClusterCounters.allocate writes the cluster id
// first in the key, so a counter of another cluster on the same media driver is not taken). The counter is looked up and
// read on every call: no value and no counter id is kept, so a counter the consensus module re-creates (a restart) is
// read rather than a stale one.
//
// ok is false when there is no such counter (yet): the state is unknown. Unknown is not ElectionStateClosed, so a caller
// that answers only in CLOSED must not answer on it.
func (agent *ClusteredServiceAgent) ElectionState() (ElectionState, bool) {
	id := agent.counters.FindCounter(electionStateCounterTypeId, func(keyBuffer *atomic.Buffer) bool {
		return keyBuffer.GetInt32(0) == agent.opts.ClusterId
	})
	if id == counters.NullCounterId {
		return 0, false
	}
	return ElectionState(agent.counters.GetCounterValue(id)), true
}
