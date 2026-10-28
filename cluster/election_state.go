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

// electionStateCounter is the election state counter ElectionState found last: its id and its registration id. A value is
// never changed after it is stored, so several goroutines may read the same one (see ClusteredServiceAgent.electionCounter).
type electionStateCounter struct {
	id             int32
	registrationId int64
}

// ElectionState returns the election state of this cluster's consensus module. It reads the counter of type
// ELECTION_STATE_TYPE_ID whose key begins with this member's cluster id (ClusterCounters.allocate writes the cluster id
// first in the key, so a counter of another cluster on the same media driver is not taken).
//
// The id and the registration id of the counter found are kept, so a call does not walk the media driver's counters
// (their number grows with every connection, publication and image). Before each read the kept id is checked, in constant
// time and without allocating: the record is allocated, its type is ELECTION_STATE_TYPE_ID, the cluster id in its key is
// this member's and its registration id is the one kept. The registration id is what tells a counter from a later one
// that the driver allocated at the same id after freeing it. A kept id that fails the check is dropped and the counter is
// looked up again (and the one found is kept), so a counter the consensus module re-creates (a restart) is read and a
// stale or foreign one never is. The one remaining window is the driver freeing and reusing the record between the check
// and the read of the value; Aeron's own readers have it too, and the next call sees the change.
//
// ok is false when there is no such counter (yet): the state is unknown. Unknown is not ElectionStateClosed, so a caller
// that answers only in CLOSED must not answer on it.
//
// Several goroutines may call it at the same time: the kept counter is one atomic pointer to a value that is never
// changed, so a caller reads a whole id and registration id, never a mix. The trading-poc query gate reads it from the
// service goroutine and its bridge reads it from its own goroutine (cloud-perf 108).
func (agent *ClusteredServiceAgent) ElectionState() (ElectionState, bool) {
	kept := agent.electionCounter.Load()
	if kept == nil || !agent.keptElectionCounterIsValid(kept) {
		id := agent.counters.FindCounter(electionStateCounterTypeId, func(keyBuffer *atomic.Buffer) bool {
			return keyBuffer.GetInt32(0) == agent.opts.ClusterId
		})
		if id == counters.NullCounterId {
			agent.electionCounter.Store(nil)
			return 0, false
		}
		kept = &electionStateCounter{id: id, registrationId: agent.counters.GetCounterRegistrationId(id)}
		agent.electionCounter.Store(kept)
	}
	return ElectionState(agent.counters.GetCounterValue(kept.id)), true
}

// keptElectionCounterIsValid reports whether the kept id still names this member's election state counter.
func (agent *ClusteredServiceAgent) keptElectionCounterIsValid(kept *electionStateCounter) bool {
	if !agent.counters.IsCounterAllocated(kept.id) ||
		agent.counters.GetCounterTypeId(kept.id) != electionStateCounterTypeId ||
		agent.counters.GetCounterRegistrationId(kept.id) != kept.registrationId {
		return false
	}
	clusterId, err := agent.counters.GetKeyPartInt32(kept.id, 0)
	return err == nil && clusterId == agent.opts.ClusterId
}
