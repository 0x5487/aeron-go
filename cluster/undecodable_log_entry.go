package cluster

import (
	"fmt"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
)

// undecodableLogEntry is a cluster log entry, or a snapshot fragment, that the service cannot decode. It carries
// everything the one Error line needs, so the line is written where the service stops.
type undecodableLogEntry struct {
	templateId       uint16
	position         int64
	leadershipTermId int64
	payload          []byte
	cause            error
}

func (entry *undecodableLogEntry) Error() string {
	return fmt.Sprintf("undecodable cluster log entry: templateId=%d position=%d leadershipTermId=%d: %v",
		entry.templateId, entry.position, entry.leadershipTermId, entry.cause)
}

// newUndecodableLogEntry copies the raw entry so the Error line can print it as hex after the fragment handler stops.
// The copy happens only on this fatal path.
func newUndecodableLogEntry(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
	templateId uint16,
	leadershipTermId int64,
	cause error,
) *undecodableLogEntry {
	return &undecodableLogEntry{
		templateId:       templateId,
		position:         header.Position(),
		leadershipTermId: leadershipTermId,
		payload:          buffer.GetBytesArray(offset, length),
		cause:            cause,
	}
}

// leadershipTermIdFrom reads the leadership term id every event in the cluster log carries as its first body field.
// A fragment too short to hold the field reports the null value instead of reading outside it.
func leadershipTermIdFrom(buffer *atomic.Buffer, offset int32, length int32) int64 {
	if length < 8 {
		return NullValue
	}
	return buffer.GetInt64(offset)
}

// stopOnUndecodableLogEntry writes the ONE Error line that identifies the entry and stops the service the same way
// an unknown session does: it panics out of the poll loop. Nothing after a corrupt entry can be applied, so the
// service must not skip it and read on. The fields are always in this order: template id, log position, leadership
// term, cause, raw payload as hex.
func stopOnUndecodableLogEntry(entry *undecodableLogEntry) {
	logger.Errorf(
		"undecodable cluster log entry: templateId=%d position=%d leadershipTermId=%d error=%v payload=%x",
		entry.templateId,
		entry.position,
		entry.leadershipTermId,
		entry.cause,
		entry.payload,
	)
	panic(entry)
}
