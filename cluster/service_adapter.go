package cluster

import (
	"bytes"
	"fmt"

	"github.com/lirm/aeron-go/aeron"
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
	"github.com/lirm/aeron-go/aeron/logbuffer/term"
	"github.com/lirm/aeron-go/cluster/codecs"
)

type serviceAdapter struct {
	marshaller   *codecs.SbeGoMarshaller
	agent        *ClusteredServiceAgent
	subscription *aeron.Subscription
	// fragmentHandler is adapter.onFragment, bound once in newServiceAdapter. Passing the method value to Poll on
	// every round allocates a closure per round.
	fragmentHandler term.FragmentHandler
}

func newServiceAdapter(subscription *aeron.Subscription) *serviceAdapter {
	adapter := &serviceAdapter{
		marshaller:   codecs.NewSbeGoMarshaller(),
		subscription: subscription,
	}
	adapter.fragmentHandler = adapter.onFragment
	return adapter
}

func (adapter *serviceAdapter) poll() int {
	if adapter.subscription.IsClosed() {
		panic("subscription closed")
	}
	return adapter.subscription.Poll(adapter.fragmentHandler, 10)
}

func (adapter *serviceAdapter) onFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) {
	entryOffset, entryLength := offset, length
	if length < SBEHeaderLength {
		stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, 0,
			adapter.agent.leadershipTermId(),
			fmt.Errorf("message is %d bytes, shorter than the SBE header", length)))
		return
	}
	blockLength := buffer.GetUInt16(offset)
	templateId := buffer.GetUInt16(offset + 2)
	schemaId := buffer.GetUInt16(offset + 4)
	version := buffer.GetUInt16(offset + 6)
	if schemaId != ClusterSchemaId {
		stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
			adapter.agent.leadershipTermId(),
			fmt.Errorf("unexpected schemaId=%d, want %d", schemaId, ClusterSchemaId)))
		return
	}
	offset += SBEHeaderLength
	length -= SBEHeaderLength

	switch templateId {
	case joinLogTemplateId:
		buf := &bytes.Buffer{}
		buffer.WriteBytes(buf, offset, length)
		joinLog := &codecs.JoinLog{}
		if err := joinLog.Decode(adapter.marshaller, buf, version, blockLength, rangeCheck); err != nil {
			stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
				adapter.agent.leadershipTermId(), err))
			return
		}
		adapter.agent.onJoinLog(
			joinLog.LogPosition,
			joinLog.MaxLogPosition,
			joinLog.MemberId,
			joinLog.LogSessionId,
			joinLog.LogStreamId,
			joinLog.IsStartup == codecs.BooleanType.TRUE,
			Role(joinLog.Role),
			string(joinLog.LogChannel),
		)
	case serviceTerminationPosTemplateId:
		logPos := buffer.GetInt64(offset)
		adapter.agent.onServiceTerminationPosition(logPos)
	default:
		logger.Debugf("serviceAdapter: unexpected templateId=%d at pos=%d", templateId, header.Position())
	}
}
