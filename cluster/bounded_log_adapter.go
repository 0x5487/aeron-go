// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

const (
	beginFrag    uint8 = 0x80
	endFrag      uint8 = 0x40
	unfragmented uint8 = 0x80 | 0x40
)

// The manual reads of the timer event and the session close event follow their generated block layouts; these are the
// shortest bodies those reads need. A shorter body would read outside the fragment, so it stops the service first.
const (
	timerEventBlockLength        = 24
	sessionCloseEventBlockLength = 28
)

type boundedLogAdapter struct {
	marshaller     *codecs.SbeGoMarshaller
	options        *Options
	agent          *ClusteredServiceAgent
	image          aeron.Image
	builder        *bytes.Buffer
	maxLogPosition int64
	// The manually decoded events reuse these structs for their range checks, so a message does not allocate.
	timerEvent           codecs.TimerEvent
	sessionCloseEvent    codecs.SessionCloseEvent
	sessionMessageHeader codecs.SessionMessageHeader
	// fragmentHandler is adapter.onFragment, bound once in newBoundedLogAdapter. Passing the method value to
	// BoundedPoll on every round allocates a closure per round.
	fragmentHandler term.FragmentHandler
}

func newBoundedLogAdapter(options *Options) *boundedLogAdapter {
	adapter := &boundedLogAdapter{marshaller: codecs.NewSbeGoMarshaller(), options: options}
	adapter.fragmentHandler = adapter.onFragment
	return adapter
}

func (adapter *boundedLogAdapter) isDone() bool {
	return adapter.image.Position() >= adapter.maxLogPosition ||
		adapter.image.IsEndOfStream() ||
		adapter.image.IsClosed()
}

func (adapter *boundedLogAdapter) poll(limitPos int64) int {
	return adapter.image.BoundedPoll(adapter.fragmentHandler, limitPos, adapter.options.LogFragmentLimit)
}

func (adapter *boundedLogAdapter) onFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) {
	flags := header.Flags()
	if (flags & unfragmented) == unfragmented {
		adapter.onMessage(buffer, offset, length, header)
	} else if (flags & beginFrag) == beginFrag {
		if adapter.builder == nil {
			adapter.builder = &bytes.Buffer{}
		}
		adapter.builder.Reset()
		buffer.WriteBytes(adapter.builder, offset, length)
	} else if adapter.builder != nil && adapter.builder.Len() != 0 {
		buffer.WriteBytes(adapter.builder, offset, length)
		if (flags & endFrag) == endFrag {
			msgLength := adapter.builder.Len()
			adapter.onMessage(
				atomic.MakeBuffer(adapter.builder.Bytes(), msgLength),
				int32(0),
				int32(msgLength),
				header)
			adapter.builder.Reset()
		}
	}
}

func (adapter *boundedLogAdapter) onMessage(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) {
	entryOffset, entryLength := offset, length
	if length < SBEHeaderLength {
		stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, 0, NullValue,
			fmt.Errorf("message is %d bytes, shorter than the SBE header", length)))
		return
	}
	blockLength := buffer.GetUInt16(offset)
	templateId := buffer.GetUInt16(offset + 2)
	schemaId := buffer.GetUInt16(offset + 4)
	version := buffer.GetUInt16(offset + 6)
	offset += SBEHeaderLength
	length -= SBEHeaderLength
	leadershipTermId := leadershipTermIdFrom(buffer, offset, length)
	if schemaId != ClusterSchemaId {
		stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
			leadershipTermId, fmt.Errorf("unexpected schemaId=%d, want %d", schemaId, ClusterSchemaId)))
		return
	}

	switch templateId {
	case timerEventTemplateId:
		if length < timerEventBlockLength {
			stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
				leadershipTermId, fmt.Errorf("timer event body is %d bytes, shorter than %d",
					length, timerEventBlockLength)))
			return
		}
		event := &adapter.timerEvent
		event.LeadershipTermId = buffer.GetInt64(offset)
		event.CorrelationId = buffer.GetInt64(offset + 8)
		event.Timestamp = buffer.GetInt64(offset + 16)
		if err := event.RangeCheck(version, event.SbeSchemaVersion()); err != nil {
			stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
				leadershipTermId, err))
			return
		}
		adapter.agent.onTimerEvent(header.Position(), event.CorrelationId, event.Timestamp)
	case sessionOpenTemplateId:
		event := &codecs.SessionOpenEvent{}
		if err := event.Decode(
			adapter.marshaller,
			toByteBuffer(buffer, offset, length),
			version,
			blockLength,
			rangeCheck,
		); err != nil {
			stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
				leadershipTermId, err))
			return
		}

		err := adapter.agent.onSessionOpen(
			event.LeadershipTermId,
			header.Position(),
			event.ClusterSessionId,
			event.Timestamp,
			event.ResponseStreamId,
			string(event.ResponseChannel),
			event.EncodedPrincipal,
		)
		if err != nil {
			panic("boundedLogAdapter: session open error: " + err.Error())
		}
	case sessionCloseTemplateId:
		if length < sessionCloseEventBlockLength {
			stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
				leadershipTermId, fmt.Errorf("session close body is %d bytes, shorter than %d",
					length, sessionCloseEventBlockLength)))
			return
		}
		event := &adapter.sessionCloseEvent
		event.LeadershipTermId = buffer.GetInt64(offset)
		event.ClusterSessionId = buffer.GetInt64(offset + 8)
		event.Timestamp = buffer.GetInt64(offset + 16)
		event.CloseReason = codecs.CloseReasonEnum(buffer.GetInt32(offset + 24))
		if err := event.RangeCheck(version, event.SbeSchemaVersion()); err != nil {
			stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
				leadershipTermId, err))
			return
		}
		adapter.agent.onSessionClose(
			event.LeadershipTermId,
			header.Position(),
			event.ClusterSessionId,
			event.Timestamp,
			event.CloseReason,
		)
	case clusterActionReqTemplateId:
		e := codecs.ClusterActionRequest{}
		buf := toByteBuffer(buffer, offset, length)
		if err := e.Decode(adapter.marshaller, buf, version, blockLength, rangeCheck); err != nil {
			stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
				leadershipTermId, err))
			return
		}
		adapter.agent.onServiceAction(e.LeadershipTermId, e.LogPosition, e.Timestamp, e.Action)
	case newLeadershipTermTemplateId:
		e := codecs.NewLeadershipTermEvent{}
		buf := toByteBuffer(buffer, offset, length)
		if err := e.Decode(adapter.marshaller, buf, version, blockLength, rangeCheck); err != nil {
			stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
				leadershipTermId, err))
			return
		}
		adapter.agent.onNewLeadershipTermEvent(e.LeadershipTermId, e.LogPosition, e.Timestamp,
			e.TermBaseLogPosition, e.LeaderMemberId, e.LogSessionId, e.TimeUnit, e.AppVersion)
	case membershipChangeTemplateId:
		e := codecs.MembershipChangeEvent{}
		buf := toByteBuffer(buffer, offset, length)
		if err := e.Decode(adapter.marshaller, buf, version, blockLength, rangeCheck); err != nil {
			stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
				leadershipTermId, err))
			return
		}
		adapter.agent.onMembershipChange(e.LogPosition, e.Timestamp, e.ChangeType, e.MemberId)
	case SessionMessageHeaderTemplateId:
		if length < SessionMessageHeaderLength {
			stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
				leadershipTermId, fmt.Errorf("session message body is %d bytes, shorter than %d",
					length, SessionMessageHeaderLength)))
			return
		}
		event := &adapter.sessionMessageHeader
		event.LeadershipTermId = buffer.GetInt64(offset)
		event.ClusterSessionId = buffer.GetInt64(offset + 8)
		event.Timestamp = buffer.GetInt64(offset + 16)
		if err := event.RangeCheck(version, event.SbeSchemaVersion()); err != nil {
			stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
				leadershipTermId, err))
			return
		}
		adapter.agent.onSessionMessage(
			header.Position(),
			event.ClusterSessionId,
			event.Timestamp,
			buffer,
			offset+SessionMessageHeaderLength,
			length-SessionMessageHeaderLength,
			header,
		)
	default:
		stopOnUndecodableLogEntry(newUndecodableLogEntry(buffer, entryOffset, entryLength, header, templateId,
			leadershipTermId, fmt.Errorf("unexpected templateId=%d", templateId)))
	}
}

func toByteBuffer(buffer *atomic.Buffer, offset int32, length int32) *bytes.Buffer {
	buf := &bytes.Buffer{}
	buffer.WriteBytes(buf, offset, length)
	return buf
}

func (adapter *boundedLogAdapter) Close() error {
	var err error
	if adapter.image != nil {
		err = adapter.image.Close()
		adapter.image = nil
	}
	return err
}
