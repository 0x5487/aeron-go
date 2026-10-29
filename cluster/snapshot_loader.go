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

type snapshotLoader struct {
	agent      *ClusteredServiceAgent
	img        aeron.Image
	marshaller *codecs.SbeGoMarshaller
	isDone     bool
	inSnapshot bool
	appVersion int32
	timeUnit   codecs.ClusterTimeUnitEnum
	// leadershipTermId is the term the snapshot was taken in, read from the last BEGIN marker. A client session does
	// not carry one, so an entry that does not decode is reported with this term.
	leadershipTermId int64
	err              error
	// fragmentHandler is loader.onFragment, bound once in newSnapshotLoader. Passing the method value to Poll on
	// every round allocates a closure per round.
	fragmentHandler term.FragmentHandler
}

func newSnapshotLoader(agent *ClusteredServiceAgent, img aeron.Image) *snapshotLoader {
	loader := &snapshotLoader{
		agent:            agent,
		img:              img,
		marshaller:       codecs.NewSbeGoMarshaller(),
		leadershipTermId: NullValue,
	}
	loader.fragmentHandler = loader.onFragment
	return loader
}

// undecodable records a fragment the loader cannot decode and ends the load. The caller that owns the starting
// service stops it with the entry's one Error line: a snapshot the service cannot decode is a log entry it cannot
// apply, and nothing after it can be trusted.
func (loader *snapshotLoader) undecodable(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
	templateId uint16,
	leadershipTermId int64,
	cause error,
) {
	loader.err = newUndecodableLogEntry(buffer, offset, length, header, templateId, leadershipTermId, cause)
	loader.isDone = true
}

func (loader *snapshotLoader) poll() int {
	if loader.isDone {
		return 0
	}
	return loader.img.Poll(loader.fragmentHandler, 1)
}

func (loader *snapshotLoader) onFragment(
	buffer *atomic.Buffer,
	offset int32,
	length int32,
	header *logbuffer.Header,
) {
	entryOffset, entryLength := offset, length
	if length < SBEHeaderLength {
		loader.undecodable(buffer, entryOffset, entryLength, header, 0, loader.leadershipTermId,
			fmt.Errorf("fragment is %d bytes, shorter than the SBE header", length))
		return
	}
	blockLength := buffer.GetUInt16(offset)
	templateId := buffer.GetUInt16(offset + 2)
	schemaId := buffer.GetUInt16(offset + 4)
	version := buffer.GetUInt16(offset + 6)
	if schemaId != ClusterSchemaId {
		loader.undecodable(buffer, entryOffset, entryLength, header, templateId, loader.leadershipTermId,
			fmt.Errorf("unexpected schemaId=%d, want %d", schemaId, ClusterSchemaId))
		return
	}
	offset += SBEHeaderLength
	length -= SBEHeaderLength
	if templateId == snapshotMarkerTemplateId && length >= 24 {
		loader.leadershipTermId = buffer.GetInt64(offset + 16)
	}

	buf := &bytes.Buffer{}
	buffer.WriteBytes(buf, offset, length)

	switch templateId {
	case snapshotMarkerTemplateId:
		marker := &codecs.SnapshotMarker{}
		if err := marker.Decode(loader.marshaller, buf, version, blockLength, rangeCheck); err != nil {
			loader.undecodable(buffer, entryOffset, entryLength, header, templateId, loader.leadershipTermId, err)
		} else if marker.TypeId != snapshotTypeId {
			loader.err = fmt.Errorf("unexpected snapshot type: %d", marker.TypeId)
			loader.isDone = true
		} else if marker.Mark == codecs.SnapshotMark.BEGIN {
			if loader.inSnapshot {
				loader.err = fmt.Errorf("already in snapshot, pos=%d", header.Position())
				loader.isDone = true
			} else {
				loader.inSnapshot = true
				loader.appVersion = marker.AppVersion
				loader.timeUnit = marker.TimeUnit
			}
		} else if marker.Mark == codecs.SnapshotMark.END {
			if !loader.inSnapshot {
				loader.err = fmt.Errorf("missing beging snapshot, pos=%d", header.Position())
				loader.isDone = true
			} else {
				loader.inSnapshot = false
				loader.isDone = true
			}
		} else {
			loader.err = fmt.Errorf("unexpected snapshot mark, pos=%d inSnapshot=%v mark=%v", header.Position(), loader.inSnapshot, marker.Mark)
			loader.isDone = true
		}
	case clientSessionTemplateId:
		s := codecs.ClientSession{}
		if err := s.Decode(loader.marshaller, buf, version, blockLength, rangeCheck); err != nil {
			loader.undecodable(buffer, entryOffset, entryLength, header, templateId, loader.leadershipTermId, err)
		} else {
			container, err := newContainerClientSession(
				s.ClusterSessionId, s.ResponseStreamId, string(s.ResponseChannel), s.EncodedPrincipal, loader.agent)
			if err == nil {
				loader.agent.addSessionFromSnapshot(container)
			} else {
				loader.err = err
				loader.isDone = true
			}
		}
	default:
		loader.undecodable(buffer, entryOffset, entryLength, header, templateId, loader.leadershipTermId,
			fmt.Errorf("unexpected templateId=%d", templateId))
	}
}
