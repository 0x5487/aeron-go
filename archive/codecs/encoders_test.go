// Copyright (C) 2021-2022 Talos, Inc.
//
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

package codecs

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var channel = "aeron:udp?endpoint=localhost:20121"
var stream = int32(1001)

// Execute all the encoders as a sanity check
func TestEncoders(t *testing.T) {
	marshaller := NewSbeGoMarshaller()

	packet, err := ConnectRequestPacket(marshaller, 99, stream, channel)
	if err != nil {
		t.Log("ConnectRequestPacket() failed")
		t.Fail()
	}
	if len(packet) != 62 { // Look for unexepected change
		t.Logf("ConnectRequestPacket failed length check: %d", len(packet))
		t.Fail()
	}
	packet, err = StartRecordingRequest2Packet(marshaller, 1234, 5678, stream, true, true, channel)
	if err != nil {
		t.Log("StartRecordingRequestPacket() failed")
		t.Fail()
	}
	if len(packet) != 74 { // Look for unexpected change
		t.Logf("StartRecordingRequestPacket() failed length check: %d", len(packet))
		t.Fail()
	}
}

// Every archive request encoder passes the range check: an id outside its schema range is an error and no bytes are
// returned, so nothing out of range reaches the wire. Each row passes one out-of-range argument to one helper, so each
// helper's own encode error branch is exercised. StartRecordingRequestPacket and ExtendRecordingRequestPacket have no
// encode of their own; their rows reach the encode through the *2 helpers they delegate to.
func TestArchiveEncodersRejectOutOfRangeArguments(t *testing.T) {
	const lowest = math.MinInt64
	cases := []struct {
		name string
		call func(m *SbeGoMarshaller) ([]byte, error)
	}{
		{"ConnectRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return ConnectRequestPacket(m, lowest, stream, channel)
		}},
		{"CloseSessionRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return CloseSessionRequestPacket(m, lowest)
		}},
		{"StartRecordingRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return StartRecordingRequestPacket(m, lowest, 2, stream, false, channel)
		}},
		{"StartRecordingRequest2Packet", func(m *SbeGoMarshaller) ([]byte, error) {
			return StartRecordingRequest2Packet(m, lowest, 2, stream, false, false, channel)
		}},
		{"StopRecordingRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return StopRecordingRequestPacket(m, lowest, 2, stream, channel)
		}},
		{"ReplayRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return ReplayRequestPacket(m, lowest, 2, 3, 4, 5, stream, channel)
		}},
		{"StopReplayRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return StopReplayRequestPacket(m, lowest, 2, 3)
		}},
		{"ListRecordingsRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return ListRecordingsRequestPacket(m, lowest, 2, 3, 4)
		}},
		{"ListRecordingsForUriRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return ListRecordingsForUriRequestPacket(m, lowest, 2, 3, 4, stream, channel)
		}},
		{"ListRecordingRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return ListRecordingRequestPacket(m, lowest, 2, 3)
		}},
		{"ExtendRecordingRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return ExtendRecordingRequestPacket(m, lowest, 2, 3, stream, SourceLocation.LOCAL, channel)
		}},
		{"ExtendRecordingRequest2Packet", func(m *SbeGoMarshaller) ([]byte, error) {
			return ExtendRecordingRequest2Packet(m, lowest, 2, 3, stream, SourceLocation.LOCAL, false, channel)
		}},
		{"RecordingPositionRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return RecordingPositionRequestPacket(m, lowest, 2, 3)
		}},
		{"TruncateRecordingRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return TruncateRecordingRequestPacket(m, lowest, 2, 3, 4)
		}},
		{"StopRecordingSubscriptionPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return StopRecordingSubscriptionPacket(m, lowest, 2, 3)
		}},
		{"StopRecordingByIdentityPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return StopRecordingByIdentityPacket(m, lowest, 2, 3)
		}},
		{"StopPositionPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return StopPositionPacket(m, lowest, 2, 3)
		}},
		{"FindLastMatchingRecordingPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return FindLastMatchingRecordingPacket(m, lowest, 2, 3, 4, stream, channel)
		}},
		{"ListRecordingSubscriptionsPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return ListRecordingSubscriptionsPacket(m, lowest, 2, 3, 4, false, stream, channel)
		}},
		{"BoundedReplayPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return BoundedReplayPacket(m, lowest, 2, 3, 4, 5, 6, stream, channel)
		}},
		{"StopAllReplaysPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return StopAllReplaysPacket(m, lowest, 2, 3)
		}},
		{"CatalogHeaderPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return CatalogHeaderPacket(m, math.MinInt32, 1, 2, 3)
		}},
		{"ReplicateRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return ReplicateRequestPacket(m, lowest, 2, 3, 4, stream, channel, channel)
		}},
		{"StopReplicationRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return StopReplicationRequestPacket(m, lowest, 2, 3)
		}},
		{"StartPositionRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return StartPositionRequestPacket(m, lowest, 2, 3)
		}},
		{"DetachSegmentsRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return DetachSegmentsRequestPacket(m, lowest, 2, 3, 4)
		}},
		{"DeleteDetachedSegmentsRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return DeleteDetachedSegmentsRequestPacket(m, lowest, 2, 3)
		}},
		{"PurgeSegmentsRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return PurgeSegmentsRequestPacket(m, lowest, 2, 3, 4)
		}},
		{"AttachSegmentsRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return AttachSegmentsRequestPacket(m, lowest, 2, 3)
		}},
		{"AuthConnectRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return AuthConnectRequestPacket(m, lowest, stream, channel, nil)
		}},
		{"ChallengeResponsePacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return ChallengeResponsePacket(m, lowest, 2, nil)
		}},
		{"MigrateSegmentsRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return MigrateSegmentsRequestPacket(m, lowest, 2, 3, 4)
		}},
		{"KeepAliveRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return KeepAliveRequestPacket(m, lowest, 2)
		}},
		{"TaggedReplicateRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return TaggedReplicateRequestPacket(m, lowest, 2, 3, 4, 5, 6, stream, channel, channel)
		}},
		{"ReplicateRequest2Packet", func(m *SbeGoMarshaller) ([]byte, error) {
			return ReplicateRequest2Packet(m, lowest, 2, 3, 4, 5, 6, stream, channel, channel, channel)
		}},
		{"PurgeRecordingRequestPacket", func(m *SbeGoMarshaller) ([]byte, error) {
			return PurgeRecordingRequestPacket(m, lowest, 2, 3)
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
