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

// The archive encoders pass the range check: an id outside its schema range is an error, not bytes on the wire
// (ConnectRequestPacket).
func TestConnectRequestPacketRejectsOutOfRangeCorrelationId(t *testing.T) {
	_, err := ConnectRequestPacket(NewSbeGoMarshaller(), math.MinInt64, stream, channel)

	require.Error(t, err)
	assert.ErrorContains(t, err, "Range check failed on c.CorrelationId")
}
