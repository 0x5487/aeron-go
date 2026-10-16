package aeron

import (
	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/lirm/aeron-go/aeron/logbuffer"
)

// NewTestingPublication wraps a log buffer made by logbuffer.NewTestingLogbuffer in a Publication that is never
// attached to a driver, with its position limit set to limit. It lets tests in other packages (cluster, archive)
// drive Offer without a media driver.
func NewTestingPublication(lb *logbuffer.LogBuffers, mtu int32, limit int64) *Publication {
	lb.Meta().MTULen.Set(mtu)
	pub := NewPublication(lb)
	pub.channel = "aeron:ipc"
	pub.pubLimit = NewPosition(atomic.MakeBuffer(make([]byte, 256)), 0)
	pub.pubLimit.set(limit)
	return pub
}
