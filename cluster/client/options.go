package client

import (
	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"go.uber.org/zap/zapcore"
)

// rangeCheck is passed to every generated Encode and Decode in this package. The cluster protocol is always range
// checked, so a value outside its schema range is an error.
const rangeCheck = true

type Options struct {
	Loglevel           zapcore.Level // [runtime] via logging.SetLevel()
	IngressEndpoints   string
	IngressChannel     string
	IngressStreamId    int32
	EgressChannel      string
	EgressStreamId     int32
	IdleStrategy       idlestrategy.Idler
	IsIngressExclusive bool
}

func NewOptions() *Options {
	return &Options{
		Loglevel:        zapcore.WarnLevel,
		IngressStreamId: 101,
		EgressChannel:   "aeron:udp?alias=cluster-egress|endpoint=localhost:0",
		EgressStreamId:  102,
		IdleStrategy:    idlestrategy.NewDefaultBackoffIdleStrategy(),
	}
}
