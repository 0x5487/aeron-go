package cluster

import (
	"time"

	"github.com/lirm/aeron-go/aeron/idlestrategy"
	"github.com/lirm/aeron-go/archive"
	"go.uber.org/zap/zapcore"
)

// rangeCheck is passed to every generated Encode and Decode in this package. The cluster protocol is always range
// checked, so a value outside its schema range is an error.
const rangeCheck = true

type Options struct {
	Timeout                 time.Duration      // [runtime] How long to try sending/receiving control messages
	IdleStrategy            idlestrategy.Idler // [runtime] Idlestrategy for sending/receiving control messagesA
	Loglevel                zapcore.Level      // [runtime] via logging.SetLevel()
	ClusterDir              string
	ClusterId               int32
	ServiceId               int32
	AppVersion              int32
	ControlChannel          string
	ConsensusModuleStreamId int32
	ServiceStreamId         int32
	SnapshotChannel         string
	SnapshotStreamId        int32
	ReplayChannel           string
	ReplayStreamId          int32
	ArchiveOptions          *archive.Options
	LogFragmentLimit        int
}

func NewOptions() *Options {
	archiveOpts := archive.DefaultOptions()
	archiveOpts.RequestChannel = "aeron:ipc?alias=cluster-service-archive-ctrl-req|term-length=128k"
	archiveOpts.ResponseChannel = "aeron:ipc?alias=cluster-service-archive-ctrl-resp|term-length=128k"
	return &Options{
		Timeout:                 time.Second * 5,
		IdleStrategy:            idlestrategy.NewDefaultBackoffIdleStrategy(),
		Loglevel:                zapcore.WarnLevel,
		ClusterDir:              "/tmp/aeron-cluster",
		ControlChannel:          "aeron:ipc?term-length=128k|alias=service-control",
		ConsensusModuleStreamId: 105,
		ServiceStreamId:         104,
		SnapshotChannel:         "aeron:ipc?alias=snapshot",
		SnapshotStreamId:        106,
		ReplayChannel:           "aeron:ipc?alias=service-replay",
		ReplayStreamId:          103,
		ArchiveOptions:          archiveOpts,
		LogFragmentLimit:        50,
	}
}
