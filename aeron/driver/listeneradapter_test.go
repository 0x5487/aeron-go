package driver

import (
	"testing"

	"github.com/lirm/aeron-go/aeron/atomic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// onNextAvailableSessionID is broadcast to every client by Aeron drivers from 1.49.0 on (found by trading-poc
// experiment 01). The Java client silently ignores events it does not know.
const onNextAvailableSessionID int32 = 0x0F0D

// TestUnknownDriverEventIsLoggedAndIgnored feeds an event id the adapter does not know. The process must survive
// (the old code called Fatalf, which exits) and exactly one Warn line must be written.
func TestUnknownDriverEventIsLoggedAndIgnored(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	orig := logger.Logger()
	origSugar := logger.Sugar()
	t.Cleanup(func() { logger.SetLogger(orig); logger.SetSugar(origSugar) })
	l := zap.New(core)
	logger.SetLogger(l)
	logger.SetSugar(l.Sugar())

	adapter := NewAdapter(nil, nil)
	buf := atomic.MakeBuffer(make([]byte, 16))

	require.NotPanics(t, func() {
		adapter.handler()(onNextAvailableSessionID, buf, 0, 16)
	})

	warns := logs.FilterLevelExact(zapcore.WarnLevel).All()
	require.Len(t, warns, 1)
	assert.Contains(t, warns[0].Message, "unhandled driver event")
	assert.Contains(t, warns[0].Message, "3853") // 0x0F0D in decimal
}
