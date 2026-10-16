package aeron

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCloseAllResourcesDoesNotRaceWithAdminCalls closes the conductor's resources while another goroutine keeps
// registering publications (every registration touches cc.pubs under adminLock). Run with -race: before the fix
// closeAllResources read and cleared cc.pubs / cc.subs / cc.counters without the lock (trading-poc experiment 07).
func TestCloseAllResourcesDoesNotRaceWithAdminCalls(t *testing.T) {
	driverProxy := new(MockDriverProxy)
	driverProxy.On("NextCorrelationID").Return(int64(123))
	driverProxy.On("AddPublication", Channel, StreamId1).Return(CorrelationId1, nil)

	// Built by hand instead of Init: Init wraps the cnc metadata with unsafe arithmetic that trips -race's checkptr.
	cc := &ClientConductor{
		driverProxy: driverProxy,
		pubs:        make([]*publicationStateDefn, 0),
		subs:        make([]*subscriptionStateDefn, 0),
		counters:    make(map[int64]*counterStateDefn),
	}
	cc.running.Set(true)
	cc.driverActive.Set(true)

	// Enough entries that the close loop is in flight while the adder appends.
	for i := 0; i < 2000; i++ {
		_, err := cc.AddPublication(Channel, StreamId1)
		require.NoError(t, err)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 2000; i++ {
			_, _ = cc.AddPublication(Channel, StreamId1)
		}
	}()

	close(start)
	cc.closeAllResources(time.Now().UnixNano())
	wg.Wait()
}
