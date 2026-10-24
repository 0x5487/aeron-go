package idlestrategy

import (
	"testing"
	"time"
)

// idleRounds is how many idle rounds the strategies that must not park are called for. A strategy that parks spends at
// least one park per round once its spins and yields are used up, so it cannot finish this many rounds inside bound.
const idleRounds = 1_000_000

// returnWithin runs rounds calls of idle on a goroutine of its own and fails when they have not all returned within
// bound. A strategy that parks cannot finish: even its shortest park is longer than the bound.
func returnWithin(t *testing.T, idle func(), rounds int, bound time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < rounds; i++ {
			idle()
		}
	}()
	select {
	case <-done:
	case <-time.After(bound):
		t.Fatalf("%d idle rounds did not return within %s: the strategy parks", rounds, bound)
	}
}

// TestNoOpIdleDoesNotPark: NoOp is Java's NoOpIdleStrategy: an idle round returns at once, so the caller spins through
// as many rounds as it is given without ever sleeping.
func TestNoOpIdleDoesNotPark(t *testing.T) {
	t.Parallel()
	returnWithin(t, func() { NoOp{}.Idle(0) }, idleRounds, time.Second)
}

// TestBusySpinIdleDoesNotPark: BusySpin is Java's BusySpinIdleStrategy: every idle round executes one CPU spin-wait
// hint and returns. It must not park either.
func TestBusySpinIdleDoesNotPark(t *testing.T) {
	t.Parallel()
	returnWithin(t, func() { BusySpin{}.Idle(0) }, idleRounds, time.Second)
}

// TestSpinWaitReturns: the hint is one instruction; a call returns at once. It exists on every architecture, so this
// test (and the package) builds with GOARCH=arm64 as well.
func TestSpinWaitReturns(t *testing.T) {
	t.Parallel()
	returnWithin(t, spinWait, 1, time.Second)
}
