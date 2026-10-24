package idlestrategy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// backoffRoundsWithoutPark is how many idle rounds a backoff of maxSpins spins and maxYields yields makes before it
// enters the parking state: the first round enters the spinning state (counting one spin), the next maxSpins rounds
// spin, and the maxYields+1 rounds after them yield, the last of which moves the strategy into the parking state. None
// of them waits; the round after them is the first park.
func backoffRoundsWithoutPark(maxSpins, maxYields int) int {
	return 1 + maxSpins + (maxYields + 1)
}

// TestBackoffPhaseTransitions: the backoff spins, then yields, then parks, in that order. Only the parking state waits,
// so the rounds up to the one that enters it return well inside a park, the first park waits the shortest park, and the
// next one waits twice as long (the park grows to the longest).
func TestBackoffPhaseTransitions(t *testing.T) {
	t.Parallel()
	const (
		spins   = 2
		yields  = 2
		minPark = 200 * time.Millisecond
		maxPark = 4 * minPark
	)
	s := NewBackoffIdleStrategy(spins, yields, int64(minPark), int64(maxPark))

	returnWithin(t, func() { s.Idle(0) }, backoffRoundsWithoutPark(spins, yields), minPark/2)

	start := time.Now()
	s.Idle(0)
	require.GreaterOrEqual(t, time.Since(start), minPark, "the first park waits the shortest park")

	start = time.Now()
	s.Idle(0)
	require.GreaterOrEqual(t, time.Since(start), 2*minPark, "the park doubles on every round up to the longest")
}

// TestBackoffWorkCountResetsThePhases: a round with work puts the backoff back into the spinning state, so the rounds
// after it spin and yield again before they park.
func TestBackoffWorkCountResetsThePhases(t *testing.T) {
	t.Parallel()
	const (
		spins  = 2
		yields = 2
		park   = 100 * time.Millisecond
	)
	s := NewBackoffIdleStrategy(spins, yields, int64(park), int64(park))
	for i := 0; i < backoffRoundsWithoutPark(spins, yields); i++ {
		s.Idle(0)
	}
	s.Idle(1)

	returnWithin(t, func() { s.Idle(0) }, backoffRoundsWithoutPark(spins, yields), park/2)
}
