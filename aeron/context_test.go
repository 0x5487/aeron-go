package aeron

import (
	"testing"
	"time"
)

func TestInterServiceTimeoutIsTheDriversUnlessSet(t *testing.T) {
	driver := 10 * time.Second
	if got := NewContext().interServiceTimeout(driver); got != driver {
		t.Fatalf("unset: got %v, want the driver's %v", got, driver)
	}
	if got := NewContext().InterServiceTimeout(2 * time.Second).interServiceTimeout(driver); got != 2*time.Second {
		t.Fatalf("set: got %v, want 2s", got)
	}
}
