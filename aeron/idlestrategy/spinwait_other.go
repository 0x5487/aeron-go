//go:build !amd64 && !arm64

package idlestrategy

// spinWait is the spin-wait hint of the architectures without one: an empty function, so BusySpin still returns at once.
func spinWait() {
}
