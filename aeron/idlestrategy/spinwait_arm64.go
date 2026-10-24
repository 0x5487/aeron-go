//go:build arm64

package idlestrategy

// spinWait is one CPU spin-wait hint: the YIELD instruction (spinwait_arm64.s).
func spinWait()
