//go:build amd64

package idlestrategy

// spinWait is one CPU spin-wait hint: the PAUSE instruction (spinwait_amd64.s).
func spinWait()
