#include "textflag.h"

// func spinWait()
// One CPU spin-wait hint: PAUSE. The Go runtime's own procyield emits the same instruction; this is the hint Intel
// recommends inside a spin loop, and Java's Thread.onSpinWait compiles to it.
TEXT ·spinWait(SB), NOSPLIT|NOFRAME, $0-0
	PAUSE
	RET
