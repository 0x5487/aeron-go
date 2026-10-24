#include "textflag.h"

// func spinWait()
// One CPU spin-wait hint: YIELD, the ARMv8 hint for a spin loop (what Java's Thread.onSpinWait compiles to).
TEXT ·spinWait(SB), NOSPLIT|NOFRAME, $0-0
	YIELD
	RET
