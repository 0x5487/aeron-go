/*
Copyright 2016 Stanislav Liberman

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package idlestrategy

import (
	"runtime"
	"time"
)

type Idler interface {
	Idle(fragmentsRead int)
}

// NoOp is the Java NoOpIdleStrategy: an idle round returns at once, doing nothing at all.
type NoOp struct {
}

func (s NoOp) Idle(fragmentsRead int) {
}

// BusySpin is the Java BusySpinIdleStrategy: an idle round executes one CPU spin-wait hint (a PAUSE on amd64, a YIELD
// on arm64) and returns, leaving the goroutine on the CPU. Unlike NoOp it tells the CPU that the goroutine is spinning.
type BusySpin struct {
}

func (s BusySpin) Idle(fragmentsRead int) {
	spinWait()
}

type Sleeping struct {
	SleepFor time.Duration
}

func (s Sleeping) Idle(fragmentsRead int) {
	if fragmentsRead == 0 {
		time.Sleep(s.SleepFor)
	}
}

type Yielding struct {
}

func (s Yielding) Idle(fragmentsRead int) {
	if fragmentsRead == 0 {
		runtime.Gosched()
	}
}
