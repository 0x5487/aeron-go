# Changelog

This is the trading-poc fork of [lirm/aeron-go](https://github.com/lirm/aeron-go), branch `trading-poc`. It lists the
changes the fork carries on top of upstream (last upstream merge: 8b05ad1, pull request #110). Every change records why
it was made and where it was found (trading-poc experiments under `docs/labs/aeron/`, tickets under `docs/tickets/`).

## Unreleased

### Fixed

- **driver**: unknown driver events are logged once at Warn and skipped instead of `Fatalf` (7371f05). Drivers from
  Aeron 1.49.0 on broadcast `ON_NEXT_AVAILABLE_SESSION_ID` (0x0F0D) to every client; the Java client ignores events it
  does not know, and the fork killed the process. Found in trading-poc experiment 01.
- **conductor**: `closeAllResources` reads and clears publications, subscriptions and counters under `adminLock`
  (1f1e528). Closing a client while the conductor was processing was a data race. Found in trading-poc experiment 07.
- **publication**: `Offer`, `Offer2` and `TryClaim` over the maximum message size return the new code
  `MessageTooLong` (-6) instead of panicking (7a08d71). A cluster leader replying with an oversized message died, and
  the retried command killed the next leader too. Adds `Publication.MaxMessageLength` and `NewTestingPublication`;
  `Cluster.Offer` returns the code to the caller. Found in trading-poc experiment 04.
- **archive**: `RecordingSignalEvent` reaches the listener on every poll path; template 106 (Ping) is skipped at debug
  level instead of printing "Unexpected message type" (cb253bc). `Control.Poll` decoded the signal and dropped it, and
  `PollForErrorResponse` called the listener twice for its own session and once for foreign ones. Found in trading-poc
  experiment 05.
- **archive tests**: the mock-based unit tests run when no media driver is available (bac340b); `TestMain` returned
  before `m.Run()` and every test was skipped silently. The rotted control tests are fixed.

### Added

- **archive**: `DefaultPollInterval` and `Options.SetPollInterval` (5ed7628). The 50 ms sleep between control-response
  polls was a literal and the latency floor of every synchronous Archive call; the default stays 50 ms.
- **aeron**: `Context.InterServiceTimeout` reaches the client conductor (28c3dd0); unset, the driver's client liveness
  timeout still applies.
- **cluster**: `ClusteredServiceAgent.ElectionState` reads the consensus module's election state counter
  (`ELECTION_STATE_TYPE_ID`, keyed by cluster id) and reports "unknown" when there is no such counter. The trading-poc
  query gate (cloud-prep 67) answers only while the election is `CLOSED`, so it asks on every request instead of keeping
  a copy of the state. Found in trading-poc lab `docs/labs/aeron/11-election-state-query-gate.md` (V4).

### Changed

- **cluster**: `ClusteredServiceAgent.ElectionState` keeps the id and registration id of the counter it found and checks
  them before each read instead of walking the media driver's counters on every call. The check is O(1) and allocates
  nothing: the record is allocated, its type is `ELECTION_STATE_TYPE_ID`, the cluster id in its key is this member's, and
  its registration id is the kept one (a counter freed and re-allocated at the same id has another registration id). A
  kept id that fails is dropped and the counter is looked up again, so a re-created counter is never read stale and no
  counter still reads as unknown. The old call walked every counter: 8.4 us with 1,000 counters and one allocation,
  35 ns and none now, independent of the count. The kept counter is not synchronised; call it from one goroutine (the
  trading-poc query gate calls it from the service goroutine). Found in trading-poc cloud-perf 19.
- **idlestrategy**: `Busy` is renamed to `NoOp`, the Java `NoOpIdleStrategy` it always was, and `BusySpin` is added as
  the Java `BusySpinIdleStrategy`: an idle round executes one CPU spin-wait hint (`PAUSE` on amd64, `YIELD` on arm64,
  an empty function on every other architecture) and returns. `BackoffIdleStrategy`'s spinning state calls the same
  hint instead of only counting spins, replacing the `We should call procyield here` TODO. Found in trading-poc
  `docs/tickets/cloud-perf/issues/10-explicit-idle-strategy.md` (second cloud round): the Go backoff's spin phase never
  told the CPU it was spinning, and the fork had no counterpart of `BusySpinIdleStrategy` to compare against. The old
  name is not aliased (trading-poc is pre-release, AGENTS.md §1.3); the examples use `NoOp`.
- **poll allocation**: the poll loops no longer allocate on every round. `Poll(x.onFragment, n)` passed a method value
  that Go boxes into a closure on the heap at each call, and the handler escapes through `aeron.Image`. The handler is
  now bound once at construction (`boundedLogAdapter`, `serviceAdapter`, `snapshotLoader`, `archive.Control`,
  `AeronCluster`'s egress), and `Control.PollForResponse` binds its handler before its loop. An idle round allocates
  nothing (`testing.AllocsPerRun` = 0 in each site's test); `-gcflags=-m` shows the bound escape only at construction.
  Found in trading-poc `docs/tickets/cloud-perf/issues/13-no-allocation-per-poll.md`: at mixed 20k/s the allocation
  profile charged 0.41 to 0.61 GB per 10 s to each of the two hottest sites (`docs/plan/stress_result_02.md` §8.4), and
  that garbage made GC run more often.
