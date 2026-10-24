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
  a copy; a restarted consensus module may re-create the counter, so no counter id is kept either. Found in trading-poc
  lab `docs/labs/aeron/11-election-state-query-gate.md` (V4).

### Changed

- **idlestrategy**: `Busy` is renamed to `NoOp`, the Java `NoOpIdleStrategy` it always was, and `BusySpin` is added as
  the Java `BusySpinIdleStrategy`: an idle round executes one CPU spin-wait hint (`PAUSE` on amd64, `YIELD` on arm64,
  an empty function on every other architecture) and returns. `BackoffIdleStrategy`'s spinning state calls the same
  hint instead of only counting spins, replacing the `We should call procyield here` TODO. Found in trading-poc
  `docs/tickets/cloud-perf/issues/10-explicit-idle-strategy.md` (second cloud round): the Go backoff's spin phase never
  told the CPU it was spinning, and the fork had no counterpart of `BusySpinIdleStrategy` to compare against. The old
  name is not aliased (trading-poc is pre-release, AGENTS.md §1.3); the examples use `NoOp`.
