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
- **cluster client**: a closed publication of a member is made again (cloud-perf 40).
  `onNewLeaderEvent` closed only `ingressPub`, so the old leader's entry in `memberByIdMap` kept a closed but non-nil
  publication; when the leader came back to that member (1 -> 0 -> 1) `updateMemberEndpoints` and `createPublications`
  saw a non-nil publication, did not make a new one, and used the closed one, which never connects. Now a new leader
  closes the publication of every member (`member.close()`, the entry becomes nil), and a closed publication counts as
  none in `updateMemberEndpoints` and `createPublications`. Java closes the whole member table on `onNewLeader` and on
  `REDIRECT`. Found in trading-poc `docs/labs/writer-reconnect/README.md`: after each leader change a writer got no
  answer for up to 15 s and returned 504.
- **cluster client**: after `NEW_LEADER` the new leader's publication, which has not connected yet, is no longer taken
  for a disconnect. The client leaves `Connected` for the new state `AwaitNewLeaderConnection`, keeps the session,
  polls the egress, and is `Connected` again when the publication connects (`OnConnect` is not called: the session is
  the same). It waits 5 s at most (Java: `AWAIT_NEW_LEADER_CONNECTION`, `messageTimeoutNs`), then the client closes and
  `OnError` says so. Before, `Poll` called `OnDisconnect` and opened a new session at once.
- **cluster client**: both waits of the connect treat their deadline the same way (cloud-perf 40 review). The client
  waited for the connect reply while `now < deadline` and so gave up exactly at the deadline, while the wait for the
  publication gives up only after it (`now > deadline`): two waits of one client read the same instant differently. It
  now reads the egress once more at the deadline and gives up one millisecond later.
- **cluster**: `ClusteredServiceAgent.ElectionState` is safe to call from several goroutines (cloud-perf 108). The kept
  election state counter was plain fields, so the service goroutine answering a query and the bridge goroutine asking for
  its term read and wrote them at the same time. The kept counter is now one atomic pointer to a value that is never
  changed (its id and registration id together), so a caller never mixes two counters. Found in trading-poc
  `docs/tickets/cloud-perf/issues/108-bridge-active-only-when-caught-up.md`.

- **archive, cluster**: the range check of decoded protocol messages is always on, and the `RangeChecking` option is
  deleted from `archive.Options`, `cluster.Options` and `cluster/client.Options` (cloud-perf 118). The Archive client kept
  the option in a package-level variable that every `Poll` wrote and every fragment handler read, so two Archive clients
  polled from two goroutines raced on it (`-race`). The Archive default was `false`, so a control response outside its
  schema, such as an unknown `ControlResponse` code, was accepted; it is now an error for `ErrorListener`. Found in
  trading-poc cloud-perf 118.

### Added

- **aeron**: `FindPublication`, `FindSubscription` and `FindCounter` return `*RegistrationError` (`RegistrationID`,
  `Code`, `Message`) for a driver rejection instead of a formatted string; `Error()` keeps the same text. Callers read the
  driver code with `errors.As` and compare it with `command.ErrorCodeResourceTemporarilyUnavailable`, so a retry is decided by
  the code, not the text. Found in trading-poc cloud-perf 75: a probe's replay subscription met the previous probe's endpoint
  still `CLOSING` (code 10) and the bisection gave up.
- **archive**: `DefaultPollInterval` and `Options.SetPollInterval` (5ed7628). The 50 ms sleep between control-response
  polls was a literal and the latency floor of every synchronous Archive call; the default stays 50 ms.
- **aeron**: `Context.InterServiceTimeout` reaches the client conductor (28c3dd0); unset, the driver's client liveness
  timeout still applies.
- **cluster**: `ClusteredServiceAgent.ElectionState` reads the consensus module's election state counter
  (`ELECTION_STATE_TYPE_ID`, keyed by cluster id) and reports "unknown" when there is no such counter. The trading-poc
  query gate (cloud-prep 67) answers only while the election is `CLOSED`, so it asks on every request instead of keeping
  a copy of the state. Found in trading-poc lab `docs/labs/aeron/11-election-state-query-gate.md` (V4).

### Changed

- **cluster client**: the client no longer waits and retries by itself; it closes and tells the listener (cloud-perf 40).
  A timeout waiting for the ingress publication (5 s) or the connect reply (3 s), an `ERROR` event, and
  `AUTHENTICATION_REJECTED` used to put the client in `Disconnected` and retry after a fixed 5 s, 30 s or 60 s. Now
  each of them calls `OnError` with the reason and sets the client closed (`IsClosed()` is true); the owner calls
  `Close` and builds a new client, as with the `CLOSED` event. Java closes the client on these timeouts. The state
  `Disconnected` and `nextRetryConnectTime` are gone. The two reconnect policies of trading-poc (a writer or bridge
  rebuilds after N seconds, and aeron-go waits 30 s) are one owner now. `ingressPublication` and `egressSubscription`
  are the interfaces the client uses of the driver objects; the tests replace them. The correlation id of a connect
  request comes from the driver through `nextCorrelationId`, another seam the tests replace.
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
